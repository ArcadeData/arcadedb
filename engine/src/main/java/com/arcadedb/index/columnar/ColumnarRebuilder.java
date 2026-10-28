/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.index.columnar;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordFactory;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.BinarySerializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;

/**
 * Background rebuild of one bucket's projection from the committed pages of the bucket (spec 6.5), on {@link ColumnarMaintenancePool}.
 * <p>
 * <b>Scheduling (R17).</b> {@link #schedule} coalesces per projection: a request is dropped while a rebuild of the same projection is
 * queued; one arriving while it runs makes it run once more afterwards (with the reason of that request). At most
 * {@link #REBUILD_SEMAPHORE} rebuilds run JVM-wide, a semaphore of its own (spec 5.6), never the one of the vector index rebuilds. No
 * worker ever waits for a permit: a rebuild without one waits in a queue, started by the rebuild that releases a permit or by the next
 * apply tick of any database ({@link #drainWaiting()}), so a rebuild never stalls the flushes that share the pool. A rebuild that
 * actually runs counts {@code rebuilds} once, with its reason; one that finds its projection closed counts nothing. A READ_ONLY
 * database, a closing manager and a closed projection schedule nothing (R16).
 * <p>
 * <b>Run.</b> Counted as a running maintenance job of its manager (a close waits for it, and it aborts at the next page once the close
 * began) and holding the projection's maintenance slot, so no flush, merge or compaction of the projection runs meanwhile. Apply passes
 * go on updating the previous state, and every page they complete from now on is recorded ({@link ColumnarProjection#beginRebuildTracking}).
 * The scan visits pages {@code 0..getCommittedPageCount()} through {@link LocalBucket#visitCommittedPage} with no lock held, extracting
 * every record into staging dictionaries (new generation files, {@link GlobalDictionary#create}, R10) and {@link SegmentWriter}s, a new
 * one at the first page boundary after {@code arcadedb.columnar.flushMaxRows} rows, and builds the content owner maps.
 * For {@code check_fix} and {@code schema} with a
 * previous valid state (and no external-property bucket, whose pages can change without the primary page's version moving) a page
 * whose committed version equals the version the previous state's segments recorded for it, and that no overlay row of that state
 * covers, copies the rows of its records from those segments (spec 6.5 "skip unchanged pages", counted in {@code rebuild.pages.reused});
 * a record with placeholder or chunk content (its content pages can change on their own) is always extracted.
 * <p>
 * <b>Publish</b> (Task 5 order, the {@link ColumnarFlusher} protocol): under the apply lock, with the generation of
 * {@link ColumnarProjection#nextGeneration()}, owner maps written, dictionaries fsynced, manifest renamed, then
 * {@link ColumnarProjection#rebuildPublished} swaps the state in and marks dirty again every page applied during the scan. Failures
 * are handled by phase: before the rename is attempted the written files are deleted; after, they stay held. A schema or
 * {@code check_fix} rebuild never makes the projection unusable: the previous valid state serves until the rebuilt one publishes.
 */
final class ColumnarRebuilder {
  static final String CREATE           = "create";
  static final String SNAPSHOT_INSTALL = "snapshot_install";
  static final String UNCLEAN_NO_WAL   = "unclean_no_wal";
  static final String SCHEMA           = "schema";
  static final String CHECK_FIX        = "check_fix";
  /** Reached once the scan wrote every segment, before the publish takes the apply lock. */
  static final String SCANNED          = "rebuild.scanned";

  /** Rebuilds running JVM-wide; separate from the vector index rebuild permits (spec 5.6). */
  static final Semaphore REBUILD_SEMAPHORE = new Semaphore(Math.max(1, GlobalConfiguration.COLUMNAR_REBUILD_CONCURRENCY.getValueAsInteger()));

  /** Rebuilds queued for a permit, JVM-wide; at most one per projection (R17). */
  private static final Queue<Job> WAITING = new ConcurrentLinkedQueue<>();

  /**
   * The coalescing state of one projection ({@link ColumnarProjection#rebuildRequest}, null when idle): queued with its reason, or
   * running, with the reason of the request that arrived meanwhile (null for none).
   */
  record Request(String reason, boolean running, String rerunReason) {
  }

  private ColumnarRebuilder() {
  }

  /**
   * Requests a rebuild of the projection with the reason ({@code create}, {@code snapshot_install}, {@code missing}, {@code corrupt},
   * {@code unclean_no_wal}, {@code schema}, {@code check_fix}). Coalesced, see the class comment; never blocks.
   */
  static void schedule(final ColumnarIndexManager manager, final ColumnarProjection projection, final String reason) {
    if (manager.isReadOnly() || manager.isClosing() || projection.isClosedOrDropped())
      return;
    final AtomicReference<Request> state = projection.rebuildRequest;
    for (; ; ) {
      final Request current = state.get();
      if (current == null) {
        if (state.compareAndSet(null, new Request(reason, false, null))) {
          WAITING.offer(new Job(manager, projection, reason));
          drainWaiting();
          return;
        }
      } else if (!current.running() || current.rerunReason() != null)
        return;
      else if (state.compareAndSet(current, new Request(current.reason(), true, reason)))
        return;
    }
  }

  /** A rebuild waits for a permit; lock free, the apply tick reads it. */
  static boolean hasWaiting() {
    return !WAITING.isEmpty();
  }

  /** Starts waiting rebuilds on {@link ColumnarMaintenancePool} while permits are free; never waits for one. */
  static void drainWaiting() {
    while (!WAITING.isEmpty() && REBUILD_SEMAPHORE.tryAcquire()) {
      final Job job = WAITING.poll();
      if (job == null) {
        // another thread took it meanwhile
        REBUILD_SEMAPHORE.release();
        return;
      }
      try {
        ColumnarMaintenancePool.getInstance().getExecutorService().execute(job);
      } catch (final RejectedExecutionException e) {
        REBUILD_SEMAPHORE.release();
        job.projection.rebuildRequest.set(null);
        return;
      }
    }
  }

  /** One rebuild holding a permit; queues the rerun its projection asked for while it ran. */
  private record Job(ColumnarIndexManager manager, ColumnarProjection projection, String reason) implements Runnable {
    @Override
    public void run() {
      try {
        // only this job leaves the queued state; everything the rebuild reads is read after this point, so a request coalesced with
        // the queued state is seen by this run
        projection.rebuildRequest.set(new Request(reason, true, null));
        manager.runRebuildBody(projection, () -> rebuild(manager, projection, reason));
      } finally {
        REBUILD_SEMAPHORE.release();
        finish();
        drainWaiting();
      }
    }

    private void finish() {
      final AtomicReference<Request> state = projection.rebuildRequest;
      for (; ; ) {
        final Request current = state.get();
        if (current.rerunReason() == null) {
          if (state.compareAndSet(current, null))
            return;
        } else if (state.compareAndSet(current, new Request(current.rerunReason(), false, null))) {
          if (manager.isClosing() || projection.isClosedOrDropped())
            state.set(null);
          else
            WAITING.offer(new Job(manager, projection, current.rerunReason()));
          return;
        }
      }
    }
  }

  /** The body of a rebuild, counted as running by its manager. */
  private static void rebuild(final ColumnarIndexManager manager, final ColumnarProjection projection, final String reason) {
    if (!projection.canWrite())
      return;
    // a running flush, merge or compaction of the projection ends soon; the rebuild then holds the slot for its whole run
    while (!projection.tryBeginMaintenance()) {
      if (!projection.canWrite() || manager.isClosing())
        return;
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
    try {
      projection.stats().rebuild(projection.typeName(), reason);
      final boolean published = ColumnarApplier.inDatabaseContext(manager.getDatabase(), () -> {
        try {
          return new Run(manager, projection, reason).execute();
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      });
      if (published)
        manager.rebuildPublished(projection);
    } catch (final ColumnarTestHooks.SimulatedCrash | ColumnarFlusher.PublishingStoppedException e) {
      // a crash test cleans nothing up, as a crash would not; a close stopped the rebuild
    } catch (final RuntimeException e) {
      LogManager.instance().log(ColumnarRebuilder.class, Level.SEVERE,
          "Rebuild (%s) of the columnar projection of bucket '%s' failed: the projection keeps its previous state", e, reason,
          projection.bucketName());
    } finally {
      projection.endMaintenance();
    }
  }

  /** One scan and publish; used once. */
  private static final class Run implements LocalBucket.CommittedSlotVisitor {
    private final ColumnarIndexManager         manager;
    private final ColumnarProjection           p;
    private final String                       reason;
    private final ColumnDefinition[]           columns;
    private final int                          maxRecordsInPage;
    private final int                          maxRows;
    private final ColumnarFlusher.Attempt      attempt          = new ColumnarFlusher.Attempt();
    private final GlobalDictionary[]           dictionaries;
    private final int[]                        generations;
    private final Map<String, GlobalDictionary> dictionaryMap   = new HashMap<>();
    private final ContentOwnerMap              owners           = new ContentOwnerMap();
    private final ContentOwnerMap              externalOwners;
    private final RowValues                    fresh;
    private final RowValues                    old;
    private final ColumnarApplier.Sink         sink;
    private final int[]                        rowFailures;
    private final long[]                       failures;

    // set by execute
    private LocalBucket                          bucket;
    private DocumentType                         type;
    private DatabaseInternal                     database;
    private RecordFactory                        recordFactory;
    private BinarySerializer                     serializer;
    private ColumnarValueExtractor               extractor;
    private ColumnarApplier.ExternalValuePages   external;
    /** The previous valid state the unchanged pages are copied from, null when nothing is reused. */
    private ProjectionState                      previous;
    private GlobalDictionary[]                   previousDictionaries;
    private SegmentWriter                        writer;
    private Path                                 writerFile;
    private long                                 writerRows;
    private long                                 unreadable;
    private long                                 reusedPages;
    // per page
    private long                                 pageVersion;
    private boolean                              reusePage;
    private boolean                              pageExtracted;
    private int                                  pageCopied;

    private Run(final ColumnarIndexManager manager, final ColumnarProjection p, final String reason) {
      this.manager = manager;
      this.p = p;
      this.reason = reason;
      this.columns = p.columns();
      this.maxRecordsInPage = p.maxRecordsInPage();
      this.maxRows = Math.max(1,
          manager.getDatabase().getConfiguration().getValueAsInteger(GlobalConfiguration.COLUMNAR_FLUSH_MAX_ROWS));
      this.dictionaries = new GlobalDictionary[columns.length];
      this.generations = new int[columns.length];
      this.externalOwners = p.externalFileId() < 0 ? null : new ContentOwnerMap();
      this.fresh = new RowValues(columns.length);
      this.old = new RowValues(columns.length);
      this.sink = new ColumnarApplier.Sink(fresh);
      this.rowFailures = new int[columns.length];
      this.failures = new long[columns.length];
    }

    /** @return true when the rebuilt state was published */
    private boolean execute() throws IOException {
      final LocalDatabase db = manager.getDatabase();
      bucket = (LocalBucket) db.getSchema().getBucketById(p.bucketId());
      type = db.getSchema().getTypeByBucketId(p.bucketId());
      database = db.getWrappedDatabaseInstance();
      recordFactory = db.getRecordFactory();
      serializer = db.getSerializer();
      extractor = new ColumnarValueExtractor(type == null ? p.typeName() : type.getName(), columns, (bucketId, property) -> {
        throw new IllegalStateException("The rebuild passes its dictionaries explicitly");
      }, p.stats());
      external = p.externalFileId() < 0 ? null : new ColumnarApplier.ExternalValuePages((LocalBucket) db.getSchema()
          .getBucketById(p.externalFileId()));
      // the conversion failures of a column are counted by the rebuild that first publishes it; a column the current state publishes
      // counted them already (spec 5.3: once per record value)
      final boolean[] countFailures = new boolean[columns.length];
      for (int k = 0; k < columns.length; k++)
        countFailures[k] = !p.isPublishedWith(columns[k].indexFileId());
      previous = retainReusableState();

      p.beginRebuildTracking();
      boolean published = false;
      try {
        ColumnarStore.createBucketDir(Path.of(db.getDatabasePath()), p.bucketId());
        p.deleteOrphansForRebuild();
        createDictionaries();
        scan();
        ColumnarTestHooks.reach(SCANNED);
        published = publish(db.getLastTransactionId());
      } catch (final ColumnarTestHooks.SimulatedCrash e) {
        throw e;
      } catch (final Throwable e) {
        abandon();
        throw e;
      } finally {
        p.endRebuildTracking();
        if (previous != null)
          previous.release();
      }
      if (!published)
        abandon();
      else
        ColumnarFlusher.settle(p, attempt);

      if (published) {
        final ColumnarStats stats = p.stats();
        if (reusedPages > 0)
          stats.rebuildPagesReused(p.typeName(), reusedPages);
        for (long i = 0; i < unreadable; i++)
          stats.unreadable(p.typeName());
        for (int k = 0; k < columns.length; k++)
          if (countFailures[k] && failures[k] > 0)
              extractor.countFailures(columns[k], (int) Math.min(failures[k], Integer.MAX_VALUE));
      }
      return published;
    }

    /** The current state when its unchanged pages may be copied (see the class comment), retained; null otherwise. */
    private ProjectionState retainReusableState() {
      if (!reason.equals(CHECK_FIX) && !reason.equals(SCHEMA) || p.externalFileId() >= 0 || !p.isUsable())
        return null;
      final ProjectionState state = p.retainState();
      final GlobalDictionary[] found = new GlobalDictionary[columns.length];
      for (int k = 0; k < columns.length; k++) {
        final ColumnKind kind = columns[k].kind();
        if (kind != ColumnKind.ORDINAL && kind != ColumnKind.ORDINAL_LIST)
          continue;
        found[k] = state.dictionary(columns[k].property());
        if (found[k] == null) {
          // a column the state has no dictionary of yet: nothing of it can be copied
          state.release();
          return null;
        }
      }
      if (!state.isUsable()) {
        state.release();
        return null;
      }
      previousDictionaries = found;
      return state;
    }

    private void createDictionaries() throws IOException {
      for (int k = 0; k < columns.length; k++) {
        final ColumnKind kind = columns[k].kind();
        if (kind != ColumnKind.ORDINAL && kind != ColumnKind.ORDINAL_LIST)
          continue;
        generations[k] = p.reserveDictionaryGeneration(columns[k].indexFileId());
        final String name = SegmentFormat.dictionaryFileName(columns[k].indexFileId(), generations[k]);
        attempt.written.add(name);
        dictionaries[k] = GlobalDictionary.create(p.bucketDir().resolve(name));
        dictionaryMap.put(columns[k].property(), dictionaries[k]);
      }
    }

    private void scan() throws IOException {
      final int pages = bucket.getCommittedPageCount();
      for (int page = 0; page < pages; page++) {
        if (manager.isKilled())
          // a crash simulation: stops like a crash, cleaning nothing up (no I/O after a kill)
          throw new ColumnarTestHooks.SimulatedCrash();
        if (p.isClosedOrDropped() || manager.isClosing())
          throw new ColumnarFlusher.PublishingStoppedException(p.bucketName());
        pageExtracted = false;
        pageCopied = 0;
        bucket.visitCommittedPage(page, this);
        if (reusePage && !pageExtracted && pageCopied > 0)
          reusedPages++;
        if (writer != null && writerRows >= maxRows)
          finishWriter();
      }
      if (writer != null)
        finishWriter();
    }

    /**
     * The previous state's segments recorded the page at this version, each covering it holds every column, and no overlay row of the
     * state is on it: its live rows in the segments are the page's records.
     */
    private boolean unchangedSincePrevious(final int page, final long version) {
      long known = -1;
      for (final SegmentReader segment : previous.segments()) {
        final long recorded = segment.pageVersion(page);
        if (recorded < 0)
          continue;
        for (final ColumnDefinition column : columns)
          if (ColumnarProjection.columnReader(segment, column) == null)
            return false;
        known = Math.max(known, recorded);
      }
      if (known != version)
        return false;
      for (final ColumnarSegmentView overlay : previous.overlays())
        if (!overlay.live(page, page + 1).isEmpty())
          return false;
      return true;
    }

    /**
     * The version of the page about to be visited, from the same committed read as its slots: the rows written for it carry it, and
     * the reuse decision compares it with the previous state's. A commit landing later re-marks the page, so a copied row older than
     * a slot read meanwhile is corrected by the re-mark at publish.
     */
    @Override
    public void onPageVersion(final int pageNumber, final long version) {
      pageVersion = Math.max(version, 0L);
      reusePage = previous != null && version >= 0 && unchangedSincePrevious(pageNumber, version);
    }

    @Override
    public void onSlot(final long position, final int kind, final Binary content, final int[] offPagePages, final int offPageCount) {
      if (kind != RECORD)
        return;
      if (content == null) {
        // a broken pointer or chain, a record in motion: not projected; a commit that repairs it re-marks the page
        unreadable++;
        pageExtracted = true;
        return;
      }
      if (reusePage && offPageCount == 0 && copyFromPrevious(position)) {
        pageCopied++;
        addRow(position);
        return;
      }
      pageExtracted = true;

      final int externalCount;
      try {
        final Record record = recordFactory.newImmutableRecord(database, type, new RID(p.bucketId(), position), content, null);
        if (!(record instanceof Document document))
          return;
        for (int k = 0; k < columns.length; k++) {
          fresh.present[k] = false;
          sink.k = k;
          rowFailures[k] = extractor.extractForApplyUncounted(document, columns[k], dictionaries[k], sink);
        }
        externalCount = external == null ? 0 : external.collect(serializer, database, document, columns);
      } catch (final ColumnarTestHooks.SimulatedCrash | UncheckedIOException e) {
        // a crash test, or a dictionary append that failed: the rebuild fails
        throw e;
      } catch (final RuntimeException e) {
        // bytes that do not deserialize, an external value that cannot be read: not projected, as a broken chain
        unreadable++;
        return;
      }
      if (offPageCount > 0)
        owners.setOwnerPages(position, offPagePages, offPageCount);
      if (externalCount > 0)
        externalOwners.setOwnerPages(position, external.pages(), externalCount);
      for (int k = 0; k < columns.length; k++)
        failures[k] += rowFailures[k];
      addRow(position);
    }

    /** Fills {@link #fresh} from the live row of the position in the previous state's segments, re-encoded; false when none holds it. */
    private boolean copyFromPrevious(final long position) {
      final SegmentReader[] segments = previous.segments();
      for (int i = segments.length - 1; i >= 0; i--) {
        final SegmentReader segment = segments[i];
        if (segment.row(position) < 0)
          continue;
        final PositionBitmap dead = segment.deadSet();
        if (dead != null && dead.get(position))
          continue;
        for (int k = 0; k < columns.length; k++) {
          ColumnarProjection.readSegmentValue(segment, position, columns[k], k, old);
          reencode(k);
        }
        return true;
      }
      return false;
    }

    /** Copies slot {@code k} of {@link #old} into {@link #fresh}, ordinals translated from the previous dictionary to the staging one. */
    private void reencode(final int k) {
      fresh.present[k] = old.present[k];
      if (!old.present[k])
        return;
      switch (columns[k].kind()) {
      case ORDINAL -> fresh.ordinal[k] = dictionaries[k].getOrAdd(previousDictionaries[k].valueOf(old.ordinal[k]));
      case LONG -> fresh.longs[k] = old.longs[k];
      case DOUBLE -> fresh.doubles[k] = old.doubles[k];
      case ORDINAL_LIST -> {
        final int count = old.listCount[k];
        int[] list = fresh.list[k];
        if (list == null || list.length < count)
          fresh.list[k] = list = new int[Math.max(count, 8)];
        for (int i = 0; i < count; i++)
          list[i] = dictionaries[k].getOrAdd(previousDictionaries[k].valueOf(old.list[k][i]));
        // the staging dictionary numbers the values in another order: strictly ascending again
        Arrays.sort(list, 0, count);
        int unique = 0;
        for (int i = 0; i < count; i++)
          if (unique == 0 || list[i] != list[unique - 1])
            list[unique++] = list[i];
        fresh.listCount[k] = unique;
      }
      }
    }

    private void addRow(final long position) {
      try {
        if (writer == null)
          openWriter();
        writer.addRow(position, pageVersion, fresh);
        writerRows++;
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private void openWriter() throws IOException {
      final int segmentId = p.reserveSegmentId();
      final String name = SegmentFormat.segmentFileName(segmentId);
      attempt.written.add(name);
      writerFile = p.bucketDir().resolve(name);
      writer = new SegmentWriter(writerFile, segmentId, maxRecordsInPage, columns, p.segmentBlockRows());
      writerRows = 0;
    }

    private void finishWriter() throws IOException {
      try {
        writer.finish();
      } finally {
        writer.close();
        writer = null;
      }
      attempt.readers.add(SegmentReader.open(writerFile, null));
    }

    /** Under the apply lock: owner maps, dictionaries, manifest, then the state swap (see the class comment). */
    private boolean publish(final long lastTransactionId) throws IOException {
      p.applyLock().lock();
      try {
        ColumnarFlusher.checkPublishing(p);
        final long generation = p.nextGeneration();
        final String ownerMapFile = ColumnarFlusher.writeOwnerMap(p, owners, null, SegmentFormat.ownerMapFileName(generation),
            attempt.written);
        final String externalOwnerMapFile = externalOwners == null ? null : ColumnarFlusher.writeOwnerMap(p, externalOwners, null,
            SegmentFormat.externalOwnerMapFileName(generation), attempt.written);
        ColumnarTestHooks.reach(ColumnarFlusher.SEGMENTS_SYNCED);

        final List<ColumnarManifest.DictionaryEntry> dictionaryEntries = new ArrayList<>();
        for (int k = 0; k < columns.length; k++)
          if (dictionaries[k] != null) {
            final GlobalDictionary.DurableState durable = dictionaries[k].fsync();
            dictionaryEntries.add(new ColumnarManifest.DictionaryEntry(columns[k].indexFileId(), generations[k],
                SegmentFormat.dictionaryFileName(columns[k].indexFileId(), generations[k]), durable.entries(), durable.bytes()));
          }
        ColumnarTestHooks.reach(ColumnarFlusher.DICTIONARIES_SYNCED);

        final SegmentReader[] readers = attempt.readers.toArray(new SegmentReader[0]);
        final List<ColumnarManifest.SegmentEntry> segmentEntries = new ArrayList<>(readers.length);
        for (final SegmentReader reader : readers)
          segmentEntries.add(new ColumnarManifest.SegmentEntry(reader.segmentId(), SegmentFormat.segmentFileName(reader.segmentId()),
              reader.rowCount(), 0, 0, reader.minPage(), reader.maxPage()));
        final ColumnarManifest manifest = new ColumnarManifest(generation, p.bucketId(), p.bucketName(), maxRecordsInPage, columns,
            segmentEntries, dictionaryEntries, ownerMapFile, externalOwnerMapFile, lastTransactionId);
        final Map<String, GlobalDictionary> published = Map.copyOf(dictionaryMap);
        ColumnarFlusher.publishManifest(p, manifest, attempt,
            () -> p.rebuildPublished(manifest, readers, published, owners, externalOwners));
        return attempt.published;
      } finally {
        p.applyLock().unlock();
      }
    }

    /** A run that did not publish: its writer, readers and dictionaries closed, its files deleted unless a rename was attempted. */
    private void abandon() {
      if (writer != null)
        try {
          writer.close();
        } catch (final IOException e) {
          LogManager.instance().log(ColumnarRebuilder.class, Level.WARNING, "Cannot close the rebuild segment '%s'", e, writerFile);
        } finally {
          writer = null;
        }
      for (final GlobalDictionary dictionary : dictionaries)
        if (dictionary != null)
          dictionary.close();
      ColumnarFlusher.settle(p, attempt);
    }
  }
}
