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

import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;

/**
 * Flush and merge of one bucket's projection, run on {@link ColumnarMaintenancePool}; at most one of them (or a compaction) per
 * projection at a time ({@link ColumnarProjection#tryBeginMaintenance()}, V-B6).
 * <p>
 * Locking: the segment file is written with no projection lock held, so apply passes and queries go on. The publish holds
 * {@link ColumnarProjection#applyLock()} (no apply pass runs, so the segment set, the dead sets, the owner maps and the dictionaries
 * stand still while their files and the manifest's tmp file are written), and takes {@link ColumnarProjection#publishLock()} only for
 * the manifest rename and the state swap, so a query releasing a snapshot never waits for the fsync of a written file.
 * <p>
 * Publish order (spec 5.5, {@link ColumnarStore}): segment and dead set files and owner maps durable ({@code publish.segmentsSynced}),
 * dictionaries durable ({@code publish.dictionariesSynced}), manifest tmp file durable, then the rename. Every new file is held through
 * the projection before it is created. What a failure cleans up depends on how far the publish got, never on the exception class:
 * before the rename was attempted, the files written so far are deleted and released (the published manifest names none of them);
 * once it was attempted, a manifest naming them may be on disk, so they are never deleted: a manifest found renamed is adopted, else
 * the files stay held and the next publish takes a higher generation ({@link ColumnarProjection#publishUncertain}). A
 * {@link ColumnarTestHooks.SimulatedCrash} cleans up nothing, as a crash would not.
 */
final class ColumnarFlusher {
  static final String FLUSH_SEGMENT_WRITTEN = "flush.segmentWritten";
  static final String MERGE_SEGMENT_WRITTEN = "merge.segmentWritten";
  static final String SEGMENTS_SYNCED       = "publish.segmentsSynced";
  static final String DICTIONARIES_SYNCED   = "publish.dictionariesSynced";

  /** Pages per window of a merge's live bitmaps, so a merge's heap stays bounded whatever the segment sizes. */
  static final int MERGE_WINDOW_PAGES = 1024;

  private ColumnarFlusher() {
  }

  /**
   * Whether the projection needs a flush now: the overlay holds {@code maxRows} rows or more, or holds rows the oldest of which is
   * {@code intervalMs} old (P39); with no overlay row (a deletes-only workload), when applied pages wait for their dead sets to be
   * persisted and the last flush is {@code intervalMs} old. Always true while a failed flush left a frozen overlay. Cheap: no
   * allocation, one tracker lock at most.
   */
  static boolean shouldFlush(final ColumnarProjection p, final long nowMs, final long intervalMs, final int maxRows) {
    if (p.hasFrozenOverlay())
      return true;
    final ColumnarOverlay overlay = p.overlay();
    final long rows = overlay.rows();
    if (rows >= maxRows)
      return true;
    if (rows > 0)
      return nowMs - overlay.oldestRowMs() >= intervalMs;
    return nowMs - p.lastFlushMs() >= intervalMs && hasUnflushedPages(p);
  }

  private static boolean hasUnflushedPages(final ColumnarProjection p) {
    final DirtyPageTracker external = p.externalTracker();
    return p.tracker().hasUnflushed() || external != null && external.hasUnflushed();
  }

  /**
   * Freezes the overlay, writes its rows as one new segment (positions ascending) and publishes it with the dead sets changed since
   * the last publish and the owner maps; the positions the apply pass superseded in the frozen overlay meanwhile start dead in the
   * new segment (V-B2). Then the pages unflushed at the freeze leave the trackers, except those re-applied since (R14). When there
   * is no row to write and the dead sets, owner maps and dictionaries are as the manifest says (pages whose apply changed nothing), the
   * pages leave the trackers with no publish. The {@code NOWAL} marker is never deleted here (R8): a no-WAL commit's bucket pages reach
   * the disk asynchronously. Nothing happens when there is nothing to persist, on a READ_ONLY database, a closed or dropped projection,
   * or one without a manifest (it waits for a rebuild).
   *
   * @param lastTransactionId written to the manifest, informational only
   *
   * @throws MaintenanceBusyException when another maintenance job of the projection runs
   */
  static void flush(final ColumnarProjection p, final long lastTransactionId) throws IOException {
    beginMaintenance(p);
    try {
      flushHoldingMaintenance(p, lastTransactionId);
    } catch (final PublishingStoppedException e) {
      // closed, dropped or stopped meanwhile: nothing happens, as for one found so at the start (the files written are deleted)
    } finally {
      p.endMaintenance();
    }
  }

  private static void flushHoldingMaintenance(final ColumnarProjection p, final long lastTransactionId) throws IOException {
    final long start = System.nanoTime();
    final ColumnarOverlay frozen;
    final ColumnDefinition[] columns;
    final int[] pages;
    final int[] externalPages;
    p.applyLock().lock();
    try {
      if (!p.canWrite() || p.manifest() == null || !p.hasFrozenOverlay() && p.overlay().rows() == 0 && !hasUnflushedPages(p))
        return;
      frozen = p.freezeOverlay();
      columns = p.columns();
      pages = p.tracker().unflushedPages();
      externalPages = p.externalTracker() == null ? new int[0] : p.externalTracker().unflushedPages();
    } finally {
      p.applyLock().unlock();
    }

    final Attempt attempt = new Attempt();
    try {
      Path segmentFile = null;
      if (frozen != null) {
        final int segmentId = p.reserveSegmentId();
        final String name = SegmentFormat.segmentFileName(segmentId);
        attempt.written.add(name);
        segmentFile = p.bucketDir().resolve(name);
        writeFrozen(p, frozen, columns, segmentFile, segmentId);
        ColumnarTestHooks.reach(FLUSH_SEGMENT_WRITTEN);
      }

      p.applyLock().lock();
      try {
        final ProjectionState current = p.currentState();
        final SegmentReader[] segments = current.segments();
        final SegmentReader[] next;
        final ColumnarSegmentView[] overlays;
        if (segmentFile == null && persistedStateIsCurrent(p, segments)) {
          // only no-op applies since the last publish: nothing to write, the pages are flushed as they are
          p.publishLock().lock();
          try {
            p.flushPublished(pages, externalPages);
          } finally {
            p.publishLock().unlock();
          }
          return;
        }
        if (segmentFile != null) {
          final SegmentReader reader = SegmentReader.open(segmentFile, p.supersededInFrozenCopy());
          attempt.readers.add(reader);
          next = new SegmentReader[segments.length + 1];
          System.arraycopy(segments, 0, next, 0, segments.length);
          next[segments.length] = reader;
          // the frozen view sits just before the live one, which stays
          final ColumnarSegmentView[] views = current.overlays();
          overlays = new ColumnarSegmentView[] { views[views.length - 1] };
        } else {
          next = segments;
          overlays = current.overlays();
        }
        publish(p, next, overlays, lastTransactionId, attempt, () -> p.flushPublished(pages, externalPages));
      } finally {
        p.applyLock().unlock();
      }
    } catch (final ColumnarTestHooks.SimulatedCrash e) {
      throw e;
    } catch (final Throwable e) {
      settle(p, attempt);
      throw e;
    }
    settle(p, attempt);

    if (attempt.published) {
      p.stats().flush(p.typeName());
      p.stats().flushDuration(p.typeName(), System.nanoTime() - start);
    }
  }

  /** Writes the frozen overlay's rows, pages and slots ascending, each with its block's page version. */
  private static void writeFrozen(final ColumnarProjection p, final ColumnarOverlay frozen, final ColumnDefinition[] columns,
      final Path file, final long segmentId) throws IOException {
    final int maxRecordsInPage = p.maxRecordsInPage();
    try (final SegmentWriter writer = new SegmentWriter(file, segmentId, maxRecordsInPage, columns, p.segmentBlockRows())) {
      final RowValues row = new RowValues(columns.length);
      for (final int page : frozen.pages()) {
        checkPublishing(p);
        final OverlayPageBlock block = frozen.page(page);
        final long[] stripe = block.liveStripe();
        final long base = (long) page * maxRecordsInPage;
        int r = 0;
        for (int w = 0; w < stripe.length; w++)
          for (long word = stripe[w]; word != 0; word &= word - 1, r++) {
            block.readRow(r, row);
            writer.addRow(base + ((long) w << 6) + Long.numberOfTrailingZeros(word), block.pageVersion(), row);
          }
      }
      writer.finish();
    }
  }

  /**
   * Rewrites the segments as one, keeping only their live rows, positions ascending; each page keeps the version of the newest input
   * covering it. The merged segment takes the place of the oldest input; positions an apply pass killed in an input while the merge
   * ran start dead in it. A merge whose inputs hold no live row only removes them. Inputs leave the manifest at the publish; their
   * files are deleted by the last release of a state holding them (R1). Nothing happens when an input is not in the current state
   * (any more), on a READ_ONLY database, a closed or dropped projection, or one without a manifest.
   *
   * @throws MaintenanceBusyException when another maintenance job of the projection runs
   */
  static void merge(final ColumnarProjection p, final long[] segmentIds) throws IOException {
    beginMaintenance(p);
    try {
      if (segmentIds.length == 0 || !p.canWrite() || p.manifest() == null)
        return;
      final ProjectionState state = p.retainState();
      try {
        mergeHoldingState(p, state, segmentIds);
      } catch (final PublishingStoppedException e) {
        // as for a flush
      } finally {
        state.release();
      }
    } finally {
      p.endMaintenance();
    }
  }

  private static void mergeHoldingState(final ColumnarProjection p, final ProjectionState state, final long[] segmentIds)
      throws IOException {
    final long start = System.nanoTime();
    final SegmentReader[] inputs = inputsInStateOrder(state.segments(), segmentIds);
    if (inputs == null)
      return;
    final ColumnDefinition[] columns = p.columns();

    final Attempt attempt = new Attempt();
    try {
      final int segmentId = p.reserveSegmentId();
      final String name = SegmentFormat.segmentFileName(segmentId);
      attempt.written.add(name);
      final Path segmentFile = p.bucketDir().resolve(name);
      final long rows = writeMerged(p, inputs, columns, segmentFile, segmentId);
      ColumnarTestHooks.reach(MERGE_SEGMENT_WRITTEN);

      p.applyLock().lock();
      try {
        final ProjectionState current = p.currentState();
        final SegmentReader[] segments = current.segments();
        final int[] at = new int[inputs.length];
        PositionBitmap deadSince = null;
        for (int i = 0; i < inputs.length; i++) {
          at[i] = indexOf(segments, inputs[i].segmentId());
          if (at[i] < 0)
            break; // replaced meanwhile (a rebuild): nothing is published, the merged file is discarded below
          deadSince = addDeadSince(deadSince, segments[at[i]].deadSet(), inputs[i].deadSet());
        }
        if (allFound(at)) {
          final SegmentReader merged = rows > 0 ? SegmentReader.open(segmentFile, deadSince) : null;
          if (merged != null)
            attempt.readers.add(merged);
          publish(p, replace(segments, at, merged), current.overlays(), p.manifest().lastTransactionId(), attempt, () -> {
          });
        }
      } finally {
        p.applyLock().unlock();
      }
    } catch (final ColumnarTestHooks.SimulatedCrash e) {
      throw e;
    } catch (final Throwable e) {
      settle(p, attempt);
      throw e;
    }
    settle(p, attempt);

    if (attempt.published) {
      p.stats().merge(p.typeName());
      p.stats().mergeDuration(p.typeName(), System.nanoTime() - start);
    }
  }

  /** The readers of the ids in the state's (oldest first) order, null when an id is not in the state. */
  private static SegmentReader[] inputsInStateOrder(final SegmentReader[] segments, final long[] segmentIds) {
    final SegmentReader[] inputs = new SegmentReader[segmentIds.length];
    int n = 0;
    for (final SegmentReader segment : segments)
      for (final long id : segmentIds)
        if (segment.segmentId() == id) {
          inputs[n++] = segment;
          break;
        }
    return n == segmentIds.length ? inputs : null;
  }

  private static boolean allFound(final int[] at) {
    for (final int index : at)
      if (index < 0)
        return false;
    return true;
  }

  private static int indexOf(final SegmentReader[] segments, final long segmentId) {
    for (int i = 0; i < segments.length; i++)
      if (segments[i].segmentId() == segmentId)
        return i;
    return -1;
  }

  /** {@code into} plus the positions dead now and not when the merge read the input (dead sets only grow); null while empty. */
  private static PositionBitmap addDeadSince(PositionBitmap into, final PositionBitmap now, final PositionBitmap then) {
    if (now == null || now == then)
      return into;
    final PositionBitmap since = now.copy();
    if (then != null)
      since.andNot(then);
    if (since.isEmpty())
      return into;
    if (into == null)
      return since;
    into.or(since);
    return into;
  }

  /** The segment set with the inputs at {@code at} (ascending) removed and {@code merged}, when not null, at the first one's place. */
  private static SegmentReader[] replace(final SegmentReader[] segments, final int[] at, final SegmentReader merged) {
    final SegmentReader[] next = new SegmentReader[segments.length - at.length + (merged == null ? 0 : 1)];
    int n = 0;
    int skip = 0;
    for (int i = 0; i < segments.length; i++) {
      if (skip < at.length && at[skip] == i) {
        if (skip == 0 && merged != null)
          next[n++] = merged;
        skip++;
        continue;
      }
      next[n++] = segments[i];
    }
    return next;
  }

  /**
   * Writes the live rows of the inputs (oldest first) into a new segment, window by window of {@link #MERGE_WINDOW_PAGES} pages. A
   * position is live in at most one input (spec 5.2); the newest wins should two disagree. Returns the rows written; with none, the
   * file is not kept.
   */
  private static long writeMerged(final ColumnarProjection p, final SegmentReader[] inputs, final ColumnDefinition[] columns,
      final Path file, final long segmentId) throws IOException {
    final int maxRecordsInPage = p.maxRecordsInPage();
    final int words = maxRecordsInPage / 64;
    int minPage = Integer.MAX_VALUE;
    int maxPage = -1;
    for (final SegmentReader input : inputs)
      if (input.minPage() >= 0) {
        minPage = Math.min(minPage, input.minPage());
        maxPage = Math.max(maxPage, input.maxPage());
      }

    long rows = 0;
    try (final SegmentWriter writer = new SegmentWriter(file, segmentId, maxRecordsInPage, columns, p.segmentBlockRows())) {
      final RowValues row = new RowValues(columns.length);
      final PositionBitmap[] live = new PositionBitmap[inputs.length];
      final int[][] pagesOf = new int[inputs.length][];
      final int[] cursor = new int[inputs.length];
      final long[][] stripes = new long[inputs.length][];
      final ColumnReader[][] readers = new ColumnReader[inputs.length][columns.length];
      for (int i = 0; i < inputs.length; i++)
        for (int k = 0; k < columns.length; k++)
          readers[i][k] = ColumnarProjection.columnReader(inputs[i], columns[k]);
      for (long from = minPage; from <= maxPage; from += MERGE_WINDOW_PAGES) {
        checkPublishing(p);
        final int to = (int) Math.min((long) maxPage + 1, from + MERGE_WINDOW_PAGES);
        for (int i = 0; i < inputs.length; i++) {
          live[i] = inputs[i].live((int) from, to);
          pagesOf[i] = live[i].pages();
          cursor[i] = 0;
        }
        for (int page = nextPage(pagesOf, cursor); page >= 0; page = nextPage(pagesOf, cursor)) {
          long version = -1;
          for (int i = inputs.length - 1; i >= 0 && version < 0; i--)
            version = inputs[i].pageVersion(page);
          for (int i = 0; i < inputs.length; i++)
            stripes[i] = live[i].stripe(page);
          final long base = (long) page * maxRecordsInPage;
          for (int w = 0; w < words; w++) {
            long union = 0;
            for (final long[] stripe : stripes)
              if (stripe != null)
                union |= stripe[w];
            for (; union != 0; union &= union - 1) {
              final long bit = union & -union;
              int source = inputs.length - 1;
              while (stripes[source] == null || (stripes[source][w] & bit) == 0)
                source--;
              final long position = base + ((long) w << 6) + Long.numberOfTrailingZeros(bit);
              for (int k = 0; k < columns.length; k++)
                ColumnarProjection.readValue(readers[source][k], position, columns[k], k, row);
              writer.addRow(position, version, row);
              rows++;
            }
          }
        }
      }
      if (rows > 0)
        writer.finish();
    }
    return rows;
  }

  /** The smallest page not yet visited over the inputs' ascending page lists, advancing past it; -1 when every list is done. */
  private static int nextPage(final int[][] pagesOf, final int[] cursor) {
    int page = Integer.MAX_VALUE;
    for (int i = 0; i < pagesOf.length; i++)
      if (cursor[i] < pagesOf[i].length)
        page = Math.min(page, pagesOf[i][cursor[i]]);
    if (page == Integer.MAX_VALUE)
      return -1;
    for (int i = 0; i < pagesOf.length; i++)
      if (cursor[i] < pagesOf[i].length && pagesOf[i][cursor[i]] == page)
        cursor[i]++;
    return page;
  }

  /**
   * Publishes {@code next} as the segment set; the caller holds the apply lock. Writes the dead set of every segment whose dead rows
   * changed since the current manifest (a segment new to it from version 1), the owner maps, fsyncs the dictionaries, then under the
   * publish lock renames the manifest, swaps the state and runs {@code onPublished}. Every file written is held and added to the
   * attempt, which records how far the publish got; {@code attempt.published} stays false when the projection was closed or dropped
   * meanwhile.
   */
  private static void publish(final ColumnarProjection p, final SegmentReader[] next, final ColumnarSegmentView[] overlays,
      final long lastTransactionId, final Attempt attempt, final Runnable onPublished) throws IOException {
    checkPublishing(p);
    final ColumnarManifest current = p.manifest();
    final Path dir = p.bucketDir();
    final long generation = p.nextGeneration();
    final List<String> written = attempt.written;

    final Map<Long, ColumnarManifest.SegmentEntry> previous = new HashMap<>();
    for (final ColumnarManifest.SegmentEntry entry : current.segments())
      previous.put(entry.segmentId(), entry);
    final List<ColumnarManifest.SegmentEntry> segments = new ArrayList<>(next.length);
    for (final SegmentReader reader : next) {
      final ColumnarManifest.SegmentEntry old = previous.get(reader.segmentId());
      final long deadRows = reader.deadCount();
      int deadVersion = old == null || deadRows == 0 ? 0 : old.deadVersion();
      if (deadRows > 0 && (old == null || deadRows != old.deadRows())) {
        // a version an uncertain publish may have named stays held: never written again
        do
          deadVersion++;
        while (p.isHeld(SegmentFormat.deadFileName(reader.segmentId(), deadVersion)));
        final String name = SegmentFormat.deadFileName(reader.segmentId(), deadVersion);
        hold(p, written, name);
        DeadSetFile.write(dir.resolve(name), reader.deadSet());
      }
      segments.add(new ColumnarManifest.SegmentEntry(reader.segmentId(), SegmentFormat.segmentFileName(reader.segmentId()),
          reader.rowCount(), deadVersion, deadRows, reader.minPage(), reader.maxPage()));
    }

    final String ownerMapFile = writeOwnerMap(p, p.owners(), p.ownersLock().readLock(), SegmentFormat.ownerMapFileName(generation),
        written);
    final String externalOwnerMapFile = p.externalOwnersLock() == null ? null : writeOwnerMap(p, p.externalOwners(),
        p.externalOwnersLock().readLock(), SegmentFormat.externalOwnerMapFileName(generation), written);
    ColumnarTestHooks.reach(SEGMENTS_SYNCED);

    final Map<String, GlobalDictionary> writerDictionaries = p.dictionaries();
    final List<ColumnarManifest.DictionaryEntry> dictionaries = new ArrayList<>(current.dictionaries().size());
    for (final ColumnarManifest.DictionaryEntry entry : current.dictionaries()) {
      final String property = p.propertyOf(entry.indexFileId());
      if (property == null)
        continue; // a column the reconciler dropped: its dictionary file is deleted once no state names it
      final GlobalDictionary dictionary = writerDictionaries.get(property);
      if (dictionary == null) {
        dictionaries.add(entry); // a column a rebuild did not publish again yet (RETYPE): kept as published
        continue;
      }
      final GlobalDictionary.DurableState durable = dictionary.fsync();
      dictionaries.add(new ColumnarManifest.DictionaryEntry(entry.indexFileId(), entry.generation(), entry.file(), durable.entries(),
          durable.bytes()));
    }
    ColumnarTestHooks.reach(DICTIONARIES_SYNCED);

    final ColumnarManifest manifest = new ColumnarManifest(generation, p.bucketId(), p.bucketName(), p.maxRecordsInPage(),
        p.manifestColumns(current), segments, dictionaries, ownerMapFile, externalOwnerMapFile, lastTransactionId);
    publishManifest(p, manifest, attempt, () -> {
      if (!p.publish(manifest, next, overlays, writerDictionaries, p.currentState().isUsable()))
        return false;
      p.ownersPublished();
      onPublished.run();
      return true;
    });
  }

  /**
   * The last step of every publish (flush, merge, rebuild): writes the manifest's tmp file outside the publish lock (the published
   * manifest is untouched until the rename), then under the publish lock renames it and runs {@code swap}, which makes the new state
   * current and answers whether it did. The caller holds the apply lock and has made every file the manifest names durable. A failed
   * rename whose manifest is found on disk is adopted; otherwise the generation is recorded as uncertain and the failure thrown.
   * {@code attempt.published} stays false when the projection was closed or dropped meanwhile.
   */
  static void publishManifest(final ColumnarProjection p, final ColumnarManifest manifest, final Attempt attempt, final BooleanSupplier swap)
      throws IOException {
    final Path dir = p.bucketDir();
    checkPublishing(p);
    manifest.writeTmp(dir);
    p.publishLock().lock();
    try {
      if (p.isClosedOrDropped())
        return;
      attempt.manifestEntered = true;
      try {
        manifest.rename(dir);
      } catch (final ColumnarTestHooks.SimulatedCrash e) {
        throw e;
      } catch (final IOException | RuntimeException | Error e) {
        if (!renamed(dir, manifest.generation())) {
          p.publishUncertain(manifest.generation());
          throw e;
        }
        LogManager.instance().log(ColumnarFlusher.class, Level.WARNING,
            "Columnar manifest %d of bucket '%s' was renamed but its publish failed after; adopting it", e, manifest.generation(),
            p.bucketName());
      }
      attempt.published = swap.getAsBoolean();
    } finally {
      p.publishLock().unlock();
    }
  }

  /** After a failed rename: whether the manifest on disk is the one of the generation (the rename happened), made durable again. */
  private static boolean renamed(final Path dir, final long generation) {
    try {
      final ColumnarManifest found = ColumnarManifest.load(dir);
      if (found == null || found.generation() != generation)
        return false;
      ColumnarStore.fsyncDirectory(dir);
      return true;
    } catch (final IOException | RuntimeException e) {
      return false;
    }
  }

  /**
   * Whether the manifest still describes the segments, owner maps and dictionaries exactly, so a flush with no row to write has nothing
   * to publish. The caller holds the apply lock.
   */
  private static boolean persistedStateIsCurrent(final ColumnarProjection p, final SegmentReader[] segments) {
    final ColumnarManifest current = p.manifest();
    if (segments.length != current.segments().size() || p.ownersChangedSincePublish())
      return false;
    for (int i = 0; i < segments.length; i++) {
      final ColumnarManifest.SegmentEntry entry = current.segments().get(i);
      if (segments[i].segmentId() != entry.segmentId() || segments[i].deadCount() != entry.deadRows())
        return false;
    }
    final Map<String, GlobalDictionary> writerDictionaries = p.dictionaries();
    for (final ColumnarManifest.DictionaryEntry entry : current.dictionaries()) {
      final String property = p.propertyOf(entry.indexFileId());
      final GlobalDictionary dictionary = property == null ? null : writerDictionaries.get(property);
      if (dictionary != null && dictionary.size() != entry.entries())
        return false;
    }
    return true;
  }

  /** One flush's, merge's or rebuild's files and how far its publish got, which decides what a failure may clean up. */
  static final class Attempt {
    final List<String>        written = new ArrayList<>();
    /** The new segments' readers, owned here until a published state owns them. */
    final List<SegmentReader> readers = new ArrayList<>();
    /** The manifest rename was attempted: a manifest naming {@link #written} may be on disk. */
    boolean            manifestEntered;
    boolean            published;
  }

  /**
   * Ends an attempt that did not crash: a published one releases its names; an unpublished one closes its reader and, unless the rename
   * was attempted, deletes and releases its files. Files a manifest on disk may name stay held, so no later publish reuses the names.
   */
  static void settle(final ColumnarProjection p, final Attempt attempt) {
    if (!attempt.published) {
      for (final SegmentReader reader : attempt.readers)
        reader.close();
      if (attempt.manifestEntered)
        return;
      discard(p, attempt.written);
    }
    p.releaseFiles(attempt.written.toArray(new String[0]));
  }

  /**
   * Writes the owner map (under its read lock, null for a map no other thread writes) as a new file; null, writing nothing, for an empty
   * map.
   */
  static String writeOwnerMap(final ColumnarProjection p, final ContentOwnerMap owners, final Lock lock, final String name,
      final List<String> written) throws IOException {
    if (lock != null)
      lock.lock();
    try {
      if (owners.isEmpty())
        return null;
      hold(p, written, name);
      owners.writeTo(p.bucketDir().resolve(name));
      return name;
    } finally {
      if (lock != null)
        lock.unlock();
    }
  }

  static void hold(final ColumnarProjection p, final List<String> written, final String name) {
    p.holdFiles(name);
    written.add(name);
  }

  /** Deletes the files an unpublished flush or merge wrote; a failure is logged, the next writable open deletes the orphan. */
  private static void discard(final ColumnarProjection p, final List<String> written) {
    for (final String name : written)
      try {
        Files.deleteIfExists(p.bucketDir().resolve(name));
      } catch (final IOException e) {
        LogManager.instance().log(ColumnarFlusher.class, Level.WARNING, "Cannot delete the unpublished columnar file '%s'", e, name);
      }
  }

  /**
   * Aborts the flush or merge once the projection stopped publishing ({@link ColumnarProjection#stopPublishing()}): the caller's failure
   * path then deletes what it wrote, as no rename was attempted yet.
   */
  static void checkPublishing(final ColumnarProjection p) {
    if (p.isClosedOrDropped())
      throw new PublishingStoppedException(p.bucketName());
  }

  private static void beginMaintenance(final ColumnarProjection p) {
    if (!p.tryBeginMaintenance())
      throw new MaintenanceBusyException(p.bucketName());
  }

  /**
   * Another flush, merge or compaction of the projection holds its maintenance slot. Not a failure: the job is retried later. An
   * {@link IllegalStateException} of its own type, so a caller can tell it from any other refusal.
   */
  static final class MaintenanceBusyException extends IllegalStateException {
    MaintenanceBusyException(final String bucketName) {
      super("Another flush, merge or compaction of the columnar projection of bucket '" + bucketName + "' is running");
    }
  }

  /**
   * The projection stopped publishing (its database closes or drops) while a flush or merge ran: thrown between two steps so the failure
   * path deletes what the job wrote, and swallowed by {@link #flush} and {@link #merge}.
   */
  static final class PublishingStoppedException extends IllegalStateException {
    PublishingStoppedException(final String bucketName) {
      super("The columnar projection of bucket '" + bucketName + "' stopped publishing: the flush, merge or rebuild was abandoned");
    }
  }
}
