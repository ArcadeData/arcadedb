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
package com.arcadedb.index.sparsevector;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.IndexException;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.utility.IntHashSet;
import com.arcadedb.utility.LongHashSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Page-component-backed orchestrator for the v1 sparse-vector storage backend. Each sealed
 * segment lives as a {@link SparseSegmentComponent} owned by ArcadeDB's {@code FileManager};
 * flushes and compactions run inside {@code database.transaction(...)} so the page WAL captures
 * every byte of the new segment alongside the regular transaction record - no separate fsync,
 * no flush-on-commit hook, no sparse-vector-specific recovery code.
 * <p>
 * <b>Concurrency.</b> Writes ({@link #put}, {@link #remove}) are lock-free against the memtable.
 * Reads ({@link #topK}) take an atomic snapshot of the current memtable + segment set; the
 * snapshot is stable for the duration of the query even if a flush or compaction commits a new
 * publication mid-query. Flush, compaction, and engine close serialize on a single mutator lock
 * to keep the segment-set publication ordering well-defined.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PaginatedSparseVectorEngine implements AutoCloseable {

  /**
   * Memtable posting count above which {@link #maybeFlush()} flushes to a sealed segment. Picked
   * so a memtable-heavy phase consumes O(100 MiB) heap rather than scaling unbounded with insert
   * volume; large enough that small individual commits don't each spawn their own segment file.
   */
  static final long DEFAULT_MEMTABLE_FLUSH_THRESHOLD = 1_000_000L;

  private final DatabaseInternal  database;
  private final String            indexName;
  private final SegmentParameters params;
  private final long              memtableFlushThreshold;

  private final AtomicReference<Memtable>                  memtable     = new AtomicReference<>(new Memtable());
  private final AtomicReference<PaginatedSegmentReader[]>  segments     = new AtomicReference<>(new PaginatedSegmentReader[0]);
  private final AtomicLong                                 nextSegmentId = new AtomicLong(1L);
  private final ReentrantLock                              mutatorLock  = new ReentrantLock();

  private volatile boolean closed;

  public PaginatedSparseVectorEngine(final DatabaseInternal database, final String indexName, final SegmentParameters params) {
    this(database, indexName, params, DEFAULT_MEMTABLE_FLUSH_THRESHOLD);
  }

  PaginatedSparseVectorEngine(final DatabaseInternal database, final String indexName, final SegmentParameters params,
      final long memtableFlushThreshold) {
    this.database = database;
    this.indexName = indexName;
    this.params = params;
    this.memtableFlushThreshold = memtableFlushThreshold;
    loadExistingSegments();
  }

  /**
   * Flush the memtable iff its posting count is at or above the configured threshold; cheap no-op
   * otherwise. Called from the wrapper's post-commit callback so a long bulk-load amortizes
   * memtable cost into a few sealed segments instead of growing unbounded toward OOM.
   */
  public void maybeFlush() {
    if (memtable.get().totalPostings() >= memtableFlushThreshold)
      flush();
  }

  // --- writes ---------------------------------------------------------------

  public void put(final int dim, final RID rid, final float weight) {
    ensureOpen();
    memtable.get().put(dim, rid, weight);
  }

  public void remove(final int dim, final RID rid) {
    ensureOpen();
    memtable.get().remove(dim, rid);
  }

  // --- reads ----------------------------------------------------------------

  public List<RidScore> topK(final int[] queryDims, final float[] queryWeights, final int k) throws IOException {
    ensureOpen();
    if (k <= 0)
      return List.of();
    if (queryDims.length != queryWeights.length)
      throw new IllegalArgumentException("queryDims and queryWeights must have equal length");

    refreshSegmentsFromFileManager();

    final Memtable mtSnapshot = memtable.get();
    final PaginatedSegmentReader[] segSnapshot = segments.get();

    final DimCursor[] cursors = new DimCursor[queryDims.length];
    try {
      for (int i = 0; i < queryDims.length; i++)
        cursors[i] = openMergedCursor(queryDims[i], mtSnapshot, segSnapshot);
      return BmwScorer.topK(queryDims, queryWeights, cursors, k);
    } finally {
      for (final DimCursor c : cursors)
        if (c != null)
          c.close();
    }
  }

  /**
   * Number of distinct live (non-tombstone) postings under one dim across the memtable + active
   * segments, after newest-source-wins merging. Used to compute IDF document frequency.
   */
  public long countDim(final int dim) throws IOException {
    final DimCursor c = openMergedCursor(dim, memtable.get(), segments.get());
    if (c == null)
      return 0L;
    long df = 0L;
    try {
      c.start();
      while (!c.isExhausted()) {
        if (!c.isTombstone())
          df++;
        if (!c.advance())
          break;
      }
    } finally {
      c.close();
    }
    return df;
  }

  // --- maintenance ----------------------------------------------------------

  /**
   * Flush the current memtable to a new sealed segment. Returns the new segment id, or
   * {@code -1L} if the memtable was empty (or this server is a Raft follower - followers receive
   * segments from the leader via the standard component-shipping path).
   * <p>
   * The build runs inside {@link DatabaseInternal#runWithCompactionReplication}: the file
   * registration and page allocations are captured by the file-manager recording session so that
   * a {@code SCHEMA_ENTRY} carrying the new component metadata + a synthetic WAL of its pages is
   * shipped to followers atomically with the leader's local commit. On a standalone (non-HA)
   * database the override is a no-op wrapper and the inner transaction is the durability point.
   */
  public long flush() {
    ensureOpen();
    mutatorLock.lock();
    try {
      final Memtable old = memtable.getAndSet(new Memtable());
      if (old.isEmpty())
        return -1L;
      final long segmentId = nextSegmentId.getAndIncrement();
      final SparseSegmentComponent[] componentRef = new SparseSegmentComponent[1];
      final boolean ranOnLeader;
      try {
        ranOnLeader = database.getWrappedDatabaseInstance().runWithCompactionReplication(() -> {
          componentRef[0] = createComponent(segmentId);
          try {
            database.transaction(() -> {
              try (final SparseSegmentBuilder b = new SparseSegmentBuilder(componentRef[0], params)) {
                b.setSegmentId(segmentId);
                for (final int dim : old.sortedDims()) {
                  final Iterator<MemtablePosting> it = old.iterateDim(dim);
                  if (!it.hasNext())
                    continue;
                  b.startDim(dim);
                  while (it.hasNext()) {
                    final MemtablePosting p = it.next();
                    if (p.tombstone())
                      b.appendTombstone(p.rid());
                    else
                      b.appendPosting(p.rid(), p.weight());
                  }
                  b.endDim();
                }
                b.finish();
              }
            });
          } catch (final RuntimeException buildFailure) {
            // The build aborted (e.g. dim_index page overflow when a single segment has more
            // unique dims than fit in one page; tracked as a follow-up). createComponent already
            // registered the segment file with the FileManager, so leaving it would expose an
            // empty file to the next refreshSegmentsFromFileManager scan and crash queries.
            // Drop it before propagating; the memtable's data is preserved by the caller's lock.
            try {
              dropComponent(componentRef[0]);
            } catch (final RuntimeException dropFailure) {
              buildFailure.addSuppressed(dropFailure);
            }
            throw buildFailure;
          }
          // The inner transaction's commit queues pages for async flush. HA's
          // runWithCompactionReplication reads the new file straight off the channel after this
          // callback returns (serializeFilePagesAsWal); without this drain it would see zeros.
          database.getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
          return Boolean.TRUE;
        });
      } catch (final IOException | InterruptedException e) {
        throw new IndexException("Failed to flush sparse vector engine '" + indexName + "'", e);
      }
      if (!ranOnLeader)
        return -1L;
      try {
        appendSegment(new PaginatedSegmentReader(componentRef[0]));
      } catch (final IOException e) {
        throw new IndexException("Failed to open freshly-flushed sparse segment '" + indexName + "_seg" + segmentId + "'", e);
      }
      return segmentId;
    } finally {
      mutatorLock.unlock();
    }
  }

  /**
   * Force a full compaction of every active segment into one. Returns the new segment id, or
   * {@code -1L} if there is nothing to compact (zero or one segment, the merge produced an
   * empty result, or this server is a Raft follower - followers receive the merged segment from
   * the leader instead of compacting independently).
   */
  public long compactAll() {
    return compactInputs(/* dropAllTombstones */ true, active -> active.length < 2 ? null
        : sortedCopy(active));
  }

  /** Compact the {@code count} oldest segments into one. */
  public long compactOldest(final int count) {
    if (count < 2)
      return -1L;
    return compactInputs(/* dropAllTombstones */ false, active -> {
      if (active.length < count)
        return null;
      final PaginatedSegmentReader[] sortedAll = sortedCopy(active);
      return Arrays.copyOf(sortedAll, count);
    });
  }

  private static PaginatedSegmentReader[] sortedCopy(final PaginatedSegmentReader[] in) {
    final PaginatedSegmentReader[] out = Arrays.copyOf(in, in.length);
    Arrays.sort(out, Comparator.comparingLong(PaginatedSegmentReader::segmentId));
    return out;
  }

  private long compactInputs(final boolean dropAllTombstones,
      final java.util.function.Function<PaginatedSegmentReader[], PaginatedSegmentReader[]> pickInputs) {
    ensureOpen();
    mutatorLock.lock();
    try {
      final PaginatedSegmentReader[] active = segments.get();
      final PaginatedSegmentReader[] inputs = pickInputs.apply(active);
      if (inputs == null || inputs.length < 2)
        return -1L;

      final long newId = nextSegmentId.getAndIncrement();
      final SparseSegmentComponent[] componentRef = new SparseSegmentComponent[1];
      final boolean[] wroteAnything = { false };
      final boolean ranOnLeader;
      try {
        ranOnLeader = database.getWrappedDatabaseInstance().runWithCompactionReplication(() -> {
          componentRef[0] = createComponent(newId);
          try {
            database.transaction(() -> {
              try (final SparseSegmentBuilder b = new SparseSegmentBuilder(componentRef[0], params)) {
                b.setSegmentId(newId);
                final long[] parentIds = new long[inputs.length];
                for (int i = 0; i < inputs.length; i++)
                  parentIds[i] = inputs[i].segmentId();
                b.setParentSegments(parentIds);
                try {
                  wroteAnything[0] = mergeIntoBuilder(b, inputs, dropAllTombstones);
                } catch (final IOException e) {
                  throw new IndexException("Failed to merge sparse segments during compaction", e);
                }
                b.finish();
              }
            });
          } catch (final RuntimeException buildFailure) {
            // Same orphan-protection as flush(): drop the partial component so the next
            // refreshSegmentsFromFileManager scan doesn't try to open an empty file.
            try {
              dropComponent(componentRef[0]);
            } catch (final RuntimeException dropFailure) {
              buildFailure.addSuppressed(dropFailure);
            }
            throw buildFailure;
          }
          // Drain the page cache's async writer so the synthetic WAL HA ships in this same
          // recording session sees the final on-disk pages instead of zeros.
          database.getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
          // Whatever we do next runs inside the same recording session so the SCHEMA_ENTRY
          // captures both the new component and any input retirements atomically.
          if (!wroteAnything[0])
            // Empty merge (everything was tombstoned): drop the empty new component and the
            // inputs together. The recording session sees the create+delete pair on the new
            // component as a wash, and the inputs go away cleanly on followers too.
            dropComponent(componentRef[0]);
          replaceSegments(inputs, /* maybeNew */ null);
          return Boolean.TRUE;
        });
      } catch (final IOException | InterruptedException e) {
        throw new IndexException("Failed to compact sparse vector engine '" + indexName + "'", e);
      }
      if (!ranOnLeader)
        return -1L;
      if (!wroteAnything[0])
        return -1L;
      try {
        appendSegment(new PaginatedSegmentReader(componentRef[0]));
      } catch (final IOException e) {
        throw new IndexException("Failed to open freshly-compacted sparse segment '" + indexName + "_seg" + newId + "'", e);
      }
      return newId;
    } finally {
      mutatorLock.unlock();
    }
  }

  // --- introspection --------------------------------------------------------

  public long memtablePostings() {
    return memtable.get().totalPostings();
  }

  public long totalPostings() {
    long total = memtable.get().totalPostings();
    for (final PaginatedSegmentReader r : segments.get())
      total += r.totalPostings();
    return total;
  }

  public int segmentCount() {
    return segments.get().length;
  }

  public long[] segmentIds() {
    final PaginatedSegmentReader[] active = segments.get();
    final long[] out = new long[active.length];
    for (int i = 0; i < active.length; i++)
      out[i] = active[i].segmentId();
    Arrays.sort(out);
    return out;
  }

  // --- lifecycle ------------------------------------------------------------

  @Override
  public void close() {
    if (closed)
      return;
    mutatorLock.lock();
    try {
      if (closed)
        return;
      // Final flush so writes since the last flush are durable; matches the legacy engine's behaviour.
      final Memtable old = memtable.getAndSet(new Memtable());
      if (!old.isEmpty()) {
        final long segmentId = nextSegmentId.getAndIncrement();
        final SparseSegmentComponent component = createComponent(segmentId);
        database.transaction(() -> {
          try (final SparseSegmentBuilder b = new SparseSegmentBuilder(component, params)) {
            b.setSegmentId(segmentId);
            for (final int dim : old.sortedDims()) {
              final Iterator<MemtablePosting> it = old.iterateDim(dim);
              if (!it.hasNext())
                continue;
              b.startDim(dim);
              while (it.hasNext()) {
                final MemtablePosting p = it.next();
                if (p.tombstone())
                  b.appendTombstone(p.rid());
                else
                  b.appendPosting(p.rid(), p.weight());
              }
              b.endDim();
            }
            b.finish();
          }
        });
      }
      // Component lifetime is owned by FileManager; nothing else to release here.
      segments.set(new PaginatedSegmentReader[0]);
      closed = true;
    } finally {
      mutatorLock.unlock();
    }
  }

  // --- internals ------------------------------------------------------------

  private void ensureOpen() {
    if (closed)
      throw new IllegalStateException("engine is closed");
  }

  /** Component name pattern: {@code <indexName>_seg<segmentId>}. */
  private String segmentComponentName(final long segmentId) {
    return indexName + "_seg" + segmentId;
  }

  /**
   * Allocate a fresh {@link SparseSegmentComponent} for the given segment id, register it with
   * the schema's file manager, and return it. Caller is responsible for the surrounding
   * transaction (the component's pages must be allocated inside one).
   */
  private SparseSegmentComponent createComponent(final long segmentId) {
    final String name = segmentComponentName(segmentId);
    final String filePath = database.getDatabasePath() + "/" + name;
    try {
      final SparseSegmentComponent c = new SparseSegmentComponent(database, name, filePath, ComponentFile.MODE.READ_WRITE,
          params.pageSize());
      ((LocalSchema) database.getSchema().getEmbedded()).registerFile(c);
      return c;
    } catch (final IOException e) {
      throw new IndexException("Failed to allocate sparse segment component '" + name + "'", e);
    }
  }

  private void dropComponent(final SparseSegmentComponent component) {
    try {
      database.getFileManager().dropFile(component.getFileId());
    } catch (final IOException e) {
      throw new IndexException("Failed to drop sparse segment component '" + component.getName() + "'", e);
    }
  }

  /**
   * Lightweight resync of the in-memory segments snapshot against the FileManager. On a Raft
   * leader the engine's {@code segments} array is populated by {@link #appendSegment} after each
   * flush; on a follower, {@code SparseSegmentComponent} files arrive via {@code SCHEMA_ENTRY}
   * replication and are registered in the FileManager + {@link com.arcadedb.schema.LocalSchema}'s
   * {@code files} list, but no code path updates this engine's snapshot. Calling this at the
   * start of each query keeps follower visibility correct without requiring a separate
   * notification path. Cost is O(F) where F is the database's total file count - microseconds in
   * practice.
   */
  private void refreshSegmentsFromFileManager() {
    final String prefix = indexName + "_seg";
    final PaginatedSegmentReader[] current = segments.get();
    final LongHashSet knownIds = new LongHashSet(Math.max(8, current.length * 2));
    for (final PaginatedSegmentReader r : current)
      knownIds.add(r.segmentId());

    boolean changed = false;
    final List<PaginatedSegmentReader> updated = new ArrayList<>(current.length);
    for (final PaginatedSegmentReader r : current)
      updated.add(r);

    final LongHashSet seenIds = new LongHashSet(Math.max(8, current.length * 2));
    for (final PaginatedSegmentReader r : current)
      seenIds.add(r.segmentId());

    for (final var componentFile : database.getFileManager().getFiles()) {
      if (componentFile == null)
        continue;
      final var component = database.getSchema().getFileByIdIfExists(componentFile.getFileId());
      if (!(component instanceof SparseSegmentComponent ssc) || !ssc.getName().startsWith(prefix))
        continue;
      // On followers the sparseseg file briefly exists between createNewFiles and the WAL apply
      // that fills its pages, so a freshly-arrived component can fail header validation for a
      // moment. Skip it; the next query will pick it up once pages are written.
      final PaginatedSegmentReader reader;
      try {
        reader = new PaginatedSegmentReader(ssc);
      } catch (final IOException e) {
        continue;
      }
      if (knownIds.contains(reader.segmentId()))
        continue;
      updated.add(reader);
      seenIds.add(reader.segmentId());
      changed = true;
    }

    // Drop any segments that the FileManager no longer knows about (a follower may apply a
    // SCHEMA_ENTRY that retires segments via removeFiles).
    for (int i = updated.size() - 1; i >= 0; i--) {
      if (!fileManagerHasComponent(updated.get(i).component())) {
        updated.remove(i);
        changed = true;
      }
    }

    if (changed) {
      updated.sort(Comparator.comparingLong(PaginatedSegmentReader::segmentId));
      segments.set(updated.toArray(new PaginatedSegmentReader[0]));
      if (!updated.isEmpty()) {
        final long highest = updated.getLast().segmentId();
        if (nextSegmentId.get() <= highest)
          nextSegmentId.set(highest + 1L);
      }
    }
  }

  private boolean fileManagerHasComponent(final SparseSegmentComponent ssc) {
    try {
      return database.getFileManager().existsFile(ssc.getFileId());
    } catch (final RuntimeException ignored) {
      return false;
    }
  }

  /**
   * Discover existing components belonging to this index by name pattern, sort them by segment
   * id, and prime {@link #nextSegmentId} above the highest known id.
   */
  private void loadExistingSegments() {
    final String prefix = indexName + "_seg";
    final List<PaginatedSegmentReader> readers = new ArrayList<>();

    // Walk every registered file by id; sparse segment components whose name starts with our
    // prefix belong to this engine.
    for (final var componentFile : database.getFileManager().getFiles()) {
      if (componentFile == null)
        continue;
      final var component = database.getSchema().getFileByIdIfExists(componentFile.getFileId());
      if (component instanceof SparseSegmentComponent ssc && ssc.getName().startsWith(prefix)) {
        try {
          readers.add(new PaginatedSegmentReader(ssc));
        } catch (final IOException e) {
          throw new IndexException("Failed to open sparse segment component '" + ssc.getName() + "'", e);
        }
      }
    }
    readers.sort(Comparator.comparingLong(PaginatedSegmentReader::segmentId));
    if (!readers.isEmpty()) {
      segments.set(readers.toArray(new PaginatedSegmentReader[0]));
      nextSegmentId.set(readers.getLast().segmentId() + 1L);
    }
  }

  private void appendSegment(final PaginatedSegmentReader newSeg) {
    while (true) {
      final PaginatedSegmentReader[] curr = segments.get();
      final PaginatedSegmentReader[] next = Arrays.copyOf(curr, curr.length + 1);
      next[curr.length] = newSeg;
      if (segments.compareAndSet(curr, next))
        return;
    }
  }

  private void replaceSegments(final PaginatedSegmentReader[] toRemove, final PaginatedSegmentReader maybeNew) {
    final LongHashSet removeIds = new LongHashSet(Math.max(8, toRemove.length * 2));
    for (final PaginatedSegmentReader r : toRemove)
      removeIds.add(r.segmentId());

    while (true) {
      final PaginatedSegmentReader[] curr = segments.get();
      final List<PaginatedSegmentReader> next = new ArrayList<>(curr.length);
      for (final PaginatedSegmentReader r : curr) {
        if (!removeIds.contains(r.segmentId()))
          next.add(r);
      }
      if (maybeNew != null)
        next.add(maybeNew);
      if (segments.compareAndSet(curr, next.toArray(new PaginatedSegmentReader[0])))
        break;
    }

    // Drop the underlying component files (and FileManager refs) for retired segments.
    for (final PaginatedSegmentReader r : toRemove)
      dropComponent(r.component());
  }

  /** Build a merged {@link DimCursor} from the memtable and segment snapshot for one dim. */
  private DimCursor openMergedCursor(final int dim, final Memtable mt, final PaginatedSegmentReader[] segSnapshot)
      throws IOException {
    final List<SourceCursor> sources = new ArrayList<>(segSnapshot.length + 1);
    for (final PaginatedSegmentReader r : segSnapshot) {
      final PaginatedSegmentDimCursor c = r.openCursor(dim);
      if (c != null)
        sources.add(c);
    }
    if (mt != null) {
      final MemtableSourceCursor mc = new MemtableSourceCursor(mt, dim);
      mc.start();
      if (!mc.isExhausted())
        sources.add(mc);
      else
        mc.close();
    }
    if (sources.isEmpty())
      return null;
    return new DimCursor(dim, sources);
  }

  /**
   * N-way merge across {@code inputs} (oldest-first), emitting per-dim postings into {@code b}.
   * Returns {@code true} if at least one posting was emitted.
   */
  private boolean mergeIntoBuilder(final SparseSegmentBuilder b, final PaginatedSegmentReader[] inputs,
      final boolean dropAllTombstones) throws IOException {
    final IntHashSet allDimsSet = new IntHashSet();
    for (final PaginatedSegmentReader r : inputs)
      for (final int d : r.dims())
        allDimsSet.add(d);
    final int[] allDims = allDimsSet.toArray();
    Arrays.sort(allDims);

    boolean wroteAnything = false;
    for (final int dim : allDims) {
      final List<DimSource> sources = new ArrayList<>(inputs.length);
      try {
        for (int i = 0; i < inputs.length; i++) {
          final PaginatedSegmentDimCursor c = inputs[i].openCursor(dim);
          if (c == null)
            continue;
          c.start();
          if (c.isExhausted()) {
            c.close();
            continue;
          }
          sources.add(new DimSource(c, i));
        }
        if (sources.isEmpty())
          continue;

        boolean dimOpened = false;
        while (!sources.isEmpty()) {
          // Find the smallest currentRid across live sources.
          RID minRid = sources.get(0).cursor.currentRid();
          for (int i = 1; i < sources.size(); i++) {
            final RID r = sources.get(i).cursor.currentRid();
            if (SparseSegmentBuilder.compareRid(r, minRid) < 0)
              minRid = r;
          }

          // Pick the newest source aligned at minRid (newest = highest priority index).
          DimSource newest = null;
          for (final DimSource s : sources) {
            if (minRid.equals(s.cursor.currentRid())) {
              if (newest == null || s.priority > newest.priority)
                newest = s;
            }
          }

          final boolean tombstone = newest.cursor.isTombstone();
          if (tombstone && dropAllTombstones) {
            // skip
          } else {
            if (!dimOpened) {
              b.startDim(dim);
              dimOpened = true;
            }
            if (tombstone)
              b.appendTombstone(minRid);
            else
              b.appendPosting(minRid, newest.cursor.currentWeight());
          }

          // Advance every cursor aligned at minRid; drop those that exhaust.
          for (final Iterator<DimSource> it = sources.iterator(); it.hasNext(); ) {
            final DimSource s = it.next();
            if (minRid.equals(s.cursor.currentRid())) {
              if (!s.cursor.advance()) {
                s.cursor.close();
                it.remove();
              }
            }
          }
        }

        if (dimOpened) {
          b.endDim();
          wroteAnything = true;
        }
      } finally {
        for (final DimSource s : sources)
          s.cursor.close();
      }
    }
    return wroteAnything;
  }

  private static final class DimSource {
    final PaginatedSegmentDimCursor cursor;
    final int                       priority; // higher = newer

    DimSource(final PaginatedSegmentDimCursor cursor, final int priority) {
      this.cursor = cursor;
      this.priority = priority;
    }
  }
}
