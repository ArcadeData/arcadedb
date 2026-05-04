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

  private final DatabaseInternal  database;
  private final String            indexName;
  private final SegmentParameters params;

  private final AtomicReference<Memtable>                  memtable     = new AtomicReference<>(new Memtable());
  private final AtomicReference<PaginatedSegmentReader[]>  segments     = new AtomicReference<>(new PaginatedSegmentReader[0]);
  private final AtomicLong                                 nextSegmentId = new AtomicLong(1L);
  private final ReentrantLock                              mutatorLock  = new ReentrantLock();

  private volatile boolean closed;

  public PaginatedSparseVectorEngine(final DatabaseInternal database, final String indexName, final SegmentParameters params) {
    this.database = database;
    this.indexName = indexName;
    this.params = params;
    loadExistingSegments();
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
   * {@code -1L} if the memtable was empty. The flush runs inside a transaction so the page WAL
   * records every page of the new segment as part of the regular commit record.
   */
  public long flush() {
    ensureOpen();
    mutatorLock.lock();
    try {
      final Memtable old = memtable.getAndSet(new Memtable());
      if (old.isEmpty())
        return -1L;
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
      try {
        appendSegment(new PaginatedSegmentReader(component));
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
   * {@code -1L} if there is nothing to compact (zero or one segment, or the merge produced an
   * empty result).
   */
  public long compactAll() {
    ensureOpen();
    mutatorLock.lock();
    try {
      final PaginatedSegmentReader[] active = segments.get();
      if (active.length < 2)
        return -1L;
      final PaginatedSegmentReader[] inputs = Arrays.copyOf(active, active.length);
      Arrays.sort(inputs, Comparator.comparingLong(PaginatedSegmentReader::segmentId));

      final long newId = nextSegmentId.getAndIncrement();
      final SparseSegmentComponent newComponent = createComponent(newId);
      final boolean[] wroteAnything = { false };
      database.transaction(() -> {
        try (final SparseSegmentBuilder b = new SparseSegmentBuilder(newComponent, params)) {
          b.setSegmentId(newId);
          final long[] parentIds = new long[inputs.length];
          for (int i = 0; i < inputs.length; i++)
            parentIds[i] = inputs[i].segmentId();
          b.setParentSegments(parentIds);
          try {
            wroteAnything[0] = mergeIntoBuilder(b, inputs, true);
          } catch (final IOException e) {
            throw new IndexException("Failed to merge sparse segments during compaction", e);
          }
          b.finish();
        }
      });
      if (!wroteAnything[0]) {
        // The merged segment had no live postings (everything was tombstoned). Drop the empty
        // component we just allocated and retire the inputs in a follow-up swap.
        dropComponent(newComponent);
        replaceSegments(inputs, null);
        return -1L;
      }
      try {
        replaceSegments(inputs, new PaginatedSegmentReader(newComponent));
      } catch (final IOException e) {
        throw new IndexException("Failed to open freshly-compacted sparse segment '" + indexName + "_seg" + newId + "'", e);
      }
      return newId;
    } finally {
      mutatorLock.unlock();
    }
  }

  /** Compact the {@code count} oldest segments into one. */
  public long compactOldest(final int count) {
    ensureOpen();
    if (count < 2)
      return -1L;
    mutatorLock.lock();
    try {
      final PaginatedSegmentReader[] active = segments.get();
      if (active.length < count)
        return -1L;
      final PaginatedSegmentReader[] sortedAll = Arrays.copyOf(active, active.length);
      Arrays.sort(sortedAll, Comparator.comparingLong(PaginatedSegmentReader::segmentId));
      final PaginatedSegmentReader[] inputs = Arrays.copyOf(sortedAll, count);

      final long newId = nextSegmentId.getAndIncrement();
      final SparseSegmentComponent newComponent = createComponent(newId);
      final boolean[] wroteAnything = { false };
      database.transaction(() -> {
        try (final SparseSegmentBuilder b = new SparseSegmentBuilder(newComponent, params)) {
          b.setSegmentId(newId);
          final long[] parentIds = new long[inputs.length];
          for (int i = 0; i < inputs.length; i++)
            parentIds[i] = inputs[i].segmentId();
          b.setParentSegments(parentIds);
          try {
            wroteAnything[0] = mergeIntoBuilder(b, inputs, false);
          } catch (final IOException e) {
            throw new IndexException("Failed to merge sparse segments during compaction", e);
          }
          b.finish();
        }
      });
      if (!wroteAnything[0]) {
        dropComponent(newComponent);
        replaceSegments(inputs, null);
        return -1L;
      }
      try {
        replaceSegments(inputs, new PaginatedSegmentReader(newComponent));
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
