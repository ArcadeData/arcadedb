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

import com.arcadedb.database.RID;
import com.arcadedb.utility.LongHashSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Orchestrator for the v2 sparse-vector storage backend. Composes the memtable, sealed
 * segment set, flush worker, and compaction worker into a single {@code put / remove / topK
 * / flush / compact} surface.
 * <p>
 * <b>Concurrency.</b> Writes ({@link #put}, {@link #remove}) are lock-free against the memtable.
 * Reads ({@link #topK}) take an atomic snapshot of the current memtable + segment set; the
 * snapshot is stable for the duration of the query even if a flush or compaction commits a new
 * publication mid-query. Flush, compaction, and engine close serialize on a single mutator lock
 * to keep the segment-set publication ordering well-defined.
 * <p>
 * <b>Durability.</b> This MVP relies on {@link #flush()} for durability. The forthcoming WAL
 * integration (Phase 3 follow-up) will make {@link #put}/{@link #remove} durable before they
 * return.
 * <p>
 * <b>File layout.</b> Each segment is stored as one {@code .sparseseg} file under the engine's
 * directory, named {@code seg-NNNNNN.sparseseg} where {@code NNNNNN} is the segment id padded
 * to 6 digits.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class SparseVectorEngine implements AutoCloseable {

  private static final String SEGMENT_FILE_PREFIX = "seg-";
  private static final String SEGMENT_FILE_SUFFIX = ".sparseseg";

  private final Path              directory;
  private final SegmentParameters params;

  private final AtomicReference<Memtable>               memtable      = new AtomicReference<>(new Memtable());
  private final AtomicReference<SparseSegmentReader[]>  segments      = new AtomicReference<>(new SparseSegmentReader[0]);
  private final AtomicLong                              nextSegmentId = new AtomicLong(1L);
  private final ReentrantLock                           mutatorLock   = new ReentrantLock();

  private volatile boolean closed;

  /** Open or create an engine at {@code directory}. Existing {@code .sparseseg} files are loaded. */
  public SparseVectorEngine(final Path directory, final SegmentParameters params) throws IOException {
    this.directory = directory;
    this.params = params;
    Files.createDirectories(directory);
    loadExistingSegments();
  }

  // ---------- writes ----------

  public void put(final int dim, final RID rid, final float weight) {
    ensureOpen();
    memtable.get().put(dim, rid, weight);
  }

  public void remove(final int dim, final RID rid) {
    ensureOpen();
    memtable.get().remove(dim, rid);
  }

  // ---------- reads ----------

  public List<RidScore> topK(final int[] queryDims, final float[] queryWeights, final int k) throws IOException {
    ensureOpen();
    if (k <= 0)
      return List.of();
    if (queryDims.length != queryWeights.length)
      throw new IllegalArgumentException("queryDims and queryWeights must have equal length");

    // Snapshot memtable and segments; queries operate on a stable view even if flush/compact races.
    final Memtable mtSnapshot = memtable.get();
    final SparseSegmentReader[] segSnapshot = segments.get();

    final DimCursor[] cursors = new DimCursor[queryDims.length];
    try {
      for (int i = 0; i < queryDims.length; i++)
        cursors[i] = DimCursor.open(queryDims[i], mtSnapshot, segSnapshot);
      return BmwScorer.topK(queryDims, queryWeights, cursors, k);
    } finally {
      for (final DimCursor c : cursors)
        if (c != null)
          c.close();
    }
  }

  // ---------- maintenance ----------

  /**
   * Flush the current memtable to a new sealed segment. Returns the segment id of the new file,
   * or {@code -1L} if the memtable was empty.
   */
  public long flush() throws IOException {
    ensureOpen();
    mutatorLock.lock();
    try {
      final Memtable old = memtable.getAndSet(new Memtable());
      if (old.isEmpty())
        return -1L;
      final long segmentId = nextSegmentId.getAndIncrement();
      final Path file = segmentFile(segmentId);
      final SparseSegmentReader newSeg = FlushWorker.flush(old, file, segmentId, params);
      if (newSeg == null)
        return -1L;
      appendSegment(newSeg);
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
  public long compactAll() throws IOException {
    ensureOpen();
    mutatorLock.lock();
    try {
      final SparseSegmentReader[] active = segments.get();
      if (active.length < 2)
        return -1L;
      // Sort defensively in case appendSegment ordering ever diverges from segment-id order.
      final SparseSegmentReader[] inputs = Arrays.copyOf(active, active.length);
      Arrays.sort(inputs, Comparator.comparingLong(SparseSegmentReader::segmentId));

      final long newId = nextSegmentId.getAndIncrement();
      final Path file = segmentFile(newId);
      final SparseSegmentReader merged = CompactionWorker.compact(inputs, file, newId, params, true);
      replaceSegments(inputs, merged);
      return merged == null ? -1L : newId;
    } finally {
      mutatorLock.unlock();
    }
  }

  /**
   * Compact the {@code count} oldest segments into one. Useful for size-tiered policies; returns
   * the new segment id or {@code -1L} if fewer than two segments are eligible.
   */
  public long compactOldest(final int count) throws IOException {
    ensureOpen();
    if (count < 2)
      return -1L;
    mutatorLock.lock();
    try {
      final SparseSegmentReader[] active = segments.get();
      if (active.length < count)
        return -1L;
      final SparseSegmentReader[] sortedAll = Arrays.copyOf(active, active.length);
      Arrays.sort(sortedAll, Comparator.comparingLong(SparseSegmentReader::segmentId));
      final SparseSegmentReader[] inputs = Arrays.copyOf(sortedAll, count);

      final long newId = nextSegmentId.getAndIncrement();
      final Path file = segmentFile(newId);
      final SparseSegmentReader merged = CompactionWorker.compact(inputs, file, newId, params, false);
      replaceSegments(inputs, merged);
      return merged == null ? -1L : newId;
    } finally {
      mutatorLock.unlock();
    }
  }

  // ---------- introspection ----------

  public long memtablePostings() {
    return memtable.get().totalPostings();
  }

  /**
   * Sum of postings across the memtable and every active segment. Includes tombstones since they
   * still occupy storage and are scanned by queries until compaction merges them away.
   */
  public long totalPostings() {
    long total = memtable.get().totalPostings();
    for (final SparseSegmentReader r : segments.get())
      total += r.totalPostings();
    return total;
  }

  /**
   * Number of distinct live (non-tombstone) postings under one dim across the memtable + active
   * segments, after newest-source-wins merging. Used to compute IDF document frequency and as
   * an introspection helper. Cost is O(df) for that dim.
   */
  public long countDim(final int dim) throws IOException {
    final DimCursor c = DimCursor.open(dim, memtable.get(), segments.get());
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

  public int segmentCount() {
    return segments.get().length;
  }

  public long[] segmentIds() {
    final SparseSegmentReader[] active = segments.get();
    final long[] out = new long[active.length];
    for (int i = 0; i < active.length; i++)
      out[i] = active[i].segmentId();
    Arrays.sort(out);
    return out;
  }

  public Path directory() {
    return directory;
  }

  // ---------- lifecycle ----------

  @Override
  public void close() throws IOException {
    if (closed)
      return;
    mutatorLock.lock();
    try {
      if (closed)
        return;
      // Final flush so writes since the last flush are durable.
      final Memtable old = memtable.getAndSet(new Memtable());
      if (!old.isEmpty()) {
        final long segmentId = nextSegmentId.getAndIncrement();
        final Path file = segmentFile(segmentId);
        final SparseSegmentReader newSeg = FlushWorker.flush(old, file, segmentId, params);
        if (newSeg != null)
          appendSegment(newSeg);
      }
      for (final SparseSegmentReader r : segments.get())
        r.close();
      segments.set(new SparseSegmentReader[0]);
      closed = true;
    } finally {
      mutatorLock.unlock();
    }
  }

  // ---------- internals ----------

  private void ensureOpen() {
    if (closed)
      throw new IllegalStateException("engine is closed");
  }

  private void loadExistingSegments() throws IOException {
    if (!Files.isDirectory(directory))
      return;
    final List<SparseSegmentReader> readers = new ArrayList<>();
    try (final var stream = Files.list(directory)) {
      stream.filter(p -> {
        final String name = p.getFileName().toString();
        return name.startsWith(SEGMENT_FILE_PREFIX) && name.endsWith(SEGMENT_FILE_SUFFIX);
      }).forEach(p -> {
        try {
          readers.add(new SparseSegmentReader(p));
        } catch (final IOException e) {
          throw new RuntimeException("failed to open existing segment " + p, e);
        }
      });
    }
    readers.sort(Comparator.comparingLong(SparseSegmentReader::segmentId));
    if (!readers.isEmpty()) {
      segments.set(readers.toArray(new SparseSegmentReader[0]));
      nextSegmentId.set(readers.getLast().segmentId() + 1L);
    }
  }

  private Path segmentFile(final long segmentId) {
    return directory.resolve(String.format("%s%06d%s", SEGMENT_FILE_PREFIX, segmentId, SEGMENT_FILE_SUFFIX));
  }

  private void appendSegment(final SparseSegmentReader newSeg) {
    while (true) {
      final SparseSegmentReader[] curr = segments.get();
      final SparseSegmentReader[] next = Arrays.copyOf(curr, curr.length + 1);
      next[curr.length] = newSeg;
      if (segments.compareAndSet(curr, next))
        return;
    }
  }

  private void replaceSegments(final SparseSegmentReader[] toRemove, final SparseSegmentReader maybeNew) throws IOException {
    // LongHashSet keeps the set primitive: no Long boxing on every contains() call inside the
    // segment-filter loop, no Long.MIN_VALUE collision concerns since segment ids start at 1.
    final LongHashSet removeIds = new LongHashSet(Math.max(8, toRemove.length * 2));
    for (final SparseSegmentReader r : toRemove)
      removeIds.add(r.segmentId());

    while (true) {
      final SparseSegmentReader[] curr = segments.get();
      final List<SparseSegmentReader> next = new ArrayList<>(curr.length);
      for (final SparseSegmentReader r : curr) {
        if (!removeIds.contains(r.segmentId()))
          next.add(r);
      }
      if (maybeNew != null)
        next.add(maybeNew);
      if (segments.compareAndSet(curr, next.toArray(new SparseSegmentReader[0])))
        break;
    }

    // Close the readers and unlink the files for the segments we just shadowed.
    for (final SparseSegmentReader r : toRemove) {
      final Path p = r.file();
      r.close();
      Files.deleteIfExists(p);
    }
  }
}
