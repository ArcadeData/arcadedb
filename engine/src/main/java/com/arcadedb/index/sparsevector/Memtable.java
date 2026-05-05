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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory mutable posting store, sorted by (dim, RID). Backs the write path of
 * {@code LSM_SPARSE_VECTOR} between flushes; periodically swapped out and serialized to a
 * sealed {@code .sparseseg} file by the flush worker.
 * <p>
 * <b>Concurrency.</b> {@link #put} and {@link #remove} are lock-free: per-dim postings live in a
 * {@link ConcurrentSkipListMap} whose {@code put} is atomic, and the per-dim map is created
 * lazily via {@link ConcurrentHashMap#computeIfAbsent}. Iteration ({@link #iterateDim} and
 * {@link #sortedDims}) operates on weakly-consistent snapshots and is safe to invoke
 * concurrently with writers, but the iterator may observe interleaved updates - the flush worker
 * holds the segment-set publishing lock to ensure flushed segments shadow any racing memtable
 * mutations.
 * <p>
 * <b>Tombstones.</b> A {@code remove(dim, rid)} writes a sentinel float ({@link Float#NaN}) at
 * the same key. Iteration surfaces tombstones to downstream consumers (compactor, segment writer)
 * which decide whether to physically drop the entry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class Memtable {

  private final ConcurrentHashMap<Integer, ConcurrentSkipListMap<RID, Float>> postings    = new ConcurrentHashMap<>();
  // Per-dim running max live weight. Maintained incrementally on every {@link #put} so the BMW
  // upper bound a cursor reports does not require a fresh full scan of the dim's postings at
  // construction time. Monotonically non-decreasing - a tombstone or an overwrite to a smaller
  // weight is a safe over-estimate (BMW pruning correctness only requires an upper bound; a
  // looser bound just means slightly less skipping). This trades a per-put map.merge() for the
  // dropped O(n) scan in {@link MemtableSourceCursor}.
  private final ConcurrentHashMap<Integer, Float>                              dimMaxWeight = new ConcurrentHashMap<>();
  private final AtomicLong totalPostings        = new AtomicLong();
  private final AtomicLong tombstoneCount       = new AtomicLong();

  /** Insert or overwrite the weight for {@code (dim, rid)}. Negative or non-finite weights are rejected. */
  public void put(final int dim, final RID rid, final float weight) {
    if (Float.isNaN(weight) || Float.isInfinite(weight))
      throw new IllegalArgumentException("weight must be a finite number: " + weight);
    if (weight < 0.0f)
      throw new IllegalArgumentException("weight must be non-negative: " + weight);
    if (rid == null)
      throw new IllegalArgumentException("rid must not be null");

    final ConcurrentSkipListMap<RID, Float> dimMap = postings.computeIfAbsent(dim, d -> new ConcurrentSkipListMap<>());
    final Float prev = dimMap.put(rid, weight);
    accountChange(prev, weight);
    // Track the running per-dim max so {@link #dimMaxWeight(int)} can answer in O(1).
    dimMaxWeight.merge(dim, weight, Math::max);
  }

  /**
   * Mark {@code (dim, rid)} as deleted. Stored as a tombstone in the memtable; physical removal
   * happens when a compaction merges past the tombstone in older segments.
   * <p>
   * Common case is a delete that targets a dim already populated by an earlier {@code put} -
   * those calls go through the cheap {@link ConcurrentHashMap#get} path. Only the rare delete
   * for a dim whose live posting was already flushed to a sealed segment falls through to
   * {@code computeIfAbsent}: we must still record the tombstone in the memtable so the
   * newest-wins merge in {@link DimCursor} masks the segment-side posting.
   */
  public void remove(final int dim, final RID rid) {
    if (rid == null)
      throw new IllegalArgumentException("rid must not be null");
    ConcurrentSkipListMap<RID, Float> dimMap = postings.get(dim);
    if (dimMap == null)
      dimMap = postings.computeIfAbsent(dim, d -> new ConcurrentSkipListMap<>());
    final Float prev = dimMap.put(rid, Float.NaN);
    accountChange(prev, Float.NaN);
  }

  /** Total number of entries (live + tombstoned). */
  public long totalPostings() {
    return totalPostings.get();
  }

  public long tombstoneCount() {
    return tombstoneCount.get();
  }

  public boolean isEmpty() {
    return totalPostings.get() == 0L;
  }

  public int dimCount() {
    return postings.size();
  }

  /**
   * Returns {@code true} if this memtable has at least one entry (live or tombstone) under
   * {@code dim}. Used by {@link PaginatedSparseVectorEngine#openMergedCursor} to avoid handing
   * a {@link DimCursor} a memtable source that's guaranteed to be empty - a cursor with zero
   * matching postings is correctness-safe (it just exhausts immediately) but it still costs a
   * scan in {@link DimCursor#materializeMin} on every advance, so dropping it is pure win.
   * <p>
   * The check has to include tombstones, not just live postings: a tombstone in the memtable
   * for {@code (dim, rid)} masks an older segment's live posting under the newest-wins merge
   * rule, so suppressing the source when only tombstones exist would silently surface deleted
   * documents.
   */
  public boolean containsDim(final int dim) {
    return postings.containsKey(dim);
  }

  /**
   * Running per-dim max live weight, maintained incrementally on {@link #put}. A monotonic
   * upper bound suitable for BMW pruning - it never drops when a posting is tombstoned or
   * overwritten with a smaller weight, but BMW only needs an upper bound for correctness so
   * over-estimation just costs a bit of skip aggressiveness, not correctness. Returns {@code 0}
   * for a dim that was never written to (which is also the natural lower bound).
   */
  public float dimMaxWeight(final int dim) {
    final Float f = dimMaxWeight.get(dim);
    return f == null ? 0.0f : f;
  }

  /**
   * Returns the dim_ids present, sorted ascending. Weakly-consistent snapshot.
   * <p>
   * <b>Concurrent dims may be silently truncated.</b> The returned array is sized at call time
   * from {@code postings.size()}; iteration walks the underlying {@link ConcurrentHashMap} key
   * set. Dims added <i>after</i> the size sample but visible during iteration are dropped to
   * avoid an array-overflow throw, and dims added after iteration completes are missed
   * altogether. Both are safe by design for the only caller, the flush worker: a flush takes a
   * snapshot of the current memtable instance and then races a fresh empty instance for new
   * writes, so any dim missed here is captured by the next flush. Direct callers outside the
   * flush worker must accept that the returned set is not a strict point-in-time view of the
   * memtable's current state.
   */
  public int[] sortedDims() {
    final int[] out = new int[postings.size()];
    int i = 0;
    for (final Integer d : postings.keySet()) {
      if (i >= out.length)
        break;  // ConcurrentHashMap snapshot grew during iteration; ignore extras.
      out[i++] = d;
    }
    final int[] trimmed = (i == out.length) ? out : Arrays.copyOf(out, i);
    Arrays.sort(trimmed);
    return trimmed;
  }

  /**
   * Per-dim posting iterator in {@code RID} ascending order. Returns an empty iterator if the dim
   * is absent. The iterator is weakly consistent: it sees updates that committed before its
   * creation and may or may not see updates that commit afterward.
   */
  public Iterator<MemtablePosting> iterateDim(final int dim) {
    final ConcurrentSkipListMap<RID, Float> dimMap = postings.get(dim);
    if (dimMap == null)
      return java.util.Collections.emptyIterator();
    return new DimIterator(dimMap.entrySet().iterator());
  }

  /**
   * Per-dim posting iterator starting at the first posting whose {@code RID} is &gt;= {@code from}.
   * Lets callers (notably {@link MemtableSourceCursor#seekTo}) jump ahead in O(log n) instead of
   * advancing the cursor one entry at a time, which is the bottleneck for BMW block-skip on a
   * large memtable. Returns an empty iterator if the dim is absent.
   */
  public Iterator<MemtablePosting> iterateDimFrom(final int dim, final RID from) {
    final ConcurrentSkipListMap<RID, Float> dimMap = postings.get(dim);
    if (dimMap == null)
      return java.util.Collections.emptyIterator();
    return new DimIterator(dimMap.tailMap(from, /* inclusive */ true).entrySet().iterator());
  }

  /** Reset to empty (call only after the contents have been flushed). */
  public void clear() {
    postings.clear();
    dimMaxWeight.clear();
    totalPostings.set(0L);
    tombstoneCount.set(0L);
  }

  // ---------- internals ----------

  private void accountChange(final Float previous, final float newValue) {
    final boolean isNewTombstone = Float.isNaN(newValue);
    if (previous == null) {
      totalPostings.incrementAndGet();
      if (isNewTombstone)
        tombstoneCount.incrementAndGet();
    } else {
      final boolean wasTombstone = Float.isNaN(previous);
      if (wasTombstone && !isNewTombstone)
        tombstoneCount.decrementAndGet();
      else if (!wasTombstone && isNewTombstone)
        tombstoneCount.incrementAndGet();
    }
  }

  private static final class DimIterator implements Iterator<MemtablePosting> {
    private final Iterator<Map.Entry<RID, Float>> backing;

    DimIterator(final Iterator<Map.Entry<RID, Float>> backing) {
      this.backing = backing;
    }

    @Override
    public boolean hasNext() {
      return backing.hasNext();
    }

    @Override
    public MemtablePosting next() {
      if (!hasNext())
        throw new NoSuchElementException();
      final Map.Entry<RID, Float> e = backing.next();
      final float v = e.getValue();
      final boolean tombstone = Float.isNaN(v);
      return new MemtablePosting(e.getKey(), tombstone ? 0.0f : v, tombstone);
    }
  }
}
