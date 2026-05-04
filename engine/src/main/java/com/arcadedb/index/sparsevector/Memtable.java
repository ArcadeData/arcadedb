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

  /** Approximate per-entry on-heap footprint: ConcurrentSkipListMap node header + RID + Float box + edges. */
  static final int APPROX_BYTES_PER_ENTRY = 96;

  private final ConcurrentHashMap<Integer, ConcurrentSkipListMap<RID, Float>> postings = new ConcurrentHashMap<>();
  private final AtomicLong totalPostings        = new AtomicLong();
  private final AtomicLong tombstoneCount       = new AtomicLong();
  private final AtomicLong heapBytesEstimate    = new AtomicLong();

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
  }

  /**
   * Mark {@code (dim, rid)} as deleted. Stored as a tombstone in the memtable; physical removal
   * happens when a compaction merges past the tombstone in older segments.
   */
  public void remove(final int dim, final RID rid) {
    if (rid == null)
      throw new IllegalArgumentException("rid must not be null");
    final ConcurrentSkipListMap<RID, Float> dimMap = postings.computeIfAbsent(dim, d -> new ConcurrentSkipListMap<>());
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

  /** Approximate on-heap footprint in bytes. Computed incrementally on each mutation; not exact. */
  public long heapBytesEstimate() {
    return heapBytesEstimate.get();
  }

  public boolean isEmpty() {
    return totalPostings.get() == 0L;
  }

  public int dimCount() {
    return postings.size();
  }

  /** Returns the dim_ids present, sorted ascending. Snapshot at call time. */
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
    return new DimIterator(dimMap);
  }

  /** Reset to empty (call only after the contents have been flushed). */
  public void clear() {
    postings.clear();
    totalPostings.set(0L);
    tombstoneCount.set(0L);
    heapBytesEstimate.set(0L);
  }

  // ---------- internals ----------

  private void accountChange(final Float previous, final float newValue) {
    final boolean isNewTombstone = Float.isNaN(newValue);
    if (previous == null) {
      totalPostings.incrementAndGet();
      heapBytesEstimate.addAndGet(APPROX_BYTES_PER_ENTRY);
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

    DimIterator(final ConcurrentSkipListMap<RID, Float> map) {
      this.backing = map.entrySet().iterator();
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
