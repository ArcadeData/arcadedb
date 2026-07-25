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
package com.arcadedb.index.vector;

import io.github.jbellis.jvector.vector.types.VectorFloat;

import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * Fixed-capacity, lock-free cache of vectors keyed by the monotonic vector id assigned by
 * {@link VectorLocationIndex}. Used on the hottest path of the whole vector engine: every distance
 * evaluation of a beam search (and of a graph build) resolves its operand through here.
 * <p>
 * It is a 2-way set-associative cache backed by a single flat array, which buys three things over the
 * {@code ConcurrentHashMap<Integer, VectorFloat<?>>} it replaces (issue #5412):
 * <ul>
 * <li>no {@link Integer} boxing on lookup - the key is compared as a primitive {@code int}. Profiling a
 * 10M-vector workload attributed ~16% of CPU to {@code ConcurrentHashMap.get} plus {@code Integer.equals}
 * alone;</li>
 * <li>a hard bound on the retained footprint: capacity is allocated once and entries are evicted on
 * collision, instead of the previous "stop inserting when full" policy that froze the cache on whatever
 * happened to be loaded first;</li>
 * <li>one indirection per probe, so a hit costs two array reads at most.</li>
 * </ul>
 * Vector ids are dense and monotonic, so the identity mapping {@code id -&gt; bucket} spreads them
 * perfectly: with a capacity at least as large as the id range every vector stays resident with no
 * collisions at all. Hashing them would only add collisions.
 * <p>
 * Thread-safe. Entries are immutable and published through an {@link AtomicReferenceArray}, so a
 * concurrent {@code put} can only ever cost a later miss, never a mismatched vector.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorCache {
  /** Entries per bucket. Two ways keep the probe cost at 2 reads while absorbing collisions. */
  private static final int WAYS = 2;

  private record Entry(int vectorId, VectorFloat<?> vector) {
  }

  private final AtomicReferenceArray<Entry> slots;
  private final int                         bucketMask;
  private final int                         capacity;
  private final LongAdder                   hits   = new LongAdder();
  private final LongAdder                   misses = new LongAdder();

  /**
   * @param requestedCapacity minimum number of vectors to hold. Rounded up so the bucket count is a power of two.
   */
  public VectorCache(final int requestedCapacity) {
    final int buckets = tableSizeFor(Math.max(1, (requestedCapacity + WAYS - 1) / WAYS));
    this.bucketMask = buckets - 1;
    this.capacity = buckets * WAYS;
    this.slots = new AtomicReferenceArray<>(this.capacity);
  }

  /**
   * @return the cached vector, or {@code null} on a miss.
   */
  public VectorFloat<?> get(final int vectorId) {
    if (vectorId < 0) {
      misses.increment();
      return null;
    }

    final int base = (vectorId & bucketMask) * WAYS;

    final Entry e0 = slots.get(base);
    if (e0 != null && e0.vectorId == vectorId) {
      hits.increment();
      return e0.vector;
    }

    final Entry e1 = slots.get(base + 1);
    if (e1 != null && e1.vectorId == vectorId) {
      hits.increment();
      return e1.vector;
    }

    misses.increment();
    return null;
  }

  /**
   * Inserts the vector, evicting the least-recently-inserted entry of its bucket when both ways are taken.
   */
  public void put(final int vectorId, final VectorFloat<?> vector) {
    if (vectorId < 0 || vector == null)
      return;

    final int base = (vectorId & bucketMask) * WAYS;

    final Entry e0 = slots.get(base);
    if (e0 != null && e0.vectorId == vectorId)
      return;

    if (e0 == null) {
      slots.set(base, new Entry(vectorId, vector));
      return;
    }

    final Entry e1 = slots.get(base + 1);
    if (e1 != null && e1.vectorId == vectorId)
      return;

    // Demote the incumbent to the second way and take the first: newest entry is always probed first
    slots.set(base + 1, e0);
    slots.set(base, new Entry(vectorId, vector));
  }

  /**
   * Drops the entry for the given vector id, if resident. Called when a vector is deleted so the cache does
   * not pin vectors that no longer exist.
   */
  public void remove(final int vectorId) {
    if (vectorId < 0)
      return;

    final int base = (vectorId & bucketMask) * WAYS;
    for (int i = 0; i < WAYS; i++) {
      final Entry e = slots.get(base + i);
      if (e != null && e.vectorId == vectorId)
        slots.set(base + i, null);
    }
  }

  public void clear() {
    for (int i = 0; i < capacity; i++)
      slots.set(i, null);
  }

  public int capacity() {
    return capacity;
  }

  public long getHits() {
    return hits.sum();
  }

  public long getMisses() {
    return misses.sum();
  }

  /**
   * @return the number of resident entries. O(capacity): for diagnostics and tests only, never call it on a hot path.
   */
  public int size() {
    int count = 0;
    for (int i = 0; i < capacity; i++)
      if (slots.get(i) != null)
        ++count;
    return count;
  }

  /**
   * @return the smallest power of two greater than or equal to {@code size}, clamped so that
   * {@code buckets * WAYS} can never overflow an int.
   */
  private static int tableSizeFor(final int size) {
    if (size >= 1 << 29)
      return 1 << 29;
    if (size <= 1)
      return 1;
    return Integer.highestOneBit(size - 1) << 1;
  }
}
