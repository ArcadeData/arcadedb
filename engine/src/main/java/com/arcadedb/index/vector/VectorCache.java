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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicReferenceArray;

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
 * <p>
 * The hit/miss counters are striped per thread and updated without atomics; see {@link #count} for why, and for
 * what that costs in accuracy.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorCache {
  /** Entries per bucket. Two ways keep the probe cost at 2 reads while absorbing collisions. */
  private static final int WAYS = 2;

  /** Longs per counter stripe. Eight of them fill a 64-byte cache line, so no two stripes share one. */
  private static final int STRIPE_LONGS = 8;
  /** Offsets of the two counters inside a stripe. Both are written by the same thread, so sharing a line is free. */
  private static final int HITS         = 0;
  private static final int MISSES       = 1;
  /** One stripe per core, rounded up to a power of two, with a floor so a small box still spreads its threads. */
  private static final int STRIPES      = tableSizeFor(Math.max(4, Runtime.getRuntime().availableProcessors()));
  private static final int STRIPE_MASK  = STRIPES - 1;

  private static final VarHandle COUNTERS = MethodHandles.arrayElementVarHandle(long[].class);

  private record Entry(int vectorId, VectorFloat<?> vector) {
  }

  private final AtomicReferenceArray<Entry> slots;
  private final int                         bucketMask;
  private final int                         capacity;
  private final long[]                      counters = new long[STRIPES * STRIPE_LONGS];

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
      count(MISSES);
      return null;
    }

    final int base = (vectorId & bucketMask) * WAYS;

    final Entry e0 = slots.get(base);
    if (e0 != null && e0.vectorId == vectorId) {
      count(HITS);
      return e0.vector;
    }

    final Entry e1 = slots.get(base + 1);
    if (e1 != null && e1.vectorId == vectorId) {
      count(HITS);
      return e1.vector;
    }

    count(MISSES);
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

  /**
   * @return the number of lookups served from the cache. Approximate under concurrency, see {@link #count}.
   */
  public long getHits() {
    return sum(HITS);
  }

  /**
   * @return the number of lookups that had to materialize the vector. Approximate under concurrency, see {@link #count}.
   */
  public long getMisses() {
    return sum(MISSES);
  }

  /**
   * Zeroes both counters. For tests and for an operator who wants a hit ratio over a window rather than over the
   * lifetime of the index.
   */
  public void resetStats() {
    for (int i = 0; i < counters.length; i++)
      COUNTERS.setOpaque(counters, i, 0L);
  }

  /**
   * Adds one to the calling thread's stripe of the given counter, with plain reads and writes.
   * <p>
   * These counters used to be two {@link java.util.concurrent.atomic.LongAdder}s. {@code LongAdder} is the right
   * structure for a contended counter, but this is the wrong place for <i>any</i> atomic counter: one lookup here
   * backs one distance evaluation, so the locked compare-and-swap that {@code LongAdder} performs per increment
   * costs the same order of magnitude as the SIMD distance the lookup exists to feed. A profile of a 10M-vector
   * DEEP graph build attributed <b>26.4% of the entire build</b> to {@code LongAdder.add}, every sample of it
   * reached through {@link #get} (issue #5577).
   * <p>
   * Striping one pair of counters per thread onto its own cache line turns each increment into a read and a write
   * of a line this thread already holds exclusively: no bus lock, no cache-line ping-pong between build workers,
   * and no growth in cost as the build pool widens. Opaque access keeps each 64-bit read and write indivisible -
   * a reader can never observe a torn value - without asking for any ordering or fencing, so on x86 and AArch64 it
   * compiles to the plain load and store.
   * <p>
   * <b>The totals are therefore approximate.</b> Two threads whose ids land on the same stripe can lose an
   * increment between the read and the write, so a count can only ever come out low, never high. That is the right
   * trade for a diagnostic counter whose only consumers are {@code getStats()} and operator dashboards, and it is
   * not a new weakening: {@code LongAdder.sum()} was never an atomic snapshot either. A dashboard deriving a hit
   * <i>ratio</i> is unaffected; one asserting an exact hit <i>count</i> was relying on something this class no
   * longer promises.
   * <p>
   * One stripe per thread is best-effort everywhere, never guaranteed. Thread ids are JVM-global and monotonic
   * over every thread the process has ever created, so even a build pool with fewer workers than {@code STRIPES}
   * can have two of them collide modulo the mask - the ids are allocated together, which tends to spread them, but
   * nothing aligns them to a stripe boundary. Off the build path it is looser still, since {@link #get} is then
   * called from arbitrary request threads. All a collision costs is a few more lost increments on an
   * already-approximate counter.
   */
  private void count(final int counter) {
    final int i = (((int) Thread.currentThread().threadId()) & STRIPE_MASK) * STRIPE_LONGS + counter;
    COUNTERS.setOpaque(counters, i, (long) COUNTERS.getOpaque(counters, i) + 1L);
  }

  private long sum(final int counter) {
    long total = 0;
    for (int i = counter; i < counters.length; i += STRIPE_LONGS)
      total += (long) COUNTERS.getOpaque(counters, i);
    return total;
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
