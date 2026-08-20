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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;

/**
 * How much heap a vector-index graph build may ask for, and how much one is going to cost.
 * <p>
 * Both of this index's auto-sized caches used to budget themselves off <em>total</em> heap
 * ({@code Runtime.maxMemory() / 100 * percent}), which is the wrong denominator for a rebuild: the old graph and
 * its search cache stay resident for the whole build, so the heap actually available to the build is what is left
 * after them, not the whole heap. Raising {@code -Xmx} did not create headroom either, because a bigger heap grew
 * both caches proportionally and the rebuild was no more likely to fit (issue #6503).
 * <p>
 * <b>Why not {@code Runtime.freeMemory()}.</b> That reports free space in the <em>currently committed</em> heap,
 * and everything allocated since the last collection counts as used whether it is live or garbage. Read just
 * before a collection it says the heap is nearly full even when almost all of it is about to be reclaimed, so a
 * budget taken from it would collapse the build cache for no reason and make the build far slower - the opposite
 * of the problem being solved. {@link MemoryPoolMXBean#getCollectionUsage()} is the garbage-free reading: it is
 * the pool's occupancy measured immediately after the JVM last collected that pool, so it approximates live
 * retained data rather than allocation since the last GC.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class VectorHeapBudget {
  /**
   * Retained heap per graph node, measured in issue #6503 (~1.1 KB/node at 128 dimensions, across the JVector
   * adjacency structure and its per-node overhead). Deliberately a single flat constant rather than a function of
   * {@code maxConnections}: the number exists to decide whether a rebuild is going to fit at all, an
   * order-of-magnitude question, and a false precision here would read as a guarantee this cannot make.
   */
  static final long APPROX_GRAPH_BYTES_PER_NODE = 1_100L;

  /** Ordinal-to-vector-id map: one int per node, held once per graph generation. */
  private static final long ORDINAL_MAP_BYTES_PER_NODE = Integer.BYTES;

  private VectorHeapBudget() {
  }

  /**
   * @return the heap ceiling, i.e. what {@code -Xmx} allows.
   */
  static long maxHeapBytes() {
    return Runtime.getRuntime().maxMemory();
  }

  /**
   * Live (post-collection) heap occupancy, summed over every heap pool that reports it.
   *
   * @return the bytes the JVM still held right after it last collected each pool, or {@code -1} when no pool
   * reports a collection usage - a JVM that has not collected yet, or a GC that does not publish the figure. A
   * caller that gets {@code -1} must fall back to the whole heap rather than assume zero pressure.
   */
  static long liveHeapBytes() {
    long live = -1L;
    for (final MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool == null || pool.getType() != MemoryType.HEAP)
        continue;
      final MemoryUsage collectionUsage;
      try {
        collectionUsage = pool.getCollectionUsage();
      } catch (final RuntimeException e) {
        // An implementation that refuses the query is indistinguishable from one that does not publish it.
        continue;
      }
      if (collectionUsage == null)
        continue;
      if (live < 0)
        live = 0L;
      live += Math.max(0L, collectionUsage.getUsed());
    }
    return live;
  }

  /**
   * Heap a new allocation can realistically expect to get: the ceiling minus what is live after the last
   * collection. Falls back to the whole ceiling when live occupancy is unknown, which reproduces exactly the
   * total-heap budgeting this class replaces - a conservative default in the sense that it changes nothing.
   */
  static long availableHeapBytes() {
    final long max = maxHeapBytes();
    final long live = liveHeapBytes();
    if (live < 0)
      return max;
    return Math.max(0L, max - live);
  }

  /**
   * The share of {@link #availableHeapBytes()} a caller is allowed to claim.
   *
   * @param percent share to claim, clamped into {@code [0, 90]}: a build is never allowed to plan on the whole
   *                heap, since the request, I/O and GC threads need some of it too
   *
   * @return the budget in bytes, never negative
   */
  static long budgetBytes(final int percent) {
    if (percent <= 0)
      return 0L;
    return availableHeapBytes() / 100 * Math.min(percent, 90);
  }

  /**
   * Retained heap of one graph generation of {@code nodes} nodes, including its ordinal map.
   */
  static long estimateGraphBytes(final long nodes) {
    if (nodes <= 0)
      return 0L;
    return nodes * (APPROX_GRAPH_BYTES_PER_NODE + ORDINAL_MAP_BYTES_PER_NODE);
  }

  /**
   * Per-entry cost of a cached vector: the float payload plus the {@code VectorFloat} wrapper, the cache entry
   * and the array slot. Same figure the two cache-sizing paths in {@code LSMVectorIndex} use.
   */
  static long bytesPerCachedVector(final int dimensions) {
    return (long) dimensions * Float.BYTES + 64;
  }

  /**
   * Peak heap a from-scratch graph build is expected to hold at once.
   *
   * @param nodes                  number of vectors the build will walk
   * @param dimensions             vector width
   * @param buildCacheCapacity     vectors the build cache will hold
   * @param oldGraphStaysResident  whether the graph being replaced is retained for the duration - true for an
   *                               online rebuild, which must keep serving searches, false on the close path,
   *                               which releases it up front (issue #6503)
   *
   * @return the estimated peak in bytes
   */
  static long estimateRebuildHeapBytes(final long nodes, final int dimensions, final long buildCacheCapacity,
      final boolean oldGraphStaysResident) {
    long estimate = estimateGraphBytes(nodes);                              // the graph being built
    estimate += Math.max(0L, buildCacheCapacity) * bytesPerCachedVector(dimensions);
    if (oldGraphStaysResident)
      estimate += estimateGraphBytes(nodes);                                // ...on top of the one being replaced
    return estimate;
  }
}
