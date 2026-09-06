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

import com.arcadedb.log.LogManager;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * How much heap a vector-index graph build may ask for, and how much one is going to cost.
 * <p>
 * Both of this index's auto-sized caches used to budget themselves off <em>total</em> heap
 * ({@code Runtime.maxMemory() / 100 * percent}), which is the wrong denominator for a rebuild: the old graph and
 * its search cache stay resident for the whole build, so the heap actually available to the build is what is left
 * after them, not the whole heap. Raising {@code -Xmx} did not create headroom either, because a bigger heap grew
 * both caches proportionally and the rebuild was no more likely to fit (issue #6503).
 * <p>
 * The graph-build cache is the exception to "percent of leftover". A served ingest that already holds the
 * corpus in this JVM would otherwise get a third of the cache an embedded build of the same corpus would get
 * (issue #7146). {@link #buildCacheBudgetBytes(int)} takes the percent of the ceiling and caps it at 90% of
 * leftover, so the two deployments agree until an online rebuild is actually holding the old graph.
 * <p>
 * <b>Why not {@code Runtime.freeMemory()}.</b> That reports free space in the <em>currently committed</em> heap,
 * and everything allocated since the last collection counts as used whether it is live or garbage. Read just
 * before a collection it says the heap is nearly full even when almost all of it is about to be reclaimed, so a
 * budget taken from it would collapse the build cache for no reason and make the build far slower - the opposite
 * of the problem being solved. {@link MemoryPoolMXBean#getCollectionUsage()} is the garbage-free reading: it is
 * the pool's occupancy measured immediately after the JVM last collected that pool, so it approximates live
 * retained data rather than allocation since the last GC.
 * <p>
 * <b>Two ways that reading can be absent or stale, both of which fail safe.</b> A JVM that has not collected yet,
 * or a collector/configuration that does not publish a collection usage at all, makes {@link #liveHeapBytes()}
 * answer {@code -1}; the budget then falls back to the whole ceiling, which is precisely the total-heap behaviour
 * this class replaces - so the fallback changes nothing rather than degrading anything. It does mean the
 * improvement is inactive there, which would otherwise be invisible, so it is reported once per JVM at FINE.
 * And for a generational collector whose old-gen collections are infrequent, the figure reflects the last such
 * collection rather than current occupancy. That errs toward reporting LESS headroom than really exists, so the
 * failure direction is a cache sized smaller or a rebuild deferred - never a rebuild admitted that should not
 * have been.
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

  /** So the fallback above is reported once per JVM rather than on every cache-sizing call. */
  private static final AtomicBoolean COLLECTION_USAGE_UNAVAILABLE_REPORTED = new AtomicBoolean();

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
    if (live < 0 && COLLECTION_USAGE_UNAVAILABLE_REPORTED.compareAndSet(false, true))
      LogManager.instance().log(VectorHeapBudget.class, Level.FINE,
          "No heap pool on this JVM publishes a collection usage, so vector index heap budgets fall back to the "
              + "total heap. That is the pre-issue-#6503 behaviour and is safe, but a rebuild holding the old "
              + "graph resident will not ask for a smaller cache on account of it");
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
   * How much of the reclaimable page cache has to be given up for an allocation of {@code bytes} to fit inside
   * {@code percent} of available heap (issue #7184).
   * <p>
   * {@link #liveHeapBytes()} counts ArcadeDB's own page read cache as live, because from the JVM's point of view it
   * is: those pages are strongly referenced and no collection will take them. But they are <em>evictable</em> - the
   * engine can drop any of them and read the page back from disk - so judging a rebuild against a reading that
   * treats them as permanently occupied is what refused a rebuild that a 24 GB heap could have run, at every
   * attempt, leaving the delta scan those pending vectors produce to grow without limit. Issue #7147 applied the
   * same correction to the build cache's own sizing; this is the admission gate's half of it.
   * <p>
   * The answer is not "count the cache as free and hope". A caller told to reclaim N bytes must actually reclaim
   * them before it allocates, or the reading is simply optimistic in the other direction - which is the failure mode
   * (an {@link OutOfMemoryError}) issue #6503 built this gate to avoid. Hence the amount, rather than a boolean.
   *
   * @param bytes            what the caller wants to allocate
   * @param percent          share of available heap it may occupy, clamped to {@code [0, 90]} as elsewhere here
   * @param availableHeap    {@link #availableHeapBytes()}, or a pinned figure in a test
   * @param reclaimableBytes evictable bytes the {@code availableHeap} reading counts as live - the page read cache
   *
   * @return {@code 0} when the allocation already fits and nothing need be given up, a positive number of bytes to
   * reclaim first, or {@code -1} when it does not fit even with every reclaimable byte handed over
   */
  static long reclaimNeededFor(final long bytes, final int percent, final long availableHeap,
      final long reclaimableBytes) {
    if (percent <= 0)
      return 0L; // gate disabled: nothing to decide, and nothing to reclaim on its behalf
    if (bytes <= 0)
      return 0L;
    // Above this the multiplication below would wrap, and a request that large does not fit any heap anyway.
    if (bytes > Long.MAX_VALUE / 100)
      return -1L;

    final int p = Math.min(percent, 90);
    // The available-heap reading that would make budgetBytes(p) cover `bytes`, rounded up so the answer never
    // lands one byte short of the budget it is derived from.
    final long neededAvailable = (bytes * 100 + p - 1) / p;
    final long shortfall = neededAvailable - Math.max(0L, availableHeap);
    if (shortfall <= 0)
      return 0L;
    return shortfall <= Math.max(0L, reclaimableBytes) ? shortfall : -1L;
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
   * Heap the graph-build cache may claim: the operator's share of the ceiling, never more than 90% of what is
   * currently free.
   * <p>
   * {@link #budgetBytes(int)} takes the percent of AVAILABLE heap, which is the right denominator for an admission
   * gate that must not OOM (issue #6503). It is the wrong denominator for the build cache itself. A served ingest
   * that has just written the corpus into this JVM reports a small leftover, so 25% of that leftover is a third of
   * the cache an embedded build of the same corpus would get - and that is the steep side of the cache curve
   * (issue #7146). Taking the percent of {@code -Xmx} instead makes the two deployments agree; capping at 90% of
   * currently free heap is what still shrinks the cache when an online rebuild is holding the old graph.
   */
  static long buildCacheBudgetBytes(final int percent) {
    return buildCacheBudgetBytes(percent, maxHeapBytes(), availableHeapBytes());
  }

  /**
   * Same arithmetic as {@link #buildCacheBudgetBytes(int)}, with the heap figures supplied so a test can pin the
   * served vs embedded numbers from issue #7146 without a 24 GB fixture.
   */
  static long buildCacheBudgetBytes(final int percent, final long maxHeap, final long availableHeap) {
    if (percent <= 0)
      return 0L;
    final int p = Math.min(percent, 90);
    final long fromCeiling = Math.max(0L, maxHeap) / 100 * p;
    final long fromAvailable = Math.max(0L, availableHeap) / 100 * 90;
    return Math.min(fromCeiling, fromAvailable);
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

  /**
   * Peak heap an ONLINE rebuild is expected to hold at once: the graph being built, its build cache, and whatever
   * the graph it is replacing actually costs to keep resident (issue #7184).
   * <p>
   * That last term is the correction. {@link #estimateRebuildHeapBytes} charges a second full on-heap graph for it -
   * {@code nodes x APPROX_GRAPH_BYTES_PER_NODE} again - which is right when the resident graph is the
   * {@code OnHeapGraphIndex} a build just produced, and badly wrong on the far more common shape: a session that
   * reopened the database serves an {@code OnDiskGraphIndex}, whose topology lives in pages, and keeping it resident
   * costs a few caches rather than a gigabyte per million nodes. Charging a phantom second graph there is what made
   * a 10M-vector index in a 24 GB heap ask for 24 GB and be refused at every attempt, when the rebuild it was
   * refusing is the same size as the one the close path runs unconditionally. Asking the graph itself
   * ({@link io.github.jbellis.jvector.util.Accountable#ramBytesUsed()}) replaces the guess with a measurement, and
   * it is a measurement of exactly the object that will still be there while the new graph is built.
   * <p>
   * The graph being BUILT cannot be measured - it does not exist yet - so it is still estimated. When the resident
   * graph is itself on-heap its measured cost per node is by far the best predictor available for it: same index,
   * same {@code maxConnections}, same dimensions, measured on this JVM rather than on the 128-dimension index of
   * issue #6503. Scaled by the neighbour overflow factor, because a build holds up to that many times the final
   * out-degree per node before {@code cleanup()} trims it. With a disk-backed resident graph there is nothing to
   * learn from, and the flat constant stands.
   *
   * @param nodes                  number of vectors the build will walk
   * @param dimensions             vector width
   * @param buildCacheCapacity     vectors the build cache will hold
   * @param residentGraphBytes     {@code ramBytesUsed()} of the graph being replaced, which stays resident so
   *                               searches keep working; 0 when there is none
   * @param residentGraphNodes     how many nodes that measurement covers, 0 when unknown
   * @param residentGraphOnHeap    whether that graph holds its topology on the heap, which is what makes its
   *                               per-node cost transferable to the graph about to be built
   * @param neighborOverflowFactor the index's configured neighbour overflow factor, values below 1 ignored
   *
   * @return the estimated peak in bytes
   */
  static long estimateOnlineRebuildHeapBytes(final long nodes, final int dimensions, final long buildCacheCapacity,
      final long residentGraphBytes, final long residentGraphNodes, final boolean residentGraphOnHeap,
      final float neighborOverflowFactor) {
    long estimate = nodes > 0 ?
        nodes * (buildBytesPerNode(residentGraphBytes, residentGraphNodes, residentGraphOnHeap,
            neighborOverflowFactor) + ORDINAL_MAP_BYTES_PER_NODE) : 0L;
    estimate += Math.max(0L, buildCacheCapacity) * bytesPerCachedVector(dimensions);
    estimate += Math.max(0L, residentGraphBytes);
    return estimate;
  }

  /**
   * Bytes per node of the graph a rebuild is about to build: the resident graph's measured cost per node raised by
   * the neighbour overflow a build holds transiently, or {@link #APPROX_GRAPH_BYTES_PER_NODE} when there is no
   * on-heap graph to measure. Never below the measurement itself, and never zero.
   */
  static long buildBytesPerNode(final long residentGraphBytes, final long residentGraphNodes,
      final boolean residentGraphOnHeap, final float neighborOverflowFactor) {
    if (!residentGraphOnHeap || residentGraphBytes <= 0 || residentGraphNodes <= 0)
      return APPROX_GRAPH_BYTES_PER_NODE;

    final long measured = Math.max(1L, residentGraphBytes / residentGraphNodes);
    final float overflow = neighborOverflowFactor > 1f ? neighborOverflowFactor : 1f;
    return Math.max(measured, (long) (measured * overflow));
  }
}
