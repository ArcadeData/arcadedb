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

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.math.BigInteger;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Oracle tests of the heap arithmetic that decides whether a graph build or rebuild is admitted, and with how large a cache
 * ({@link VectorHeapBudget}): every function is held to an independent BigInteger model written from its Javadoc, over a grid
 * that includes the exact boundaries (a request that needs exactly the reclaimable bytes, a graph that exactly fills the
 * budget, a percent at the 90 clamp, a size that would wrap a long).
 */
class VectorHeapBudgetOracleTest {
  private static final BigInteger HUNDRED = BigInteger.valueOf(100);

  /** What the models charge a graph node and a cached vector, built from the constants of the production class. */
  private static final long GRAPH_NODE_BYTES       = VectorHeapBudget.APPROX_GRAPH_BYTES_PER_NODE + Integer.BYTES;
  private static final long CACHED_VECTOR_OVERHEAD = VectorHeapBudget.bytesPerCachedVector(0);

  @Test
  void budgetIsThePercentOfTheAvailableHeapRoundedDownAndClampedAtNinety() {
    for (final int percent : new int[] { Integer.MIN_VALUE, -1, 0, 1, 25, 89, 90, 91, 100, Integer.MAX_VALUE })
      for (final long available : new long[] { 0L, 1L, 99L, 100L, 101L, 12_345L, 1L << 30, 24L << 30, Long.MAX_VALUE }) {
        final long expected = percent <= 0 ? 0L : BigInteger.valueOf(available).divide(HUNDRED)
            .multiply(BigInteger.valueOf(Math.min(percent, 90))).longValueExact();
        assertThat(VectorHeapBudget.budgetBytes(percent, available)).as("percent=%d available=%d", percent, available)
            .isEqualTo(expected);
      }
  }

  @Test
  void budgetAgainstTheLiveHeapStaysInsideTheHeap() {
    // The one-argument form reads the JVM: it is the two-argument form over the figures the JVM publishes
    final long available = VectorHeapBudget.availableHeapBytes();
    assertThat(available).isBetween(0L, VectorHeapBudget.maxHeapBytes());
    assertThat(VectorHeapBudget.maxHeapBytes()).isEqualTo(Runtime.getRuntime().maxMemory());
    for (final int percent : new int[] { 1, 50, 90, 100 }) {
      final long budget = VectorHeapBudget.budgetBytes(percent);
      assertThat(budget).as("percent=%d", percent).isPositive().isLessThanOrEqualTo(VectorHeapBudget.maxHeapBytes() / 100 * Math.min(percent, 90));
      assertThat(VectorHeapBudget.buildCacheBudgetBytes(percent)).as("build cache, percent=%d", percent).isPositive()
          .isLessThanOrEqualTo(VectorHeapBudget.maxHeapBytes() / 100 * Math.min(percent, 90));
    }
    assertThat(VectorHeapBudget.budgetBytes(0)).isZero();
    assertThat(VectorHeapBudget.buildCacheBudgetBytes(0)).isZero();
  }

  @Test
  void buildCacheBudgetIsThePercentOfTheCeilingCappedAtNinetyPercentOfWhatIsFree() {
    final long[] heaps = { 0L, 1L, 100L, 1L << 20, 8L << 30, 24L << 30 };
    for (final int percent : new int[] { -5, 0, 1, 25, 90, 95 })
      for (final long max : heaps)
        for (final long available : heaps) {
          final BigInteger fromCeiling = BigInteger.valueOf(max).divide(HUNDRED).multiply(BigInteger.valueOf(Math.min(percent, 90)));
          final BigInteger fromAvailable = BigInteger.valueOf(available).divide(HUNDRED).multiply(BigInteger.valueOf(90));
          final long expected = percent <= 0 ? 0L : fromCeiling.min(fromAvailable).longValueExact();
          assertThat(VectorHeapBudget.buildCacheBudgetBytes(percent, max, available))
              .as("percent=%d max=%d available=%d", percent, max, available).isEqualTo(expected);
        }
    // Negative figures count as nothing
    assertThat(VectorHeapBudget.buildCacheBudgetBytes(50, -1L << 20, 1L << 30)).isZero();
    assertThat(VectorHeapBudget.buildCacheBudgetBytes(50, 1L << 30, -1L << 20)).isZero();
  }

  @Test
  void reclaimNeededForMatchesTheCeilingModelAtEveryBoundary() {
    final Random random = new Random(41);
    for (int round = 0; round < 20_000; round++) {
      final long bytes = pick(random, new long[] { -1L, 0L, 1L, 99L, 100L, 1L << 20, 1L << 40, Long.MAX_VALUE / 100 - 1,
          Long.MAX_VALUE / 100 + 1, Long.MAX_VALUE });
      final int percent = (int) pick(random, new long[] { -1, 0, 1, 7, 50, 89, 90, 91, 1000 });
      final long available = pick(random, new long[] { -5L, 0L, 1L, 1L << 20, 1L << 33, Long.MAX_VALUE });
      final long reclaimable = pick(random, new long[] { -5L, 0L, 1L, 1L << 20, 1L << 33, Long.MAX_VALUE });
      assertThat(VectorHeapBudget.reclaimNeededFor(bytes, percent, available, reclaimable))
          .as("bytes=%d percent=%d available=%d reclaimable=%d", bytes, percent, available, reclaimable)
          .isEqualTo(reclaimModel(bytes, percent, available, reclaimable));
    }

    // The exact boundaries, built from the model: a request that needs `r` more bytes than are available, offered r, r - 1 and r + 1
    for (final int percent : new int[] { 1, 3, 50, 90, 100 })
      for (final long bytes : new long[] { 1L, 7L, 1_000L, 123_456_789L }) {
        final BigInteger p = BigInteger.valueOf(Math.min(percent, 90));
        final long needed = BigInteger.valueOf(bytes).add(p).subtract(BigInteger.ONE).divide(p).multiply(HUNDRED).longValueExact();
        // The reading is the least one the budget covers the request at, whatever the rounding of budgetBytes()
        assertThat(VectorHeapBudget.budgetBytes(percent, needed)).as("budget at the needed reading covers %d", bytes)
            .isGreaterThanOrEqualTo(bytes);
        assertThat(VectorHeapBudget.budgetBytes(percent, needed - 1)).as("budget one byte below does not cover %d", bytes)
            .isLessThan(bytes);
        for (final long shortfall : new long[] { 1L, 5L, 1_000L }) {
          final long available = needed - shortfall;
          if (available < 0)
            continue;
          assertThat(VectorHeapBudget.reclaimNeededFor(bytes, percent, available, shortfall)).as("exactly the reclaimable bytes")
              .isEqualTo(shortfall);
          assertThat(VectorHeapBudget.reclaimNeededFor(bytes, percent, available, shortfall - 1)).as("one byte short").isEqualTo(-1L);
          assertThat(VectorHeapBudget.reclaimNeededFor(bytes, percent, available, shortfall + 1)).isEqualTo(shortfall);
          assertThat(VectorHeapBudget.reclaimNeededFor(bytes, percent, needed, 0L)).as("fits exactly").isZero();
          assertThat(VectorHeapBudget.reclaimNeededFor(bytes, percent, needed - 1, 0L)).as("one byte short, nothing to give").isEqualTo(-1L);
          assertThat(VectorHeapBudget.reclaimNeededFor(bytes, percent, needed - 1, 1L)).as("one byte short, one to give").isEqualTo(1L);
        }
      }

    // A request the budget does not cover is never admitted for free, whatever the rounding: 1 byte at 90% of a reading of 2
    assertThat(VectorHeapBudget.budgetBytes(90, 2)).isZero();
    assertThat(VectorHeapBudget.reclaimNeededFor(1L, 90, 2L, 0L)).isEqualTo(-1L);
    assertThat(VectorHeapBudget.reclaimNeededFor(1L, 90, 2L, 98L)).isEqualTo(98L);
    assertThat(VectorHeapBudget.reclaimNeededFor(1L, 90, 100L, 0L)).isZero();
  }

  @Test
  void largestBuildCacheThatFitsMatchesTheModelAndNeverAdmitsAByteTooMany() {
    final Random random = new Random(43);
    for (int round = 0; round < 20_000; round++) {
      final long nodes = pick(random, new long[] { -1L, 0L, 1L, 10L, 1_000L, 100_000L, 10_000_000L });
      final int dimensions = (int) pick(random, new long[] { 1, 8, 384, 1024, 1536 });
      final int percent = (int) pick(random, new long[] { -1, 0, 1, 25, 90, 120 });
      final long available = pick(random, new long[] { -1L, 0L, 1L << 20, 1L << 30, 8L << 30, Long.MAX_VALUE });
      final long reclaimable = pick(random, new long[] { -1L, 0L, 1L << 20, 1L << 30, Long.MAX_VALUE });
      assertThat(VectorHeapBudget.largestBuildCacheThatFits(nodes, dimensions, percent, available, reclaimable))
          .as("nodes=%d dimensions=%d percent=%d available=%d reclaimable=%d", nodes, dimensions, percent, available, reclaimable)
          .isEqualTo(cacheModel(nodes, dimensions, percent, available, reclaimable));
    }
  }

  @Test
  void aGraphThatExactlyFillsTheBudgetLeavesRoomForACacheOfZeroVectors() {
    // A graph that exactly fills the budget leaves room for a cache of zero vectors, one byte less for none at all
    final long nodes = 1_000L;
    final long graph = VectorHeapBudget.estimateRebuildHeapBytes(nodes, 16, 0L, false);
    final long exactTotal = (graph / 90 + 1) * 100; // 100 / 90 of the graph: budget >= graph
    final long budget = exactTotal / 100 * 90;
    assertThat(budget).isGreaterThanOrEqualTo(graph);
    final long cache = VectorHeapBudget.largestBuildCacheThatFits(nodes, 16, 90, exactTotal, 0L);
    assertThat(cache).isEqualTo((budget - graph) / VectorHeapBudget.bytesPerCachedVector(16));
    // The estimate of the cache that was answered is one reclaimNeededFor admits, and one more vector is not
    final long withCache = VectorHeapBudget.estimateRebuildHeapBytes(nodes, 16, cache, false);
    assertThat(VectorHeapBudget.reclaimNeededFor(withCache, 90, exactTotal, 0L)).isZero();
    assertThat(VectorHeapBudget.reclaimNeededFor(VectorHeapBudget.estimateRebuildHeapBytes(nodes, 16, cache + 1, false), 90,
        exactTotal, 0L)).isEqualTo(-1L);
  }

  @Test
  void theLargestGraphThatFitsIsTheLastOneTheEstimateLetsThrough() {
    final long total = 100L * 1_000_000L;
    final long budgetExact = total / 100 * 90; // 90,000,000
    long n = 1L;
    while (VectorHeapBudget.estimateRebuildHeapBytes(n, 16, 0L, false) <= budgetExact)
      n *= 2;
    long lo = n / 2;
    long hi = n;
    while (hi - lo > 1) {
      final long mid = (lo + hi) / 2;
      if (VectorHeapBudget.estimateRebuildHeapBytes(mid, 16, 0L, false) <= budgetExact)
        lo = mid;
      else
        hi = mid;
    }
    assertThat(VectorHeapBudget.largestBuildCacheThatFits(lo, 16, 90, total, 0L)).as("the largest graph that fits").isGreaterThanOrEqualTo(0L);
    assertThat(VectorHeapBudget.largestBuildCacheThatFits(hi, 16, 90, total, 0L)).as("one node more").isEqualTo(-1L);
  }

  @Test
  void aGraphEqualToTheBudgetToTheByteGetsACacheOfZeroVectorsNotARefusal() {
    // A graph that equals the budget to the byte: the cache is zero vectors, not a refusal
    final long exactNodes = 90_000L;
    final long exactGraph = VectorHeapBudget.estimateGraphBytes(exactNodes);
    assertThat(exactGraph % 90).isZero();
    final long exactAvailable = exactGraph / 90 * 100;
    assertThat(exactAvailable / 100 * 90).isEqualTo(exactGraph);
    assertThat(VectorHeapBudget.largestBuildCacheThatFits(exactNodes, 16, 90, exactAvailable, 0L)).isZero();
    assertThat(VectorHeapBudget.largestBuildCacheThatFits(exactNodes, 16, 90, exactAvailable - 100, 0L)).isEqualTo(-1L);
  }

  @Test
  void heapReadingsThatWouldWrapALongWhenSummedStillAnswer() {
    // Heap readings that would wrap a long when summed
    assertThat(VectorHeapBudget.largestBuildCacheThatFits(1_000L, 16, 90, Long.MAX_VALUE, Long.MAX_VALUE)).isPositive();
    assertThat(VectorHeapBudget.largestBuildCacheThatFits(1_000L, 16, 90, Long.MAX_VALUE - 1, 1L)).isPositive();
  }

  @Test
  void graphAndRebuildEstimatesAreTheDocumentedProducts() {
    assertThat(VectorHeapBudget.estimateGraphBytes(0L)).isZero();
    assertThat(VectorHeapBudget.estimateGraphBytes(-7L)).isZero();
    assertThat(VectorHeapBudget.estimateGraphBytes(1L)).isEqualTo(GRAPH_NODE_BYTES);
    assertThat(VectorHeapBudget.estimateGraphBytes(1_000_000L)).isEqualTo(1_000_000L * GRAPH_NODE_BYTES);

    // A cached vector costs its floats plus a fixed overhead per vector
    assertThat(CACHED_VECTOR_OVERHEAD).isPositive();
    assertThat(VectorHeapBudget.bytesPerCachedVector(1024)).isEqualTo(cachedVector(1024));

    for (final long nodes : new long[] { 0L, 1L, 1_000L })
      for (final long cache : new long[] { -3L, 0L, 10L, 5_000L })
        for (final boolean resident : new boolean[] { false, true }) {
          final long graph = nodes * GRAPH_NODE_BYTES;
          final long expected = graph + Math.max(0L, cache) * cachedVector(16) + (resident ? graph : 0L);
          assertThat(VectorHeapBudget.estimateRebuildHeapBytes(nodes, 16, cache, resident))
              .as("nodes=%d cache=%d resident=%s", nodes, cache, resident).isEqualTo(expected);
        }
  }

  @Test
  void onlineRebuildEstimateLearnsTheCostPerNodeOnlyFromAnOnHeapGraph() {
    final long flat = VectorHeapBudget.APPROX_GRAPH_BYTES_PER_NODE;
    // No usable measurement: the flat constant, whichever way the measurement is missing
    assertThat(VectorHeapBudget.buildBytesPerNode(5_000_000L, 1_000L, false, 2f)).as("disk-backed").isEqualTo(flat);
    assertThat(VectorHeapBudget.buildBytesPerNode(0L, 1_000L, true, 2f)).as("no bytes").isEqualTo(flat);
    assertThat(VectorHeapBudget.buildBytesPerNode(-1L, 1_000L, true, 2f)).as("negative bytes").isEqualTo(flat);
    assertThat(VectorHeapBudget.buildBytesPerNode(5_000_000L, 0L, true, 2f)).as("no nodes").isEqualTo(flat);
    assertThat(VectorHeapBudget.buildBytesPerNode(5_000_000L, -1L, true, 2f)).as("negative nodes").isEqualTo(flat);
    // The measurement, raised by the overflow factor, never below the measurement and never zero
    assertThat(VectorHeapBudget.buildBytesPerNode(5_000_000L, 1_000L, true, 1f)).isEqualTo(5_000L);
    assertThat(VectorHeapBudget.buildBytesPerNode(5_000_000L, 1_000L, true, 1.2f)).isEqualTo(6_000L);
    assertThat(VectorHeapBudget.buildBytesPerNode(5_000_000L, 1_000L, true, 0.5f)).as("below 1 is ignored").isEqualTo(5_000L);
    assertThat(VectorHeapBudget.buildBytesPerNode(5_000_000L, 1_000L, true, Float.NaN)).as("NaN is ignored").isEqualTo(5_000L);
    assertThat(VectorHeapBudget.buildBytesPerNode(1L, 1_000L, true, 1f)).as("never zero").isEqualTo(1L);
    assertThat(VectorHeapBudget.buildBytesPerNode(1L, 1L, true, 3f)).isEqualTo(3L);

    // The estimate: the graph being built at that cost plus the ordinal map, the cache, and what stays resident
    assertThat(VectorHeapBudget.estimateOnlineRebuildHeapBytes(1_000L, 16, 10L, 5_000_000L, 1_000L, true, 1f))
        .isEqualTo(1_000L * (5_000L + Integer.BYTES) + 10L * cachedVector(16) + 5_000_000L);
    assertThat(VectorHeapBudget.estimateOnlineRebuildHeapBytes(1_000L, 16, 10L, 5_000_000L, 1_000L, true, 1f, 700L))
        .as("the twin that stays resident is charged, the on-heap measurement only teaches the per-node cost")
        .isEqualTo(1_000L * (5_000L + Integer.BYTES) + 10L * cachedVector(16) + 700L);
    assertThat(VectorHeapBudget.estimateOnlineRebuildHeapBytes(1_000L, 16, 0L, 0L, 0L, false, 1.2f))
        .as("no resident graph: the flat constant").isEqualTo(1_000L * GRAPH_NODE_BYTES);
    assertThat(VectorHeapBudget.estimateOnlineRebuildHeapBytes(0L, 16, 10L, 0L, 0L, false, 1.2f, 0L)).as("no nodes: no graph")
        .isEqualTo(10L * cachedVector(16));
    assertThat(VectorHeapBudget.estimateOnlineRebuildHeapBytes(-5L, 16, -10L, 0L, 0L, false, 1.2f, -9L)).as("negatives count as nothing")
        .isZero();
  }

  @Test
  void collectorNamesAreWhitelisted() {
    for (final String tenured : new String[] { "G1 Old Generation", "G1 Concurrent GC", "MarkSweepCompact", "PS MarkSweep",
        "ZGC Cycles", "ZGC Major Cycles", "Shenandoah Cycles", "Concurrent Mark Sweep", "ConcurrentMarkSweep", "MAJOR" })
      assertThat(VectorHeapBudget.isTenuredCollector(tenured)).as(tenured).isTrue();
    for (final String young : new String[] { "G1 Young Generation", "Copy", "PS Scavenge", "ParNew", "ZGC Minor Cycles",
        "Shenandoah Pauses", "Epsilon", "", "G1 Minor", "Young Old" })
      assertThat(VectorHeapBudget.isTenuredCollector(young)).as(young).isFalse();
    assertThat(VectorHeapBudget.isTenuredCollector(null)).isFalse();
  }

  @Test
  void liveHeapAndLastCollectionStayInsideWhatTheJvmPublishes() {
    // A hint: under -XX:+DisableExplicitGC, or a collector that publishes no collection usage, the test is skipped below
    System.gc();
    final long live = VectorHeapBudget.liveHeapBytes();
    assumeTrue(live > 0, "this JVM publishes no usable collection usage of its tenured pool");
    assertThat(live).isLessThanOrEqualTo(VectorHeapBudget.maxHeapBytes());
    assertThat(VectorHeapBudget.availableHeapBytes()).as("ceiling minus live, never negative").isBetween(0L, VectorHeapBudget.maxHeapBytes());

    final long lastCollection = VectorHeapBudget.lastCollectionEndMillis();
    assumeTrue(lastCollection >= 0, "this JVM publishes no end time of its last collection");
    // A wall-clock time: after the JVM started and not after now. A sign slip would put it before the start
    assertThat(lastCollection).isBetween(ManagementFactory.getRuntimeMXBean().getStartTime(), System.currentTimeMillis() + 1_000L);
  }

  /** {@link VectorHeapBudget#reclaimNeededFor} written from its Javadoc, in exact arithmetic. */
  private static long reclaimModel(final long bytes, final int percent, final long availableHeap, final long reclaimable) {
    if (percent <= 0 || bytes <= 0)
      return 0L;
    if (BigInteger.valueOf(bytes).multiply(HUNDRED).compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0)
      return -1L; // "a request that large does not fit any heap anyway"
    final BigInteger p = BigInteger.valueOf(Math.min(percent, 90));
    // 100 * ceil(bytes / p): budgetBytes() divides by 100 before it multiplies by p, so the reading that makes it cover `bytes` is a
    // multiple of 100 (bytes 1, percent 90, available 2: budget 0, the request does not fit)
    final BigInteger needed = BigInteger.valueOf(bytes).add(p).subtract(BigInteger.ONE).divide(p).multiply(HUNDRED);
    final BigInteger shortfall = needed.subtract(BigInteger.valueOf(Math.max(0L, availableHeap)));
    if (shortfall.signum() <= 0)
      return 0L;
    return shortfall.compareTo(BigInteger.valueOf(Math.max(0L, reclaimable))) <= 0 ? shortfall.longValueExact() : -1L;
  }

  /** {@link VectorHeapBudget#largestBuildCacheThatFits} written from its Javadoc, in exact arithmetic. */
  private static long cacheModel(final long nodes, final int dimensions, final int percent, final long availableHeap,
      final long reclaimable) {
    if (percent <= 0 || nodes <= 0)
      return Long.MAX_VALUE;
    final BigInteger total = BigInteger.valueOf(Math.max(0L, availableHeap)).add(BigInteger.valueOf(Math.max(0L, reclaimable)))
        .min(BigInteger.valueOf(Long.MAX_VALUE));
    final BigInteger budget = total.divide(HUNDRED).multiply(BigInteger.valueOf(Math.min(percent, 90)));
    final BigInteger graph = BigInteger.valueOf(nodes).multiply(BigInteger.valueOf(GRAPH_NODE_BYTES));
    if (graph.compareTo(budget) > 0)
      return -1L;
    return budget.subtract(graph).divide(BigInteger.valueOf(cachedVector(dimensions))).longValueExact();
  }

  private static long cachedVector(final int dimensions) {
    return dimensions * (long) Float.BYTES + CACHED_VECTOR_OVERHEAD;
  }

  private static long pick(final Random random, final long[] values) {
    return values[random.nextInt(values.length)];
  }
}
