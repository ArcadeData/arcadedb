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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The heap arithmetic behind issue #6503, on its own: every decision that used to be made against TOTAL heap and
 * now has to be made against the heap actually available, plus the footprint estimate the rebuild admission gate
 * compares to it. Cheaper to pin here than through a database, and each of these is a property no realistic
 * fixture would exercise from both sides.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6503VectorHeapBudgetTest {

  @Test
  void availableHeapNeverExceedsTheCeilingAndIsNeverNegative() {
    final long max = VectorHeapBudget.maxHeapBytes();
    final long available = VectorHeapBudget.availableHeapBytes();

    assertThat(max).as("a JVM always reports a heap ceiling").isPositive();
    assertThat(available)
        .as("available heap must stay inside [0, -Xmx]: a negative budget would size a cache to nothing and a "
            + "budget past the ceiling would be the total-heap bug this replaces")
        .isBetween(0L, max);
  }

  /**
   * A JVM that has not collected yet, or a GC that does not publish a collection usage, must fall back to the
   * whole ceiling - which reproduces exactly the previous total-heap behaviour. Assuming zero live heap would be
   * the same thing; assuming a FULL heap would silently disable every auto-sized cache in the engine.
   */
  @Test
  void unknownLiveHeapFallsBackToTheWholeCeilingRatherThanToZeroBudget() {
    final long live = VectorHeapBudget.liveHeapBytes();
    if (live < 0)
      assertThat(VectorHeapBudget.availableHeapBytes()).isEqualTo(VectorHeapBudget.maxHeapBytes());
    else
      assertThat(live).as("a published live-heap reading is an occupancy, so it cannot be negative").isNotNegative();
  }

  @Test
  void budgetIsAShareOfAvailableHeapAndIsCappedAtNinetyPercent() {
    assertThat(VectorHeapBudget.budgetBytes(0)).as("0 means 'no budget', not 'unlimited'").isZero();
    assertThat(VectorHeapBudget.budgetBytes(-5)).as("a negative percentage is not a licence to overcommit").isZero();

    final long available = VectorHeapBudget.availableHeapBytes();
    assertThat(VectorHeapBudget.budgetBytes(50)).isLessThanOrEqualTo(available / 2 + 1);

    // 100% is clamped to 90: a build must never plan on the whole heap, since the request, I/O and GC threads
    // need some of it too.
    assertThat(VectorHeapBudget.budgetBytes(100)).isEqualTo(VectorHeapBudget.budgetBytes(90));
    assertThat(VectorHeapBudget.budgetBytes(90)).isLessThan(available + 1);
  }

  @Test
  void budgetGrowsMonotonicallyWithTheRequestedShare() {
    // Not a tautology: budgetBytes() clamps and divides, and an implementation that clamped in the wrong order
    // (or divided before multiplying by a clamped value) could easily invert this for a share above the cap.
    assertThat(VectorHeapBudget.budgetBytes(10)).isLessThanOrEqualTo(VectorHeapBudget.budgetBytes(25));
    assertThat(VectorHeapBudget.budgetBytes(25)).isLessThanOrEqualTo(VectorHeapBudget.budgetBytes(50));
    assertThat(VectorHeapBudget.budgetBytes(50)).isLessThanOrEqualTo(VectorHeapBudget.budgetBytes(90));
  }

  @Test
  void anEmptyGraphCostsNothingAndNodeCostScalesLinearly() {
    assertThat(VectorHeapBudget.estimateGraphBytes(0)).isZero();
    assertThat(VectorHeapBudget.estimateGraphBytes(-1)).as("a negative node count is not a negative footprint")
        .isZero();

    final long one = VectorHeapBudget.estimateGraphBytes(1);
    assertThat(one).isGreaterThanOrEqualTo(VectorHeapBudget.APPROX_GRAPH_BYTES_PER_NODE);
    assertThat(VectorHeapBudget.estimateGraphBytes(1_000)).isEqualTo(one * 1_000);
  }

  /**
   * This is the 1.73x the issue measured (1,700 MB for a rebuild against 959 MB for the first build of the same
   * 50,000 x 128 corpus). The estimate does not have to reproduce that ratio exactly - it is deliberately coarse -
   * but a rebuild that retains the old graph must come out strictly more expensive than one that does not, or the
   * admission gate is measuring the wrong thing entirely.
   */
  @Test
  void keepingTheOldGraphResidentCostsStrictlyMoreThanReleasingIt() {
    final long nodes = 50_000;
    final int dimensions = 128;
    final long cacheCapacity = nodes;

    final long online = VectorHeapBudget.estimateRebuildHeapBytes(nodes, dimensions, cacheCapacity, true);
    final long onClose = VectorHeapBudget.estimateRebuildHeapBytes(nodes, dimensions, cacheCapacity, false);

    assertThat(online).isGreaterThan(onClose);
    assertThat(online - onClose)
        .as("the difference is exactly one resident graph generation - what the close path releases up front")
        .isEqualTo(VectorHeapBudget.estimateGraphBytes(nodes));
  }

  @Test
  void theEstimateAccountsForTheBuildCacheAsWellAsTheGraphs() {
    final long nodes = 10_000;
    final int dimensions = 256;

    final long withoutCache = VectorHeapBudget.estimateRebuildHeapBytes(nodes, dimensions, 0, false);
    final long withCache = VectorHeapBudget.estimateRebuildHeapBytes(nodes, dimensions, nodes, false);

    assertThat(withCache - withoutCache)
        .as("a build cache holding the whole corpus costs one cached vector per node")
        .isEqualTo(nodes * VectorHeapBudget.bytesPerCachedVector(dimensions));
    // A negative capacity must not credit the estimate with heap it is not going to give back.
    assertThat(VectorHeapBudget.estimateRebuildHeapBytes(nodes, dimensions, -1_000, false)).isEqualTo(withoutCache);
  }

  @Test
  void cachedVectorCostCoversThePayloadPlusTheWrapperOverhead() {
    assertThat(VectorHeapBudget.bytesPerCachedVector(128))
        .as("the float payload alone would be 512 bytes; the wrapper, cache entry and array slot are the rest")
        .isGreaterThan(128L * Float.BYTES);
    // Linear in the dimension count, so a wide-vector index is charged for being wide.
    assertThat(VectorHeapBudget.bytesPerCachedVector(256) - VectorHeapBudget.bytesPerCachedVector(128))
        .isEqualTo(128L * Float.BYTES);
  }
}
