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
 * Issue #7146: {@code graphBuildCacheMaxHeapPercent} sized the HNSW build cache from leftover heap at
 * build start. A served ingest that already held the corpus in the same JVM therefore chose a third of
 * the cache an embedded build of the same corpus would get. The numbers below are the DEEP-10M snapshot
 * from the report (24 GB {@code -Xmx}, 96-d, 9,990,000 vectors, served leftover ~6.6 GB).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7146GraphBuildCacheBudgetTest {

  private static final int  DIMENSIONS          = 96;
  private static final int  CORPUS              = 9_990_000;
  private static final int  DEFAULT_PERCENT     = 25;
  /** 24 GiB, the {@code -Xmx} both deployments used. */
  private static final long MAX_HEAP            = 24L * 1024 * 1024 * 1024;
  /**
   * Leftover heap the engine saw on the served arm: working back from the log line
   * {@code cache enabled: size=3674697} at 448 bytes/vector and a 25% share of available heap.
   */
  private static final long SERVED_AVAILABLE    = 6_585_057_024L;
  /** Embedded arm: corpus lives in numpy outside the JVM, so almost the whole ceiling is free. */
  private static final long EMBEDDED_AVAILABLE  = MAX_HEAP - 512L * 1024 * 1024;

  @Test
  void theOldPercentOfAvailableFormulaChoseAThirdOfTheCorpusOnTheServedArm() {
    final long bytesPer = VectorHeapBudget.bytesPerCachedVector(DIMENSIONS);
    assertThat(bytesPer).isEqualTo(448L);

    // This is budgetBytes(): percent of leftover, not of the ceiling.
    final long oldServedBudget = SERVED_AVAILABLE / 100 * DEFAULT_PERCENT;
    final long oldServedCapacity = oldServedBudget / bytesPer;

    assertThat(oldServedCapacity)
        .as("the size=3674697 log line the served default actually chose")
        .isBetween(3_600_000L, 3_700_000L);
    assertThat(oldServedCapacity).isLessThan(CORPUS);

    final long oldEmbeddedBudget = EMBEDDED_AVAILABLE / 100 * DEFAULT_PERCENT;
    final long oldEmbeddedCapacity = oldEmbeddedBudget / bytesPer;
    assertThat(oldEmbeddedCapacity)
        .as("embedded 25% of ~23.5 GB pays for more than the corpus, so it was capped at N")
        .isGreaterThanOrEqualTo(CORPUS);
  }

  @Test
  void theDefaultPercentOfTheCeilingPaysForTheWholeDeep10mCorpusOnBothArms() {
    final long bytesPer = VectorHeapBudget.bytesPerCachedVector(DIMENSIONS);

    final long servedBudget = VectorHeapBudget.buildCacheBudgetBytes(DEFAULT_PERCENT, MAX_HEAP, SERVED_AVAILABLE);
    final long servedCapacity = servedBudget / bytesPer;
    assertThat(servedCapacity)
        .as("served must no longer land on the steep side of the cache curve at the default")
        .isGreaterThanOrEqualTo(CORPUS);

    final long embeddedBudget = VectorHeapBudget.buildCacheBudgetBytes(DEFAULT_PERCENT, MAX_HEAP, EMBEDDED_AVAILABLE);
    final long embeddedCapacity = embeddedBudget / bytesPer;
    assertThat(embeddedCapacity).isGreaterThanOrEqualTo(CORPUS);

    // Served leftover is the cap (~90% of 6.6 GB), embedded leftover is not; both still clear the corpus.
    assertThat(servedBudget).isLessThanOrEqualTo(embeddedBudget);
  }

  @Test
  void anOnlineRebuildWithLittleLeftoverStillCapsAtAvailableHeap() {
    // Old graph resident: ~2 GB free. The cache must not ask for 25% of 24 GB on top of that (issue #6503).
    final long tightAvailable = 2L * 1024 * 1024 * 1024;
    final long budget = VectorHeapBudget.buildCacheBudgetBytes(DEFAULT_PERCENT, MAX_HEAP, tightAvailable);

    assertThat(budget).isEqualTo(tightAvailable / 100 * 90);
    assertThat(budget).isLessThan(MAX_HEAP / 100 * DEFAULT_PERCENT);
  }

  @Test
  void buildCacheBudgetIsNeverSmallerThanTheAdmissionGateShareOfTheSameLeftover() {
    // Admission control (budgetBytes) stays on leftover heap. The build cache may claim more than that
    // share of leftover, but never less: otherwise we would reintroduce the served under-size.
    for (final int percent : new int[] { 10, 25, 50, 75, 90, 100 }) {
      final long admission = SERVED_AVAILABLE / 100 * Math.min(percent, 90);
      final long cache = VectorHeapBudget.buildCacheBudgetBytes(percent, MAX_HEAP, SERVED_AVAILABLE);
      assertThat(cache)
          .as("build-cache budget at %d%% must not undershoot the admission-gate share of leftover", percent)
          .isGreaterThanOrEqualTo(admission);
    }
  }

  @Test
  void aZeroOrNegativePercentIsNoBudgetAndValuesAboveNinetyClamp() {
    assertThat(VectorHeapBudget.buildCacheBudgetBytes(0, MAX_HEAP, SERVED_AVAILABLE)).isZero();
    assertThat(VectorHeapBudget.buildCacheBudgetBytes(-5, MAX_HEAP, SERVED_AVAILABLE)).isZero();
    assertThat(VectorHeapBudget.buildCacheBudgetBytes(100, MAX_HEAP, EMBEDDED_AVAILABLE))
        .isEqualTo(VectorHeapBudget.buildCacheBudgetBytes(90, MAX_HEAP, EMBEDDED_AVAILABLE));
  }

  /**
   * The live overload reads {@link VectorHeapBudget#availableHeapBytes()} itself, and that reading moves whenever
   * the JVM collects - so comparing it against a second reading taken here would be a coin flip on whether a GC
   * landed between the two, which in a full-suite run it regularly does. What is invariant, and is the whole
   * point of the change, is the upper bound: a share of a ceiling that cannot move for the life of the JVM.
   */
  @Test
  void theLiveJvmOverloadNeverExceedsTheShareOfTheCeilingItIsGiven() {
    final int percent = 25;
    final long budget = VectorHeapBudget.buildCacheBudgetBytes(percent);

    assertThat(budget).isNotNegative();
    assertThat(budget)
        .as("the share of -Xmx is the most the build cache may claim, whatever the live heap happens to say")
        .isLessThanOrEqualTo(VectorHeapBudget.maxHeapBytes() / 100 * percent);
  }
}
