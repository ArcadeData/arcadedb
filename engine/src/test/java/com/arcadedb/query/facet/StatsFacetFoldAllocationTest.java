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
package com.arcadedb.query.facet;

import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The raw min/max fold of the stats facet folds a value as {@code min()}/{@code max()} fold a row, and it cast every pair of
 * numbers it folded, allocating a fresh pair of numbers twice per raw value (once for the min, once for the max) even when the
 * two values were of one class and the cast changed neither of them (#13009).
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class StatsFacetFoldAllocationTest {

  private static final int WARMUP_ROUNDS   = 20_000;
  private static final int MEASURED_ROUNDS = 200_000;

  @Test
  void foldOfTwoNumbersOfOneClassAllocatesNothing() {
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    assumeTrue(threads.isThreadAllocatedMemorySupported() && threads.isThreadAllocatedMemoryEnabled(),
        "this JVM does not count the bytes a thread allocates");

    final Object current = Double.valueOf(1.5d);
    final Object candidate = Double.valueOf(2.5d);
    Object sink = null;
    for (int i = 0; i < WARMUP_ROUNDS; i++)
      sink = StatsFacetCollector.fold(current, candidate, (i & 1) == 0);

    final long threadId = Thread.currentThread().threadId();
    final long before = threads.getThreadAllocatedBytes(threadId);
    for (int i = 0; i < MEASURED_ROUNDS; i++)
      sink = StatsFacetCollector.fold(current, candidate, (i & 1) == 0);
    final long allocated = threads.getThreadAllocatedBytes(threadId) - before;

    assertThat(sink).isNotNull();
    assertThat((double) allocated / MEASURED_ROUNDS).isLessThan(8);
  }

  @Test
  void foldAnswersAsBefore() {
    // one class: the values themselves, a tie keeps the current one
    assertThat(StatsFacetCollector.fold(3L, 7L, true)).isEqualTo(3L);
    assertThat(StatsFacetCollector.fold(3L, 7L, false)).isEqualTo(7L);
    final Double current = Double.valueOf(2.0d);
    assertThat(StatsFacetCollector.fold(current, Double.valueOf(2.0d), true)).isSameAs(current);

    // two classes: the result in the common class
    assertThat(StatsFacetCollector.fold(3, 7L, true)).isEqualTo(3L);
    assertThat(StatsFacetCollector.fold(3, new BigDecimal("2.5"), true)).isEqualTo(new BigDecimal("2.5"));

    // a pair of decimals is still stripped of its trailing zeros
    assertThat(StatsFacetCollector.fold(new BigDecimal("1.50"), new BigDecimal("1.750"), true)).isEqualTo(new BigDecimal("1.5"));
    assertThat(StatsFacetCollector.fold(new BigDecimal("1.50"), new BigDecimal("1.750"), false)).isEqualTo(new BigDecimal("1.75"));
  }
}
