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
package com.arcadedb.function.sql.math;

import com.arcadedb.function.sql.SQLAggregatedFunction;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cross-row fold of {@code min()}/{@code max()} allocated a pair of numbers for every row it folded, even when
 * the two values were of one class and the cast changed neither of them: the pair is what
 * {@link Type#castComparableNumber(Number, Number)} answers, and it is a fresh array on every call.
 * <p>
 * A column of one numeric type - what a typed column holds - is that case, so an aggregation over a million rows
 * allocated a million pairs to say nothing.
 * <p>
 * The cost is measured in the bytes the thread allocates, not in time: the pair is a fixed-size allocation the fold
 * makes per row, so the bytes per fold say what a clock cannot say without one.
 */
class SQLFunctionMinMaxAllocationTest {

  private static final int WARMUP_ROUNDS = 20_000;
  private static final int MEASURED_ROUNDS = 200_000;

  @Test
  void minFoldsARowWithoutAllocatingAPair() {
    assertThat(bytesPerFold(new SQLFunctionMin())).isLessThan(8);
  }

  @Test
  void maxFoldsARowWithoutAllocatingAPair() {
    assertThat(bytesPerFold(new SQLFunctionMax())).isLessThan(8);
  }

  @Test
  void minStillFoldsValuesOfTwoClasses() {
    final SQLFunctionMin f = new SQLFunctionMin();
    f.config(new Object[] { "v" });
    f.aggregate(null, new Object[] { Integer.valueOf(7) }, null);
    f.aggregate(null, new Object[] { new BigDecimal("2.5") }, null);
    assertThat(((Number) f.getResult()).doubleValue()).isEqualTo(2.5);
  }

  @Test
  void maxStillFoldsValuesOfTwoClasses() {
    final SQLFunctionMax f = new SQLFunctionMax();
    f.config(new Object[] { "v" });
    f.aggregate(null, new Object[] { Integer.valueOf(7) }, null);
    f.aggregate(null, new Object[] { new BigDecimal("2.5") }, null);
    assertThat(((Number) f.getResult()).doubleValue()).isEqualTo(7.0);
  }

  @Test
  void minStillStripsTheTrailingZerosOfAPairOfDecimals() {
    // A PAIR OF DECIMALS IS NOT AN IDENTITY FOR THE CAST: castComparableNumber() STRIPS THE TRAILING ZEROS OF BOTH, AND AN
    // EQUALITY-BASED CALLER DEPENDS ON IT. THE FOLD THEREFORE KEEPS CASTING A PAIR OF DECIMALS
    final SQLFunctionMin f = new SQLFunctionMin();
    f.config(new Object[] { "v" });
    f.aggregate(null, new Object[] { new BigDecimal("1.50") }, null);
    f.aggregate(null, new Object[] { new BigDecimal("1.75") }, null);
    assertThat(f.getResult()).isEqualTo(new BigDecimal("1.5"));
  }

  /**
   * The bytes one fold of one row costs the thread that makes it: the average over {@link #MEASURED_ROUNDS} folds,
   * after the JIT has settled.
   */
  private static double bytesPerFold(final SQLAggregatedFunction fold) {
    // ONE PARAMETER, AND NOT ONE THAT NAMES $current: THE CROSS-ROW FORM OF THE FUNCTION
    fold.config(new Object[] { "v" });
    final Object[] row = { Double.valueOf(1.5d) };

    for (int i = 0; i < WARMUP_ROUNDS; i++)
      fold.aggregate(null, row, null);

    final com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    final long threadId = Thread.currentThread().threadId();
    final long before = threads.getThreadAllocatedBytes(threadId);
    for (int i = 0; i < MEASURED_ROUNDS; i++)
      fold.aggregate(null, row, null);
    final long allocated = threads.getThreadAllocatedBytes(threadId) - before;

    return (double) allocated / MEASURED_ROUNDS;
  }
}
