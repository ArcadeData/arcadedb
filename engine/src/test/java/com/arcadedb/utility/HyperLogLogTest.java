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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * The hash is fixed (no seed drawn at run time) and the inputs are fixed, so every estimate below is reproducible. The bounds are
 * nevertheless justified by the estimator's statistics, not tuned to the observed values: with p = 14 (m = 16384 registers) the
 * standard error of the raw estimator is 1.04 / sqrt(m), about 0.81%, so a 3% bound is more than 3.7 standard errors.
 */
class HyperLogLogTest {

  private static HyperLogLog hll(final long from, final long to) {
    final HyperLogLog h = new HyperLogLog();
    for (long i = from; i < to; i++)
      h.addLong(i);
    return h;
  }

  @Test
  void smallCardinalitiesThroughLinearCounting() {
    assertThat(new HyperLogLog().estimate()).isEqualTo(0L);
    assertThat(hll(0, 1).estimate()).isEqualTo(1L);
    // the chance of two of ten values sharing a register is about 0.3%, so one off at most
    assertThat(hll(0, 10).estimate()).isCloseTo(10L, within(1L));
    // linear counting's standard error is sqrt(m * (e^t - t - 1)) with t = n / m, about 5.6 at n = 1000: allow 4.5 of them
    assertThat(hll(0, 1000).estimate()).isCloseTo(1000L, within(25L));
  }

  @Test
  void relativeErrorBelowThreePercent() {
    for (final long n : new long[] { 10_000, 100_000, 1_000_000 })
      assertThat(Math.abs(hll(0, n).estimate() - n) / (double) n).as("n=%d", n).isLessThan(0.03);
  }

  @Test
  void duplicatesDoNotCount() {
    final HyperLogLog h = new HyperLogLog();
    for (int round = 0; round < 1000; round++)
      for (long i = 0; i < 1000; i++)
        h.addLong(i);
    assertThat(h.estimate()).isEqualTo(hll(0, 1000).estimate());
  }

  @Test
  void mergeEqualsUnion() {
    final HyperLogLog a = hll(0, 500_000);
    a.merge(hll(250_000, 750_000));
    assertThat(Math.abs(a.estimate() - 750_000L) / 750_000.0).isLessThan(0.03);
  }

  @Test
  void mergeIsRegisterWiseMaxSoItEqualsTheSketchOfTheUnion() {
    final HyperLogLog a = hll(0, 5000);
    a.merge(hll(2500, 7500));
    assertThat(a.estimate()).isEqualTo(hll(0, 7500).estimate());
  }

  @Test
  void deterministic() {
    assertThat(hll(0, 123_456).estimate()).isEqualTo(hll(0, 123_456).estimate());
    assertThat(HyperLogLog.hash(42L)).isEqualTo(HyperLogLog.hash(42L));
    assertThat(HyperLogLog.hash(1L)).isNotEqualTo(HyperLogLog.hash(2L));
  }
}
