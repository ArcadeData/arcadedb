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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RangeBucketerTest {
  @Test
  void halfOpenAndOverlapping() {
    final RangeBucketer r = new RangeBucketer(new double[] { 0, 5, Double.NaN }, new double[] { 10, 15, 5 });
    for (final double v : new double[] { -1, 0, 5, 9.999, 10, 15 })
      r.add(v);
    assertThat(r.rangeCount(0)).isEqualTo(3);
    assertThat(r.rangeCount(1)).isEqualTo(3);
    assertThat(r.rangeCount(2)).isEqualTo(2);
    assertThat(r.defaultCount()).isEqualTo(1);
  }

  @Test
  void singleIntervalForSkipping() {
    final RangeBucketer r = new RangeBucketer(new double[] { 0, 10 }, new double[] { 10, 20 });
    assertThat(r.singleIntervalOf(11d, 19d)).isNotNegative();
    assertThat(r.singleIntervalOf(11d, 19d)).isEqualTo(r.interval(15d));
    assertThat(r.singleIntervalOf(9d, 10d)).isEqualTo(-1);
    assertThat(r.singleIntervalOf(-5d, -1d)).isNotNegative();
  }

  @Test
  void addIntervalCountsWholeSegmentsAndAgreesWithPerValue() {
    final double[] from = { 0, 10, Double.NaN };
    final double[] to = { 10, 20, 100 };
    final RangeBucketer whole = new RangeBucketer(from, to);
    final RangeBucketer each = new RangeBucketer(from, to);
    whole.addInterval(whole.singleIntervalOf(11d, 19d), 3);
    for (final double v : new double[] { 11, 15, 19 })
      each.add(v);
    for (int i = 0; i < 3; i++)
      assertThat(whole.rangeCount(i)).isEqualTo(each.rangeCount(i));
    assertThat(whole.defaultCount()).isEqualTo(each.defaultCount());
    assertThat(whole.rangeCount(1)).isEqualTo(3);
  }

  @Test
  void emptyAndUnboundedRanges() {
    final RangeBucketer r = new RangeBucketer(new double[] { 5, Double.NaN, 3 }, new double[] { 5, Double.NaN, 2 });
    for (final double v : new double[] { -100, 2, 5, 1e9 })
      r.add(v);
    assertThat(r.rangeCount(0)).isZero();
    assertThat(r.rangeCount(1)).isEqualTo(4);
    assertThat(r.rangeCount(2)).isZero();
    assertThat(r.defaultCount()).isZero();
  }

  @Test
  void nanValueFallsInDefault() {
    final RangeBucketer r = new RangeBucketer(new double[] { 0 }, new double[] { 10 });
    r.add(Double.NaN);
    assertThat(r.rangeCount(0)).isZero();
    assertThat(r.defaultCount()).isEqualTo(1);
  }

  @Test
  void countsAfterMoreAddsAreNotStale() {
    final RangeBucketer r = new RangeBucketer(new double[] { 0 }, new double[] { 10 });
    r.add(1d);
    assertThat(r.rangeCount(0)).isEqualTo(1);
    r.add(2d);
    assertThat(r.rangeCount(0)).isEqualTo(2);
  }

  @Test
  void longBoundsAreExactAbove2Pow53() {
    final long base = 9_007_199_254_740_992L; // 2^53
    // [*, 2^53 + 1) and [2^53 + 1, *): a double cannot tell 2^53 + 1 from 2^53
    final RangeBucketer r = new RangeBucketer(new long[] { 0, base + 1 }, new long[] { base + 1, 0 }, new boolean[] { true, false },
        new boolean[] { false, true });
    r.add(base);
    r.add(base + 1);
    r.add(base + 1);
    r.add(base + 2);
    assertThat(r.rangeCount(0)).isEqualTo(1);
    assertThat(r.rangeCount(1)).isEqualTo(3);
    assertThat(r.defaultCount()).isZero();
    assertThat((double) (base + 1)).isEqualTo((double) base);
  }

  @Test
  void longNanosecondTimestampsJustBelowABoundStayBelow() {
    final long bound = 1_700_000_000_000_000_000L;
    final RangeBucketer r = new RangeBucketer(new long[] { bound - 1_000, bound }, new long[] { bound, bound + 1_000 },
        new boolean[2], new boolean[2]);
    r.add(bound - 1);
    r.add(bound);
    r.add(bound + 1);
    r.add(bound - 1_001);
    assertThat(r.rangeCount(0)).isEqualTo(1);
    assertThat(r.rangeCount(1)).isEqualTo(2);
    assertThat(r.defaultCount()).isEqualTo(1);
    assertThat(r.singleIntervalOf(bound - 5, bound - 1)).isNotNegative();
    assertThat(r.singleIntervalOf(bound - 1, bound)).isEqualTo(-1);
    assertThat(r.singleIntervalOf(9L, 3L)).isEqualTo(-1);
  }

  @Test
  void longWholeSegmentCountingAgreesWithPerValue() {
    final long[] from = { 0, 100 };
    final long[] to = { 100, 0 };
    final boolean[] noFrom = { false, false };
    final boolean[] openTo = { false, true };
    final RangeBucketer whole = new RangeBucketer(from, to, noFrom, openTo);
    final RangeBucketer each = new RangeBucketer(from, to, noFrom, openTo);
    whole.addInterval(whole.singleIntervalOf(100L, 5_000L), 3);
    for (final long v : new long[] { 100, 2_000, 5_000 })
      each.add(v);
    assertThat(whole.rangeCount(1)).isEqualTo(each.rangeCount(1)).isEqualTo(3);
    assertThat(whole.rangeCount(0)).isZero();
  }

  @Test
  void methodsOfTheOtherModeAreRefused() {
    final RangeBucketer doubles = new RangeBucketer(new double[] { 0 }, new double[] { 1 });
    final RangeBucketer longs = new RangeBucketer(new long[] { 0 }, new long[] { 1 }, new boolean[1], new boolean[1]);
    assertThatThrownBy(() -> doubles.add(5L)).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> longs.add(5.0d)).isInstanceOf(IllegalStateException.class);
  }
}
