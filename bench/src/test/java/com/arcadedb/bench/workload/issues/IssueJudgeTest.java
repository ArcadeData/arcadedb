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
package com.arcadedb.bench.workload.issues;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IssueJudgeTest {
  @Test
  void ratioUnderLimitPasses() {
    final IssueJudge.Verdict v = IssueJudge.ratio(10.0, 5.0, 3.0);
    assertThat(v.value()).isEqualTo(2.0);
    assertThat(v.judged()).isTrue();
    assertThat(v.failed()).isFalse();
  }

  @Test
  void ratioOverLimitFails() {
    final IssueJudge.Verdict v = IssueJudge.ratio(30.0, 5.0, 3.0);
    assertThat(v.failed()).isTrue();
    assertThat(v.reason()).contains("slow median 30.0 ms").contains("reference median 5.0 ms").contains("limit 3.0");
  }

  @Test
  void aFastSlowFormIsNotJudgedButAnExpensiveOneIs() {
    final IssueJudge.Verdict fast = IssueJudge.ratio(4.9, 0.098, 3.0);
    assertThat(fast.value()).isGreaterThan(40.0);
    assertThat(fast.judged()).isFalse();
    assertThat(fast.failed()).isFalse();

    final IssueJudge.Verdict slow = IssueJudge.ratio(6.0, 0.12, 3.0);
    assertThat(slow.judged()).isTrue();
    assertThat(slow.failed()).isTrue();
  }

  @Test
  void exponentOfLinearAndFlatSeries() {
    assertThat(IssueJudge.exponent(new double[] { 1, 2, 4 }, new double[] { 10, 20, 40 })).isCloseTo(1.0, within(0.01));
    assertThat(IssueJudge.exponent(new double[] { 1, 2, 4 }, new double[] { 10, 10, 10 })).isCloseTo(0.0, within(0.01));
  }

  @Test
  void exponentVerdictJudgesOnTheLargestSize() {
    final double[] sizes = { 1, 2, 4 };
    assertThat(IssueJudge.exponent(sizes, new double[] { 1, 2, 4 }, 0.5).judged()).isFalse();
    final IssueJudge.Verdict v = IssueJudge.exponent(sizes, new double[] { 10, 20, 40 }, 0.5);
    assertThat(v.judged()).isTrue();
    assertThat(v.failed()).isTrue();
    assertThat(IssueJudge.exponent(sizes, new double[] { 10, 10, 10 }, 0.5).failed()).isFalse();
  }

  @Test
  void degenerateExponentInputFailsInsteadOfPassingSilently() {
    final IssueJudge.Verdict single = IssueJudge.exponent(new double[] { 4 }, new double[] { 10 }, 0.5);
    assertThat(single.failed()).isTrue();
    assertThat(single.reason()).contains("at least 2");
    final IssueJudge.Verdict equal = IssueJudge.exponent(new double[] { 4, 4 }, new double[] { 10, 20 }, 0.5);
    assertThat(equal.failed()).isTrue();
    assertThat(equal.reason()).contains("all sizes are equal");
    final IssueJudge.Verdict zero = IssueJudge.exponent(new double[] { 1, 2 }, new double[] { 0, 20 }, 0.5);
    assertThat(zero.failed()).isTrue();
    assertThat(zero.reason()).contains("median 0.0");
  }

  @Test
  void ratioWithBadMediansFails() {
    final IssueJudge.Verdict nan = IssueJudge.ratio(0.0, 0.0, 3.0);
    assertThat(nan.failed()).isTrue();
    assertThat(nan.reason()).contains("invalid medians");
    final IssueJudge.Verdict inf = IssueJudge.ratio(6.0, 0.0, 3.0);
    assertThat(inf.value()).isInfinite();
    assertThat(inf.failed()).isTrue();
  }

  @Test
  void unsortedSizesGiveTheSameSlope() {
    assertThat(IssueJudge.exponent(new double[] { 4, 1, 2 }, new double[] { 40, 10, 20 })).isCloseTo(1.0, within(0.01));
    final IssueJudge.Verdict v = IssueJudge.exponent(new double[] { 4, 1, 2 }, new double[] { 40, 10, 20 }, 2.0);
    assertThat(v.judged()).isTrue();
    assertThat(v.failed()).isFalse();
  }

  private static Offset<Double> within(final double d) {
    return Offset.offset(d);
  }

  @Test
  void aSecondTimingConfirmsOrClearsAFailure() {
    final IssueJudge.Verdict failed = IssueJudge.ratio(60.0, 2.0, 3.0);
    final IssueJudge.Verdict passed = IssueJudge.ratio(6.0, 4.0, 3.0);
    assertThat(IssueJudge.confirm(failed, passed)).isEqualTo(passed);
    final IssueJudge.Verdict twice = IssueJudge.confirm(failed, IssueJudge.ratio(50.0, 2.0, 3.0));
    assertThat(twice.failed()).isTrue();
    assertThat(twice.value()).isEqualTo(25.0);
    assertThat(twice.reason()).startsWith("failed twice: " + failed.reason() + "; then ");
  }
}
