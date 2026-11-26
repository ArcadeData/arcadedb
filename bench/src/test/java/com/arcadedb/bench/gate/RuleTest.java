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
package com.arcadedb.bench.gate;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class RuleTest {

  static Point point(final double median, final double[] values, final String unit, final String better) {
    return new Point("nightly", "c", "c", null, Instant.EPOCH, Instant.EPOCH, "r", "b", "ok", median, values, unit, better);
  }

  static Point ms(final double v) {
    return point(v, new double[] { v, v }, "ms", "lower");
  }

  static Verdict.Kind cmp(final Point base, final Point n) {
    return Rule.compare(base, n);
  }

  @Test
  void bandFloorIsTenPercent() {
    assertThat(Rule.MIN_BAND).isEqualTo(0.10);
    assertThat(cmp(ms(100), ms(109))).isEqualTo(Verdict.Kind.SAME);
    assertThat(cmp(ms(100), ms(111))).isEqualTo(Verdict.Kind.WORSE);
  }

  @Test
  void bandWidensToTheSpread() {
    final Point base = point(100, new double[] { 80, 100, 120 }, "ms", "lower");
    assertThat(cmp(base, ms(135))).isEqualTo(Verdict.Kind.SAME);
    assertThat(cmp(base, ms(145))).isEqualTo(Verdict.Kind.WORSE);
  }

  @Test
  void msFloorIsOneMillisecond() {
    assertThat(Rule.absoluteFloor("ms")).isEqualTo(1.0);
    assertThat(cmp(ms(0.4), ms(0.6))).isEqualTo(Verdict.Kind.SAME);
  }

  @Test
  void secondsFloorIsHalfASecond() {
    assertThat(Rule.absoluteFloor("s")).isEqualTo(0.5);
    assertThat(cmp(point(2.0, new double[] { 2, 2 }, "s", "lower"), point(2.4, new double[] { 2.4, 2.4 }, "s", "lower"))).isEqualTo(Verdict.Kind.SAME);
    assertThat(cmp(point(4, new double[] { 4, 4 }, "s", "lower"), point(4.6, new double[] { 4.6, 4.6 }, "s", "lower"))).isEqualTo(Verdict.Kind.WORSE);
  }

  @Test
  void bytesFloorIsOneMebibyte() {
    assertThat(Rule.absoluteFloor("bytes")).isEqualTo(1048576);
    final double mib = 1048576;
    assertThat(cmp(point(4 * mib, new double[] { 4 * mib }, "bytes", "lower"), point(4.9 * mib, new double[] { 4.9 * mib }, "bytes", "lower")))
        .isEqualTo(Verdict.Kind.SAME);
    assertThat(cmp(point(4 * mib, new double[] { 4 * mib }, "bytes", "lower"), point(6 * mib, new double[] { 6 * mib }, "bytes", "lower")))
        .isEqualTo(Verdict.Kind.WORSE);
  }

  @Test
  void bandWidensToTheNewestPointsSpread() {
    final Point newest = point(130, new double[] { 110, 130, 150 }, "ms", "lower");
    assertThat(Rule.spread(130, newest.values())).isGreaterThan(0.3);
    assertThat(cmp(ms(100), newest)).isEqualTo(Verdict.Kind.SAME);
  }

  @Test
  void ratesHaveNoFloor() {
    assertThat(Rule.absoluteFloor("records/s")).isEqualTo(0);
    assertThat(Rule.absoluteFloor("ratio")).isEqualTo(0);
  }

  @Test
  void higherIsBetterFlipsTheSign() {
    final Point base = point(1000, new double[] { 1000, 1000 }, "records/s", "higher");
    assertThat(cmp(base, point(800, new double[] { 800, 800 }, "records/s", "higher"))).isEqualTo(Verdict.Kind.WORSE);
    assertThat(cmp(base, point(1200, new double[] { 1200, 1200 }, "records/s", "higher"))).isEqualTo(Verdict.Kind.BETTER);
  }

  @Test
  void lowerIsBetterImprovementIsBetter() {
    assertThat(cmp(ms(100), ms(50))).isEqualTo(Verdict.Kind.BETTER);
  }

  @Test
  void zeroBaseUsesTheFloorAlone() {
    assertThat(cmp(ms(0), ms(0.5))).isEqualTo(Verdict.Kind.SAME);
    assertThat(cmp(ms(0), ms(2))).isEqualTo(Verdict.Kind.WORSE);
    assertThat(cmp(point(0, new double[] { 0 }, "ratio", "lower"), point(0.01, new double[] { 0.01 }, "ratio", "lower")))
        .isEqualTo(Verdict.Kind.WORSE);
    assertThat(Rule.change(ms(0), ms(2))).isEqualTo(0);
  }

  @Test
  void spreadNeedsTwoValues() {
    assertThat(Rule.spread(10, new double[] { 10 })).isEqualTo(0);
    assertThat(Rule.spread(100, new double[] { 80, 100, 120 })).isEqualTo(0.4);
  }

  @Test
  void factorWobbleIsNotReported() {
    assertThat(Rule.absoluteFloor("factor")).isEqualTo(1.0);
    assertThat(cmp(factor(1.1), factor(1.9))).isEqualTo(Verdict.Kind.SAME);
  }

  @Test
  void factorJumpIsReported() {
    assertThat(cmp(factor(1.1), factor(4.0))).isEqualTo(Verdict.Kind.WORSE);
  }

  @Test
  void exponentFloor() {
    assertThat(Rule.absoluteFloor("exponent")).isEqualTo(0.25);
    assertThat(cmp(exponent(0.45), exponent(0.6))).isEqualTo(Verdict.Kind.SAME);
    assertThat(cmp(exponent(0.45), exponent(1.2))).isEqualTo(Verdict.Kind.WORSE);
  }

  @Test
  void recallRatioKeepsNoFloor() {
    assertThat(Rule.absoluteFloor("ratio")).isEqualTo(0.0);
    assertThat(cmp(point(0.99, new double[] { 0.99, 0.99 }, "ratio", "higher"), point(0.85, new double[] { 0.85, 0.85 }, "ratio", "higher")))
        .isEqualTo(Verdict.Kind.WORSE);
  }

  @Test
  void informationalUnitsAreNeverJudged() {
    assertThat(Rule.absoluteFloor("flag")).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(Rule.absoluteFloor("steps")).isEqualTo(Double.POSITIVE_INFINITY);
    // a flag from a zero baseline either way, in either direction of better
    assertThat(cmp(point(0, new double[] { 0, 0 }, "flag", "lower"), point(1, new double[] { 1, 1 }, "flag", "lower")))
        .isEqualTo(Verdict.Kind.SAME);
    assertThat(cmp(point(1, new double[] { 1, 1 }, "flag", "lower"), point(0, new double[] { 0, 0 }, "flag", "lower")))
        .isEqualTo(Verdict.Kind.SAME);
    // more steps is a higher knee, not a regression: +13% is over the 10% band of any other unit
    assertThat(cmp(point(23, new double[] { 23, 23 }, "steps", "lower"), point(26, new double[] { 26, 26 }, "steps", "lower")))
        .isEqualTo(Verdict.Kind.SAME);
    // the judged units keep their floors
    assertThat(Rule.absoluteFloor("ms")).isEqualTo(1.0);
    assertThat(cmp(factor(1.1), factor(4.0))).isEqualTo(Verdict.Kind.WORSE);
    assertThat(cmp(ms(10), ms(20))).isEqualTo(Verdict.Kind.WORSE);
  }

  private static Point factor(final double v) {
    return point(v, new double[] { v, v }, "factor", "lower");
  }

  private static Point exponent(final double v) {
    return point(v, new double[] { v, v }, "exponent", "lower");
  }
}
