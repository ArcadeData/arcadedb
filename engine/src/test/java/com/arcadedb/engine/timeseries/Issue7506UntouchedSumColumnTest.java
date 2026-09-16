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
package com.arcadedb.engine.timeseries;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7506: a SUM request that was never offered a sample in a bucket reported {@link TimeSeriesNaN#ABSENT}
 * instead of zero.
 * <p>
 * Issue #7089 put SUM/AVG under the NaN-as-absent policy by seeding their accumulators with {@code ABSENT}, which
 * made "the request was never asked anything" indistinguishable from "the request was asked and every sample was
 * absent". The two are different questions and have different answers: the empty sum is the additive identity, and
 * a window whose samples were all absent is a measurement gap. This test pins both halves, and pins that the AVG
 * of nothing stays absent rather than joining SUM at zero - an empty average is undefined, not zero.
 * <p>
 * The reported repro is {@link MultiColumnAggregationResultTest#emptySumAndCountStayZeroNotNaN()}, which the
 * #7089 change turned red on main. Everything below drives the same invariant through the other entry points.
 */
class Issue7506UntouchedSumColumnTest {

  private static final long HOUR = 3_600_000L;

  private static List<MultiColumnAggregationRequest> sumThenCount() {
    return List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sumA"),
        new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB"));
  }

  private static List<MultiColumnAggregationRequest> sumAndAvg() {
    return List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sumA"),
        new MultiColumnAggregationRequest(1, AggregationType.AVG, "avgA"),
        new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB"));
  }

  // ---- the fold itself ----

  /**
   * The fold turns "untouched" into "absent" the moment an absent sample reaches it, so a zero seed can still
   * answer the #7089 question. Every pre-existing caller seeds at ABSENT, for which this is a no-op.
   */
  @Test
  void anAbsentSampleTurnsTheUntouchedAccumulatorAbsent() {
    assertThat(TimeSeriesNaN.sum(0.0, 0, Double.NaN)).as("untouched, then an absent sample: absent").isNaN();
    assertThat(TimeSeriesNaN.sum(TimeSeriesNaN.ABSENT, 0, Double.NaN)).as("unchanged for an ABSENT seed").isNaN();
    assertThat(TimeSeriesNaN.sum(0.0, 0, 5.0)).as("the first real sample replaces the seed").isEqualTo(5.0);
    assertThat(TimeSeriesNaN.sum(5.0, 1, Double.NaN)).as("a real total is not disturbed").isEqualTo(5.0);
  }

  /**
   * Merging is the same rule, and absence propagates through the addition: untouched + untouched is untouched,
   * untouched + absent is absent.
   */
  @Test
  void mergingAnAbsentPartialIntoAnUntouchedAccumulatorIsAbsent() {
    assertThat(TimeSeriesNaN.mergeSum(0.0, 0, 0.0, 0)).as("untouched + untouched").isEqualTo(0.0);
    assertThat(TimeSeriesNaN.mergeSum(0.0, 0, TimeSeriesNaN.ABSENT, 0)).as("untouched + absent").isNaN();
    assertThat(TimeSeriesNaN.mergeSum(TimeSeriesNaN.ABSENT, 0, 0.0, 0)).as("absent + untouched").isNaN();
    // The #7089 assertions are untouched by the new branch: a real total on either side still wins.
    assertThat(TimeSeriesNaN.mergeSum(5.0, 1, TimeSeriesNaN.ABSENT, 0)).isEqualTo(5.0);
    assertThat(TimeSeriesNaN.mergeSum(TimeSeriesNaN.ABSENT, 0, 5.0, 1)).isEqualTo(5.0);
  }

  // ---- accumulate(bucketTs, requestIndex, value): the reported entry point ----

  @Test
  void mapModeUntouchedSumIsZeroWhenAnotherRequestTouchedTheBucket() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount());
    result.accumulate(1000L, 1, 7.0);

    assertThat(result.getValue(1000L, 0)).as("SUM of nothing is the additive identity").isEqualTo(0.0);
    assertThat(result.getCount(1000L, 0)).isZero();
    assertThat(result.getValue(1000L, 1)).as("COUNT is real").isEqualTo(1.0);
  }

  @Test
  void flatModeUntouchedSumIsZeroWhenAnotherRequestTouchedTheBucket() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount(), 0L, 1000L, 16);
    result.accumulate(1000L, 1, 7.0);

    assertThat(result.isFlatMode()).isTrue();
    assertThat(result.getValue(1000L, 0)).isEqualTo(0.0);
    assertThat(result.getValue(1000L, 1)).isEqualTo(1.0);
  }

  /**
   * A bucket outside the pre-allocated flat window is parked in the overflow map (issue #6937), which seeds its
   * accumulators through the same helper and must answer the same way.
   */
  @Test
  void flatModeOverflowBucketUntouchedSumIsZero() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount(), 0L, 1000L, 2);
    result.accumulate(5000L, 1, 7.0);

    assertThat(result.isFlatMode()).isTrue();
    assertThat(result.getOverflowBucketCount()).as("the bucket fell outside the window").isEqualTo(1);
    assertThat(result.getValue(5000L, 0)).isEqualTo(0.0);
    assertThat(result.getValue(5000L, 1)).isEqualTo(1.0);
  }

  /**
   * A bucket nothing ever touched already answered zero, and still does: the two "no data" shapes agree.
   */
  @Test
  void aBucketNoRequestEverTouchedIsZero() {
    final MultiColumnAggregationResult mapMode = new MultiColumnAggregationResult(sumThenCount());
    mapMode.accumulate(1000L, 1, 7.0);
    assertThat(mapMode.getValue(2000L, 0)).isEqualTo(0.0);

    final MultiColumnAggregationResult flatMode = new MultiColumnAggregationResult(sumThenCount(), 0L, 1000L, 16);
    flatMode.accumulate(1000L, 1, 7.0);
    assertThat(flatMode.getValue(2000L, 0)).isEqualTo(0.0);
  }

  // ---- the #7089 half: offered, but nothing real ----

  @Test
  void aSumOfferedOnlyAbsentSamplesStaysAbsentInBothModes() {
    final MultiColumnAggregationResult mapMode = new MultiColumnAggregationResult(sumThenCount());
    mapMode.accumulate(1000L, 0, Double.NaN);
    assertThat(mapMode.getValue(1000L, 0)).as("offered an absent sample: absent, not zero").isNaN();
    assertThat(mapMode.getCount(1000L, 0)).isZero();

    final MultiColumnAggregationResult flatMode = new MultiColumnAggregationResult(sumThenCount(), 0L, 1000L, 16);
    flatMode.accumulate(1000L, 0, Double.NaN);
    assertThat(flatMode.getValue(1000L, 0)).isNaN();
  }

  @Test
  void accumulateRowOfAbsentSamplesKeepsTheSumAbsent() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAndAvg(), 0L, HOUR, 2);
    result.accumulateRow(0L, new double[] { Double.NaN, Double.NaN, 1.0 });
    result.accumulateRow(0L, new double[] { Double.NaN, Double.NaN, 1.0 });
    result.finalizeAvg();

    assertThat(result.getValue(0L, 0)).as("all-absent SUM").isNaN();
    assertThat(result.getValue(0L, 1)).as("all-absent AVG").isNaN();
    assertThat(result.getValue(0L, 2)).as("COUNT counts rows").isEqualTo(2.0);
  }

  /**
   * The block-statistics fast path hands over a value with the count of the real samples behind it. A count of
   * zero means the block's column was all absent, which is not the same as the request never being asked.
   */
  @Test
  void blockStatsWithAZeroRealCountKeepsTheSumAbsent() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAndAvg(), 0L, HOUR, 2);
    result.accumulateBlockStats(0L, new double[] { TimeSeriesNaN.ABSENT, TimeSeriesNaN.ABSENT, 4.0 },
        new long[] { 0, 0, 4 });
    result.finalizeAvg();

    assertThat(result.getValue(0L, 0)).isNaN();
    assertThat(result.getValue(0L, 1)).isNaN();
    assertThat(result.getValue(0L, 2)).isEqualTo(4.0);
  }

  @Test
  void singleStatDistinguishesAnUntouchedRequestFromAnAbsentOne() {
    final MultiColumnAggregationResult untouched = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    untouched.accumulateSingleStat(0L, 1, 3.0, 3);
    assertThat(untouched.getValue(0L, 0)).as("the SUM request was never asked").isEqualTo(0.0);

    final MultiColumnAggregationResult absent = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    absent.accumulateSingleStat(0L, 0, TimeSeriesNaN.ABSENT, 0);
    absent.accumulateSingleStat(0L, 1, 3.0, 3);
    assertThat(absent.getValue(0L, 0)).as("the SUM request was asked and saw nothing real").isNaN();
  }

  // ---- merging ----

  @Test
  void mergingFlatResultsPropagatesAbsenceButNotUntouchedness() {
    final MultiColumnAggregationResult untouchedLeft = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    final MultiColumnAggregationResult untouchedRight = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    untouchedRight.accumulate(0L, 1, 7.0);
    untouchedLeft.mergeFrom(untouchedRight);
    assertThat(untouchedLeft.getValue(0L, 0)).as("neither side ever asked the SUM request").isEqualTo(0.0);
    assertThat(untouchedLeft.getValue(0L, 1)).isEqualTo(1.0);

    final MultiColumnAggregationResult absentLeft = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    final MultiColumnAggregationResult absentRight = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    absentRight.accumulate(0L, 0, Double.NaN);
    absentLeft.mergeFrom(absentRight);
    assertThat(absentLeft.getValue(0L, 0)).as("an absent partial makes the merged sum absent").isNaN();

    // A real total still wins over both shapes of nothing.
    final MultiColumnAggregationResult realLeft = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    final MultiColumnAggregationResult realRight = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    realLeft.accumulate(0L, 0, 2.0);
    realRight.accumulate(0L, 0, 3.0);
    realLeft.mergeFrom(realRight);
    assertThat(realLeft.getValue(0L, 0)).isEqualTo(5.0);
  }

  @Test
  void mergingMapModeResultsAgreesWithFlatMode() {
    final MultiColumnAggregationResult left = new MultiColumnAggregationResult(sumThenCount());
    final MultiColumnAggregationResult right = new MultiColumnAggregationResult(sumThenCount());
    right.accumulate(1000L, 1, 7.0);
    left.mergeFrom(right);

    assertThat(left.getValue(1000L, 0)).isEqualTo(0.0);
    assertThat(left.getValue(1000L, 1)).isEqualTo(1.0);

    final MultiColumnAggregationResult absentLeft = new MultiColumnAggregationResult(sumThenCount());
    final MultiColumnAggregationResult absentRight = new MultiColumnAggregationResult(sumThenCount());
    absentRight.accumulate(1000L, 0, Double.NaN);
    absentLeft.mergeFrom(absentRight);
    assertThat(absentLeft.getValue(1000L, 0)).isNaN();
  }

  // ---- AVG does not follow SUM to zero ----

  /**
   * The empty sum is zero; the empty average is undefined. An AVG request never offered a sample keeps the absent
   * marker, so a chart draws a gap rather than a line at zero.
   */
  @Test
  void anUntouchedAvgStaysAbsent() {
    final MultiColumnAggregationResult mapMode = new MultiColumnAggregationResult(sumAndAvg());
    mapMode.accumulate(1000L, 2, 7.0);
    mapMode.finalizeAvg();
    assertThat(mapMode.getValue(1000L, 0)).as("SUM of nothing is zero").isEqualTo(0.0);
    assertThat(mapMode.getValue(1000L, 1)).as("AVG of nothing is undefined").isNaN();

    final MultiColumnAggregationResult flatMode = new MultiColumnAggregationResult(sumAndAvg(), 0L, 1000L, 16);
    flatMode.accumulate(1000L, 2, 7.0);
    flatMode.finalizeAvg();
    assertThat(flatMode.getValue(1000L, 0)).isEqualTo(0.0);
    assertThat(flatMode.getValue(1000L, 1)).isNaN();
  }

  /**
   * MIN/MAX keep the answer issue #4597 needed: absent, whether the request was asked or not, because there is no
   * identity element to fall back on.
   */
  @Test
  void anUntouchedMinOrMaxStaysAbsent() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(
        List.of(new MultiColumnAggregationRequest(1, AggregationType.MIN, "minA"),
            new MultiColumnAggregationRequest(1, AggregationType.MAX, "maxA"),
            new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB")));
    result.accumulate(1000L, 2, 7.0);

    assertThat(result.getValue(1000L, 0)).isNaN();
    assertThat(result.getValue(1000L, 1)).isNaN();
  }
}
