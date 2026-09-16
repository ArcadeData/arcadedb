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
 * Issue #7506 asked for a SUM request that was never offered a sample in a bucket to report the additive identity
 * rather than {@link TimeSeriesNaN#ABSENT}, on the reading that "never asked" and "asked, and nothing was real"
 * are different questions with different answers.
 * <p>
 * <b>Issue #7694 reversed that answer</b>, because PR #7676 (this issue) and PR #7662 (issue #7584) merged 32
 * seconds apart deciding it in opposite directions and left {@code main} red. Both shapes of "this request holds
 * no real sample" now answer {@link TimeSeriesNaN#ABSENT}: the two vectorized SUM implementations already answered
 * absent for an empty range, so the carve-out left the accumulator disagreeing with the ops that feed it, and no
 * production path can reach the "never asked" case in the first place - every accumulation loop offers a value to
 * every request index. {@link Issue7694AbsentSumContractTest} carries the decision and its evidence.
 * <p>
 * What survives here unchanged is this class's other half, which is issue #7089's and is not in dispute: an
 * absent sample never poisons a real total, a partial with no real sample never displaces one that has them, AVG
 * of nothing is undefined, and MIN/MAX answer absent. The assertions that pinned "untouched is zero" now pin
 * "untouched is absent", so that this class and {@link MultiColumnAggregationResultTest} cannot disagree again.
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
   * The fold keeps the accumulator when the sample is absent, and every caller seeds SUM and AVG with the absent
   * marker, so an accumulator that has been offered nothing real reads back absent (issue #7694).
   */
  @Test
  void anAbsentSampleLeavesTheAccumulatorAbsent() {
    assertThat(TimeSeriesNaN.sum(TimeSeriesNaN.ABSENT, 0, Double.NaN)).as("absent seed, absent sample").isNaN();
    assertThat(TimeSeriesNaN.sum(TimeSeriesNaN.ABSENT, 0, 5.0)).as("the first real sample replaces the seed")
        .isEqualTo(5.0);
    assertThat(TimeSeriesNaN.sum(5.0, 1, Double.NaN)).as("a real total is not disturbed").isEqualTo(5.0);
  }

  /**
   * Merging is the same rule: a partial holding no real sample is skipped, so absence survives rather than being
   * added into a zero (issue #7694).
   */
  @Test
  void mergingAnAbsentPartialIntoAnAbsentAccumulatorIsAbsent() {
    assertThat(TimeSeriesNaN.mergeSum(TimeSeriesNaN.ABSENT, 0, TimeSeriesNaN.ABSENT, 0))
        .as("neither side holds a real sample").isNaN();
    // The #7089 assertions are unaffected: a real total on either side still wins.
    assertThat(TimeSeriesNaN.mergeSum(5.0, 1, TimeSeriesNaN.ABSENT, 0)).isEqualTo(5.0);
    assertThat(TimeSeriesNaN.mergeSum(TimeSeriesNaN.ABSENT, 0, 5.0, 1)).isEqualTo(5.0);
  }

  // ---- accumulate(bucketTs, requestIndex, value): the reported entry point ----

  @Test
  void mapModeUntouchedSumIsAbsentWhenAnotherRequestTouchedTheBucket() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount());
    result.accumulate(1000L, 1, 7.0);

    assertThat(result.getValue(1000L, 0)).as("SUM holding no real sample is absent (issue #7694)").isNaN();
    assertThat(result.getCount(1000L, 0)).isZero();
    assertThat(result.getValue(1000L, 1)).as("COUNT is real").isEqualTo(1.0);
  }

  @Test
  void flatModeUntouchedSumIsAbsentWhenAnotherRequestTouchedTheBucket() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount(), 0L, 1000L, 16);
    result.accumulate(1000L, 1, 7.0);

    assertThat(result.isFlatMode()).isTrue();
    assertThat(result.getValue(1000L, 0)).isNaN();
    assertThat(result.getValue(1000L, 1)).isEqualTo(1.0);
  }

  /**
   * A bucket outside the pre-allocated flat window is parked in the overflow map (issue #6937), which seeds its
   * accumulators through the same helper and must answer the same way.
   */
  @Test
  void flatModeOverflowBucketUntouchedSumIsAbsent() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount(), 0L, 1000L, 2);
    result.accumulate(5000L, 1, 7.0);

    assertThat(result.isFlatMode()).isTrue();
    assertThat(result.getOverflowBucketCount()).as("the bucket fell outside the window").isEqualTo(1);
    assertThat(result.getValue(5000L, 0)).isNaN();
    assertThat(result.getValue(5000L, 1)).isEqualTo(1.0);
  }

  /**
   * A bucket NO request ever touched is a third question, and it is not about the seed at all: there is no
   * accumulator to read, and {@code getValue} answers a plain zero for an unknown bucket in either mode. That is
   * why every assertion above establishes the bucket exists before reading an absent answer out of it.
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
  void singleStatAnswersAbsentForAnUntouchedRequestAndForAnAbsentOneAlike() {
    final MultiColumnAggregationResult untouched = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    untouched.accumulateSingleStat(0L, 1, 3.0, 3);
    assertThat(untouched.getValue(0L, 0)).as("the SUM request was never asked").isNaN();

    final MultiColumnAggregationResult absent = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    absent.accumulateSingleStat(0L, 0, TimeSeriesNaN.ABSENT, 0);
    absent.accumulateSingleStat(0L, 1, 3.0, 3);
    assertThat(absent.getValue(0L, 0)).as("the SUM request was asked and saw nothing real").isNaN();
  }

  // ---- merging ----

  @Test
  void mergingFlatResultsPropagatesAbsence() {
    final MultiColumnAggregationResult untouchedLeft = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    final MultiColumnAggregationResult untouchedRight = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    untouchedRight.accumulate(0L, 1, 7.0);
    untouchedLeft.mergeFrom(untouchedRight);
    assertThat(untouchedLeft.getValue(0L, 0)).as("neither side ever asked the SUM request").isNaN();
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

    assertThat(left.getValue(1000L, 0)).isNaN();
    assertThat(left.getValue(1000L, 1)).isEqualTo(1.0);

    final MultiColumnAggregationResult absentLeft = new MultiColumnAggregationResult(sumThenCount());
    final MultiColumnAggregationResult absentRight = new MultiColumnAggregationResult(sumThenCount());
    absentRight.accumulate(1000L, 0, Double.NaN);
    absentLeft.mergeFrom(absentRight);
    assertThat(absentLeft.getValue(1000L, 0)).isNaN();
  }

  // ---- AVG does not follow SUM to zero ----

  /**
   * SUM and AVG answer alike (issue #7694): a request offered no real sample is a gap for both, so a chart draws
   * one rather than a line at zero on the SUM series and a gap on the AVG series beside it.
   */
  @Test
  void anUntouchedSumAndAvgAreBothAbsent() {
    final MultiColumnAggregationResult mapMode = new MultiColumnAggregationResult(sumAndAvg());
    mapMode.accumulate(1000L, 2, 7.0);
    mapMode.finalizeAvg();
    assertThat(mapMode.getValue(1000L, 0)).as("SUM over no real sample is absent").isNaN();
    assertThat(mapMode.getValue(1000L, 1)).as("AVG over no real sample is absent").isNaN();

    final MultiColumnAggregationResult flatMode = new MultiColumnAggregationResult(sumAndAvg(), 0L, 1000L, 16);
    flatMode.accumulate(1000L, 2, 7.0);
    flatMode.finalizeAvg();
    assertThat(flatMode.getValue(1000L, 0)).isNaN();
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
