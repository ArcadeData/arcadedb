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
 * Issue #7694: the one absent-SUM/AVG contract, driven through every entry point that can decide it.
 * <p>
 * A SUM or AVG request holding no real sample in a bucket answers {@link TimeSeriesNaN#ABSENT}, whether it was
 * offered only absent samples or offered none at all. Issue #7506 briefly made those two cases differ, seeding SUM
 * at the additive identity so a request nothing was ever offered could answer the empty sum; #7694 removed the
 * distinction, because no query produces that state (only the single-request
 * {@link MultiColumnAggregationResult#accumulate(long, int, double)} constructs it, as the tests below do, and it
 * has no caller in {@code src/main}) and because it disagreed with SQL, where {@code SUM} is NULL over an empty
 * group and over an all-NULL group alike.
 * <p>
 * Every entry point issue #7506's coverage table listed is still driven below; what changed is the answer each one
 * asserts. The #7089 half - a request that was offered samples and found none real - is unchanged, and is kept here
 * alongside so the two can no longer drift apart.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7694">issue #7694</a>
 */
class Issue7694AbsentSumAvgContractTest {

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

  /**
   * Asserting the bucket is really there is what makes an absent answer read afterwards evidence about the SEED
   * that {@code newInitializedValues()} laid down for that request index, rather than about the bucket's
   * existence - which, since issue #7698, answers absent as well.
   */
  private static void assertBucketExists(final MultiColumnAggregationResult result, final long bucketTs) {
    assertThat(result.getBucketTimestamps())
        .as("the bucket must exist, or an absent answer proves nothing about the per-request seed")
        .contains(bucketTs);
  }

  // ---- the fold itself ----

  /**
   * The fold leaves an absent sample where it found the accumulator, which is why the SEED carries the whole
   * contract: with the absent seed "nothing real arrived" IS the value, and with a numeric seed the fold would
   * hand that number straight back. The last assertion is the one that would have to change for SUM to answer
   * zero again, and it is here so that it cannot change quietly.
   */
  @Test
  void anAbsentSampleLeavesTheAccumulatorWhereItFoundIt() {
    assertThat(TimeSeriesNaN.sum(TimeSeriesNaN.ABSENT, 0, Double.NaN)).as("absent seed, absent sample: absent")
        .isNaN();
    assertThat(TimeSeriesNaN.sum(TimeSeriesNaN.ABSENT, 0, 5.0)).as("the first real sample replaces the seed")
        .isEqualTo(5.0);
    assertThat(TimeSeriesNaN.sum(5.0, 1, Double.NaN)).as("a real total is not disturbed").isEqualTo(5.0);
    assertThat(TimeSeriesNaN.sum(0.0, 0, Double.NaN))
        .as("the fold does not manufacture an absence: a numeric seed survives, so the seed is the contract")
        .isEqualTo(0.0);
  }

  /**
   * Merging is the same rule: a partial with no real sample behind it is skipped whatever its value, so the
   * running accumulator - absent until a real sample reaches it - is what comes back.
   */
  @Test
  void mergingAPartialWithNoRealSampleLeavesTheAccumulator() {
    assertThat(TimeSeriesNaN.mergeSum(TimeSeriesNaN.ABSENT, 0, TimeSeriesNaN.ABSENT, 0)).as("absent + absent")
        .isNaN();
    assertThat(TimeSeriesNaN.mergeSum(TimeSeriesNaN.ABSENT, 0, 5.0, 1)).as("a real partial replaces an absent run")
        .isEqualTo(5.0);
    assertThat(TimeSeriesNaN.mergeSum(5.0, 1, TimeSeriesNaN.ABSENT, 0)).as("a real total is not disturbed")
        .isEqualTo(5.0);
    assertThat(TimeSeriesNaN.mergeSum(2.0, 1, 3.0, 1)).as("two totals are added").isEqualTo(5.0);
  }

  // ---- accumulate(bucketTs, requestIndex, value): the state the two PRs disagreed about ----

  @Test
  void mapModeUntouchedSumIsAbsentWhenAnotherRequestTouchedTheBucket() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount());
    result.accumulate(1000L, 1, 7.0);

    assertBucketExists(result, 1000L);
    assertThat(result.getValue(1000L, 0)).as("a SUM holding no real sample is absent, not a total of zero").isNaN();
    assertThat(result.getCount(1000L, 0)).isZero();
    assertThat(result.getValue(1000L, 1)).as("COUNT still counts rows").isEqualTo(1.0);
  }

  @Test
  void flatModeUntouchedSumIsAbsentWhenAnotherRequestTouchedTheBucket() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount(), 0L, 1000L, 16);
    result.accumulate(1000L, 1, 7.0);

    assertThat(result.isFlatMode()).isTrue();
    assertBucketExists(result, 1000L);
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
    assertBucketExists(result, 5000L);
    assertThat(result.getValue(5000L, 0)).isNaN();
    assertThat(result.getValue(5000L, 1)).isEqualTo(1.0);
  }

  /**
   * A bucket NO request ever touched used to be the one residue of this decision - it answered {@code 0.0} for
   * every aggregate, SUM and MIN alike, from an arm of {@code getValue} that predates the whole policy. Issue
   * #7698 closed it: the bucket that does not exist now answers absent like the request that was offered nothing,
   * because they are the same question. It is still absent from {@code getBucketTimestamps()}, which is why no
   * reader in {@code src/main} could observe either answer.
   *
   * @see Issue7698AbsentBucketThatDoesNotExistTest
   */
  @Test
  void aBucketNoRequestEverTouchedAnswersAbsentAndIsNotInTheTimestamps() {
    final MultiColumnAggregationResult mapMode = new MultiColumnAggregationResult(sumThenCount());
    mapMode.accumulate(1000L, 1, 7.0);
    assertThat(mapMode.getBucketTimestamps()).as("no reader can reach 2000L: it is not a bucket")
        .doesNotContain(2000L);
    assertThat(mapMode.getValue(2000L, 0)).isNaN();

    final MultiColumnAggregationResult flatMode = new MultiColumnAggregationResult(sumThenCount(), 0L, 1000L, 16);
    flatMode.accumulate(1000L, 1, 7.0);
    assertThat(flatMode.getBucketTimestamps()).doesNotContain(2000L);
    assertThat(flatMode.getValue(2000L, 0)).isNaN();
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
   * zero means the block's column carried nothing real, and the merge must not let the seed show through.
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

  /**
   * The vectorized segment path, and the decision itself in one test: a request that was never asked and one that
   * was asked and saw nothing real now answer the SAME thing. That agreement is what #7694 bought, and the
   * assertion that would go red if the seed moved back.
   */
  @Test
  void singleStatAnswersAbsentForAnUntouchedRequestAndForAnAbsentOneAlike() {
    final MultiColumnAggregationResult untouched = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    untouched.accumulateSingleStat(0L, 1, 3.0, 3);
    assertBucketExists(untouched, 0L);
    assertThat(untouched.getValue(0L, 0)).as("the SUM request was never asked").isNaN();

    final MultiColumnAggregationResult absent = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    absent.accumulateSingleStat(0L, 0, TimeSeriesNaN.ABSENT, 0);
    absent.accumulateSingleStat(0L, 1, 3.0, 3);
    assertThat(absent.getValue(0L, 0)).as("the SUM request was asked and saw nothing real").isNaN();
  }

  /**
   * A SUM that really did total to zero must stay the number zero. This is the distinction the absent marker
   * exists to make, and the reason "absent" is not simply "zero by another name".
   */
  @Test
  void aSumOfRealSamplesThatCancelToZeroIsStillTheNumberZero() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAndAvg(), 0L, HOUR, 2);
    result.accumulateRow(0L, new double[] { 2.5, 2.5, 1.0 });
    result.accumulateRow(0L, new double[] { -2.5, -2.5, 1.0 });
    result.finalizeAvg();

    assertThat(result.getValue(0L, 0)).as("a real total of zero is data, not a gap").isEqualTo(0.0);
    assertThat(result.getValue(0L, 1)).isEqualTo(0.0);
    assertThat(result.getCount(0L, 0)).isEqualTo(2);
  }

  // ---- merging ----

  @Test
  void mergingFlatResultsKeepsAnEmptySumAbsentOnBothSides() {
    final MultiColumnAggregationResult untouchedLeft = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    final MultiColumnAggregationResult untouchedRight = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    untouchedRight.accumulate(0L, 1, 7.0);
    untouchedLeft.mergeFrom(untouchedRight);
    assertBucketExists(untouchedLeft, 0L);
    assertThat(untouchedLeft.getValue(0L, 0)).as("neither side ever asked the SUM request").isNaN();
    assertThat(untouchedLeft.getValue(0L, 1)).isEqualTo(1.0);

    final MultiColumnAggregationResult absentLeft = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    final MultiColumnAggregationResult absentRight = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    absentRight.accumulate(0L, 0, Double.NaN);
    absentLeft.mergeFrom(absentRight);
    assertThat(absentLeft.getValue(0L, 0)).as("an absent partial leaves the merged sum absent").isNaN();

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

    assertBucketExists(left, 1000L);
    assertThat(left.getValue(1000L, 0)).isNaN();
    assertThat(left.getValue(1000L, 1)).isEqualTo(1.0);

    final MultiColumnAggregationResult absentLeft = new MultiColumnAggregationResult(sumThenCount());
    final MultiColumnAggregationResult absentRight = new MultiColumnAggregationResult(sumThenCount());
    absentRight.accumulate(1000L, 0, Double.NaN);
    absentLeft.mergeFrom(absentRight);
    assertThat(absentLeft.getValue(1000L, 0)).isNaN();
  }

  // ---- the other three aggregates ----

  /**
   * SUM and AVG now answer alike for an empty request, which is the agreement issue #7506's seed gave up. COUNT
   * is the one aggregate outside the policy and still answers the number zero.
   */
  @Test
  void anUntouchedSumAndAnUntouchedAvgBothStayAbsentWhileCountStaysZero() {
    final MultiColumnAggregationResult mapMode = new MultiColumnAggregationResult(sumAndAvg());
    mapMode.accumulate(1000L, 2, 7.0);
    mapMode.finalizeAvg();
    assertBucketExists(mapMode, 1000L);
    assertThat(mapMode.getValue(1000L, 0)).as("SUM of nothing is absent").isNaN();
    assertThat(mapMode.getValue(1000L, 1)).as("AVG of nothing is absent").isNaN();

    final MultiColumnAggregationResult flatMode = new MultiColumnAggregationResult(sumAndAvg(), 0L, 1000L, 16);
    flatMode.accumulate(1000L, 2, 7.0);
    flatMode.finalizeAvg();
    assertThat(flatMode.getValue(1000L, 0)).isNaN();
    assertThat(flatMode.getValue(1000L, 1)).isNaN();

    // The counter-case, so "absent" cannot quietly become "everything empty is absent".
    final MultiColumnAggregationResult countEmpty = new MultiColumnAggregationResult(sumThenCount());
    countEmpty.accumulate(1000L, 0, 7.0);
    assertThat(countEmpty.getValue(1000L, 1)).as("COUNT of no rows is 0, not absent").isEqualTo(0.0);
  }

  /**
   * MIN/MAX keep the answer issue #4596 needed: absent, whether the request was asked or not, because there is no
   * identity element to fall back on.
   */
  @Test
  void anUntouchedMinOrMaxStaysAbsent() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(
        List.of(new MultiColumnAggregationRequest(1, AggregationType.MIN, "minA"),
            new MultiColumnAggregationRequest(1, AggregationType.MAX, "maxA"),
            new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB")));
    result.accumulate(1000L, 2, 7.0);

    assertBucketExists(result, 1000L);
    assertThat(result.getValue(1000L, 0)).isNaN();
    assertThat(result.getValue(1000L, 1)).isNaN();
  }
}
