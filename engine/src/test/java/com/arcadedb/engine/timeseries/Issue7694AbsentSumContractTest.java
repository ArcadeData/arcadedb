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

import com.arcadedb.engine.timeseries.simd.ScalarTimeSeriesVectorOps;
import com.arcadedb.engine.timeseries.simd.TimeSeriesVectorOps;
import com.arcadedb.engine.timeseries.simd.TimeSeriesVectorOpsProvider;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7694 - the decision record for the absent SUM/AVG contract, and the test that keeps it decided.
 * <p>
 * Two PRs merged 32 seconds apart answered the same question in opposite directions and turned {@code main} red:
 * PR #7662 (issue #7584) pinned "a SUM request that received no real sample in a bucket is ABSENT", while PR #7676
 * (issue #7506) reseeded the accumulator so the same case answered {@code 0.0}, on the rationale that the empty sum
 * is the additive identity. The decision recorded here is #7584's: <b>a SUM or AVG request holding no real sample
 * for a bucket reads back {@link TimeSeriesNaN#ABSENT}, whether it was offered only absent samples or offered
 * nothing at all.</b> {@code 0.0} means a total of real samples that cancelled to zero, and COUNT.
 * <p>
 * The evidence, rather than a preference between two defensible readings:
 * <ol>
 *   <li>Both vectorized SUM implementations already answer ABSENT for an empty range - the literal empty sum - and
 *       say so in their own comments. A {@code 0.0} seed here left two layers of one subsystem disagreeing about
 *       the empty sum, which is the defect class {@link TimeSeriesNaN} exists to remove. Pinned by
 *       {@link #theVectorOpsEmptySumAgreesWithTheAccumulator()}.</li>
 *   <li>The distinction #7506 drew - "offered nothing" versus "offered only absent samples" - is not reachable
 *       through any production path. Every accumulation loop in {@code TimeSeriesSealedStore} and
 *       {@code TimeSeriesEngine} walks {@code r = 0 .. reqCount} and offers each request a value, so a request
 *       cannot stay untouched in a bucket a sibling created. The only API that can leave one untouched is the
 *       three-argument {@code accumulate(bucketTs, requestIndex, value)}, which no production code calls.</li>
 *   <li>AVG, MIN and MAX in this same class answer ABSENT for the same shape of nothing, so a chart draws four
 *       gaps rather than three gaps and one line at zero, and SQL's {@code SUM} over zero rows is NULL - which is
 *       what {@code AggregateFromTimeSeriesStep} serializes an absent push-down total to.</li>
 * </ol>
 * Every accumulation entry point that production actually uses is driven below, so the contract cannot be
 * reversed again by a change to one path alone.
 */
class Issue7694AbsentSumContractTest {

  private static final long HOUR   = 3_600_000L;
  private static final long BUCKET = 1000L;

  private static List<MultiColumnAggregationRequest> sumThenCount() {
    return List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sumA"),
        new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB"));
  }

  private static List<MultiColumnAggregationRequest> sumAvgCount() {
    return List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sumA"),
        new MultiColumnAggregationRequest(1, AggregationType.AVG, "avgA"),
        new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB"));
  }

  /**
   * An absent answer read out of a bucket that was never created proves nothing about the seed, because
   * {@code getValue} answers a plain {@code 0.0} for an unknown bucket. Every "this request held nothing" case
   * below asserts the bucket is really there first.
   */
  private static void assertBucketExists(final MultiColumnAggregationResult result, final long bucketTs) {
    assertThat(result.getBucketTimestamps())
        .as("the bucket must exist, or an absent answer proves nothing about the per-request seed")
        .contains(bucketTs);
  }

  // ---- the claim the decision rests on: the two layers agree about the empty sum ----

  /**
   * The reason ABSENT wins rather than zero. {@code ScalarTimeSeriesVectorOps} and the SIMD implementation behind
   * {@code TimeSeriesVectorOpsProvider} both answer the absent marker for an empty range and for an all-absent
   * one, and {@code TimeSeriesSealedStore}'s vectorized path feeds exactly that value into the accumulator. An
   * accumulator that seeded SUM at zero answered a different empty sum than the ops it consumes.
   */
  @Test
  void theVectorOpsEmptySumAgreesWithTheAccumulator() {
    final double[] allAbsent = { Double.NaN, Double.NaN, Double.NaN };

    final TimeSeriesVectorOps scalar = new ScalarTimeSeriesVectorOps();
    assertThat(scalar.sum(allAbsent, 0, 0)).as("the empty range is the literal empty sum").isNaN();
    assertThat(scalar.sum(allAbsent, 0, 3)).as("an all-absent range").isNaN();

    final TimeSeriesVectorOps active = TimeSeriesVectorOpsProvider.getInstance();
    assertThat(active.sum(allAbsent, 0, 0)).as("whichever implementation this JVM selected").isNaN();
    assertThat(active.sum(allAbsent, 0, 3)).isNaN();
    assertThat(active.countPresent(allAbsent, 0, 3)).isZero();

    // And the accumulator that consumes those values answers the same thing.
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    result.accumulateSingleStat(0L, 0, active.sum(allAbsent, 0, 3), active.countPresent(allAbsent, 0, 3));
    assertBucketExists(result, 0L);
    assertThat(result.getValue(0L, 0)).as("the accumulator must not turn the ops' absence into a zero").isNaN();
  }

  // ---- the fold ----

  @Test
  void theFoldAnswersAbsentWhenNoRealSampleReachedIt() {
    assertThat(TimeSeriesNaN.sum(TimeSeriesNaN.ABSENT, 0, Double.NaN)).as("absent seed, absent sample").isNaN();
    assertThat(TimeSeriesNaN.sum(TimeSeriesNaN.ABSENT, 0, 5.0)).as("the first real sample replaces the seed")
        .isEqualTo(5.0);
    assertThat(TimeSeriesNaN.sum(5.0, 1, Double.NaN)).as("a real total is not disturbed by an absent sample")
        .isEqualTo(5.0);
    assertThat(TimeSeriesNaN.sum(5.0, 1, 2.0)).isEqualTo(7.0);
  }

  @Test
  void mergingAnAbsentPartialLeavesTheRunningTotalAlone() {
    assertThat(TimeSeriesNaN.mergeSum(TimeSeriesNaN.ABSENT, 0, TimeSeriesNaN.ABSENT, 0))
        .as("neither side holds a real sample").isNaN();
    assertThat(TimeSeriesNaN.mergeSum(5.0, 1, TimeSeriesNaN.ABSENT, 0))
        .as("a real total survives an absent partial").isEqualTo(5.0);
    assertThat(TimeSeriesNaN.mergeSum(TimeSeriesNaN.ABSENT, 0, 5.0, 1))
        .as("a real partial replaces an absent running total").isEqualTo(5.0);
    assertThat(TimeSeriesNaN.mergeSum(2.0, 1, 3.0, 1)).isEqualTo(5.0);
  }

  // ---- accumulateRow: TimeSeriesEngine:659,706 and TimeSeriesSealedStore:1144 ----

  @Test
  void accumulateRowOfAbsentSamplesAnswersAbsentForSumAndAvg() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount(), 0L, HOUR, 2);
    result.accumulateRow(0L, new double[] { Double.NaN, Double.NaN, 1.0 });
    result.accumulateRow(0L, new double[] { Double.NaN, Double.NaN, 1.0 });
    result.finalizeAvg();

    assertBucketExists(result, 0L);
    assertThat(result.getValue(0L, 0)).as("SUM over no real sample").isNaN();
    assertThat(result.getValue(0L, 1)).as("AVG over no real sample").isNaN();
    assertThat(result.getValue(0L, 2)).as("COUNT counts rows and is never absent").isEqualTo(2.0);
  }

  // ---- accumulateBlockStats: TimeSeriesSealedStore:1004 ----

  @Test
  void blockStatsWithNoRealSampleAnswersAbsent() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount(), 0L, HOUR, 2);
    result.accumulateBlockStats(0L, new double[] { TimeSeriesNaN.ABSENT, TimeSeriesNaN.ABSENT, 4.0 },
        new long[] { 0, 0, 4 });
    result.finalizeAvg();

    assertBucketExists(result, 0L);
    assertThat(result.getValue(0L, 0)).isNaN();
    assertThat(result.getValue(0L, 1)).isNaN();
    assertThat(result.getValue(0L, 2)).isEqualTo(4.0);
  }

  // ---- accumulateSingleStat: TimeSeriesSealedStore:1081,1084,1112,1124 ----

  /**
   * The decision in one test. Both shapes of "this request holds no real sample" answer the same thing, so no
   * caller has to know which one it is looking at - and the engine could not tell it apart anyway, because every
   * one of its loops offers a value to every request index.
   */
  @Test
  void singleStatAnswersAbsentWhetherTheRequestWasOfferedNothingOrOnlyAbsentSamples() {
    final MultiColumnAggregationResult neverOffered = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    neverOffered.accumulateSingleStat(0L, 1, 3.0, 3);
    assertBucketExists(neverOffered, 0L);
    assertThat(neverOffered.getValue(0L, 0)).as("the SUM request was never offered a sample").isNaN();
    assertThat(neverOffered.getCount(0L, 0)).isZero();
    assertThat(neverOffered.getValue(0L, 1)).as("COUNT is real").isEqualTo(3.0);

    final MultiColumnAggregationResult offeredAbsent = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    offeredAbsent.accumulateSingleStat(0L, 0, TimeSeriesNaN.ABSENT, 0);
    offeredAbsent.accumulateSingleStat(0L, 1, 3.0, 3);
    assertBucketExists(offeredAbsent, 0L);
    assertThat(offeredAbsent.getValue(0L, 0)).as("the SUM request saw nothing real").isNaN();

    // Double.compare, not isEqualTo: the two values ARE both NaN, and NaN == NaN is false.
    assertThat(Double.compare(neverOffered.getValue(0L, 0), offeredAbsent.getValue(0L, 0)))
        .as("the two shapes of nothing answer alike, so no caller has to tell them apart").isZero();
  }

  // ---- the untouched request, through every mode ----

  @Test
  void anUntouchedSumIsAbsentInMapMode() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount());
    result.accumulate(BUCKET, 1, 7.0);

    assertBucketExists(result, BUCKET);
    assertThat(result.getValue(BUCKET, 0)).as("SUM holding no real sample is absent, not a total of zero").isNaN();
    assertThat(result.getCount(BUCKET, 0)).isZero();
    assertThat(result.getValue(BUCKET, 1)).as("COUNT still counts rows").isEqualTo(1.0);
  }

  @Test
  void anUntouchedSumIsAbsentInFlatMode() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount(), 0L, BUCKET, 16);
    result.accumulate(BUCKET, 1, 7.0);

    assertThat(result.isFlatMode()).isTrue();
    assertBucketExists(result, BUCKET);
    assertThat(result.getValue(BUCKET, 0)).isNaN();
    assertThat(result.getValue(BUCKET, 1)).isEqualTo(1.0);
  }

  /**
   * A bucket that falls outside the pre-allocated flat window is parked in the overflow map (issue #6937), whose
   * accumulators come from the same seeding helper and must answer the same way.
   */
  @Test
  void anUntouchedSumIsAbsentInAnOverflowBucket() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount(), 0L, BUCKET, 2);
    result.accumulate(5000L, 1, 7.0);

    assertThat(result.isFlatMode()).isTrue();
    assertThat(result.getOverflowBucketCount()).as("the bucket fell outside the window").isEqualTo(1);
    assertBucketExists(result, 5000L);
    assertThat(result.getValue(5000L, 0)).isNaN();
    assertThat(result.getValue(5000L, 1)).isEqualTo(1.0);
  }

  // ---- mergeFrom: TimeSeriesEngine:638, the per-shard merge ----

  @Test
  void mergingFlatShardResultsAnswersAbsent() {
    final MultiColumnAggregationResult left = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    final MultiColumnAggregationResult right = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    right.accumulateSingleStat(0L, 1, 3.0, 3);
    left.mergeFrom(right);

    assertBucketExists(left, 0L);
    assertThat(left.getValue(0L, 0)).as("no shard held a real sample for the SUM request").isNaN();
    assertThat(left.getValue(0L, 1)).isEqualTo(3.0);

    // A real total on either side still wins over absence.
    final MultiColumnAggregationResult realLeft = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    final MultiColumnAggregationResult realRight = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    realLeft.accumulateSingleStat(0L, 0, 2.0, 1);
    realRight.accumulateSingleStat(0L, 0, 3.0, 1);
    realLeft.mergeFrom(realRight);
    assertThat(realLeft.getValue(0L, 0)).isEqualTo(5.0);
  }

  @Test
  void mergingMapModeResultsAgreesWithFlatMode() {
    final MultiColumnAggregationResult left = new MultiColumnAggregationResult(sumThenCount());
    final MultiColumnAggregationResult right = new MultiColumnAggregationResult(sumThenCount());
    right.accumulate(BUCKET, 1, 7.0);
    left.mergeFrom(right);

    assertBucketExists(left, BUCKET);
    assertThat(left.getValue(BUCKET, 0)).isNaN();
    assertThat(left.getValue(BUCKET, 1)).isEqualTo(1.0);
  }

  // ---- the distinction the absent marker exists to make ----

  @Test
  void aSumOfRealSamplesThatCancelsToZeroIsTheNumberZero() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount());
    result.accumulate(BUCKET, 0, 3.0);
    result.accumulate(BUCKET, 0, -3.0);

    assertThat(result.getValue(BUCKET, 0)).as("a real total of zero is data, not a gap").isEqualTo(0.0);
    assertThat(result.getCount(BUCKET, 0)).isEqualTo(2);
  }

  @Test
  void aPopulatedSumSkipsTheAbsentSamplesRatherThanBeingPoisonedByThem() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount());
    result.accumulate(BUCKET, 0, 4.0);
    result.accumulate(BUCKET, 0, Double.NaN);
    result.accumulate(BUCKET, 0, 2.5);

    assertThat(result.getValue(BUCKET, 0)).isEqualTo(6.5);
    assertThat(result.getCount(BUCKET, 0)).isEqualTo(2);
  }

  /**
   * The consistency claim the decision is made for: the four aggregates that skip absent samples answer alike, and
   * COUNT is the only one that reports "nothing here" as the number zero.
   */
  @Test
  void sumAvgMinAndMaxAllAnswerAbsentTogetherAndOnlyCountAnswersZero() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(
        List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sumA"),
            new MultiColumnAggregationRequest(1, AggregationType.AVG, "avgA"),
            new MultiColumnAggregationRequest(1, AggregationType.MIN, "minA"),
            new MultiColumnAggregationRequest(1, AggregationType.MAX, "maxA"),
            new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB")));
    result.accumulateRow(BUCKET, new double[] { Double.NaN, Double.NaN, Double.NaN, Double.NaN, 1.0 });
    result.finalizeAvg();

    assertBucketExists(result, BUCKET);
    assertThat(result.getValue(BUCKET, 0)).as("SUM").isNaN();
    assertThat(result.getValue(BUCKET, 1)).as("AVG").isNaN();
    assertThat(result.getValue(BUCKET, 2)).as("MIN").isNaN();
    assertThat(result.getValue(BUCKET, 3)).as("MAX").isNaN();
    assertThat(result.getValue(BUCKET, 4)).as("COUNT reports no rows as the number zero, never as absent")
        .isEqualTo(1.0);
  }
}
