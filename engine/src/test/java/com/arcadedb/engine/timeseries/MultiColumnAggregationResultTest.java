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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4592 sibling #4597: {@code getValue} must not leak the internal
 * {@code Double.MAX_VALUE} / {@code -Double.MAX_VALUE} MIN/MAX sentinel when a bucket is touched by
 * another request (e.g. COUNT) but the MIN/MAX request itself received no data. The agreed
 * NaN-as-absent policy (issue #4596) applies here too.
 * <p>
 * Issue #7584 extends the same coverage to SUM and AVG. Those two joined the policy in issue #7089, which reseeded
 * their accumulator from {@code 0.0} to {@link TimeSeriesNaN#ABSENT} without updating the assertion here, so this
 * class encoded the superseded contract and was red on {@code main} from that commit on. The contract SUM and AVG
 * answer to now is MIN/MAX's: a request that received no real sample in a bucket reads back absent, because a zero
 * seed cannot tell "nothing arrived" apart from "it all added up to zero".
 */
class MultiColumnAggregationResultTest {

  private static List<MultiColumnAggregationRequest> minThenCount() {
    return List.of(new MultiColumnAggregationRequest(1, AggregationType.MIN, "minA"),
        new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB"));
  }

  private static List<MultiColumnAggregationRequest> maxThenCount() {
    return List.of(new MultiColumnAggregationRequest(1, AggregationType.MAX, "maxA"),
        new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB"));
  }

  @Test
  void mapModeEmptyMinReturnsNaNWhenBucketTouchedByAnotherRequest() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(minThenCount());
    // only the COUNT request gets data in this bucket; the MIN request stays empty
    result.accumulate(1000L, 1, 7.0);
    assertThat(result.getCount(1000L, 0)).isZero();
    assertThat(result.getValue(1000L, 0)).isNaN();
    assertThat(result.getValue(1000L, 1)).isEqualTo(1.0); // COUNT is real
  }

  @Test
  void mapModeEmptyMaxReturnsNaN() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(maxThenCount());
    result.accumulate(1000L, 1, 7.0);
    assertThat(result.getValue(1000L, 0)).isNaN();
  }

  @Test
  void flatModeEmptyMinReturnsNaN() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(minThenCount(), 0L, 1000L, 16);
    result.accumulate(1000L, 1, 7.0);
    assertThat(result.isFlatMode()).isTrue();
    assertThat(result.getValue(1000L, 0)).isNaN();
  }

  @Test
  void populatedMinReturnsRealValueNotSentinelNorNaN() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(minThenCount());
    result.accumulate(1000L, 0, 4.0);
    result.accumulate(1000L, 0, 2.0);
    result.accumulate(1000L, 1, 9.0);
    assertThat(result.getValue(1000L, 0)).isEqualTo(2.0);
  }

  /**
   * Every "this request stayed empty" test below writes to ONE request and reads back another, so each one has to
   * rule out the answer it would get from a bucket that was never touched at all: {@code getValue} returns a plain
   * {@code 0.0} for an unknown bucket in map mode, and for an unset {@code bucketUsed[idx]} in flat mode. Asserting
   * the bucket is really there is what makes the absent marker read afterwards evidence about the SEED that
   * {@code newInitializedValues()} laid down per request index, rather than about the bucket's existence
   * (code review on PR #7662).
   */
  private static void assertBucketExists(final MultiColumnAggregationResult result) {
    assertThat(result.getBucketTimestamps())
        .as("the bucket must exist, or an absent answer proves nothing about the per-request seed")
        .containsExactly(1000L);
  }

  private static List<MultiColumnAggregationRequest> sumThenCount() {
    return List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sumA"),
        new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB"));
  }

  private static List<MultiColumnAggregationRequest> avgThenCount() {
    return List.of(new MultiColumnAggregationRequest(1, AggregationType.AVG, "avgA"),
        new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB"));
  }

  /**
   * Issue #7584. This assertion used to read {@code isEqualTo(0.0)}, which was the contract before issue #7089 put
   * SUM under the NaN-as-absent policy; the seed moved to {@link TimeSeriesNaN#ABSENT} and the assertion did not
   * follow, so the class went red on {@code main}. Absent is the answer the rest of the stack is built on: COUNT is
   * the aggregate that reports "no rows" as the number zero, and SUM says so by being absent.
   */
  @Test
  void emptySumIsAbsentNotZero() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount());
    // only the COUNT request gets data in this bucket; the SUM request stays empty
    result.accumulate(1000L, 1, 7.0);
    assertBucketExists(result);
    assertThat(result.getCount(1000L, 0)).isZero();
    assertThat(result.getValue(1000L, 0)).as("SUM over no real sample is absent, not a total of zero").isNaN();
    assertThat(result.getValue(1000L, 1)).as("COUNT still counts rows").isEqualTo(1.0);
  }

  /**
   * The surviving half of this class's original {@code emptySumAndCountStayZeroNotNaN}: COUNT is the one aggregate
   * that is NOT under the NaN-as-absent policy. It counts rows the way SQL's {@code COUNT(*)} does, so a bucket in
   * which the COUNT request itself saw nothing reads back the number {@code 0} - never the absent marker.
   */
  @Test
  void emptyCountStaysZeroNotAbsent() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount());
    // only the SUM request gets data in this bucket; the COUNT request stays empty
    result.accumulate(1000L, 0, 7.0);
    assertBucketExists(result);
    assertThat(result.getValue(1000L, 1)).as("COUNT of no rows is 0, not absent").isEqualTo(0.0);
    assertThat(result.getCount(1000L, 1)).isZero();
  }

  /**
   * The same contract on the pre-allocated path. Flat and map mode answering differently is what issue #7089's
   * scalar-versus-vectorized divergence was, so both modes are pinned.
   */
  @Test
  void flatModeEmptySumIsAbsentNotZero() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount(), 0L, 1000L, 16);
    result.accumulate(1000L, 1, 7.0);
    assertThat(result.isFlatMode()).isTrue();
    assertBucketExists(result);
    assertThat(result.getValue(1000L, 0)).isNaN();
    assertThat(result.getValue(1000L, 1)).isEqualTo(1.0);
  }

  /**
   * AVG divides by the count of REAL samples, so an empty request must survive {@link
   * MultiColumnAggregationResult#finalizeAvg()} without becoming {@code 0/0} or a zero.
   */
  @Test
  void emptyAvgStaysAbsentThroughFinalize() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(avgThenCount());
    result.accumulate(1000L, 1, 7.0);
    result.finalizeAvg();
    assertBucketExists(result);
    assertThat(result.getCount(1000L, 0)).isZero();
    assertThat(result.getValue(1000L, 0)).as("AVG over no real sample is absent, not zero").isNaN();
  }

  @Test
  void flatModeEmptyAvgStaysAbsentThroughFinalize() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(avgThenCount(), 0L, 1000L, 16);
    result.accumulate(1000L, 1, 7.0);
    result.finalizeAvg();
    assertThat(result.isFlatMode()).isTrue();
    assertBucketExists(result);
    assertThat(result.getValue(1000L, 0)).isNaN();
  }

  /**
   * The absent answer is the marker, not a blanket rule: a SUM that did receive samples still totals them, and a
   * NaN sample among real ones is skipped rather than poisoning the total.
   */
  @Test
  void populatedSumTotalsTheRealSamplesAndSkipsTheAbsentOnes() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount());
    result.accumulate(1000L, 0, 4.0);
    result.accumulate(1000L, 0, Double.NaN);
    result.accumulate(1000L, 0, 2.5);
    assertThat(result.getValue(1000L, 0)).isEqualTo(6.5);
    assertThat(result.getCount(1000L, 0)).isEqualTo(2);
  }

  /**
   * A SUM that really did total to zero must stay the number zero, which is the distinction the absent marker
   * exists to make: {@code 0.0} here and NaN in {@link #emptySumIsAbsentNotZero()}.
   */
  @Test
  void aSumOfRealSamplesThatCancelToZeroIsZeroNotAbsent() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount());
    result.accumulate(1000L, 0, 3.0);
    result.accumulate(1000L, 0, -3.0);
    assertThat(result.getValue(1000L, 0)).isEqualTo(0.0);
    assertThat(result.getCount(1000L, 0)).isEqualTo(2);
  }

  @Nested
  /**
   * Issue #7698: a bucket that does not EXIST answers the absent marker, not {@code 0.0}.
   * <p>
   * {@code MultiColumnAggregationResult.getValue()} had two "no data" answers. A bucket that exists whose
   * SUM/MIN/MAX/AVG request holds no real sample answered {@link TimeSeriesNaN#ABSENT}, decided by issues
   * #4596/#7043/#7089 and settled by #7694. A bucket nothing ever landed in answered {@code 0.0}, from an arm that
   * predates all of them - for SUM and MIN alike. They are the same question, and a reader that asks it of a
   * timestamp computed from the REQUEST range rather than taken from {@code getBucketTimestamps()} - the natural
   * way to fill the gaps of a fixed-step series, which is exactly what a Grafana-style endpoint wants - could not
   * tell a missing bucket from a real total of zero. That is the failure mode the whole policy exists to prevent.
   * <p>
   * COUNT is the exception, and stays one: it answers {@code 0} over no rows the way {@code COUNT(*)} does, which
   * is the same reason its accumulator is seeded with a number rather than with the marker.
   * <p>
   * The three arms are pinned separately, because they are three pieces of code: map mode, flat mode inside the
   * pre-allocated window, and flat mode outside it (where the overflow map of issue #6937 is consulted first).
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7698">issue #7698</a>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  class AbsentBucketThatDoesNotExist {

    /** One request of each type, so an answer can never be mistaken for another type's. */
    private static List<MultiColumnAggregationRequest> oneOfEach() {
      return List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum"),
          new MultiColumnAggregationRequest(1, AggregationType.AVG, "avg"),
          new MultiColumnAggregationRequest(1, AggregationType.MIN, "min"),
          new MultiColumnAggregationRequest(1, AggregationType.MAX, "max"),
          new MultiColumnAggregationRequest(1, AggregationType.COUNT, "count"));
    }

    private static void assertBucketIsAbsentEverywhereButCount(final MultiColumnAggregationResult result,
        final long bucketTs) {
      assertThat(result.getBucketTimestamps()).as("the bucket must not exist, or this proves nothing")
          .doesNotContain(bucketTs);

      assertThat(result.getValue(bucketTs, 0)).as("SUM over a bucket that does not exist").isNaN();
      assertThat(result.getValue(bucketTs, 1)).as("AVG over a bucket that does not exist").isNaN();
      assertThat(result.getValue(bucketTs, 2)).as("MIN over a bucket that does not exist").isNaN();
      assertThat(result.getValue(bucketTs, 3)).as("MAX over a bucket that does not exist").isNaN();
      assertThat(result.getValue(bucketTs, 4)).as("COUNT answers 0 rows, exactly as COUNT(*) does").isEqualTo(0.0);

      // TimeSeriesNaN.isAbsent is the test every reader is supposed to use, and it has to agree with the raw NaN.
      assertThat(TimeSeriesNaN.isAbsent(result.getValue(bucketTs, 0))).isTrue();
      assertThat(TimeSeriesNaN.isAbsent(result.getValue(bucketTs, 4))).isFalse();
    }

    // #7698

    @Test
    void mapModeAnswersAbsentForABucketItNeverHeardOf() {
      final MultiColumnAggregationResult result = new MultiColumnAggregationResult(oneOfEach());
      result.accumulateRow(1_000L, new double[] { 5.0, 5.0, 5.0, 5.0, 5.0 });

      assertBucketIsAbsentEverywhereButCount(result, 2_000L);
    }

    // #7698

    @Test
    void flatModeAnswersAbsentForAnUntouchedBucketInsideTheWindow() {
      final MultiColumnAggregationResult result = new MultiColumnAggregationResult(oneOfEach(), 0L, 1_000L, 16);
      result.accumulateRow(1_000L, new double[] { 5.0, 5.0, 5.0, 5.0, 5.0 });

      assertThat(result.isFlatMode()).isTrue();
      assertBucketIsAbsentEverywhereButCount(result, 2_000L);
    }

    /**
     * Outside the pre-allocated window the overflow map is consulted first, and its miss is a third {@code return}
     * with its own history: the overflow map exists only when something actually overflowed (issue #6937), so this
     * covers both the never-allocated map and the allocated-but-missing key.
     */
    // #7698
    @Test
    void flatModeAnswersAbsentForABucketOutsideTheWindow() {
      final MultiColumnAggregationResult neverOverflowed = new MultiColumnAggregationResult(oneOfEach(), 0L, 1_000L, 2);
      neverOverflowed.accumulateRow(0L, new double[] { 5.0, 5.0, 5.0, 5.0, 5.0 });
      assertThat(neverOverflowed.getOverflowBucketCount()).isZero();
      assertBucketIsAbsentEverywhereButCount(neverOverflowed, 9_000L);

      final MultiColumnAggregationResult overflowed = new MultiColumnAggregationResult(oneOfEach(), 0L, 1_000L, 2);
      overflowed.accumulateRow(5_000L, new double[] { 5.0, 5.0, 5.0, 5.0, 5.0 });
      assertThat(overflowed.getOverflowBucketCount()).as("5000 fell outside the 2-bucket window").isEqualTo(1);
      assertBucketIsAbsentEverywhereButCount(overflowed, 9_000L);
    }

    /**
     * The distinction the policy exists to make, at the one seam this issue closes: a bucket whose real samples add
     * up to zero is the NUMBER zero, and it must stay tellable apart from the bucket that is not there at all.
     */
    // #7698
    @Test
    void aRealTotalOfZeroIsStillTheNumberZero() {
      final MultiColumnAggregationResult result = new MultiColumnAggregationResult(oneOfEach(), 0L, 1_000L, 16);
      result.accumulateRow(1_000L, new double[] { 2.5, 2.5, 2.5, 2.5, 1.0 });
      result.accumulateRow(1_000L, new double[] { -2.5, -2.5, -2.5, -2.5, 1.0 });

      assertThat(result.getValue(1_000L, 0)).as("a real total of zero is data, not a gap").isEqualTo(0.0);
      assertThat(TimeSeriesNaN.isAbsent(result.getValue(1_000L, 0))).isFalse();
      assertBucketIsAbsentEverywhereButCount(result, 2_000L);
    }

    /**
     * The four readers in {@code src/main} are unaffected because they only ask for timestamps
     * {@code getBucketTimestamps()} handed them, and every one of those is a bucket that exists. Stated as a test
     * so that the claim "this arm is unreachable today" is checked rather than asserted in a comment.
     */
    // #7698
    @Test
    void everyTimestampTheResultPublishesIsABucketThatExists() {
      final MultiColumnAggregationResult result = new MultiColumnAggregationResult(oneOfEach(), 0L, 1_000L, 2);
      result.accumulateRow(0L, new double[] { 1.0, 1.0, 1.0, 1.0, 1.0 });
      result.accumulateRow(7_000L, new double[] { 3.0, 3.0, 3.0, 3.0, 1.0 });

      for (final long bucketTs : result.getBucketTimestamps())
        assertThat(result.getValue(bucketTs, 0)).as("bucket %d was published, so it holds a real sum", bucketTs)
            .isNotNaN();
    }
  }
}
