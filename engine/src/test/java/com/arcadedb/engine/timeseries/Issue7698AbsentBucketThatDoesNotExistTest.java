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
class Issue7698AbsentBucketThatDoesNotExistTest {

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

  @Test
  void mapModeAnswersAbsentForABucketItNeverHeardOf() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(oneOfEach());
    result.accumulateRow(1_000L, new double[] { 5.0, 5.0, 5.0, 5.0, 5.0 });

    assertBucketIsAbsentEverywhereButCount(result, 2_000L);
  }

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
