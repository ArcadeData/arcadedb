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
    assertThat(result.getCount(1000L, 0)).isZero();
    assertThat(result.getValue(1000L, 0)).as("AVG over no real sample is absent, not zero").isNaN();
  }

  @Test
  void flatModeEmptyAvgStaysAbsentThroughFinalize() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(avgThenCount(), 0L, 1000L, 16);
    result.accumulate(1000L, 1, 7.0);
    result.finalizeAvg();
    assertThat(result.isFlatMode()).isTrue();
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
}
