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
 * Issue #7089 put SUM and AVG under that same policy, so the last test here covers them as well: the only
 * accumulator that still seeds at zero is COUNT.
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
   * The zero seed this assertion was written for (issue #4597) went away with issue #7089, deliberately: a SUM
   * seeded at zero cannot tell "no real sample ever arrived" from "the real samples added up to zero", and the
   * {@code +=} it fed turned one NaN sample into a NaN total. Since #7089 SUM and AVG carry the same absent
   * marker MIN and MAX have carried since #7043, and only COUNT - which counts rows, so zero rows IS zero -
   * still seeds at 0.0. The old expectation outlived the change and turned {@code main} red for every PR
   * (issue #7495); this is the same scenario, asserting the policy that replaced it.
   */
  @Test
  void anUntouchedSumIsAbsentWhileAnUntouchedCountIsZero() {
    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(1, AggregationType.SUM, "sumA"),
        new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB"),
        new MultiColumnAggregationRequest(3, AggregationType.AVG, "avgC"),
        new MultiColumnAggregationRequest(4, AggregationType.COUNT, "countD"));

    final MultiColumnAggregationResult mapMode = new MultiColumnAggregationResult(requests);
    mapMode.accumulate(1000L, 1, 7.0);
    assertUntouchedRequestsAreAbsentExceptCount(mapMode);

    // Flat mode reads the bucket through its own branch of getValue(), so it gets its own pass.
    final MultiColumnAggregationResult flatMode = new MultiColumnAggregationResult(requests, 1000L, 1000L, 16);
    flatMode.accumulate(1000L, 1, 7.0);
    assertThat(flatMode.isFlatMode()).isTrue();
    assertUntouchedRequestsAreAbsentExceptCount(flatMode);
  }

  private static void assertUntouchedRequestsAreAbsentExceptCount(final MultiColumnAggregationResult result) {
    // Only countB got a sample; every other request is untouched in a bucket that exists.
    assertThat(result.getValue(1000L, 1)).as("COUNT counts the row it got").isEqualTo(1.0);

    assertThat(result.getValue(1000L, 0)).as("an untouched SUM is absent, not zero").isNaN();
    assertThat(TimeSeriesNaN.isAbsent(result.getValue(1000L, 0))).isTrue();
    assertThat(result.getCount(1000L, 0)).isZero();

    assertThat(result.getValue(1000L, 2)).as("an untouched AVG is absent, not 0/0").isNaN();
    assertThat(result.getCount(1000L, 2)).isZero();

    assertThat(result.getValue(1000L, 3)).as("an untouched COUNT is zero: zero rows IS zero").isEqualTo(0.0);
    assertThat(result.getCount(1000L, 3)).isZero();
  }
}
