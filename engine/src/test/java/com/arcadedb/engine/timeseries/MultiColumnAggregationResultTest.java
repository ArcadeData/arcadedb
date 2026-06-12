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

  @Test
  void emptySumAndCountStayZeroNotNaN() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(
        List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sumA"),
            new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB")));
    result.accumulate(1000L, 1, 7.0);
    // SUM/COUNT of nothing is 0, not NaN
    assertThat(result.getValue(1000L, 0)).isEqualTo(0.0);
  }
}
