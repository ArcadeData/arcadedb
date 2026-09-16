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
package com.arcadedb.server.grpc;

import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.engine.timeseries.MultiColumnAggregationRequest;
import com.arcadedb.engine.timeseries.MultiColumnAggregationResult;
import com.arcadedb.engine.timeseries.TimeSeriesNaN;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7653, resolved as part of the #7694 contract decision: the gRPC TimeSeries aggregation stream is the
 * fourth reader of {@code MultiColumnAggregationResult.getValue()}, and it was the only one with no regression
 * test pinning how it encodes an absent SUM or AVG.
 * <p>
 * {@code ArcadeDbGrpcService} builds each {@code TimeSeriesBucket} with
 * {@code bucket.addValues(GrpcTimeSeriesSupport.toSampleValue(result.getValue(timestamp, r)))}. The two lines
 * below are that pair, driven against a real accumulator rather than a hand-written NaN, so the test fails if
 * either half changes: if the accumulator stops answering absent (which is what issue #7506's seed did, and what
 * #7694 reverted), or if {@code toSampleValue} starts encoding absence as {@code double_value} NaN, which a
 * client would read as a number instead of as null.
 */
class Issue7694AbsentSumAvgGrpcEncodingTest {

  private static final long BUCKET = 1_000L;

  private static List<MultiColumnAggregationRequest> sumAvgCount() {
    return List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "value_sum"),
        new MultiColumnAggregationRequest(1, AggregationType.AVG, "value_avg"),
        new MultiColumnAggregationRequest(2, AggregationType.COUNT, "value_count"));
  }

  /** The bucket the gRPC stream would emit, built exactly the way {@code ArcadeDbGrpcService} builds it. */
  private static TimeSeriesBucket encode(final MultiColumnAggregationResult result, final long timestamp,
      final int requestCount) {
    final TimeSeriesBucket.Builder bucket = TimeSeriesBucket.newBuilder().setTimestamp(timestamp);
    for (int r = 0; r < requestCount; r++)
      bucket.addValues(GrpcTimeSeriesSupport.toSampleValue(result.getValue(timestamp, r)));
    return bucket.build();
  }

  @Test
  void anAllAbsentBucketStreamsSumAndAvgAsUnsetValuesNotAsNaNNumbers() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount(), 0L, BUCKET, 4);
    result.accumulateRow(BUCKET, new double[] { Double.NaN, Double.NaN, 1.0 });
    result.accumulateRow(BUCKET, new double[] { Double.NaN, Double.NaN, 1.0 });
    result.finalizeAvg();

    assertThat(result.getBucketTimestamps()).as("the bucket must exist for the read to mean anything")
        .contains(BUCKET);
    assertThat(result.getValue(BUCKET, 0)).as("the accumulator itself answers absent").isNaN();
    assertThat(result.getValue(BUCKET, 1)).isNaN();

    final TimeSeriesBucket bucket = encode(result, BUCKET, 3);

    assertThat(bucket.getValuesCount()).isEqualTo(3);
    assertThat(bucket.getValues(0).getKindCase()).as("absent SUM must have no kind set, so it decodes to null")
        .isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(bucket.getValues(1).getKindCase()).as("absent AVG must have no kind set")
        .isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(bucket.getValues(0).getDoubleValue()).as("and must NOT be sent as a NaN double").isZero();

    assertThat(bucket.getValues(2).getKindCase()).as("COUNT counts rows and is never absent")
        .isNotEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(bucket.getValues(2).getDoubleValue()).isEqualTo(2.0);
  }

  /**
   * A SUM request holding no real sample is the case #7694 decided, and the gRPC stream must render it as the
   * same gap the HTTP endpoints put a JSON {@code null} in - not as the zero that a client would plot.
   */
  @Test
  void aSumRequestOfferedNoRealSampleStreamsAsAnUnsetValue() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount(), 0L, BUCKET, 4);
    // Only the COUNT request is given a statistic, the way a sibling column's samples would create the bucket.
    result.accumulateSingleStat(BUCKET, 2, 3.0, 3);

    assertThat(result.getBucketTimestamps()).contains(BUCKET);

    final TimeSeriesBucket bucket = encode(result, BUCKET, 3);

    assertThat(bucket.getValues(0).getKindCase()).isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(bucket.getValues(1).getKindCase()).isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(bucket.getValues(2).getDoubleValue()).isEqualTo(3.0);
  }

  /**
   * The distinction the absent marker exists to make: a total of real samples that cancels to zero is data, and
   * must reach the client as the number zero rather than as the same gap an absent total gets.
   */
  @Test
  void aRealTotalOfZeroStreamsAsTheNumberZeroNotAsAGap() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount(), 0L, BUCKET, 4);
    result.accumulateRow(BUCKET, new double[] { 3.0, 3.0, 1.0 });
    result.accumulateRow(BUCKET, new double[] { -3.0, -3.0, 1.0 });
    result.finalizeAvg();

    final TimeSeriesBucket bucket = encode(result, BUCKET, 3);

    assertThat(bucket.getValues(0).getKindCase()).as("a real total of zero is data, not a gap")
        .isNotEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(bucket.getValues(0).getDoubleValue()).isEqualTo(0.0);
    assertThat(bucket.getValues(1).getDoubleValue()).isEqualTo(0.0);
  }

  /**
   * And the encoder itself, on the marker the accumulator hands it. {@code TimeSeriesNaN.ABSENT} is a NaN, which
   * {@code GrpcTypeConverter} would otherwise put in {@code double_value}.
   */
  @Test
  void theEncoderRendersTheAbsentMarkerAsAnUnsetValue() {
    assertThat(GrpcTimeSeriesSupport.toSampleValue(TimeSeriesNaN.ABSENT).getKindCase())
        .isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(GrpcTimeSeriesSupport.toSampleValue(Double.NaN).getKindCase())
        .isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(GrpcTimeSeriesSupport.toSampleValue(0.0).getDoubleValue()).isZero();
    assertThat(GrpcTimeSeriesSupport.toSampleValue(0.0).getKindCase())
        .isNotEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
  }
}
