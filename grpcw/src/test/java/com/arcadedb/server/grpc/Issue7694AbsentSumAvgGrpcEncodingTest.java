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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7694, gRPC half - the fourth reader of {@code MultiColumnAggregationResult.getValue()}, and the one
 * issue #7653 found had no regression coverage at all.
 * <p>
 * The other three readers are pinned: {@code Issue7584AbsentSumAvgSampleEncodingTest} and
 * {@code Issue7584AbsentSumAvgOverHttpIT} for the two HTTP time-series handlers,
 * {@code Issue7584AbsentSumAvgSqlPushDownTest} for {@code AggregateFromTimeSeriesStep}. The gRPC aggregation
 * stream in {@code ArcadeDbGrpcService} hands every value to {@link GrpcTimeSeriesSupport#toSampleValue}, which
 * maps an absent sample to an unset {@code GrpcValue} - correct by inspection, and until now driven by nothing, so
 * "simplifying" it to a bare {@code setDoubleValue} would have shipped green and a client would have read a gap as
 * a measurement.
 * <p>
 * This drives the REAL accumulator into the REAL encoder, the same composition the RPC performs
 * ({@code toSampleValue(result.getValue(timestamp, r))}), so it pins the contract rather than the mapper in
 * isolation, and without needing a bound gRPC port.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7694">issue #7694</a>
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7653">issue #7653</a>
 */
class Issue7694AbsentSumAvgGrpcEncodingTest {

  private static final long BUCKET = 1_000L;

  private static List<MultiColumnAggregationRequest> sumAvgCount() {
    return List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum"),
        new MultiColumnAggregationRequest(1, AggregationType.AVG, "avg"),
        new MultiColumnAggregationRequest(1, AggregationType.COUNT, "count"));
  }

  /** The encoding loop of {@code ArcadeDbGrpcService.timeSeriesQuery}'s aggregation branch, verbatim. */
  private static List<GrpcValue> encode(final MultiColumnAggregationResult result) {
    final TimeSeriesBucket.Builder bucket = TimeSeriesBucket.newBuilder().setTimestamp(BUCKET);
    for (int r = 0; r < 3; r++)
      bucket.addValues(GrpcTimeSeriesSupport.toSampleValue(result.getValue(BUCKET, r)));
    return bucket.build().getValuesList();
  }

  /**
   * A bucket whose every sample was absent: SUM and AVG must reach the client as an UNSET {@code GrpcValue} - the
   * proto's own null - and COUNT, the one aggregate outside the policy, as the number of rows it really saw.
   */
  @Test
  void anAbsentSumAndAvgAreEncodedAsAnUnsetGrpcValue() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount());
    result.accumulateRow(BUCKET, new double[] { Double.NaN, Double.NaN, Double.NaN });
    result.accumulateRow(BUCKET, new double[] { Double.NaN, Double.NaN, Double.NaN });
    result.finalizeAvg();

    assertThat(result.getValue(BUCKET, 0)).as("the accumulator itself answers absent").isNaN();
    assertThat(result.getValue(BUCKET, 1)).isNaN();

    final List<GrpcValue> values = encode(result);
    assertThat(values.get(0).getKindCase()).as("an absent SUM must arrive unset, never as a double")
        .isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(values.get(1).getKindCase()).as("an absent AVG must arrive unset, never as a double")
        .isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(values.get(2).getKindCase()).as("COUNT counts rows and is never absent")
        .isEqualTo(GrpcValue.KindCase.DOUBLE_VALUE);
    assertThat(values.get(2).getDoubleValue()).isEqualTo(2.0);
  }

  /**
   * The unset value is the absent marker, not a blanket rule: real samples that happen to cancel to zero still
   * arrive as the number {@code 0}. This is the distinction that makes the unset above mean something.
   */
  @Test
  void aRealTotalOfZeroIsStillTheNumberZero() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount());
    result.accumulateRow(BUCKET, new double[] { 2.5, 2.5, 2.5 });
    result.accumulateRow(BUCKET, new double[] { -2.5, -2.5, -2.5 });
    result.finalizeAvg();

    final List<GrpcValue> values = encode(result);
    assertThat(values.get(0).getKindCase()).as("a real total of zero is data, not a gap")
        .isEqualTo(GrpcValue.KindCase.DOUBLE_VALUE);
    assertThat(values.get(0).getDoubleValue()).isEqualTo(0.0);
    // getDoubleValue() answers 0.0 for an UNSET oneof too, so the kind has to be asserted first or this pair
    // would pass against the very regression the class exists to catch (CodeRabbit on PR #7718).
    assertThat(values.get(1).getKindCase()).isEqualTo(GrpcValue.KindCase.DOUBLE_VALUE);
    assertThat(values.get(1).getDoubleValue()).isEqualTo(0.0);
  }

  /** Absent samples among real ones are skipped rather than propagated, all the way to the wire value. */
  @Test
  void absentSamplesAreSkippedRatherThanPoisoningTheTotal() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount());
    result.accumulateRow(BUCKET, new double[] { 4.0, 4.0, 4.0 });
    result.accumulateRow(BUCKET, new double[] { Double.NaN, Double.NaN, Double.NaN });
    result.accumulateRow(BUCKET, new double[] { 6.0, 6.0, 6.0 });
    result.finalizeAvg();

    final List<GrpcValue> values = encode(result);
    assertThat(values.get(0).getDoubleValue()).as("SUM of the real samples only").isEqualTo(10.0);
    assertThat(values.get(1).getDoubleValue()).as("AVG divides by the 2 real samples, not by 3").isEqualTo(5.0);
  }

  /**
   * A SUM request in a bucket a sibling request brought into existence: the state issue #7506 made answer zero and
   * issue #7694 decided answers absent. Over gRPC the difference is a client reading a total of {@code 0} where the
   * series has no data at all.
   */
  @Test
  void aSumRequestNoSampleReachedInAnExistingBucketAlsoArrivesUnset() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount());
    // Only the COUNT request is offered a sample; SUM and AVG over the same column stay empty.
    result.accumulate(BUCKET, 2, 7.0);
    result.finalizeAvg();

    assertThat(result.getBucketTimestamps()).as("the bucket exists, so the RPC does emit it").containsExactly(BUCKET);

    final List<GrpcValue> values = encode(result);
    assertThat(values.get(0).getKindCase()).isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(values.get(1).getKindCase()).isEqualTo(GrpcValue.KindCase.KIND_NOT_SET);
    assertThat(values.get(2).getDoubleValue()).as("COUNT is real").isEqualTo(1.0);
  }

  /**
   * The failure mode the encoder exists to prevent, pinned so nobody "simplifies" {@code toSampleValue} into the
   * plain converter: that one sets the double field, and a NaN in a set field is a number as far as a client's
   * generated accessor is concerned.
   */
  @Test
  void thePlainConverterIsWhatWouldTurnTheGapIntoASetDouble() {
    final GrpcValue raw = GrpcTypeConverter.toGrpcValue(Double.NaN);

    assertThat(raw.getKindCase()).isEqualTo(GrpcValue.KindCase.DOUBLE_VALUE);
    assertThat(Double.isNaN(raw.getDoubleValue())).isTrue();
  }
}
