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
package com.arcadedb.server.http.handler;

import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.engine.timeseries.MultiColumnAggregationRequest;
import com.arcadedb.engine.timeseries.MultiColumnAggregationResult;
import com.arcadedb.serializer.json.JSONArray;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7584: the server-free half of {@code Issue7584AbsentSumAvgOverHttpIT}.
 * <p>
 * Issue #7089 put SUM and AVG under the NaN-as-absent policy, so a request that received no real sample in a bucket
 * now reads back {@code NaN} where it used to read back {@code 0.0}. #7584 asked for that change to be said out
 * loud because of what it does at the response boundary: JSON has no NaN literal, and
 * {@link JSONArray#put(Number)} resolves that by rewriting NaN to {@code 0}, which is indistinguishable from a
 * genuine total of zero and draws a Grafana panel a dip where the series has a gap.
 * <p>
 * This test drives the real accumulator into the real encoder - the same
 * {@code AbstractServerHttpHandler.putSampleValue} both time-series response builders call - so the contract is
 * exercised without needing a bound HTTP port. The IT covers the same ground over the wire.
 */
class Issue7584AbsentSumAvgSampleEncodingTest {

  private static final long BUCKET = 1_000L;

  private static List<MultiColumnAggregationRequest> sumAvgCount() {
    return List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum"),
        new MultiColumnAggregationRequest(1, AggregationType.AVG, "avg"),
        new MultiColumnAggregationRequest(1, AggregationType.COUNT, "count"));
  }

  private static JSONArray encode(final MultiColumnAggregationResult result) {
    final JSONArray values = new JSONArray();
    for (int r = 0; r < 3; r++)
      AbstractServerHttpHandler.putSampleValue(values, result.getValue(BUCKET, r));
    return values;
  }

  /**
   * A bucket whose every sample was absent: SUM and AVG must reach the client as JSON {@code null}, and COUNT -
   * the one aggregate outside the policy - as the number of rows it really saw.
   */
  @Test
  void anAbsentSumAndAvgAreEncodedAsJsonNull() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount());
    result.accumulateRow(BUCKET, new double[] { Double.NaN, Double.NaN, Double.NaN });
    result.accumulateRow(BUCKET, new double[] { Double.NaN, Double.NaN, Double.NaN });
    result.finalizeAvg();

    assertThat(result.getValue(BUCKET, 0)).as("the accumulator itself answers absent").isNaN();
    assertThat(result.getValue(BUCKET, 1)).isNaN();

    final JSONArray values = encode(result);
    assertThat(values.isNull(0)).as("absent SUM must be JSON null, not 0").isTrue();
    assertThat(values.isNull(1)).as("absent AVG must be JSON null, not 0").isTrue();
    assertThat(values.isNull(2)).as("COUNT counts rows and is never absent").isFalse();
    assertThat(((Number) values.get(2)).doubleValue()).isEqualTo(2.0);
  }

  /**
   * The encoding is a marker, not a blanket rule: real samples that happen to cancel to zero stay the number
   * {@code 0}. This is the distinction that makes the {@code null} above meaningful.
   */
  @Test
  void aRealTotalOfZeroIsStillTheNumberZero() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount());
    result.accumulateRow(BUCKET, new double[] { 2.5, 2.5, 2.5 });
    result.accumulateRow(BUCKET, new double[] { -2.5, -2.5, -2.5 });
    result.finalizeAvg();

    final JSONArray values = encode(result);
    assertThat(values.isNull(0)).as("a real total of zero is data, not a gap").isFalse();
    assertThat(((Number) values.get(0)).doubleValue()).isEqualTo(0.0);
    assertThat(((Number) values.get(1)).doubleValue()).isEqualTo(0.0);
  }

  /**
   * And the absent samples among real ones are skipped rather than propagated: the SUM totals the real ones and
   * the AVG divides by their count.
   */
  @Test
  void absentSamplesAreSkippedRatherThanPoisoningTheTotal() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAvgCount());
    result.accumulateRow(BUCKET, new double[] { 4.0, 4.0, 4.0 });
    result.accumulateRow(BUCKET, new double[] { Double.NaN, Double.NaN, Double.NaN });
    result.accumulateRow(BUCKET, new double[] { 6.0, 6.0, 6.0 });
    result.finalizeAvg();

    final JSONArray values = encode(result);
    assertThat(((Number) values.get(0)).doubleValue()).as("SUM of the real samples only").isEqualTo(10.0);
    assertThat(((Number) values.get(1)).doubleValue()).as("AVG divides by the 2 real samples, not by 3")
        .isEqualTo(5.0);
  }

  /**
   * The failure mode the encoder exists to prevent, pinned so nobody "simplifies" {@code putSampleValue} back into
   * a bare {@code put}: the raw overload turns the absent marker into a measurement of zero.
   */
  @Test
  void theRawJsonArrayOverloadIsWhatWouldTurnTheGapIntoAZero() {
    final JSONArray raw = new JSONArray();
    raw.put((Number) Double.NaN);
    assertThat(raw.isNull(0)).isFalse();
    assertThat(((Number) raw.get(0)).doubleValue()).isEqualTo(0.0);
  }
}
