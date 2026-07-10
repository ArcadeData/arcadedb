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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.simd.ScalarTimeSeriesVectorOps;
import com.arcadedb.engine.timeseries.simd.SimdTimeSeriesVectorOps;
import com.arcadedb.engine.timeseries.simd.TimeSeriesVectorOps;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4596: inconsistent NaN handling across the MIN/MAX paths
 * (sealed block-stats vs row-iter vs cross-shard merge vs SIMD vs scalar).
 * <p>
 * The agreed policy is to SKIP NaN: a {@code NaN} sample is treated as absent, so MIN/MAX
 * over a window that contains at least one real value always returns that real min/max,
 * deterministically, regardless of which code path or storage layout produced it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesNaNHandlingTest extends TestHelper {

  /**
   * SIMD and scalar MIN/MAX must agree when the data contains NaN. The array is sized so the
   * NaN falls inside the SIMD-reduced prefix (not only the scalar tail).
   */
  @Test
  void simdAndScalarSkipNaN() {
    final TimeSeriesVectorOps simd = new SimdTimeSeriesVectorOps();
    final TimeSeriesVectorOps scalar = new ScalarTimeSeriesVectorOps();

    final double[] data = new double[64];
    for (int i = 0; i < data.length; i++)
      data[i] = i + 1; // 1..64
    data[10] = Double.NaN;
    data[40] = Double.NaN;

    assertThat(simd.min(data, 0, data.length)).isEqualTo(1.0);
    assertThat(simd.max(data, 0, data.length)).isEqualTo(64.0);

    // SIMD and scalar must be identical (deterministic, NaN skipped)
    assertThat(simd.min(data, 0, data.length)).isEqualTo(scalar.min(data, 0, data.length));
    assertThat(simd.max(data, 0, data.length)).isEqualTo(scalar.max(data, 0, data.length));
  }

  /**
   * Cross-shard merge must skip NaN. Whichever side carries the NaN, the merged MIN/MAX must
   * be the real value from the other side.
   */
  @Test
  void crossShardMergeSkipsNaN() {
    for (final AggregationType type : new AggregationType[] { AggregationType.MIN, AggregationType.MAX }) {
      // NaN on the right
      final AggregationResult left = new AggregationResult();
      left.addBucket(0L, 5.0, 1);
      final AggregationResult right = new AggregationResult();
      right.addBucket(0L, Double.NaN, 1);
      left.merge(right, type);
      assertThat(left.getValue(0)).as("NaN on right, type=%s", type).isEqualTo(5.0);

      // NaN on the left (the seeded/running value is NaN, a real value must win)
      final AggregationResult left2 = new AggregationResult();
      left2.addBucket(0L, Double.NaN, 1);
      final AggregationResult right2 = new AggregationResult();
      right2.addBucket(0L, 7.0, 1);
      left2.merge(right2, type);
      assertThat(left2.getValue(0)).as("NaN on left, type=%s", type).isEqualTo(7.0);
    }
  }

  /**
   * End-to-end: a window that mixes NaN samples with real values must yield the real MIN/MAX,
   * and the single-column ({@code aggregate}) and multi-column ({@code aggregateMulti}) paths
   * must agree.
   */
  @Test
  void aggregateSkipsNaNConsistently() throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "test_nan", cols, 1);

    // First sample is NaN on purpose: it seeds the running MIN/MAX value
    engine.appendSamples(
        new long[] { 1000L, 2000L, 3000L, 4000L },
        new Object[] { Double.NaN, 20.0, 5.0, Double.NaN });
    database.commit();

    database.begin();
    // Single bucket (bucketIntervalMs <= 0)
    final AggregationResult min = engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 0, AggregationType.MIN, 0, null);
    final AggregationResult max = engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 0, AggregationType.MAX, 0, null);

    assertThat(min.size()).isEqualTo(1);
    assertThat(min.getValue(0)).as("single-column MIN skips NaN").isEqualTo(5.0);
    assertThat(max.getValue(0)).as("single-column MAX skips NaN").isEqualTo(20.0);

    // Multi-column path over the same data must agree (columnIndex is the raw row index here,
    // so the value column is at index 1; 0 would be the timestamp).
    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(1, AggregationType.MIN, "min"),
        new MultiColumnAggregationRequest(1, AggregationType.MAX, "max"));
    final MultiColumnAggregationResult multi = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, 0, null);
    assertThat(multi.size()).isEqualTo(1);
    final long bucketTs = multi.getBucketTimestamps().getFirst();
    assertThat(multi.getValue(bucketTs, 0)).as("multi-column MIN skips NaN").isEqualTo(5.0);
    assertThat(multi.getValue(bucketTs, 1)).as("multi-column MAX skips NaN").isEqualTo(20.0);
    database.commit();

    engine.drop();
  }
}
