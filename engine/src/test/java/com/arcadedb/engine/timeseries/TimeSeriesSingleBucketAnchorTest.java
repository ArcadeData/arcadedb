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
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #4520: single-bucket aggregation (bucketIntervalMs &lt;= 0) must not anchor the
 * resulting bucket timestamp to the caller-supplied {@code fromTs} when {@code fromTs} is the
 * {@link Long#MIN_VALUE} "no lower bound" sentinel. A {@code Long.MIN_VALUE} bucket key gets misformatted
 * by downstream consumers as a real epoch. The single bucket must instead be anchored at a valid epoch.
 */
class TimeSeriesSingleBucketAnchorTest extends TestHelper {

  private static final List<ColumnDefinition> COLS = List.of(
      new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
      new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
  );

  // Closed in @AfterEach so the engine/files are released even when an assertion fails mid-test.
  private TimeSeriesEngine engine;

  @AfterEach
  void closeEngine() throws Exception {
    if (engine != null) {
      engine.close();
      engine = null;
    }
  }

  @Test
  void singleBucketAnchorMapsSentinelToEpoch() {
    // The MIN_VALUE "no lower bound" sentinel maps to the epoch anchor; any real fromTs is preserved.
    assertThat(TimeSeriesEngine.singleBucketAnchor(Long.MIN_VALUE)).isEqualTo(0L);
    assertThat(TimeSeriesEngine.singleBucketAnchor(0L)).isEqualTo(0L);
    assertThat(TimeSeriesEngine.singleBucketAnchor(1000L)).isEqualTo(1000L);
  }

  @Test
  void aggregateSingleBucketWithMinValueFromTsIsNotSentinel() throws Exception {
    database.begin();
    engine = new TimeSeriesEngine((DatabaseInternal) database, "ts_single_bucket", COLS, 1);
    engine.appendSamples(new long[] { 1000L, 2000L, 3000L }, new Object[] { 10.0, 20.0, 30.0 });
    database.commit();

    database.begin();
    // bucketIntervalMs = 0 -> single bucket; fromTs = Long.MIN_VALUE -> "no lower bound" sentinel
    final AggregationResult result = engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 0,
        AggregationType.SUM, 0, null);

    // All three rows collapse into one bucket
    assertThat(result.size()).isEqualTo(1);
    // The bug: the bucket key was Long.MIN_VALUE (the sentinel). It must be the epoch anchor instead.
    assertThat(result.getBucketTimestamp(0)).isEqualTo(0L);
    // SUM is unaffected by the anchor
    assertThat(result.getValue(0)).isEqualTo(60.0);
    assertThat(result.getCount(0)).isEqualTo(3);
    database.commit();
  }

  @Test
  void aggregateSingleBucketWithRealFromTsKeepsAnchor() throws Exception {
    database.begin();
    engine = new TimeSeriesEngine((DatabaseInternal) database, "ts_single_bucket_real", COLS, 1);
    engine.appendSamples(new long[] { 1000L, 2000L, 3000L }, new Object[] { 10.0, 20.0, 30.0 });
    database.commit();

    database.begin();
    // A real lower bound must still anchor the single bucket at fromTs (unchanged behavior).
    final AggregationResult result = engine.aggregate(1000L, Long.MAX_VALUE, 0,
        AggregationType.SUM, 0, null);

    assertThat(result.size()).isEqualTo(1);
    assertThat(result.getBucketTimestamp(0)).isEqualTo(1000L);
    assertThat(result.getValue(0)).isEqualTo(60.0);
    assertThat(result.getCount(0)).isEqualTo(3);
    database.commit();
  }

  @Test
  void aggregateSingleBucketWithRealFromTsKeepsAnchorOnSealedData() throws Exception {
    database.begin();
    engine = new TimeSeriesEngine((DatabaseInternal) database, "ts_single_bucket_real_sealed", COLS, 1);
    engine.appendSamples(new long[] { 1000L, 2000L, 3000L, 4000L }, new Object[] { 10.0, 20.0, 30.0, 40.0 });
    database.commit();

    // Force the sealed-store path so a real lower bound is preserved there too.
    database.begin();
    engine.compactAll();
    database.commit();

    database.begin();
    final AggregationResult result = engine.aggregate(1000L, Long.MAX_VALUE, 0,
        AggregationType.SUM, 0, null);

    assertThat(result.size()).isEqualTo(1);
    assertThat(result.getBucketTimestamp(0)).isEqualTo(1000L);
    assertThat(result.getValue(0)).isEqualTo(100.0);
    assertThat(result.getCount(0)).isEqualTo(4);
    database.commit();
  }

  @Test
  void aggregateMultiSingleBucketWithMinValueFromTsIsNotSentinel() throws Exception {
    database.begin();
    engine = new TimeSeriesEngine((DatabaseInternal) database, "ts_single_bucket_multi", COLS, 1);
    engine.appendSamples(new long[] { 1000L, 2000L, 3000L }, new Object[] { 10.0, 20.0, 30.0 });
    database.commit();

    database.begin();
    // columnIndex 1 = the "value" column in the full schema (index 0 = timestamp)
    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum_value"));

    final MultiColumnAggregationResult result = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
        requests, 0, null);

    final List<Long> buckets = result.getBucketTimestamps();
    assertThat(buckets).hasSize(1);
    assertThat(buckets.getFirst()).isEqualTo(0L);
    assertThat(result.getValue(buckets.getFirst(), 0)).isEqualTo(60.0);
    database.commit();
  }

  @Test
  void aggregateSingleColumnSingleBucketReadsSealedData() throws Exception {
    database.begin();
    engine = new TimeSeriesEngine((DatabaseInternal) database, "ts_single_bucket_sealed_col", COLS, 1);
    engine.appendSamples(new long[] { 1000L, 2000L, 3000L, 4000L }, new Object[] { 10.0, 20.0, 30.0, 40.0 });
    database.commit();

    // Force the sealed-store path so TimeSeriesSealedStore.aggregate() (single-column) is exercised.
    database.begin();
    engine.compactAll();
    database.commit();

    database.begin();
    final AggregationResult result = engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 0,
        AggregationType.SUM, 0, null);

    assertThat(result.size()).isEqualTo(1);
    assertThat(result.getBucketTimestamp(0)).isEqualTo(0L);
    assertThat(result.getValue(0)).isEqualTo(100.0);
    assertThat(result.getCount(0)).isEqualTo(4);
    database.commit();
  }

  @Test
  void aggregateMultiSingleBucketReadsSealedData() throws Exception {
    database.begin();
    engine = new TimeSeriesEngine((DatabaseInternal) database, "ts_single_bucket_sealed", COLS, 1);
    engine.appendSamples(new long[] { 1000L, 2000L, 3000L, 4000L }, new Object[] { 10.0, 20.0, 30.0, 40.0 });
    database.commit();

    // Force sealed-store path so the sealed-store single-bucket anchor is also exercised.
    database.begin();
    engine.compactAll();
    database.commit();

    database.begin();
    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum_value"));

    final MultiColumnAggregationResult result = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
        requests, 0, null);

    final List<Long> buckets = result.getBucketTimestamps();
    assertThat(buckets).hasSize(1);
    assertThat(buckets.getFirst()).isEqualTo(0L);
    assertThat(result.getValue(buckets.getFirst(), 0)).isEqualTo(100.0);
    database.commit();
  }
}
