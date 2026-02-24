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

import com.arcadedb.engine.timeseries.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.timeseries.codec.GorillaXORCodec;
import com.arcadedb.engine.timeseries.codec.Simple8bCodec;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for per-block statistics in sealed TimeSeries blocks.
 * Covers: stats persistence/reload, aggregation fast path,
 * boundary blocks (slow path), truncation preserving stats,
 * and equivalence between stats-based and decompression-based results.
 */
class TimeSeriesBlockStatsTest {

  private static final String TEST_PATH = "target/databases/TimeSeriesBlockStatsTest/sealed";
  private List<ColumnDefinition> columns;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File("target/databases/TimeSeriesBlockStatsTest"));
    new File("target/databases/TimeSeriesBlockStatsTest").mkdirs();

    columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD),
        new ColumnDefinition("count", Type.LONG, ColumnDefinition.ColumnRole.FIELD)
    );
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File("target/databases/TimeSeriesBlockStatsTest"));
  }

  @Test
  void testAppendBlockWithStatsAndReload() throws Exception {
    final long[] timestamps = { 1000L, 2000L, 3000L, 4000L, 5000L };
    final double[] temperatures = { 10.0, 20.0, 30.0, 40.0, 50.0 };
    final long[] counts = { 1L, 2L, 3L, 4L, 5L };

    final byte[][] compressed = {
        DeltaOfDeltaCodec.encode(timestamps),
        GorillaXORCodec.encode(temperatures),
        Simple8bCodec.encode(counts)
    };

    final double[] mins = { Double.NaN, 10.0, 1.0 };
    final double[] maxs = { Double.NaN, 50.0, 5.0 };
    final double[] sums = { Double.NaN, 150.0, 15.0 };

    // Write block with stats
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(5, 1000L, 5000L, compressed, mins, maxs, sums, null);
      assertThat(store.getBlockCount()).isEqualTo(1);
    }

    // Reload and verify stats are preserved
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      assertThat(store.getBlockCount()).isEqualTo(1);
      assertThat(store.getGlobalMinTimestamp()).isEqualTo(1000L);
      assertThat(store.getGlobalMaxTimestamp()).isEqualTo(5000L);

      // Data should still be readable
      final List<Object[]> results = store.scanRange(1000L, 5000L, null, null);
      assertThat(results).hasSize(5);
      assertThat((double) results.get(0)[1]).isEqualTo(10.0);
      assertThat((double) results.get(4)[1]).isEqualTo(50.0);
    }
  }

  @Test
  void testAggregationUsesStatsFastPath() throws Exception {
    // Block fits entirely within one 1-hour bucket (bucket interval = 3600000ms)
    final long[] timestamps = { 0L, 1000L, 2000L, 3000L, 4000L };
    final double[] temperatures = { 10.0, 20.0, 30.0, 40.0, 50.0 };
    final long[] counts = { 2L, 4L, 6L, 8L, 10L };

    final byte[][] compressed = {
        DeltaOfDeltaCodec.encode(timestamps),
        GorillaXORCodec.encode(temperatures),
        Simple8bCodec.encode(counts)
    };

    final double[] mins = { Double.NaN, 10.0, 2.0 };
    final double[] maxs = { Double.NaN, 50.0, 10.0 };
    final double[] sums = { Double.NaN, 150.0, 30.0 };

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(5, 0L, 4000L, compressed, mins, maxs, sums, null);

      final long bucketInterval = 3600000L; // 1 hour

      final List<MultiColumnAggregationRequest> requests = List.of(
          new MultiColumnAggregationRequest(1, AggregationType.AVG, "avg_temp"),
          new MultiColumnAggregationRequest(1, AggregationType.MIN, "min_temp"),
          new MultiColumnAggregationRequest(1, AggregationType.MAX, "max_temp"),
          new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum_temp"),
          new MultiColumnAggregationRequest(-1, AggregationType.COUNT, "cnt")
      );

      final MultiColumnAggregationResult result = new MultiColumnAggregationResult(requests);
      store.aggregateMultiBlocks(0L, 4000L, requests, bucketInterval, result, null, null);
      result.finalizeAvg();

      assertThat(result.size()).isEqualTo(1);
      final long bucket = result.getBucketTimestamps().get(0);
      assertThat(bucket).isEqualTo(0L);

      // AVG = 150/5 = 30
      assertThat(result.getValue(bucket, 0)).isCloseTo(30.0, within(0.01));
      // MIN = 10
      assertThat(result.getValue(bucket, 1)).isCloseTo(10.0, within(0.01));
      // MAX = 50
      assertThat(result.getValue(bucket, 2)).isCloseTo(50.0, within(0.01));
      // SUM = 150
      assertThat(result.getValue(bucket, 3)).isCloseTo(150.0, within(0.01));
      // COUNT = 5
      assertThat(result.getValue(bucket, 4)).isCloseTo(5.0, within(0.01));
    }
  }

  @Test
  void testBoundaryBlockUsesSlowPath() throws Exception {
    // Block spans two 1-second buckets: timestamps 500-1500ms
    // bucket(500)=0, bucket(1500)=1000 → two buckets → slow path
    final long[] timestamps = { 500L, 800L, 1200L, 1500L };
    final double[] temperatures = { 10.0, 20.0, 30.0, 40.0 };
    final long[] counts = { 1L, 2L, 3L, 4L };

    final byte[][] compressed = {
        DeltaOfDeltaCodec.encode(timestamps),
        GorillaXORCodec.encode(temperatures),
        Simple8bCodec.encode(counts)
    };

    final double[] mins = { Double.NaN, 10.0, 1.0 };
    final double[] maxs = { Double.NaN, 40.0, 4.0 };
    final double[] sums = { Double.NaN, 100.0, 10.0 };

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(4, 500L, 1500L, compressed, mins, maxs, sums, null);

      final long bucketInterval = 1000L;

      final List<MultiColumnAggregationRequest> requests = List.of(
          new MultiColumnAggregationRequest(1, AggregationType.AVG, "avg_temp"),
          new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum_temp")
      );

      final MultiColumnAggregationResult result = new MultiColumnAggregationResult(requests);
      store.aggregateMultiBlocks(500L, 1500L, requests, bucketInterval, result, null, null);
      result.finalizeAvg();

      // Should have 2 buckets: 0 and 1000
      assertThat(result.size()).isEqualTo(2);

      // Bucket 0 (500, 800): avg=(10+20)/2=15, sum=30
      assertThat(result.getValue(0L, 0)).isCloseTo(15.0, within(0.01));
      assertThat(result.getValue(0L, 1)).isCloseTo(30.0, within(0.01));

      // Bucket 1000 (1200, 1500): avg=(30+40)/2=35, sum=70
      assertThat(result.getValue(1000L, 0)).isCloseTo(35.0, within(0.01));
      assertThat(result.getValue(1000L, 1)).isCloseTo(70.0, within(0.01));
    }
  }

  @Test
  void testMultipleBlocksAggregation() throws Exception {
    final byte[][] block1 = {
        DeltaOfDeltaCodec.encode(new long[] { 1000L, 2000L }),
        GorillaXORCodec.encode(new double[] { 10.0, 20.0 }),
        Simple8bCodec.encode(new long[] { 1L, 2L })
    };

    final byte[][] block2 = {
        DeltaOfDeltaCodec.encode(new long[] { 3000L, 4000L }),
        GorillaXORCodec.encode(new double[] { 30.0, 40.0 }),
        Simple8bCodec.encode(new long[] { 3L, 4L })
    };

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(2, 1000L, 2000L, block1,
          new double[] { Double.NaN, 10.0, 1.0 },
          new double[] { Double.NaN, 20.0, 2.0 },
          new double[] { Double.NaN, 30.0, 3.0 }, null);

      store.appendBlock(2, 3000L, 4000L, block2,
          new double[] { Double.NaN, 30.0, 3.0 },
          new double[] { Double.NaN, 40.0, 4.0 },
          new double[] { Double.NaN, 70.0, 7.0 }, null);

      assertThat(store.getBlockCount()).isEqualTo(2);

      // Aggregation over both blocks (both fit in 1h bucket → fast path)
      final List<MultiColumnAggregationRequest> requests = List.of(
          new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum_temp"),
          new MultiColumnAggregationRequest(-1, AggregationType.COUNT, "cnt")
      );

      final MultiColumnAggregationResult result = new MultiColumnAggregationResult(requests);
      store.aggregateMultiBlocks(1000L, 4000L, requests, 3600000L, result, null, null);

      // SUM = 10+20+30+40 = 100
      final long bucket = result.getBucketTimestamps().get(0);
      assertThat(result.getValue(bucket, 0)).isCloseTo(100.0, within(0.01));
      // COUNT = 4
      assertThat(result.getValue(bucket, 1)).isCloseTo(4.0, within(0.01));
    }
  }

  @Test
  void testTruncatePreservesStats() throws Exception {
    final byte[][] block1 = {
        DeltaOfDeltaCodec.encode(new long[] { 1000L, 2000L }),
        GorillaXORCodec.encode(new double[] { 10.0, 20.0 }),
        Simple8bCodec.encode(new long[] { 1L, 2L })
    };

    final byte[][] block2 = {
        DeltaOfDeltaCodec.encode(new long[] { 5000L, 6000L }),
        GorillaXORCodec.encode(new double[] { 50.0, 60.0 }),
        Simple8bCodec.encode(new long[] { 5L, 6L })
    };

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(2, 1000L, 2000L, block1,
          new double[] { Double.NaN, 10.0, 1.0 },
          new double[] { Double.NaN, 20.0, 2.0 },
          new double[] { Double.NaN, 30.0, 3.0 }, null);

      store.appendBlock(2, 5000L, 6000L, block2,
          new double[] { Double.NaN, 50.0, 5.0 },
          new double[] { Double.NaN, 60.0, 6.0 },
          new double[] { Double.NaN, 110.0, 11.0 }, null);

      // Truncate: remove block 1
      store.truncateBefore(3000L);
      assertThat(store.getBlockCount()).isEqualTo(1);

      // Verify aggregation still works with stats on the retained block
      final List<MultiColumnAggregationRequest> requests = List.of(
          new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum_temp"),
          new MultiColumnAggregationRequest(1, AggregationType.MIN, "min_temp"),
          new MultiColumnAggregationRequest(1, AggregationType.MAX, "max_temp")
      );

      final MultiColumnAggregationResult result = new MultiColumnAggregationResult(requests);
      store.aggregateMultiBlocks(5000L, 6000L, requests, 3600000L, result, null, null);

      final long bucket = result.getBucketTimestamps().get(0);
      assertThat(result.getValue(bucket, 0)).isCloseTo(110.0, within(0.01));
      assertThat(result.getValue(bucket, 1)).isCloseTo(50.0, within(0.01));
      assertThat(result.getValue(bucket, 2)).isCloseTo(60.0, within(0.01));
    }
  }

  @Test
  void testTruncatePreservesStatsAfterReload() throws Exception {
    final byte[][] block1 = {
        DeltaOfDeltaCodec.encode(new long[] { 1000L, 2000L }),
        GorillaXORCodec.encode(new double[] { 10.0, 20.0 }),
        Simple8bCodec.encode(new long[] { 1L, 2L })
    };

    final byte[][] block2 = {
        DeltaOfDeltaCodec.encode(new long[] { 5000L, 6000L }),
        GorillaXORCodec.encode(new double[] { 50.0, 60.0 }),
        Simple8bCodec.encode(new long[] { 5L, 6L })
    };

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      store.appendBlock(2, 1000L, 2000L, block1,
          new double[] { Double.NaN, 10.0, 1.0 },
          new double[] { Double.NaN, 20.0, 2.0 },
          new double[] { Double.NaN, 30.0, 3.0 }, null);

      store.appendBlock(2, 5000L, 6000L, block2,
          new double[] { Double.NaN, 50.0, 5.0 },
          new double[] { Double.NaN, 60.0, 6.0 },
          new double[] { Double.NaN, 110.0, 11.0 }, null);

      store.truncateBefore(3000L);
    }

    // Reload and verify the retained block is still intact
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(TEST_PATH, columns)) {
      assertThat(store.getBlockCount()).isEqualTo(1);

      final List<Object[]> results = store.scanRange(0L, 10000L, null, null);
      assertThat(results).hasSize(2);
      assertThat((double) results.get(0)[1]).isEqualTo(50.0);
      assertThat((double) results.get(1)[1]).isEqualTo(60.0);
    }
  }
}
