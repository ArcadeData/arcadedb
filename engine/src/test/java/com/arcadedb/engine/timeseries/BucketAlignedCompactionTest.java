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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for bucket-aligned compaction in TimeSeries.
 * When compactionBucketIntervalMs is set, sealed blocks are split at
 * bucket boundaries so each block fits entirely within one time bucket,
 * enabling 100% fast-path aggregation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BucketAlignedCompactionTest {

  private static final String DB_PATH = "target/databases/BucketAlignedCompactionTest";
  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void testBucketAlignedCompactionProducesSingleBucketBlocks() throws Exception {
    // Create type with 1-second compaction bucket interval
    database.command("sql",
        "CREATE TIMESERIES TYPE Sensor TIMESTAMP ts TAGS (id STRING) FIELDS (value DOUBLE) " +
            "SHARDS 1 COMPACTION_INTERVAL 1 HOURS");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Sensor")).getEngine();

    // Insert data spanning 3 hours (at 100ms intervals = 108,000 samples)
    final int samplesPerHour = 36_000; // 1h / 100ms
    final int totalSamples = samplesPerHour * 3;
    final long baseTs = 0L; // start at epoch 0 for simplicity

    final long[] timestamps = new long[totalSamples];
    final Object[] ids = new Object[totalSamples];
    final Object[] values = new Object[totalSamples];
    for (int i = 0; i < totalSamples; i++) {
      timestamps[i] = baseTs + i * 100L;
      ids[i] = "s1";
      values[i] = 10.0 + (i % 100);
    }

    database.begin();
    engine.appendSamples(timestamps, ids, values);
    database.commit();

    // Compact with bucket-aligned splitting
    engine.compactAll();

    // Verify: each sealed block should fit within one 1-hour bucket
    final TimeSeriesShard shard = engine.getShard(0);
    final TimeSeriesSealedStore store = shard.getSealedStore();
    final int blockCount = store.getBlockCount();

    // With 3 hours of data and 1h buckets, we expect exactly 3 blocks
    assertThat(blockCount).isEqualTo(3);

    // Verify each block's timestamp range fits within one hour bucket
    for (int b = 0; b < blockCount; b++) {
      final long blockMin = store.getBlockMinTimestamp(b);
      final long blockMax = store.getBlockMaxTimestamp(b);
      final long bucketOfMin = (blockMin / 3_600_000L) * 3_600_000L;
      final long bucketOfMax = (blockMax / 3_600_000L) * 3_600_000L;
      assertThat(bucketOfMin).as("Block %d should fit in one bucket", b).isEqualTo(bucketOfMax);
    }

    // Verify data integrity: count should match
    assertThat(engine.countSamples()).isEqualTo(totalSamples);
  }

  @Test
  void testBucketAlignedAggregationUses100PercentFastPath() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Sensor TIMESTAMP ts TAGS (id STRING) FIELDS (value DOUBLE) " +
            "SHARDS 1 COMPACTION_INTERVAL 1 HOURS");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Sensor")).getEngine();

    // Insert data spanning 2 hours
    final int samplesPerHour = 36_000;
    final int totalSamples = samplesPerHour * 2;
    final long baseTs = 0L;

    final long[] timestamps = new long[totalSamples];
    final Object[] ids = new Object[totalSamples];
    final Object[] values = new Object[totalSamples];
    for (int i = 0; i < totalSamples; i++) {
      timestamps[i] = baseTs + i * 100L;
      ids[i] = "s1";
      values[i] = (double) (i + 1);
    }

    database.begin();
    engine.appendSamples(timestamps, ids, values);
    database.commit();
    engine.compactAll();

    // Aggregate with metrics to verify fast path usage
    final AggregationMetrics metrics = new AggregationMetrics();
    final MultiColumnAggregationResult result = engine.aggregateMulti(
        Long.MIN_VALUE, Long.MAX_VALUE,
        List.of(
            new MultiColumnAggregationRequest(2, AggregationType.SUM, "sum_val"),
            new MultiColumnAggregationRequest(-1, AggregationType.COUNT, "cnt")
        ),
        3_600_000L, null, metrics);

    // With bucket-aligned compaction, ALL blocks should use fast path
    assertThat(metrics.getFastPathBlocks()).isEqualTo(2);
    assertThat(metrics.getSlowPathBlocks()).isEqualTo(0);

    // Verify 2 buckets
    assertThat(result.size()).isEqualTo(2);

    // Verify count per bucket
    assertThat(result.getValue(0L, 1)).isCloseTo((double) samplesPerHour, within(0.01));
    assertThat(result.getValue(3_600_000L, 1)).isCloseTo((double) samplesPerHour, within(0.01));
  }

  @Test
  void testDefaultCompactionDoesNotSplitAtBuckets() throws Exception {
    // Without COMPACTION_INTERVAL, blocks use fixed SEALED_BLOCK_SIZE chunking
    database.command("sql",
        "CREATE TIMESERIES TYPE Sensor TIMESTAMP ts TAGS (id STRING) FIELDS (value DOUBLE) SHARDS 1");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Sensor")).getEngine();

    // Insert 100,000 samples spanning ~2.78 hours at 100ms intervals
    final int totalSamples = 100_000;
    final long baseTs = 0L;

    final long[] timestamps = new long[totalSamples];
    final Object[] ids = new Object[totalSamples];
    final Object[] values = new Object[totalSamples];
    for (int i = 0; i < totalSamples; i++) {
      timestamps[i] = baseTs + i * 100L;
      ids[i] = "s1";
      values[i] = 1.0;
    }

    database.begin();
    engine.appendSamples(timestamps, ids, values);
    database.commit();
    engine.compactAll();

    // Without bucket alignment, blocks use SEALED_BLOCK_SIZE=65536 → 2 blocks
    final TimeSeriesShard shard = engine.getShard(0);
    assertThat(shard.getSealedStore().getBlockCount()).isEqualTo(2);

    // First block spans ~1.82 hours → crosses 1h boundary → slow path
    final AggregationMetrics metrics = new AggregationMetrics();
    engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
        List.of(new MultiColumnAggregationRequest(-1, AggregationType.COUNT, "cnt")),
        3_600_000L, null, metrics);

    // At least one block should use slow path (crossing bucket boundary)
    assertThat(metrics.getSlowPathBlocks()).isGreaterThan(0);
  }

  @Test
  void testSqlDdlWithCompactionInterval() {
    // Test that COMPACTION_INTERVAL is properly parsed and persisted
    database.command("sql",
        "CREATE TIMESERIES TYPE SensorHourly TIMESTAMP ts TAGS (id STRING) FIELDS (temp DOUBLE) " +
            "SHARDS 2 COMPACTION_INTERVAL 1 HOURS");

    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("SensorHourly");
    assertThat(type).isNotNull();
    assertThat(type.getCompactionBucketIntervalMs()).isEqualTo(3_600_000L);
  }

  @Test
  void testSqlDdlWithCompactionIntervalMinutes() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SensorMinute TIMESTAMP ts TAGS (id STRING) FIELDS (temp DOUBLE) " +
            "SHARDS 1 COMPACTION_INTERVAL 15 MINUTES");

    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("SensorMinute");
    assertThat(type).isNotNull();
    assertThat(type.getCompactionBucketIntervalMs()).isEqualTo(15 * 60_000L);
  }

  @Test
  void testCompactionBucketIntervalPersistedAndReloaded() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Sensor TIMESTAMP ts TAGS (id STRING) FIELDS (value DOUBLE) " +
            "SHARDS 1 COMPACTION_INTERVAL 1 HOURS");

    // Insert some data and compact
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Sensor")).getEngine();
    final long[] timestamps = { 0L, 100L, 200L };
    final Object[] ids = { "s1", "s1", "s1" };
    final Object[] values = { 1.0, 2.0, 3.0 };

    database.begin();
    engine.appendSamples(timestamps, ids, values);
    database.commit();

    database.close();

    // Reopen and verify the config is preserved
    database = new DatabaseFactory(DB_PATH).open();
    final LocalTimeSeriesType reloaded = (LocalTimeSeriesType) database.getSchema().getType("Sensor");
    assertThat(reloaded.getCompactionBucketIntervalMs()).isEqualTo(3_600_000L);
  }
}
