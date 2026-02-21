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

/**
 * Tests for {@link AggregationMetrics} instrumentation.
 */
class AggregationMetricsTest {

  private static final String DB_PATH = "target/databases/AggregationMetricsTest";
  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.command("sql",
        "CREATE TIMESERIES TYPE Sensor TIMESTAMP ts TAGS (id STRING) FIELDS (value DOUBLE) SHARDS 1");
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void metricsCountersArePopulated() throws Exception {
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Sensor")).getEngine();

    // Insert enough data to create sealed blocks
    final int batchSize = 10_000;
    final long baseTs = 1_000_000_000L;
    final long[] timestamps = new long[batchSize];
    final Object[] ids = new Object[batchSize];
    final Object[] values = new Object[batchSize];
    for (int i = 0; i < batchSize; i++) {
      timestamps[i] = baseTs + i * 100L;
      ids[i] = "s1";
      values[i] = 10.0 + i;
    }

    database.begin();
    engine.appendSamples(timestamps, ids, values);
    database.commit();
    engine.compactAll();

    // Run aggregation with metrics
    final AggregationMetrics metrics = new AggregationMetrics();
    final MultiColumnAggregationResult result = engine.aggregateMulti(
        Long.MIN_VALUE, Long.MAX_VALUE,
        List.of(new MultiColumnAggregationRequest(2, AggregationType.AVG, "avg_val")),
        3_600_000L, null, metrics);

    // Verify counters are consistent
    final int totalBlocks = metrics.getFastPathBlocks() + metrics.getSlowPathBlocks() + metrics.getSkippedBlocks();
    assertThat(totalBlocks).isGreaterThan(0);
    assertThat(result.size()).isGreaterThan(0);

    // toString should contain readable output
    final String str = metrics.toString();
    assertThat(str).contains("AggMetrics[");
    assertThat(str).contains("io=");
    assertThat(str).contains("fast=");
  }

  @Test
  void mergeFromCombinesCounters() {
    final AggregationMetrics a = new AggregationMetrics();
    a.addIo(100);
    a.addDecompTs(200);
    a.addFastPathBlock();
    a.addSlowPathBlock();

    final AggregationMetrics b = new AggregationMetrics();
    b.addIo(50);
    b.addDecompVal(300);
    b.addSkippedBlock();
    b.addSlowPathBlock();

    a.mergeFrom(b);
    assertThat(a.getIoNanos()).isEqualTo(150);
    assertThat(a.getDecompTsNanos()).isEqualTo(200);
    assertThat(a.getDecompValNanos()).isEqualTo(300);
    assertThat(a.getFastPathBlocks()).isEqualTo(1);
    assertThat(a.getSlowPathBlocks()).isEqualTo(2);
    assertThat(a.getSkippedBlocks()).isEqualTo(1);
  }
}
