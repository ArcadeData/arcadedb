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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for CREATE TIMESERIES TYPE SQL statement.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CreateTimeSeriesTypeStatementTest extends TestHelper {

  @Test
  void basicCreateTimeSeriesType() {
    final ResultSet result = database.command("sql",
        "CREATE TIMESERIES TYPE SensorData TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat((String) row.getProperty("operation")).isEqualTo("create timeseries type");
    assertThat((String) row.getProperty("typeName")).isEqualTo("SensorData");

    assertThat(database.getSchema().existsType("SensorData")).isTrue();
    final DocumentType type = database.getSchema().getType("SensorData");
    assertThat(type).isInstanceOf(LocalTimeSeriesType.class);

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) type;
    assertThat(tsType.getTimestampColumn()).isEqualTo("ts");
    assertThat(tsType.getTsColumns()).hasSize(3); // ts + sensor_id + temperature
  }

  @Test
  void createWithShardsAndRetention() {
    database.command("sql",
        "CREATE TIMESERIES TYPE Metrics TIMESTAMP ts TAGS (host STRING) FIELDS (cpu DOUBLE, mem LONG) SHARDS 4 RETENTION 90 DAYS");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("Metrics");
    assertThat(tsType.getShardCount()).isEqualTo(4);
    assertThat(tsType.getRetentionMs()).isEqualTo(90L * 86400000L);
    assertThat(tsType.getTsColumns()).hasSize(4); // ts + host + cpu + mem
  }

  @Test
  void createWithRetentionHours() {
    database.command("sql",
        "CREATE TIMESERIES TYPE HourlyData TIMESTAMP ts FIELDS (value DOUBLE) RETENTION 24 HOURS");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("HourlyData");
    assertThat(tsType.getRetentionMs()).isEqualTo(24L * 3600000L);
  }

  @Test
  void createWithMultipleTags() {
    database.command("sql",
        "CREATE TIMESERIES TYPE MultiTag TIMESTAMP ts TAGS (region STRING, zone INTEGER) FIELDS (temp DOUBLE)");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("MultiTag");
    assertThat(tsType.getTsColumns()).hasSize(4); // ts + region + zone + temp

    // Verify roles
    assertThat(tsType.getTsColumns().get(0).getRole()).isEqualTo(ColumnDefinition.ColumnRole.TIMESTAMP);
    assertThat(tsType.getTsColumns().get(1).getRole()).isEqualTo(ColumnDefinition.ColumnRole.TAG);
    assertThat(tsType.getTsColumns().get(2).getRole()).isEqualTo(ColumnDefinition.ColumnRole.TAG);
    assertThat(tsType.getTsColumns().get(3).getRole()).isEqualTo(ColumnDefinition.ColumnRole.FIELD);
  }

  @Test
  void createIfNotExists() {
    database.command("sql", "CREATE TIMESERIES TYPE Existing TIMESTAMP ts FIELDS (value DOUBLE)");
    // Should not throw
    database.command("sql", "CREATE TIMESERIES TYPE Existing IF NOT EXISTS TIMESTAMP ts FIELDS (value DOUBLE)");

    assertThat(database.getSchema().existsType("Existing")).isTrue();
  }

  @Test
  void createWithPrecisionAndCompactionInterval() {
    // Reproduces the documented syntax from GitHub Discussion #3819:
    // PRECISION NANOSECOND and COMPACTION INTERVAL (with space) should be accepted
    database.command("sql", """
        CREATE TIMESERIES TYPE SensorReading
          TIMESTAMP ts PRECISION NANOSECOND
          TAGS (
            sensor_id STRING,
            location  STRING,
            floor     STRING
          )
          FIELDS (
            temperature DOUBLE,
            humidity    DOUBLE,
            pressure    DOUBLE
          )
          SHARDS 16
          RETENTION 90 DAYS
          COMPACTION INTERVAL 30 SECONDS
        """);

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("SensorReading");
    assertThat(tsType.getTimestampColumn()).isEqualTo("ts");
    assertThat(tsType.getPrecision()).isEqualTo("NANOSECOND");
    assertThat(tsType.getShardCount()).isEqualTo(16);
    assertThat(tsType.getRetentionMs()).isEqualTo(90L * 86400000L);
    // 30 SECONDS = 30 * 1000ms = 30000ms
    assertThat(tsType.getCompactionBucketIntervalMs()).isEqualTo(30_000L);
    assertThat(tsType.getTsColumns()).hasSize(7); // ts + 3 tags + 3 fields
  }

  @Test
  void createWithCompactionIntervalUnderscore() {
    // COMPACTION_INTERVAL with underscore should still work
    database.command("sql",
        "CREATE TIMESERIES TYPE MetricsA TIMESTAMP ts FIELDS (value DOUBLE) COMPACTION_INTERVAL 1 HOURS");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("MetricsA");
    assertThat(tsType.getCompactionBucketIntervalMs()).isEqualTo(3600000L);
  }

  @Test
  void createWithRetentionSeconds() {
    // SECONDS time unit should be accepted for RETENTION
    database.command("sql",
        "CREATE TIMESERIES TYPE SecData TIMESTAMP ts FIELDS (value DOUBLE) RETENTION 120 SECONDS");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("SecData");
    assertThat(tsType.getRetentionMs()).isEqualTo(120_000L);
  }

  @Test
  void createWithPrecisionMicrosecond() {
    database.command("sql",
        "CREATE TIMESERIES TYPE MicroData TIMESTAMP ts PRECISION MICROSECOND FIELDS (value DOUBLE)");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("MicroData");
    assertThat(tsType.getPrecision()).isEqualTo("MICROSECOND");
  }

  @Test
  void createWithPrecisionMillisecond() {
    database.command("sql",
        "CREATE TIMESERIES TYPE MilliData TIMESTAMP ts PRECISION MILLISECOND FIELDS (value DOUBLE)");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("MilliData");
    assertThat(tsType.getPrecision()).isEqualTo("MILLISECOND");
  }

  @Test
  void createWithPrecisionSecond() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SecPrecData TIMESTAMP ts PRECISION SECOND FIELDS (value DOUBLE)");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("SecPrecData");
    assertThat(tsType.getPrecision()).isEqualTo("SECOND");
  }

  @Test
  void createMinimal() {
    database.command("sql", "CREATE TIMESERIES TYPE Minimal TIMESTAMP ts FIELDS (value DOUBLE)");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("Minimal");
    assertThat(tsType.getTimestampColumn()).isEqualTo("ts");
    final int expectedShards = database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS);
    assertThat(tsType.getShardCount()).isEqualTo(expectedShards);
    assertThat(tsType.getRetentionMs()).isEqualTo(0L);
  }
}
