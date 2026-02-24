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
public class CreateTimeSeriesTypeStatementTest extends TestHelper {

  @Test
  public void testBasicCreateTimeSeriesType() {
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
  public void testCreateWithShardsAndRetention() {
    database.command("sql",
        "CREATE TIMESERIES TYPE Metrics TIMESTAMP ts TAGS (host STRING) FIELDS (cpu DOUBLE, mem LONG) SHARDS 4 RETENTION 90 DAYS");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("Metrics");
    assertThat(tsType.getShardCount()).isEqualTo(4);
    assertThat(tsType.getRetentionMs()).isEqualTo(90L * 86400000L);
    assertThat(tsType.getTsColumns()).hasSize(4); // ts + host + cpu + mem
  }

  @Test
  public void testCreateWithRetentionHours() {
    database.command("sql",
        "CREATE TIMESERIES TYPE HourlyData TIMESTAMP ts FIELDS (value DOUBLE) RETENTION 24 HOURS");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("HourlyData");
    assertThat(tsType.getRetentionMs()).isEqualTo(24L * 3600000L);
  }

  @Test
  public void testCreateWithMultipleTags() {
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
  public void testCreateIfNotExists() {
    database.command("sql", "CREATE TIMESERIES TYPE Existing TIMESTAMP ts FIELDS (value DOUBLE)");
    // Should not throw
    database.command("sql", "CREATE TIMESERIES TYPE Existing IF NOT EXISTS TIMESTAMP ts FIELDS (value DOUBLE)");

    assertThat(database.getSchema().existsType("Existing")).isTrue();
  }

  @Test
  public void testCreateMinimal() {
    database.command("sql", "CREATE TIMESERIES TYPE Minimal TIMESTAMP ts FIELDS (value DOUBLE)");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("Minimal");
    assertThat(tsType.getTimestampColumn()).isEqualTo("ts");
    final int expectedShards = database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS);
    assertThat(tsType.getShardCount()).isEqualTo(expectedShards);
    assertThat(tsType.getRetentionMs()).isEqualTo(0L);
  }
}
