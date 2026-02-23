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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for TimeSeries schema type integration.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeSeriesTypeTest extends TestHelper {

  @Test
  public void testCreateTimeSeriesType() {
    final LocalTimeSeriesType type = database.getSchema().buildTimeSeriesType()
        .withName("SensorData")
        .withTimestamp("ts")
        .withTag("sensor_id", Type.STRING)
        .withField("temperature", Type.DOUBLE)
        .withShards(2)
        .withRetention(86400000L)
        .create();

    assertThat(type).isNotNull();
    assertThat(type.getName()).isEqualTo("SensorData");
    assertThat(type.getTimestampColumn()).isEqualTo("ts");
    assertThat(type.getShardCount()).isEqualTo(2);
    assertThat(type.getRetentionMs()).isEqualTo(86400000L);
    assertThat(type.getTsColumns()).hasSize(3);
    assertThat(type.getEngine()).isNotNull();

    // Verify properties registered
    assertThat(type.existsProperty("ts")).isTrue();
    assertThat(type.existsProperty("sensor_id")).isTrue();
    assertThat(type.existsProperty("temperature")).isTrue();

    // Verify type is in schema
    assertThat(database.getSchema().existsType("SensorData")).isTrue();
    final DocumentType fromSchema = database.getSchema().getType("SensorData");
    assertThat(fromSchema).isInstanceOf(LocalTimeSeriesType.class);
  }

  @Test
  public void testTimeSeriesTypeJSON() {
    final LocalTimeSeriesType type = database.getSchema().buildTimeSeriesType()
        .withName("Metrics")
        .withTimestamp("ts")
        .withTag("host", Type.STRING)
        .withField("cpu", Type.DOUBLE)
        .withField("mem", Type.LONG)
        .withShards(1)
        .create();

    final var json = type.toJSON();
    assertThat(json.getString("type")).isEqualTo("t");
    assertThat(json.getString("timestampColumn")).isEqualTo("ts");
    assertThat(json.getInt("shardCount")).isEqualTo(1);
    assertThat(json.getJSONArray("tsColumns").length()).isEqualTo(4);
  }

  @Test
  public void testTimeSeriesTypePersistence() {
    database.getSchema().buildTimeSeriesType()
        .withName("PersistentTS")
        .withTimestamp("ts")
        .withTag("device", Type.STRING)
        .withField("value", Type.DOUBLE)
        .withShards(2)
        .withRetention(3600000L)
        .create();

    // Close and reopen
    final String dbPath = database.getDatabasePath();
    database.close();

    database = new DatabaseFactory(dbPath).open();

    assertThat(database.getSchema().existsType("PersistentTS")).isTrue();
    final DocumentType reloaded = database.getSchema().getType("PersistentTS");
    assertThat(reloaded).isInstanceOf(LocalTimeSeriesType.class);

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) reloaded;
    assertThat(tsType.getTimestampColumn()).isEqualTo("ts");
    assertThat(tsType.getShardCount()).isEqualTo(2);
    assertThat(tsType.getRetentionMs()).isEqualTo(3600000L);
    assertThat(tsType.getTsColumns()).hasSize(3);

    // Verify column roles restored correctly
    final ColumnDefinition tsCol = tsType.getTsColumns().get(0);
    assertThat(tsCol.getName()).isEqualTo("ts");
    assertThat(tsCol.getRole()).isEqualTo(ColumnDefinition.ColumnRole.TIMESTAMP);

    final ColumnDefinition tagCol = tsType.getTsColumns().get(1);
    assertThat(tagCol.getName()).isEqualTo("device");
    assertThat(tagCol.getRole()).isEqualTo(ColumnDefinition.ColumnRole.TAG);

    final ColumnDefinition fieldCol = tsType.getTsColumns().get(2);
    assertThat(fieldCol.getName()).isEqualTo("value");
    assertThat(fieldCol.getRole()).isEqualTo(ColumnDefinition.ColumnRole.FIELD);
  }

  @Test
  public void testColumnDefinitions() {
    final LocalTimeSeriesType type = database.getSchema().buildTimeSeriesType()
        .withName("AllTypes")
        .withTimestamp("ts")
        .withTag("region", Type.STRING)
        .withTag("zone", Type.INTEGER)
        .withField("temp", Type.DOUBLE)
        .withField("count", Type.LONG)
        .create();

    assertThat(type.getTsColumns()).hasSize(5);

    // Verify timestamp column
    assertThat(type.getTsColumns().get(0).getRole()).isEqualTo(ColumnDefinition.ColumnRole.TIMESTAMP);
    assertThat(type.getTsColumns().get(0).getDataType()).isEqualTo(Type.LONG);

    // Verify tags
    assertThat(type.getTsColumns().get(1).getRole()).isEqualTo(ColumnDefinition.ColumnRole.TAG);
    assertThat(type.getTsColumns().get(2).getRole()).isEqualTo(ColumnDefinition.ColumnRole.TAG);

    // Verify fields
    assertThat(type.getTsColumns().get(3).getRole()).isEqualTo(ColumnDefinition.ColumnRole.FIELD);
    assertThat(type.getTsColumns().get(4).getRole()).isEqualTo(ColumnDefinition.ColumnRole.FIELD);
  }

  @Test
  public void testDefaultShardCountMatchesAsyncWorkerThreads() {
    final int expectedShards = database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS);

    final LocalTimeSeriesType type = database.getSchema().buildTimeSeriesType()
        .withName("DefaultShards")
        .withTimestamp("ts")
        .withField("value", Type.DOUBLE)
        .create();

    assertThat(type.getShardCount()).isEqualTo(expectedShards);
    assertThat(type.getEngine().getShardCount()).isEqualTo(expectedShards);
  }
}
