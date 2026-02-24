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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.ContinuousAggregate;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ContinuousAggregateTest extends TestHelper {

  @Test
  public void testCreateAndInitialPopulation() {
    createSensorType();
    insertInitialData();

    final ContinuousAggregate ca = database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    assertThat(ca.getName()).isEqualTo("hourly_temps");
    assertThat(ca.getSourceTypeName()).isEqualTo("SensorReading");
    assertThat(ca.getBucketColumn()).isEqualTo("hour");
    assertThat(ca.getBucketIntervalMs()).isEqualTo(3_600_000L);
    assertThat(ca.getStatus()).isEqualTo("VALID");
    assertThat(ca.getWatermarkTs()).isGreaterThanOrEqualTo(0);

    // Verify backing type has data
    final ResultSet rs = database.query("sql", "SELECT FROM hourly_temps");
    final List<Result> results = collectResults(rs);
    assertThat(results).isNotEmpty();
  }

  @Test
  public void testIncrementalRefreshOnInsert() {
    createSensorType();
    insertInitialData();

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    final long initialWatermark = database.getSchema().getContinuousAggregate("hourly_temps").getWatermarkTs();

    // Insert new data in a later hour
    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 7200000, sensor_id = 'A', temperature = 30.0");
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 7201000, sensor_id = 'A', temperature = 32.0");
    });

    // The post-commit callback should have triggered an incremental refresh
    final ContinuousAggregate ca = database.getSchema().getContinuousAggregate("hourly_temps");
    assertThat(ca.getWatermarkTs()).isGreaterThanOrEqualTo(initialWatermark);
    assertThat(ca.getStatus()).isEqualTo("VALID");

    // Verify the new bucket data exists
    final ResultSet rs = database.query("sql",
        "SELECT FROM hourly_temps WHERE hour >= ?", 7200000L);
    final List<Result> results = collectResults(rs);
    assertThat(results).isNotEmpty();
  }

  @Test
  public void testWatermarkAdvances() {
    createSensorType();

    // Insert data at hour 0
    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 100, sensor_id = 'A', temperature = 20.0");
    });

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    final long wm1 = database.getSchema().getContinuousAggregate("hourly_temps").getWatermarkTs();
    assertThat(wm1).isEqualTo(0L); // bucket start for ts=100 with 1h interval is 0

    // Insert data at hour 1
    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 3600000, sensor_id = 'A', temperature = 25.0");
    });

    final long wm2 = database.getSchema().getContinuousAggregate("hourly_temps").getWatermarkTs();
    assertThat(wm2).isGreaterThanOrEqualTo(wm1);
  }

  @Test
  public void testDropContinuousAggregate() {
    createSensorType();
    insertInitialData();

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    assertThat(database.getSchema().existsContinuousAggregate("hourly_temps")).isTrue();
    assertThat(database.getSchema().existsType("hourly_temps")).isTrue();

    database.getSchema().dropContinuousAggregate("hourly_temps");

    assertThat(database.getSchema().existsContinuousAggregate("hourly_temps")).isFalse();
    assertThat(database.getSchema().existsType("hourly_temps")).isFalse();
  }

  @Test
  public void testIfNotExistsIdempotent() {
    createSensorType();
    insertInitialData();

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    // Should not throw
    final ContinuousAggregate ca2 = database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .withIgnoreIfExists(true)
        .create();

    assertThat(ca2.getName()).isEqualTo("hourly_temps");
  }

  @Test
  public void testManualRefresh() {
    createSensorType();
    insertInitialData();

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    final ContinuousAggregate ca = database.getSchema().getContinuousAggregate("hourly_temps");
    final long countBefore = ca.getRefreshCount();

    ca.refresh();

    assertThat(ca.getRefreshCount()).isGreaterThan(countBefore);
    assertThat(ca.getStatus()).isEqualTo("VALID");
  }

  @Test
  public void testSchemaPersistence() {
    createSensorType();
    insertInitialData();

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    // Close and reopen
    final String dbPath = database.getDatabasePath();
    database.close();
    database = factory.open();

    assertThat(database.getSchema().existsContinuousAggregate("hourly_temps")).isTrue();
    final ContinuousAggregate ca = database.getSchema().getContinuousAggregate("hourly_temps");
    assertThat(ca.getSourceTypeName()).isEqualTo("SensorReading");
    assertThat(ca.getBucketColumn()).isEqualTo("hour");
  }

  @Test
  public void testInvalidQueryNoTimeBucket() {
    createSensorType();

    assertThatThrownBy(() ->
        database.getSchema().buildContinuousAggregate()
            .withName("bad_ca")
            .withQuery("SELECT sensor_id, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id")
            .create()
    ).isInstanceOf(SchemaException.class)
        .hasMessageContaining("ts.timeBucket");
  }

  @Test
  public void testInvalidQueryNonTimeSeriesSource() {
    database.getSchema().buildDocumentType().withName("RegularDoc").create();

    assertThatThrownBy(() ->
        database.getSchema().buildContinuousAggregate()
            .withName("bad_ca")
            .withQuery("SELECT ts.timeBucket('1h', ts) AS hour, count(*) AS cnt FROM RegularDoc GROUP BY hour")
            .create()
    ).isInstanceOf(SchemaException.class)
        .hasMessageContaining("not a TimeSeries type");
  }

  @Test
  public void testInvalidQueryNoGroupBy() {
    createSensorType();

    assertThatThrownBy(() ->
        database.getSchema().buildContinuousAggregate()
            .withName("bad_ca")
            .withQuery("SELECT ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading")
            .create()
    ).isInstanceOf(SchemaException.class)
        .hasMessageContaining("GROUP BY");
  }

  @Test
  public void testGetContinuousAggregates() {
    createSensorType();
    insertInitialData();

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    final ContinuousAggregate[] aggregates = database.getSchema().getContinuousAggregates();
    assertThat(aggregates).hasSize(1);
    assertThat(aggregates[0].getName()).isEqualTo("hourly_temps");
  }

  @Test
  public void testProtectSourceTypeFromDrop() {
    createSensorType();
    insertInitialData();

    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    assertThatThrownBy(() -> database.getSchema().dropType("SensorReading"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("continuous aggregate");
  }

  private void createSensorType() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
  }

  private void insertInitialData() {
    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 1000, sensor_id = 'A', temperature = 22.5");
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 2000, sensor_id = 'B', temperature = 23.1");
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 3000, sensor_id = 'A', temperature = 21.8");
    });
  }

  private List<Result> collectResults(final ResultSet rs) {
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    return results;
  }
}
