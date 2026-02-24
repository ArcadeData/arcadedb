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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ContinuousAggregateSQLTest extends TestHelper {

  @Test
  public void testCreateViaSql() {
    createSensorType();
    insertInitialData();

    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE hourly_temps AS " +
            "SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp " +
            "FROM SensorReading GROUP BY sensor_id, hour");

    assertThat(database.getSchema().existsContinuousAggregate("hourly_temps")).isTrue();
    assertThat(database.getSchema().existsType("hourly_temps")).isTrue();
  }

  @Test
  public void testCreateIfNotExistsViaSql() {
    createSensorType();
    insertInitialData();

    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE IF NOT EXISTS hourly_temps AS " +
            "SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp " +
            "FROM SensorReading GROUP BY sensor_id, hour");

    // Should not throw
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE IF NOT EXISTS hourly_temps AS " +
            "SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp " +
            "FROM SensorReading GROUP BY sensor_id, hour");

    assertThat(database.getSchema().existsContinuousAggregate("hourly_temps")).isTrue();
  }

  @Test
  public void testDropViaSql() {
    createSensorType();
    insertInitialData();

    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE hourly_temps AS " +
            "SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp " +
            "FROM SensorReading GROUP BY sensor_id, hour");

    database.command("sql", "DROP CONTINUOUS AGGREGATE hourly_temps");

    assertThat(database.getSchema().existsContinuousAggregate("hourly_temps")).isFalse();
  }

  @Test
  public void testDropIfExistsViaSql() {
    // Should not throw even if it doesn't exist
    database.command("sql", "DROP CONTINUOUS AGGREGATE IF EXISTS nonexistent");
  }

  @Test
  public void testRefreshViaSql() {
    createSensorType();
    insertInitialData();

    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE hourly_temps AS " +
            "SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp " +
            "FROM SensorReading GROUP BY sensor_id, hour");

    database.command("sql", "REFRESH CONTINUOUS AGGREGATE hourly_temps");

    assertThat(database.getSchema().getContinuousAggregate("hourly_temps").getStatus()).isEqualTo("VALID");
  }

  @Test
  public void testSelectFromSchemaMetadata() {
    createSensorType();
    insertInitialData();

    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE hourly_temps AS " +
            "SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp " +
            "FROM SensorReading GROUP BY sensor_id, hour");

    final ResultSet rs = database.query("sql", "SELECT FROM schema:continuousAggregates");
    final List<Result> results = collectResults(rs);
    assertThat(results).hasSize(1);

    final Result r = results.get(0);
    assertThat(r.<String>getProperty("name")).isEqualTo("hourly_temps");
    assertThat(r.<String>getProperty("sourceType")).isEqualTo("SensorReading");
    assertThat(r.<String>getProperty("bucketColumn")).isEqualTo("hour");
    assertThat(r.<Long>getProperty("bucketIntervalMs")).isEqualTo(3_600_000L);
    assertThat(r.<String>getProperty("status")).isEqualTo("VALID");
  }

  @Test
  public void testEndToEndIncrementalUpdate() {
    createSensorType();

    // Insert initial data (hour 0)
    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 1000, sensor_id = 'A', temperature = 20.0");
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 2000, sensor_id = 'A', temperature = 22.0");
    });

    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE hourly_temps AS " +
            "SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp " +
            "FROM SensorReading GROUP BY sensor_id, hour");

    // Verify initial aggregate
    List<Result> results = collectResults(database.query("sql", "SELECT FROM hourly_temps"));
    assertThat(results).hasSize(1);

    // Insert more data (hour 1)
    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 3600000, sensor_id = 'A', temperature = 30.0");
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 3601000, sensor_id = 'A', temperature = 32.0");
    });

    // Verify incrementally updated aggregate
    results = collectResults(database.query("sql", "SELECT FROM hourly_temps"));
    assertThat(results).hasSizeGreaterThanOrEqualTo(2);
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
