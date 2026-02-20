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

/**
 * End-to-end SQL tests for TimeSeries INSERT and SELECT.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeSeriesSQLTest extends TestHelper {

  @Test
  public void testInsertAndSelectAll() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");

    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 1000, sensor_id = 'A', temperature = 22.5");
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 2000, sensor_id = 'B', temperature = 23.1");
      database.command("sql",
          "INSERT INTO SensorReading SET ts = 3000, sensor_id = 'A', temperature = 21.8");
    });

    final ResultSet rs = database.query("sql", "SELECT FROM SensorReading");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3);
  }

  @Test
  public void testSelectWithBetween() {
    database.command("sql",
        "CREATE TIMESERIES TYPE TempData TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO TempData SET ts = 1000, value = 10.0");
      database.command("sql", "INSERT INTO TempData SET ts = 2000, value = 20.0");
      database.command("sql", "INSERT INTO TempData SET ts = 3000, value = 30.0");
      database.command("sql", "INSERT INTO TempData SET ts = 4000, value = 40.0");
      database.command("sql", "INSERT INTO TempData SET ts = 5000, value = 50.0");
    });

    final ResultSet rs = database.query("sql", "SELECT FROM TempData WHERE ts BETWEEN 2000 AND 4000");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3); // 2000, 3000, 4000
  }

  @Test
  public void testInsertWithSET() {
    database.command("sql",
        "CREATE TIMESERIES TYPE DeviceMetrics TIMESTAMP ts TAGS (device STRING) FIELDS (cpu DOUBLE, mem LONG)");

    database.transaction(() -> {
      database.command("sql",
          "INSERT INTO DeviceMetrics SET ts = 1000, device = 'server1', cpu = 75.5, mem = 8192");
    });

    final ResultSet rs = database.query("sql", "SELECT FROM DeviceMetrics");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat((String) row.getProperty("device")).isEqualTo("server1");
    assertThat(((Number) row.getProperty("cpu")).doubleValue()).isEqualTo(75.5);
  }

  @Test
  public void testTimeBucketFunction() {
    database.command("sql",
        "CREATE TIMESERIES TYPE HourlyMetrics TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      // Insert samples at different times within the same hour bucket (3600000ms = 1 hour)
      database.command("sql", "INSERT INTO HourlyMetrics SET ts = 3600000, value = 10.0");
      database.command("sql", "INSERT INTO HourlyMetrics SET ts = 3601000, value = 20.0");
      database.command("sql", "INSERT INTO HourlyMetrics SET ts = 3602000, value = 30.0");
    });

    // Query using time_bucket function - this is a standard SQL function call
    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS hour, avg(value) AS avg_val FROM HourlyMetrics GROUP BY hour");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(1); // All in same hour bucket
    assertThat(((Number) results.get(0).getProperty("avg_val")).doubleValue()).isEqualTo(20.0);
  }
}
