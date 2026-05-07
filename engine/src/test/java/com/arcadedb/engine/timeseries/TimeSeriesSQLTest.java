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
class TimeSeriesSQLTest extends TestHelper {

  @Test
  void insertAndSelectAll() {
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
  void selectWithBetween() {
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
  void insertWithSET() {
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

  /**
   * Regression test for issue #4128: combining "=" and LIKE on a TAG column with OR
   * was returning 0 results because the time-series planner pushed down the equality
   * branch as if it were a global AND condition, dropping rows that should have matched
   * any of the LIKE branches.
   */
  @Test
  void selectTagsEqualityOrLikeCsv() {
    database.command("sql",
        "CREATE TIMESERIES TYPE TaggedMetric TIMESTAMP ts TAGS (tags STRING) FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO TaggedMetric SET ts = 1000, tags = 'CI', value = 1.0");
      database.command("sql", "INSERT INTO TaggedMetric SET ts = 2000, tags = 'CI,slow', value = 2.0");
      database.command("sql", "INSERT INTO TaggedMetric SET ts = 3000, tags = '1m,CI,fast', value = 3.0");
      database.command("sql", "INSERT INTO TaggedMetric SET ts = 4000, tags = 'fast,CI', value = 4.0");
      database.command("sql", "INSERT INTO TaggedMetric SET ts = 5000, tags = 'other', value = 5.0");
    });

    // Mixing = and LIKE in OR must return all 4 rows tagged with CI in any position.
    final ResultSet rs = database.query("sql",
        "SELECT FROM TaggedMetric WHERE ts BETWEEN 0 AND 10000 AND ("
            + "tags = 'CI' OR tags LIKE 'CI,%' OR tags LIKE '%,CI' OR tags LIKE '%,CI,%')");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);

    // Only-LIKE form (the workaround in the bug report) should also return 4 rows.
    final ResultSet rs2 = database.query("sql",
        "SELECT FROM TaggedMetric WHERE ts BETWEEN 0 AND 10000 AND ("
            + "tags LIKE 'CI' OR tags LIKE 'CI,%' OR tags LIKE '%,CI' OR tags LIKE '%,CI,%')");
    final List<Result> results2 = new ArrayList<>();
    while (rs2.hasNext())
      results2.add(rs2.next());

    assertThat(results2).hasSize(4);
  }

  /**
   * Push-down sanity: when the WHERE has a single equality on a tag, the engine
   * still uses the optimized tag filter and returns the correct row.
   */
  @Test
  void selectSingleTagEqualityPushDown() {
    database.command("sql",
        "CREATE TIMESERIES TYPE PushDownMetric TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO PushDownMetric SET ts = 1000, host = 'h1', value = 1.0");
      database.command("sql", "INSERT INTO PushDownMetric SET ts = 2000, host = 'h2', value = 2.0");
      database.command("sql", "INSERT INTO PushDownMetric SET ts = 3000, host = 'h1', value = 3.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT FROM PushDownMetric WHERE ts BETWEEN 0 AND 10000 AND host = 'h1'");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
  }

  /**
   * Push-down for OR of equalities on the same tag column: must return the union.
   */
  @Test
  void selectTagOrEqualitiesPushDown() {
    database.command("sql",
        "CREATE TIMESERIES TYPE OrEqMetric TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO OrEqMetric SET ts = 1000, host = 'h1', value = 1.0");
      database.command("sql", "INSERT INTO OrEqMetric SET ts = 2000, host = 'h2', value = 2.0");
      database.command("sql", "INSERT INTO OrEqMetric SET ts = 3000, host = 'h3', value = 3.0");
      database.command("sql", "INSERT INTO OrEqMetric SET ts = 4000, host = 'h1', value = 4.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT FROM OrEqMetric WHERE ts BETWEEN 0 AND 10000 AND (host = 'h1' OR host = 'h2')");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3);
  }

  @Test
  void timeBucketFunction() {
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
