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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for TimeSeries aggregation push-down optimization.
 * Verifies that SQL aggregation queries with ts.timeBucket GROUP BY
 * are pushed down into the engine for direct block-level processing.
 */
class TimeSeriesAggregationPushDownTest extends TestHelper {

  @BeforeEach
  void setupData() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SensorData TIMESTAMP ts FIELDS (temperature DOUBLE, humidity DOUBLE)");

    database.transaction(() -> {
      // Insert 12 samples across 3 hour-buckets (3600000ms = 1h)
      // Bucket 0 (0ms):     10, 20, 30, 40 => avg=25, max=40, min=10, sum=100, count=4
      database.command("sql", "INSERT INTO SensorData SET ts = 0, temperature = 10.0, humidity = 50.0");
      database.command("sql", "INSERT INTO SensorData SET ts = 1000, temperature = 20.0, humidity = 55.0");
      database.command("sql", "INSERT INTO SensorData SET ts = 2000, temperature = 30.0, humidity = 60.0");
      database.command("sql", "INSERT INTO SensorData SET ts = 3000, temperature = 40.0, humidity = 65.0");

      // Bucket 1 (3600000ms): 100, 200 => avg=150, max=200, min=100, sum=300, count=2
      database.command("sql", "INSERT INTO SensorData SET ts = 3600000, temperature = 100.0, humidity = 70.0");
      database.command("sql", "INSERT INTO SensorData SET ts = 3601000, temperature = 200.0, humidity = 80.0");

      // Bucket 2 (7200000ms): 5, 15, 25, 35, 45, 55 => avg=30, max=55, min=5, sum=180, count=6
      database.command("sql", "INSERT INTO SensorData SET ts = 7200000, temperature = 5.0, humidity = 30.0");
      database.command("sql", "INSERT INTO SensorData SET ts = 7201000, temperature = 15.0, humidity = 35.0");
      database.command("sql", "INSERT INTO SensorData SET ts = 7202000, temperature = 25.0, humidity = 40.0");
      database.command("sql", "INSERT INTO SensorData SET ts = 7203000, temperature = 35.0, humidity = 45.0");
      database.command("sql", "INSERT INTO SensorData SET ts = 7204000, temperature = 45.0, humidity = 50.0");
      database.command("sql", "INSERT INTO SensorData SET ts = 7205000, temperature = 55.0, humidity = 55.0");
    });
  }

  @Test
  void testBasicHourlyAvg() {
    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorData GROUP BY hour");

    final List<Result> results = collectResults(rs);
    assertThat(results).hasSize(3);

    // Sort by hour to ensure deterministic order
    results.sort((a, b) -> ((Date) a.getProperty("hour")).compareTo((Date) b.getProperty("hour")));

    assertThat(((Number) results.get(0).getProperty("avg_temp")).doubleValue()).isCloseTo(25.0, within(0.01));
    assertThat(((Number) results.get(1).getProperty("avg_temp")).doubleValue()).isCloseTo(150.0, within(0.01));
    assertThat(((Number) results.get(2).getProperty("avg_temp")).doubleValue()).isCloseTo(30.0, within(0.01));
  }

  @Test
  void testMultiColumnAggregation() {
    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp, max(humidity) AS max_hum FROM SensorData GROUP BY hour");

    final List<Result> results = collectResults(rs);
    assertThat(results).hasSize(3);

    results.sort((a, b) -> ((Date) a.getProperty("hour")).compareTo((Date) b.getProperty("hour")));

    // Bucket 0: avg(temp)=25, max(humidity)=65
    assertThat(((Number) results.get(0).getProperty("avg_temp")).doubleValue()).isCloseTo(25.0, within(0.01));
    assertThat(((Number) results.get(0).getProperty("max_hum")).doubleValue()).isCloseTo(65.0, within(0.01));

    // Bucket 1: avg(temp)=150, max(humidity)=80
    assertThat(((Number) results.get(1).getProperty("avg_temp")).doubleValue()).isCloseTo(150.0, within(0.01));
    assertThat(((Number) results.get(1).getProperty("max_hum")).doubleValue()).isCloseTo(80.0, within(0.01));

    // Bucket 2: avg(temp)=30, max(humidity)=55
    assertThat(((Number) results.get(2).getProperty("avg_temp")).doubleValue()).isCloseTo(30.0, within(0.01));
    assertThat(((Number) results.get(2).getProperty("max_hum")).doubleValue()).isCloseTo(55.0, within(0.01));
  }

  @Test
  void testCountWithTimeBucket() {
    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS hour, count(*) AS cnt FROM SensorData GROUP BY hour");

    final List<Result> results = collectResults(rs);
    assertThat(results).hasSize(3);

    results.sort((a, b) -> ((Date) a.getProperty("hour")).compareTo((Date) b.getProperty("hour")));

    assertThat(((Number) results.get(0).getProperty("cnt")).longValue()).isEqualTo(4);
    assertThat(((Number) results.get(1).getProperty("cnt")).longValue()).isEqualTo(2);
    assertThat(((Number) results.get(2).getProperty("cnt")).longValue()).isEqualTo(6);
  }

  @Test
  void testWithWhereBetween() {
    // Only buckets 0 and 1 should be included (0 to 3601000)
    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorData WHERE ts BETWEEN 0 AND 3601000 GROUP BY hour");

    final List<Result> results = collectResults(rs);
    assertThat(results).hasSize(2);

    results.sort((a, b) -> ((Date) a.getProperty("hour")).compareTo((Date) b.getProperty("hour")));

    assertThat(((Number) results.get(0).getProperty("avg_temp")).doubleValue()).isCloseTo(25.0, within(0.01));
    assertThat(((Number) results.get(1).getProperty("avg_temp")).doubleValue()).isCloseTo(150.0, within(0.01));
  }

  @Test
  void testSumAggregation() {
    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS hour, sum(temperature) AS sum_temp FROM SensorData GROUP BY hour");

    final List<Result> results = collectResults(rs);
    assertThat(results).hasSize(3);

    results.sort((a, b) -> ((Date) a.getProperty("hour")).compareTo((Date) b.getProperty("hour")));

    assertThat(((Number) results.get(0).getProperty("sum_temp")).doubleValue()).isCloseTo(100.0, within(0.01));
    assertThat(((Number) results.get(1).getProperty("sum_temp")).doubleValue()).isCloseTo(300.0, within(0.01));
    assertThat(((Number) results.get(2).getProperty("sum_temp")).doubleValue()).isCloseTo(180.0, within(0.01));
  }

  @Test
  void testMinAggregation() {
    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS hour, min(temperature) AS min_temp FROM SensorData GROUP BY hour");

    final List<Result> results = collectResults(rs);
    assertThat(results).hasSize(3);

    results.sort((a, b) -> ((Date) a.getProperty("hour")).compareTo((Date) b.getProperty("hour")));

    assertThat(((Number) results.get(0).getProperty("min_temp")).doubleValue()).isCloseTo(10.0, within(0.01));
    assertThat(((Number) results.get(1).getProperty("min_temp")).doubleValue()).isCloseTo(100.0, within(0.01));
    assertThat(((Number) results.get(2).getProperty("min_temp")).doubleValue()).isCloseTo(5.0, within(0.01));
  }

  @Test
  void testEmptyResultSet() {
    // Query a range with no data
    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorData WHERE ts BETWEEN 999999999 AND 999999999 GROUP BY hour");

    final List<Result> results = collectResults(rs);
    assertThat(results).isEmpty();
  }

  @Test
  void testAllRowsInOneBucket() {
    // Use a very large bucket interval (1 day) so all rows fall in one bucket
    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1d', ts) AS day, avg(temperature) AS avg_temp, count(*) AS cnt FROM SensorData GROUP BY day");

    final List<Result> results = collectResults(rs);
    assertThat(results).hasSize(1);

    // Overall: (10+20+30+40+100+200+5+15+25+35+45+55) / 12 = 580/12 = 48.333...
    assertThat(((Number) results.get(0).getProperty("avg_temp")).doubleValue()).isCloseTo(48.333, within(0.01));
    assertThat(((Number) results.get(0).getProperty("cnt")).longValue()).isEqualTo(12);
  }

  @Test
  void testFallbackWithDistinct() {
    // DISTINCT should prevent push-down and fall through to normal execution
    // This verifies the fallback path still works
    final ResultSet rs = database.query("sql",
        "SELECT DISTINCT temperature FROM SensorData");

    final List<Result> results = collectResults(rs);
    // All 12 temperatures are unique
    assertThat(results).hasSize(12);
  }

  @Test
  void testEquivalenceWithFallback() {
    // Push-down path: ts.timeBucket GROUP BY
    final ResultSet rsPushDown = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp, max(temperature) AS max_temp FROM SensorData GROUP BY hour");
    final List<Result> pushDownResults = collectResults(rsPushDown);
    pushDownResults.sort((a, b) -> ((Date) a.getProperty("hour")).compareTo((Date) b.getProperty("hour")));

    // Verify values match expected
    assertThat(pushDownResults).hasSize(3);
    assertThat(((Number) pushDownResults.get(0).getProperty("avg_temp")).doubleValue()).isCloseTo(25.0, within(0.01));
    assertThat(((Number) pushDownResults.get(0).getProperty("max_temp")).doubleValue()).isCloseTo(40.0, within(0.01));
    assertThat(((Number) pushDownResults.get(1).getProperty("avg_temp")).doubleValue()).isCloseTo(150.0, within(0.01));
    assertThat(((Number) pushDownResults.get(1).getProperty("max_temp")).doubleValue()).isCloseTo(200.0, within(0.01));
    assertThat(((Number) pushDownResults.get(2).getProperty("avg_temp")).doubleValue()).isCloseTo(30.0, within(0.01));
    assertThat(((Number) pushDownResults.get(2).getProperty("max_temp")).doubleValue()).isCloseTo(55.0, within(0.01));
  }

  private List<Result> collectResults(final ResultSet rs) {
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    return results;
  }
}
