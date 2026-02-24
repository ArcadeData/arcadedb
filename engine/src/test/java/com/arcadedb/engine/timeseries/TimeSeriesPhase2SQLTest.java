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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * End-to-end SQL integration tests for all Phase 2 TimeSeries functions.
 */
public class TimeSeriesPhase2SQLTest extends TestHelper {

  @BeforeEach
  public void setupData() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SensorData TIMESTAMP ts TAGS (sensor STRING) FIELDS (value DOUBLE)");

    database.transaction(() -> {
      // Sensor A: linear increase 0..9 over 10 seconds
      for (int i = 0; i < 10; i++)
        database.command("sql",
            "INSERT INTO SensorData SET ts = " + (i * 1000) + ", sensor = 'A', value = " + (double) i);

      // Sensor B: decreasing 100..91 over 10 seconds
      for (int i = 0; i < 10; i++)
        database.command("sql",
            "INSERT INTO SensorData SET ts = " + (i * 1000) + ", sensor = 'B', value = " + (100.0 - i));
    });
  }

  @Test
  public void testTsFirstTsLastGroupBySensor() {
    final ResultSet rs = database.query("sql",
        "SELECT sensor, ts.first(value, ts) AS first_val, ts.last(value, ts) AS last_val " +
            "FROM SensorData GROUP BY sensor ORDER BY sensor");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
    // Sensor A: first=0, last=9
    assertThat(((Number) results.get(0).getProperty("first_val")).doubleValue()).isEqualTo(0.0);
    assertThat(((Number) results.get(0).getProperty("last_val")).doubleValue()).isEqualTo(9.0);
    // Sensor B: first=100, last=91
    assertThat(((Number) results.get(1).getProperty("first_val")).doubleValue()).isEqualTo(100.0);
    assertThat(((Number) results.get(1).getProperty("last_val")).doubleValue()).isEqualTo(91.0);
  }

  @Test
  public void testRateGroupBySensor() {
    final ResultSet rs = database.query("sql",
        "SELECT sensor, ts.rate(value, ts) AS r FROM SensorData GROUP BY sensor ORDER BY sensor");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
    // Sensor A: (9-0)/(9000ms) * 1000 = 1.0 per second
    assertThat(((Number) results.get(0).getProperty("r")).doubleValue()).isCloseTo(1.0, within(0.001));
    // Sensor B: (91-100)/(9000ms) * 1000 = -1.0 per second
    assertThat(((Number) results.get(1).getProperty("r")).doubleValue()).isCloseTo(-1.0, within(0.001));
  }

  @Test
  public void testDeltaGroupBySensor() {
    final ResultSet rs = database.query("sql",
        "SELECT sensor, ts.delta(value, ts) AS d FROM SensorData GROUP BY sensor ORDER BY sensor");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
    assertThat(((Number) results.get(0).getProperty("d")).doubleValue()).isEqualTo(9.0);
    assertThat(((Number) results.get(1).getProperty("d")).doubleValue()).isEqualTo(-9.0);
  }

  @Test
  public void testCorrelateAcrossSensors() {
    // Create a joined view with both sensors' values
    database.command("sql",
        "CREATE TIMESERIES TYPE JoinedData TIMESTAMP ts FIELDS (a DOUBLE, b DOUBLE)");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql",
            "INSERT INTO JoinedData SET ts = " + (i * 1000) + ", a = " + (double) i + ", b = " + (100.0 - i));
    });

    final ResultSet rs = database.query("sql", "SELECT ts.correlate(a, b) AS corr FROM JoinedData");
    assertThat(rs.hasNext()).isTrue();
    // Perfect negative correlation
    assertThat(((Number) rs.next().getProperty("corr")).doubleValue()).isCloseTo(-1.0, within(0.001));
  }

  @Test
  public void testMovingAvgOnSensor() {
    final ResultSet rs = database.query("sql",
        "SELECT ts.movingAvg(value, 3) AS ma FROM SensorData WHERE sensor = 'A'");

    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Double> ma = (List<Double>) rs.next().getProperty("ma");
    assertThat(ma).hasSize(10);
    // Position 2: avg(0,1,2) = 1.0
    assertThat(ma.get(2)).isCloseTo(1.0, within(0.001));
    // Position 9: avg(7,8,9) = 8.0
    assertThat(ma.get(9)).isCloseTo(8.0, within(0.001));
  }

  @Test
  public void testRateWithTimeBucket() {
    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('5s', ts) AS tb, ts.rate(value, ts) AS r " +
            "FROM SensorData WHERE sensor = 'A' GROUP BY tb ORDER BY tb");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
    // Both buckets should have rate of 1.0/s
    assertThat(((Number) results.get(0).getProperty("r")).doubleValue()).isCloseTo(1.0, within(0.001));
    assertThat(((Number) results.get(1).getProperty("r")).doubleValue()).isCloseTo(1.0, within(0.001));
  }

  @Test
  public void testAllFunctionsTogether() {
    // Single query using ts_first, ts_last, rate, and delta
    final ResultSet rs = database.query("sql",
        "SELECT sensor, ts.first(value, ts) AS first_val, ts.last(value, ts) AS last_val, " +
            "ts.rate(value, ts) AS r, ts.delta(value, ts) AS d " +
            "FROM SensorData GROUP BY sensor ORDER BY sensor");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
    final Result a = results.get(0);
    assertThat(((Number) a.getProperty("first_val")).doubleValue()).isEqualTo(0.0);
    assertThat(((Number) a.getProperty("last_val")).doubleValue()).isEqualTo(9.0);
    assertThat(((Number) a.getProperty("r")).doubleValue()).isCloseTo(1.0, within(0.001));
    assertThat(((Number) a.getProperty("d")).doubleValue()).isEqualTo(9.0);
  }
}
