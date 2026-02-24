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
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for gap analysis features: counter reset in ts.rate(), time range operators,
 * multi-tag filtering, linear interpolation, ts.percentile.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesGapAnalysisTest extends TestHelper {

  // ===== Counter Reset Handling in ts.rate() =====

  @Test
  void testRateWithCounterReset() {
    database.command("sql",
        "CREATE TIMESERIES TYPE ResetCounter TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      // Counter goes 0 -> 100 then resets to 0 -> 50
      database.command("sql", "INSERT INTO ResetCounter SET ts = 0,    value = 0.0");
      database.command("sql", "INSERT INTO ResetCounter SET ts = 1000, value = 50.0");
      database.command("sql", "INSERT INTO ResetCounter SET ts = 2000, value = 100.0");
      // Counter reset here
      database.command("sql", "INSERT INTO ResetCounter SET ts = 3000, value = 10.0");
      database.command("sql", "INSERT INTO ResetCounter SET ts = 4000, value = 50.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.rate(value, ts, true) AS r FROM ResetCounter");
    assertThat(rs.hasNext()).isTrue();
    final double rate = ((Number) rs.next().getProperty("r")).doubleValue();
    // Total increase: 50 + 50 (0->50->100) + 10 + 40 (reset: 10 from 0, then +40) = 150
    // Over 4 seconds => 150/4 = 37.5/s
    assertThat(rate).isCloseTo(37.5, within(0.01));
  }

  @Test
  void testRateWithMultipleResets() {
    database.command("sql",
        "CREATE TIMESERIES TYPE MultiResetCounter TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO MultiResetCounter SET ts = 0,    value = 0.0");
      database.command("sql", "INSERT INTO MultiResetCounter SET ts = 1000, value = 100.0");
      // Reset 1
      database.command("sql", "INSERT INTO MultiResetCounter SET ts = 2000, value = 20.0");
      // Reset 2
      database.command("sql", "INSERT INTO MultiResetCounter SET ts = 3000, value = 10.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.rate(value, ts, true) AS r FROM MultiResetCounter");
    assertThat(rs.hasNext()).isTrue();
    final double rate = ((Number) rs.next().getProperty("r")).doubleValue();
    // Total increase: 100 (0->100) + 20 (reset, +20) + 10 (reset, +10) = 130
    // Over 3 seconds => 130/3 ≈ 43.33/s
    assertThat(rate).isCloseTo(130.0 / 3.0, within(0.01));
  }

  @Test
  void testRateWithoutResetDetection() {
    database.command("sql",
        "CREATE TIMESERIES TYPE NoResetCounter TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO NoResetCounter SET ts = 0,    value = 0.0");
      database.command("sql", "INSERT INTO NoResetCounter SET ts = 1000, value = 10.0");
      database.command("sql", "INSERT INTO NoResetCounter SET ts = 2000, value = 20.0");
    });

    // Without counter reset detection (default), simple rate = (last - first) / time
    final ResultSet rs = database.query("sql", "SELECT ts.rate(value, ts) AS r FROM NoResetCounter");
    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("r")).doubleValue()).isEqualTo(10.0);
  }

  @Test
  void testRateDecreasingWithoutResetDetection() {
    database.command("sql",
        "CREATE TIMESERIES TYPE DecGauge TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO DecGauge SET ts = 0,    value = 100.0");
      database.command("sql", "INSERT INTO DecGauge SET ts = 2000, value = 80.0");
    });

    // Without counter reset detection, decreasing values produce negative rate
    final ResultSet rs = database.query("sql", "SELECT ts.rate(value, ts) AS r FROM DecGauge");
    assertThat(((Number) rs.next().getProperty("r")).doubleValue()).isEqualTo(-10.0);
  }

  // ===== Time Range Operators =====

  @Test
  void testGreaterThanOperator() {
    database.command("sql",
        "CREATE TIMESERIES TYPE GtSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO GtSensor SET ts = " + (i * 1000) + ", value = " + (double) i);
    });

    // ts > 5000 should return timestamps 6000, 7000, 8000, 9000
    final ResultSet rs = database.query("sql", "SELECT value FROM GtSensor WHERE ts > 5000");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    assertThat(results).hasSize(4);
  }

  @Test
  void testGreaterThanOrEqualOperator() {
    database.command("sql",
        "CREATE TIMESERIES TYPE GeSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO GeSensor SET ts = " + (i * 1000) + ", value = " + (double) i);
    });

    // ts >= 5000 should return timestamps 5000, 6000, 7000, 8000, 9000
    final ResultSet rs = database.query("sql", "SELECT value FROM GeSensor WHERE ts >= 5000");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    assertThat(results).hasSize(5);
  }

  @Test
  void testLessThanOperator() {
    database.command("sql",
        "CREATE TIMESERIES TYPE LtSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO LtSensor SET ts = " + (i * 1000) + ", value = " + (double) i);
    });

    // ts < 3000 should return timestamps 0, 1000, 2000
    final ResultSet rs = database.query("sql", "SELECT value FROM LtSensor WHERE ts < 3000");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    assertThat(results).hasSize(3);
  }

  @Test
  void testCombinedRangeOperators() {
    database.command("sql",
        "CREATE TIMESERIES TYPE CombinedSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO CombinedSensor SET ts = " + (i * 1000) + ", value = " + (double) i);
    });

    // ts >= 2000 AND ts <= 5000 should return timestamps 2000, 3000, 4000, 5000
    final ResultSet rs = database.query("sql", "SELECT value FROM CombinedSensor WHERE ts >= 2000 AND ts <= 5000");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    assertThat(results).hasSize(4);
  }

  // ===== Multi-Tag Filtering =====

  @Test
  void testMultiTagFilter() {
    final TagFilter filter = TagFilter.eq(0, "us-east")
        .and(1, "prod");

    // Row: [timestamp, tag1, tag2, value]
    final Object[] matchingRow = { 1000L, "us-east", "prod", 42.0 };
    final Object[] wrongRegion = { 1000L, "eu-west", "prod", 42.0 };
    final Object[] wrongEnv = { 1000L, "us-east", "dev", 42.0 };

    assertThat(filter.matches(matchingRow)).isTrue();
    assertThat(filter.matches(wrongRegion)).isFalse();
    assertThat(filter.matches(wrongEnv)).isFalse();
  }

  @Test
  void testMultiTagFilterWithIn() {
    final TagFilter filter = TagFilter.in(0, Set.of("us-east", "us-west"))
        .and(1, "prod");

    final Object[] row1 = { 1000L, "us-east", "prod", 42.0 };
    final Object[] row2 = { 1000L, "us-west", "prod", 42.0 };
    final Object[] row3 = { 1000L, "eu-west", "prod", 42.0 };

    assertThat(filter.matches(row1)).isTrue();
    assertThat(filter.matches(row2)).isTrue();
    assertThat(filter.matches(row3)).isFalse();
  }

  @Test
  void testSingleTagFilterBackwardCompatibility() {
    final TagFilter filter = TagFilter.eq(0, "sensor-1");
    assertThat(filter.getColumnIndex()).isEqualTo(0);
    assertThat(filter.getConditionCount()).isEqualTo(1);

    final Object[] match = { 1000L, "sensor-1", 42.0 };
    final Object[] noMatch = { 1000L, "sensor-2", 42.0 };
    assertThat(filter.matches(match)).isTrue();
    assertThat(filter.matches(noMatch)).isFalse();
  }

  // ===== Linear Interpolation =====

  @Test
  void testLinearInterpolation() {
    database.command("sql", "CREATE DOCUMENT TYPE LinearInterp");
    database.command("sql", "CREATE PROPERTY LinearInterp.ts LONG");
    database.command("sql", "CREATE PROPERTY LinearInterp.value DOUBLE");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO LinearInterp SET ts = 1000, value = 10.0");
      database.command("sql", "INSERT INTO LinearInterp SET ts = 2000");  // null value
      database.command("sql", "INSERT INTO LinearInterp SET ts = 3000, value = 30.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.interpolate(value, 'linear', ts) AS filled FROM LinearInterp ORDER BY ts");
    @SuppressWarnings("unchecked")
    final List<Object> filled = (List<Object>) rs.next().getProperty("filled");

    assertThat(filled).hasSize(3);
    assertThat(((Number) filled.get(0)).doubleValue()).isEqualTo(10.0);
    assertThat(((Number) filled.get(1)).doubleValue()).isCloseTo(20.0, within(0.01)); // linearly interpolated
    assertThat(((Number) filled.get(2)).doubleValue()).isEqualTo(30.0);
  }

  @Test
  void testLinearInterpolationMultipleGaps() {
    database.command("sql", "CREATE DOCUMENT TYPE LinearMultiGap");
    database.command("sql", "CREATE PROPERTY LinearMultiGap.ts LONG");
    database.command("sql", "CREATE PROPERTY LinearMultiGap.value DOUBLE");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO LinearMultiGap SET ts = 0,    value = 0.0");
      database.command("sql", "INSERT INTO LinearMultiGap SET ts = 1000");  // null
      database.command("sql", "INSERT INTO LinearMultiGap SET ts = 2000");  // null
      database.command("sql", "INSERT INTO LinearMultiGap SET ts = 3000, value = 30.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.interpolate(value, 'linear', ts) AS filled FROM LinearMultiGap ORDER BY ts");
    @SuppressWarnings("unchecked")
    final List<Object> filled = (List<Object>) rs.next().getProperty("filled");

    assertThat(filled).hasSize(4);
    assertThat(((Number) filled.get(0)).doubleValue()).isEqualTo(0.0);
    assertThat(((Number) filled.get(1)).doubleValue()).isCloseTo(10.0, within(0.01));
    assertThat(((Number) filled.get(2)).doubleValue()).isCloseTo(20.0, within(0.01));
    assertThat(((Number) filled.get(3)).doubleValue()).isEqualTo(30.0);
  }

  @Test
  void testLinearInterpolationNoGaps() {
    database.command("sql",
        "CREATE TIMESERIES TYPE LinearNoGap TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO LinearNoGap SET ts = 1000, value = 1.0");
      database.command("sql", "INSERT INTO LinearNoGap SET ts = 2000, value = 2.0");
      database.command("sql", "INSERT INTO LinearNoGap SET ts = 3000, value = 3.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.interpolate(value, 'linear', ts) AS filled FROM LinearNoGap");
    @SuppressWarnings("unchecked")
    final List<Object> filled = (List<Object>) rs.next().getProperty("filled");

    assertThat(filled).hasSize(3);
    assertThat(((Number) filled.get(0)).doubleValue()).isEqualTo(1.0);
    assertThat(((Number) filled.get(1)).doubleValue()).isEqualTo(2.0);
    assertThat(((Number) filled.get(2)).doubleValue()).isEqualTo(3.0);
  }

  // ===== ts.percentile() =====

  @Test
  void testPercentileP50() {
    database.command("sql",
        "CREATE TIMESERIES TYPE PercSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      for (int i = 1; i <= 100; i++)
        database.command("sql", "INSERT INTO PercSensor SET ts = " + (i * 1000) + ", value = " + (double) i);
    });

    final ResultSet rs = database.query("sql", "SELECT ts.percentile(value, 0.5) AS p50 FROM PercSensor");
    assertThat(rs.hasNext()).isTrue();
    final double p50 = ((Number) rs.next().getProperty("p50")).doubleValue();
    assertThat(p50).isCloseTo(50.5, within(0.5));
  }

  @Test
  void testPercentileP95() {
    database.command("sql",
        "CREATE TIMESERIES TYPE Perc95Sensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      for (int i = 1; i <= 100; i++)
        database.command("sql", "INSERT INTO Perc95Sensor SET ts = " + (i * 1000) + ", value = " + (double) i);
    });

    final ResultSet rs = database.query("sql", "SELECT ts.percentile(value, 0.95) AS p95 FROM Perc95Sensor");
    assertThat(rs.hasNext()).isTrue();
    final double p95 = ((Number) rs.next().getProperty("p95")).doubleValue();
    assertThat(p95).isCloseTo(95.0, within(1.0));
  }

  @Test
  void testPercentileP99() {
    database.command("sql",
        "CREATE TIMESERIES TYPE Perc99Sensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      for (int i = 1; i <= 1000; i++)
        database.command("sql", "INSERT INTO Perc99Sensor SET ts = " + (i * 100) + ", value = " + (double) i);
    });

    final ResultSet rs = database.query("sql", "SELECT ts.percentile(value, 0.99) AS p99 FROM Perc99Sensor");
    assertThat(rs.hasNext()).isTrue();
    final double p99 = ((Number) rs.next().getProperty("p99")).doubleValue();
    assertThat(p99).isCloseTo(990.0, within(2.0));
  }

  @Test
  void testPercentileWithGroupBy() {
    database.command("sql",
        "CREATE TIMESERIES TYPE PercGroupSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      // Bucket 1: values 1-10
      for (int i = 1; i <= 10; i++)
        database.command("sql", "INSERT INTO PercGroupSensor SET ts = " + (i * 1000) + ", value = " + (double) i);
      // Bucket 2: values 100-110
      for (int i = 1; i <= 10; i++)
        database.command("sql", "INSERT INTO PercGroupSensor SET ts = " + (60000 + i * 1000) + ", value = " + (double) (100 + i));
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1m', ts) AS minute, ts.percentile(value, 0.5) AS p50 FROM PercGroupSensor GROUP BY minute ORDER BY minute");

    final Result r1 = rs.next();
    final Result r2 = rs.next();
    assertThat(((Number) r1.getProperty("p50")).doubleValue()).isCloseTo(5.5, within(0.5));
    assertThat(((Number) r2.getProperty("p50")).doubleValue()).isCloseTo(105.5, within(0.5));
  }

  // ===== Automatic Scheduler =====

  @Test
  void testMaintenanceSchedulerCreation() {
    final TimeSeriesMaintenanceScheduler scheduler = new TimeSeriesMaintenanceScheduler();
    // Should not throw
    scheduler.cancel("nonexistent");
    scheduler.shutdown();
  }

  @Test
  void testMaintenanceSchedulerAlwaysSchedulesEvenWithoutPolicies() {
    // Regression: scheduler previously skipped scheduling when no retention/downsampling
    // policy was set, leaving the mutable bucket growing unboundedly.
    database.command("sql", "CREATE TIMESERIES TYPE NoPolicySeries TIMESTAMP ts FIELDS (value DOUBLE)");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType("NoPolicySeries");
    assertThat(tsType.getRetentionMs()).isZero();
    assertThat(tsType.getDownsamplingTiers()).isEmpty();

    final TimeSeriesMaintenanceScheduler scheduler = new TimeSeriesMaintenanceScheduler();
    try {
      // schedule() must register a task even though no policies are configured
      scheduler.schedule(database, tsType);
      // Verify a task was registered: cancelling it should not throw
      scheduler.cancel("NoPolicySeries");
    } finally {
      scheduler.shutdown();
    }
  }

  // ===== Tag Filter on Aggregation Queries =====

  @Test
  void testTagFilterOnAggregation() {
    database.command("sql",
        "CREATE TIMESERIES TYPE TagAggStocks TIMESTAMP ts TAGS (symbol STRING) FIELDS (price DOUBLE)");

    database.transaction(() -> {
      // Insert data for two symbols
      for (int i = 0; i < 10; i++) {
        database.command("sql", "INSERT INTO TagAggStocks SET ts = " + (i * 1000) + ", symbol = 'TSLA', price = " + (100.0 + i));
        database.command("sql", "INSERT INTO TagAggStocks SET ts = " + (i * 1000) + ", symbol = 'AAPL', price = " + (200.0 + i));
      }
    });

    // Query for TSLA only — should get average around 104.5 (100..109)
    final ResultSet rs = database.query("sql",
        "SELECT avg(price) AS avg_price FROM TagAggStocks WHERE symbol = 'TSLA'");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    final double avgPrice = ((Number) row.getProperty("avg_price")).doubleValue();
    // TSLA prices: 100, 101, ..., 109 => avg = 104.5
    assertThat(avgPrice).isCloseTo(104.5, within(0.01));

    // Query for AAPL only — should get average around 204.5 (200..209)
    final ResultSet rs2 = database.query("sql",
        "SELECT avg(price) AS avg_price FROM TagAggStocks WHERE symbol = 'AAPL'");
    assertThat(rs2.hasNext()).isTrue();
    final double avgPrice2 = ((Number) rs2.next().getProperty("avg_price")).doubleValue();
    assertThat(avgPrice2).isCloseTo(204.5, within(0.01));

    // Query for nonexistent symbol — should get no results
    final ResultSet rs3 = database.query("sql",
        "SELECT avg(price) AS avg_price FROM TagAggStocks WHERE symbol = 'MSFT'");
    if (rs3.hasNext()) {
      final Object val = rs3.next().getProperty("avg_price");
      // Should be null or empty
      assertThat(val).isNull();
    }
  }

  // ===== Block-Level Tag Filter Tests (after compaction) =====

  @Test
  void testTagFilterBlockSkipping() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE BlockSkipStocks TIMESTAMP ts TAGS (symbol STRING) FIELDS (price DOUBLE)");

    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        database.command("sql", "INSERT INTO BlockSkipStocks SET ts = " + (i * 1000) + ", symbol = 'TSLA', price = " + (100.0 + i));
        database.command("sql", "INSERT INTO BlockSkipStocks SET ts = " + (i * 1000 + 1) + ", symbol = 'AAPL', price = " + (200.0 + i));
      }
    });

    // Compact to create sealed blocks with tag metadata
    ((LocalTimeSeriesType) database.getSchema().getType("BlockSkipStocks")).getEngine().compactAll();

    // Query for TSLA only — verify correct results
    final ResultSet rs = database.query("sql",
        "SELECT price FROM BlockSkipStocks WHERE symbol = 'TSLA' ORDER BY ts");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    assertThat(results).hasSize(20);
    assertThat(((Number) results.getFirst().getProperty("price")).doubleValue()).isCloseTo(100.0, within(0.01));
    assertThat(((Number) results.getLast().getProperty("price")).doubleValue()).isCloseTo(119.0, within(0.01));
  }

  @Test
  void testTagFilterAggregationAfterCompaction() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE AggCompactStocks TIMESTAMP ts TAGS (symbol STRING) FIELDS (price DOUBLE)");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        database.command("sql", "INSERT INTO AggCompactStocks SET ts = " + (i * 1000) + ", symbol = 'TSLA', price = " + (100.0 + i));
        database.command("sql", "INSERT INTO AggCompactStocks SET ts = " + (i * 1000 + 1) + ", symbol = 'AAPL', price = " + (200.0 + i));
      }
    });

    // Compact to create sealed blocks with tag metadata
    ((LocalTimeSeriesType) database.getSchema().getType("AggCompactStocks")).getEngine().compactAll();

    // AVG for TSLA only — should be 104.5 (100..109)
    final ResultSet rs = database.query("sql",
        "SELECT avg(price) AS avg_price FROM AggCompactStocks WHERE symbol = 'TSLA'");
    assertThat(rs.hasNext()).isTrue();
    final double avgPrice = ((Number) rs.next().getProperty("avg_price")).doubleValue();
    assertThat(avgPrice).isCloseTo(104.5, within(0.01));

    // SUM for AAPL only — should be 2045 (200+201+...+209)
    final ResultSet rs2 = database.query("sql",
        "SELECT sum(price) AS sum_price FROM AggCompactStocks WHERE symbol = 'AAPL'");
    assertThat(rs2.hasNext()).isTrue();
    final double sumPrice = ((Number) rs2.next().getProperty("sum_price")).doubleValue();
    assertThat(sumPrice).isCloseTo(2045.0, within(0.01));
  }

  @Test
  void testTagFilterNonexistentTag() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE NonExistTagStocks TIMESTAMP ts TAGS (symbol STRING) FIELDS (price DOUBLE)");

    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.command("sql", "INSERT INTO NonExistTagStocks SET ts = " + (i * 1000) + ", symbol = 'TSLA', price = " + (100.0 + i));
    });

    // Compact to create sealed blocks with tag metadata
    ((LocalTimeSeriesType) database.getSchema().getType("NonExistTagStocks")).getEngine().compactAll();

    // Query for MSFT which doesn't exist — should return empty or null
    final ResultSet rs = database.query("sql",
        "SELECT avg(price) AS avg_price FROM NonExistTagStocks WHERE symbol = 'MSFT'");
    if (rs.hasNext()) {
      final Object val = rs.next().getProperty("avg_price");
      assertThat(val).isNull();
    }

    // Also verify raw query returns no rows
    final ResultSet rs2 = database.query("sql",
        "SELECT price FROM NonExistTagStocks WHERE symbol = 'MSFT'");
    assertThat(rs2.hasNext()).isFalse();
  }
}
