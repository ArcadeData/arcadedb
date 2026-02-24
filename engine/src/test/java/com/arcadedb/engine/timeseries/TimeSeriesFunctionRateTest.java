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

import static org.assertj.core.api.Assertions.assertThat;

public class TimeSeriesFunctionRateTest extends TestHelper {

  @Test
  public void testLinearIncrease() {
    database.command("sql",
        "CREATE TIMESERIES TYPE RateSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      // 10 values, 1 second apart, value = ts/1000 (so rate = 1.0 per second)
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO RateSensor SET ts = " + (i * 1000) + ", value = " + (double) i);
    });

    final ResultSet rs = database.query("sql", "SELECT ts.rate(value, ts) AS r FROM RateSensor");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("r")).doubleValue()).isEqualTo(1.0);
  }

  @Test
  public void testConstantValues() {
    database.command("sql",
        "CREATE TIMESERIES TYPE ConstSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.command("sql", "INSERT INTO ConstSensor SET ts = " + (i * 1000) + ", value = 42.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.rate(value, ts) AS r FROM ConstSensor");
    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("r")).doubleValue()).isEqualTo(0.0);
  }

  @Test
  public void testDecreasing() {
    database.command("sql",
        "CREATE TIMESERIES TYPE DecSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO DecSensor SET ts = 0,    value = 100.0");
      database.command("sql", "INSERT INTO DecSensor SET ts = 2000, value = 80.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.rate(value, ts) AS r FROM DecSensor");
    assertThat(((Number) rs.next().getProperty("r")).doubleValue()).isEqualTo(-10.0);
  }

  @Test
  public void testSingleSample() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SingleRateSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO SingleRateSensor SET ts = 1000, value = 5.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.rate(value, ts) AS r FROM SingleRateSensor");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("r")).isNull();
  }

  @Test
  public void testWithTimeBucketGroupBy() {
    database.command("sql",
        "CREATE TIMESERIES TYPE BucketRateSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      // Two 1-minute buckets: 0-59s and 60-119s
      // Bucket 1: value goes 0 -> 10 over 10 seconds => rate 1.0/s
      database.command("sql", "INSERT INTO BucketRateSensor SET ts = 0,     value = 0.0");
      database.command("sql", "INSERT INTO BucketRateSensor SET ts = 10000, value = 10.0");
      // Bucket 2: value goes 100 -> 120 over 10 seconds => rate 2.0/s
      database.command("sql", "INSERT INTO BucketRateSensor SET ts = 60000, value = 100.0");
      database.command("sql", "INSERT INTO BucketRateSensor SET ts = 70000, value = 120.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1m', ts) AS minute, ts.rate(value, ts) AS r FROM BucketRateSensor GROUP BY minute ORDER BY minute");
    final Result r1 = rs.next();
    final Result r2 = rs.next();
    assertThat(((Number) r1.getProperty("r")).doubleValue()).isEqualTo(1.0);
    assertThat(((Number) r2.getProperty("r")).doubleValue()).isEqualTo(2.0);
  }
}
