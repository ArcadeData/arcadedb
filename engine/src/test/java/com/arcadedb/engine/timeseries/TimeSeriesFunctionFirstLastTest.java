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

public class TimeSeriesFunctionFirstLastTest extends TestHelper {

  @Test
  public void testBasicFirstLast() {
    database.command("sql",
        "CREATE TIMESERIES TYPE Sensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Sensor SET ts = 3000, value = 30.0");
      database.command("sql", "INSERT INTO Sensor SET ts = 1000, value = 10.0");
      database.command("sql", "INSERT INTO Sensor SET ts = 5000, value = 50.0");
      database.command("sql", "INSERT INTO Sensor SET ts = 2000, value = 20.0");
      database.command("sql", "INSERT INTO Sensor SET ts = 4000, value = 40.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.first(value, ts) AS first_val, ts.last(value, ts) AS last_val FROM Sensor");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("first_val")).doubleValue()).isEqualTo(10.0);
    assertThat(((Number) row.getProperty("last_val")).doubleValue()).isEqualTo(50.0);
  }

  @Test
  public void testUnsortedInput() {
    database.command("sql",
        "CREATE TIMESERIES TYPE UnsortedSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO UnsortedSensor SET ts = 5000, value = 50.0");
      database.command("sql", "INSERT INTO UnsortedSensor SET ts = 1000, value = 10.0");
      database.command("sql", "INSERT INTO UnsortedSensor SET ts = 3000, value = 30.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.first(value, ts) AS first_val, ts.last(value, ts) AS last_val FROM UnsortedSensor");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("first_val")).doubleValue()).isEqualTo(10.0);
    assertThat(((Number) row.getProperty("last_val")).doubleValue()).isEqualTo(50.0);
  }

  @Test
  public void testWithGroupBy() {
    database.command("sql",
        "CREATE TIMESERIES TYPE GroupedSensor TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO GroupedSensor SET ts = 1000, sensor_id = 'A', value = 10.0");
      database.command("sql", "INSERT INTO GroupedSensor SET ts = 3000, sensor_id = 'A', value = 30.0");
      database.command("sql", "INSERT INTO GroupedSensor SET ts = 2000, sensor_id = 'B', value = 200.0");
      database.command("sql", "INSERT INTO GroupedSensor SET ts = 4000, sensor_id = 'B', value = 400.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT sensor_id, ts.first(value, ts) AS first_val, ts.last(value, ts) AS last_val FROM GroupedSensor GROUP BY sensor_id ORDER BY sensor_id");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
    assertThat(((Number) results.get(0).getProperty("first_val")).doubleValue()).isEqualTo(10.0);
    assertThat(((Number) results.get(0).getProperty("last_val")).doubleValue()).isEqualTo(30.0);
    assertThat(((Number) results.get(1).getProperty("first_val")).doubleValue()).isEqualTo(200.0);
    assertThat(((Number) results.get(1).getProperty("last_val")).doubleValue()).isEqualTo(400.0);
  }

  @Test
  public void testSingleRow() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SingleSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO SingleSensor SET ts = 1000, value = 42.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.first(value, ts) AS first_val, ts.last(value, ts) AS last_val FROM SingleSensor");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("first_val")).doubleValue()).isEqualTo(42.0);
    assertThat(((Number) row.getProperty("last_val")).doubleValue()).isEqualTo(42.0);
  }

  @Test
  public void testNullHandling() {
    database.command("sql",
        "CREATE TIMESERIES TYPE NullSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO NullSensor SET ts = 1000, value = 10.0");
      database.command("sql", "INSERT INTO NullSensor SET ts = 3000, value = 30.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.first(value, ts) AS first_val, ts.last(value, ts) AS last_val FROM NullSensor");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("first_val")).doubleValue()).isEqualTo(10.0);
    assertThat(((Number) row.getProperty("last_val")).doubleValue()).isEqualTo(30.0);
  }
}
