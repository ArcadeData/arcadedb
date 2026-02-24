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

public class TimeSeriesFunctionDeltaTest extends TestHelper {

  @Test
  public void testIncreasingCounter() {
    database.command("sql",
        "CREATE TIMESERIES TYPE DeltaSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO DeltaSensor SET ts = 1000, value = 100.0");
      database.command("sql", "INSERT INTO DeltaSensor SET ts = 2000, value = 150.0");
      database.command("sql", "INSERT INTO DeltaSensor SET ts = 3000, value = 250.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.delta(value, ts) AS d FROM DeltaSensor");
    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("d")).doubleValue()).isEqualTo(150.0);
  }

  @Test
  public void testNegativeDelta() {
    database.command("sql",
        "CREATE TIMESERIES TYPE NegDeltaSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO NegDeltaSensor SET ts = 1000, value = 100.0");
      database.command("sql", "INSERT INTO NegDeltaSensor SET ts = 2000, value = 40.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.delta(value, ts) AS d FROM NegDeltaSensor");
    assertThat(((Number) rs.next().getProperty("d")).doubleValue()).isEqualTo(-60.0);
  }

  @Test
  public void testSingleSample() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SingleDelta TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO SingleDelta SET ts = 1000, value = 42.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.delta(value, ts) AS d FROM SingleDelta");
    assertThat(((Number) rs.next().getProperty("d")).doubleValue()).isEqualTo(0.0);
  }

  @Test
  public void testWithGroupBy() {
    database.command("sql",
        "CREATE TIMESERIES TYPE GroupedDelta TIMESTAMP ts TAGS (sensor STRING) FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO GroupedDelta SET ts = 1000, sensor = 'A', value = 10.0");
      database.command("sql", "INSERT INTO GroupedDelta SET ts = 3000, sensor = 'A', value = 50.0");
      database.command("sql", "INSERT INTO GroupedDelta SET ts = 1000, sensor = 'B', value = 100.0");
      database.command("sql", "INSERT INTO GroupedDelta SET ts = 3000, sensor = 'B', value = 80.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT sensor, ts.delta(value, ts) AS d FROM GroupedDelta GROUP BY sensor ORDER BY sensor");
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
    assertThat(((Number) results.get(0).getProperty("d")).doubleValue()).isEqualTo(40.0);
    assertThat(((Number) results.get(1).getProperty("d")).doubleValue()).isEqualTo(-20.0);
  }
}
