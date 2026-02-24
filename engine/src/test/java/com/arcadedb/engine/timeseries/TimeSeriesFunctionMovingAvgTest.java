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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

public class TimeSeriesFunctionMovingAvgTest extends TestHelper {

  @Test
  public void testWindowOf3On5Values() {
    database.command("sql",
        "CREATE TIMESERIES TYPE MaSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO MaSensor SET ts = 1000, value = 1.0");
      database.command("sql", "INSERT INTO MaSensor SET ts = 2000, value = 2.0");
      database.command("sql", "INSERT INTO MaSensor SET ts = 3000, value = 3.0");
      database.command("sql", "INSERT INTO MaSensor SET ts = 4000, value = 4.0");
      database.command("sql", "INSERT INTO MaSensor SET ts = 5000, value = 5.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.movingAvg(value, 3) AS ma FROM MaSensor");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Double> ma = (List<Double>) rs.next().getProperty("ma");

    assertThat(ma).hasSize(5);
    // Position 0: avg(1) = 1.0
    assertThat(ma.get(0)).isCloseTo(1.0, within(0.001));
    // Position 1: avg(1,2) = 1.5
    assertThat(ma.get(1)).isCloseTo(1.5, within(0.001));
    // Position 2: avg(1,2,3) = 2.0
    assertThat(ma.get(2)).isCloseTo(2.0, within(0.001));
    // Position 3: avg(2,3,4) = 3.0
    assertThat(ma.get(3)).isCloseTo(3.0, within(0.001));
    // Position 4: avg(3,4,5) = 4.0
    assertThat(ma.get(4)).isCloseTo(4.0, within(0.001));
  }

  @Test
  public void testWindowOf1() {
    database.command("sql",
        "CREATE TIMESERIES TYPE Ma1Sensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Ma1Sensor SET ts = 1000, value = 10.0");
      database.command("sql", "INSERT INTO Ma1Sensor SET ts = 2000, value = 20.0");
      database.command("sql", "INSERT INTO Ma1Sensor SET ts = 3000, value = 30.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.movingAvg(value, 1) AS ma FROM Ma1Sensor");
    @SuppressWarnings("unchecked")
    final List<Double> ma = (List<Double>) rs.next().getProperty("ma");

    assertThat(ma).hasSize(3);
    assertThat(ma.get(0)).isCloseTo(10.0, within(0.001));
    assertThat(ma.get(1)).isCloseTo(20.0, within(0.001));
    assertThat(ma.get(2)).isCloseTo(30.0, within(0.001));
  }

  @Test
  public void testWindowLargerThanData() {
    database.command("sql",
        "CREATE TIMESERIES TYPE MaBigWindow TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO MaBigWindow SET ts = 1000, value = 4.0");
      database.command("sql", "INSERT INTO MaBigWindow SET ts = 2000, value = 8.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.movingAvg(value, 10) AS ma FROM MaBigWindow");
    @SuppressWarnings("unchecked")
    final List<Double> ma = (List<Double>) rs.next().getProperty("ma");

    assertThat(ma).hasSize(2);
    assertThat(ma.get(0)).isCloseTo(4.0, within(0.001));
    assertThat(ma.get(1)).isCloseTo(6.0, within(0.001));
  }

  @Test
  public void testEmptyInput() {
    database.command("sql",
        "CREATE TIMESERIES TYPE MaEmpty TIMESTAMP ts FIELDS (value DOUBLE)");

    final ResultSet rs = database.query("sql", "SELECT ts.movingAvg(value, 3) AS ma FROM MaEmpty");
    // Empty result set means no rows returned or null result
    if (rs.hasNext()) {
      final Result row = rs.next();
      final Object ma = row.getProperty("ma");
      if (ma instanceof List)
        assertThat((List<?>) ma).isEmpty();
    }
  }
}
