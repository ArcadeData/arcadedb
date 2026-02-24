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
import static org.assertj.core.api.Assertions.within;

public class TimeSeriesFunctionCorrelateTest extends TestHelper {

  @Test
  public void testPerfectPositiveCorrelation() {
    database.command("sql",
        "CREATE TIMESERIES TYPE CorrSensor TIMESTAMP ts FIELDS (a DOUBLE, b DOUBLE)");

    database.transaction(() -> {
      for (int i = 1; i <= 10; i++)
        database.command("sql",
            "INSERT INTO CorrSensor SET ts = " + (i * 1000) + ", a = " + (double) i + ", b = " + (double) i);
    });

    final ResultSet rs = database.query("sql", "SELECT ts.correlate(a, b) AS corr FROM CorrSensor");
    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("corr")).doubleValue()).isCloseTo(1.0, within(0.001));
  }

  @Test
  public void testPerfectNegativeCorrelation() {
    database.command("sql",
        "CREATE TIMESERIES TYPE NegCorrSensor TIMESTAMP ts FIELDS (a DOUBLE, b DOUBLE)");

    database.transaction(() -> {
      for (int i = 1; i <= 10; i++)
        database.command("sql",
            "INSERT INTO NegCorrSensor SET ts = " + (i * 1000) + ", a = " + (double) i + ", b = " + (double) (-i));
    });

    final ResultSet rs = database.query("sql", "SELECT ts.correlate(a, b) AS corr FROM NegCorrSensor");
    assertThat(((Number) rs.next().getProperty("corr")).doubleValue()).isCloseTo(-1.0, within(0.001));
  }

  @Test
  public void testUncorrelated() {
    database.command("sql",
        "CREATE TIMESERIES TYPE UncorrSensor TIMESTAMP ts FIELDS (a DOUBLE, b DOUBLE)");

    // a increases, b alternates — near zero correlation
    database.transaction(() -> {
      database.command("sql", "INSERT INTO UncorrSensor SET ts = 1000, a = 1.0, b = 1.0");
      database.command("sql", "INSERT INTO UncorrSensor SET ts = 2000, a = 2.0, b = -1.0");
      database.command("sql", "INSERT INTO UncorrSensor SET ts = 3000, a = 3.0, b = 1.0");
      database.command("sql", "INSERT INTO UncorrSensor SET ts = 4000, a = 4.0, b = -1.0");
      database.command("sql", "INSERT INTO UncorrSensor SET ts = 5000, a = 5.0, b = 1.0");
      database.command("sql", "INSERT INTO UncorrSensor SET ts = 6000, a = 6.0, b = -1.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.correlate(a, b) AS corr FROM UncorrSensor");
    final double corr = ((Number) rs.next().getProperty("corr")).doubleValue();
    assertThat(Math.abs(corr)).isLessThan(0.3);
  }

  @Test
  public void testSingleSample() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SingleCorr TIMESTAMP ts FIELDS (a DOUBLE, b DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO SingleCorr SET ts = 1000, a = 5.0, b = 10.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.correlate(a, b) AS corr FROM SingleCorr");
    assertThat(rs.hasNext()).isTrue();
    assertThat((Object) rs.next().getProperty("corr")).isNull();
  }

  @Test
  public void testConstantSeries() {
    database.command("sql",
        "CREATE TIMESERIES TYPE ConstCorr TIMESTAMP ts FIELDS (a DOUBLE, b DOUBLE)");

    database.transaction(() -> {
      for (int i = 1; i <= 5; i++)
        database.command("sql",
            "INSERT INTO ConstCorr SET ts = " + (i * 1000) + ", a = 42.0, b = " + (double) i);
    });

    final ResultSet rs = database.query("sql", "SELECT ts.correlate(a, b) AS corr FROM ConstCorr");
    assertThat((Object) rs.next().getProperty("corr")).isNull();
  }
}
