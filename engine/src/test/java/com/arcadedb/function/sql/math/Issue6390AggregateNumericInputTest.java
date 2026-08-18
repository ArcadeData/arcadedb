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
package com.arcadedb.function.sql.math;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code sum()} was hardened to reject non-numeric input with a typed error (issue #5799); its aggregate and window
 * siblings kept the raw {@code (Number)} cast and answered a ClassCastException - an HTTP 500 - for the same input
 * (issue #6390). They all share {@code SQLFunctionAbstract.requireNumericOrNull} now.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6390AggregateNumericInputTest extends TestHelper {

  @Test
  void everySumSiblingRejectsANonNumericElement() {
    for (final String query : new String[] { "SELECT sum(['a', 1]) AS r", "SELECT avg(['a', 1]) AS r",
        "SELECT variance(['a']) AS r", "SELECT stddev(['a']) AS r", "SELECT variancep(['a']) AS r",
        "SELECT stddevp(['a']) AS r", "SELECT percentile(['a'], 0.5) AS r", "SELECT median(['a']) AS r" })
      assertThatThrownBy(() -> database.query("sql", query).next())
          .as(query)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("requires numeric input");
  }

  @Test
  void everySumSiblingRejectsANonNumericScalar() {
    for (final String query : new String[] { "SELECT sum('a') AS r", "SELECT avg('a') AS r", "SELECT variance('a') AS r",
        "SELECT percentile('a', 0.5) AS r" })
      assertThatThrownBy(() -> database.query("sql", query).next())
          .as(query)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("requires numeric input");
  }

  @Test
  void timeSeriesAggregatesRejectNonNumericInput() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Series6390");
      database.command("sql", "INSERT INTO Series6390 SET a = 'x', b = 'y', ts = 1000, v = 'z'");
      database.command("sql", "INSERT INTO Series6390 SET a = 'x', b = 'y', ts = 2000, v = 'z'");
    });

    database.transaction(() -> {
      for (final String query : new String[] { "SELECT ts.correlate(a, b) AS r FROM Series6390",
          "SELECT ts.rate(v, ts) AS r FROM Series6390", "SELECT ts.delta(v, ts) AS r FROM Series6390",
          "SELECT ts.percentile(v, 0.5) AS r FROM Series6390", "SELECT ts.movingAvg(v, 2) AS r FROM Series6390" })
        assertThatThrownBy(() -> database.query("sql", query).next())
            .as(query)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("requires numeric input");
    });
  }

  @Test
  void windowConfigurationArgumentsAreCheckedToo() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Window6390");
      database.command("sql", "INSERT INTO Window6390 SET v = 1");
      database.command("sql", "INSERT INTO Window6390 SET v = 3");
    });

    database.transaction(() -> {
      assertThatThrownBy(() -> database.query("sql", "SELECT ts.movingAvg(v, 'wide') AS r FROM Window6390").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("window_size");
      assertThatThrownBy(() -> database.query("sql", "SELECT ts.percentile(v, 'half') AS r FROM Window6390").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("percentile");
    });
  }

  /**
   * The same bug class in three functions the first sweep missed: a raw {@code (Number)} on {@code pow()}'s
   * exponent, a raw {@code Comparable} sort in {@code ts.rank} (the {@code ts.lag}/{@code ts.lead} shape), and raw
   * casts in {@code ts.interpolate}'s linear branch, unlike the siblings around it.
   */
  @Test
  void theRemainingRawCastSiblingsAnswerTypedErrorsToo() {
    assertThatThrownBy(() -> database.query("sql", "SELECT pow(2, 'x') AS r").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("numeric");

    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Sweep6390");
      // A gap in the middle, so the linear branch actually has something to interpolate across.
      database.command("sql", "INSERT INTO Sweep6390 SET v = 'a', ts = 1000");
      database.command("sql", "INSERT INTO Sweep6390 SET ts = 2000");
      database.command("sql", "INSERT INTO Sweep6390 SET v = 'b', ts = 3000");
    });

    database.transaction(() -> {
      assertThatThrownBy(() -> database.query("sql", "SELECT ts.interpolate(v, 'linear', ts) AS r FROM Sweep6390").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("requires numeric input");
      // ts.rank orders by the timestamp, and a row whose timestamp is null keeps its arrival order at the end
      // rather than NPE the comparator - the ts.lag/ts.lead treatment, which rank had not had either.
      try (final ResultSet rs = database.query("sql", "SELECT ts.rank(v, ts) AS r FROM Sweep6390")) {
        assertThat(rs.next().<List<Object>>getProperty("r")).hasSize(3);
      }
      database.command("sql", "INSERT INTO Sweep6390 SET v = 'c'");
      try (final ResultSet rs = database.query("sql", "SELECT ts.rank(v, ts) AS r FROM Sweep6390")) {
        assertThat(rs.next().<List<Object>>getProperty("r")).hasSize(4);
      }
    });
  }

  @Test
  void powStillRaisesNumbers() {
    try (final ResultSet rs = database.query("sql", "SELECT pow(2, 10) AS a, pow(2, '10') AS b")) {
      final var row = rs.next();
      assertThat(row.<Number>getProperty("a").intValue()).isEqualTo(1024);
      assertThat(row.<Number>getProperty("b").intValue()).isEqualTo(1024);
    }
  }

  @Test
  void numericInputStillAggregatesNormally() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Ok6390");
      database.command("sql", "INSERT INTO Ok6390 SET v = 2, ts = 1000");
      database.command("sql", "INSERT INTO Ok6390 SET v = 4, ts = 2000");
      database.command("sql", "INSERT INTO Ok6390 SET v = 6, ts = 3000");
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT sum(v) AS s, avg(v) AS a, variance(v) AS va, ts.delta(v, ts) AS d FROM Ok6390")) {
        final var row = rs.next();
        assertThat(row.<Number>getProperty("s").intValue()).isEqualTo(12);
        assertThat(row.<Number>getProperty("a").doubleValue()).isEqualTo(4.0);
        assertThat(row.<Number>getProperty("va").doubleValue()).isEqualTo(4.0);
        assertThat(row.<Number>getProperty("d").doubleValue()).isEqualTo(4.0);
      }
      try (final ResultSet rs = database.query("sql", "SELECT ts.movingAvg(v, 2) AS m FROM Ok6390")) {
        assertThat(rs.next().<List<Double>>getProperty("m")).containsExactly(2.0, 3.0, 5.0);
      }
      // A null is skipped, not rejected: that is the documented aggregation behaviour and it must survive the guard.
      try (final ResultSet rs = database.query("sql", "SELECT avg([1, null, 3]) AS a, variance([2, null, 4]) AS v")) {
        final var row = rs.next();
        assertThat(row.<Number>getProperty("a").doubleValue()).isEqualTo(2.0);
        assertThat(row.<Number>getProperty("v").doubleValue()).isEqualTo(2.0);
      }
    });
  }
}
