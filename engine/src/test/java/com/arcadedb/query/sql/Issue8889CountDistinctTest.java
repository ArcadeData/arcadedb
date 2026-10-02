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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code COUNT(DISTINCT expr)} was a syntax error, with or without GROUP BY: only a DISTINCT subquery counted the
 * distinct values (issue #8889).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8889CountDistinctTest extends TestHelper {

  private void load() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO P SET g = 'x', b = 1");
      database.command("sql", "INSERT INTO P SET g = 'x', b = 1");
      database.command("sql", "INSERT INTO P SET g = 'x', b = 2");
      database.command("sql", "INSERT INTO P SET g = 'y', b = 3");
    });
  }

  @Test
  void countDistinctWithoutGroupBy() {
    load();
    try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT b) AS n FROM P")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(3L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(distinct b) AS n, count(b) AS total, count(*) AS rows FROM P")) {
      final var row = rs.next();
      assertThat(row.<Long>getProperty("n")).isEqualTo(3L);
      assertThat(row.<Long>getProperty("total")).isEqualTo(4L);
      assertThat(row.<Long>getProperty("rows")).isEqualTo(4L);
    }
  }

  @Test
  void countDistinctWithGroupBy() {
    load();
    final Map<String, Long> counts = new HashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT g, count(DISTINCT b) AS n FROM P GROUP BY g")) {
      while (rs.hasNext()) {
        final var row = rs.next();
        counts.put(row.getProperty("g"), row.getProperty("n"));
      }
    }
    assertThat(counts).containsOnly(Map.entry("x", 2L), Map.entry("y", 1L));
  }

  @Test
  void countDistinctOfAParenthesizedArgument() {
    load();
    // used to be rejected as "'distinct' is supported only as the whole SELECT projection"
    try (final ResultSet rs = database.query("sql", "SELECT count(distinct(b)) AS n FROM P")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(3L);
    }
  }

  @Test
  void numbersAreOneValueWhateverTheirTypeOrScale() {
    database.command("sql", "CREATE DOCUMENT TYPE N");
    database.transaction(() -> {
      database.newDocument("N").set("v", 1).save();
      database.newDocument("N").set("v", 1.0).save();
      database.newDocument("N").set("v", 1L).save();
      database.newDocument("N").set("v", new BigDecimal("19.9")).save();
      database.newDocument("N").set("v", new BigDecimal("19.90")).save();
      database.newDocument("N").set("v", 19.9d).save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT v) AS n FROM N")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(2L);
    }
  }

  @Test
  void nullsAreNotCountedAndAnEmptyTypeCountsZero() {
    database.command("sql", "CREATE DOCUMENT TYPE Q");
    try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT v) AS n FROM Q")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(0L);
    }
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Q SET v = 'a'");
      database.command("sql", "INSERT INTO Q SET v = 'a'");
      database.command("sql", "INSERT INTO Q SET v = 'b'");
      database.command("sql", "INSERT INTO Q SET w = 1");
      database.command("sql", "INSERT INTO Q SET v = null");
    });
    try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT v) AS n FROM Q")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(2L);
    }
  }

  @Test
  void sumAndAvgOfDistinctValues() {
    load();
    try (final ResultSet rs = database.query("sql", "SELECT sum(DISTINCT b) AS s, avg(DISTINCT b) AS a FROM P")) {
      final var row = rs.next();
      assertThat(row.<Number>getProperty("s").intValue()).isEqualTo(6);
      assertThat(row.<Number>getProperty("a").doubleValue()).isEqualTo(2.0);
    }
  }

  @Test
  void manyRowsSpreadOverGroups() {
    database.command("sql", "CREATE DOCUMENT TYPE Big");
    database.transaction(() -> {
      for (int i = 0; i < 6_000; i++)
        database.newDocument("Big").set("g", i % 3, "v", i % 100).save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT g, count(DISTINCT v) AS n FROM Big GROUP BY g")) {
      int groups = 0;
      while (rs.hasNext()) {
        final var row = rs.next();
        // g = 0 sees i = 0,3,6... so v covers every residue of 100 reachable by multiples of 3: all 100 of them
        assertThat(row.<Long>getProperty("n")).isEqualTo(100L);
        groups++;
      }
      assertThat(groups).isEqualTo(3);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT v) AS n FROM Big")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(100L);
    }
  }

  @Test
  void distinctInAScalarFunctionIsRejected() {
    load();
    assertThatThrownBy(() -> database.query("sql", "SELECT abs(DISTINCT b) AS n FROM P").next())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("DISTINCT is supported only inside an aggregate function");
  }

  @Test
  void countDistinctOfAStarIsASyntaxError() {
    load();
    assertThatThrownBy(() -> database.query("sql", "SELECT count(DISTINCT *) AS n FROM P").next())
        .isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void countDistinctWithoutATarget() {
    try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT 1) AS n")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(1L);
    }
  }

  @Test
  void distinctInsideAnExpressionHavingAndOrderBy() {
    load();
    try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT b) + 1 AS n, count(DISTINCT b) * 1.0 / count(*) AS r FROM P")) {
      final var row = rs.next();
      assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(4L);
      assertThat(row.<Number>getProperty("r").doubleValue()).isEqualTo(0.75);
    }
    try (final ResultSet rs = database.query("sql",
        "SELECT FROM (SELECT g, count(DISTINCT b) AS n FROM P GROUP BY g) WHERE n > 1")) {
      assertThat(rs.next().<String>getProperty("g")).isEqualTo("x");
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT g, count(DISTINCT b) AS n FROM P GROUP BY g ORDER BY n DESC")) {
      assertThat(rs.next().<String>getProperty("g")).isEqualTo("x");
      assertThat(rs.next().<String>getProperty("g")).isEqualTo("y");
    }
  }

  @Test
  void aNullIsOneDistinctValueForAFunctionThatKeepsIt() {
    database.command("sql", "CREATE DOCUMENT TYPE Nl");
    database.transaction(() -> {
      database.newDocument("Nl").set("v", 1).save();
      database.newDocument("Nl").set("w", 1).save();
      database.newDocument("Nl").set("w", 2).save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT list(DISTINCT v) AS l FROM Nl")) {
      assertThat(rs.next().<List<Object>>getProperty("l")).hasSizeLessThanOrEqualTo(2);
    }
  }

  @Test
  void distinctOnAnAggregateThatHoldsEveryValue() {
    load();
    try (final ResultSet rs = database.query("sql", "SELECT list(DISTINCT b) AS l FROM P")) {
      assertThat(rs.next().<List<Object>>getProperty("l")).hasSize(3);
    }
  }

  @Test
  void collectionsAreOneValueWhateverTheirNumbersAreTypedAs() {
    database.command("sql", "CREATE DOCUMENT TYPE C");
    database.transaction(() -> {
      database.newDocument("C").set("v", new ArrayList<>(List.of(1, 2))).save();
      database.newDocument("C").set("v", new ArrayList<>(List.of(1.0, 2L))).save();
      database.newDocument("C").set("v", new ArrayList<>(List.of(2, 1))).save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT count(DISTINCT v) AS n FROM C")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(2L);
    }
  }

  @Test
  void distinctAndPlainCallsDoNotCollide() {
    load();
    final var statement = ((DatabaseInternal) database).getStatementCache().get("SELECT count(DISTINCT b) AS n, count(b) AS m FROM P");
    assertThat(statement.toString()).contains("count(DISTINCT b)").contains("count(b)");
    assertThat(statement.copy().toString()).isEqualTo(statement.toString());
    assertThat(((DatabaseInternal) database).getStatementCache().get("SELECT count(DISTINCT b) FROM P"))
        .isNotEqualTo(((DatabaseInternal) database).getStatementCache().get("SELECT count(b) FROM P"));
  }

  @Test
  void plainCountStillWorks() {
    load();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM P")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(4L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM (SELECT DISTINCT b FROM P)")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(3L);
    }
  }
}
