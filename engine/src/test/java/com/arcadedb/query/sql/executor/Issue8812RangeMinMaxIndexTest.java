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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #8812: {@code SELECT min(a) FROM V WHERE a > x} (and {@code max} with {@code <}) read every
 * record of the range instead of the first entry of the index on {@code a}. The answer has to stay the one the scan
 * gives, and the plan has to stop at one index entry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8812RangeMinMaxIndexTest extends TestHelper {

  @BeforeEach
  void load() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE PROPERTY V.a LONG");
      database.command("sql", "CREATE PROPERTY V.b LONG");
      database.command("sql", "CREATE INDEX ON V (a) NOTUNIQUE");
      for (int i = 0; i < 2_000; i++)
        database.command("sql", "CREATE VERTEX V SET a = " + (i * 2) + ", b = " + (i % 7));
      // a vertex with no value: a range must never return it
      database.command("sql", "CREATE VERTEX V SET b = 1");
    });
  }

  @Test
  void minOverLowerBoundUsesTheIndex() {
    assertAnswer("SELECT min(a) AS c FROM V WHERE a > 1001", "SELECT min(a + 0) AS c FROM V WHERE a + 0 > 1001", 1002L);
    assertAnswer("SELECT min(a) AS c FROM V WHERE a >= 1002", "SELECT min(a + 0) AS c FROM V WHERE a + 0 >= 1002", 1002L);
    assertUsesOrderedIndexFetch("SELECT min(a) AS c FROM V WHERE a > 1001");
  }

  @Test
  void maxOverUpperBoundUsesTheIndex() {
    assertAnswer("SELECT max(a) AS c FROM V WHERE a < 1001", "SELECT max(a + 0) AS c FROM V WHERE a + 0 < 1001", 1000L);
    assertAnswer("SELECT max(a) AS c FROM V WHERE a <= 1000", "SELECT max(a + 0) AS c FROM V WHERE a + 0 <= 1000", 1000L);
    assertUsesOrderedIndexFetch("SELECT max(a) AS c FROM V WHERE a < 1001");
  }

  @Test
  void minAndMaxOverBothBounds() {
    assertAnswer("SELECT min(a) AS c FROM V WHERE a > 100 AND a < 200", "SELECT min(a + 0) AS c FROM V WHERE a + 0 > 100 AND a + 0 < 200",
        102L);
    assertAnswer("SELECT max(a) AS c FROM V WHERE a > 100 AND a < 200", "SELECT max(a + 0) AS c FROM V WHERE a + 0 > 100 AND a + 0 < 200",
        198L);
    assertAnswer("SELECT min(a) AS c FROM V WHERE a BETWEEN 101 AND 200", "SELECT min(a + 0) AS c FROM V WHERE a + 0 BETWEEN 101 AND 200",
        102L);
    assertAnswer("SELECT max(a) AS c FROM V WHERE a BETWEEN 101 AND 199", "SELECT max(a + 0) AS c FROM V WHERE a + 0 BETWEEN 101 AND 199",
        198L);
  }

  @Test
  void minOverTheOppositeBoundAndMaxOverTheSameBound() {
    assertAnswer("SELECT min(a) AS c FROM V WHERE a < 1001", "SELECT min(a + 0) AS c FROM V WHERE a + 0 < 1001", 0L);
    assertAnswer("SELECT max(a) AS c FROM V WHERE a > 1001", "SELECT max(a + 0) AS c FROM V WHERE a + 0 > 1001", 3998L);
  }

  @Test
  void emptyRangeStillAnswersOneNullRow() {
    try (final ResultSet rs = database.query("sql", "SELECT min(a) AS c FROM V WHERE a > 999999")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("c")).isNull();
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT max(a) FROM V WHERE a < 0")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("max(a)")).isNull();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void columnNameIsTheOneTheStatementAsked() {
    try (final ResultSet rs = database.query("sql", "SELECT max(a) FROM V WHERE a < 100")) {
      final Result row = rs.next();
      assertThat(row.getPropertyNames()).containsExactly("max(a)");
      assertThat(row.<Long>getProperty("max(a)")).isEqualTo(98L);
    }
  }

  @Test
  void parametersAreReadAtEachExecution() {
    final String sql = "SELECT min(a) AS c FROM V WHERE a > :lo";
    for (final long lo : new long[] { 10, 1001, 3000 }) {
      try (final ResultSet rs = database.query("sql", sql, Map.of("lo", lo))) {
        final Object value = rs.next().getProperty("c");
        try (final ResultSet scan = database.query("sql", "SELECT min(a + 0) AS c FROM V WHERE a + 0 > :lo", Map.of("lo", lo))) {
          assertThat(value).isEqualTo(scan.next().getProperty("c"));
        }
      }
    }
  }

  @Test
  void otherShapesStillAggregate() {
    // a condition on another property, or an equality, is not a range of the aggregated property
    assertAnswer("SELECT min(a) AS c FROM V WHERE a > 100 AND b = 3", "SELECT min(a + 0) AS c FROM V WHERE a + 0 > 100 AND b = 3", null);
    assertAnswer("SELECT min(b) AS c FROM V WHERE a > 100", "SELECT min(b + 0) AS c FROM V WHERE a + 0 > 100", null);
  }

  @Test
  void aliasEqualToTheSourceColumnAndANullParameter() {
    try (final ResultSet rs = database.query("sql", "SELECT max(a) AS a FROM V WHERE a < 100")) {
      assertThat(rs.next().<Long>getProperty("a")).isEqualTo(98L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT min(a) AS c FROM V WHERE a > :lo", Map.of("lo", Long.MIN_VALUE))) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(0L);
    }
    final HashMap<String, Object> nullParam = new HashMap<>();
    nullParam.put("lo", null);
    final Object viaIndex;
    try (final ResultSet rs = database.query("sql", "SELECT min(a) AS c FROM V WHERE a > :lo", nullParam)) {
      viaIndex = rs.next().getProperty("c");
    }
    try (final ResultSet rs = database.query("sql", "SELECT min(a + 0) AS c FROM V WHERE a + 0 > :lo", nullParam)) {
      assertThat(viaIndex).isEqualTo(rs.next().getProperty("c"));
    }
  }

  @Test
  void aNullBoundOfARangeMatchesNothing() {
    final HashMap<String, Object> nullParam = new HashMap<>();
    nullParam.put("lo", null);
    // a comparison with null is never true, through the index as through a scan: the null bound used to read as "no bound"
    try (final ResultSet rs = database.query("sql", "SELECT a FROM V WHERE a > :lo ORDER BY a LIMIT 1", nullParam)) {
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT a FROM V WHERE a < :lo", nullParam)) {
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT a FROM V WHERE a BETWEEN :lo AND 10", nullParam)) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void aNullBoundOnACompositeIndexMatchesNothingAndRealBoundsStillWork() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE C");
      database.command("sql", "CREATE PROPERTY C.x LONG");
      database.command("sql", "CREATE PROPERTY C.y LONG");
      database.command("sql", "CREATE INDEX ON C (x, y) NOTUNIQUE");
      for (int x = 0; x < 3; x++)
        for (int y = 0; y < 10; y++)
          database.command("sql", "CREATE VERTEX C SET x = " + x + ", y = " + y);
    });
    final HashMap<String, Object> nullParam = new HashMap<>();
    nullParam.put("p", null);
    for (final String where : new String[] { "x = 1 AND y > :p", "x = 1 AND y < :p", "x = 1 AND y >= :p", "x = 1 AND y BETWEEN :p AND 5" })
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM C WHERE " + where, nullParam)) {
        assertThat(rs.next().<Long>getProperty("c")).as(where).isEqualTo(0L);
      }
    for (final String where : new String[] { "x = 1 AND y > 4", "x = 1 AND y <= 4", "x = 1 AND y BETWEEN 2 AND 5" })
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM C WHERE " + where);
          final ResultSet scan = database.query("sql", "SELECT count(*) AS c FROM C WHERE " + where.replace("y", "y + 0"))) {
        assertThat(rs.next().<Long>getProperty("c")).as(where).isEqualTo(scan.next().<Long>getProperty("c"));
      }
  }

  @Test
  void anEqualityNullPrefixOfACompositeIndexIsNotANullRangeBound() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE N");
      database.command("sql", "CREATE PROPERTY N.x LONG");
      database.command("sql", "CREATE PROPERTY N.y LONG");
      database.command("sql", "CREATE INDEX ON N (x, y) NOTUNIQUE NULL_STRATEGY INDEX");
      for (int y = 0; y < 10; y++) {
        database.command("sql", "CREATE VERTEX N SET y = " + y);
        database.command("sql", "CREATE VERTEX N SET x = 1, y = " + y);
      }
    });
    for (final String where : new String[] { "x IS NULL AND y > 6", "x IS NULL AND y < 3", "x IS NULL AND y >= 6", "x IS NULL AND y BETWEEN 2 AND 4" })
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM N WHERE " + where);
          final ResultSet scan = database.query("sql", "SELECT count(*) AS c FROM N WHERE " + where.replace("y", "y + 0"))) {
        final long expected = scan.next().<Long>getProperty("c");
        assertThat(expected).as(where).isGreaterThan(0L);
        assertThat(rs.next().<Long>getProperty("c")).as(where).isEqualTo(expected);
      }
  }

  @Test
  void changesOfTheOpenTransactionAreSeen() {
    database.begin();
    try {
      database.command("sql", "CREATE VERTEX V SET a = 1003");
      database.command("sql", "DELETE FROM V WHERE a = 1002");
      assertAnswer("SELECT min(a) AS c FROM V WHERE a > 1001", "SELECT min(a + 0) AS c FROM V WHERE a + 0 > 1001", 1003L);
    } finally {
      database.rollback();
    }
    assertAnswer("SELECT min(a) AS c FROM V WHERE a > 1001", "SELECT min(a + 0) AS c FROM V WHERE a + 0 > 1001", 1002L);
  }

  private void assertAnswer(final String sql, final String checkSql, final Object expected) {
    final Object actual;
    final Object check;
    try (final ResultSet rs = database.query("sql", sql)) {
      actual = rs.next().getProperty("c");
    }
    try (final ResultSet rs = database.query("sql", checkSql)) {
      check = rs.next().getProperty("c");
    }
    assertThat(actual).as(sql).isEqualTo(check);
    if (expected != null)
      assertThat(actual).as(sql).isEqualTo(expected);
  }

  private void assertUsesOrderedIndexFetch(final String sql) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      final String plan = rs.next().getProperty("executionPlanAsString");
      assertThat(plan).contains("FETCH FROM INDEX").contains("LIMIT 1").contains("FIRST ROW VALUE");
      assertThat(plan).doesNotContain("AGGREGAT");
    }
  }
}
