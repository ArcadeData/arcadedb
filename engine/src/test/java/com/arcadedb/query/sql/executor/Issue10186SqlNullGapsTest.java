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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #10186: three places where SQL still answered a NULL as a value after the three-valued logic of #4591,
 * #8979 and #9032 - {@code NOT IN} over a sub-query returning a NULL, {@code NOT LIKE} on a NULL, and {@code +}/{@code -} with
 * a NULL operand.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10186SqlNullGapsTest extends TestHelper {

  @BeforeEach
  void load() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.name STRING");
    database.command("sql", "CREATE PROPERTY T.i INTEGER");
    database.command("sql", "CREATE PROPERTY T.s STRING");
    database.command("sql", "CREATE DOCUMENT TYPE U");
    database.command("sql", "CREATE PROPERTY U.k INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE V");
    database.command("sql", "CREATE PROPERTY V.k INTEGER");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET name = 'three', i = 3, s = 'abc'");
      database.command("sql", "INSERT INTO T SET name = 'null', i = null, s = null");
      database.command("sql", "INSERT INTO T SET name = 'missing'");
      database.command("sql", "INSERT INTO U SET k = 4");
      database.command("sql", "INSERT INTO U SET k = null");
      database.command("sql", "INSERT INTO V SET k = 4");
    });
  }

  private List<String> names(final String where) {
    final List<String> got = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM T WHERE " + where + " ORDER BY name")) {
      while (rs.hasNext())
        got.add(rs.next().getProperty("name"));
    }
    return got;
  }

  @Test
  void notInOverASubQueryReturningANullIsUnknown() {
    // 3 NOT IN (4, NULL) is unknown, as the literal list already answers
    assertThat(names("i NOT IN [4, null]")).isEmpty();
    assertThat(names("i NOT IN (SELECT k FROM U)")).isEmpty();
    assertThat(names("NOT (i IN (SELECT k FROM U))")).isEmpty();
    assertThat(names("i IN (SELECT k FROM U)")).isEmpty();
    // a NULL row read through an expression, not a bare property, is the same NULL
    assertThat(names("i NOT IN (SELECT k + 0 AS k FROM U)")).isEmpty();
    // without the NULL the sub-query answers a definite FALSE for 3, and unknown for the rows without i
    assertThat(names("i NOT IN (SELECT k FROM V)")).containsExactly("three");
    assertThat(names("i NOT IN (SELECT k FROM U WHERE k IS NOT NULL)")).containsExactly("three");
    // an empty sub-query: NOT IN is TRUE for every row, even the ones without i
    assertThat(names("i NOT IN (SELECT k FROM U WHERE k = 99)")).containsExactly("missing", "null", "three");
    // a row of several columns is a row, not a NULL, even when one of its columns is null
    assertThat(names("i NOT IN (SELECT k, 1 AS one FROM U)")).containsExactly("three");
    // a record of the sub-query is not a NULL either
    assertThat(names("i NOT IN (SELECT FROM U)")).containsExactly("three");
  }

  @Test
  void notInOverALargeSubQueryWithOneNull() {
    database.transaction(() -> {
      for (int k = 100; k < 5_100; k++)
        database.newDocument("V").set("k", k).save();
    });
    assertThat(names("i NOT IN (SELECT k FROM V)")).containsExactly("three");
    database.transaction(() -> database.newDocument("V").set("k", null).save());
    assertThat(names("i NOT IN (SELECT k FROM V)")).isEmpty();
    assertThat(names("i IN (SELECT k FROM V)")).isEmpty();
  }

  @Test
  void notInOverAParameterSetOrArrayHoldingANullIsUnknown() {
    final Set<Object> set = new HashSet<>(Arrays.asList(4, null));
    for (final Object right : new Object[] { set, new Object[] { 4, null }, Arrays.asList(4, null) }) {
      final Map<String, Object> params = new HashMap<>();
      params.put("p", right);
      try (final ResultSet rs = database.query("sql", "SELECT name FROM T WHERE i NOT IN :p", params)) {
        assertThat(rs.hasNext()).as(right.getClass().getSimpleName()).isFalse();
      }
    }
  }

  @Test
  void anUnknownLikeDoesNotHideATrueBranchOfAnOr() {
    assertThat(names("s LIKE 'x%' OR name = 'null'")).containsExactly("null");
    assertThat(names("s NOT LIKE 'x%' OR name = 'null'")).containsExactly("null", "three");
    assertThat(names("NOT (s LIKE 'x%' AND name = 'null')")).containsExactly("missing", "three");
  }

  @Test
  void aNullPatternParameterIsUnknown() {
    final Map<String, Object> params = new HashMap<>();
    params.put("p", null);
    for (final String where : new String[] { "NOT (s LIKE :p)", "s NOT ILIKE :p", "NOT (s MATCHES :p)" })
      try (final ResultSet rs = database.query("sql", "SELECT name FROM T WHERE " + where, params)) {
        assertThat(rs.hasNext()).as(where).isFalse();
      }
  }

  @Test
  void notInOverASubQueryReturningANullIsUnknownThroughAnIndex() {
    database.command("sql", "CREATE INDEX ON T (i) NOTUNIQUE");
    assertThat(names("i NOT IN (SELECT k FROM U)")).isEmpty();
    assertThat(names("i IN (SELECT k FROM U)")).isEmpty();
    assertThat(names("i NOT IN (SELECT k FROM V)")).containsExactly("three");
  }

  @Test
  void likeOnANullIsUnknownThroughAnIndex() {
    database.command("sql", "CREATE INDEX ON T (s) NOTUNIQUE");
    assertThat(names("s NOT LIKE 'x%'")).containsExactly("three");
    assertThat(names("s LIKE 'a%'")).containsExactly("three");
    assertThat(names("NOT (s LIKE null)")).isEmpty();
  }

  @Test
  void likeOnANullIsUnknown() {
    for (final String where : new String[] { "s NOT LIKE 'x%'", "NOT (s LIKE 'x%')", "s NOT ILIKE 'x%'", "NOT (s ILIKE 'x%')",
        "NOT (s MATCHES 'x.*')", "NOT (s = 'x')", "s LIKE '%'", "s ILIKE '%'", "s MATCHES '.*'" })
      assertThat(names(where)).as(where).containsExactly("three");

    // a NULL pattern is unknown too
    assertThat(names("NOT (s LIKE null)")).isEmpty();
    assertThat(names("NOT (s ILIKE null)")).isEmpty();
    assertThat(names("NOT (s MATCHES null)")).isEmpty();
    assertThat(names("s IS NULL")).containsExactly("missing", "null");
  }

  @Test
  void additionAndSubtractionWithANullNumberAreNull() {
    try (final ResultSet rs = database.query("sql",
        "SELECT i + 1 AS plus, 1 + i AS plus2, 1 - i AS minus, i - 1 AS minus2, -i AS neg, i * 2 AS times, i / 2 AS div FROM T WHERE name = 'null'")) {
      final Result r = rs.next();
      for (final String p : new String[] { "plus", "plus2", "minus", "minus2", "neg", "times", "div" })
        assertThat(r.<Object>getProperty(p)).as(p).isNull();
    }

    assertThat(names("i - 4 < 6")).containsExactly("three");
    assertThat(names("i + 4 < 60")).containsExactly("three");
    assertThat(names("-i < 6")).containsExactly("three");
    assertThat(names("NOT (i - 4 < 6)")).isEmpty();
    assertThat(names("i * 1 < 6")).containsExactly("three");
    assertThat(names("i < 10")).containsExactly("three");
  }

  @Test
  void nullOperandIsNullForEveryScalarArithmeticOperator() {
    for (final String expr : new String[] { "i + 1", "1 + i", "i - 1", "1 - i", "-i", "i + 1 + 2", "1 + 2 * i", "i * 2 + 1", "i ^ 1",
        "1 ^ i", "i | 1", "1 | i", "i & 1", "null + null", "null - null", "date('2026-10-17', 'yyyy-MM-dd') - i",
        "date('2026-10-17', 'yyyy-MM-dd') + i", "i - date('2026-10-17', 'yyyy-MM-dd')", "duration(1, 'hour') + i", "i + duration(1, 'hour')" })
      assertThat(this.<Object>value(expr)).as(expr).isNull();

    // the non-null forms are unchanged
    assertThat(this.<Number>valueOf("three", "i + 1 + 2").intValue()).isEqualTo(6);
    assertThat(this.<Number>valueOf("three", "-i").intValue()).isEqualTo(-3);
    assertThat(this.<Number>valueOf("three", "i ^ 1").intValue()).isEqualTo(2);
    assertThat(this.<Number>valueOf("three", "i | 4").intValue()).isEqualTo(7);
  }

  @Test
  void concatenationKeepsTreatingANullAsAbsent() {
    assertThat(this.<String>value("'a' + s")).isEqualTo("a");
    assertThat(this.<String>value("s + 'a'")).isEqualTo("a");
    assertThat(this.<String>value("'a' + i")).isEqualTo("a");
    // a value that is not a number, a date or a duration is concatenated, not added, so it keeps the concatenation rule
    database.transaction(() -> database.command("sql", "UPDATE T SET flag = true WHERE name = 'null'"));
    assertThat(this.<Boolean>value("flag + i")).isTrue();
    assertThat(this.<List<Object>>value("[1, 2] + i")).containsExactly(1, 2);
    assertThat(this.<List<Object>>value("[1, 2] - i")).containsExactly(1, 2);
    assertThat(this.<Map<String, Object>>value("{'a': 1} - i")).containsOnlyKeys("a");
  }

  @Test
  void compoundAssignmentStartsAMissingCounterFromNothing() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET name = 'counter'");
      database.command("sql", "UPDATE T SET up += 2, down -= 3, text += 'x', times *= 2, div /= 2 WHERE name = 'counter'");
    });
    try (final ResultSet rs = database.query("sql", "SELECT up, down, text, times, div FROM T WHERE name = 'counter'")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("up").intValue()).isEqualTo(2);
      assertThat(r.<Number>getProperty("down").intValue()).isEqualTo(-3);
      assertThat(r.<String>getProperty("text")).isEqualTo("x");
      assertThat(r.<Object>getProperty("times")).isNull();
      assertThat(r.<Object>getProperty("div")).isNull();
    }

    database.transaction(() -> database.command("sql", "UPDATE T SET up += 2, down -= 3, text += 'y' WHERE name = 'counter'"));
    try (final ResultSet rs = database.query("sql", "SELECT up, down, text FROM T WHERE name = 'counter'")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("up").intValue()).isEqualTo(4);
      assertThat(r.<Number>getProperty("down").intValue()).isEqualTo(-6);
      assertThat(r.<String>getProperty("text")).isEqualTo("xy");
    }

    // a NULL increment leaves the field as it is: SET n += :unset does not wipe n
    final Map<String, Object> params = new HashMap<>();
    params.put("x", null);
    database.transaction(() -> database.command("sql", "UPDATE T SET up += :x, down -= :x, text += :x WHERE name = 'counter'", params));
    try (final ResultSet rs = database.query("sql", "SELECT up, down, text FROM T WHERE name = 'counter'")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("up").intValue()).isEqualTo(4);
      assertThat(r.<Number>getProperty("down").intValue()).isEqualTo(-6);
      assertThat(r.<String>getProperty("text")).isEqualTo("xy");
    }

    // a NULL increment of an absent field leaves it absent, and rewrites nothing
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE T SET never += :x, never2 -= :x WHERE name = 'counter'", params)) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
      }
    });
    try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE name = 'counter'")) {
      final Result r = rs.next();
      assertThat(r.hasProperty("never")).isFalse();
      assertThat(r.hasProperty("never2")).isFalse();
      assertThat(r.getRecord().get().asDocument().has("never")).isFalse();
    }

    // a list starts from nothing too, and subtracting from a missing field has nothing to subtract from
    database.transaction(() -> database.command("sql", "UPDATE T SET tags += ['a'], gone -= 'a', gone2 -= 5 WHERE name = 'counter'"));
    database.transaction(() -> database.command("sql", "UPDATE T SET tags += ['b'], tags -= 'a' WHERE name = 'counter'"));
    try (final ResultSet rs = database.query("sql", "SELECT tags, gone, gone2 FROM T WHERE name = 'counter'")) {
      final Result r = rs.next();
      assertThat(r.<List<Object>>getProperty("tags")).containsExactly("b");
      assertThat(r.<Object>getProperty("gone")).isNull();
      assertThat(r.<Number>getProperty("gone2").intValue()).isEqualTo(-5);
    }

    // the right-hand side of a compound assignment is evaluated once per record
    database.transaction(() -> database.command("sql", "UPDATE T SET up += (SELECT count(*) AS c FROM U) WHERE name = 'counter'"));
    try (final ResultSet rs = database.query("sql", "SELECT up FROM T WHERE name = 'counter'")) {
      assertThat(rs.next().<Number>getProperty("up").intValue()).isEqualTo(6);
    }
  }

  private <T> T value(final String expression) {
    return valueOf("null", expression);
  }

  private <T> T valueOf(final String name, final String expression) {
    try (final ResultSet rs = database.query("sql", "SELECT " + expression + " AS v FROM T WHERE name = ?", name)) {
      return rs.next().getProperty("v");
    }
  }
}
