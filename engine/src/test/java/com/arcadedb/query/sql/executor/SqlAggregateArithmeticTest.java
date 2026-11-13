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
import com.arcadedb.function.sql.math.SQLFunctionAverage;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Arithmetic and aggregates in SQL over every numeric width: bound parameters of unusual numeric classes, sums that overflow a
 * LONG, BYTE operands, and the hardwired aggregate shortcuts that must keep the expression around the aggregate.
 */
class SqlAggregateArithmeticTest extends TestHelper {

  // ---- #7917: arithmetic on a bound BigInteger or Byte parameter ----
  /*
   * The SQL surface of issue #7917: a BigInteger or Byte bound as a query parameter reaches MathExpression.Operator.apply
   * unconverted, so SELECT abs(:big) + 1 answered HTTP 500 with an IllegalArgumentException from the expression evaluator while
   * the comparison of the identical pair, which #7669 fixed, answered correctly.
   */

  /** Regression test for issue #7917. */
  @Test
  void arithmeticOnABoundBigIntegerParameterWorks() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.begin();
    try {
      database.command("sql", "INSERT INTO T SET n = 1");
    } finally {
      database.commit();
    }

    final BigInteger big = BigInteger.valueOf(2).pow(70);

    try (final ResultSet rs = database.query("sql", "SELECT abs(:big) + 1 AS r FROM T", Map.of("big", big))) {
      final Number r = rs.next().getProperty("r");
      assertThat(new BigDecimal(r.toString())).isEqualByComparingTo(new BigDecimal(big.add(BigInteger.ONE)));
    }

    try (final ResultSet rs = database.query("sql", "SELECT 1 + :big AS r FROM T", Map.of("big", big))) {
      final Number r = rs.next().getProperty("r");
      assertThat(new BigDecimal(r.toString())).isEqualByComparingTo(new BigDecimal(big.add(BigInteger.ONE)));
    }

    // the comparison path, which has worked since #7669 - here so the two stay pinned together
    try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE n < :big", Map.of("big", big))) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  /** Regression test for issue #7917. */
  @Test
  void arithmeticOnABoundByteParameterWorks() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.begin();
    try {
      database.command("sql", "INSERT INTO T SET n = 1");
    } finally {
      database.commit();
    }

    try (final ResultSet rs = database.query("sql", "SELECT :b + 1 AS r FROM T", Map.of("b", (byte) 2))) {
      assertThat(((Number) rs.next().getProperty("r")).longValue()).isEqualTo(3L);
    }
  }

  // ---- #8974: sum() over LONG values widens instead of wrapping ----
  /*
   * sum() over LONG values wrapped silently past Long.MAX_VALUE and SQL avg() was computed from the wrapped sum.
   */

  private void longSumSetup() {
    database.command("sql", "CREATE VERTEX TYPE L");
    database.command("sql", "CREATE PROPERTY L.n LONG");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newVertex("L").set("n", 4_000_000_000_000_000_000L).save();
    });
  }

  /** Regression test for issue #8974. */
  @Test
  void sqlSumWidens() {
    longSumSetup();
    try (final ResultSet rs = database.query("sql", "SELECT sum(n) AS s, avg(n) AS a FROM L")) {
      final Result row = rs.next();
      assertThat(new BigDecimal(row.<Number>getProperty("s").toString())).isEqualByComparingTo("12000000000000000000");
      assertThat(row.<Number>getProperty("a").doubleValue()).isEqualTo(4.0E18);
    }
  }

  /** Regression test for issue #8974. */
  @Test
  void cypherSumWidens() {
    longSumSetup();
    try (final ResultSet rs = database.query("opencypher", "MATCH (x:L) RETURN sum(x.n) AS s, avg(x.n) AS a")) {
      final Result row = rs.next();
      assertThat(new BigDecimal(row.<Number>getProperty("s").toString())).isEqualByComparingTo("12000000000000000000");
      assertThat(row.<Number>getProperty("a").doubleValue()).isEqualTo(4.0E18);
    }
  }

  /** Regression test for issue #8974. */
  @Test
  void negativeOverflowAndMixedWidths() {
    longSumSetup();
    assertThat(Type.increment(Long.MIN_VALUE, -1L)).isEqualTo(new BigDecimal("-9223372036854775809"));
    assertThat(Type.increment(Long.MAX_VALUE, 1)).isEqualTo(new BigDecimal("9223372036854775808"));
    assertThat(Type.increment(1, Long.MAX_VALUE)).isEqualTo(new BigDecimal("9223372036854775808"));
    assertThat(Type.increment((short) 1, Long.MAX_VALUE)).isEqualTo(new BigDecimal("9223372036854775808"));
    assertThat(Type.increment(Long.MAX_VALUE - 1, 1L)).isEqualTo(Long.MAX_VALUE);
  }

  /** Regression test for issue #8974. */
  @Test
  void decimalAvgKeepsDecimalResult() {
    longSumSetup();
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.x DECIMAL");
    database.transaction(() -> {
      database.newDocument("D").set("x", new BigDecimal("10")).save();
      database.newDocument("D").set("x", new BigDecimal("3")).save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT avg(x) AS a FROM D")) {
      assertThat(rs.next().<Object>getProperty("a")).isInstanceOf(BigDecimal.class);
    }
  }

  /** Regression test for issue #8974. */
  @Test
  void negativeOverflowAndGroupedAvgAtSqlLevel() {
    longSumSetup();
    database.command("sql", "CREATE VERTEX TYPE M");
    database.command("sql", "CREATE PROPERTY M.g INTEGER");
    database.command("sql", "CREATE PROPERTY M.n LONG");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newVertex("M").set("g", 1, "n", -4_000_000_000_000_000_000L).save();
      database.newVertex("M").set("g", 2, "n", 7L).save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT g, sum(n) AS s, avg(n) AS a FROM M GROUP BY g ORDER BY g")) {
      final Result g1 = rs.next();
      assertThat(new BigDecimal(g1.<Number>getProperty("s").toString())).isEqualByComparingTo("-12000000000000000000");
      assertThat(g1.<Number>getProperty("a").doubleValue()).isEqualTo(-4.0E18);
      final Result g2 = rs.next();
      assertThat(g2.<Number>getProperty("s").longValue()).isEqualTo(7L);
      assertThat(g2.<Number>getProperty("a").doubleValue()).isEqualTo(7.0);
    }
  }

  /** Regression test for issue #8974. */
  @Test
  void widenedSumSurvivesNonFiniteOperand() {
    longSumSetup();
    final Number widened = Type.increment(Long.MAX_VALUE, 1L);
    assertThat(Type.increment(widened, Double.NaN).doubleValue()).isNaN();
    assertThat(Type.increment(widened, Double.POSITIVE_INFINITY).doubleValue()).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(Type.increment(widened, Float.NaN).floatValue()).isNaN();
  }

  /** Regression test for issue #8974. */
  @Test
  void widenedSumPlusDecimalStaysDecimal() {
    longSumSetup();
    assertThat(Type.increment(Type.increment(Long.MAX_VALUE, 1L), new BigDecimal("1.5"))).isEqualTo(new BigDecimal("9223372036854775809.5"));
  }

  /** Regression test for issue #8974. */
  @Test
  void largeSumOverManyRowsWidens() {
    longSumSetup();
    database.transaction(() -> {
      for (int i = 0; i < 3000; i++)
        database.newVertex("L").set("n", 4_000_000_000_000_000L).save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT sum(n) AS s FROM L")) {
      assertThat(new BigDecimal(rs.next().<Number>getProperty("s").toString())).isEqualByComparingTo("24000000000000000000");
    }
  }

  /** Regression test for issue #8974. */
  @Test
  void averageMergesAnOverflowedPartialWithANormalOne() {
    longSumSetup();
    final SQLFunctionAverage overflowed = new SQLFunctionAverage();
    overflowed.config(new Object[] { "n" });
    for (int i = 0; i < 3; i++)
      overflowed.execute(null, null, null, new Object[] { 4_000_000_000_000_000_000L }, null);

    final SQLFunctionAverage normal = new SQLFunctionAverage();
    normal.config(new Object[] { "n" });
    normal.execute(null, null, null, new Object[] { 4_000_000_000_000_000_000L }, null);

    overflowed.mergePartial(normal);
    assertThat(((Number) overflowed.getResult()).doubleValue()).isEqualTo(4.0E18);

    // a real decimal input in the other partial flips the result back to a BigDecimal
    final SQLFunctionAverage decimal = new SQLFunctionAverage();
    decimal.config(new Object[] { "n" });
    decimal.execute(null, null, null, new Object[] { new BigDecimal("1.5") }, null);
    overflowed.mergePartial(decimal);
    assertThat(overflowed.getResult()).isInstanceOf(BigDecimal.class);
  }

  // ---- #8976: hardwired aggregate shortcuts keep the projection around the aggregate ----
  /*
   * The count(*) and min()/max() shortcuts that answer a whole-type aggregate without reading records dropped the arithmetic or
   * the modifier around the aggregate.
   */

  private void hardwiredAggregateSetup() {
    for (final String type : new String[] { "Plain", "Indexed" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".i INTEGER");
      database.command("sql", "CREATE PROPERTY " + type + ".s STRING");
    }
    database.command("sql", "CREATE INDEX ON Indexed (s) NOTUNIQUE");
    database.transaction(() -> {
      for (final String type : new String[] { "Plain", "Indexed" }) {
        database.newDocument(type).set("i", 10, "s", "abc").save();
        database.newDocument(type).set("i", 20, "s", "z").save();
        database.newDocument(type).set("s", "hello").save();
      }
    });
  }

  private Object single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("v");
    }
  }

  /** Regression test for issue #8976. */
  @Test
  void countStarWithArithmetic() {
    hardwiredAggregateSetup();
    assertThat(((Number) single("SELECT count(*) + 1 AS v FROM Plain")).longValue()).isEqualTo(4L);
    assertThat(((Number) single("SELECT count(*) * 10 AS v FROM Plain")).longValue()).isEqualTo(30L);
    assertThat(((Number) single("SELECT count(*) + 1 AS v FROM Indexed")).longValue()).isEqualTo(4L);
    assertThat(((Number) single("SELECT count(*) AS v FROM Plain")).longValue()).isEqualTo(3L);
  }

  /** Regression test for issue #8976. */
  @Test
  void minMaxWithModifierOnIndexedProperty() {
    hardwiredAggregateSetup();
    for (final String type : new String[] { "Plain", "Indexed" }) {
      assertThat(((Number) single("SELECT max(s.length()) AS v FROM " + type)).intValue()).isEqualTo(5);
      assertThat(((Number) single("SELECT min(s.length()) AS v FROM " + type)).intValue()).isEqualTo(1);
      assertThat(single("SELECT max(s.toUpperCase()) AS v FROM " + type)).isEqualTo("Z");
    }
    assertThat(single("SELECT max(s) AS v FROM Indexed")).isEqualTo("z");
    assertThat(single("SELECT min(s) AS v FROM Indexed")).isEqualTo("abc");
  }

  // ---- #9281: sum() and avg() over a BYTE property ----

  /** Regression test for issue #9281: sum() and avg() over a BYTE property threw IllegalArgumentException (SQL and openCypher). */
  @Test
  void sumAvgMinMaxOverByte() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.b BYTE");
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE PROPERTY V.b BYTE");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET b = 100");
      database.command("sql", "INSERT INTO T SET b = 50");
      database.command("sql", "CREATE VERTEX V SET b = 100");
      database.command("sql", "CREATE VERTEX V SET b = 50");
    });

    assertThat(one("sql", "SELECT sum(b) AS r FROM T")).isEqualTo(150);
    assertThat(((Number) one("sql", "SELECT avg(b) AS r FROM T")).doubleValue()).isEqualTo(75.0);
    assertThat(((Number) one("sql", "SELECT min(b) AS r FROM T")).intValue()).isEqualTo(50);
    assertThat(((Number) one("sql", "SELECT max(b) AS r FROM T")).intValue()).isEqualTo(100);
    assertThat(((Number) one("opencypher", "MATCH (n:V) RETURN sum(n.b) AS r")).intValue()).isEqualTo(150);
    assertThat(((Number) one("opencypher", "MATCH (n:V) RETURN avg(n.b) AS r")).doubleValue()).isEqualTo(75.0);
    assertThat(((Number) one("opencypher", "MATCH (n:V) RETURN min(n.b) AS r")).intValue()).isEqualTo(50);
    assertThat(((Number) one("opencypher", "MATCH (n:V) RETURN max(n.b) AS r")).intValue()).isEqualTo(100);
  }

  private Object one(final String language, final String query) {
    try (final ResultSet rs = database.query(language, query)) {
      return rs.next().getProperty("r");
    }
  }
}
