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
package com.arcadedb.query;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IntegralKeyBound;
import com.arcadedb.query.select.SelectOperator;
import com.arcadedb.query.select.SelectWhereAfterBlock;
import com.arcadedb.query.select.SelectWhereOperatorBlock;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.BinaryTypes;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9021: an index on an integral property (BYTE, SHORT, INTEGER, LONG) converted a bound to its key type, truncating a
 * fraction and clamping a value past the range, so it answered for another bound than the scan: {@code i = 12.5} found 12,
 * {@code i < 12.5} lost it, and {@code l = 1e19} found {@code Long.MAX_VALUE}. Every query runs on the indexed type {@code I}
 * and on the identical unindexed type {@code S}, and the two must agree.
 */
class Issue9021IntegralIndexInexactBoundTest extends TestHelper {

  private static final String[] OPERATORS = { "=", "<", "<=", ">", ">=" };

  @Override
  public void beginTest() {
    database.transaction(() -> {
      for (final String type : new String[] { "I", "S" }) {
        database.command("sql", "CREATE VERTEX TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".i INTEGER");
        database.command("sql", "CREATE PROPERTY " + type + ".l LONG");
        database.command("sql", "CREATE PROPERTY " + type + ".s SHORT");
        database.command("sql", "CREATE PROPERTY " + type + ".b BYTE");
        database.command("sql", "CREATE PROPERTY " + type + ".c INTEGER");
        database.command("sql", "CREATE PROPERTY " + type + ".d INTEGER");
      }
      database.command("sql", "CREATE INDEX ON I (i) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON I (l) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON I (s) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON I (b) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON I (c, d) NOTUNIQUE");
      for (final String type : new String[] { "I", "S" })
        for (int k = 11; k <= 13; k++)
          database.newVertex(type).set("i", k, "l", Long.MAX_VALUE - 13 + k, "s", (short) k, "b", (byte) k, "c", k % 2, "d", k).save();
    });
  }

  @Test
  void sqlLinesOfTheIssue() {
    for (final String type : new String[] { "I", "S" }) {
      assertThat(sql("SELECT i FROM " + type + " WHERE i = 12.5")).as(type).isEmpty();
      assertThat(sql("SELECT i FROM " + type + " WHERE i = ?", 12.5d)).as(type).isEmpty();
      assertThat(sql("SELECT i FROM " + type + " WHERE i >= 12.5")).as(type).containsExactly(13);
      assertThat(sql("SELECT i FROM " + type + " WHERE i < 12.5")).as(type).containsExactly(11, 12);
      assertThat(sql("SELECT i FROM " + type + " WHERE i < ?", 12.5d)).as(type).containsExactly(11, 12);
      assertThat(sql("SELECT i FROM " + type + " WHERE l = ?", 1e19d)).as(type).isEmpty();
      assertThat(sql("SELECT i FROM " + type + " WHERE l = ?", new BigInteger("18446744073709551615"))).as(type).isEmpty();
      assertThat(sql("SELECT i FROM " + type + " WHERE l > ?", 1e19d)).as(type).isEmpty();
      assertThat(sql("SELECT i FROM " + type + " WHERE l < ?", 1e19d)).as(type).containsExactly(11, 12, 13);
      // the scan of BETWEEN converted its bounds to the value's class, so 11 was between 11.5 and 12.5
      assertThat(sql("SELECT i FROM " + type + " WHERE i BETWEEN 11.5 AND 12.5")).as(type).containsExactly(12);
      assertThat(sql("SELECT i FROM " + type + " WHERE i BETWEEN ? AND 13", -3_000_000_000L)).as(type).containsExactly(11, 12, 13);
    }
  }

  @Test
  void fluentSelectIndexAgreesWithTheScan() {
    for (final String type : new String[] { "I", "S" }) {
      assertThat(fluent(type, SelectOperator.lt, 12.5d)).as(type + " <").containsExactly(11, 12);
      assertThat(fluent(type, SelectOperator.le, 12.5d)).as(type + " <=").containsExactly(11, 12);
      assertThat(fluent(type, SelectOperator.gt, 11.5d)).as(type + " >").containsExactly(12, 13);
      assertThat(fluent(type, SelectOperator.ge, 11.5d)).as(type + " >=").containsExactly(12, 13);
      assertThat(fluent(type, SelectOperator.eq, 12.5d)).as(type + " =").isEmpty();
      assertThat(fluent(type, SelectOperator.lt, 3_000_000_000L)).as(type + " < 3e9").containsExactly(11, 12, 13);
      assertThat(fluent(type, SelectOperator.gt, 3_000_000_000L)).as(type + " > 3e9").isEmpty();
      assertThat(fluent(type, SelectOperator.between, 11.5d, 12.5d)).as(type + " between").containsExactly(12);
      assertThat(fluent(type, SelectOperator.between, 12.2d, 12.7d)).as(type + " empty between").isEmpty();
    }
  }

  private List<Object> fluent(final String type, final SelectOperator operator, final Object... value) {
    final SelectWhereOperatorBlock where = database.select().fromType(type).where().property("i");
    final SelectWhereAfterBlock after = switch (operator) {
      case lt -> where.lt().value(value[0]);
      case le -> where.le().value(value[0]);
      case gt -> where.gt().value(value[0]);
      case ge -> where.ge().value(value[0]);
      case eq -> where.eq().value(value[0]);
      default -> where.between().values(value[0], value[1]);
    };
    final List<Object> rows = new ArrayList<>();
    after.vertices().forEachRemaining(v -> rows.add(v.get("i")));
    rows.sort(null);
    return rows;
  }

  @Test
  void cypherLinesOfTheIssue() {
    for (final String type : new String[] { "I", "S" }) {
      assertThat(cypher("MATCH (n:" + type + ") WHERE n.i = 12.5 RETURN n.i AS i ORDER BY i", Map.of())).as(type).isEmpty();
      assertThat(cypher("MATCH (n:" + type + ") WHERE n.i >= 12.5 RETURN n.i AS i ORDER BY i", Map.of())).as(type).containsExactly(13);
      assertThat(cypher("MATCH (n:" + type + ") WHERE n.i < 12.5 RETURN n.i AS i ORDER BY i", Map.of())).as(type).containsExactly(11, 12);
    }
  }

  @Test
  void theIndexIsUsedForTheBoundsUnderTest() {
    // the agreement below proves nothing if the indexed type is answered by a scan as well
    for (final String where : new String[] { "i = 12.5", "i < 12.5", "i >= ?", "l = ?", "s > 12.5", "b <= 12.5", "c = 1 AND d < 12.5" })
      try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT i FROM I WHERE " + where, 12.5d)) {
        assertThat(rs.next().<String>getProperty("executionPlanAsString")).as(where).contains("FETCH FROM INDEX");
      }
  }

  @Test
  void sqlIndexAgreesWithTheScanForEveryOperatorAndInexactBound() {
    final Object[] bounds = { 12.5d, 12.5f, -12.5d, 10.5d, 13.5d, 12.0d, new BigDecimal("12.5"), 3_000_000_000L, -3_000_000_000L, 1e19d, -1e19d,
        new BigInteger("18446744073709551615"), Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN, 40_000, 300 };
    for (final String field : new String[] { "i", "l", "s", "b" })
      for (final String op : OPERATORS)
        for (final Object bound : bounds) {
          final String where = field + " " + op + " ?";
          assertThat(sql("SELECT i FROM I WHERE " + where, bound)).as(where + " with " + describe(bound))
              .isEqualTo(sql("SELECT i FROM S WHERE " + where, bound));
        }
  }

  @Test
  void sqlLiteralBoundsAgreeWithTheScan() {
    for (final String op : OPERATORS)
      for (final String literal : new String[] { "12.5", "-12.5", "13.5", "10.5", "12.0", "3000000000", "-3000000000" })
        for (final String field : new String[] { "i", "s", "b" }) {
          final String where = field + " " + op + " " + literal;
          assertThat(sql("SELECT i FROM I WHERE " + where)).as(where).isEqualTo(sql("SELECT i FROM S WHERE " + where));
        }
  }

  @Test
  void sqlTwoSidedRangesBetweenAndInAgreeWithTheScan() {
    final String[] wheres = { "i > 11.5 AND i < 12.5", "i >= 11.5 AND i <= 13.5", "i > ? AND i <= 13", "i BETWEEN 11.5 AND 12.5",
        "i BETWEEN ? AND 13", "i IN [12.5, 13]", "i IN [12.5]", "i IN ?", "l BETWEEN ? AND 9223372036854775807", "c = 1 AND d < 12.5",
        "c = 1 AND d >= 12.5", "c = 0.5 AND d > 0", "i > 12.2 AND i < 12.7", "i BETWEEN 12.2 AND 12.7", "i BETWEEN 13 AND 12", "c = 1 AND d = 12.5", "c = 1 AND d > 11.5 AND d < 13.5", "c = 1 AND d BETWEEN 10.5 AND 12.5" };
    for (final String where : wheres) {
      final Object parameter = where.contains("IN ?") ? List.of(12.5d, 11) : where.startsWith("l ") ? 1e19d : 11.5d;
      assertThat(sql("SELECT i FROM I WHERE " + where, parameter)).as(where).isEqualTo(sql("SELECT i FROM S WHERE " + where, parameter));
    }
  }

  @Test
  void sqlDescendingOrderOverAnInexactRange() {
    for (final String type : new String[] { "I", "S" }) {
      assertThat(sql("SELECT i FROM " + type + " WHERE i < 12.5 ORDER BY i DESC")).as(type).containsExactly(12, 11);
      assertThat(sql("SELECT i FROM " + type + " WHERE i > 11.5 ORDER BY i DESC")).as(type).containsExactly(13, 12);
    }
  }

  @Test
  void cypherIndexAgreesWithTheScanForEveryOperatorAndInexactBound() {
    final Object[] bounds = { 12.5d, -12.5d, 10.5d, 13.5d, 12.0d, 3_000_000_000L, -3_000_000_000L, 1e19d, -1e19d, Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY };
    for (final String field : new String[] { "i", "l", "s", "b" })
      for (final String op : OPERATORS)
        for (final Object bound : bounds) {
          final String where = "n." + field + " " + op + " $p";
          assertThat(cypher("MATCH (n:I) WHERE " + where + " RETURN n.i AS i ORDER BY i", Map.of("p", bound))).as(where + " with " + describe(bound))
              .isEqualTo(cypher("MATCH (n:S) WHERE " + where + " RETURN n.i AS i ORDER BY i", Map.of("p", bound)));
        }
    for (final String where : new String[] { "n.i > 12.2 AND n.i < 12.7", "n.i > 11.5 AND n.i < 12.5", "n.i >= 10.5 AND n.i <= 12.5", "n.i < 12.5", "n.i <= 12.5", "n.i > 12.5" })
      assertThat(cypher("MATCH (n:I) WHERE " + where + " RETURN n.i AS i ORDER BY i DESC", Map.of())).as(where)
          .isEqualTo(cypher("MATCH (n:S) WHERE " + where + " RETURN n.i AS i ORDER BY i DESC", Map.of()));
  }

  @Test
  void boundMapping() {
    final byte INT = BinaryTypes.TYPE_INT;
    assertThat(IntegralKeyBound.isInexact(INT, 12)).isFalse();
    assertThat(IntegralKeyBound.isInexact(INT, 12.0d)).isFalse();
    assertThat(IntegralKeyBound.isInexact(INT, new BigDecimal("12.000"))).isFalse();
    assertThat(IntegralKeyBound.isInexact(INT, 12.5d)).isTrue();
    assertThat(IntegralKeyBound.isInexact(INT, 3_000_000_000L)).isTrue();
    assertThat(IntegralKeyBound.isInexact(INT, Double.NaN)).isTrue();
    assertThat(IntegralKeyBound.isInexact(BinaryTypes.TYPE_DOUBLE, 12.5d)).isFalse();
    assertThat(IntegralKeyBound.isInexact(INT, "12.5")).isFalse();

    assertThat(IntegralKeyBound.ceiling(INT, 12.5d)).isEqualTo(13);
    assertThat(IntegralKeyBound.floor(INT, 12.5d)).isEqualTo(12);
    assertThat(IntegralKeyBound.ceiling(INT, -12.5d)).isEqualTo(-12);
    assertThat(IntegralKeyBound.floor(INT, -12.5d)).isEqualTo(-13);
    assertThat(IntegralKeyBound.ceiling(INT, 3_000_000_000L)).isNull();
    assertThat(IntegralKeyBound.floor(INT, 3_000_000_000L)).isEqualTo(Integer.MAX_VALUE);
    assertThat(IntegralKeyBound.ceiling(INT, -3_000_000_000L)).isEqualTo(Integer.MIN_VALUE);
    assertThat(IntegralKeyBound.floor(INT, -3_000_000_000L)).isNull();
    assertThat(IntegralKeyBound.ceiling(BinaryTypes.TYPE_LONG, 1e19d)).isNull();
    assertThat(IntegralKeyBound.floor(BinaryTypes.TYPE_LONG, 1e19d)).isEqualTo(Long.MAX_VALUE);
    assertThat(IntegralKeyBound.floor(BinaryTypes.TYPE_SHORT, 12.5d)).isEqualTo((short) 12);
    assertThat(IntegralKeyBound.ceiling(BinaryTypes.TYPE_BYTE, 12.5d)).isEqualTo((byte) 13);
    // NaN orders above every number, as the scan compares it
    assertThat(IntegralKeyBound.ceiling(INT, Double.NaN)).isNull();
    assertThat(IntegralKeyBound.floor(INT, Double.NaN)).isEqualTo(Integer.MAX_VALUE);
  }

  private static String describe(final Object bound) {
    return bound.getClass().getSimpleName() + " " + bound;
  }

  private List<Object> sql(final String query, final Object... args) {
    final List<Object> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query + (query.contains("ORDER BY") ? "" : " ORDER BY i"), args)) {
      rs.forEachRemaining(r -> rows.add(r.getProperty("i")));
    }
    return rows;
  }

  private List<Object> cypher(final String query, final Map<String, Object> params) {
    final List<Object> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      rs.forEachRemaining(r -> rows.add(r.getProperty("i")));
    }
    return rows;
  }
}
