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
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8970 (follow-up of #8919 / #8872): a bound passed as a parameter that the key type of an index cannot hold (an integer
 * past 2^53 on a DOUBLE, past 2^24 on a FLOAT, a BigDecimal on a DOUBLE) was converted to the key type by the index, which answered
 * for the rounded bound. A literal bound is kept away from the index at planning, but a plan with a parameter is cached and the
 * decision depends on the value. The indexed type must answer as the unindexed one does, for every operator and for a cached plan
 * run with different values.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8970LossyParameterBoundTest extends TestHelper {
  private static final long TWO_53 = 1L << 53;

  @Override
  public void beginTest() {
    database.transaction(() -> {
      for (final String type : new String[] { "I", "N" }) {
        database.command("sql", "CREATE VERTEX TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER");
        database.command("sql", "CREATE PROPERTY " + type + ".d DOUBLE");
        database.command("sql", "CREATE PROPERTY " + type + ".f FLOAT");
      }
      database.command("sql", "CREATE INDEX ON I (d) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON I (f) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON I (d, id) NOTUNIQUE");
      for (final String type : new String[] { "I", "N" }) {
        database.command("sql", "INSERT INTO " + type + " SET id = 1, d = 5.0, f = 5.0");
        database.command("sql", "INSERT INTO " + type + " SET id = 2, d = 9007199254740992.0, f = 16777216.0");
        database.command("sql", "INSERT INTO " + type + " SET id = 3, d = 9007199254740994.0, f = 16777218.0");
        database.command("sql", "INSERT INTO " + type + " SET id = 4, d = 9007199254740996.0, f = 16777220.0");
        database.command("sql", "INSERT INTO " + type + " SET id = 5, d = 0.1, f = 0.1");
        database.command("sql", "INSERT INTO " + type + " SET id = 6, d = 0.2, f = 0.2");
      }
    });
  }

  @Test
  void anIntegerParameterPastTwoToThe53OnADoubleAnswersAsTheScanDoes() {
    // 2^53 + 1 is not a double: equality matches nothing, and a value is never equal to a bound and below it at once
    assertThat(sql("I", "d = ?", TWO_53 + 1)).isEmpty();
    assertThat(sql("I", "d < ?", TWO_53 + 1)).containsExactly(1, 2, 5, 6);
    assertThat(sql("I", "d <= ?", TWO_53 + 1)).containsExactly(1, 2, 5, 6);
    assertThat(sql("I", "d >= ?", TWO_53 + 1)).containsExactly(3, 4);
    assertThat(sql("I", "d > ?", TWO_53 + 1)).containsExactly(3, 4);
  }

  @Test
  void everyOperatorAgreesWithTheUnindexedTypeForUnrepresentableBounds() {
    final Object[] bounds = { TWO_53 + 1, TWO_53 + 3, TWO_53 + 2, TWO_53 - 1, -(TWO_53 + 1), Long.MAX_VALUE, Long.MIN_VALUE,
        BigInteger.valueOf(TWO_53).add(BigInteger.valueOf(5)), new BigDecimal("9007199254740993"), new BigDecimal("0.1"),
        new BigDecimal("0.2"), new BigDecimal("0.10000000000000001"), new BigDecimal("0.100000000000000006"),
        new BigDecimal("0.15") };
    for (final Object bound : bounds)
      for (final String operator : new String[] { "=", "<", "<=", ">", ">=", "<>" })
        assertSameAsScan("d " + operator + " ?", bound);
  }

  @Test
  void betweenAndInAgreeWithTheUnindexedType() {
    assertSameAsScan("d BETWEEN ? AND ?", TWO_53 + 1, TWO_53 + 5);
    assertSameAsScan("d BETWEEN ? AND ?", TWO_53 + 3, TWO_53 + 3);
    assertSameAsScan("d BETWEEN ? AND ?", new BigDecimal("0.1"), new BigDecimal("0.15"));
    assertSameAsScan("d IN [?, ?]", TWO_53 + 1, TWO_53 + 2);
    assertSameAsScan("d IN [?, ?, ?]", TWO_53 + 1, TWO_53 + 3, 5L);
    assertSameAsScan("d IN ?", List.of(TWO_53 + 1, TWO_53 + 3, new BigDecimal("0.1")));
    assertThat(sql("I", "d IN ?", List.of(TWO_53 + 1, 5L))).containsExactly(1);
  }

  @Test
  void aBoundOnADoubleCombinedWithAnotherConditionAgreesWithTheUnindexedType() {
    assertSameAsScan("d = ? AND id = ?", TWO_53 + 1, 2);
    assertSameAsScan("d >= ? AND id > ?", TWO_53 + 1, 3);
    assertSameAsScan("d > ? AND d < ?", TWO_53 + 1, TWO_53 + 5);
    assertSameAsScan("d > ? AND d <= ?", new BigDecimal("0.1"), new BigDecimal("0.2"));
  }

  @Test
  void anIntegerParameterPastTwoToThe24OnAFloatAnswersAsTheScanDoes() {
    for (final long bound : new long[] { 16777217L, 16777219L, 16777218L, -16777217L })
      for (final String operator : new String[] { "=", "<", "<=", ">", ">=", "<>" })
        assertSameAsScan("f " + operator + " ?", bound);
    assertThat(sql("I", "f = ?", 16777217L)).isEmpty();
  }

  @Test
  void theSamePlanRunWithRepresentableAndUnrepresentableValuesAnswersEachOneRight() {
    // the plan of a parametrised statement is cached by its text: the first value must not decide the next ones
    final long[] sequence = { 5, TWO_53 + 1, TWO_53, TWO_53 + 3, 5, TWO_53 + 2, TWO_53 + 1 };
    for (final long bound : sequence) {
      assertSameAsScan("d = ?", bound);
      assertSameAsScan("d < ?", bound);
      assertSameAsScan("d >= ?", bound);
    }
    assertThat(sql("I", "d = ?", 5L)).containsExactly(1);
    assertThat(sql("I", "d = ?", TWO_53 + 2)).containsExactly(3);
  }

  @Test
  void aNamedParameterAnswersAsAPositionalOneDoes() {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM I WHERE d = :bound", java.util.Map.of("bound", TWO_53 + 1))) {
      while (rs.hasNext())
        ids.add(rs.next().<Integer>getProperty("id"));
    }
    assertThat(ids).isEmpty();
  }

  @Test
  void cypherParametersAgreeWithTheUnindexedType() {
    final Object[] bounds = { TWO_53 + 1, TWO_53 + 3, TWO_53 + 2, new BigDecimal("0.1"), new BigDecimal("9007199254740993") };
    for (final Object bound : bounds)
      for (final String operator : new String[] { "=", "<", "<=", ">", ">=", "<>" })
        assertThat(cypher("I", "d " + operator + " $b", bound)).as("cypher d %s %s", operator, bound)
            .isEqualTo(cypher("N", "d " + operator + " $b", bound));
    assertThat(cypher("I", "d = $b", TWO_53 + 1)).isEmpty();
    assertThat(cypher("I", "f = $b", 16777217L)).isEmpty();
  }

  private List<Integer> cypher(final String type, final String where, final Object bound) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:" + type + ") WHERE n." + where + " RETURN n.id AS id ORDER BY id",
        java.util.Map.of("b", bound))) {
      while (rs.hasNext())
        ids.add(rs.next().<Number>getProperty("id").intValue());
    }
    return ids;
  }

  private void assertSameAsScan(final String where, final Object... parameters) {
    final List<Integer> scan = sql("N", where, parameters);
    assertThat(sql("I", where, parameters)).as("WHERE %s with %s", where, List.of(parameters)).isEqualTo(scan);
  }

  private List<Integer> sql(final String type, final String where, final Object... parameters) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM " + type + " WHERE " + where + " ORDER BY id", parameters)) {
      while (rs.hasNext())
        ids.add(rs.next().<Integer>getProperty("id"));
    }
    return ids;
  }
}
