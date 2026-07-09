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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5163: <b>integer</b> division/modulo by zero must fail the query
 * (like Neo4j, and as required by the OpenCypher TCK) instead of silently returning {@code null}.
 * <p>
 * Note: <b>floating-point</b> division by zero is NOT an error. The OpenCypher TCK (see
 * {@code clauses/return-orderby/ReturnOrderBy1.feature}) requires {@code 0.0 / 0.0} to yield {@code NaN}
 * and {@code x / 0.0} to yield {@code ±Infinity}, matching IEEE 754 and Neo4j. This test also locks in
 * that behavior so a future "fix" does not regress TCK conformance.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5163DivisionByZeroTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./databases/test-issue5163").create();
    database.getSchema().createVertexType("U");
    database.transaction(() -> database.newVertex("U").set("id", 1).set("zero", 0).set("zeroD", 0.0).save());
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private void assertFails(final String cypher) {
    assertThatThrownBy(() -> {
      final ResultSet rs = database.query("opencypher", cypher);
      // Force full evaluation (execution is lazy).
      while (rs.hasNext())
        rs.next();
    }).hasMessageContaining("by zero");
  }

  @Test
  void integerDivisionByZeroConstant() {
    assertFails("RETURN 1 / 0 AS r");
  }

  @Test
  void zeroDividedByZeroConstant() {
    assertFails("RETURN 0 / 0 AS r");
  }

  @Test
  void integerModuloByZeroConstant() {
    assertFails("RETURN 1 % 0 AS r");
  }

  @Test
  void integerDivisionByZeroRuntime() {
    // Divisor comes from a property so constant folding cannot pre-compute it.
    assertFails("MATCH (u:U) RETURN 1 / u.zero AS r");
  }

  @Test
  void integerModuloByZeroRuntime() {
    assertFails("MATCH (u:U) RETURN 1 % u.zero AS r");
  }

  // ---- Floating-point division by zero: IEEE 754 semantics, NOT an error (OpenCypher TCK) ----

  @Test
  void floatDivisionByZeroYieldsInfinity() {
    final ResultSet rs = database.query("opencypher", "RETURN 1 / 0.0 AS r");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("r").doubleValue()).isEqualTo(Double.POSITIVE_INFINITY);
  }

  @Test
  void floatZeroDividedByZeroYieldsNaN() {
    final ResultSet rs = database.query("opencypher", "RETURN 0.0 / 0.0 AS r");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("r").doubleValue()).isNaN();
  }

  @Test
  void floatModuloByZeroYieldsNaN() {
    final ResultSet rs = database.query("opencypher", "RETURN 1 % 0.0 AS r");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("r").doubleValue()).isNaN();
  }

  @Test
  void floatDivisionByZeroRuntimeYieldsInfinity() {
    final ResultSet rs = database.query("opencypher", "MATCH (u:U) RETURN 1 / u.zeroD AS r");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("r").doubleValue()).isEqualTo(Double.POSITIVE_INFINITY);
  }

  @Test
  void divisionByZeroInWhere() {
    // Must fail, not return the row (silent null propagation bug).
    assertFails("MATCH (u:U) WHERE 1 / 0 IS NULL RETURN u.id");
  }

  @Test
  void divisionByZeroInCase() {
    assertFails("RETURN CASE WHEN 1 / 0 IS NULL THEN 'got_null' ELSE 'ok' END AS r");
  }

  @Test
  void divisionByZeroInWrite() {
    assertThatThrownBy(() -> database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", "CREATE (x:Div {v: 1 / 0}) RETURN x.v");
      while (rs.hasNext())
        rs.next();
    })).hasMessageContaining("by zero");
  }

  @Test
  void validIntegerDivisionStillWorks() {
    final ResultSet rs = database.query("opencypher", "RETURN 1 / 2 AS r");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("r").longValue()).isEqualTo(0L);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void validFloatDivisionStillWorks() {
    final ResultSet rs = database.query("opencypher", "RETURN 3.0 / 2 AS r");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("r").doubleValue()).isEqualTo(1.5);
    assertThat(rs.hasNext()).isFalse();
  }
}
