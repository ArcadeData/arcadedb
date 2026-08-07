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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5835: {@code AND} and {@code OR} evaluated their result-irrelevant
 * operand and surfaced its runtime error, even though the boolean result was already determined
 * by the other operand (e.g. {@code false AND E} or {@code true OR E}). An equivalent unreachable
 * expression inside {@code CASE} was correctly skipped, which made the same runtime expression
 * behave differently depending only on which construct wrapped it.
 * <p>
 * {@code AND}/{@code OR} are built with two independent AST node types depending on where they
 * appear: {@code TernaryLogicalExpression} for {@code RETURN}/{@code WITH} projections and
 * {@code LogicalExpression} for {@code WHERE} predicates. Both need the short-circuit fix.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5835AndOrShortCircuitTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (:_Oracle {_id:'n1'}), (:_Oracle {_id:'n2'})");
  }

  // --- RETURN/WITH projections (TernaryLogicalExpression) ---

  @Test
  void andWithFalseLeftShortCircuitsStaticRightError() {
    assertThat(scalar("RETURN false AND (left('abc', -1) = 0) AS x")).isEqualTo(false);
  }

  @Test
  void orWithTrueLeftShortCircuitsStaticRightError() {
    assertThat(scalar("RETURN true OR (left('abc', -1) = 0) AS x")).isEqualTo(true);
  }

  @Test
  void andWithFalseLeftShortCircuitsRuntimeBoundRightError() {
    assertThat(scalar("WITH 2 AS k RETURN false AND (left('abc', 0-k) = 0) AS x")).isEqualTo(false);
  }

  @Test
  void andWithTrueLeftStillEvaluatesRightAndThrows() {
    // Control: the right operand's value is required to determine the result, so it must
    // still be evaluated and its error must still surface.
    assertThatThrownBy(() -> scalar("RETURN true AND (left('abc', -1) = 0) AS x"))
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void orWithFalseLeftStillEvaluatesRightAndThrows() {
    // Control: same reasoning for OR when the left operand is false.
    assertThatThrownBy(() -> scalar("RETURN false OR (left('abc', -1) = 0) AS x"))
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void unselectedCaseBranchStillSkipsRuntimeBoundError() {
    // Control: CASE already behaved correctly; must remain unaffected by the AND/OR fix.
    assertThat(scalar("""
        WITH 2 AS k \
        RETURN CASE \
          WHEN false THEN left('abc', 0-k) \
          ELSE 1 \
        END AS x""")).isEqualTo(1L);
  }

  // --- Three-valued (null) logic must be unaffected by the short-circuit fix ---

  @Test
  void nullAndFalseIsFalse() {
    assertThat(scalar("RETURN (null AND false) AS x")).isEqualTo(false);
  }

  @Test
  void trueAndNullIsNull() {
    assertThat(scalar("RETURN (true AND null) AS x")).isNull();
  }

  @Test
  void nullOrTrueIsTrue() {
    assertThat(scalar("RETURN (null OR true) AS x")).isEqualTo(true);
  }

  @Test
  void falseOrNullIsNull() {
    assertThat(scalar("RETURN (false OR null) AS x")).isNull();
  }

  // --- WHERE predicates (LogicalExpression) ---

  @Test
  void whereFalseAndShortCircuitsStaticRightError() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:_Oracle) WHERE false AND (left('abc', -1) = 0) RETURN n._id AS id")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void whereFalseAndShortCircuitsRuntimeBoundRightError() {
    // Left operand is a runtime-evaluated comparison (not a literal), so it cannot be caught
    // by any compile-time boolean-literal simplification: this exercises LogicalExpression's
    // own short-circuit, not just a rewrite pass.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:_Oracle) WHERE (n._id = 'does-not-exist') AND (left('abc', -1) = 0) RETURN n._id AS id")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void whereTrueOrShortCircuitsRuntimeBoundRightError() {
    // A WITH-bound (non-literal) true flag on the left of OR: cannot be caught by any
    // compile-time boolean-literal simplification either.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:_Oracle) WHERE n._id = 'n1' WITH n, true AS flag " +
            "WHERE flag OR (left('abc', -1) = 0) RETURN n._id AS id")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("id")).isEqualTo("n1");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void whereTrueOrShortCircuitsStaticRightError() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:_Oracle) WHERE true OR (left('abc', -1) = 0) RETURN n._id AS id ORDER BY id")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("id")).isEqualTo("n1");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("id")).isEqualTo("n2");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void whereTrueAndStillEvaluatesRightAndThrows() {
    // Control: the right operand's value is required, so its error must still surface.
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (n:_Oracle) WHERE true AND (left('abc', -1) = 0) RETURN n")) {
        while (rs.hasNext())
          rs.next();
      }
    }).isInstanceOf(CommandSemanticException.class);
  }

  private Object scalar(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      assertThat(rs.hasNext()).as("query returned no rows: %s", cypher).isTrue();
      final Result r = rs.next();
      return r.getProperty("x");
    }
  }
}
