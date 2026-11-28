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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #13616 (grouped under #13665): a pattern predicate that is an operand of NOT, AND, OR or XOR,
 * or a CASE WHEN condition, is a boolean and is accepted in a RETURN or WITH projection, as Neo4j 5.26 does (LDBC SNB
 * Interactive IC7 is written with {@code not((liker)-[:KNOWS]-(person)) AS isNew}). It was refused with "Pattern expressions
 * are not allowed in RETURN projections". The positions Neo4j refuses (a bare pattern, an operand of a comparison, of IS NULL,
 * an argument of coalesce()) keep their error.
 * <p>
 * The graph holds one {@code (:A)-[:R]->(:B {k: 2})} pair and a second {@code (:B {k: 3})} with no relationship, so every
 * expression is read on a row where the pattern exists and on one where it does not.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class CypherPatternPredicateBooleanOperandIssue13616Test extends TestHelper {
  private static final String HEAD = "MATCH (a:A), (b:B) ";

  @BeforeEach
  void createGraph() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {k: 1})-[:R]->(:B {k: 2}), (:B {k: 3})").close());
  }

  private List<Object> column(final String query, final String column) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(column));
    }
    return values;
  }

  private List<Object> returned(final String expression) {
    return column(HEAD + "RETURN b.k AS k, " + expression + " AS x ORDER BY k", "x");
  }

  @Test
  void notOfAPatternInReturn() {
    assertThat(returned("NOT ((a)-[:R]-(b))")).containsExactly(false, true);
    assertThat(returned("NOT (a)-[:R]-(b)")).containsExactly(false, true);
    assertThat(returned("not((a)-[:R]-(b))")).containsExactly(false, true);
    assertThat(returned("NOT NOT (a)-[:R]-(b)")).containsExactly(true, false);
  }

  @Test
  void andOfAPatternInReturn() {
    assertThat(returned("(a)-[:R]-(b) AND true")).containsExactly(true, false);
    assertThat(returned("true AND (a)-[:R]-(b)")).containsExactly(true, false);
    assertThat(returned("(a)-[:R]-(b) AND b.k = 3")).containsExactly(false, false);
  }

  @Test
  void orAndXorOfAPatternInReturn() {
    assertThat(returned("(a)-[:R]-(b) OR false")).containsExactly(true, false);
    assertThat(returned("(a)-[:R]-(b) OR b.k = 3")).containsExactly(true, true);
    assertThat(returned("(a)-[:R]-(b) XOR false")).containsExactly(true, false);
    assertThat(returned("(a)-[:R]-(b) XOR true")).containsExactly(false, true);
  }

  @Test
  void twoPatternsUnderOneOperatorInReturn() {
    assertThat(returned("(a)-[:R]->(b) AND NOT (a)<-[:R]-(b)")).containsExactly(true, false);
    assertThat(returned("(a)<-[:R]-(b) OR (a)-[:R]->(b)")).containsExactly(true, false);
  }

  @Test
  void caseWhenOfAPatternInReturn() {
    assertThat(returned("CASE WHEN (a)-[:R]-(b) THEN 1 ELSE 0 END")).containsExactly(1L, 0L);
    assertThat(returned("CASE WHEN NOT (a)-[:R]-(b) THEN 'new' ELSE 'known' END")).containsExactly("known", "new");
  }

  @Test
  void theOperatorMayBeNestedInALargerExpression() {
    assertThat(returned("[NOT (a)-[:R]-(b), b.k]")).containsExactly(List.of(false, 2), List.of(true, 3));
    assertThat(returned("toString(NOT (a)-[:R]-(b))")).containsExactly("false", "true");
    assertThat(returned("(NOT (a)-[:R]-(b)) = true")).containsExactly(false, true);
  }

  @Test
  void theWhereOfAComprehensionOrOfAListPredicateIsABooleanPositionToo() {
    assertThat(returned("[x IN [1] WHERE (a)-[:R]-(b) | x]")).containsExactly(List.of(1L), List.of());
    assertThat(returned("any(x IN [1] WHERE (a)-[:R]-(b))")).containsExactly(true, false);
  }

  @Test
  void existsOfAPatternInReturnIsUnchanged() {
    assertThat(returned("exists((a)-[:R]-(b))")).containsExactly(true, false);
    assertThat(returned("NOT exists((a)-[:R]-(b))")).containsExactly(false, true);
  }

  @Test
  void notAndCaseWhenOfAPatternInWith() {
    assertThat(column(HEAD + "WITH a, b, NOT ((a)-[:R]-(b)) AS isNew RETURN isNew ORDER BY b.k", "isNew")).containsExactly(false,
        true);
    assertThat(
        column(HEAD + "WITH a, b, CASE WHEN (a)-[:R]-(b) THEN 'yes' ELSE 'no' END AS c RETURN c ORDER BY b.k", "c")).containsExactly(
        "yes", "no");
    assertThat(column(HEAD + "WITH b, (a)-[:R]-(b) OR false AS known WHERE known RETURN b.k AS k", "k")).containsExactly(2);
  }

  @Test
  void theFlagOfAWithIsReadByALaterAggregation() {
    // the shape of LDBC SNB Interactive IC7 / IC10: a pattern flag computed per row, then aggregated
    assertThat(column(HEAD + "WITH b, NOT ((a)-[:R]-(b)) AS isNew RETURN isNew, count(b) AS c ORDER BY isNew", "c")).containsExactly(
        1L, 1L);
  }

  @Test
  void theSameCheckGuardsLetAndSetValues() {
    // LET and SET share the check of RETURN and WITH, so they accept and refuse the same positions
    assertThat(column(HEAD + "LET isNew = NOT (a)-[:R]-(b) RETURN isNew ORDER BY b.k", "isNew")).containsExactly(false, true);
    database.transaction(() -> database.command("opencypher", HEAD + "SET b.isNew = NOT (a)-[:R]-(b)").close());
    assertThat(column("MATCH (b:B) RETURN b.isNew AS isNew ORDER BY b.k", "isNew")).containsExactly(false, true);

    assertThatThrownBy(() -> database.query("opencypher", HEAD + "LET x = (a)-[:R]-(b) = true RETURN x").close()).isInstanceOf(
        CommandParsingException.class).hasMessageContaining("Pattern expressions are not allowed in LET");
    assertThatThrownBy(() -> database.command("opencypher", HEAD + "SET b.isNew = (a)-[:R]-(b) IS NULL").close()).isInstanceOf(
        CommandParsingException.class).hasMessageContaining("Pattern expressions are not allowed in SET values");
  }

  @Test
  void aBarePatternInReturnIsStillRefused() {
    for (final String expression : List.of("(a)-[:R]-(b)", "((a)-[:R]-(b))", "(a)-[:R]-(b) = true", "(a)-[:R]-(b) IS NULL",
        "coalesce((a)-[:R]-(b), false)", "[(a)-[:R]-(b)]", "size((a)-[:R]-(b))", "(a)-[:R]-(b) IN [true]",
        "CASE WHEN true THEN (a)-[:R]-(b) ELSE false END", "CASE (a)-[:R]-(b) WHEN true THEN 1 ELSE 0 END"))
      assertThatThrownBy(() -> database.query("opencypher", HEAD + "RETURN " + expression + " AS x").close()).as(expression)
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("Pattern expressions are not allowed in RETURN projections");
  }

  @Test
  void aBarePatternBesideABooleanOneIsStillRefused() {
    assertThatThrownBy(
        () -> database.query("opencypher", HEAD + "RETURN [NOT (a)-[:R]-(b), (a)-[:R]-(b)] AS x").close()).isInstanceOf(
        CommandParsingException.class).hasMessageContaining("Pattern expressions are not allowed in RETURN projections");
  }

  @Test
  void aBarePatternInWithIsStillRefused() {
    for (final String expression : List.of("(a)-[:R]-(b)", "(a)-[:R]-(b) = true", "(a)-[:R]-(b) IS NULL",
        "coalesce((a)-[:R]-(b), false)"))
      assertThatThrownBy(
          () -> database.query("opencypher", HEAD + "WITH a, b, " + expression + " AS x RETURN x").close()).as(expression)
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("Pattern expressions are not allowed in WITH projections");
  }
}
