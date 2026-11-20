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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

/**
 * Issue #11888: a pattern expression in a WHERE that is not used as a boolean (inside {@code size()}, {@code toString()} or
 * {@code coalesce()}, as an operand of a comparison, IS NULL, IN or arithmetic) evaluated without error to a wrong answer:
 * {@code size((p)-->())} was null for every node, so {@code WHERE size((p)-->()) > 1} matched nothing. Like Neo4j 5, and
 * like a RETURN projection already does, the statement is now refused. A pattern expression where a boolean is expected
 * keeps answering.
 * <p>
 * Graph: a -> b, a -> c, b -> c, so a has two outgoing relationships, b one and c none.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherPatternExpressionNonBooleanWhereIssue11888Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:P {n: 'a'}), (b:P {n: 'b'}), (c:P {n: 'c'}), (a)-[:K]->(b), (a)-[:K]->(c), (b)-[:K]->(c)"));
  }

  @Test
  void sizeOfPatternIsRefusedNamingCount() {
    for (final String statement : List.of(//
        "MATCH (p:P) WHERE size((p)-->()) > 1 RETURN p.n AS n",//
        "MATCH (p:P) WHERE size((p)-->()) IS NULL RETURN p.n AS n",//
        "MATCH (p:P) WHERE size((p)-[:K]->()) >= 0 RETURN p.n AS n",//
        "MATCH (p:P) WHERE size((p)-->()) RETURN p.n AS n",//
        "MATCH (p:P) WHERE SIZE( ( p )-[:K]->() ) + 0 > 1 RETURN p.n AS n",//
        "MATCH (p:P) WHERE p.n <> 'x' AND size((p)-->()) > 1 RETURN p.n AS n"))
      assertThatThrownBy(() -> rows(statement)).as(statement).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("inside the function size()").hasMessageContaining("COUNT {}");
  }

  @Test
  void patternOutsideABooleanPositionIsRefused() {
    for (final String statement : List.of(//
        "MATCH (p:P) WHERE (p)-->() = true RETURN p.n AS n",//
        "MATCH (p:P) WHERE ((p)-->()) = true RETURN p.n AS n",//
        "MATCH (p:P) WHERE (p)-->() <> (p)<--() RETURN p.n AS n",//
        "MATCH (p:P) WHERE (p)-->() IS NULL RETURN p.n AS n",//
        "MATCH (p:P) WHERE (p)-->() IS NOT NULL RETURN p.n AS n",//
        "MATCH (p:P) WHERE (p)-->() IN [true] RETURN p.n AS n",//
        "MATCH (p:P) WHERE toString((p)-->()) = 'x' RETURN p.n AS n",//
        "MATCH (p:P) WHERE length((p)-->()) > 0 RETURN p.n AS n",//
        "MATCH (p:P) WHERE coalesce((p)-->(), false) RETURN p.n AS n",//
        "MATCH (p:P) WHERE [(p)-->()] <> [] RETURN p.n AS n",//
        "MATCH (p:P) WHERE (p)-->().n = 'x' RETURN p.n AS n",//
        "MATCH (p:P) WHERE -(p)-->() RETURN p.n AS n",//
        "MATCH (p:P) WHERE any(x IN [p] WHERE toString((x)-->()) = 'x') RETURN p.n AS n"))
      assertThatThrownBy(() -> rows(statement)).as(statement).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("contexts that evaluate to a boolean");
  }

  /**
   * The walk that tells a boolean position from the rest passes through one level of the expression grammar at a time, and
   * a level it passes through by mistake would let a pattern in as an operand. One operator per level of the grammar that
   * holds an operand: additive, multiplicative, power, string and list comparison.
   */
  @Test
  void patternAsAnOperandOfEveryOperatorLevelIsRefused() {
    for (final String statement : List.of(//
        "MATCH (p:P) WHERE (p)-->() + 1 > 0 RETURN p.n AS n",//
        "MATCH (p:P) WHERE 1 - (p)-->() > 0 RETURN p.n AS n",//
        "MATCH (p:P) WHERE 'x' || (p)-->() = 'x' RETURN p.n AS n",//
        "MATCH (p:P) WHERE (p)-->() * 2 > 0 RETURN p.n AS n",//
        "MATCH (p:P) WHERE 2 / (p)-->() > 0 RETURN p.n AS n",//
        "MATCH (p:P) WHERE (p)-->() % 2 > 0 RETURN p.n AS n",//
        "MATCH (p:P) WHERE (p)-->() ^ 2 > 0 RETURN p.n AS n",//
        "MATCH (p:P) WHERE (p)-->() STARTS WITH 'x' RETURN p.n AS n",//
        "MATCH (p:P) WHERE 'x' CONTAINS (p)-->() RETURN p.n AS n",//
        "MATCH (p:P) WHERE 'x' = (p)-->() RETURN p.n AS n"))
      assertThatThrownBy(() -> rows(statement)).as(statement).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("contexts that evaluate to a boolean");
  }

  @Test
  void everyPredicatePositionIsChecked() {
    for (final String statement : List.of(//
        "MATCH (p:P) WITH p WHERE size((p)-->()) > 1 RETURN p.n AS n",//
        "MATCH (p:P) OPTIONAL MATCH (p)-->(q) WHERE size((q)-->()) > 0 RETURN q.n AS n",//
        "MATCH (p:P WHERE size((p)-->()) > 1) RETURN p.n AS n",//
        "MATCH (p:P)-[r:K WHERE size((p)-->()) > 1]->(q) RETURN p.n AS n",//
        "MATCH ((a:P)-[:K]->(b:P) WHERE size((b)-->()) > 0) RETURN a.n AS n",//
        "MATCH (p:P) FILTER size((p)-->()) > 1 RETURN p.n AS n",//
        "MATCH (p:P) WHERE EXISTS { MATCH (p)-->(q) WHERE size((q)-->()) > 0 } RETURN p.n AS n"))
      assertThatThrownBy(() -> rows(statement)).as(statement).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("COUNT {}");
  }

  /**
   * A subquery body whose AST build failed fell back to its text, which deferred the refusal to the first row; a subquery
   * the boolean simplifier then dropped never reported it. The inline WHERE of a pattern that the expression builder
   * parses (the argument of exists() in a RETURN) was not checked at all.
   */
  @Test
  void subqueryBodiesAndExpressionPatternsAreChecked() {
    assertSoftly(softly -> {
      for (final String statement : List.of(//
          "MATCH (p:P) WHERE true OR EXISTS { MATCH (q:P) WHERE size((q)-->()) > 0 } RETURN p.n AS n",//
          "MATCH (p:P) WHERE true OR COUNT { MATCH (q:P) WHERE size((q)-->()) > 0 RETURN q } > 0 RETURN p.n AS n",//
          "MATCH (p:P) RETURN exists((p)-[:K WHERE size((p)-->()) > 0]->()) AS n",//
          "MATCH (p:P) RETURN exists((p)-->(q WHERE size((q)-->()) > 0)) AS n"))
        softly.assertThatThrownBy(() -> rows(statement)).as(statement).isInstanceOf(CommandParsingException.class)
            .hasMessageContaining("COUNT {}");
    });
  }

  /** The operand of NOT, AND, OR or XOR is a boolean position, wherever the result of the operator goes. */
  @Test
  void operandOfABooleanOperatorAnswersInsideAComparison() {
    assertThat(rows("MATCH (p:P) WHERE (NOT (p)-->()) = true RETURN p.n AS n")).containsExactly("c");
    assertThat(rows("MATCH (p:P) WHERE ((p)-->() AND (p)<--()) = true RETURN p.n AS n")).containsExactly("b");
    assertThat(rows("MATCH (p:P) WHERE ((p)<--() XOR p.n = 'c') = true RETURN p.n AS n")).containsExactly("b");
    assertThat(rows("MATCH (p:P) WHERE ((p)-->() OR p.n = 'c') IS NOT NULL RETURN p.n AS n")).containsExactly("a", "b",
        "c");
  }

  @Test
  void patternAsABooleanKeepsAnswering() {
    assertThat(rows("MATCH (p:P) WHERE (p)-->() RETURN p.n AS n")).containsExactly("a", "b");
    assertThat(rows("MATCH (p:P) WHERE ((p)-->()) RETURN p.n AS n")).containsExactly("a", "b");
    assertThat(rows("MATCH (p:P) WHERE NOT (p)-->() RETURN p.n AS n")).containsExactly("c");
    assertThat(rows("MATCH (p:P) WHERE (p)-->() AND (p)<--() RETURN p.n AS n")).containsExactly("b");
    assertThat(rows("MATCH (p:P) WHERE (p)-[:K]->(:P {n: 'b'}) OR p.n = 'c' RETURN p.n AS n")).containsExactly("a", "c");
    assertThat(rows("MATCH (p:P) WHERE (p)<--() XOR p.n = 'c' RETURN p.n AS n")).containsExactly("b");
    assertThat(rows("MATCH (p:P) WHERE exists((p)-->()) RETURN p.n AS n")).containsExactly("a", "b");
    assertThat(rows("MATCH (p:P) WHERE NOT exists((p)-->()) RETURN p.n AS n")).containsExactly("c");
    assertThat(rows("MATCH (p:P) WHERE exists((p)-->()) = true RETURN p.n AS n")).containsExactly("a", "b");
    assertThat(rows("MATCH (p:P) WITH p WHERE (p)-->() RETURN p.n AS n")).containsExactly("a", "b");
    assertThat(rows("MATCH (p:P WHERE (p)-->()) RETURN p.n AS n")).containsExactly("a", "b");
    assertThat(rows("MATCH (p:P) FILTER (p)-->() RETURN p.n AS n")).containsExactly("a", "b");
    assertThat(rows("MATCH ((a:P)-[:K]->(b:P) WHERE (b)-->()) RETURN a.n AS n")).containsExactly("a");
  }

  @Test
  void countAndComprehensionsStayTheWayToCount() {
    assertThat(rows("MATCH (p:P) WHERE COUNT { (p)-->() } > 1 RETURN p.n AS n")).containsExactly("a");
    assertThat(rows("MATCH (p:P) WHERE size([(p)-->(q) | q]) > 1 RETURN p.n AS n")).containsExactly("a");
    assertThat(rows("MATCH (p:P) WHERE size([(p)-->(q) WHERE (q)-->() | q]) > 0 RETURN p.n AS n")).containsExactly("a");
    assertThat(rows("MATCH (p:P) WHERE EXISTS { MATCH (p)-->(q) WHERE (q)-->() } RETURN p.n AS n")).containsExactly("a");
  }

  /**
   * A pattern expression nested in a boolean position inside a larger expression (a CASE WHEN condition, the WHERE of a
   * list predicate or of a list comprehension) was taken for the whole predicate, or failed: the WHERE parser returned the
   * first pattern it found in the operand.
   */
  @Test
  void nestedBooleanPositionsAnswer() {
    assertThat(rows("MATCH (p:P) WHERE CASE WHEN (p)-->() THEN false ELSE true END RETURN p.n AS n")).containsExactly("c");
    assertThat(rows("MATCH (p:P) WHERE CASE WHEN (p)-->() THEN true ELSE false END RETURN p.n AS n")).containsExactly("a",
        "b");
    assertThat(rows("MATCH (p:P) WHERE any(x IN [p] WHERE (x)-->()) RETURN p.n AS n")).containsExactly("a", "b");
    assertThat(rows("MATCH (p:P) WHERE any(x IN [p] WHERE NOT (x)-->()) RETURN p.n AS n")).containsExactly("c");
    assertThat(rows("MATCH (p:P) WHERE none(x IN [p] WHERE (x)-->()) RETURN p.n AS n")).containsExactly("c");
    assertThat(rows("MATCH (p:P) WHERE size([x IN [p] WHERE (x)<--()]) > 0 RETURN p.n AS n")).containsExactly("b", "c");
    assertThat(rows("MATCH (p:P) WHERE p.n IN [x IN [p] WHERE (x)-->() | x.n] RETURN p.n AS n")).containsExactly("a", "b");
  }

  /**
   * A list predicate (any, all, none, single) nested in a larger expression of a WHERE, such as a CASE, was taken for the
   * whole predicate: the WHERE parser returned the first one it found in the operand and dropped the CASE around it. With
   * a pattern in its own WHERE the same statement used to fail on the unbound variable, so refusing the pattern outside a
   * boolean position was not enough to make it answer.
   */
  @Test
  void listPredicateInsideACaseKeepsTheCase() {
    assertThat(rows("MATCH (p:P) WHERE CASE WHEN false THEN any(x IN [p] WHERE x.n = 'a') ELSE false END RETURN p.n AS n"))
        .isEmpty();
    assertThat(rows("MATCH (p:P) WHERE CASE WHEN true THEN false ELSE any(x IN [p] WHERE x.n = 'a') END RETURN p.n AS n"))
        .isEmpty();
    assertThat(rows("MATCH (p:P) WHERE CASE WHEN any(x IN [p] WHERE x.n = 'a') THEN false ELSE true END RETURN p.n AS n"))
        .containsExactly("b", "c");
    assertThat(rows("MATCH (p:P) WHERE CASE WHEN false THEN all(x IN [p] WHERE x.n = 'a') ELSE false END RETURN p.n AS n"))
        .isEmpty();
    assertThat(rows("MATCH (p:P) WHERE CASE WHEN false THEN any(x IN [p] WHERE (x)-->()) ELSE false END RETURN p.n AS n"))
        .isEmpty();
    assertThat(rows("MATCH (p:P) WHERE CASE WHEN true THEN any(x IN [p] WHERE (x)-->()) ELSE false END RETURN p.n AS n"))
        .containsExactly("a", "b");
    assertThat(rows("MATCH (p:P) WHERE CASE WHEN p.n = 'c' THEN true ELSE any(x IN [p] WHERE (x)-->()) END RETURN p.n AS n"))
        .containsExactly("a", "b", "c");
  }

  /** A list predicate that is the whole operand keeps answering, bare, in parentheses and under a boolean operator. */
  @Test
  void listPredicateAsABooleanKeepsAnswering() {
    assertThat(rows("MATCH (p:P) WHERE any(x IN [p] WHERE (x)-->()) RETURN p.n AS n")).containsExactly("a", "b");
    assertThat(rows("MATCH (p:P) WHERE (any(x IN [p] WHERE (x)-->())) RETURN p.n AS n")).containsExactly("a", "b");
    assertThat(rows("MATCH (p:P) WHERE any(x IN [p] WHERE (x)-->()) AND p.n <> 'a' RETURN p.n AS n")).containsExactly("b");
    assertThat(rows("MATCH (p:P) WHERE NOT any(x IN [p] WHERE x.n = 'a') RETURN p.n AS n")).containsExactly("b", "c");
    assertThat(rows("MATCH (p:P) WHERE single(x IN [p.n, 'z'] WHERE x = 'a') RETURN p.n AS n")).containsExactly("a");
  }

  private List<Object> rows(final String statement) {
    try (final ResultSet rs = database.query("opencypher", statement + " ORDER BY n")) {
      return rs.stream().map(r -> r.getProperty("n")).toList();
    }
  }
}
