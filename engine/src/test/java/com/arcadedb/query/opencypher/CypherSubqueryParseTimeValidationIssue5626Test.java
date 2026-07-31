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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5626: the body of a subquery escaped the parse-time argument validation that
 * issue #5602 had just extended to every other clause.
 *
 * <p>{@code abs('x')} is rejected before the query runs when written in a {@code WHERE}, and was let through when
 * written inside {@code EXISTS { }}, {@code COUNT { }}, {@code COLLECT { }} or a {@code CALL { }} body. The runtime
 * check still caught it whenever the subquery actually executed, but a subquery over a pattern matching no row never
 * runs it - and the three expression forms absorb a failure into their neutral value, so the mistake could surface as
 * a plain {@code false} / {@code 0} / {@code []} instead of an error. Neo4j type-checks a subquery body exactly as it
 * does the enclosing query.
 *
 * <p>The queries here are run under {@code EXPLAIN}, so they are parsed and planned but never executed: what they
 * assert is that the failure happens before any row is touched.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherSubqueryParseTimeValidationIssue5626Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher",
        "CREATE (a:P {name: 'a', age: 30})-[:KNOWS {since: 2020}]->(b:P {name: 'b', age: 40})");
  }

  // ===================== the reproducer, one shape per subquery form =====================

  @Test
  void existsSubqueryBodyIsValidated() {
    assertRejectedLikeTheOuterWhere("MATCH (n:P) WHERE EXISTS { MATCH (m:P) WHERE abs('x') > 0 RETURN m } RETURN n");
  }

  @Test
  void countSubqueryBodyIsValidated() {
    assertRejectedLikeTheOuterWhere("MATCH (n:P) RETURN COUNT { MATCH (m:P) WHERE abs('x') > 0 RETURN m } AS r");
  }

  @Test
  void collectSubqueryBodyIsValidated() {
    assertRejectedLikeTheOuterWhere("MATCH (n:P) RETURN COLLECT { MATCH (m:P) WHERE abs('x') > 0 RETURN m } AS r");
  }

  @Test
  void callSubqueryBodyIsValidated() {
    assertRejectedLikeTheOuterWhere("MATCH (n:P) CALL { MATCH (m:P) WHERE abs('x') > 0 RETURN m } RETURN n");
  }

  /**
   * The asymmetry the issue reports: the same call is rejected in the outer WHERE and must be rejected in the body
   * too, with the same class and the same message.
   */
  private void assertRejectedLikeTheOuterWhere(final String queryWithSubquery) {
    assertThatThrownBy(() -> explain("MATCH (n:P) WHERE abs('x') > 0 RETURN n"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> explain(queryWithSubquery))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()")
        .hasMessageContaining("STRING");
  }

  // ===================== every check of the phase reaches inside, not only the type one =====================

  /**
   * The predicate here is a bare function call rather than a comparison, which used to get an anonymous
   * {@code BooleanExpression} adapter of its own - a leaf as far as the walk was concerned - so the call inside it
   * escaped the checks whether it was written in a subquery body or in the enclosing {@code WHERE}.
   */
  @Test
  void unknownFunctionAsABarePredicateIsRejectedInsideAndOutside() {
    assertThatThrownBy(() -> explain("MATCH (n:P) WHERE nosuchfn(n) RETURN n"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("nosuchfn");
    assertThatThrownBy(() -> explain("MATCH (n:P) WHERE EXISTS { MATCH (m:P) WHERE nosuchfn(m) RETURN m } RETURN n"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("nosuchfn");
  }

  @Test
  void wrongArgumentCountInsideASubqueryBodyIsRejected() {
    assertThatThrownBy(() -> explain("MATCH (n:P) RETURN COUNT { MATCH (m:P) WHERE atan2(1) > 0 RETURN m } AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("atan2");
  }

  @Test
  void propertyAccessOnAPathVariableInsideASubqueryBodyIsRejected() {
    assertThatThrownBy(() -> explain("MATCH (n:P) RETURN COLLECT { MATCH q = (m:P)-->() RETURN q.name } AS r"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("path variable");
  }

  // ===================== the positions a body can be written in =====================

  @Test
  void barePatternExistsBodyIsValidated() {
    assertThatThrownBy(() -> explain("MATCH (n:P) WHERE EXISTS { (n)-[:KNOWS]->(m) WHERE abs('x') > 0 } RETURN n"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()");
  }

  @Test
  void barePatternCountBodyIsValidated() {
    assertThatThrownBy(() -> explain("MATCH (n:P) RETURN COUNT { (n)-[:KNOWS]->(m) WHERE abs('x') > 0 } AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()");
  }

  @Test
  void negatedExistsBodyIsValidated() {
    assertThatThrownBy(
        () -> explain("MATCH (n:P) WHERE NOT EXISTS { MATCH (m:P) WHERE abs('x') > 0 RETURN m } RETURN n"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()");
  }

  @Test
  void existsBodyInsideAConjunctionIsValidated() {
    assertThatThrownBy(() -> explain(
        "MATCH (n:P) WHERE n.age > 1 AND EXISTS { MATCH (m:P) WHERE abs('x') > 0 RETURN m } RETURN n"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()");
  }

  @Test
  void subqueryNestedInsideAnotherSubqueryIsValidated() {
    assertThatThrownBy(() -> explain("MATCH (n:P) WHERE EXISTS { MATCH (m:P) "
        + "WHERE COUNT { MATCH (o:P) WHERE abs('x') > 0 RETURN o } > 0 RETURN m } RETURN n"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()");
  }

  @Test
  void unionBranchOfACallBodyIsValidated() {
    assertThatThrownBy(() -> explain("CALL { MATCH (m:P) RETURN m.name AS x "
        + "UNION MATCH (o:P) WHERE abs('x') > 0 RETURN o.name AS x } RETURN x"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()");
  }

  @Test
  void subqueryBodyOfAWithWhereIsValidated() {
    assertThatThrownBy(() -> explain("MATCH (n:P) WITH n WHERE EXISTS { MATCH (m:P) WHERE abs('x') > 0 RETURN m } "
        + "RETURN n"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()");
  }

  // ===================== two more expression positions the same walk now reaches =====================

  @Test
  void procedureCallArgumentIsValidated() {
    assertThatThrownBy(() -> explain("CALL algo.degree(abs('x')) YIELD node RETURN node"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()");
  }

  @Test
  void loadCsvUrlExpressionIsValidated() {
    assertThatThrownBy(() -> explain("LOAD CSV FROM abs('x') AS row RETURN row"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("abs()");
  }

  // ===================== nothing valid is newly rejected =====================

  @Test
  void aCorrelatedSubqueryBodyStillParses() {
    assertThatCode(() -> explain("MATCH (n:P) WHERE EXISTS { MATCH (n)-[:KNOWS]->(m:P) WHERE m.age > n.age RETURN m } "
        + "RETURN n")).doesNotThrowAnyException();
  }

  /**
   * {@code type()} wants a relationship and {@code labels()} a node: both are answered from the kinds the body itself
   * declares, not from the enclosing statement's.
   */
  @Test
  void variableKindsAreReadFromTheBodyThatDeclaresThem() {
    assertThatCode(() -> explain("MATCH (n:P) RETURN COLLECT { MATCH (a:P)-[r:KNOWS]->(b:P) RETURN type(r) } AS r"))
        .doesNotThrowAnyException();
    assertThatCode(() -> explain("MATCH (n:P) RETURN COLLECT { MATCH (a:P)-[r:KNOWS]->(b:P) RETURN labels(a) } AS r"))
        .doesNotThrowAnyException();
  }

  /**
   * An implicit {@code CALL { }} imports nothing, so a body is free to re-use an enclosing name for something of its
   * own kind. The enclosing kind must not decide a check about the body's variable: here {@code p} is a path outside
   * and a node inside, and {@code p.name} inside is a plain property read.
   */
  @Test
  void anUnimportedOuterNameDoesNotDecideTheBodysCheck() {
    assertThatCode(() -> explain("MATCH p = (a:P)-[:KNOWS]->(b:P) CALL { MATCH (p:P) RETURN p.name AS n } "
        + "RETURN n")).doesNotThrowAnyException();
  }

  /** The enclosing kind still applies to a name the body does not re-declare. */
  @Test
  void anOuterPathVariableIsStillAPathInsideACorrelatedBody() {
    assertThatThrownBy(() -> explain("MATCH p = (a:P)-[:KNOWS]->(b:P) "
        + "RETURN COLLECT { MATCH (m:P) RETURN p.name } AS r"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("path variable");
  }

  // ===================== EXISTS in a WHERE still evaluates as before =====================

  @Test
  void existsInWhereStillFiltersCorrectly() {
    assertThat(names("MATCH (n:P) WHERE EXISTS { MATCH (n)-[:KNOWS]->(:P) } RETURN n.name AS name ORDER BY name"))
        .containsExactly("a");
    assertThat(names("MATCH (n:P) WHERE NOT EXISTS { MATCH (n)-[:KNOWS]->(:P) } RETURN n.name AS name ORDER BY name"))
        .containsExactly("b");
    assertThat(names("MATCH (n:P) WHERE EXISTS { MATCH (m:P) WHERE m.age > 100 RETURN m } RETURN n.name AS name"))
        .isEmpty();
  }

  @Test
  void countAndCollectSubqueriesStillEvaluate() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:P {name: 'a'}) RETURN COUNT { (n)-[:KNOWS]->(:P) } AS c, "
            + "COLLECT { MATCH (n)-[:KNOWS]->(m:P) RETURN m.name } AS names")) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(1L);
      assertThat((List<Object>) row.getProperty("names")).containsExactly("b");
    }
  }

  private List<String> names(final String query) {
    final List<String> result = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        result.add(rs.next().getProperty("name"));
    }
    return result;
  }

  private void explain(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
