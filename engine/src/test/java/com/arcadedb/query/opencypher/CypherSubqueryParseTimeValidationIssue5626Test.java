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
import com.arcadedb.query.opencypher.ast.ClauseEntry;
import com.arcadedb.query.opencypher.ast.CollectExpression;
import com.arcadedb.query.opencypher.ast.CountExpression;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.ExistsExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser;
import com.arcadedb.query.opencypher.parser.CypherExpressionWalker;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

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

  /**
   * Each branch of a {@code UNION} binds its own variables and is entered as a scope of its own, so a check that
   * reads variable kinds sees the branch it is looking at. Merging the branches into one scope instead would keep
   * only what they agree on, and a variable declared by a single branch would escape - asserted from both branch
   * positions, since a scope taken from one branch would still answer for that one.
   */
  @Test
  void aKindIsReadFromTheUnionBranchThatDeclaresIt() {
    assertThatThrownBy(() -> explain("CALL { MATCH (a:P) RETURN a.name AS x "
        + "UNION MATCH q = (m:P)-->() RETURN q.name AS x } RETURN x"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("path variable");
    assertThatThrownBy(() -> explain("CALL { MATCH q = (m:P)-->() RETURN q.name AS x "
        + "UNION MATCH (a:P) RETURN a.name AS x } RETURN x"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("path variable");
    assertThatCode(() -> explain("CALL { MATCH (a:P) RETURN a.name AS x "
        + "UNION MATCH (b:P) RETURN b.name AS x } RETURN x")).doesNotThrowAnyException();
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

  /**
   * A body that imports an enclosing variable keeps its kind, however it imports it: through the explicit scope list,
   * through a leading {@code WITH}, or renamed on the way in. What decides the kind is that the item is a plain
   * variable reference, not the clause it was written in.
   */
  @Test
  void anImportedOuterPathVariableKeepsItsKind() {
    assertThatThrownBy(() -> explain("MATCH p = (a:P)-[:KNOWS]->(b:P) CALL (p) { RETURN p.name AS n } RETURN n"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("path variable");
    assertThatThrownBy(() -> explain("MATCH p = (a:P)-[:KNOWS]->(b:P) CALL { WITH p RETURN p.name AS n } RETURN n"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("path variable");
    assertThatThrownBy(
        () -> explain("MATCH p = (a:P)-[:KNOWS]->(b:P) CALL (p) { WITH p AS q RETURN q.name AS n } RETURN n"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("path variable");
    assertThatThrownBy(() -> explain("MATCH (n:P) RETURN COLLECT { MATCH q = (m:P)-->() WITH q RETURN q.name } AS r"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("path variable");
  }

  /** A {@code WITH} that computes rather than passes through drops the kind, and a renamed one carries it. */
  @Test
  void aWithProjectionDecidesWhichKindSurvivesIt() {
    // Re-bound to a number: no longer a path, so the property read is no longer this phase's business.
    assertThatCode(() -> explain("MATCH p = (a:P)-[:KNOWS]->(b:P) CALL (p) { WITH 1 AS p RETURN p AS n } RETURN n"))
        .doesNotThrowAnyException();
    // Renamed pass-through: still a node, still a relationship.
    assertThatCode(() -> explain("MATCH (n:P) RETURN COLLECT { MATCH (m:P) WITH m AS x RETURN labels(x) } AS r"))
        .doesNotThrowAnyException();
    assertThatCode(
        () -> explain("MATCH (n:P) RETURN COLLECT { MATCH (m:P)-[r:KNOWS]->() WITH r AS e RETURN type(e) } AS r"))
        .doesNotThrowAnyException();
    // WITH * passes the whole scope through.
    assertThatCode(() -> explain("MATCH (n:P) RETURN COLLECT { MATCH (m:P) WITH *, 1 AS z RETURN labels(m) } AS r"))
        .doesNotThrowAnyException();
  }

  /**
   * An implicit {@code CALL { }} imports nothing, so its body may not reference an enclosing variable at all. The
   * kinds it inherits are therefore never consulted for such a name: the scope check runs first and reports it as the
   * undefined variable it is, rather than this phase answering about a variable the body cannot see.
   */
  @Test
  void anUnimportedReferenceIsReportedAsUndefinedNotAsAKindError() {
    assertThatThrownBy(() -> explain("MATCH p = (a:P)-[:KNOWS]->(b:P) CALL { RETURN p.name } RETURN *"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'p'");
    // The explicit empty scope list imports nothing either, and is answered the same way.
    assertThatThrownBy(() -> explain("MATCH p = (a:P)-[:KNOWS]->(b:P) CALL () { RETURN p.name AS n } RETURN n"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable")
        .hasMessageContaining("'p'");
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

  // ===================== the walker visits each expression exactly once =====================

  /**
   * The walk covers the whole clause tree from the ordered clause list alone, which is only correct because a
   * {@code WITH} registered on {@code getWithClauses()} is the very same object registered on
   * {@code getClausesInOrder()} - one builder method puts it on both. Asserted here rather than assumed, because a
   * builder path that ever registered a WITH on the statement alone would make the walk, and every check running
   * through it, silently skip that clause.
   * <p>
   * The visit count then holds the other half: no expression is reached twice, which is what lets a visitor
   * accumulate rather than only assert. It is checked on a query with a WITH in the outer statement and another in a
   * {@code CALL} body, so a double-registration would have to show up in both scopes to go unnoticed.
   */
  @Test
  void everyExpressionIsVisitedExactlyOnce() {
    final CypherStatement statement = new Cypher25AntlrParser().parse(
        "MATCH (n:P) WITH n, abs(n.age) AS a WHERE a > 0 "
            + "CALL { MATCH (m:P) WITH m WHERE m.age > 0 RETURN m.name AS b } RETURN a, b ORDER BY a");

    final List<WithClause> ordered = statement.getClausesInOrder().stream()
        .filter(entry -> entry.getType() == ClauseEntry.ClauseType.WITH)
        .map(entry -> (WithClause) entry.getTypedClause())
        .toList();
    assertThat(ordered).isNotEmpty();
    assertThat(statement.getWithClauses()).isNotEmpty();
    for (final WithClause withClause : statement.getWithClauses())
      assertThat(ordered.stream().anyMatch(entry -> entry == withClause))
          .as("a WITH on the statement must be the same instance on the ordered list, or the walk would miss it")
          .isTrue();

    final Map<Expression, Integer> visits = new IdentityHashMap<>();
    CypherExpressionWalker.walk(statement, expression -> visits.merge(expression, 1, Integer::sum));

    assertThat(visits).isNotEmpty();
    assertThat(visits.entrySet().stream().filter(entry -> entry.getValue() > 1).map(entry -> entry.getKey().getText())
        .toList()).isEmpty();
  }

  /**
   * Building the body AST puts the statement builder on the parse path of something that used to be carried as text,
   * so a body it cannot assemble must leave the query alone rather than fail it: the build is best-effort and the
   * expression keeps a null body. What that costs is coverage - the walk sees a leaf, exactly as it did before
   * #5626 - and what it must not cost is an exception, which would turn a running query into a parse error.
   * <p>
   * The three expressions are checked in the shape the fallback leaves them, and the walk is asserted to have
   * carried on past each: a null body that stopped the traversal would take the rest of the query's checks with it.
   * <p>
   * The visitor here dereferences the statement it is handed, the way the validator's does when it builds the inner
   * scope. That is the assertion that matters: the guarantee is not "the walk survives a null body" but "a boundary
   * hook is never called with one", and only a visitor that reads the statement can tell the two apart.
   */
  @Test
  void aSubqueryExpressionWithNoBodyIsALeafRatherThanAFailure() {
    final List<Expression> withoutBody = List.of(
        new ExistsExpression("MATCH (m:P) RETURN m", "EXISTS { MATCH (m:P) RETURN m }", null),
        new CountExpression("MATCH (m:P) RETURN m", "COUNT { MATCH (m:P) RETURN m }", null),
        new CollectExpression("MATCH (m:P) RETURN m.name", "COLLECT { MATCH (m:P) RETURN m.name }", null));

    for (final Expression subquery : withoutBody) {
      final List<Expression> visited = new ArrayList<>();
      final Expression sibling = new VariableExpression("afterTheSubquery");
      final Expression enclosing = new ListExpression(List.of(subquery, sibling), "[subquery, afterTheSubquery]");

      final CypherExpressionWalker.Visitor readsTheNestedStatement = new CypherExpressionWalker.Visitor() {
        @Override
        public void visit(final Expression expression) {
          visited.add(expression);
        }

        @Override
        public CypherExpressionWalker.Visitor forNestedStatement(final CypherStatement nested) {
          nested.getClausesInOrder();
          return this;
        }
      };

      assertThatCode(() -> CypherExpressionWalker.walk(enclosing, readsTheNestedStatement)).doesNotThrowAnyException();

      assertThat(visited).contains(subquery);
      assertThat(visited)
          .as("the walk must carry on past a body-less subquery, not stop at it")
          .contains(sibling);
    }
  }

  /**
   * Kind propagation is one construction now, shared by the statement and by a subquery body, and folding them
   * together closed an asymmetry that predated this issue: the statement's copy dropped every kind at a {@code
   * WITH *} because it kept only what the projection names, while the body's copy passed the incoming scope
   * through. So the same path access was rejected inside a body and accepted outside one - the shape #5602 and
   * #5626 exist to remove, in the phase that decides what a name is.
   */
  @Test
  void aKindSurvivesWithStarInsideAndOutsideASubqueryBody() {
    assertThatThrownBy(() -> explain("MATCH p = (a:P)-[:KNOWS]->(b:P) WITH * RETURN p.name"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("path variable");
    assertThatThrownBy(() -> explain(
        "MATCH (n:P) RETURN COLLECT { MATCH p = (a:P)-[:KNOWS]->(b:P) WITH * RETURN p.name } AS r"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("path variable");
    // A projection that names what it keeps still drops the rest, inside and outside alike.
    assertThatCode(() -> explain("MATCH p = (a:P)-[:KNOWS]->(b:P) WITH 1 AS p RETURN p")).doesNotThrowAnyException();
  }

  /**
   * A name declared twice as two different kinds is a {@code VariableTypeConflict}, and it is one inside a body for
   * the same reason it is one outside: that is what "validate a body like the query around it" means.
   * <p>
   * What must still be allowed is the other thing that looks like it. A body that binds a name the enclosing query
   * also uses is shadowing, not clashing - an implicit {@code CALL { }} imports nothing, so the outer name is not
   * one the body is redefining. Both are asserted, because a check that told them apart in only one direction would
   * either miss the conflict or reject the shadow.
   */
  @Test
  void aKindClashInsideABodyIsAConflictWhileShadowingAnOuterNameIsNot() {
    assertThatThrownBy(() -> explain("MATCH (x:P) MATCH ()-[x]->() RETURN x"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("VariableTypeConflict");
    assertThatThrownBy(
        () -> explain("MATCH (n:P) WHERE EXISTS { MATCH (x:P) MATCH ()-[x]->() RETURN x } RETURN n"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("VariableTypeConflict");
    assertThatThrownBy(
        () -> explain("MATCH (n:P) CALL { MATCH (x:P) MATCH ()-[x]->() RETURN x AS r } RETURN r"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("VariableTypeConflict");

    // Shadowing: the body binds a name the outer query declared as another kind, and imports nothing.
    assertThatCode(() -> explain("MATCH p = (a:P)-[:KNOWS]->(b:P) CALL { MATCH (p:P) RETURN p.name AS n } RETURN n"))
        .doesNotThrowAnyException();
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
