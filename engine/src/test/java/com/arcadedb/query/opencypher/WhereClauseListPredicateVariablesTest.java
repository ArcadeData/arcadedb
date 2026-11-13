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

import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.WhereClause;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import java.util.List;
import org.junit.jupiter.api.Nested;

/**
 * Regression tests for {@link WhereClause#collectVariables} with list predicates
 * (any/all/none/single) and list comprehensions.
 * <p>
 * These guard the fix from PR #3891: outer variables referenced inside list-predicate
 * and list-comprehension bodies must be collected, while the loop-scoped iterator
 * variable must not. The filter-pushdown machinery in the optimizer relies on this
 * to decide which predicates depend on which variables.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class WhereClauseListPredicateVariablesTest {
  private static final Cypher25AntlrParser PARSER = new Cypher25AntlrParser();

  @Test
  void anyPredicateInComparisonCollectsOuterVariable() {
    // any(...) wrapped in an explicit comparison reaches the ListPredicateExpression
    // branch of collectExpressionVariables: the outer variable 'p' must be collected.
    assertThat(collect("MATCH (p:Person) WHERE any(x IN ['Alice'] WHERE x = p.name) = true RETURN p"))
        .containsExactly("p");
  }

  @Test
  void allPredicateInComparisonCollectsOuterVariable() {
    assertThat(collect("MATCH (p:Person) WHERE all(x IN ['Alice','Bob'] WHERE x <> p.name) = true RETURN p"))
        .containsExactly("p");
  }

  @Test
  void nonePredicateInComparisonCollectsOuterVariable() {
    assertThat(collect("MATCH (p:Person) WHERE none(x IN ['Alice'] WHERE x = p.name) = true RETURN p"))
        .containsExactly("p");
  }

  @Test
  void singlePredicateInComparisonCollectsOuterVariable() {
    assertThat(collect("MATCH (p:Person) WHERE single(x IN ['Alice'] WHERE x = p.name) = true RETURN p"))
        .containsExactly("p");
  }

  @Test
  void listComprehensionExcludesLoopIterator() {
    // The iterator 'x' must not leak into the collected variable set; only the
    // outer 'p' should appear. This also covers the ListComprehensionExpression
    // branch that was aligned with the list-predicate fix.
    assertThat(collect("MATCH (p:Person) WHERE size([x IN ['Alice'] WHERE x = p.name]) > 0 RETURN p"))
        .containsExactly("p");
  }

  @Test
  void listComprehensionCombinedWithAndCollectsBothOuterVariables() {
    // Combined predicates: 'p' comes from both sides; iterator 'x' must stay hidden.
    assertThat(collect(
        "MATCH (p:Person) WHERE size([x IN ['Alice'] WHERE x = p.name]) > 0 AND p.age > 0 RETURN p"))
        .containsExactly("p");
  }

  private static Set<String> collect(final String query) {
    final CypherStatement stmt = PARSER.parse(query);
    final WhereClause where = stmt.getMatchClauses().get(0).getWhereClause();
    return WhereClause.collectVariables(where.getConditionExpression());
  }

  /**
   * Regression test for GitHub issue #6567.
   * <p>
   * A relationship variable read only inside a WHERE list predicate ({@code any}/{@code all}/
   * {@code none}/{@code single}) was silently anonymized: {@link com.arcadedb.query.opencypher.executor.CypherVariableUsage}
   * decided whether {@code r0} kept its real binding by scanning {@code Expression#getText()} for the
   * name as a standalone word, but that text is ANTLR's default {@code getText()} - it drops every
   * whitespace between tokens, so {@code any(item IN r0.k8 WHERE item IS NOT NULL)} became
   * {@code "any(itemINr0.k8WHEREitemISNOTNULL)"} and "r0" was no longer a standalone word once glued to
   * the "N" of "IN". The relationship pattern step then bound the edge under an internal anonymous name
   * instead of "r0", so the WHERE clause's own (correct, AST-based) evaluation of {@code r0.k8} read a
   * missing binding, silently turned a true predicate false, and dropped every row downstream - including
   * an entire {@code CALL () { ... } IN TRANSACTIONS} subquery whose results a later {@code collect()}
   * should have seen.
   * <p>
   * A value-preserving {@code WITH r0 AS r0} (or any other clause re-mentioning {@code r0} in a way the
   * text scan could parse) "fixed" the symptom by making the variable look referenced again - which is
   * why the issue surfaced as two textually-equivalent queries returning different results, rather than
   * as an outright wrong answer.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue6567ListPredicateEdgeVariable extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("opencypher",
          "CREATE (n1:Person {id: 'n1'})-[r1:rt2]->(x:Person {id: 'x', klist: ['a']})-[:rt10]->(n0:Person {id: 'n0'})"
              + "-[r0:rt8 {k9: 267022112, k8: [1]}]->(t:Person {id: 't', k0: true, k8: [1, 2]})");
      database.command("opencypher",
          "CREATE (s:Person {id: 22})-[:rt5]->(n3:Person {id: 'n3', klist: []})-[:rt8]->(n2:Person {id: 'n2', k3: 'e'})");
    }

    /**
     * The issue's own reproducer, minimized: a value-preserving {@code WITH} inserted between the
     * {@code CALL () {...} IN TRANSACTIONS} clause and the final {@code RETURN} must not change the
     * {@code collect()} result, and both forms must see the subquery's row.
     */
    @Test
    void identityWithBetweenCallInTransactionsAndReturnDoesNotChangeTheResult() {
      final String withoutIdentityWith = """
          MATCH
            (n1)-[r1:rt2]->(x {klist: ['a']})-[:rt10]->(n0)-[r0:rt8|rt1|rt7 {k9: 267022112}]->(t {k0: true, k8: [1, 2]})
          WHERE any(item IN r0.k8 WHERE item IS NOT NULL)
          CALL () {
            MATCH (n2), (n3 {klist: []})-[:rt8]->(n2),
                  ({id: 22})-[r2:rt5|rt2|rt7]->(n3)
            RETURN n2 AS n15
          } IN TRANSACTIONS OF 10 ROWS
          RETURN collect(toStringOrNull(n15.k3)) AS result
          """;
      final String withIdentityWith = """
          MATCH
            (n1)-[r1:rt2]->(x {klist: ['a']})-[:rt10]->(n0)-[r0:rt8|rt1|rt7 {k9: 267022112}]->(t {k0: true, k8: [1, 2]})
          WHERE any(item IN r0.k8 WHERE item IS NOT NULL)
          CALL () {
            MATCH (n2), (n3 {klist: []})-[:rt8]->(n2),
                  ({id: 22})-[r2:rt5|rt2|rt7]->(n3)
            RETURN n2 AS n15
          } IN TRANSACTIONS OF 10 ROWS
          WITH CASE WHEN r0 IS NULL THEN null ELSE r0 END AS r0, n15
          RETURN collect(toStringOrNull(n15.k3)) AS result
          """;

      assertThat(singleResult(withoutIdentityWith)).containsExactly("e");
      assertThat(singleResult(withIdentityWith)).containsExactly("e");
    }

    /** Same defect, without CALL/IN TRANSACTIONS: the any() predicate alone must not lose the edge binding. */
    @Test
    void anyPredicateOnRelationshipPropertyDoesNotDropTheMatch() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (n1)-[r1:rt2]->(x)-[:rt10]->(n0)-[r0:rt8 {k9: 267022112}]->(t) "
              + "WHERE any(item IN r0.k8 WHERE item IS NOT NULL) "
              + "RETURN count(*) AS c")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
      }
    }

    @SuppressWarnings("unchecked")
    private List<Object> singleResult(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        return (List<Object>) row.getProperty("result");
      }
    }
  }

  /**
   * Regression test for GitHub issue #6673.
   * <p>
   * The list predicates {@code all()}/{@code any()}/{@code none()}/{@code single()} lose Cypher
   * three-valued logic (3VL) when evaluated inside a projection that also contains an aggregation:
   * {@link com.arcadedb.query.opencypher.executor.ExpressionEvaluator} used to special-case
   * {@code ListPredicateExpression} with its own {@code evaluateListPredicate} that counted only
   * {@code Boolean.TRUE} matches and could only ever return {@code true} or {@code false}, never
   * {@code null} - diverging from the primary AST implementation
   * ({@link com.arcadedb.query.opencypher.ast.ListPredicateExpression#evaluate}) and from the
   * openCypher truth tables, where a predicate that is {@code null} for every element yields
   * {@code null}, not {@code false}/{@code true}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue6673ListPredicateAggregation3vl extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("opencypher", "CREATE (:Person {name: 'A'})");
      database.command("opencypher", "CREATE (:Person {name: 'B'})");
      database.command("opencypher", "CREATE (:Person {name: 'C'})");
    }

    @Test
    void anyReturnsNullWhenEveryElementPredicateIsNullInsideAggregation() {
      // Every element's predicate is null (x.nonexistent > 0 -> null) over a non-empty collected list.
      final ResultSet rs = database.query("opencypher",
          "MATCH (v:Person) RETURN any(x IN collect(v) WHERE x.nonexistent > 0) AS r");

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("r")).isNull();
      assertThat(rs.hasNext()).isFalse();
    }

    @Test
    void allReturnsNullWhenEveryElementPredicateIsNullInsideAggregation() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (v:Person) RETURN all(x IN collect(v) WHERE x.nonexistent > 0) AS r");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("r")).isNull();
    }

    @Test
    void noneReturnsNullWhenEveryElementPredicateIsNullInsideAggregation() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (v:Person) RETURN none(x IN collect(v) WHERE x.nonexistent > 0) AS r");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("r")).isNull();
    }

    @Test
    void singleReturnsNullWhenEveryElementPredicateIsNullInsideAggregation() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (v:Person) RETURN single(x IN collect(v) WHERE x.nonexistent > 0) AS r");

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("r")).isNull();
    }

    @Test
    void anyStillTrueWhenAtLeastOneElementMatchesInsideAggregation() {
      // Sanity check: the fix must not turn a genuine match into null. Two elements are null, one is true.
      final ResultSet rs = database.query("opencypher",
          "MATCH (v:Person) RETURN any(x IN collect(v) WHERE x.name = 'A' OR x.nonexistent > 0) AS r");

      assertThat(rs.hasNext()).isTrue();
      assertThat((Boolean) rs.next().getProperty("r")).isTrue();
    }

    @Test
    void noneStillFalseWhenAnElementMatchesInsideAggregation() {
      // Sanity check: a genuine true predicate among nulls must still make none() false, not null.
      final ResultSet rs = database.query("opencypher",
          "MATCH (v:Person) RETURN none(x IN collect(v) WHERE x.name = 'A' OR x.nonexistent > 0) AS r");

      assertThat(rs.hasNext()).isTrue();
      assertThat((Boolean) rs.next().getProperty("r")).isFalse();
    }
  }
}
