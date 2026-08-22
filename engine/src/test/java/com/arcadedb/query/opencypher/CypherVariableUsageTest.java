/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.query.opencypher.executor.CypherVariableUsage;
import com.arcadedb.query.opencypher.parser.Cypher25AntlrParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link CypherVariableUsage} answers one question for both Cypher executors - is this relationship
 * variable read anywhere outside the pattern that binds it - and both use the answer to decide whether
 * to materialize edge records or walk adjacency ids instead. They have to agree, so a miss here does
 * not fail loudly: it silently changes which path a query takes, or, when a binding is dropped that
 * something did need, silently changes the ROWS.
 * <p>
 * Both failure modes have already happened. {@code RETURN *} was missed because the check read only
 * the explicit return items, and the relationship column vanished from the result (found by the TCK
 * while reviewing #5446). An inline pattern predicate was missed and the edge evaluated against an
 * unbound variable, so {@code count(*)} returned 0 where {@code count(r)} returned 1 (#5466).
 * <p>
 * Hence one case per clause shape the analysis walks, so a future clause type that forgets to look
 * inside itself fails here rather than in a query someone runs months later. Every case below was
 * checked by mutation - disabling the branch it covers makes it fail - except where noted.
 * <p>
 * Two implementation details this cannot reach from a parsed query, recorded so the next reader does
 * not mistake them for gaps: {@code RETURN *} and {@code UNWIND} are each scanned twice (once through
 * {@code clausesInOrder}, once directly), so disabling one path alone changes nothing; and the
 * statement-level {@code WHERE} scan is unreachable, because the parser attaches a MATCH's WHERE to
 * the MATCH clause and a WITH's to the WITH clause - nothing it produces ever sets
 * {@code CypherStatement.getWhereClause()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherVariableUsageTest {
  private static final Cypher25AntlrParser PARSER = new Cypher25AntlrParser();

  @Test
  void aVariableNobodyReadsIsNotReferenced() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) RETURN b.name", "r")).isFalse();
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) RETURN count(*)", "r")).isFalse();
  }

  @Test
  void returnReadsIt() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) RETURN r", "r")).isTrue();
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) RETURN r.since", "r")).isTrue();
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) RETURN collect(r.since) AS years", "r")).isTrue();
  }

  /** RETURN * projects every variable in scope by name, so nothing under it is unreferenced. */
  @Test
  void returnStarReadsEverything() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) RETURN *", "r")).isTrue();
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) WITH a, b, r RETURN *", "r")).isTrue();
  }

  @Test
  void withReadsIt() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) WITH r AS edge RETURN edge", "r")).isTrue();
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) WITH a WHERE r.since > 2000 RETURN a", "r")).isTrue();
  }

  @Test
  void whereReadsIt() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) WHERE r.since > 2000 RETURN b", "r")).isTrue();
  }

  @Test
  void orderByReadsIt() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) RETURN b ORDER BY r.since", "r")).isTrue();
  }

  @Test
  void unwindReadsIt() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) UNWIND [r] AS e RETURN e", "r")).isTrue();
  }

  @Test
  void setReadsItAsTargetAndAsValue() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) SET r.seen = true RETURN b", "r")).isTrue();
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) SET a.last = r.since RETURN b", "r")).isTrue();
  }

  @Test
  void removeReadsIt() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) REMOVE r.since RETURN b", "r")).isTrue();
  }

  @Test
  void deleteReadsIt() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) DELETE r", "r")).isTrue();
  }

  @Test
  void foreachReadsItInTheListAndInsideTheBody() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) FOREACH (x IN [r] | SET a.touched = true) RETURN a", "r")).isTrue();
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) FOREACH (x IN [1] | DELETE r) RETURN a", "r")).isTrue();
  }

  @Test
  void aCallSubqueryReadsItByImportAndInsideItsBody() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) CALL (r) { RETURN 1 AS one } RETURN one", "r")).isTrue();
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) CALL { WITH r RETURN r.since AS s } RETURN s", "r")).isTrue();
  }

  /** The same name bound by a second pattern is a reference: the two hops must agree on one edge. */
  @Test
  void aSecondPatternBindingTheSameNameReadsIt() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) MATCH (c)-[r]->(d) RETURN c", "r")).isTrue();
  }

  /**
   * An inline pattern predicate reads the variable it constrains. Missing this dropped the edge
   * binding, so the predicate evaluated against an unbound variable and filtered out every row (#5466).
   */
  @Test
  void anInlinePatternPredicateReadsIt() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS WHERE r.since > 2000]->(b) RETURN count(*)", "r")).isTrue();
  }

  /** Word-boundary matching: a longer identifier that merely contains the name is not a reference. */
  @Test
  void aLongerIdentifierContainingTheNameIsNotAReference() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) RETURN b.relation", "r")).isFalse();
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) RETURN b.r_id", "r")).isFalse();
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) RETURN b.prefixr", "r")).isFalse();
  }

  /**
   * Issue #6567: a relationship read only inside a list predicate (any/all/none/single) or list
   * comprehension must keep its real binding. The word-boundary text scan this class used before
   * missed it: {@code any(item IN r0.k8 WHERE item IS NOT NULL)}'s {@code getText()} - ANTLR's default,
   * which drops all whitespace between tokens - reads {@code "any(itemINr0.k8WHEREitemISNOTNULL)"},
   * where "r0" no longer starts a word once it is glued to the "N" of "IN". Anonymizing the edge under
   * that text then made the WHERE clause's own AST-based evaluation of {@code r0.k8} read a missing
   * binding instead of the real list, turning a true predicate false and dropping every row.
   */
  @Test
  void aRelationshipReadOnlyInsideAListPredicateIsReferenced() {
    assertThat(isReferenced(
        "MATCH (a)-[r0:KNOWS]->(b) WHERE any(item IN r0.tags WHERE item IS NOT NULL) RETURN count(*)", "r0"))
        .isTrue();
    assertThat(isReferenced(
        "MATCH (a)-[r0:KNOWS]->(b) WHERE all(item IN r0.tags WHERE item IS NOT NULL) RETURN count(*)", "r0"))
        .isTrue();
    assertThat(isReferenced(
        "MATCH (a)-[r0:KNOWS]->(b) WHERE none(item IN r0.tags WHERE item IS NULL) RETURN count(*)", "r0"))
        .isTrue();
    assertThat(isReferenced(
        "MATCH (a)-[r0:KNOWS]->(b) WHERE single(item IN r0.tags WHERE item IS NOT NULL) RETURN count(*)", "r0"))
        .isTrue();
    assertThat(isReferenced(
        "MATCH (a)-[r0:KNOWS]->(b) WHERE size([item IN r0.tags WHERE item IS NOT NULL]) > 0 RETURN count(*)", "r0"))
        .isTrue();
    // Control: an edge truly absent from the predicate - not even as a loop-scoped iterator name -
    // must not be reported as referenced just because the statement contains a list predicate at all.
    assertThat(isReferenced(
        "MATCH (a)-[unused:KNOWS]->(b) WHERE any(item IN [1,2] WHERE item > 0) RETURN count(*)", "unused"))
        .isFalse();
  }

  /**
   * Issue #6567 review: a relationship read through a chained property access on a function result -
   * {@code startNode(r).name}, not the simple {@code r.name} - must also keep its real binding.
   * {@link com.arcadedb.query.opencypher.parser.CypherExpressionWalker} had no case for that node type
   * (built by {@code CypherExpressionBuilder.ChainedPropertyAccessExpression} whenever a property access
   * follows something other than a bare variable) and treated it as a leaf, so the walk never reached
   * the {@code r} inside {@code startNode(r)}.
   */
  @Test
  void aRelationshipReadThroughAChainedPropertyAccessIsReferenced() {
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) RETURN startNode(r).name AS n", "r")).isTrue();
    assertThat(isReferenced("MATCH (a)-[r:KNOWS]->(b) RETURN endNode(r).name AS n", "r")).isTrue();
  }

  @Test
  void expressionMatchingIsNullSafeAndBoundaryAware() {
    assertThat(CypherVariableUsage.expressionReferencesVariable((String) null, "r")).isFalse();
    assertThat(CypherVariableUsage.expressionReferencesVariable("r.since", null)).isFalse();
    assertThat(CypherVariableUsage.expressionReferencesVariable("r", "r")).isTrue();
    assertThat(CypherVariableUsage.expressionReferencesVariable("r.since + rate", "r")).isTrue();
    assertThat(CypherVariableUsage.expressionReferencesVariable("rate", "r")).isFalse();
    assertThat(CypherVariableUsage.expressionReferencesVariable("my_r", "r")).isFalse();
    assertThat(CypherVariableUsage.expressionReferencesVariable("r2", "r")).isFalse();
  }

  private static boolean isReferenced(final String query, final String variable) {
    final CypherStatement statement = PARSER.parse(query);
    return CypherVariableUsage.isEdgeVariableReferenced(statement, variable);
  }
}
