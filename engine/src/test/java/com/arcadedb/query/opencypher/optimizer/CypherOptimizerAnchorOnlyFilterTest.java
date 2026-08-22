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
package com.arcadedb.query.opencypher.optimizer;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the {@code CypherOptimizer.readsOnlyTheAnchor} half of issue #6567.
 * <p>
 * {@code readsOnlyTheAnchor} decides whether a WHERE conjunct can be pushed straight onto the
 * anchor node's {@code NodeByLabelScan} - safe only when the conjunct reads the anchor variable and
 * nothing else. It used to answer that with the same {@code Expression#getText()} word-boundary scan
 * that caused the executor-side bug: ANTLR's {@code getText()} drops whitespace between tokens, so
 * {@code any(item IN r0.tags WHERE ...)} renders as {@code "any(itemINr0.tagsWHERE...")}, and a
 * {@code \br0\b} search no longer finds "r0" because it is glued onto the immediately preceding "IN".
 * <p>
 * When the anchor is a different variable than the one glued away, this isn't just a missed
 * optimization: {@code readsOnlyTheAnchor} wrongly concludes the conjunct depends on the anchor alone
 * and pushes it onto the anchor scan, where the relationship variable it actually reads is not bound
 * yet. This test's predicate reads both the anchor node's property and the relationship's list
 * property in the same {@code any(...)} conjunct, both node labels are present so the query is
 * eligible for the cost-based optimizer, and it asserts the plan actually took that path (rather than
 * silently falling back to the legacy evaluator, which was never affected by this bug).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherOptimizerAnchorOnlyFilterTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("KNOWS");
    });
    // Matches: 5 is in the relationship's tags list.
    database.command("opencypher", "CREATE (:Person {id: 1, threshold: 5})-[:KNOWS {tags: [5, 6]}]->(:Person {id: 11})");
    // Does not match: 99 is not in the relationship's tags list.
    database.command("opencypher", "CREATE (:Person {id: 2, threshold: 99})-[:KNOWS {tags: [5, 6]}]->(:Person {id: 12})");
  }

  @Test
  void anyPredicateReadingBothAnchorAndRelationshipIsNotWronglyPushedToAnchorScan() {
    final String query = """
        MATCH (a:Person)-[r0:KNOWS]->(b:Person)
        WHERE any(item IN r0.tags WHERE item = a.threshold)
        RETURN a.id AS aid
        """;

    // Confirm this query is actually eligible for the cost-based optimizer (both endpoints labeled),
    // so the assertion below pins CypherOptimizer.readsOnlyTheAnchor rather than the legacy evaluator.
    try (final ResultSet explain = database.query("opencypher", "EXPLAIN " + query)) {
      final String plan = explain.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(plan).contains("Using Cost-Based Query Optimizer");
    }

    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("aid").intValue()).isEqualTo(1);
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
