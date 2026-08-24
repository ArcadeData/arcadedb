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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6482: a label-disjunction anchor with an incident relationship
 * ({@code MATCH (n:A|B {id: $x})-[:REL]->(m) RETURN m}) used to bail out of the cost-based optimizer
 * for the whole statement - {@code CypherExecutionPlanner.shouldUseOptimizer()} refused any path where a
 * disjunction node had a relationship, even though the disjunction sits on the anchor and only the
 * anchor is served by {@code NodeByLabelDisjunctionScan}/{@code NodeByLabelDisjunctionIndexSeek} (issue
 * #6397). The query still returned correct rows through the legacy engine, just without the index-seek
 * speedup.
 * <p>
 * Also pins the safety net this fix adds: when the disjunction is NOT on the anchor - it sits on a node
 * reached by a relationship instead - the optimizer must still decline (fall back to the legacy engine)
 * rather than build an {@code ExpandAll}/{@code ExpandInto} that can only filter on the disjunction's
 * first label. {@link CypherLabelDisjunctionOnExpandedNodeIssue6338Test} already pins the end-to-end
 * correctness of that shape; the assertions here additionally confirm those queries are not silently
 * routed through the optimizer now that the AST-level gate has been relaxed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherDisjunctionAnchorWithRelationshipIssue6482Test extends TestHelper {
  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final var typeA = database.getSchema().createVertexType("Alpha6482");
      typeA.createProperty("id", String.class);
      typeA.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

      final var typeB = database.getSchema().createVertexType("Bravo6482");
      typeB.createProperty("id", String.class);
      typeB.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

      database.getSchema().createVertexType("Target6482");
      database.getSchema().createEdgeType("LINKS6482");
    });

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Alpha6482 {id: 'a1'})");
      database.command("opencypher", "CREATE (:Bravo6482 {id: 'b1'})");
      database.command("opencypher", "CREATE (:Target6482 {id: 't1'})");
      database.command("opencypher",
          "MATCH (n:Alpha6482 {id: 'a1'}), (m:Target6482 {id: 't1'}) CREATE (n)-[:LINKS6482]->(m)");
      database.command("opencypher",
          "MATCH (n:Bravo6482 {id: 'b1'}), (m:Target6482 {id: 't1'}) CREATE (n)-[:LINKS6482]->(m)");
    });
  }

  @Test
  void disjunctionAnchorWithEqualityPredicateAndRelationshipUsesTheOptimizer() {
    final String query = "MATCH (n:Alpha6482|Bravo6482 {id: 'a1'})-[:LINKS6482]->(m:Target6482) RETURN m.id AS k";
    final String plan = profilePlan(query);
    assertThat(plan).as("plan\n%s", plan)
        .contains("Execution Plan (Cost-Based Optimizer)")
        .contains("NodeByLabelDisjunctionIndexSeek");

    assertThat(ids(query)).containsExactly("t1");
  }

  @Test
  void disjunctionAnchorViaWhereClauseWithRelationshipUsesTheOptimizer() {
    final String query = "MATCH (n:Alpha6482|Bravo6482)-[:LINKS6482]->(m:Target6482) WHERE n.id = 'b1' RETURN m.id AS k";
    final String plan = profilePlan(query);
    assertThat(plan).as("plan\n%s", plan)
        .contains("Execution Plan (Cost-Based Optimizer)")
        .contains("NodeByLabelDisjunctionIndexSeek");

    assertThat(ids(query)).containsExactly("t1");
  }

  @Test
  void bothAlternativesStillReachTheSameTargetOverTheRelationship() {
    assertThat(ids("MATCH (n:Alpha6482|Bravo6482 {id: 'a1'})-[:LINKS6482]->(m:Target6482) RETURN m.id AS k"))
        .containsExactly("t1");
    assertThat(ids("MATCH (n:Alpha6482|Bravo6482 {id: 'b1'})-[:LINKS6482]->(m:Target6482) RETURN m.id AS k"))
        .containsExactly("t1");
  }

  @Test
  void aValueNoAlternativeHasReturnsNothingEvenWithARelationship() {
    assertThat(ids("MATCH (n:Alpha6482|Bravo6482 {id: 'nope'})-[:LINKS6482]->(m:Target6482) RETURN m.id AS k"))
        .isEmpty();
  }

  /**
   * The disjunction sits on the node a relationship expands INTO, not on the anchor. This is the shape
   * {@link CypherLabelDisjunctionOnExpandedNodeIssue6338Test} already pins end-to-end; this assertion
   * additionally confirms the optimizer declines it (falls back to the legacy engine) instead of silently
   * filtering on only the disjunction's first label via {@code addTargetLabelFilter}.
   */
  @Test
  void disjunctionOnTheExpandedNodeDoesNotUseTheOptimizerButStaysCorrect() {
    final String query = "MATCH (m:Target6482)<-[:LINKS6482]-(n:Alpha6482|Bravo6482) RETURN n.id AS k ORDER BY k";
    final String plan = profilePlan(query);
    assertThat(plan).as("plan\n%s", plan).doesNotContain("Execution Plan (Cost-Based Optimizer)");

    assertThat(ids(query)).containsExactly("a1", "b1");
  }

  private List<String> ids(final String cypher) {
    final List<String> result = new ArrayList<>();
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", cypher);
      while (rs.hasNext())
        result.add(rs.next().getProperty("k"));
      rs.close();
    });
    return result;
  }

  private String profilePlan(final String cypher) {
    final StringBuilder plan = new StringBuilder();
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", "PROFILE " + cypher);
      while (rs.hasNext())
        rs.next();
      plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
      rs.close();
    });
    return plan.toString();
  }
}
