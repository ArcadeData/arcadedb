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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5810: a disconnected {@code MATCH} that follows a relationship
 * pattern must independently bind its new variable to every match, producing a Cartesian product,
 * not preserve a single row with the new variable bound to {@code null}.
 * <p>
 * The cost-based optimizer's {@code optimize()} builds a single anchor + expansion chain from
 * {@code logicalPlan.getRelationships()}. A node introduced by a later, disconnected {@code MATCH}
 * (e.g. {@code x} in {@code MATCH (n)-[:E]->(m) MATCH (x)}) shares no variable with any relationship,
 * so it was never turned into a physical operator at all: it silently fell out of the plan and its
 * variable resolved to {@code null} downstream, without eliminating the row the way {@code OPTIONAL
 * MATCH} would. The fix Cartesian-joins every such disconnected single-node component into the plan,
 * the same way {@code optimizeMultiMatchIndependent} already does when a query has no relationships
 * at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5810DisconnectedMatchAfterRelationshipTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE A_0 IF NOT EXISTS");
    database.command("sql", "CREATE VERTEX TYPE B_0 IF NOT EXISTS");
    database.command("sql", "CREATE EDGE TYPE E IF NOT EXISTS");
    database.command("sql", "CREATE PROPERTY A_0.id IF NOT EXISTS INTEGER");
    database.command("sql", "CREATE PROPERTY B_0.id IF NOT EXISTS INTEGER");
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON A_0 (id) UNIQUE");
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON B_0 (id) UNIQUE");

    database.command("opencypher", "CREATE (:A_0 {id: 1})");
    database.command("opencypher", "CREATE (:B_0 {id: 2})");
    database.command("opencypher", "CREATE (:B_0 {id: 3})");
    database.command("opencypher", "MATCH (a:A_0 {id: 1}), (b:B_0 {id: 2}) CREATE (a)-[:E]->(b)");
  }

  /**
   * The reporter's exact query. The relationship MATCH produces one row (n=1, m=2); the disconnected
   * MATCH (x:B_0) must independently bind x to both B_0 nodes, producing 2 rows: (2,2) and (2,3).
   */
  @Test
  void disconnectedMatchAfterRelationshipProducesCartesianProduct() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A_0)-[:E]->(m:B_0) MATCH (x:B_0) RETURN m.id AS m, x.id AS x ORDER BY x");

    final List<String> rows = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      rows.add(r.<Integer>getProperty("m") + "|" + r.<Integer>getProperty("x"));
    }
    rs.close();

    assertThat(rows).containsExactly("2|2", "2|3");
  }

  /** Same query via count(*): must count 2 rows, not 1. */
  @Test
  void countStarReflectsTheCartesianProduct() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A_0)-[:E]->(m:B_0) MATCH (x:B_0) RETURN count(*) AS c");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("c")).isEqualTo(2L);
    rs.close();
  }

  /** The fix must exercise the CartesianProduct operator, not silently fall back to the legacy path. */
  @Test
  void planUsesCartesianProductOperator() {
    final ResultSet rs = database.query("opencypher",
        "PROFILE MATCH (n:A_0)-[:E]->(m:B_0) MATCH (x:B_0) RETURN m.id AS m, x.id AS x");
    while (rs.hasNext())
      rs.next();
    final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    rs.close();

    assertThat(plan).contains("CartesianProduct");
  }

  /** Control: reversing clause order already worked before the fix and must keep working. */
  @Test
  void disconnectedMatchBeforeRelationshipStillWorks() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (x:B_0) MATCH (n:A_0)-[:E]->(m:B_0) RETURN count(*) AS c");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("c")).isEqualTo(2L);
    rs.close();
  }

  /**
   * The disconnected node does not even need a variable to affect the row count: an anonymous
   * standalone node in a later MATCH clause must still multiply the rows, not vanish from the plan.
   */
  @Test
  void anonymousDisconnectedNodeStillMultipliesRows() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A_0)-[:E]->(m:B_0) MATCH (:B_0) RETURN count(*) AS c");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("c")).isEqualTo(2L);
    rs.close();
  }

  /**
   * A WHERE predicate that reads only the disconnected variable must still be evaluated correctly
   * once that variable is bound by the Cartesian join, not before (when it would not resolve at all).
   */
  @Test
  void whereFilterOnDisconnectedVariableIsAppliedAfterTheJoin() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A_0)-[:E]->(m:B_0) MATCH (x:B_0) WHERE x.id > 2 RETURN m.id AS m, x.id AS x");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Integer>getProperty("m")).isEqualTo(2);
    assertThat(row.<Integer>getProperty("x")).isEqualTo(3);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  /**
   * Review finding: {@link com.arcadedb.query.opencypher.optimizer.AnchorSelector#selectAnchor} is
   * purely cost-based with no relationship-reachability awareness. Before the fix, a disconnected
   * node with its own indexed equality filter (cheaper than either endpoint of the unfiltered
   * relationship pattern) could win anchor selection outright; {@code buildExpansionChain} would then
   * try to expand the {@code n-[:E]->m} relationship starting from {@code x}, which is not one of its
   * endpoints, silently returning 0 rows instead of the correct single row.
   */
  @Test
  void disconnectedNodeWithOwnIndexedFilterDoesNotHijackAnchorSelection() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A_0)-[:E]->(m:B_0) MATCH (x:B_0 {id: 3}) RETURN m.id AS m, x.id AS x");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Integer>getProperty("m")).isEqualTo(2);
    assertThat(row.<Integer>getProperty("x")).isEqualTo(3);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  /** Two disconnected single-node MATCH clauses after a relationship pattern must both fan out. */
  @Test
  void multipleDisconnectedNodesChainCorrectly() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A_0)-[:E]->(m:B_0) MATCH (x:B_0) MATCH (y:B_0) RETURN count(*) AS c");
    assertThat(rs.hasNext()).isTrue();
    // 1 relationship row x 2 B_0 nodes (x) x 2 B_0 nodes (y) = 4
    assertThat(rs.next().<Long>getProperty("c")).isEqualTo(4L);
    rs.close();
  }

  /**
   * A compound WHERE mixing a connected conjunct (n.id = 1, always true here) and a disconnected
   * conjunct (x.id = 3) must apply both correctly: the connected conjunct is safe to push down
   * alongside anchor selection, the disconnected one only after the Cartesian join binds x.
   */
  @Test
  void mixedConnectedAndDisconnectedWhereClauseAppliesBothParts() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A_0)-[:E]->(m:B_0) MATCH (x:B_0) WHERE n.id = 1 AND x.id = 3 RETURN m.id AS m, x.id AS x");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Integer>getProperty("m")).isEqualTo(2);
    assertThat(row.<Integer>getProperty("x")).isEqualTo(3);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  /** Same mixed clause, but the connected conjunct excludes the only row: must return nothing. */
  @Test
  void mixedConnectedAndDisconnectedWhereClauseConnectedConjunctCanExcludeAllRows() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A_0)-[:E]->(m:B_0) MATCH (x:B_0) WHERE n.id = 999 AND x.id = 3 RETURN m.id AS m, x.id AS x");
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }
}
