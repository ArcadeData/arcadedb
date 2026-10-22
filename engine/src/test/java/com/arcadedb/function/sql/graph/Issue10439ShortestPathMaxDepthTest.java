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
package com.arcadedb.function.sql.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10439: {@code shortestPath()}'s {@code maxDepth} option counted vertices instead of edges,
 * so a path of exactly {@code maxDepth} edges was missed and {@code maxDepth: 1} could never find even a direct edge.
 * The option now bounds the number of edges, the same as Cypher's {@code *..n}.
 */
class Issue10439ShortestPathMaxDepthTest extends TestHelper {
  private static final int LENGTH = 8;

  /** A chain {@code 0 -> 1 -> ... -> LENGTH} plus a fan of dead-end leaves on vertex 0, so the two BFS fronts differ in size. */
  private void buildChain() {
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("L");
    database.transaction(() -> {
      for (int i = 0; i <= LENGTH; i++)
        database.command("sql", "INSERT INTO N SET id = ?", i);
      for (int i = 0; i < LENGTH; i++)
        database.command("sql", "CREATE EDGE L FROM (SELECT FROM N WHERE id = ?) TO (SELECT FROM N WHERE id = ?)", i, i + 1);
      for (int i = 0; i < 5; i++) {
        database.command("sql", "INSERT INTO N SET id = ?", 100 + i);
        database.command("sql", "CREATE EDGE L FROM (SELECT FROM N WHERE id = 0) TO (SELECT FROM N WHERE id = ?)", 100 + i);
      }
    });
  }

  private List<?> sqlPath(final int from, final int to, final String direction, final String options) {
    try (final ResultSet rs = database.query("sql",
        "SELECT shortestPath((SELECT FROM N WHERE id = ?), (SELECT FROM N WHERE id = ?), '" + direction + "', 'L', " + options
            + ") AS p", from, to)) {
      return rs.next().<List<?>>getProperty("p");
    }
  }

  private List<?> consolidatedPath(final int from, final int to, final int maxDepth) {
    try (final ResultSet rs = database.query("sql",
        "SELECT shortestPath((SELECT FROM N WHERE id = ?), (SELECT FROM N WHERE id = ?), {direction: 'OUT', edgeType: 'L', maxDepth: "
            + maxDepth + "}) AS p", from, to)) {
      return rs.next().<List<?>>getProperty("p");
    }
  }

  private void assertEdgeBound(final String label) {
    for (final String direction : new String[] { "OUT", "BOTH" })
      for (int to = 1; to <= LENGTH; to++) {
        assertThat(sqlPath(0, to, direction, "{maxDepth: " + to + "}")).as("%s %s: %d edges, maxDepth %d", label, direction, to, to)
            .hasSize(to + 1);
        assertThat(sqlPath(0, to, direction, "{maxDepth: " + (to - 1) + "}"))
            .as("%s %s: %d edges, maxDepth %d", label, direction, to, to - 1).isEmpty();
        // Reversed endpoints: the destination side has the smaller front, so the right-first branch runs
        assertThat(sqlPath(to, 0, "BOTH", "{maxDepth: " + to + "}")).as("%s reversed: %d edges, maxDepth %d", label, to, to)
            .hasSize(to + 1);
        assertThat(sqlPath(to, 0, "BOTH", "{maxDepth: " + (to - 1) + "}"))
            .as("%s reversed: %d edges, maxDepth %d", label, to, to - 1).isEmpty();
      }
  }

  @Test
  void maxDepthBoundsTheNumberOfEdges() {
    buildChain();
    // The reported repro: three edges, maxDepth 3
    assertThat(sqlPath(0, 3, "OUT", "{maxDepth: 3}")).hasSize(4);
    assertThat(sqlPath(0, 3, "OUT", "{maxDepth: 2}")).isEmpty();
    // A single edge used to be unreachable at any maxDepth below 2
    assertThat(sqlPath(0, 1, "OUT", "{maxDepth: 1}")).hasSize(2);
    assertThat(sqlPath(0, 1, "OUT", "{maxDepth: 0}")).isEmpty();
    assertEdgeBound("OLTP");
  }

  @Test
  void consolidatedOptionsMapUsesTheSameBound() {
    buildChain();
    for (int to = 1; to <= LENGTH; to++) {
      assertThat(consolidatedPath(0, to, to)).as("%d edges, maxDepth %d", to, to).hasSize(to + 1);
      assertThat(consolidatedPath(0, to, to - 1)).as("%d edges, maxDepth %d", to, to - 1).isEmpty();
    }
  }

  @Test
  void edgeModeUsesTheSameBound() {
    buildChain();
    for (int to = 1; to <= LENGTH; to++) {
      // edge: true returns vertices and edges interleaved
      assertThat(sqlPath(0, to, "OUT", "{maxDepth: " + to + ", edge: true}")).as("%d edges, maxDepth %d", to, to)
          .hasSize(2 * to + 1);
      assertThat(sqlPath(0, to, "OUT", "{maxDepth: " + (to - 1) + ", edge: true}")).as("%d edges, maxDepth %d", to, to - 1)
          .isEmpty();
    }
  }

  @Test
  void csrAcceleratedSearchUsesTheSameBound() {
    buildChain();
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("issue-10439-view").withVertexTypes("N")
        .withEdgeTypes("L").build();
    try {
      // Without this the loop below could pass on the OLTP path twice
      try (final ResultSet profile = database.command("sql",
          "PROFILE SELECT shortestPath((SELECT FROM N WHERE id = 0), (SELECT FROM N WHERE id = 3), 'OUT', 'L', {maxDepth: 3}) AS p")) {
        assertThat(profile.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("CSR-accelerated");
      }
      assertEdgeBound("CSR");
    } finally {
      view.drop();
    }
  }

  @Test
  void sameSourceAndDestinationIsAZeroEdgePath() {
    buildChain();
    assertThat(sqlPath(2, 2, "OUT", "{maxDepth: 0}")).hasSize(1);
  }

  @Test
  void cypherHopBoundsAgreeWithSqlMaxDepth() {
    buildChain();
    for (int to = 1; to <= LENGTH; to++) {
      assertThat(cypherPathLength(to, to)).as("Cypher *..%d over %d edges", to, to).isEqualTo(to + 1);
      assertThat(cypherPathLength(to, to - 1)).as("Cypher *..%d over %d edges", to - 1, to).isEqualTo(-1);
      assertThat(sqlPath(0, to, "OUT", "{maxDepth: " + to + "}")).hasSize(cypherPathLength(to, to));
    }
  }

  /** Number of vertices on the Cypher shortest path from 0 to {@code to} within {@code max} hops, or -1 for no row. */
  private int cypherPathLength(final int to, final int max) {
    if (max < 1)
      return -1;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH p = shortestPath((a:N {id: 0})-[:L*.." + max + "]->(b:N {id: " + to + "})) RETURN [n IN nodes(p) | n.id] AS path")) {
      if (!rs.hasNext())
        return -1;
      final Result row = rs.next();
      final List<?> path = row.getProperty("path");
      return path == null ? -1 : path.size();
    }
  }
}
