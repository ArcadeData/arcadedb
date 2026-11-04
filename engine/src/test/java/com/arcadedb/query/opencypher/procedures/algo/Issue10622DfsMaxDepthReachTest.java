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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10622: {@code algo.dfs} with a {@code maxDepth} left out nodes reachable within the bound. A node was marked when it
 * was pushed and expanded only while its depth was below the bound, so a node first reached by a long path at the bound was
 * never expanded, the shorter path to it found it marked already, and what it led to was lost - which nodes depended on the
 * order the neighbours were read. A bounded walk now expands a node again when it reaches it at a smaller depth, so it reports
 * every node within the bound, exactly the nodes {@code algo.bfs} reports for the same bound.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10622DfsMaxDepthReachTest extends TestHelper {

  /** The issue's graph: {@code w} is 3 hops away through {@code b}, and 4 through the long branch {@code a1 -> a2}. */
  private void createIssueGraph(final String type, final boolean longBranchFirst) {
    database.getSchema().createVertexType(type);
    final String edges = longBranchFirst ?
        "(s)-[:E]->(b), (s)-[:E]->(a1), (a1)-[:E]->(a2), (a2)-[:E]->(u), (b)-[:E]->(u), (u)-[:E]->(w)" :
        "(s)-[:E]->(a1), (s)-[:E]->(b), (a1)-[:E]->(a2), (a2)-[:E]->(u), (b)-[:E]->(u), (u)-[:E]->(w)";
    database.transaction(() -> database.command("opencypher",
        ("CREATE (s:T {id:'s'}), (a1:T {id:'a1'}), (a2:T {id:'a2'}), (u:T {id:'u'}), (b:T {id:'b'}), (w:T {id:'w'}), " + edges)
            .replace(":T ", ":" + type + " ")));
  }

  @Test
  void aNodeWithinTheBoundIsReportedWhateverOrderTheEdgesAreReadIn() {
    database.getSchema().createEdgeType("E");
    // The same graph twice, with its edges created in the two orders: which order reaches w first decided what was lost
    createIssueGraph("LongFirst", true);
    createIssueGraph("ShortFirst", false);

    for (final String type : new String[] { "LongFirst", "ShortFirst" }) {
      for (int maxDepth = 0; maxDepth <= 5; maxDepth++)
        assertThat(dfs(type, "s", "OUT", maxDepth).keySet()).as("%s maxDepth=%d, no view", type, maxDepth)
            .isEqualTo(bfs(type, "s", "OUT", maxDepth).keySet());

      // The issue's call: w is 3 hops away, so a bound of 3 reports it
      assertThat(dfs(type, "s", "OUT", 3)).as(type).containsKey("w");
    }

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("issue-10622")
        .withVertexTypes("LongFirst", "ShortFirst").withEdgeTypes("E").withUpdateMode(GraphAnalyticalView.UpdateMode.OFF).build();
    try {
      for (final String type : new String[] { "LongFirst", "ShortFirst" })
        for (int maxDepth = 0; maxDepth <= 5; maxDepth++)
          assertThat(dfs(type, "s", "OUT", maxDepth).keySet()).as("%s maxDepth=%d, view", type, maxDepth)
              .isEqualTo(bfs(type, "s", "OUT", maxDepth).keySet());
    } finally {
      view.drop();
    }
  }

  /**
   * A denser graph where many nodes are first met by a long path: every bound and direction, with and without a view, reports
   * the nodes a breadth-first walk reports, each at most once and at a depth within the bound and no smaller than its distance.
   */
  @Test
  void everyBoundReportsTheNodesWithinItAtADepthWithinIt() {
    final int nodes = 120;
    database.getSchema().createVertexType("G");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      final Vertex[] v = new Vertex[nodes];
      for (int i = 0; i < nodes; i++)
        v[i] = database.newVertex("G").set("id", String.valueOf(i)).save();
      for (int i = 0; i < nodes; i++) {
        // A long chain read first, then shortcuts that reach the same nodes in fewer hops
        if (i + 1 < nodes)
          v[i].asVertex().modify().newEdge("E", v[i + 1]).save();
        v[i].asVertex().modify().newEdge("E", v[(i * 11 + 5) % nodes]).save();
        v[i].asVertex().modify().newEdge("E", v[(i * 3 + 17) % nodes]).save();
      }
    });

    checkAgainstBfs("no view");
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("issue-10622-g").withVertexTypes("G")
        .withEdgeTypes("E").withUpdateMode(GraphAnalyticalView.UpdateMode.OFF).build();
    try {
      checkAgainstBfs("view");
    } finally {
      view.drop();
    }
  }

  private void checkAgainstBfs(final String label) {
    for (final String dir : new String[] { "OUT", "IN", "BOTH" })
      for (final int maxDepth : new int[] { 1, 2, 3, 4, 6, 9 }) {
        final Map<String, Integer> distances = bfs("G", "0", dir, maxDepth);
        final Map<String, Integer> reported = dfs("G", "0", dir, maxDepth);
        assertThat(reported.keySet()).as("%s dir=%s maxDepth=%d", label, dir, maxDepth).isEqualTo(distances.keySet());
        for (final Map.Entry<String, Integer> row : reported.entrySet())
          assertThat(row.getValue()).as("%s dir=%s maxDepth=%d node=%s", label, dir, maxDepth, row.getKey())
              .isBetween(distances.get(row.getKey()), maxDepth);
      }
  }

  /** The nodes {@code algo.dfs} reports, by id, with their depth; fails if a node is reported twice. */
  private Map<String, Integer> dfs(final String type, final String start, final String dir, final int maxDepth) {
    return call("algo.dfs", type, start, dir, maxDepth);
  }

  /** The nodes {@code algo.bfs} reports, by id, with their distance. */
  private Map<String, Integer> bfs(final String type, final String start, final String dir, final int maxDepth) {
    return call("algo.bfs", type, start, dir, maxDepth);
  }

  private Map<String, Integer> call(final String procedure, final String type, final String start, final String dir,
      final int maxDepth) {
    final Map<String, Integer> rows = new LinkedHashMap<>();
    final Set<String> duplicates = new HashSet<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:" + type + " {id: $start}) CALL " + procedure + "(s, 'E', $dir, $k) YIELD node, depth RETURN node.id AS id, depth",
        new HashMap<>(Map.of("start", start, "dir", dir, "k", (long) maxDepth)))) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final String id = row.getProperty("id");
        if (rows.put(id, row.<Number>getProperty("depth").intValue()) != null)
          duplicates.add(id);
      }
    }
    assertThat(duplicates).as("%s reported these nodes more than once", procedure).isEmpty();
    return rows;
  }
}
