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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10539: {@code algo.bfs} on a Graph Analytical View ran the BFS kernel over the whole reachable graph and filtered
 * by {@code maxDepth} afterwards, and the paths without a ready view loaded the whole graph before walking it. Every path
 * now stops at {@code maxDepth} and answers exactly what the unbounded search filtered by depth answered.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue10539BfsHonoursMaxDepthTest extends TestHelper {
  private static final int   NODES     = 300;
  private static final int[] DEPTHS    = { 0, 1, 2, 3, Integer.MAX_VALUE };
  private final RID[]        rids      = new RID[NODES];
  private final List<int[]>  edges     = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("E");
    // A chain 0 -> 1 -> ... plus a deterministic shortcut out of every node, so that depths are not just positions
    database.transaction(() -> {
      final MutableVertex[] v = new MutableVertex[NODES];
      for (int i = 0; i < NODES; i++) {
        v[i] = database.newVertex("Node").set("id", i).save();
        rids[i] = v[i].getIdentity();
      }
      for (int i = 0; i < NODES; i++) {
        if (i + 1 < NODES)
          addEdge(v, i, i + 1);
        final int shortcut = (i * 7 + 3) % NODES;
        if (shortcut != i && shortcut != i + 1)
          addEdge(v, i, shortcut);
      }
    });
  }

  private void addEdge(final MutableVertex[] v, final int from, final int to) {
    v[from].newEdge("E", v[to]).save();
    edges.add(new int[] { from, to });
  }

  @Test
  void theKernelStopsExpandingAtMaxDepth() throws Exception {
    // The result holds only what the search reached: a bound of one hop reaches the start node's two neighbours, not the
    // 300 nodes an unbounded search reaches and a filter used to throw away
    final GraphAnalyticalView view = createView();
    try {
      final int source = view.getNodeId(rids[0]);
      final GraphAlgorithms.BFSLevels bounded = GraphAlgorithms.breadthFirst(view, source, 1, Vertex.DIRECTION.OUT, "E");
      final Map<Integer, Integer> expected = referenceBfs(0, 1, Vertex.DIRECTION.OUT);
      assertThat(bounded.size()).isEqualTo(expected.size());
      assertThat(bounded.levels()).isEqualTo(2);

      final GraphAlgorithms.BFSLevels zero = GraphAlgorithms.breadthFirst(view, source, 0, Vertex.DIRECTION.OUT, "E");
      assertThat(zero.size()).isEqualTo(1);
      assertThat(zero.order()[0]).isEqualTo(source);

      for (final int maxDepth : DEPTHS)
        for (final Vertex.DIRECTION dir : Vertex.DIRECTION.values()) {
          final GraphAlgorithms.BFSLevels bfs = GraphAlgorithms.breadthFirst(view, source, maxDepth, dir, "E");
          final Map<Integer, Integer> reference = referenceBfs(0, maxDepth, dir);
          final Map<Integer, Integer> actual = new HashMap<>();
          for (int i = 0; i < bfs.size(); i++)
            actual.put(idOf(view.getRID(bfs.order()[i])), bfs.depthAt(i));
          assertThat(actual).as("maxDepth=%d dir=%s", maxDepth, dir).isEqualTo(reference);
          // BFS order: depths never decrease along the result
          for (int i = 1; i < bfs.size(); i++)
            assertThat(bfs.depthAt(i)).isGreaterThanOrEqualTo(bfs.depthAt(i - 1));

          // The distance array of the bounded overload is the unbounded one with every node past the bound unreachable
          final int[] unbounded = GraphAlgorithms.shortestPathAll(view, source, dir, "E");
          final int[] dist = GraphAlgorithms.shortestPathAll(view, source, maxDepth, dir, "E");
          for (int i = 0; i < dist.length; i++)
            assertThat(dist[i]).isEqualTo(unbounded[i] <= maxDepth ? unbounded[i] : -1);
        }
    } finally {
      view.drop();
    }
  }

  @Test
  void theKernelAgreesWithAReferenceBfsAcrossItsParallelAndPullLevels() throws Exception {
    // Wide enough that the second level (5000 nodes) exceeds the parallel push threshold (4096) without reaching the pull
    // threshold (n / 8 = 6250), and the third is large and edge-dense enough to pull, in parallel (n > 8192)
    database.getSchema().createVertexType("Wide");
    database.getSchema().createEdgeType("W");
    final int wide = 50_000;
    final RID[] wideRids = new RID[wide];
    final List<int[]> wideEdges = new ArrayList<>();
    database.transaction(() -> {
      final MutableVertex[] v = new MutableVertex[wide];
      for (int i = 0; i < wide; i++) {
        v[i] = database.newVertex("Wide").set("id", i).save();
        wideRids[i] = v[i].getIdentity();
      }
      for (int i = 1; i <= 5000; i++) {
        v[0].newEdge("W", v[i]);
        wideEdges.add(new int[] { 0, i });
      }
      for (int i = 1; i < wide; i++)
        for (int k = 1; k <= 2; k++) {
          final int to = (int) ((i * 31L + k * 7919L) % wide);
          if (to != i) {
            v[i].newEdge("W", v[to]);
            wideEdges.add(new int[] { i, to });
          }
        }
    });

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("issue-10539-wide").withVertexTypes("Wide")
        .withEdgeTypes("W").build();
    try {
      final Map<RID, Integer> idByRid = new HashMap<>();
      for (int i = 0; i < wide; i++)
        idByRid.put(wideRids[i], i);
      final int source = view.getNodeId(wideRids[0]);
      for (final int maxDepth : DEPTHS)
        for (final Vertex.DIRECTION dir : Vertex.DIRECTION.values()) {
          final GraphAlgorithms.BFSLevels bfs = GraphAlgorithms.breadthFirst(view, source, maxDepth, dir, "W");
          final Map<Integer, Integer> actual = new HashMap<>();
          for (int i = 0; i < bfs.size(); i++)
            actual.put(idByRid.get(view.getRID(bfs.order()[i])), bfs.depthAt(i));
          assertThat(actual).as("maxDepth=%d dir=%s", maxDepth, dir).isEqualTo(referenceBfs(wideEdges, wide, 0, maxDepth, dir));
        }
    } finally {
      view.drop();
    }
  }

  @Test
  void algoBfsOnAViewAnswersWhatTraverseAnswers() throws Exception {
    final Map<Integer, Map<Integer, Integer>> withoutView = new HashMap<>();
    for (final int maxDepth : DEPTHS)
      withoutView.put(maxDepth, cypherBfs(maxDepth));

    final GraphAnalyticalView view = createView();
    try {
      for (final int maxDepth : DEPTHS) {
        final Map<Integer, Integer> expected = referenceBfs(0, maxDepth, Vertex.DIRECTION.OUT);
        expected.remove(0);
        final Map<Integer, Integer> actual = cypherBfs(maxDepth);
        assertThat(actual).as("maxDepth=%d", maxDepth).isEqualTo(expected);
        assertThat(actual).as("maxDepth=%d, with and without the view", maxDepth).isEqualTo(withoutView.get(maxDepth));
        assertThat(cypherCount(maxDepth)).isEqualTo(expected.size());
        assertThat(traverseCount(maxDepth)).as("TRAVERSE minus the root, maxDepth=%d", maxDepth).isEqualTo(expected.size());
      }
    } finally {
      view.drop();
    }
  }

  @Test
  void algoBfsOnAViewServingPendingChangesStopsAtMaxDepth() throws Exception {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("issue-10539-pending").withVertexTypes("Node")
        .withEdgeTypes("E").withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).withCompactionThreshold(Integer.MAX_VALUE)
        .build();
    try {
      // A new edge out of the start node, held as a pending change of the view
      database.transaction(() -> {
        rids[0].asVertex().modify().newEdge("E", rids[NODES - 1].asVertex()).save();
        edges.add(new int[] { 0, NODES - 1 });
      });
      assertThat(view.hasPendingChanges()).isTrue();

      for (final int maxDepth : DEPTHS) {
        final Map<Integer, Integer> expected = referenceBfs(0, maxDepth, Vertex.DIRECTION.OUT);
        expected.remove(0);
        assertThat(cypherBfs(maxDepth)).as("maxDepth=%d", maxDepth).isEqualTo(expected);
        assertThat(cypherCount(maxDepth)).isEqualTo(expected.size());
      }
    } finally {
      view.drop();
    }
  }

  @Test
  void withoutAViewABoundedCallLoadsOnlyWhatItReaches() {
    for (final int maxDepth : DEPTHS) {
      final Map<Integer, Integer> expected = referenceBfs(0, maxDepth, Vertex.DIRECTION.OUT);
      expected.remove(0);
      assertThat(cypherBfs(maxDepth)).as("maxDepth=%d", maxDepth).isEqualTo(expected);
    }

    // Room for a handful of loaded vertices: loading the whole graph first, as the call used to, is refused, and a bounded
    // walk that reaches a few vertices is not
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
        AbstractAlgoProcedure.OLTP_VERTEX_BYTES * 10);
    try {
      final Map<Integer, Integer> expected = referenceBfs(0, 1, Vertex.DIRECTION.OUT);
      expected.remove(0);
      assertThat(cypherBfs(1)).isEqualTo(expected);
      assertThatThrownBy(() -> cypherBfs(Integer.MAX_VALUE)).hasStackTraceContaining("the visited vertices");
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
          GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
    }
  }

  @Test
  void theVisitEstimateIsAClosedFormThatSaturates() {
    assertThat(AlgoBFS.expectedVisits(3.0, Integer.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
    assertThat(AlgoBFS.expectedVisits(-1.0, 2)).isEqualTo(Long.MAX_VALUE);
    assertThat(AlgoBFS.expectedVisits(3.0, 0)).isZero();
    assertThat(AlgoBFS.expectedVisits(3.0, 2)).isEqualTo(3 + 9);
    assertThat(AlgoBFS.expectedVisits(0.5, 1_000_000)).isEqualTo(1_000_000);
    // A mean just above one and a bound a caller set to two billion: answered at once, not by a loop over the hops
    assertThat(AlgoBFS.expectedVisits(1.000000001, 2_000_000_000)).isPositive();
    assertThat(AlgoBFS.expectedVisits(40.0, 1_000)).isEqualTo(Long.MAX_VALUE);
  }

  private GraphAnalyticalView createView() {
    return GraphAnalyticalView.builder(database).withName("issue-10539").withVertexTypes("Node").withEdgeTypes("E").build();
  }

  private Map<Integer, Integer> cypherBfs(final int maxDepth) {
    final Map<Integer, Integer> depthById = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:Node {id: 0}) CALL algo.bfs(s, 'E', 'OUT', $k) YIELD node, depth RETURN node.id AS id, depth",
        Map.of("k", (long) maxDepth))) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(depthById.put(row.<Number>getProperty("id").intValue(), row.<Number>getProperty("depth").intValue()))
            .as("a node reported twice").isNull();
      }
    }
    return depthById;
  }

  private long cypherCount(final int maxDepth) {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:Node {id: 0}) CALL algo.bfs(s, 'E', 'OUT', $k) YIELD node RETURN count(*) AS c", Map.of("k", (long) maxDepth))) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private long traverseCount(final int maxDepth) {
    final String bound = maxDepth == Integer.MAX_VALUE ? "" : " MAXDEPTH " + maxDepth;
    long count = 0;
    try (final ResultSet rs = database.query("sql",
        "TRAVERSE out('E') FROM " + rids[0] + bound + " STRATEGY BREADTH_FIRST")) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }
    return count - 1;
  }

  private int idOf(final RID rid) {
    for (int i = 0; i < NODES; i++)
      if (rids[i].equals(rid))
        return i;
    throw new IllegalArgumentException("unknown " + rid);
  }

  private Map<Integer, Integer> referenceBfs(final int source, final int maxDepth, final Vertex.DIRECTION dir) {
    return referenceBfs(edges, NODES, source, maxDepth, dir);
  }

  /** A textbook queue BFS over the edge list the test wrote: node id to depth, the source at depth 0. */
  private static Map<Integer, Integer> referenceBfs(final List<int[]> edgeList, final int nodes, final int source, final int maxDepth,
      final Vertex.DIRECTION dir) {
    final List<List<Integer>> adj = new ArrayList<>();
    for (int i = 0; i < nodes; i++)
      adj.add(new ArrayList<>());
    for (final int[] e : edgeList) {
      if (dir != Vertex.DIRECTION.IN)
        adj.get(e[0]).add(e[1]);
      if (dir != Vertex.DIRECTION.OUT)
        adj.get(e[1]).add(e[0]);
    }
    final int[] depth = new int[nodes];
    Arrays.fill(depth, -1);
    depth[source] = 0;
    final ArrayDeque<Integer> queue = new ArrayDeque<>();
    queue.add(source);
    final Map<Integer, Integer> result = new HashMap<>();
    result.put(source, 0);
    while (!queue.isEmpty()) {
      final int v = queue.poll();
      if (depth[v] >= maxDepth)
        continue;
      for (final int u : adj.get(v))
        if (depth[u] < 0) {
          depth[u] = depth[v] + 1;
          result.put(u, depth[u]);
          queue.add(u);
        }
    }
    return result;
  }
}
