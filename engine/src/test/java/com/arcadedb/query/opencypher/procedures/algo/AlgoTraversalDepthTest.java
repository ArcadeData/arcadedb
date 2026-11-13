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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code algo.bfs} and {@code algo.dfs} with a {@code maxDepth}: the walk is bounded by it and reaches every node within it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoTraversalDepthTest {
  /**
   * Issue #10539: {@code algo.bfs} on a Graph Analytical View ran the BFS kernel over the whole reachable graph and filtered
   * by {@code maxDepth} afterwards, and the paths without a ready view loaded the whole graph before walking it. Every path
   * now stops at {@code maxDepth} and answers exactly what the unbounded search filtered by depth answered.
   */
  @Nested
  class BfsHonoursMaxDepth extends TestHelper {
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
    void aNegativeBoundAndAStartWithoutEdgesReachOnlyTheStart() throws Exception {
      final RID[] lonely = new RID[1];
      database.transaction(() -> lonely[0] = database.newVertex("Node").set("id", -1).save().getIdentity());

      // Without a view: the record walk
      assertThat(cypherBfs(0, -1)).isEmpty();
      assertThat(cypherBfs(-1, 3)).isEmpty();

      final GraphAnalyticalView view = createView();
      try {
        final int source = view.getNodeId(rids[0]);
        final GraphAlgorithms.BFSLevels negative = GraphAlgorithms.breadthFirst(view, source, -1, Vertex.DIRECTION.OUT, "E");
        assertThat(negative.size()).isEqualTo(1);
        assertThat(negative.levels()).isEqualTo(1);

        final int lonelySource = view.getNodeId(lonely[0]);
        for (final Vertex.DIRECTION dir : Vertex.DIRECTION.values()) {
          final GraphAlgorithms.BFSLevels alone = GraphAlgorithms.breadthFirst(view, lonelySource, Integer.MAX_VALUE, dir, "E");
          assertThat(alone.size()).as("dir=%s", dir).isEqualTo(1);
          assertThat(alone.levels()).as("dir=%s", dir).isEqualTo(1);
          assertThat(alone.order()[0]).isEqualTo(lonelySource);
        }

        // On the view: the kernel through the procedure
        assertThat(cypherBfs(0, -1)).isEmpty();
        assertThat(cypherBfs(-1, 3)).isEmpty();
        assertThat(cypherBfs(-1, Integer.MAX_VALUE)).isEmpty();
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
      return cypherBfs(0, maxDepth);
    }

    private Map<Integer, Integer> cypherBfs(final int startId, final int maxDepth) {
      final Map<Integer, Integer> depthById = new HashMap<>();
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (s:Node {id: $start}) CALL algo.bfs(s, 'E', 'OUT', $k) YIELD node, depth RETURN node.id AS id, depth",
          Map.of("start", startId, "k", (long) maxDepth))) {
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

  /**
   * Issue #10580: {@code algo.dfs} paid O(V+E) for every call, whatever {@code maxDepth} was: it loaded every vertex of the
   * database (or took the whole adjacency of the view) and allocated a visited set and two stacks of the node count before the
   * walk started, the same defect #10539 fixed for {@code algo.bfs}. The walk now reads the nodes it reaches and allocates by what
   * it reaches. Every answer is compared, in order and with its depth, with the textbook iterative DFS the procedure always ran,
   * fed the neighbour rows the engine answers for each node.
   */
  @Nested
  class DfsBoundedWalk extends TestHelper {
    private static final int   NODES  = 300;
    private static final int[] DEPTHS = { 0, 1, 2, 3, Integer.MAX_VALUE };

    private final RID[] rids = new RID[NODES];

    /** A node reported by the walk, by its test id, with the depth it was reported at. */
    private record Row(int id, int depth) {
    }

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
            v[i].newEdge("E", v[i + 1]).save();
          final int shortcut = (i * 7 + 3) % NODES;
          if (shortcut != i && shortcut != i + 1)
            v[i].newEdge("E", v[shortcut]).save();
        }
      });
    }

    @Test
    void withoutAViewTheAnswerIsTheTextbookDfsOverTheRecords() {
      for (final Vertex.DIRECTION dir : Vertex.DIRECTION.values())
        for (final int maxDepth : DEPTHS)
          assertThat(cypherDfs(0, dir, maxDepth)).as("maxDepth=%d dir=%s", maxDepth, dir)
              .isEqualTo(referenceDfs(recordNeighbors(dir), 0, maxDepth));
    }

    @Test
    void withAViewTheAnswerIsTheTextbookDfsOverTheViewRows() {
      final GraphAnalyticalView view = createView(GraphAnalyticalView.UpdateMode.OFF);
      try {
        for (final Vertex.DIRECTION dir : Vertex.DIRECTION.values())
          for (final int maxDepth : DEPTHS)
            assertThat(cypherDfs(0, dir, maxDepth)).as("maxDepth=%d dir=%s", maxDepth, dir)
                .isEqualTo(referenceDfs(viewNeighbors(view, dir), 0, maxDepth));
      } finally {
        view.drop();
      }
    }

    @Test
    void withAViewServingPendingChangesTheAnswerIsTheTextbookDfsOverTheViewRows() {
      final GraphAnalyticalView view = createView(GraphAnalyticalView.UpdateMode.SYNCHRONOUS);
      try {
        // A new edge out of the start node and one into it, held as pending changes of the view
        database.transaction(() -> {
          rids[0].asVertex().modify().newEdge("E", rids[NODES - 1].asVertex()).save();
          rids[NODES - 2].asVertex().modify().newEdge("E", rids[0].asVertex()).save();
        });
        assertThat(view.hasPendingChanges()).isTrue();

        for (final Vertex.DIRECTION dir : Vertex.DIRECTION.values())
          for (final int maxDepth : DEPTHS)
            assertThat(cypherDfs(0, dir, maxDepth)).as("maxDepth=%d dir=%s", maxDepth, dir)
                .isEqualTo(referenceDfs(viewNeighbors(view, dir), 0, maxDepth));
      } finally {
        view.drop();
      }
    }

    @Test
    void parallelEdgesAndSelfLoopsAreWalkedInTheOrderTheEarlierAlgorithmWalkedThem() {
      // A node met twice in one row is pushed where its last entry is, and a self-loop names a node already visited
      database.transaction(() -> {
        rids[1].asVertex().modify().newEdge("E", rids[2].asVertex()).save();
        rids[1].asVertex().modify().newEdge("E", rids[2].asVertex()).save();
        rids[5].asVertex().modify().newEdge("E", rids[6].asVertex()).save();
        rids[7].asVertex().modify().newEdge("E", rids[7].asVertex()).save();
        rids[0].asVertex().modify().newEdge("E", rids[0].asVertex()).save();
        rids[3].asVertex().modify().newEdge("E", rids[1].asVertex()).save();
      });

      for (final Vertex.DIRECTION dir : Vertex.DIRECTION.values())
        for (final int maxDepth : DEPTHS)
          assertThat(cypherDfs(0, dir, maxDepth)).as("no view, maxDepth=%d dir=%s", maxDepth, dir)
              .isEqualTo(referenceDfs(recordNeighbors(dir), 0, maxDepth));

      final GraphAnalyticalView view = createView(GraphAnalyticalView.UpdateMode.OFF);
      try {
        for (final Vertex.DIRECTION dir : Vertex.DIRECTION.values())
          for (final int maxDepth : DEPTHS)
            assertThat(cypherDfs(0, dir, maxDepth)).as("view, maxDepth=%d dir=%s", maxDepth, dir)
                .isEqualTo(referenceDfs(viewNeighbors(view, dir), 0, maxDepth));
      } finally {
        view.drop();
      }
    }

    @Test
    void aCountOnlyCallAnswersTheSizeOfTheWalk() {
      for (final int maxDepth : DEPTHS) {
        final long expected = referenceDfs(recordNeighbors(Vertex.DIRECTION.OUT), 0, maxDepth).size();
        assertThat(cypherCount(maxDepth)).as("maxDepth=%d, no view", maxDepth).isEqualTo(expected);
      }
      final GraphAnalyticalView view = createView(GraphAnalyticalView.UpdateMode.OFF);
      try {
        for (final int maxDepth : DEPTHS)
          assertThat(cypherCount(maxDepth)).as("maxDepth=%d, view", maxDepth)
              .isEqualTo(referenceDfs(viewNeighbors(view, Vertex.DIRECTION.OUT), 0, maxDepth).size());
      } finally {
        view.drop();
      }
    }

    @Test
    void aNegativeBoundAndAStartWithoutEdgesReachNothing() {
      final RID[] lonely = new RID[1];
      database.transaction(() -> lonely[0] = database.newVertex("Node").set("id", -1).save().getIdentity());

      assertThat(cypherDfs(0, Vertex.DIRECTION.OUT, -1)).isEmpty();
      assertThat(cypherDfs(-1, Vertex.DIRECTION.BOTH, 3)).isEmpty();
      assertThat(cypherDfs(-1, Vertex.DIRECTION.BOTH, Integer.MAX_VALUE)).isEmpty();
    }

    @Test
    void withoutAViewABoundedCallLoadsOnlyWhatItReaches() {
      // Room for a handful of loaded vertices: loading the whole graph first, as the call used to, is refused, and a bounded
      // walk that reaches a few vertices is not
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
          AbstractAlgoProcedure.OLTP_VERTEX_BYTES * 10);
      try {
        assertThat(cypherDfs(0, Vertex.DIRECTION.OUT, 1)).isEqualTo(referenceDfs(recordNeighbors(Vertex.DIRECTION.OUT), 0, 1));
        assertThatThrownBy(() -> cypherDfs(0, Vertex.DIRECTION.OUT, Integer.MAX_VALUE)).hasStackTraceContaining("the visited vertices");
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
            GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
      }
    }

    @Test
    void withAViewABoundedCallCopiesNoAdjacencyAndAllocatesByWhatItReaches() {
      final GraphAnalyticalView view = createView(GraphAnalyticalView.UpdateMode.OFF);
      // Less than the copy of the adjacency of the whole view costs (a row header per node alone is 300 * 32 bytes): the call used
      // to be refused with "the adjacency list" before it walked a single node
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
          AbstractAlgoProcedure.OLTP_VERTEX_BYTES * 10);
      try {
        assertThat(cypherDfs(0, Vertex.DIRECTION.OUT, 1)).isEqualTo(referenceDfs(viewNeighbors(view, Vertex.DIRECTION.OUT), 0, 1));
        assertThat(cypherDfs(0, Vertex.DIRECTION.OUT, 2)).isEqualTo(referenceDfs(viewNeighbors(view, Vertex.DIRECTION.OUT), 0, 2));
        // A walk that reaches the whole graph still has a working set to pay for, and is refused by the budget that bounds it
        assertThatThrownBy(() -> cypherDfs(0, Vertex.DIRECTION.OUT, Integer.MAX_VALUE)).hasStackTraceContaining("the traversal state");
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
            GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
        view.drop();
      }
    }

    private GraphAnalyticalView createView(final GraphAnalyticalView.UpdateMode mode) {
      return GraphAnalyticalView.builder(database).withName("issue-10580").withVertexTypes("Node").withEdgeTypes("E")
          .withUpdateMode(mode).withCompactionThreshold(Integer.MAX_VALUE).build();
    }

    /** Node ids and depths in the order the procedure returned them. */
    private List<Row> cypherDfs(final int startId, final Vertex.DIRECTION dir, final int maxDepth) {
      final List<Row> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (s:Node {id: $start}) CALL algo.dfs(s, 'E', $dir, $k) YIELD node, depth RETURN node.id AS id, depth",
          Map.of("start", startId, "dir", dir.name(), "k", (long) maxDepth))) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          rows.add(new Row(row.<Number>getProperty("id").intValue(), row.<Number>getProperty("depth").intValue()));
        }
      }
      return rows;
    }

    private long cypherCount(final int maxDepth) {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (s:Node {id: 0}) CALL algo.dfs(s, 'E', 'OUT', $k) YIELD node RETURN count(*) AS c", Map.of("k", (long) maxDepth))) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    private Map<RID, Integer> idByRid() {
      final Map<RID, Integer> map = new HashMap<>();
      for (int i = 0; i < NODES; i++)
        map.put(rids[i], i);
      return map;
    }

    /** The neighbour row of every node as the records answer it, in test ids. */
    private IntFunction<int[]> recordNeighbors(final Vertex.DIRECTION dir) {
      final Map<RID, Integer> ids = idByRid();
      return node -> {
        final List<Integer> row = new ArrayList<>();
        for (final Vertex neighbor : rids[node].asVertex().getVertices(dir, "E"))
          row.add(ids.get(neighbor.getIdentity()));
        return row.stream().mapToInt(Integer::intValue).toArray();
      };
    }

    /** The neighbour row of every node as the view answers it, in test ids. */
    private IntFunction<int[]> viewNeighbors(final GraphAnalyticalView view, final Vertex.DIRECTION dir) {
      final Map<RID, Integer> ids = idByRid();
      return node -> {
        final int[] row = view.getNeighborIds(view.getNodeId(rids[node]), dir, "E");
        final int[] mapped = new int[row.length];
        for (int i = 0; i < row.length; i++)
          mapped[i] = ids.get(view.getRID(row[i]));
        return mapped;
      };
    }

    /**
     * The iterative DFS {@code algo.dfs} runs, written out with plain arrays over the full node count: a node is pushed when it is
     * first met, the neighbours are pushed in reverse so the first is popped first, and a node is reported when it is first
     * popped, at the depth of the node that pushed it plus one, except the start. Since issue #10622 a bounded walk also pushes a
     * node again when it meets it at a smaller depth than before, skips an entry overtaken that way, and does not push a node
     * reported already that the bound would not expand.
     */
    private static List<Row> referenceDfs(final IntFunction<int[]> neighbors, final int start, final int maxDepth) {
      final boolean bounded = maxDepth != Integer.MAX_VALUE;
      final int[] best = new int[NODES];
      Arrays.fill(best, Integer.MAX_VALUE);
      final boolean[] reported = new boolean[NODES];
      final List<int[]> stack = new ArrayList<>();
      final List<Row> result = new ArrayList<>();
      stack.add(new int[] { start, 0 });
      best[start] = 0;
      reported[start] = true;
      while (!stack.isEmpty()) {
        final int[] entry = stack.remove(stack.size() - 1);
        final int v = entry[0];
        final int d = entry[1];
        if (d > best[v])
          continue;
        if (!reported[v]) {
          reported[v] = true;
          result.add(new Row(v, d));
        }
        if (d < maxDepth) {
          final int[] row = neighbors.apply(v);
          for (int i = row.length - 1; i >= 0; i--) {
            final int u = row[i];
            final boolean firstMet = best[u] == Integer.MAX_VALUE;
            if (firstMet || (bounded && d + 1 < best[u])) {
              best[u] = d + 1;
              if (!firstMet && reported[u] && d + 1 == maxDepth)
                continue;
              stack.add(new int[] { u, d + 1 });
            }
          }
        }
      }
      return result;
    }
  }

  /**
   * Issue #10622: {@code algo.dfs} with a {@code maxDepth} left out nodes reachable within the bound. A node was marked when it
   * was pushed and expanded only while its depth was below the bound, so a node first reached by a long path at the bound was
   * never expanded, the shorter path to it found it marked already, and what it led to was lost - which nodes depended on the
   * order the neighbours were read. A bounded walk now expands a node again when it reaches it at a smaller depth, so it reports
   * every node within the bound, exactly the nodes {@code algo.bfs} reports for the same bound.
   */
  @Nested
  class DfsMaxDepthReach extends TestHelper {

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
}
