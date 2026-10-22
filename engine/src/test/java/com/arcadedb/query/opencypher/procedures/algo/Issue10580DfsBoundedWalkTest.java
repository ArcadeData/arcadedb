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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10580: {@code algo.dfs} paid O(V+E) for every call, whatever {@code maxDepth} was: it loaded every vertex of the
 * database (or took the whole adjacency of the view) and allocated a visited set and two stacks of the node count before the
 * walk started, the same defect #10539 fixed for {@code algo.bfs}. The walk now reads the nodes it reaches and allocates by what
 * it reaches. Every answer is compared, in order and with its depth, with the textbook iterative DFS the procedure always ran,
 * fed the neighbour rows the engine answers for each node.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10580DfsBoundedWalkTest extends TestHelper {
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
   * The iterative DFS {@code algo.dfs} always ran, written out with plain arrays over the full node count: a node is marked when it
   * is pushed, the neighbours are pushed in reverse so the first is popped first, and a node is reported when it is popped, at
   * the depth of the node that pushed it plus one, except the start.
   */
  private static List<Row> referenceDfs(final IntFunction<int[]> neighbors, final int start, final int maxDepth) {
    final boolean[] visited = new boolean[NODES];
    final int[] stack = new int[NODES];
    final int[] stackDepth = new int[NODES];
    final List<Row> result = new ArrayList<>();
    int top = 0;
    stack[top] = start;
    stackDepth[top] = 0;
    top++;
    visited[start] = true;
    while (top > 0) {
      top--;
      final int v = stack[top];
      final int d = stackDepth[top];
      if (d > 0)
        result.add(new Row(v, d));
      if (d < maxDepth) {
        final int[] row = neighbors.apply(v);
        for (int i = row.length - 1; i >= 0; i--) {
          final int u = row[i];
          if (!visited[u]) {
            visited[u] = true;
            stack[top] = u;
            stackDepth[top] = d + 1;
            top++;
          }
        }
      }
    }
    return result;
  }
}
