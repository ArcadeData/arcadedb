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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
class TraverseStatementExecutionTest extends TestHelper {
  @Test
  void plainTraverse() {
    database.transaction(() -> {
      final String classPrefix = "testPlainTraverse_";
      database.getSchema().createVertexType(classPrefix + "V");
      database.getSchema().createEdgeType(classPrefix + "E");
      database.command("sql", "create vertex " + classPrefix + "V set name = 'a'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'b'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'c'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'd'").close();

      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'a') to (select from " + classPrefix
              + "V where name = 'b')").close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'b') to (select from " + classPrefix
              + "V where name = 'c')").close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'c') to (select from " + classPrefix
              + "V where name = 'd')").close();

      final ResultSet result = database.query("sql", "traverse out() from (select from " + classPrefix + "V where name = 'a')");

      for (int i = 0; i < 4; i++) {
        assertThat(result.hasNext()).isTrue();
        final Result item = result.next();
        assertThat(item.getMetadata("$depth")).isEqualTo(i);
      }
      assertThat(result.hasNext()).isFalse();
      result.close();
    });
  }

  @Test
  void withDepth() {
    database.transaction(() -> {
      final String classPrefix = "testWithDepth_";
      database.getSchema().createVertexType(classPrefix + "V");
      database.getSchema().createEdgeType(classPrefix + "E");
      database.command("sql", "create vertex " + classPrefix + "V set name = 'a'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'b'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'c'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'd'").close();

      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'a') to (select from " + classPrefix
              + "V where name = 'b')").close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'b') to (select from " + classPrefix
              + "V where name = 'c')").close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'c') to (select from " + classPrefix
              + "V where name = 'd')").close();

      final ResultSet result = database.query("sql",
          "traverse out() from (select from " + classPrefix + "V where name = 'a') WHILE $depth < 2");

      for (int i = 0; i < 2; i++) {
        assertThat(result.hasNext()).isTrue();
        final Result item = result.next();
        assertThat(item.getMetadata("$depth")).isEqualTo(i);
      }
      assertThat(result.hasNext()).isFalse();
      result.close();
    });
  }

  @Test
  void maxDepth() {
    database.transaction(() -> {
      final String classPrefix = "testMaxDepth";
      database.getSchema().createVertexType(classPrefix + "V");
      database.getSchema().createEdgeType(classPrefix + "E");
      database.command("sql", "create vertex " + classPrefix + "V set name = 'a'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'b'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'c'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'd'").close();

      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'a') to (select from " + classPrefix
              + "V where name = 'b')").close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'b') to (select from " + classPrefix
              + "V where name = 'c')").close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'c') to (select from " + classPrefix
              + "V where name = 'd')").close();

      ResultSet result = database.query("sql",
          "traverse out() from (select from " + classPrefix + "V where name = 'a') MAXDEPTH 1");

      for (int i = 0; i < 2; i++) {
        assertThat(result.hasNext()).isTrue();
        final Result item = result.next();
        assertThat(item.getMetadata("$depth")).isEqualTo(i);
      }
      assertThat(result.hasNext()).isFalse();
      result.close();

      result = database.query("sql", "traverse out() from (select from " + classPrefix + "V where name = 'a') MAXDEPTH 2");

      for (int i = 0; i < 3; i++) {
        assertThat(result.hasNext()).isTrue();
        final Result item = result.next();
        assertThat(item.getMetadata("$depth")).isEqualTo(i);
      }
      assertThat(result.hasNext()).isFalse();
      result.close();
    });
  }

  @Test
  void maxDepthReturnsMonotonicBallOnMultiPathGraph() {
    // Contract: TRAVERSE ... MAXDEPTH d must return the nodes reachable within d hops (the d-hop ball). That set is monotonically non-decreasing in d -- raising
    // the bound can only add nodes, never remove them -- and once d reaches the root's eccentricity the set is the whole reachable graph. This must hold
    // irrespective of edge-insertion order or of multiple paths to the same node.
    //
    // Graph (a DAG -- cycles are NOT needed to trigger the bug): R reaches X by a short path R->a->X (X at true depth 2) and a long path R->b->c->d->X (depth 4),
    // and X fans out to s1..s10. 16 nodes total. True d-hop ball sizes: d2 = 5; d>=3 = 16 (every node). The two R out-edges are created long-branch-first
    // (R->b before R->a) so the depth-first walk pops the long path first and first-reaches X at depth 4.
    //
    // Regression guard for issue #5159. Before the fix DepthFirstTraverseStep recorded a node's depth on FIRST arrival with no relaxation when a shorter path
    // arrived later, and gated subtree expansion on that recorded depth. So at MAXDEPTH 4, X was stamped depth 4 (via the long path), the gate 4 > 4 was false,
    // and X's 10-node subtree was never expanded -> 6 nodes; at MAXDEPTH 3 the long path is cut before X, so X is reached via the short path at depth 2, expanded,
    // and all 16 appear. The result thus DROPPED from 16 (d3) to 6 (d4) before returning to 16 (d5): a non-monotonic under-approximation, now yielding [16,16,16].
    database.transaction(() -> {
      final String v = "MaxDepthBallV";
      final String e = "MaxDepthBallE";
      database.getSchema().createVertexType(v);
      database.getSchema().createEdgeType(e);

      final Map<String, RID> ids = new HashMap<>();
      final String[] names = { "R", "a", "b", "c", "d", "X", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10" };
      for (final String name : names)
        ids.put(name, database.command("sql", "create vertex " + v + " set name = '" + name + "'").next().getIdentity().get());

      // R->b (long branch) is created before R->a (short branch) so the DFS pops the long path to X first -- the exact order that triggered the pre-fix bug. With
      // the relaxation fix the expected [16,16,16] holds for any adjacency iteration order; this order is kept only so the test keeps exercising that scenario.
      final String[][] edges = { { "R", "b" }, { "R", "a" }, { "a", "X" }, { "b", "c" }, { "c", "d" }, { "d", "X" }, //
          { "X", "s1" }, { "X", "s2" }, { "X", "s3" }, { "X", "s4" }, { "X", "s5" }, { "X", "s6" }, { "X", "s7" }, { "X", "s8" }, { "X", "s9" }, { "X", "s10" } };
      final Map<String, Object> params = new HashMap<>();
      for (final String[] edge : edges) {
        params.put("from", ids.get(edge[0]));
        params.put("to", ids.get(edge[1]));
        database.command("sql", "create edge " + e + " from :from to :to", params).close();
      }

      final int d3 = countWithinDepth(ids.get("R"), e, 3);
      final int d4 = countWithinDepth(ids.get("R"), e, 4);
      final int d5 = countWithinDepth(ids.get("R"), e, 5);

      // Every one of MAXDEPTH 3/4/5 must return the full 16-node graph. The bug yields [16, 6, 16] -- MAXDEPTH 4 loses X's whole subtree.
      assertThat(new int[] { d3, d4, d5 })
          .as("d-hop ball sizes at MAXDEPTH 3,4,5 must all be 16; a dip means raising the bound dropped reachable nodes")
          .containsExactly(16, 16, 16);
    });
  }

  private int countWithinDepth(final RID root, final String edgeType, final int maxDepth) {
    final Map<String, Object> params = new HashMap<>();
    params.put("root", root);
    try (final ResultSet rs = database.query("sql",
        "select count(*) as c from (traverse out('" + edgeType + "') from :root MAXDEPTH " + maxDepth + ")", params)) {
      return ((Number) rs.next().getProperty("c")).intValue();
    }
  }

  @Test
  void breadthFirstOrder() {
    // Contract: STRATEGY BREADTH_FIRST must visit the graph level by level -- every node at depth d is emitted before any node at depth d+1. That level-ordering
    // is the definition of a breadth-first traversal and the only observable difference from DEPTH_FIRST. (Sibling order *within* a level is intentionally not
    // asserted: TRAVERSE has no clause to control expansion order -- SQLParser.g4:217, only MAXDEPTH/WHILE/LIMIT/STRATEGY -- and BFS does not guarantee it.) On
    // the balanced binary tree below (1 -> {2,3}; 2 -> {4,5}; 3 -> {6,7}) the emitted $depth sequence of a correct BFS is therefore exactly [0,1,1,2,2,2,2] for
    // any sibling order. A linear chain cannot catch a violation because BFS and DFS emit identically on it; distinguishing the two strategies requires two
    // branches of equal depth. depthFirstOrder() is the DFS counterpart on the same tree -- the two together pin the strategies apart.
    database.transaction(() -> {
      final String v = "BfsLevelV";
      final String e = "BfsLevelE";
      database.getSchema().createVertexType(v);
      database.getSchema().createEdgeType(e);

      final Map<String, RID> ids = new HashMap<>();
      for (int n = 1; n <= 7; n++)
        ids.put("" + n, database.command("sql", "create vertex " + v + " set name = '" + n + "'").next().getIdentity().get());

      final int[][] edges = { { 1, 2 }, { 1, 3 }, { 2, 4 }, { 2, 5 }, { 3, 6 }, { 3, 7 } };
      final Map<String, Object> params = new HashMap<>();
      for (final int[] edge : edges) {
        params.put("from", ids.get("" + edge[0]));
        params.put("to", ids.get("" + edge[1]));
        database.command("sql", "create edge " + e + " from :from to :to", params).close();
      }

      params.clear();
      params.put("root", ids.get("1"));
      final List<String> names = new ArrayList<>();
      final List<Integer> depths = new ArrayList<>();
      try (final ResultSet result = database.query("sql",
          "traverse out('" + e + "') from :root STRATEGY BREADTH_FIRST", params)) {
        while (result.hasNext()) {
          final Result item = result.next();
          names.add(item.getProperty("name"));
          depths.add((Integer) item.getMetadata("$depth"));
        }
      }

      // Completeness: every node is visited exactly once (the defect is order, not which nodes are reached).
      assertThat(names).containsExactlyInAnyOrder("1", "2", "3", "4", "5", "6", "7");
      // Level ordering: the depth sequence rises monotonically by level. The DFS stack underlying BREADTH_FIRST instead emits [0,1,2,2,1,2,2], diving into node
      // 2's children before visiting node 3.
      assertThat(depths).containsExactly(0, 1, 1, 2, 2, 2, 2);
    });
  }

  @Test
  void depthFirstOrder() {
    // Contract: STRATEGY DEPTH_FIRST (also the default when no STRATEGY is given) must fully explore one child's subtree before moving to the next sibling -- so
    // it descends to depth 2 and back up before visiting the second depth-1 node. This is the mirror of breadthFirstOrder() on the same balanced binary tree
    // (1 -> {2,3}; 2 -> {4,5}; 3 -> {6,7}): a correct DFS emits the $depth sequence [0,1,2,2,1,2,2] (one subtree drained fully, then the other) for any sibling
    // order, versus BFS's [0,1,1,2,2,2,2]. It is exactly this dive-before-breadth shape that BREADTH_FIRST was wrongly producing before the fix. As in the BFS
    // test, sibling order within the tree is not asserted (TRAVERSE exposes no expansion-order clause).
    database.transaction(() -> {
      final String v = "DfsLevelV";
      final String e = "DfsLevelE";
      database.getSchema().createVertexType(v);
      database.getSchema().createEdgeType(e);

      final Map<String, RID> ids = new HashMap<>();
      for (int n = 1; n <= 7; n++)
        ids.put("" + n, database.command("sql", "create vertex " + v + " set name = '" + n + "'").next().getIdentity().get());

      final int[][] edges = { { 1, 2 }, { 1, 3 }, { 2, 4 }, { 2, 5 }, { 3, 6 }, { 3, 7 } };
      final Map<String, Object> params = new HashMap<>();
      for (final int[] edge : edges) {
        params.put("from", ids.get("" + edge[0]));
        params.put("to", ids.get("" + edge[1]));
        database.command("sql", "create edge " + e + " from :from to :to", params).close();
      }

      params.clear();
      params.put("root", ids.get("1"));
      final List<String> names = new ArrayList<>();
      final List<Integer> depths = new ArrayList<>();
      try (final ResultSet result = database.query("sql",
          "traverse out('" + e + "') from :root STRATEGY DEPTH_FIRST", params)) {
        while (result.hasNext()) {
          final Result item = result.next();
          names.add(item.getProperty("name"));
          depths.add((Integer) item.getMetadata("$depth"));
        }
      }

      // Completeness: every node visited exactly once.
      assertThat(names).containsExactlyInAnyOrder("1", "2", "3", "4", "5", "6", "7");
      // Depth-first ordering: one level-1 subtree is drained to depth 2 before the other level-1 node is visited.
      assertThat(depths).containsExactly(0, 1, 2, 2, 1, 2, 2);
    });
  }

  @Test
  void traverseInBatchTx() {
    database.transaction(() -> {
      String script = """
          drop type testTraverseInBatchTx_V if exists unsafe;
          create vertex type testTraverseInBatchTx_V;
          create property testTraverseInBatchTx_V.name STRING;
          drop type testTraverseInBatchTx_E if exists unsafe;
          create edge type testTraverseInBatchTx_E;
          begin;
          insert into testTraverseInBatchTx_V(name) values ('a'), ('b'), ('c');
          create edge testTraverseInBatchTx_E from (select from testTraverseInBatchTx_V where name = 'a') to (select from testTraverseInBatchTx_V where name = 'b');
          create edge testTraverseInBatchTx_E from (select from testTraverseInBatchTx_V where name = 'b') to (select from testTraverseInBatchTx_V where name = 'c');
          let top = (select * from (traverse in('testTraverseInBatchTx_E') from (select from testTraverseInBatchTx_V where name='c')) where in('testTraverseInBatchTx_E').size() == 0);
          commit;
          return $top;
          """;
      final ResultSet result = database.command("sqlscript", script);
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
      result.close();
    });
  }

  @Test
  void branchingTreeTraverse() {
    // Regression test for the ArrayList->ArrayDeque swap on entryPoints. A shallow but wide tree stresses the deque depth more than any linear chain covered by
    // the other tests; with the old ArrayList the addFirst/removeFirst hot path was O(n) and the full traversal was O(n^2), which made non-trivial subgraphs
    // visibly slow even though the visited count was small. Correctness (total visited, per-depth count) must be unchanged.
    database.transaction(() -> {
      final String v = "BranchingTreeV";
      final String e = "BranchingTreeE";
      database.getSchema().createVertexType(v);
      database.getSchema().createEdgeType(e);

      final int fanout = 20;
      final RID root = database.command("sql", "create vertex " + v + " set name = 'root'").next().getIdentity().get();
      final Map<String, Object> params = new HashMap<>();
      params.put("from", root);

      int expectedTotal = 1;
      for (int i = 0; i < fanout; i++) {
        final RID child = database.command("sql", "create vertex " + v + " set name = 'c" + i + "'").next().getIdentity().get();
        params.put("to", child);
        database.command("sql", "create edge " + e + " from :from to :to", params).close();
        expectedTotal++;
        for (int j = 0; j < fanout; j++) {
          final RID grand = database.command("sql", "create vertex " + v + " set name = 'g" + i + "_" + j + "'").next().getIdentity().get();
          params.put("from", child);
          params.put("to", grand);
          database.command("sql", "create edge " + e + " from :from to :to", params).close();
          expectedTotal++;
        }
        params.put("from", root);
      }

      params.clear();
      params.put("root", root);
      try (final ResultSet rs = database.query("sql", "select count(*) as c from (traverse out('" + e + "') from :root)", params)) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(expectedTotal);
      }
    });
  }

  @Test
  void postFilterPushdownByType() {
    // Outer `SELECT ... FROM (TRAVERSE ...) WHERE @type = 'X'` should evaluate the @type filter inside the traverse step, so non-matching vertices are visited
    // (to keep expansion correct) but not emitted. Correctness is what this test guards; the perf win is that the outer SubQueryStep no longer shuttles every
    // intermediate vertex through a FilterStep.
    database.transaction(() -> {
      final String folder = "PushdownFolderV";
      final String leaf = "PushdownLeafV";
      final String edge = "PushdownE";
      database.getSchema().createVertexType(folder);
      database.getSchema().createVertexType(leaf);
      database.getSchema().createEdgeType(edge);

      final RID root = database.command("sql", "create vertex " + folder + " set name = 'root'").next().getIdentity().get();
      final Map<String, Object> params = new HashMap<>();
      int expectedLeaves = 0;
      for (int i = 0; i < 5; i++) {
        final RID sub = database.command("sql", "create vertex " + folder + " set name = 'sub" + i + "'").next().getIdentity().get();
        params.clear();
        params.put("from", root);
        params.put("to", sub);
        database.command("sql", "create edge " + edge + " from :from to :to", params).close();
        for (int j = 0; j < 4; j++) {
          final RID l = database.command("sql", "create vertex " + leaf + " set name = 'leaf" + i + "_" + j + "'").next().getIdentity().get();
          params.clear();
          params.put("from", sub);
          params.put("to", l);
          database.command("sql", "create edge " + edge + " from :from to :to", params).close();
          expectedLeaves++;
        }
      }

      params.clear();
      params.put("root", root);
      try (final ResultSet rs = database.query("sql",
          "select @rid as rid, @type as t from (traverse out('" + edge + "') from :root) where @type = '" + leaf + "'", params)) {
        int count = 0;
        while (rs.hasNext()) {
          final Result r = rs.next();
          assertThat((String) r.getProperty("t")).isEqualTo(leaf);
          count++;
        }
        assertThat(count).isEqualTo(expectedLeaves);
      }

      // Count form - the pushdown should not change count-aggregation semantics.
      try (final ResultSet rs = database.query("sql",
          "select count(*) as c from (traverse out('" + edge + "') from :root) where @type = '" + leaf + "'", params)) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(expectedLeaves);
      }

      // Safety: when outer WHERE references a LET variable, pushdown must NOT fire (because the LET lives in the outer scope) and behavior must still be
      // correct.
      try (final ResultSet rs = database.query("sql",
          "select count(*) as c from (traverse out('" + edge + "') from :root) let $t = '" + leaf + "' where @type = $t", params)) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(expectedLeaves);
      }
    });
  }

  @Test
  void traverseFromRID() {
    database.command("sql", "CREATE VERTEX TYPE TVtx IF NOT EXISTS");
    database.command("sql", "CREATE EDGE TYPE TEdg IF NOT EXISTS");

    database.transaction(() -> {
      RID newVtx0Id = database.command("sql", "CREATE VERTEX TVtx").next().getIdentity().get();
      RID newVtx1Id = database.command("sql", "CREATE VERTEX TVtx").next().getIdentity().get();

      Map<String, Object> params = new HashMap<>();
      params.put("fromRid", newVtx0Id);
      params.put("toRid", newVtx1Id);
      RID newEdgRid = database.command("sql", "CREATE EDGE TEdg FROM :fromRid TO :toRid", params).next().getIdentity().get();

      params.clear();
      params.put("rid", newVtx0Id);
      String traverseQuery = "SELECT FROM (TRAVERSE out('TEdg') FROM :rid MAXDEPTH 1)";
      //This also does not recognize RID parameter
      //String traverseQuery="SELECT FROM (TRAVERSE inV() FROM :rid MAXDEPTH 1)";
      database.command("sql", traverseQuery, params);
    });
  }
}
