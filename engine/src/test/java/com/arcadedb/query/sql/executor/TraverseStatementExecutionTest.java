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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
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

  @Disabled("KNOWN BUG (not fixed on this branch): TRAVERSE ... MAXDEPTH returns a non-monotonic under-approximation of the d-hop ball -- on the graph below it "
      + "yields 16 nodes at MAXDEPTH 3, 6 at MAXDEPTH 4, then 16 again at MAXDEPTH 5. Remove @Disabled to reproduce the failure. Root cause: DepthFirstTraverseStep "
      + "records depth at first arrival with no relaxation and gates subtree expansion on it.")
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
    // This is left as a FAILING test documenting a defect (not fixed here). DepthFirstTraverseStep records a node's depth on FIRST arrival with no relaxation
    // when a shorter path arrives later, and gates subtree expansion on that recorded depth. So at MAXDEPTH 4, X is stamped depth 4 (via the long path), the
    // gate 4 > 4 is false, and X's 10-node subtree is never expanded -> 6 nodes; at MAXDEPTH 3 the long path is cut before X, so X is reached via the short path
    // at depth 2, expanded, and all 16 appear. The result thus DROPS from 16 (d3) to 6 (d4) before returning to 16 (d5): a non-monotonic under-approximation.
    database.transaction(() -> {
      final String v = "MaxDepthBallV";
      final String e = "MaxDepthBallE";
      database.getSchema().createVertexType(v);
      database.getSchema().createEdgeType(e);

      final Map<String, RID> ids = new HashMap<>();
      final String[] names = { "R", "a", "b", "c", "d", "X", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10" };
      for (final String name : names)
        ids.put(name, database.command("sql", "create vertex " + v + " set name = '" + name + "'").next().getIdentity().get());

      // Order matters: R->b (long branch) is created before R->a (short branch) so the DFS pops the long path to X first. If a future build changes adjacency
      // iteration order, swap these two to keep the long branch first.
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
  void breadthFirst() {
    database.transaction(() -> {
      final String classPrefix = "testBreadthFirst_";
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
          "traverse out() from (select from " + classPrefix + "V where name = 'a') STRATEGY BREADTH_FIRST");

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
