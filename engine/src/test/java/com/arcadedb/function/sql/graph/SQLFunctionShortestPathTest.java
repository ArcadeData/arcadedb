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
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SQLFunctionShortestPathTest {

  private final Map<Integer, MutableVertex> vertices = new HashMap<>();
  private       SQLFunctionShortestPath     function;

  @Test
  void execute() throws Exception {
    TestHelper.executeInNewDatabase("testExecute", graph -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final List<RID> result = function.execute(null, null, null, new Object[] { vertices.get(1), vertices.get(4) },
          new BasicCommandContext());
      assertThat(result).hasSize(3);
      assertThat(result.getFirst()).isEqualTo(vertices.get(1).getIdentity());
      assertThat(result.get(1)).isEqualTo(vertices.get(3).getIdentity());
      assertThat(result.get(2)).isEqualTo(vertices.get(4).getIdentity());
    });
  }

  @Test
  void executeOut() throws Exception {
    TestHelper.executeInNewDatabase("testExecuteOut", graph -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final List<RID> result = function.execute(null, null, null, new Object[] { vertices.get(1), vertices.get(4), "out", null },
          new BasicCommandContext());

      assertThat(result).hasSize(4);
      assertThat(result.getFirst()).isEqualTo(vertices.get(1).getIdentity());
      assertThat(result.get(1)).isEqualTo(vertices.get(2).getIdentity());
      assertThat(result.get(2)).isEqualTo(vertices.get(3).getIdentity());
      assertThat(result.get(3)).isEqualTo(vertices.get(4).getIdentity());
    });
  }

  @Test
  void executeOnlyEdge1() throws Exception {
    TestHelper.executeInNewDatabase("testExecuteOnlyEdge1", graph -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final List<RID> result = function.execute(null, null, null, new Object[] { vertices.get(1), vertices.get(4), null, "Edge1" },
          new BasicCommandContext());

      assertThat(result).hasSize(4);
      assertThat(result.getFirst()).isEqualTo(vertices.get(1).getIdentity());
      assertThat(result.get(1)).isEqualTo(vertices.get(2).getIdentity());
      assertThat(result.get(2)).isEqualTo(vertices.get(3).getIdentity());
      assertThat(result.get(3)).isEqualTo(vertices.get(4).getIdentity());
    });
  }

  @Test
  void executeOnlyEdge1AndEdge2() throws Exception {
    TestHelper.executeInNewDatabase("testExecuteOnlyEdge1AndEdge2", graph -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final List<RID> result = function.execute(null, null, null,
          new Object[] { vertices.get(1), vertices.get(4), "BOTH", asList("Edge1", "Edge2") }, new BasicCommandContext());

      assertThat(result).hasSize(3);
      assertThat(result.getFirst()).isEqualTo(vertices.get(1).getIdentity());
      assertThat(result.get(1)).isEqualTo(vertices.get(3).getIdentity());
      assertThat(result.get(2)).isEqualTo(vertices.get(4).getIdentity());
    });
  }

  @Test
  void testLong() throws Exception {
    TestHelper.executeInNewDatabase("testLong", graph -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final List<RID> result = function.execute(null, null, null, new Object[] { vertices.get(1), vertices.get(20) },
          new BasicCommandContext());

      assertThat(result).hasSize(11);
      assertThat(result.getFirst()).isEqualTo(vertices.get(1).getIdentity());
      assertThat(result.get(1)).isEqualTo(vertices.get(3).getIdentity());
      int next = 2;
      for (int i = 4; i <= 20; i += 2) {
        assertThat(result.get(next++)).isEqualTo(vertices.get(i).getIdentity());
      }
    });
  }

  @Test
  void maxDepth1() throws Exception {
    TestHelper.executeInNewDatabase("testMaxDepth1", graph -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final Map<String, Object> additionalParams = new HashMap<String, Object>();
      additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 11);
      final List<RID> result = function.execute(null, null, null,
          new Object[] { vertices.get(1), vertices.get(20), null, null, additionalParams }, new BasicCommandContext());

      assertThat(result).hasSize(11);
    });
  }

  @Test
  void maxDepth2() throws Exception {
    TestHelper.executeInNewDatabase("testMaxDepth2", graph -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final Map<String, Object> additionalParams = new HashMap<String, Object>();
      additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 12);
      final List<RID> result = function.execute(null, null, null,
          new Object[] { vertices.get(1), vertices.get(20), null, null, additionalParams }, new BasicCommandContext());

      assertThat(result).hasSize(11);
    });
  }

  @Test
  void maxDepth3() throws Exception {
    TestHelper.executeInNewDatabase("testMaxDepth3", graph -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final Map<String, Object> additionalParams = new HashMap<String, Object>();
      additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 10);
      final List<RID> result = function.execute(null, null, null,
          new Object[] { vertices.get(1), vertices.get(20), null, null, additionalParams }, new BasicCommandContext());

      assertThat(result).isEmpty();
    });
  }

  @Test
  void maxDepth4() throws Exception {
    TestHelper.executeInNewDatabase("testMaxDepth4", graph -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final Map<String, Object> additionalParams = new HashMap<String, Object>();
      additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 3);
      final List<RID> result = function.execute(null, null, null,
          new Object[] { vertices.get(1), vertices.get(20), null, null, additionalParams }, new BasicCommandContext());

      assertThat(result).isEmpty();
    });
  }

  @Test
  void consolidatedOptionsMap() throws Exception {
    TestHelper.executeInNewDatabase("testConsolidatedOptions", graph -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final Map<String, Object> options = new HashMap<>(Map.of(
          "direction", "BOTH",
          "edgeTypeNames", asList("Edge1", "Edge2"),
          "maxDepth", 10));

      final List<RID> result = function.execute(null, null, null,
          new Object[] { vertices.get(1), vertices.get(4), options }, new BasicCommandContext());

      assertThat(result).hasSize(3);
      assertThat(result.getFirst()).isEqualTo(vertices.get(1).getIdentity());
      assertThat(result.get(2)).isEqualTo(vertices.get(4).getIdentity());
    });
  }

  @Test
  void edgeTrueDirectionBothWithAsymmetricEdges() throws Exception {
    TestHelper.executeInNewDatabase("testEdgeBothAsymmetric", graph -> {
      final MutableVertex[] verts = new MutableVertex[2];
      final RID[] edgeRid = new RID[1];

      graph.transaction(() -> {
        graph.getSchema().createVertexType("BugSP_V");
        graph.getSchema().createEdgeType("BugSP_E");

        verts[0] = graph.newVertex("BugSP_V").set("name", "a").save();
        verts[1] = graph.newVertex("BugSP_V").set("name", "b").save();
        edgeRid[0] = verts[0].newEdge("BugSP_E", verts[1]).getIdentity();
      });

      function = new SQLFunctionShortestPath();

      final Map<String, Object> options = new HashMap<>(Map.of(
          "direction", "BOTH",
          "edge", true));

      final List<RID> result = function.execute(null, null, null, new Object[] { verts[0], verts[1], options },
          new BasicCommandContext());

      // expected: [a-rid, edge-rid, b-rid]
      assertThat(result).hasSize(3);
      assertThat(result.getFirst()).isEqualTo(verts[0].getIdentity());
      assertThat(result.get(1)).isEqualTo(edgeRid[0]);
      assertThat(result.getLast()).isEqualTo(verts[1].getIdentity());
    });
  }

  @Test
  void edgeTrueDirectionBothReverseAsymmetric() throws Exception {
    // Mirror of edgeTrueDirectionBothWithAsymmetricEdges: search from the destination back to the source,
    // so the OUT side of the start vertex is empty and the IN half of the fix is exercised.
    TestHelper.executeInNewDatabase("testEdgeBothReverseAsymmetric", graph -> {
      final MutableVertex[] verts = new MutableVertex[2];
      final RID[] edgeRid = new RID[1];

      graph.transaction(() -> {
        graph.getSchema().createVertexType("BugSP_V");
        graph.getSchema().createEdgeType("BugSP_E");

        verts[0] = graph.newVertex("BugSP_V").set("name", "a").save();
        verts[1] = graph.newVertex("BugSP_V").set("name", "b").save();
        edgeRid[0] = verts[0].newEdge("BugSP_E", verts[1]).getIdentity();
      });

      function = new SQLFunctionShortestPath();

      final Map<String, Object> options = new HashMap<>(Map.of(
          "direction", "BOTH",
          "edge", true));

      // search b -> a
      final List<RID> result = function.execute(null, null, null, new Object[] { verts[1], verts[0], options },
          new BasicCommandContext());

      // expected: [b-rid, edge-rid, a-rid]
      assertThat(result).hasSize(3);
      assertThat(result.getFirst()).isEqualTo(verts[1].getIdentity());
      assertThat(result.get(1)).isEqualTo(edgeRid[0]);
      assertThat(result.getLast()).isEqualTo(verts[0].getIdentity());
    });
  }

  @Test
  void edgeTrueDirectionIn() throws Exception {
    // Pure IN traversal with edge:true: from b, follow incoming edges back to a.
    TestHelper.executeInNewDatabase("testEdgeDirectionIn", graph -> {
      final MutableVertex[] verts = new MutableVertex[2];
      final RID[] edgeRid = new RID[1];

      graph.transaction(() -> {
        graph.getSchema().createVertexType("BugSP_V");
        graph.getSchema().createEdgeType("BugSP_E");

        verts[0] = graph.newVertex("BugSP_V").set("name", "a").save();
        verts[1] = graph.newVertex("BugSP_V").set("name", "b").save();
        edgeRid[0] = verts[0].newEdge("BugSP_E", verts[1]).getIdentity();
      });

      function = new SQLFunctionShortestPath();

      final Map<String, Object> options = new HashMap<>(Map.of(
          "direction", "IN",
          "edge", true));

      // a -OUT-> b, so from b the IN edge leads to a
      final List<RID> result = function.execute(null, null, null, new Object[] { verts[1], verts[0], options },
          new BasicCommandContext());

      // expected: [b-rid, edge-rid, a-rid]
      assertThat(result).hasSize(3);
      assertThat(result.getFirst()).isEqualTo(verts[1].getIdentity());
      assertThat(result.get(1)).isEqualTo(edgeRid[0]);
      assertThat(result.getLast()).isEqualTo(verts[0].getIdentity());
    });
  }

  @Test
  void rejectsUnknownOption() throws Exception {
    TestHelper.executeInNewDatabase("testShortestPathUnknownOption", graph -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final Map<String, Object> options = new HashMap<>();
      options.put("whoops", 1);

      assertThatThrownBy(() -> function.execute(null, null, null,
          new Object[] { vertices.get(1), vertices.get(4), options }, new BasicCommandContext()))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("whoops")
          .hasMessageContaining("shortestPath");
    });
  }

  /**
   * Regression: a ghost edge (dangling segment pointer whose backing edge record is gone, as after a
   * manual HA leader-to-follower copy) must be skipped during edge-mode bidirectional search rather
   * than throwing RecordNotFoundException. Graph: A->B is ghosted, A->C->B remains; the search must
   * route around the ghost and return the surviving path.
   */
  @Test
  void edgeModeSkipsGhostEdge() throws Exception {
    TestHelper.executeInNewDatabase("testShortestPathGhostEdge", graph -> {
      final MutableVertex[] v = new MutableVertex[3];
      final RID[] ghost = new RID[1];

      graph.transaction(() -> {
        graph.getSchema().createVertexType("GNode");
        graph.getSchema().createEdgeType("GEdge");

        v[0] = graph.newVertex("GNode").set("name", "A").save();
        v[1] = graph.newVertex("GNode").set("name", "B").save();
        v[2] = graph.newVertex("GNode").set("name", "C").save();

        ghost[0] = v[0].newEdge("GEdge", v[1]).getIdentity(); // A->B, to be ghosted
        v[0].newEdge("GEdge", v[2]);                          // A->C
        v[2].newEdge("GEdge", v[1]);                          // C->B
      });

      // Delete only the A->B edge record, leaving its segment pointer dangling.
      graph.transaction(() -> graph.getSchema().getBucketById(ghost[0].getBucketId()).deleteRecord(ghost[0]));

      function = new SQLFunctionShortestPath();
      final Map<String, Object> options = new HashMap<>(Map.of(
          "direction", "OUT",
          "edge", true));

      final List<RID> result = function.execute(null, null, null, new Object[] { v[0], v[1], options },
          new BasicCommandContext());

      // The ghost A->B is skipped, so the only surviving route is A -[edge]-> C -[edge]-> B, which in
      // edge mode is exactly the 5 elements [A, edge(A->C), C, edge(C->B), B].
      assertThat(result).hasSize(5);
      assertThat(result.getFirst()).isEqualTo(v[0].getIdentity()); // A
      assertThat(result.get(2)).isEqualTo(v[2].getIdentity()); // C (routed around the ghost)
      assertThat(result.get(4)).isEqualTo(v[1].getIdentity()); // B
      assertThat(result).doesNotContain(ghost[0]);
    });
  }

  private void setUpDatabase(final Database graph) {
    graph.transaction(() -> {
      graph.getSchema().createVertexType("Node");
      graph.getSchema().createEdgeType("Edge1");
      graph.getSchema().createEdgeType("Edge2");

      vertices.put(1, graph.newVertex("Node"));
      vertices.put(2, graph.newVertex("Node"));
      vertices.put(3, graph.newVertex("Node"));
      vertices.put(4, graph.newVertex("Node"));

      vertices.get(1).set("node_id", "A");
      vertices.get(2).set("node_id", "B");
      vertices.get(3).set("node_id", "C");
      vertices.get(4).set("node_id", "D");

      vertices.get(1).save();
      vertices.get(2).save();
      vertices.get(3).save();
      vertices.get(4).save();

      vertices.get(1).newEdge("Edge1", vertices.get(2));
      vertices.get(2).newEdge("Edge1", vertices.get(3));
      vertices.get(3).newEdge("Edge2", vertices.get(1));
      vertices.get(3).newEdge("Edge1", vertices.get(4));

      for (int i = 5; i <= 20; i++) {
        vertices.put(i, graph.newVertex("Node"));
        vertices.get(i).set("node_id", "V" + i);
        vertices.get(i).save();

        vertices.get(i - 1).newEdge("Edge1", vertices.get(i));
        if (i % 2 == 0) {
          vertices.get(i - 2).newEdge("Edge1", vertices.get(i));
        }
      }
    });
  }
}
