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
package com.arcadedb.query.sql.functions.graph;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.function.graph.SQLFunctionShortestPath;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLFunctionShortestPathTest {

  private final Map<Integer, MutableVertex> vertices = new HashMap<>();
  private       SQLFunctionShortestPath     function;

  @Test
  public void testExecute() throws Exception {
    TestHelper.executeInNewDatabase("testExecute", (graph) -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final List<RID> result = function.execute(null, null, null, new Object[] { vertices.get(1), vertices.get(4) }, new BasicCommandContext());
      assertEquals(3, result.size());
      assertEquals(vertices.get(1).getIdentity(), result.get(0));
      assertEquals(vertices.get(3).getIdentity(), result.get(1));
      assertEquals(vertices.get(4).getIdentity(), result.get(2));
    });
  }

  @Test
  public void testExecuteOut() throws Exception {
    TestHelper.executeInNewDatabase("testExecuteOut", (graph) -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final List<RID> result = function.execute(null, null, null, new Object[] { vertices.get(1), vertices.get(4), "out", null }, new BasicCommandContext());

      assertEquals(4, result.size());
      assertEquals(vertices.get(1).getIdentity(), result.get(0));
      assertEquals(vertices.get(2).getIdentity(), result.get(1));
      assertEquals(vertices.get(3).getIdentity(), result.get(2));
      assertEquals(vertices.get(4).getIdentity(), result.get(3));
    });
  }

  @Test
  public void testExecuteOnlyEdge1() throws Exception {
    TestHelper.executeInNewDatabase("testExecuteOnlyEdge1", (graph) -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final List<RID> result = function.execute(null, null, null, new Object[] { vertices.get(1), vertices.get(4), null, "Edge1" }, new BasicCommandContext());

      assertEquals(4, result.size());
      assertEquals(vertices.get(1).getIdentity(), result.get(0));
      assertEquals(vertices.get(2).getIdentity(), result.get(1));
      assertEquals(vertices.get(3).getIdentity(), result.get(2));
      assertEquals(vertices.get(4).getIdentity(), result.get(3));
    });
  }

  @Test
  public void testExecuteOnlyEdge1AndEdge2() throws Exception {
    TestHelper.executeInNewDatabase("testExecuteOnlyEdge1AndEdge2", (graph) -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final List<RID> result = function
          .execute(null, null, null, new Object[] { vertices.get(1), vertices.get(4), "BOTH", asList("Edge1", "Edge2") }, new BasicCommandContext());

      assertEquals(3, result.size());
      assertEquals(vertices.get(1).getIdentity(), result.get(0));
      assertEquals(vertices.get(3).getIdentity(), result.get(1));
      assertEquals(vertices.get(4).getIdentity(), result.get(2));
    });
  }

  @Test
  public void testLong() throws Exception {
    TestHelper.executeInNewDatabase("testLong", (graph) -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      final List<RID> result = function.execute(null, null, null, new Object[] { vertices.get(1), vertices.get(20) }, new BasicCommandContext());

      assertEquals(11, result.size());
      assertEquals(vertices.get(1).getIdentity(), result.get(0));
      assertEquals(vertices.get(3).getIdentity(), result.get(1));
      int next = 2;
      for (int i = 4; i <= 20; i += 2) {
        assertEquals(vertices.get(i).getIdentity(), result.get(next++));
      }
    });
  }

  @Test
  public void testMaxDepth1() throws Exception {
    TestHelper.executeInNewDatabase("testMaxDepth1", (graph) -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      Map<String, Object> additionalParams = new HashMap<String, Object>();
      additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 11);
      final List<RID> result = function
          .execute(null, null, null, new Object[] { vertices.get(1), vertices.get(20), null, null, additionalParams }, new BasicCommandContext());

      assertEquals(11, result.size());
    });
  }

  @Test
  public void testMaxDepth2() throws Exception {
    TestHelper.executeInNewDatabase("testMaxDepth2", (graph) -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      Map<String, Object> additionalParams = new HashMap<String, Object>();
      additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 12);
      final List<RID> result = function
          .execute(null, null, null, new Object[] { vertices.get(1), vertices.get(20), null, null, additionalParams }, new BasicCommandContext());

      assertEquals(11, result.size());
    });
  }

  @Test
  public void testMaxDepth3() throws Exception {
    TestHelper.executeInNewDatabase("testMaxDepth3", (graph) -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      Map<String, Object> additionalParams = new HashMap<String, Object>();
      additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 10);
      final List<RID> result = function
          .execute(null, null, null, new Object[] { vertices.get(1), vertices.get(20), null, null, additionalParams }, new BasicCommandContext());

      assertEquals(0, result.size());
    });
  }

  @Test
  public void testMaxDepth4() throws Exception {
    TestHelper.executeInNewDatabase("testMaxDepth4", (graph) -> {
      setUpDatabase(graph);
      function = new SQLFunctionShortestPath();

      Map<String, Object> additionalParams = new HashMap<String, Object>();
      additionalParams.put(SQLFunctionShortestPath.PARAM_MAX_DEPTH, 3);
      final List<RID> result = function
          .execute(null, null, null, new Object[] { vertices.get(1), vertices.get(20), null, null, additionalParams }, new BasicCommandContext());

      assertEquals(0, result.size());
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

      vertices.get(1).newEdge("Edge1", vertices.get(2), true);
      vertices.get(2).newEdge("Edge1", vertices.get(3), true);
      vertices.get(3).newEdge("Edge2", vertices.get(1), true);
      vertices.get(3).newEdge("Edge1", vertices.get(4), true);

      for (int i = 5; i <= 20; i++) {
        vertices.put(i, graph.newVertex("Node"));
        vertices.get(i).set("node_id", "V" + i);
        vertices.get(i).save();

        vertices.get(i - 1).newEdge("Edge1", vertices.get(i), true);
        if (i % 2 == 0) {
          vertices.get(i - 2).newEdge("Edge1", vertices.get(i), true);
        }
      }
    });
  }
}
