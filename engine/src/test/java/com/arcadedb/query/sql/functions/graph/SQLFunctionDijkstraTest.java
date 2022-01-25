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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.function.graph.SQLFunctionDijkstra;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SQLFunctionDijkstraTest {

  private MutableVertex       v1;
  private MutableVertex       v2;
  private MutableVertex       v3;
  private MutableVertex       v4;
  private SQLFunctionDijkstra functionDijkstra;

  public void setUp(Database graph) throws Exception {
    graph.transaction((() -> {
      graph.getSchema().createVertexType("node");
      graph.getSchema().createEdgeType("weight");

      v1 = graph.newVertex("node");
      v2 = graph.newVertex("node");
      v3 = graph.newVertex("node");
      v4 = graph.newVertex("node");

      v1.set("node_id", "A").save();
      v2.set("node_id", "B").save();
      v3.set("node_id", "C").save();
      v4.set("node_id", "D").save();

      MutableEdge e1 = v1.newEdge("weight", v2, true);
      e1.set("weight", 1.0f);
      e1.save();

      MutableEdge e2 = v2.newEdge("weight", v3, true);
      e2.set("weight", 1.0f);
      e2.save();

      MutableEdge e3 = v1.newEdge("weight", v3, true);
      e3.set("weight", 100.0f);
      e3.save();

      MutableEdge e4 = v3.newEdge("weight", v4, true);
      e4.set("weight", 1.0f);
      e4.save();

      functionDijkstra = new SQLFunctionDijkstra();
    }));
  }

  @Test
  public void testExecute() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionDijkstraTest", (graph) -> {
      setUp(graph);
      final List<Vertex> result = functionDijkstra.execute(null, null, null, new Object[] { v1, v4, "'weight'" }, new BasicCommandContext());

      assertEquals(4, result.size());
      assertEquals(v1, result.get(0));
      assertEquals(v2, result.get(1));
      assertEquals(v3, result.get(2));
      assertEquals(v4, result.get(3));
    });
  }
}
