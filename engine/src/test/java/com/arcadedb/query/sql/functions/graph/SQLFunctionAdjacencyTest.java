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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.function.graph.SQLFunctionBoth;
import com.arcadedb.query.sql.function.graph.SQLFunctionBothE;
import com.arcadedb.query.sql.function.graph.SQLFunctionBothV;
import com.arcadedb.query.sql.function.graph.SQLFunctionIn;
import com.arcadedb.query.sql.function.graph.SQLFunctionInE;
import com.arcadedb.query.sql.function.graph.SQLFunctionInV;
import com.arcadedb.query.sql.function.graph.SQLFunctionOut;
import com.arcadedb.query.sql.function.graph.SQLFunctionOutE;
import com.arcadedb.query.sql.function.graph.SQLFunctionOutV;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class SQLFunctionAdjacencyTest {

  private final Map<Integer, MutableVertex> vertices = new HashMap<>();
  private final Map<Integer, MutableEdge>   edges    = new HashMap<>();

  @Test
  public void testOutE() throws Exception {
    TestHelper.executeInNewDatabase("testOutE", (graph) -> {
      setUpDatabase(graph);

      final Iterator<Identifiable> iterator = (Iterator<Identifiable>) new SQLFunctionOutE().execute(vertices.get(3), null, null, new Object[] {},
          new BasicCommandContext().setDatabase(graph));

      final Set<RID> result = new HashSet<>();
      while (iterator.hasNext()) {
        final Edge edge = iterator.next().asEdge(true);
        Assertions.assertNotNull(edge);
        result.add(edge.getIdentity());
      }

      Assertions.assertTrue(result.contains(edges.get(3).getIdentity()));
      Assertions.assertTrue(result.contains(edges.get(4).getIdentity()));

      Assertions.assertEquals(2, result.size());
    });
  }

  @Test
  public void testInE() throws Exception {
    TestHelper.executeInNewDatabase("testInE", (graph) -> {
      setUpDatabase(graph);

      final Iterator<Identifiable> iterator = (Iterator<Identifiable>) new SQLFunctionInE().execute(vertices.get(3), null, null, new Object[] {},
          new BasicCommandContext().setDatabase(graph));

      final Set<RID> result = new HashSet<>();
      while (iterator.hasNext()) {
        final Edge edge = iterator.next().asEdge(true);
        Assertions.assertNotNull(edge);
        result.add(edge.getIdentity());
      }

      Assertions.assertTrue(result.contains(edges.get(2).getIdentity()));

      Assertions.assertEquals(1, result.size());
    });
  }

  @Test
  public void testBothE() throws Exception {
    TestHelper.executeInNewDatabase("testBothE", (graph) -> {
      setUpDatabase(graph);

      final Iterator<Identifiable> iterator = (Iterator<Identifiable>) new SQLFunctionBothE().execute(vertices.get(3), null, null, new Object[] {},
          new BasicCommandContext().setDatabase(graph));

      final Set<RID> result = new HashSet<>();
      while (iterator.hasNext()) {
        final Edge edge = iterator.next().asEdge(true);
        Assertions.assertNotNull(edge);
        result.add(edge.getIdentity());
      }

      Assertions.assertTrue(result.contains(edges.get(2).getIdentity()));
      Assertions.assertTrue(result.contains(edges.get(3).getIdentity()));
      Assertions.assertTrue(result.contains(edges.get(4).getIdentity()));

      Assertions.assertEquals(3, result.size());
    });
  }

  @Test
  public void testBoth() throws Exception {
    TestHelper.executeInNewDatabase("testBoth", (graph) -> {
      setUpDatabase(graph);

      final Iterator<Identifiable> iterator = (Iterator<Identifiable>) new SQLFunctionBoth().execute(vertices.get(3), null, null, new Object[] {},
          new BasicCommandContext().setDatabase(graph));

      final Set<RID> result = new HashSet<>();
      while (iterator.hasNext()) {
        final Vertex vertex = iterator.next().asVertex(true);
        Assertions.assertNotNull(vertex);
        result.add(vertex.getIdentity());
      }

      Assertions.assertTrue(result.contains(vertices.get(2).getIdentity()));
      Assertions.assertTrue(result.contains(vertices.get(1).getIdentity()));
      Assertions.assertTrue(result.contains(vertices.get(4).getIdentity()));

      Assertions.assertEquals(3, result.size());
    });
  }

  @Test
  public void testOut() throws Exception {
    TestHelper.executeInNewDatabase("testOut", (graph) -> {
      setUpDatabase(graph);

      final Iterator<Identifiable> iterator = (Iterator<Identifiable>) new SQLFunctionOut().execute(vertices.get(3), null, null, new Object[] {},
          new BasicCommandContext().setDatabase(graph));

      final Set<RID> result = new HashSet<>();
      while (iterator.hasNext()) {
        final Vertex vertex = iterator.next().asVertex(true);
        Assertions.assertNotNull(vertex);
        result.add(vertex.getIdentity());
      }

      Assertions.assertTrue(result.contains(vertices.get(1).getIdentity()));
      Assertions.assertTrue(result.contains(vertices.get(4).getIdentity()));

      Assertions.assertEquals(2, result.size());
    });
  }

  @Test
  public void testIn() throws Exception {
    TestHelper.executeInNewDatabase("testIn", (graph) -> {
      setUpDatabase(graph);

      final Iterator<Identifiable> iterator = (Iterator<Identifiable>) new SQLFunctionIn().execute(vertices.get(3), null, null, new Object[] {},
          new BasicCommandContext().setDatabase(graph));

      final Set<RID> result = new HashSet<>();
      while (iterator.hasNext()) {
        final Vertex vertex = iterator.next().asVertex(true);
        Assertions.assertNotNull(vertex);
        result.add(vertex.getIdentity());
      }

      Assertions.assertTrue(result.contains(vertices.get(2).getIdentity()));

      Assertions.assertEquals(1, result.size());
    });
  }

  @Test
  public void testOutV() throws Exception {
    TestHelper.executeInNewDatabase("testOutV", (graph) -> {
      setUpDatabase(graph);

      Vertex v = (Vertex) new SQLFunctionOutV().execute(edges.get(3), null, null, new Object[] {}, new BasicCommandContext().setDatabase(graph));

      Assertions.assertEquals(v.getIdentity(), vertices.get(3).getIdentity());
    });
  }

  @Test
  public void testInV() throws Exception {
    TestHelper.executeInNewDatabase("testInV", (graph) -> {
      setUpDatabase(graph);

      Vertex v = (Vertex) new SQLFunctionInV().execute(edges.get(3), null, null, new Object[] {}, new BasicCommandContext().setDatabase(graph));

      Assertions.assertEquals(v.getIdentity(), vertices.get(1).getIdentity());
    });
  }

  @Test
  public void testBothV() throws Exception {
    TestHelper.executeInNewDatabase("testBothV", (graph) -> {
      setUpDatabase(graph);

      final ArrayList<Identifiable> iterator = (ArrayList<Identifiable>) new SQLFunctionBothV().execute(edges.get(3), null, null, new Object[] {},
          new BasicCommandContext().setDatabase(graph));

      Assertions.assertTrue(iterator.contains(vertices.get(3).getIdentity()));
      Assertions.assertTrue(iterator.contains(vertices.get(1).getIdentity()));

      Assertions.assertEquals(2, iterator.size());
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

      edges.put(1, vertices.get(1).newEdge("Edge1", vertices.get(2), true));
      edges.put(2, vertices.get(2).newEdge("Edge1", vertices.get(3), true));
      edges.put(3, vertices.get(3).newEdge("Edge2", vertices.get(1), true));
      edges.put(4, vertices.get(3).newEdge("Edge1", vertices.get(4), true));

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
