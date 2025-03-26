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
package com.arcadedb.query.sql.function.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class SQLFunctionAdjacencyTest {

  private final Map<Integer, MutableVertex> vertices = new HashMap<>();
  private final Map<Integer, MutableEdge>   edges    = new HashMap<>();

  @Test
  public void testOutE() throws Exception {
    TestHelper.executeInNewDatabase("testOutE", (graph) -> {
      setUpDatabase(graph);

      final Iterator<Identifiable> iterator = ((Iterable<Identifiable>) new SQLFunctionOutE().execute(vertices.get(3), null, null,
          new Object[] {}, new BasicCommandContext().setDatabase(graph))).iterator();

      final Set<RID> result = new HashSet<>();
      while (iterator.hasNext()) {
        final Edge edge = iterator.next().asEdge(true);
        assertThat(edge).isNotNull();
        result.add(edge.getIdentity());
      }

      assertThat(result.contains(edges.get(3).getIdentity())).isTrue();
      assertThat(result.contains(edges.get(4).getIdentity())).isTrue();

      assertThat(result).hasSize(2);
    });
  }

  @Test
  public void testInE() throws Exception {
    TestHelper.executeInNewDatabase("testInE", (graph) -> {
      setUpDatabase(graph);

      final Iterator<Identifiable> iterator = ((Iterable<Identifiable>) new SQLFunctionInE().execute(vertices.get(3), null, null,
          new Object[] {}, new BasicCommandContext().setDatabase(graph))).iterator();

      final Set<RID> result = new HashSet<>();
      while (iterator.hasNext()) {
        final Edge edge = iterator.next().asEdge(true);
        assertThat(edge).isNotNull();
        result.add(edge.getIdentity());
      }

      assertThat(result.contains(edges.get(2).getIdentity())).isTrue();

      assertThat(result).hasSize(1);
    });
  }

  @Test
  public void testBothE() throws Exception {
    TestHelper.executeInNewDatabase("testBothE", (graph) -> {
      setUpDatabase(graph);

      final Iterator<Identifiable> iterator = ((Iterable<Identifiable>) new SQLFunctionBothE().execute(vertices.get(3), null, null,
          new Object[] {}, new BasicCommandContext().setDatabase(graph))).iterator();

      final Set<RID> result = new HashSet<>();
      while (iterator.hasNext()) {
        final Edge edge = iterator.next().asEdge(true);
        assertThat(edge).isNotNull();
        result.add(edge.getIdentity());
      }

      assertThat(result.contains(edges.get(2).getIdentity())).isTrue();
      assertThat(result.contains(edges.get(3).getIdentity())).isTrue();
      assertThat(result.contains(edges.get(4).getIdentity())).isTrue();

      assertThat(result).hasSize(3);
    });
  }

  @Test
  public void testBoth() throws Exception {
    TestHelper.executeInNewDatabase("testBoth", (graph) -> {
      setUpDatabase(graph);

      final Iterator<Identifiable> iterator = ((Iterable<Identifiable>) new SQLFunctionBoth().execute(vertices.get(3), null, null,
          new Object[] {}, new BasicCommandContext().setDatabase(graph))).iterator();

      final Set<RID> result = new HashSet<>();
      while (iterator.hasNext()) {
        final Vertex vertex = iterator.next().asVertex(true);
        assertThat(vertex).isNotNull();
        result.add(vertex.getIdentity());
      }

      assertThat(result.contains(vertices.get(2).getIdentity())).isTrue();
      assertThat(result.contains(vertices.get(1).getIdentity())).isTrue();
      assertThat(result.contains(vertices.get(4).getIdentity())).isTrue();

      assertThat(result).hasSize(3);
    });
  }

  @Test
  public void testOut() throws Exception {
    TestHelper.executeInNewDatabase("testOut", (graph) -> {
      setUpDatabase(graph);

      final Iterator<Identifiable> iterator = ((Iterable<Identifiable>) new SQLFunctionOut().execute(vertices.get(3), null, null,
          new Object[] {}, new BasicCommandContext().setDatabase(graph))).iterator();

      final Set<RID> result = new HashSet<>();
      while (iterator.hasNext()) {
        final Vertex vertex = iterator.next().asVertex(true);
        assertThat(vertex).isNotNull();
        result.add(vertex.getIdentity());
      }

      assertThat(result.contains(vertices.get(1).getIdentity())).isTrue();
      assertThat(result.contains(vertices.get(4).getIdentity())).isTrue();

      assertThat(result).hasSize(2);
    });
  }

  @Test
  public void testIn() throws Exception {
    TestHelper.executeInNewDatabase("testIn", (graph) -> {
      setUpDatabase(graph);

      final Iterator<Identifiable> iterator = ((Iterable<Identifiable>) new SQLFunctionIn().execute(vertices.get(3), null, null,
          new Object[] {}, new BasicCommandContext().setDatabase(graph))).iterator();

      final Set<RID> result = new HashSet<>();
      while (iterator.hasNext()) {
        final Vertex vertex = iterator.next().asVertex(true);
        assertThat(vertex).isNotNull();
        result.add(vertex.getIdentity());
      }

      assertThat(result.contains(vertices.get(2).getIdentity())).isTrue();

      assertThat(result).hasSize(1);
    });
  }

  @Test
  public void testOutV() throws Exception {
    TestHelper.executeInNewDatabase("testOutV", (graph) -> {
      setUpDatabase(graph);

      final Vertex v = (Vertex) new SQLFunctionOutV().execute(edges.get(3), null, null, new Object[] {},
          new BasicCommandContext().setDatabase(graph));

      assertThat(vertices.get(3).getIdentity()).isEqualTo(v.getIdentity());
    });
  }

  @Test
  public void testInV() throws Exception {
    TestHelper.executeInNewDatabase("testInV", (graph) -> {
      setUpDatabase(graph);

      final Vertex v = (Vertex) new SQLFunctionInV().execute(edges.get(3), null, null, new Object[] {},
          new BasicCommandContext().setDatabase(graph));

      assertThat(vertices.get(1).getIdentity()).isEqualTo(v.getIdentity());
    });
  }

  @Test
  public void testBothV() throws Exception {
    TestHelper.executeInNewDatabase("testBothV", (graph) -> {
      setUpDatabase(graph);

      final ArrayList<Identifiable> iterator = (ArrayList<Identifiable>) new SQLFunctionBothV().execute(edges.get(3), null, null,
          new Object[] {}, new BasicCommandContext().setDatabase(graph));

      assertThat(iterator.contains(vertices.get(3).getIdentity())).isTrue();
      assertThat(iterator.contains(vertices.get(1).getIdentity())).isTrue();

      assertThat(iterator).hasSize(2);
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

      edges.put(1, vertices.get(1).newEdge("Edge1", vertices.get(2)));
      edges.put(2, vertices.get(2).newEdge("Edge1", vertices.get(3)));
      edges.put(3, vertices.get(3).newEdge("Edge2", vertices.get(1)));
      edges.put(4, vertices.get(3).newEdge("Edge1", vertices.get(4)));

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
