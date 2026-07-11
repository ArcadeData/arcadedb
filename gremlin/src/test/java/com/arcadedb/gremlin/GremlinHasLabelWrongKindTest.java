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
package com.arcadedb.gremlin;

import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces https://github.com/ArcadeData/arcadedb/issues/5223 : {@code hasLabel()} must preserve the element kind of
 * the traversal. {@code g.V().hasLabel(<edge-type>)} must return no vertices and {@code g.E().hasLabel(<vertex-type>)}
 * must return no edges, regardless of whether the named type exists as the opposite element kind.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinHasLabelWrongKindTest {

  private ArcadeGraph createGraph() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin-5223");
    graph.getDatabase().getSchema().createVertexType("A");
    graph.getDatabase().getSchema().createVertexType("B");
    graph.getDatabase().getSchema().createEdgeType("GEdge");

    graph.getDatabase().transaction(() -> {
      final ArcadeVertex a = graph.addVertex("A");
      a.property("v", 1);
      final ArcadeVertex b = graph.addVertex("B");
      b.property("v", 2);
      a.addEdge("GEdge", b);
    });
    return graph;
  }

  private long count(final ArcadeGraph graph, final String gremlin) {
    final ResultSet rs = graph.gremlin(gremlin).execute();
    return (Long) rs.nextIfAvailable().getProperty("result");
  }

  @Test
  void sanityCounts() {
    final ArcadeGraph graph = createGraph();
    try {
      assertThat(count(graph, "g.V().count()")).isEqualTo(2);
      assertThat(count(graph, "g.E().count()")).isEqualTo(1);

      // genuine labels
      assertThat(count(graph, "g.V().hasLabel('A').count()")).isEqualTo(1);
      assertThat(count(graph, "g.V().hasLabel('B').count()")).isEqualTo(1);
      assertThat(count(graph, "g.E().hasLabel('GEdge').count()")).isEqualTo(1);

      // nonexistent label
      assertThat(count(graph, "g.V().hasLabel('DoesNotExist').count()")).isEqualTo(0);
      assertThat(count(graph, "g.E().hasLabel('DoesNotExist').count()")).isEqualTo(0);
    } finally {
      graph.drop();
    }
  }

  @Test
  void vertexTraversalMustNotCountEdgeType() {
    final ArcadeGraph graph = createGraph();
    try {
      assertThat(count(graph, "g.V().hasLabel('GEdge').count()")).isEqualTo(0);
    } finally {
      graph.drop();
    }
  }

  @Test
  void edgeTraversalMustNotCountVertexType() {
    final ArcadeGraph graph = createGraph();
    try {
      assertThat(count(graph, "g.E().hasLabel('A').count()")).isEqualTo(0);
    } finally {
      graph.drop();
    }
  }

  @Test
  void vertexTraversalMustNotReturnEdgeType() {
    final ArcadeGraph graph = createGraph();
    try {
      final ResultSet rs = graph.gremlin("g.V().hasLabel('GEdge')").execute();
      assertThat(rs.hasNext()).isFalse();
    } finally {
      graph.drop();
    }
  }

  @Test
  void edgeTraversalMustNotReturnVertexType() {
    final ArcadeGraph graph = createGraph();
    try {
      final ResultSet rs = graph.gremlin("g.E().hasLabel('A')").execute();
      assertThat(rs.hasNext()).isFalse();
    } finally {
      graph.drop();
    }
  }
}
