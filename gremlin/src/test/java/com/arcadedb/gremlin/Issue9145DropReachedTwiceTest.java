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

import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9145: {@code drop()} of an element the traversal reaches twice failed with RecordNotFoundException,
 * where TinkerGraph removes it once. Graph: a-&gt;b twice, a-&gt;c, b-&gt;d, c-&gt;d and a self-loop on e.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9145DropReachedTwiceTest {
  private static final String PATH = "./target/test-gremlin-9145-drop";

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(PATH));
    graph = ArcadeGraph.open(PATH);
    final Schema schema = graph.getDatabase().getSchema();
    schema.createVertexType("N");
    schema.createEdgeType("E");

    final Vertex a = graph.addVertex(T.label, "N", "name", "a");
    final Vertex b = graph.addVertex(T.label, "N", "name", "b");
    final Vertex c = graph.addVertex(T.label, "N", "name", "c");
    final Vertex d = graph.addVertex(T.label, "N", "name", "d");
    final Vertex e = graph.addVertex(T.label, "N", "name", "e");
    a.addEdge("E", b);
    a.addEdge("E", b);
    a.addEdge("E", c);
    b.addEdge("E", d);
    c.addEdge("E", d);
    e.addEdge("E", e);
    graph.tx().commit();
  }

  @AfterEach
  void teardown() {
    if (graph != null) {
      if (graph.tx().isOpen())
        graph.tx().rollback();
      graph.drop();
    }
    FileUtils.deleteRecursively(new File(PATH));
  }

  private String state(final GraphTraversalSource g) {
    final List<String> names = new ArrayList<>();
    g.V().<String>values("name").forEachRemaining(names::add);
    Collections.sort(names);
    return names + " vertices, " + g.E().count().next() + " edges";
  }

  private void assertDrop(final Function<GraphTraversalSource, GraphTraversal<?, ?>> traversal, final String expectedState) {
    final GraphTraversalSource g = graph.traversal();
    traversal.apply(g).iterate();
    assertThat(state(g)).isEqualTo(expectedState);
    graph.tx().commit();
    assertThat(state(graph.traversal())).isEqualTo(expectedState);
  }

  @Test
  void aVertexReachedFromTwoNeighboursIsDroppedOnce() {
    assertDrop(g -> g.V().hasLabel("N").out().drop(), "[a, d] vertices, 0 edges");
  }

  @Test
  void aVertexReachedByParallelEdgesIsDroppedOnce() {
    assertDrop(g -> g.V().has("name", "a").out().drop(), "[a, d, e] vertices, 1 edges");
  }

  @Test
  void aSelfLoopVertexReachedByBothIsDroppedOnce() {
    assertDrop(g -> g.V().has("name", "e").both().drop(), "[a, b, c, d] vertices, 5 edges");
  }

  @Test
  void aSelfLoopEdgeReachedByBothEIsDroppedOnce() {
    assertDrop(g -> g.V().has("name", "e").bothE().drop(), "[a, b, c, d, e] vertices, 5 edges");
  }

  @Test
  void everyEdgeReachedFromBothEndsIsDroppedOnce() {
    assertDrop(g -> g.V().hasLabel("N").bothE().drop(), "[a, b, c, d, e] vertices, 0 edges");
  }

  @Test
  void aTraversalWithNoRepeatedElementStillDropsEverything() {
    assertDrop(g -> g.V().has("name", "a").out().out().drop(), "[a, b, c, e] vertices, 4 edges");
  }

  @Test
  void anEdgeRemovedWithItsVertexIsNotRemovedAgain() {
    final GraphTraversalSource g = graph.traversal();
    final Vertex b = g.V().has("name", "b").next();
    final List<Edge> edges = g.V(b).bothE().toList();
    assertThat(edges).hasSize(3);

    b.remove();
    for (final Edge edge : edges)
      edge.remove();
    b.remove();

    assertThat(state(g)).isEqualTo("[a, c, d, e] vertices, 3 edges");
  }
}
