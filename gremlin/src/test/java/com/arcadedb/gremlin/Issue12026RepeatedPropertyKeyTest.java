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

import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12026: {@code properties(k, k)}, {@code values(k, k)} and {@code valueMap(k, k)} returned the property once for every
 * time its key was named. TinkerPop's reference graph returns each property of the element once.
 */
class Issue12026RepeatedPropertyKeyTest {
  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/testgremlin-12026");
    graph.getDatabase().getSchema().createVertexType("P");
    graph.getDatabase().getSchema().createEdgeType("K");
    graph.getDatabase().transaction(() -> {
      final MutableVertex v1 = graph.getDatabase().newVertex("P").set("n", 1).set("m", "a").save();
      final MutableVertex v2 = graph.getDatabase().newVertex("P").set("n", 2).save();
      v1.newEdge("K", v2, "w", 5);
    });
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void vertexPropertiesAndValuesNameEachPropertyOnce() {
    final GraphTraversalSource g = graph.traversal();
    assertThat(g.V().values("n", "n").count().next()).isEqualTo(2L);
    assertThat(g.V().has("n", 1).properties("n", "n").count().next()).isEqualTo(1L);
    assertThat(g.V().has("n", 1).values("n", "m", "n", "m").toList()).containsExactlyInAnyOrder(1, "a");
    assertThat(g.V().has("n", 1).valueMap("n", "n").next()).isEqualTo(Map.of("n", List.of(1)));
  }

  @Test
  void edgePropertiesAndValuesNameEachPropertyOnce() {
    final GraphTraversalSource g = graph.traversal();
    assertThat(g.E().values("w", "w").count().next()).isEqualTo(1L);
    assertThat(g.E().properties("w", "w").count().next()).isEqualTo(1L);
    assertThat(g.E().valueMap("w", "w").next()).isEqualTo(Map.of("w", 5));
  }

  /**
   * A null key never named a property: {@code Document.get(null)} answers null, so the key was skipped. Naming one next to another
   * key must not turn into a {@link NullPointerException} in the repeated-key scan.
   */
  @Test
  void aNullKeyIsStillSkipped() {
    final Vertex v = graph.traversal().V().has("n", 1).next();
    assertThat(v.<Object>values("n", null)).toIterable().containsExactly(1);
    assertThat(v.<Object>values("n", null, null, "m", "n")).toIterable().containsExactly(1, "a");
    assertThat(v.<Object>properties("n", null, "n")).toIterable().hasSize(1);
    final Edge e = graph.traversal().E().next();
    assertThat(e.<Object>values("w", null, "w")).toIterable().containsExactly(5);
    assertThat(e.<Object>properties("w", null, "w")).toIterable().hasSize(1);
  }

  /**
   * A key list longer than a handful of names, as a generated traversal passes: each property still comes once, in the order its
   * key was first named.
   */
  @Test
  void aWideKeyListNamesEachPropertyOnce() {
    final String[] keys = new String[64];
    for (int i = 0; i < keys.length; i++)
      keys[i] = "k" + (i % 30);
    keys[5] = "m";
    keys[40] = "n";
    keys[41] = "m";
    keys[63] = "n";

    final Vertex v = graph.traversal().V().has("n", 1).next();
    assertThat(v.<Object>values(keys)).toIterable().containsExactly("a", 1);
    assertThat(v.<Object>properties(keys)).toIterable().hasSize(2);

    final GraphTraversalSource g = graph.traversal();
    assertThat(g.V().has("n", 1).valueMap(keys).next()).isEqualTo(Map.of("m", List.of("a"), "n", List.of(1)));
  }

  @Test
  void aStringTraversalAgrees() {
    try (final ResultSet rs = graph.gremlin("g.V().values('n','n').count()").execute()) {
      assertThat(rs.next().<Long>getProperty("result")).isEqualTo(2L);
    }
  }
}
