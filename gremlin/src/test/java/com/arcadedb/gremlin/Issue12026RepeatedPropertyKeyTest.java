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

  @Test
  void aStringTraversalAgrees() {
    try (final ResultSet rs = graph.gremlin("g.V().values('n','n').count()").execute()) {
      assertThat(rs.next().<Long>getProperty("result")).isEqualTo(2L);
    }
  }
}
