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
package com.arcadedb.gremlin.support;

import com.arcadedb.gremlin.ArcadeFilterByTypeStep;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.gremlin.ArcadeTraversalStrategy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Self-test for the differential helper. Asserts that it agrees when the two paths agree, AND that
 * it actually detects a disagreement. A comparison utility that cannot report a mismatch would let
 * every optimizer defect through silently.
 */
class DifferentialTraversalTest {

  private ArcadeGraph createGraph() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/test-differential");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().transaction(() -> {
      graph.addVertex("Person").property("name", "Alice");
      graph.addVertex("Person").property("name", "Bob");
    });
    return graph;
  }

  @Test
  void agreesWhenBothPathsReturnTheSameRows() {
    final ArcadeGraph graph = createGraph();
    try {
      DifferentialTraversal.on(graph).assertSameResults(g -> g.V().hasLabel("Person").values("name"));
    } finally {
      graph.drop();
    }
  }

  @Test
  void reportsADisagreementWhenTheTwoSidesDiffer() {
    final ArcadeGraph graph = createGraph();
    try {
      final DifferentialTraversal diff = DifferentialTraversal.on(graph);
      // Feed deliberately different queries to the two sides to prove the comparison bites.
      assertThatThrownBy(() -> diff.assertResultsMatch(
          diff.optimized(g -> g.V().hasLabel("Person").values("name")),
          diff.unoptimized(g -> g.V().hasLabel("Person").limit(1).values("name"))))
          .isInstanceOf(AssertionError.class);
    } finally {
      graph.drop();
    }
  }

  @Test
  void bothSidesSeeTheSameData() {
    final ArcadeGraph graph = createGraph();
    try {
      final DifferentialTraversal diff = DifferentialTraversal.on(graph);
      assertThat(diff.optimized(g -> g.V().hasLabel("Person").values("name")))
          .containsExactlyInAnyOrder("Alice", "Bob");
      assertThat(diff.unoptimized(g -> g.V().hasLabel("Person").values("name")))
          .containsExactlyInAnyOrder("Alice", "Bob");

      assertThat(TraversalPlans.hasStepOfType(
          graph.traversal().withoutStrategies(ArcadeTraversalStrategy.class).V().hasLabel("Person"),
          ArcadeFilterByTypeStep.class)).isFalse();
    } finally {
      graph.drop();
    }
  }
}
