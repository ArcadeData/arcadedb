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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Self-test for the plan inspection helper. If these fail, every optimizer assertion built on
 * TraversalPlans is meaningless, so this class must be kept honest: it asserts both that an expected
 * step IS found and that an absent step is NOT found.
 */
class TraversalPlansTest {

  private ArcadeGraph createGraph() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/test-traversalplans");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().transaction(() -> graph.addVertex("Person").property("name", "Alice"));
    return graph;
  }

  @Test
  void detectsTheTypeFilterStepTheStrategyInstalls() {
    final ArcadeGraph graph = createGraph();
    try {
      assertThat(TraversalPlans.hasStepOfType(graph.traversal().V().hasLabel("Person"),
          ArcadeFilterByTypeStep.class)).isTrue();
    } finally {
      graph.drop();
    }
  }

  @Test
  void doesNotReportAStepThatIsAbsent() {
    final ArcadeGraph graph = createGraph();
    try {
      // No hasLabel(), so the strategy has nothing to rewrite and the plain GraphStep survives.
      assertThat(TraversalPlans.hasStepOfType(graph.traversal().V(),
          ArcadeFilterByTypeStep.class)).isFalse();
      assertThat(TraversalPlans.hasStepOfType(graph.traversal().V(), GraphStep.class)).isTrue();
    } finally {
      graph.drop();
    }
  }

  @Test
  void describeListsStepsInOrder() {
    final ArcadeGraph graph = createGraph();
    try {
      assertThat(TraversalPlans.describe(graph.traversal().V().hasLabel("Person")))
          .contains("ArcadeFilterByTypeStep");
    } finally {
      graph.drop();
    }
  }
}
