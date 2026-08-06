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

import com.arcadedb.gremlin.support.DifferentialTraversal;
import com.arcadedb.gremlin.support.TraversalPlans;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the ArcadeFilterByTypeStep and ArcadeCountGlobalStep rewrites in ArcadeTraversalStrategy,
 * including the guards that must SUPPRESS the rewrite: more than one label container, and a label
 * naming a type of the opposite element kind (issue #5223).
 */
class ArcadeFilterByTypeStepTest {

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-filterbytype");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().getSchema().createVertexType("Company");
    graph.getDatabase().getSchema().createEdgeType("WORKS_AT");
    graph.getDatabase().transaction(() -> {
      final ArcadeVertex alice = graph.addVertex("Person");
      alice.property("name", "Alice");
      final ArcadeVertex bob = graph.addVertex("Person");
      bob.property("name", "Bob");
      final ArcadeVertex acme = graph.addVertex("Company");
      acme.property("name", "Acme");
      alice.addEdge("WORKS_AT", acme);
    });
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void hasLabelInstallsTheTypeFilterStep() {
    assertThat(TraversalPlans.hasStepOfType(graph.traversal().V().hasLabel("Person"),
        ArcadeFilterByTypeStep.class)).isTrue();
  }

  @Test
  void hasLabelMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().hasLabel("Person").values("name"));
  }

  @Test
  void edgeHasLabelMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.E().hasLabel("WORKS_AT").count());
  }

  @Test
  void countAfterHasLabelInstallsTheCountStep() {
    assertThat(TraversalPlans.hasStepOfType(graph.traversal().V().hasLabel("Person").count(),
        ArcadeCountGlobalStep.class))
        .as("plan was: %s", TraversalPlans.describe(graph.traversal().V().hasLabel("Person").count()))
        .isTrue();
  }

  @Test
  void countAfterHasLabelMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().hasLabel("Person").count());
  }

  @Test
  void twoLabelsSuppressTheRewrite() {
    // NOTE: hasLabel(String, String...) compiles to ONE HasContainer with a P.within(labels)
    // predicate, not two ~label containers - see TraversalPlans-based verification in the task
    // report. This test therefore exercises the "totalLabels == 1 && typeNameToMatch != null"
    // guard (typeNameToMatch stays null because the predicate is P.within, not Compare.eq), not
    // ArcadeTraversalStrategy's totalLabels > 1 branch. See twoChainedLabelContainersSuppressTheRewrite
    // below for a test that reaches the totalLabels > 1 branch.
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().hasLabel("Person", "Company"), ArcadeFilterByTypeStep.class)).isFalse();
  }

  @Test
  void twoLabelsStillReturnTheRightRows() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person", "Company").values("name"));
  }

  @Test
  void twoChainedLabelContainersSuppressTheRewrite() {
    // Chained .hasLabel(a).hasLabel(b) calls merge onto the SAME HasStep as two separate
    // Compare.eq ~label HasContainers (confirmed by direct pre-strategy inspection), so this
    // genuinely reaches ArcadeTraversalStrategy's totalLabels > 1 branch: it clears typeNameToMatch
    // and the containersToRemove set, and both label containers survive on the HasStep. Removing
    // only one of them (e.g. dropping the containersToRemove.clear() call) would silently widen the
    // AND semantics of two chained hasLabel calls into a single-label filter.
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().hasLabel("Person").hasLabel("Company"), ArcadeFilterByTypeStep.class))
        .isFalse();
  }

  @Test
  void twoChainedLabelContainersStillReturnTheRightRows() {
    // Two ~label containers on one HasStep are ANDed: no vertex can be both a Person and a Company,
    // so the correct result is empty. A broken suppression that dropped the Person container would
    // widen this to "every Company vertex" (Acme, from the fixture below) - a non-empty result that
    // would diverge from the unoptimized path and fail this assertion.
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").hasLabel("Company").values("name"));
  }

  @Test
  void vertexTraversalWithAnEdgeLabelReturnsNothing() {
    // Issue #5223: a wrong-kind index scan would return elements of the opposite category.
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().hasLabel("WORKS_AT").count());
  }

  @Test
  void edgeTraversalWithAVertexLabelReturnsNothing() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.E().hasLabel("Person").count());
  }

  @Test
  void hasLabelOnANonExistentTypeReturnsNothing() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().hasLabel("DoesNotExist").count());
  }

  @Test
  void graphStepWithExplicitIdsIsNotRewritten() {
    // ArcadeTraversalStrategy skips the rewrite when prevStepGraph.getIds().length != 0.
    final Object id = graph.traversal().V().hasLabel("Person").next().id();
    assertThat(TraversalPlans.hasStepOfType(graph.traversal().V(id).hasLabel("Person"),
        ArcadeFilterByTypeStep.class)).isFalse();
  }
}
