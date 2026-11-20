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

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.gremlin.support.DifferentialTraversal;
import com.arcadedb.gremlin.support.TraversalPlans;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the CSR-accelerated GAV steps. Every test asserts the accelerated step is present in the
 * compiled plan BEFORE asserting behavior, then compares the accelerated result against the
 * unoptimized traversal. Result-only assertions cannot distinguish a correct optimization from an
 * optimization that never ran.
 */
class ArcadeGAVStepsTest {

  private ArcadeGraph          graph;
  private GraphAnalyticalView  gav;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gav-steps");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().getSchema().createEdgeType("KNOWS");
    graph.getDatabase().getSchema().createEdgeType("LIKES");

    // A -KNOWS-> B -KNOWS-> C -KNOWS-> D, plus A -LIKES-> C and a disconnected E.
    // Three KNOWS hops make a 3-step fusion chain reachable; E exercises the empty-neighbor path.
    graph.getDatabase().transaction(() -> {
      final MutableVertex a = graph.getDatabase().newVertex("Person").set("name", "A").save();
      final MutableVertex b = graph.getDatabase().newVertex("Person").set("name", "B").save();
      final MutableVertex c = graph.getDatabase().newVertex("Person").set("name", "C").save();
      final MutableVertex d = graph.getDatabase().newVertex("Person").set("name", "D").save();
      graph.getDatabase().newVertex("Person").set("name", "E").save();
      a.newEdge("KNOWS", b);
      b.newEdge("KNOWS", c);
      c.newEdge("KNOWS", d);
      a.newEdge("LIKES", c);
    });

    gav = GraphAnalyticalView.builder((Database) graph.getDatabase())
        .withVertexTypes("Person")
        .withEdgeTypes("KNOWS", "LIKES")
        .build();
  }

  @AfterEach
  void teardown() {
    if (gav != null)
      gav.drop();
    if (graph != null)
      graph.drop();
  }

  @Test
  void twoConsecutiveHopsFuseIntoASingleStep() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().has("name", "A").out("KNOWS").out("KNOWS"), ArcadeGAVFusedStep.class))
        .as("plan was: %s",
            TraversalPlans.describe(graph.traversal().V().has("name", "A").out("KNOWS").out("KNOWS")))
        .isTrue();
  }

  @Test
  void aSingleHopUsesTheUnfusedGAVStep() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().has("name", "A").out("KNOWS"), ArcadeGAVVertexStep.class))
        .as("plan was: %s", TraversalPlans.describe(graph.traversal().V().has("name", "A").out("KNOWS")))
        .isTrue();
  }

  @Test
  void singleHopMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().has("name", "A").out("KNOWS").values("name"));
  }

  @Test
  void twoHopFusedChainMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().has("name", "A").out("KNOWS").out("KNOWS").values("name"));
  }

  @Test
  void threeHopFusedChainMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().has("name", "A").out("KNOWS").out("KNOWS").out("KNOWS").values("name"));
  }

  @Test
  void inDirectionMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().has("name", "D").in("KNOWS").values("name"));
  }

  @Test
  void bothDirectionMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().has("name", "B").both("KNOWS").values("name"));
  }

  @Test
  void mixedEdgeLabelsAcrossHopsMatchTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().has("name", "A").out("LIKES").out("KNOWS").values("name"));
  }

  @Test
  void unlabeledHopMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().has("name", "A").out().values("name"));
  }

  @Test
  void aVertexWithNoOutgoingEdgesYieldsNothingOnBothPaths() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().has("name", "E").out("KNOWS").values("name"));
  }

  @Test
  void aChainLongerThanTheGraphYieldsNothingOnBothPaths() {
    // Walks past D, where the CSR neighbor array is empty at an intermediate hop.
    DifferentialTraversal.on(graph).assertSameResults(
        g -> g.V().has("name", "A").out("KNOWS").out("KNOWS").out("KNOWS").out("KNOWS").values("name"));
  }

  @Test
  void aVertexDeletedAfterTheCsrBuildIsSkippedNotThrown() {
    graph.getDatabase().transaction(() -> {
      final var it = graph.getDatabase().query("sql", "SELECT FROM Person WHERE name = 'D'");
      it.next().getElement().get().asVertex().delete();
    });
    // The CSR still holds D's node id. The fused chain must skip it rather than propagate the
    // RecordNotFoundException.
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().has("name", "A").out("KNOWS").out("KNOWS").out("KNOWS").values("name"));
  }

  /**
   * Regression for #11636: the fused chain checked that a vertex still exists only at the final hop, and walked the stale CSR
   * straight through an INTERMEDIATE vertex deleted since the build, answering the vertices reachable only through it. The
   * unfused step skips the deleted vertex before it feeds the next hop, so the same query had two answers.
   */
  @Test
  void aVertexDeletedAfterTheCsrBuildIsNotWalkedThroughAtAnIntermediateHop() {
    serveWhenStale();
    deletePerson("B");
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().has("name", "A").out("KNOWS").out("KNOWS"), ArcadeGAVFusedStep.class))
        .as("the stale view still serves the fused chain, plan was: %s",
            TraversalPlans.describe(graph.traversal().V().has("name", "A").out("KNOWS").out("KNOWS")))
        .isTrue();

    // C is reachable from A only through B
    assertThat(graph.traversal().V().has("name", "A").out("KNOWS").out("KNOWS").values("name").toList()).isEmpty();
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().has("name", "A").out("KNOWS").out("KNOWS").values("name"));
  }

  @Test
  void aVertexDeletedAfterTheCsrBuildIsNotWalkedThroughAtALaterIntermediateHop() {
    serveWhenStale();
    deletePerson("C");
    // A -LIKES-> C was the other way to C: D is not reachable through it either
    assertThat(graph.traversal().V().has("name", "A").out("LIKES").out("KNOWS").values("name").toList()).isEmpty();
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().has("name", "A").out("LIKES").out("KNOWS").values("name"));

    // Three hops fuse into ONE chain only without the barrier TinkerPop puts after every second hop: the deleted vertex is then the
    // chain's second intermediate one, met after a live first one
    final GraphTraversalSource noBarrier = graph.traversal().withoutStrategies(LazyBarrierStrategy.class);
    final List<Step> steps = TraversalPlans.stepsOf(noBarrier.V().has("name", "A").out("KNOWS").out("KNOWS").out("KNOWS"));
    assertThat(steps.stream().filter(s -> s instanceof ArcadeGAVFusedStep).count()).as("plan was: %s", steps).isEqualTo(1L);
    assertThat(steps.stream().anyMatch(s -> s instanceof ArcadeGAVVertexStep || s instanceof VertexStep)).as("plan was: %s", steps)
        .isFalse();
    // D is reachable from A only through B then C
    assertThat(noBarrier.V().has("name", "A").out("KNOWS").out("KNOWS").out("KNOWS").values("name").toList()).isEmpty();
  }

  /** The view of the setup, but one that keeps serving once a commit made it STALE ({@code gavUseWhenStale}). */
  private void serveWhenStale() {
    gav.drop();
    gav = null;
    gav = GraphAnalyticalView.builder((Database) graph.getDatabase())
        .withVertexTypes("Person")
        .withEdgeTypes("KNOWS", "LIKES")
        .withUseWhenStale(true)
        .build();
  }

  private void deletePerson(final String name) {
    graph.getDatabase().transaction(() -> {
      final var it = graph.getDatabase().query("sql", "SELECT FROM Person WHERE name = ?", name);
      it.next().getElement().get().asVertex().delete();
    });
  }
}
