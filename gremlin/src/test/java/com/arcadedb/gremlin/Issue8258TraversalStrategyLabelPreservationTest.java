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
import com.arcadedb.gremlin.support.DifferentialTraversal;
import com.arcadedb.gremlin.support.TraversalPlans;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8258
 * <p>
 * {@code ArcadeTraversalStrategy} replaces a step with a rewritten one via {@code removeStep()}/{@code
 * addStep()} at five sites, none of which carried the removed step's {@code as()} label onto the
 * replacement - TinkerPop drops a step's labels together with the step itself. A later {@code
 * select()}/{@code path()} naming that label then found nothing: an empty result, never an error.
 * <p>
 * Two of the five sites (the plain type-filter and index-filter rewrites) turn out to be unreachable
 * through the fluent API for THIS particular failure: TinkerPop relocates a label from the leading
 * {@code GraphStep} onto the surviving {@code HasStep} before {@code ArcadeTraversalStrategy} runs (see
 * the class javadoc there), and that {@code HasStep} is never removed by those two rewrites, so the
 * label survives by accident regardless of the fix. The fix still copies it defensively, for the same
 * reason the other three sites need it: a future TinkerPop version, or a query shape this test does not
 * cover, could stop relocating it. The count rewrite removes the {@code GraphStep} AND the {@code
 * HasStep}, so it does not get that accidental safety net - {@code countArmAlsoPreservesTheLeadingGraphStepLabel}
 * below pins a live repro of that gap.
 * <p>
 * The GAV fused-chain rewrite has the extra wrinkle that it collapses several hops into one step that
 * emits only the final traverser, so it must not fuse when an intermediate hop carries a label, or when
 * the traversal needs the full {@code path()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8258TraversalStrategyLabelPreservationTest {
  private ArcadeGraph          graph;
  private GraphAnalyticalView  gav;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-issue8258-labels");
    graph.getDatabase().getSchema().createVertexType("Person").createProperty("name", com.arcadedb.schema.Type.STRING);
    graph.getDatabase().getSchema().createEdgeType("KNOWS");

    graph.getDatabase().transaction(() -> {
      final MutableVertex alice = graph.getDatabase().newVertex("Person").set("name", "Alice").save();
      final MutableVertex bob = graph.getDatabase().newVertex("Person").set("name", "Bob").save();
      final MutableVertex charlie = graph.getDatabase().newVertex("Person").set("name", "Charlie").save();
      alice.newEdge("KNOWS", bob);
      bob.newEdge("KNOWS", charlie);
    });

    // Build the GAV through the builder: the public GraphAnalyticalView(Database) constructor never
    // registers as a traversal provider, so a view built that way would be invisible to
    // GraphTraversalProviderRegistry.findProvider() and the GAV rewrite this test targets would never fire.
    gav = GraphAnalyticalView.builder((Database) graph.getDatabase())
        .withVertexTypes("Person")
        .withEdgeTypes("KNOWS")
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
  void countThenSelectMatchesUnoptimizedPath() {
    // exact repro from the issue: hasLabel().count().as('c').select('c') threw NoSuchElementException
    // because the count rewrite dropped CountGlobalStep's label along with the step
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").count().as("c").select("c"));
  }

  @Test
  void countGlobalStepLabelSurvivesTheRewrite() {
    final Step<?, ?> countStep = lastStepOf(graph.traversal().V().hasLabel("Person").count().as("c"));
    assertThat(countStep).isInstanceOf(ArcadeCountGlobalStep.class);
    assertThat(countStep.getLabels()).containsExactly("c");
  }

  @Test
  void countArmDoesNotBindTheLeadingGraphStepLabelToTheCount() {
    // PR #8309 review: an earlier version of this fix copied the GraphStep's label onto the count too, which
    // is wrong rather than merely unreachable - count() is a REDUCING BARRIER, and real TinkerPop's own
    // CountGlobalStep does not carry a label from before it forward: the unoptimized
    // g.V().as("v").hasLabel("Person").count().select("v") throws NoSuchElementException, it does not answer
    // the vertex. Only the count's OWN label (asserted in countGlobalStepLabelSurvivesTheRewrite above) may
    // survive the rewrite.
    final Step<?, ?> countStep = lastStepOf(graph.traversal().V().as("v").hasLabel("Person").count());
    assertThat(countStep).isInstanceOf(ArcadeCountGlobalStep.class);
    assertThat(countStep.getLabels()).as("a label from before count() must not survive it, matching real TinkerPop").isEmpty();

    assertThatThrownBy(() -> graph.traversal().V().as("v").hasLabel("Person").count().select("v").next())
        .as("matching the unoptimized path's own NoSuchElementException for this shape")
        .isInstanceOf(java.util.NoSuchElementException.class);
  }

  @Test
  void singleGavHopLabelSurvivesTheRewrite() {
    final Step<?, ?> gavStep = lastStepOf(graph.traversal().V().has("name", "Alice").out("KNOWS").as("b"));
    assertThat(gavStep).isInstanceOf(ArcadeGAVVertexStep.class);
    assertThat(gavStep.getLabels()).containsExactly("b");

    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().has("name", "Alice").out("KNOWS").as("b").select("b").values("name"));
  }

  @Test
  void gavChainDoesNotFuseWhenAnIntermediateHopIsLabelled() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().has("name", "Alice").out("KNOWS").as("mid").out("KNOWS"), ArcadeGAVFusedStep.class))
        .as("plan was: %s", TraversalPlans.describe(
            graph.traversal().V().has("name", "Alice").out("KNOWS").as("mid").out("KNOWS")))
        .isFalse();

    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().has("name", "Alice").out("KNOWS").as("mid").out("KNOWS").select("mid").values("name"));
  }

  @Test
  void gavChainStillFusesWhenOnlyTheLastHopIsLabelled() {
    final Step<?, ?> fusedStep = lastStepOf(graph.traversal().V().has("name", "Alice").out("KNOWS").out("KNOWS").as("last"));
    assertThat(fusedStep).isInstanceOf(ArcadeGAVFusedStep.class);
    assertThat(fusedStep.getLabels()).containsExactly("last");
  }

  @Test
  void gavChainDoesNotFuseWhenThePathIsRequested() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().has("name", "Alice").out("KNOWS").out("KNOWS").path(), ArcadeGAVFusedStep.class))
        .isFalse();

    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().has("name", "Alice").out("KNOWS").out("KNOWS").path());
  }

  private static Step<?, ?> lastStepOf(final org.apache.tinkerpop.gremlin.process.traversal.Traversal<?, ?> traversal) {
    final List<Step> steps = TraversalPlans.stepsOf(traversal);
    return steps.get(steps.size() - 1);
  }
}
