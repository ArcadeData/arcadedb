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
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.SimpleBindings;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the where(outE('X').count().is(...)) rewrite into an O(1) degree check. The rewrite requires
 * a registered GraphTraversalProvider, so the view is built through the builder (the bare
 * GraphAnalyticalView constructor never registers).
 */
class ArcadeEdgeCountFilterStepTest {

  private ArcadeGraph         graph;
  private GraphAnalyticalView gav;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-edgecountfilter");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().getSchema().createEdgeType("KNOWS");

    // Out-degrees: A=2, B=1, C=0, D=0
    graph.getDatabase().transaction(() -> {
      final MutableVertex a = graph.getDatabase().newVertex("Person").set("name", "A").save();
      final MutableVertex b = graph.getDatabase().newVertex("Person").set("name", "B").save();
      final MutableVertex c = graph.getDatabase().newVertex("Person").set("name", "C").save();
      final MutableVertex d = graph.getDatabase().newVertex("Person").set("name", "D").save();
      a.newEdge("KNOWS", b);
      a.newEdge("KNOWS", c);
      b.newEdge("KNOWS", d);
    });

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
  void theDegreeFilterStepIsInstalled() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().where(outE("KNOWS").count().is(P.gt(1))), ArcadeEdgeCountFilterStep.class))
        .as("plan was: %s",
            TraversalPlans.describe(graph.traversal().V().where(outE("KNOWS").count().is(P.gt(1)))))
        .isTrue();
  }

  @Test
  void whenTheRewriteDoesNotInstallTinkerPopsCountStrategyExplainsWhy() {
    // Companion to the disabled theDegreeFilterStepIsInstalled above. Whether ArcadeTraversalStrategy or
    // TinkerPop's CountStrategy applies first for this traversal is not pinned down by
    // ArcadeTraversalStrategy.applyPrior() (it only orders itself after InlineFilterStrategy), so this
    // assertion is written to hold either way instead of asserting a specific, order-dependent outcome:
    // if the GAV step is missing, the where-child must show CountStrategy's RangeGlobalStep fingerprint,
    // confirming that IS the reason it did not install (as opposed to some unrelated regression).
    final Traversal<?, ?> probe = graph.traversal().V().where(outE("KNOWS").count().is(P.gt(1)));
    final boolean installed = TraversalPlans.hasStepOfType(probe, ArcadeEdgeCountFilterStep.class);
    if (!installed) {
      final String plan = describeWithChildren(graph.traversal().V().where(outE("KNOWS").count().is(P.gt(1))));
      assertThat(plan)
          .as("rewrite did not install; expected TinkerPop's CountStrategy to have inserted a "
              + "RangeGlobalStep ahead of it, plan: %s", plan)
          .contains("RangeGlobalStep");
    }
  }

  /**
   * CHARACTERIZATION TEST OF A KNOWN DEFECT. This asserts the CURRENT WRONG behavior on purpose, to pin
   * it and detect if it silently changes. It is NOT a statement of the intended contract.
   * <p>
   * graph.gremlin(String) parses through gremlin-lang, a different front end from the fluent Java API
   * every other test in this class uses. Task 4/5 found the GAV VertexStep rewrite is blind on this path
   * for labeled hops: gremlin-lang builds a VertexStepPlaceholder that GValueReductionStrategy only
   * resolves after ArcadeTraversalStrategy has already run, so "step instanceof VertexStep" never
   * matches. This rewrite keys off VertexStep the same way, so it inherits the same gap: confirmed, the
   * O(1) degree-check optimization does not fire for this query shape when it arrives as a string.
   * ArcadeGraph.getGremlinJavaEngine() is the same GremlinLangScriptEngine that
   * ArcadeGremlin.executeStatement() uses internally for the "java" engine (the default, and the first
   * one "auto" tries), so this drives the traversal through the identical parsing entry point
   * graph.gremlin(String) uses in production.
   * <p>
   * WHEN THE VertexStepPlaceholder GAP IS FIXED, INVERT THIS TEST: the expectation becomes that the
   * rewrite DOES install on the string path, and the method should be renamed accordingly (e.g.
   * theRewriteEngagesViaTheStringEntryPoint). Tracked as issue #5840; see PR #5829 for the full writeup.
   */
  @Test
  void characterizesTheRewriteNotEngagingViaTheStringEntryPoint() throws Exception {
    final GremlinLangScriptEngine engine = graph.getGremlinJavaEngine();
    final SimpleBindings bindings = new SimpleBindings();
    bindings.put("g", graph.traversal());
    final Object result = engine.eval("g.V().where(outE('KNOWS').count().is(gt(1)))", bindings);

    assertThat(result).isInstanceOf(Traversal.class);
    final Traversal<?, ?> stringPathTraversal = (Traversal<?, ?>) result;

    assertThat(TraversalPlans.hasStepOfType(stringPathTraversal, ArcadeEdgeCountFilterStep.class))
        .as("plan was: %s",
            describeWithChildren(
                (Traversal<?, ?>) engine.eval("g.V().where(outE('KNOWS').count().is(gt(1)))", bindings)))
        .isFalse();
  }

  @Test
  void greaterThanMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().where(outE("KNOWS").count().is(P.gt(1))).values("name"));
  }

  @Test
  void equalToZeroMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().where(outE("KNOWS").count().is(P.eq(0))).values("name"));
  }

  @Test
  void equalToOneMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().where(outE("KNOWS").count().is(P.eq(1))).values("name"));
  }

  @Test
  void lessThanMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().where(outE("KNOWS").count().is(P.lt(2))).values("name"));
  }

  @Test
  void greaterThanOrEqualMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().where(outE("KNOWS").count().is(P.gte(1))).values("name"));
  }

  @Test
  void anUnlabeledEdgeCountMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().where(outE().count().is(P.gt(0))).values("name"));
  }

  /** Recursive plan dump used only for diagnostics: TraversalPlans.describe() is intentionally flat. */
  private static String describeWithChildren(final Traversal<?, ?> traversal) {
    final StringBuilder sb = new StringBuilder();
    for (final Object step : TraversalPlans.stepsOf(traversal)) {
      sb.append(step.getClass().getSimpleName());
      if (step instanceof TraversalParent parent) {
        for (final Object child : parent.getLocalChildren()) {
          sb.append('[');
          for (final Step<?, ?> childStep : ((Traversal.Admin<?, ?>) child).getSteps())
            sb.append(childStep.getClass().getSimpleName()).append(' ');
          sb.append(']');
        }
      }
      sb.append(" -> ");
    }
    return sb.toString();
  }
}
