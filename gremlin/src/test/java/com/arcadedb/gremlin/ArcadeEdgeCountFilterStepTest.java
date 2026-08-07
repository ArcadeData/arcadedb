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

  /**
   * Was {@code theDegreeFilterStepIsInstalled}, asserting {@code .isTrue()}. Inverted by the #5840 fix
   * (PR #5899): {@link ArcadeTraversalStrategy} moved to {@code TraversalStrategy.ProviderOptimizationStrategy}
   * so it deterministically runs after every {@code OptimizationStrategy}, including TinkerPop's
   * {@code CountStrategy} - the opposite ordering #5841 had arranged via an explicit
   * {@code applyPost(CountStrategy.class)}, which is gone (it does not type-check under the new category
   * and would no longer help: category ordering is unconditional and now always places
   * {@code CountStrategy} first). {@code CountStrategy} rewrites the bounded-predicate shape before this
   * strategy's {@code applyEdgeCountFilterOptimization} gets a chance to pattern-match it, so the O(1)
   * degree-check step never installs for {@code where(outE(X).count().is(boundedPredicate))} - reliably
   * now, instead of on roughly half of JVM processes as before #5841. Results stay correct via the OLTP
   * fallback either way, see the {@code *MatchesTheUnoptimizedPath} differential tests below. See
   * docs/5840-gav-csr-labeled-gremlin-string-traversals.md ("Known trade-off") for the full analysis;
   * extending {@code applyEdgeCountFilterOptimization} to recognize {@code CountStrategy}'s rewritten
   * shapes is tracked there as follow-up work, not done here.
   */
  @Test
  void theDegreeFilterStepDoesNotInstallForBoundedPredicates() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().where(outE("KNOWS").count().is(P.gt(1))), ArcadeEdgeCountFilterStep.class))
        .as("plan was: %s",
            TraversalPlans.describe(graph.traversal().V().where(outE("KNOWS").count().is(P.gt(1)))))
        .isFalse();
  }

  @Test
  void countStrategyExplainsWhyTheDegreeFilterStepDoesNotInstall() {
    // Confirms the mechanism, not just the outcome: the where-child must show CountStrategy's
    // RangeGlobalStep fingerprint, proving CountStrategy's bounded-predicate rewrite is what pre-empts
    // ArcadeEdgeCountFilterStep, as opposed to some unrelated regression producing the same absence.
    final String plan = describeWithChildren(graph.traversal().V().where(outE("KNOWS").count().is(P.gt(1))));
    assertThat(plan)
        .as("expected TinkerPop's CountStrategy to have inserted a RangeGlobalStep ahead of the "
            + "degree-check step, plan: %s", plan)
        .contains("RangeGlobalStep");
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
