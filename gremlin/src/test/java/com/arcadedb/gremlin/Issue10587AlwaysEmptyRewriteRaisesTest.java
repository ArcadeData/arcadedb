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

import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10587: the always-empty filter rewrite of #10307 replaced {@code filter(...limit(0))} with a
 * {@code limit(0)} whenever no step in front of it, or in its child, was on a list of the steps known to have an effect. Raising an
 * exception is an effect of running almost any step ({@code math()} on a division by zero, on an unbound variable, on a value that is
 * not a number), so the rewrite turned an exception TinkerPop raises into an empty answer. The rewrite now applies only when every step
 * is on a list of the steps known to do nothing but read, and each query here must answer exactly what it answers without the strategy:
 * the same rows, or the same exception.
 */
class Issue10587AlwaysEmptyRewriteRaisesTest {
  private static final String PATH = "./target/test-gremlin-10587-empty-filter-raises";

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(PATH));
    graph = ArcadeGraph.open(PATH);
    graph.getDatabase().getSchema().createVertexType("P");
    graph.getDatabase().getSchema().createEdgeType("E");
    final Vertex x = graph.addVertex(T.label, "P", "n", 1L, "s", "x");
    final Vertex y = graph.addVertex(T.label, "P", "n", 2L, "s", "y");
    final Vertex z = graph.addVertex(T.label, "P", "n", 3L, "s", "z");
    x.addEdge("E", y, "w", 1L);
    y.addEdge("E", z, "w", 2L);
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

  @Test
  void aDivisionByZeroInsideTheFilterChildStillRaises() {
    assertSameAnswer(g -> g.V().filter(__.math("1/0").limit(0)).count());
    assertSameAnswer(g -> g.V().where(__.math("1/0").limit(0)).count());
    assertRaises(g -> g.V().filter(__.math("1/0").limit(0)).count());
  }

  @Test
  void anUnboundMathVariableInsideTheFilterChildStillRaises() {
    assertSameAnswer(g -> g.V().filter(__.math("x").limit(0)).count());
    assertRaises(g -> g.V().filter(__.math("x").limit(0)).count());
  }

  @Test
  void aMathOverAStringInsideAModulatorOfTheFilterChildStillRaises() {
    // DATA DEPENDENT: THE SAME QUERY OVER n INSTEAD OF s ANSWERS 0 ROWS IN BOTH
    assertSameAnswer(g -> g.V().filter(__.values("s").order().by(__.math("_")).limit(0)).count());
    assertRaises(g -> g.V().filter(__.values("s").order().by(__.math("_")).limit(0)).count());
    assertSameAnswer(g -> g.V().filter(__.values("n").order().by(__.math("_")).limit(0)).count());
  }

  @Test
  void aThrowingStepInFrontOfTheFilterStillRaisesWithoutABarrier() {
    // NO LazyBarrierStrategy: NOTHING IN FRONT OF THE limit(0) DRAINS THE STEPS BEFORE IT, SO ONLY THE REWRITE DECIDES WHETHER THEY RUN
    final Function<GraphTraversalSource, GraphTraversal<?, ?>> query = g -> g.V().math("1/0").filter(__.limit(0)).count();
    assertSameAnswer(g -> query.apply(g.withoutStrategies(LazyBarrierStrategy.class)));
    assertRaises(g -> query.apply(g.withoutStrategies(LazyBarrierStrategy.class)));
  }

  @Test
  void aStepThatReadsANonElementStillRaises() {
    // values() OVER A NUMBER IS A TYPE ERROR TINKERPOP RAISES: THE STEP IS ONE THAT ONLY READS, BUT NOT OVER WHAT inject() FEEDS IT
    assertSameAnswer(g -> g.inject(1).out().filter(__.limit(0)).count());
    assertSameAnswer(g -> g.inject(1).values("s").filter(__.limit(0)).count());
    assertSameAnswer(g -> g.V().values("s").filter(__.out().limit(0)).count());
    assertSameAnswer(g -> g.V().id().filter(__.values("s").limit(0)).count());
  }

  @Test
  void aReadOnlyFilterIsStillRewrittenAndAnswersTheSame() {
    assertSameAnswer(g -> g.V().out().has("n", 1L).filter(__.values("s").limit(0)).count());
    assertSameAnswer(g -> g.V().filter(__.out().dedup().limit(0)).count());
    assertSameAnswer(g -> g.V().hasLabel("P").where(__.limit(0)).fold());

    // THE PLAN OF A READ-ONLY FILTER STILL STOPS AT THE limit(0): THE REWRITE IS STILL THERE FOR WHAT IT CAN PROVE
    final String plan = graph.traversal().V().hasLabel("P").out().filter(__.values("s").limit(0)).count().explain().prettyPrint();
    assertThat(plan.substring(plan.lastIndexOf("Final Traversal"))).doesNotContain("TraversalFilterStep");
  }

  /**
   * The guarantee the rewrite has to keep, step by step: whatever kind of traverser a step is fed (a vertex, an edge, a number, a
   * string, a property), in front of an always-empty filter or inside its child, the answer with the strategy is the answer without
   * it, rows or exception. A step the rewrite does not know is never rewritten, so a step added to TinkerPop later cannot fail this.
   */
  @Test
  void everyStepFedEveryKindOfTraverserAnswersAsWithoutTheStrategy() {
    final List<Function<GraphTraversalSource, GraphTraversal<?, ?>>> feeders = List.of(//
        g -> g.V(), g -> g.E(), g -> g.inject(1), g -> g.V().values("s"), g -> g.V().id(), g -> g.V().properties("s"),
        g -> g.V().outE().values("w"));
    final List<Function<GraphTraversal<?, ?>, GraphTraversal<?, ?>>> steps = List.of(//
        t -> t.out(), t -> t.outE(), t -> t.inV(), t -> t.otherV(), t -> t.values("s"), t -> t.properties(), t -> t.id(),
        t -> t.label(), t -> t.has("n", 1L), t -> t.hasLabel("P"), t -> t.is(1), t -> t.dedup(), t -> t.identity(), t -> t.constant(1),
        t -> t.count(), t -> t.V(), t -> t.math("_"), t -> t.math("1/0"), t -> t.order().by(__.math("_")), t -> t.not(__.out()),
        t -> t.local(__.out()), t -> t.union(__.out(), __.identity()), t -> t.filter(__.values("s")), t -> t.value(), t -> t.key(),
        t -> t.select("a"), t -> t.dedup("a"));

    for (final Function<GraphTraversalSource, GraphTraversal<?, ?>> feeder : feeders)
      for (final Function<GraphTraversal<?, ?>, GraphTraversal<?, ?>> step : steps) {
        // IN FRONT OF THE FILTER, WITH NO BARRIER TO DRAIN IT
        assertSameAnswer(g -> step.apply(feeder.apply(g.withoutStrategies(LazyBarrierStrategy.class))).filter(__.limit(0)).count());
        // INSIDE THE FILTER'S CHILD, FED WHAT THE FEEDER PRODUCES
        assertSameAnswer(g -> feeder.apply(g).filter(step.apply(__.start()).limit(0)).count());
      }
  }

  private void assertSameAnswer(final Function<GraphTraversalSource, GraphTraversal<?, ?>> query) {
    final Object withStrategy = outcome(query, graph.traversal());
    final Object withoutStrategy = outcome(query, graph.traversal().withoutStrategies(ArcadeTraversalStrategy.class));
    assertThat(withStrategy).as("%s", query.apply(graph.traversal())).isEqualTo(withoutStrategy);
  }

  private void assertRaises(final Function<GraphTraversalSource, GraphTraversal<?, ?>> query) {
    assertThat(outcome(query, graph.traversal())).asString().startsWith("raised ");
  }

  private static Object outcome(final Function<GraphTraversalSource, GraphTraversal<?, ?>> query, final GraphTraversalSource g) {
    try {
      final List<?> rows = query.apply(g).toList();
      return rows;
    } catch (final RuntimeException e) {
      return "raised " + e.getClass().getName() + ": " + e.getMessage();
    }
  }
}
