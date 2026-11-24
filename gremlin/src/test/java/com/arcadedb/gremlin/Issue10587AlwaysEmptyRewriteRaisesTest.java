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
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

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
  // THE SITE DROPS ITS MESSAGE AFTER 20,000 TO 45,000 RUNS (MEASURED, ABOUT A SECOND): THIS BOUNDS A JVM THAT NEVER DOES
  private static final int MAX_WARM_UP_RUNS = 200_000;

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
        t -> t.select("a"), t -> t.dedup("a"),
        // THE PREDICATES OF has() AND is() OVER A VALUE OF ANOTHER TYPE: A COMPARISON THAT RAISED WOULD BE THE SAME HOLE
        t -> t.has("s", P.gt(1)), t -> t.has("n", P.lt("a")), t -> t.has("n", P.within("a", 1L)), t -> t.has("n", TextP.containing("x")),
        t -> t.has("s", TextP.regex("^x")), t -> t.has("n", P.between("a", "z")), t -> t.is(P.gt("a")), t -> t.is(TextP.startingWith("x")),
        t -> t.is(P.neq(null)), t -> t.has("n", P.not(P.eq("a"))), t -> t.has("n", TextP.notContaining("q")),
        // THE CASTS ARE FOR THE GENERICS ONLY: AN and() OF A NUMBER COMPARISON AND A TEXT PREDICATE
        t -> t.is(P.gt((Object) 1).and((P) TextP.containing("x"))), t -> t.has("s", P.gt(1).or(P.within("x"))));

    for (final Function<GraphTraversalSource, GraphTraversal<?, ?>> feeder : feeders)
      for (final Function<GraphTraversal<?, ?>, GraphTraversal<?, ?>> step : steps) {
        // IN FRONT OF THE FILTER, WITH NO BARRIER TO DRAIN IT
        assertSameAnswer(g -> step.apply(feeder.apply(g.withoutStrategies(LazyBarrierStrategy.class))).filter(__.limit(0)).count());
        // INSIDE THE FILTER'S CHILD, FED WHAT THE FEEDER PRODUCES
        assertSameAnswer(g -> feeder.apply(g).filter(step.apply(__.start()).limit(0)).count());
      }
  }

  @Test
  void aFilterInsideAChildWhoseParentRaisesAnswersAsWithoutTheStrategy() {
    // THE CHILD'S INPUT COMES FROM ITS PARENT'S PREFIX: WHEN THAT PREFIX IS NOT READ-ONLY, THE INPUT IS UNKNOWN, NOT AN ELEMENT
    assertSameAnswer(g -> g.withoutStrategies(LazyBarrierStrategy.class).V().math("1/0").union(__.filter(__.limit(0))).count());
    assertSameAnswer(g -> g.withoutStrategies(LazyBarrierStrategy.class).V().values("s").union(__.filter(__.limit(0)), __.identity()).count());
    assertSameAnswer(g -> g.withoutStrategies(LazyBarrierStrategy.class).V().values("s").union(__.out().filter(__.limit(0))).count());
    assertSameAnswer(g -> g.withoutStrategies(LazyBarrierStrategy.class).V().math("_ + 1").local(__.values("s").filter(__.limit(0))).count());
  }

  @Test
  void aGraphStepIdThatIsNotARidAnswersAsWithoutTheStrategy() {
    // A NUMBER IS SKIPPED BY ArcadeGraph.vertices(), A STRING IS PARSED AS A RID AND A MALFORMED ONE RAISES: BOTH MUST STAY AS THEY ARE
    assertSameAnswer(g -> g.V(1).filter(__.limit(0)).count());
    assertSameAnswer(g -> g.V(1L).out().filter(__.limit(0)).count());
    assertSameAnswer(g -> g.V("not-a-rid").filter(__.limit(0)).count());
    assertSameAnswer(g -> g.V().filter(__.V("not-a-rid").limit(0)).count());
    assertSameAnswer(g -> g.E("#999:0").filter(__.limit(0)).count());
    final Object rid = graph.traversal().V().has("n", 1L).id().next();
    assertSameAnswer(g -> g.V(rid.toString()).out().filter(__.limit(0)).count());
    assertSameAnswer(g -> g.V(graph.traversal().V().has("n", 1L).next()).out().filter(__.limit(0)).count());
  }

  /**
   * The comparison of two outcomes, with no JIT in it (issue #11997): whatever message a raised exception carries, or none, it is
   * the same answer as the same exception, and only another class, another message when both have one, or rows instead of an
   * exception tell two outcomes apart. The test below needs the JVM to drop a message and skips when it does not, so this one is
   * what keeps the comparison pinned on every run.
   */
  @Test
  void aRaisedExceptionIsTheSameAnswerWhateverMessageItCarries() {
    final Raised withMessage = new Raised(ClassCastException.class, "class java.lang.Integer cannot be cast to class Element");
    final Raised withoutMessage = new Raised(ClassCastException.class, null);

    // a hot site throws the same exception without its message, and either of the two runs can be the one that does
    assertThat(withMessage.sameAs(withoutMessage)).isTrue();
    assertThat(withoutMessage.sameAs(withMessage)).isTrue();
    assertThat(withoutMessage.sameAs(new Raised(ClassCastException.class, null))).isTrue();
    assertThat(withMessage.sameAs(new Raised(ClassCastException.class, withMessage.message()))).isTrue();
    assertSameOutcome(withMessage, withoutMessage, "the same exception, with and without its message");

    // still told apart: another exception, or the same one with another message
    assertThat(withMessage.sameAs(new Raised(IllegalStateException.class, null))).isFalse();
    assertThat(withoutMessage.sameAs(new Raised(IllegalStateException.class, "another message"))).isFalse();
    assertThat(withMessage.sameAs(new Raised(ClassCastException.class, "another message"))).isFalse();
    assertThatThrownBy(() -> assertSameOutcome(withMessage, new Raised(ClassCastException.class, "another message"), "another message"))
        .isInstanceOf(AssertionError.class);
    assertThatThrownBy(() -> assertSameOutcome(withoutMessage, new Raised(IllegalStateException.class, null), "another class"))
        .isInstanceOf(AssertionError.class);

    // rows are compared as they always were, and an exception is never rows
    assertSameOutcome(List.of(1L), List.of(1L), "the same rows");
    assertThatThrownBy(() -> assertSameOutcome(List.of(1L), List.of(2L), "other rows")).isInstanceOf(AssertionError.class);
    assertThatThrownBy(() -> assertSameOutcome(List.of(), withoutMessage, "no rows, then an exception")).isInstanceOf(AssertionError.class);
    assertThatThrownBy(() -> assertSameOutcome(withoutMessage, List.of(), "an exception, then no rows")).isInstanceOf(AssertionError.class);
  }

  /**
   * Once a throwing site is hot enough to be compiled, HotSpot raises a preallocated exception there, without a message
   * (-XX:+OmitStackTraceInFastThrow, on by default), and the run with the strategy and the run without it can fall on either
   * side of that compilation (issue #11997). This is that pair, produced by the JVM itself: the first run of a site is cold and
   * carries its message, a later one is hot and does not, and they are the same answer. The site is one no other test here feeds a
   * non-element. When one already has, or the JVM never drops the message, there is no pair to compare and the test skips: the
   * comparison itself is pinned without the JVM by {@link #aRaisedExceptionIsTheSameAnswerWhateverMessageItCarries()}.
   */
  @Test
  void anExceptionIsTheSameAnswerWithOrWithoutItsMessage() {
    final Function<GraphTraversalSource, GraphTraversal<?, ?>> query = g -> g.inject(1).elementMap();
    final Object cold = outcome(query, graph.traversal());
    assumeThat(cold instanceof Raised raised && raised.message() != null).as("the site was already hot before this test").isTrue();

    Object hot = cold;
    for (int run = 0; run < MAX_WARM_UP_RUNS && !(hot instanceof Raised raised && raised.message() == null); run++)
      hot = outcome(query, graph.traversal());
    assumeThat(hot instanceof Raised raised && raised.message() == null).as("this JVM never dropped the message of a hot exception")
        .isTrue();

    assertSameOutcome(cold, hot, query.apply(graph.traversal()));
  }

  private void assertSameAnswer(final Function<GraphTraversalSource, GraphTraversal<?, ?>> query) {
    final Object withStrategy = outcome(query, graph.traversal());
    final Object withoutStrategy = outcome(query, graph.traversal().withoutStrategies(ArcadeTraversalStrategy.class));
    assertSameOutcome(withStrategy, withoutStrategy, query.apply(graph.traversal()));
  }

  private static void assertSameOutcome(final Object withStrategy, final Object withoutStrategy, final Object query) {
    if (withStrategy instanceof Raised raised && withoutStrategy instanceof Raised other)
      assertThat(raised.sameAs(other)).as("%s%n with the strategy: %s%n without it: %s", query, raised, other).isTrue();
    else
      assertThat(withStrategy).as("%s", query).isEqualTo(withoutStrategy);
  }

  private void assertRaises(final Function<GraphTraversalSource, GraphTraversal<?, ?>> query) {
    assertThat(outcome(query, graph.traversal())).isInstanceOf(Raised.class);
  }

  /** The rows of a traversal, or the {@link Raised} exception it ended with. */
  private static Object outcome(final Function<GraphTraversalSource, GraphTraversal<?, ?>> query, final GraphTraversalSource g) {
    try {
      return query.apply(g).toList();
    } catch (final RuntimeException e) {
      return new Raised(e.getClass(), e.getMessage());
    }
  }

  /**
   * An exception a traversal raised. Two are the same answer when they are of the same class and, if both carry a message, the
   * same message: a hot site throws without one (see {@link #anExceptionIsTheSameAnswerWithOrWithoutItsMessage()}).
   */
  private record Raised(Class<?> type, String message) {
    boolean sameAs(final Raised other) {
      return type == other.type && (message == null || other.message == null || message.equals(other.message));
    }

    @Override
    public String toString() {
      return "raised " + type.getName() + ": " + message;
    }
  }
}
