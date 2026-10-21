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

import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #10307 (child of #10309): a {@code filter(__.limit(0))} (or {@code where(__.limit(0))}) can never let a
 * traverser through, yet the traversal expanded every neighbor of the vertex in front of it only for the filter to drop each one.
 * When nothing in front of the filter has an effect of its own, the filter now stops the walk where {@code limit(0)} does.
 * <p>
 * The work is measured in records looked up (the database's own {@code existsRecord} and {@code readRecord} counters), never in wall
 * clock time. The cases that must NOT take the shortcut (a mutation or a side effect in front of the filter) pin that the traversal
 * still does everything TinkerPop's does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10307AlwaysEmptyFilterTest {
  private static final String PATH    = "./target/test-gremlin-10307-empty-filter";
  private static final int    TARGETS = 3_000;
  private static final long   BUDGET  = 200;

  private ArcadeGraph graph;
  private Vertex      root;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(PATH));
    graph = ArcadeGraph.open(PATH);
    final Schema schema = graph.getDatabase().getSchema();
    schema.createVertexType("Root");
    schema.createVertexType("Target");
    schema.createEdgeType("High");

    root = graph.addVertex(T.label, "Root", "uid", 1L);
    for (int i = 0; i < TARGETS; i++)
      root.addEdge("High", graph.addVertex(T.label, "Target", "uid", (long) i));
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

  private long lookups() {
    final var stats = graph.getDatabase().getStats();
    return ((Number) stats.get("existsRecord")).longValue() + ((Number) stats.get("readRecord")).longValue();
  }

  @Test
  void anAlwaysEmptyFilterAfterTheExpansionReadsNoNeighbor() {
    final GraphTraversalSource g = graph.traversal();
    final long before = lookups();

    final List<Map<String, Object>> rows = g.inject(1).V().hasLabel("Root").has("uid", 1L).as("v0").outE("High").inV().hasLabel("Target").as("v1")
        .filter(__.limit(0)).<Object>project("c0").by(__.constant(1L)).toList();

    assertThat(rows).isEmpty();
    assertThat(lookups() - before).isLessThan(BUDGET);
  }

  @Test
  void anAlwaysEmptyWhereAfterTheExpansionReadsNoNeighbor() {
    final GraphTraversalSource g = graph.traversal();
    final long before = lookups();

    final List<Vertex> rows = g.V(root.id()).out("High").where(__.limit(0)).toList();

    assertThat(rows).isEmpty();
    assertThat(lookups() - before).isLessThan(BUDGET);
  }

  @Test
  void anAlwaysEmptyFilterBehindAPathOfFiltersReadsNoNeighbor() {
    final GraphTraversalSource g = graph.traversal();
    final long before = lookups();

    final List<Vertex> rows = g.V(root.id()).out("High").has("uid", P.gte(0L)).filter(__.out().limit(0)).toList();

    assertThat(rows).isEmpty();
    assertThat(lookups() - before).isLessThan(BUDGET);
  }

  @Test
  void anAlwaysEmptyFilterInsideAChildTraversalSpendsNothingEither() {
    final GraphTraversalSource g = graph.traversal();
    final long before = lookups();

    final Long count = g.V(root.id()).union(__.out("High").filter(__.limit(0)), __.identity()).count().next();

    assertThat(count).isEqualTo(1L);
    assertThat(lookups() - before).isLessThan(BUDGET);
  }

  @Test
  void theResultIsTheSameAsWithoutTheStrategy() {
    final GraphTraversalSource g = graph.traversal();
    final GraphTraversalSource plain = graph.traversal().withoutStrategies(ArcadeTraversalStrategy.class);

    assertThat(g.V(root.id()).out("High").filter(__.limit(0)).toList())
        .isEqualTo(plain.V(root.id()).out("High").filter(__.limit(0)).toList()).isEmpty();
    assertThat(g.V(root.id()).out("High").filter(__.limit(0)).count().next()).isZero();
    assertThat(g.V(root.id()).out("High").where(__.limit(0)).fold().next()).isEmpty();
    assertThat(g.V(root.id()).out("High").filter(__.limit(0)).fold().next()).isEmpty();
  }

  @Test
  void aFilterThatCanLetTrafficThroughIsNotTouched() {
    final GraphTraversalSource g = graph.traversal();

    assertThat(g.V(root.id()).out("High").filter(__.limit(1)).count().next()).isEqualTo((long) TARGETS);
    assertThat(g.V(root.id()).out("High").filter(__.identity().limit(1)).count().next()).isEqualTo((long) TARGETS);
    assertThat(g.V(root.id()).out("High").filter(__.limit(Long.MAX_VALUE)).count().next()).isEqualTo((long) TARGETS);
  }

  @Test
  void aMutationInFrontOfTheFilterStillRuns() {
    final GraphTraversalSource g = graph.traversal();

    // TINKERPOP'S ANSWER: THE VERTEX IS ADDED, THE FILTER DROPS THE TRAVERSER AFTERWARDS
    g.addV("Target").property("uid", -1L).filter(__.limit(0)).iterate();
    graph.tx().commit();

    assertThat(graph.traversal().V().hasLabel("Target").has("uid", -1L).count().next()).isEqualTo(1L);
  }

  @Test
  void aSideEffectInFrontOfTheFilterStillCollectsEveryNeighbor() {
    final GraphTraversalSource g = graph.traversal();

    final Object collected = g.V(root.id()).out("High").aggregate("x").filter(__.limit(0)).cap("x").next();

    assertThat((Collection<?>) collected).hasSize(TARGETS);
  }

  @Test
  void aPropertyWriteInFrontOfTheFilterStillRunsForEveryNeighbor() {
    final GraphTraversalSource g = graph.traversal();

    g.V(root.id()).out("High").property("seen", true).filter(__.limit(0)).iterate();
    graph.tx().commit();

    assertThat(graph.traversal().V().has("seen", true).count().next()).isEqualTo((long) TARGETS);
  }

  @Test
  void aLambdaInFrontOfTheFilterStillSeesEveryNeighbor() {
    final GraphTraversalSource g = graph.traversal();
    final AtomicLong seen = new AtomicLong();

    g.V(root.id()).out("High").sideEffect(t -> seen.incrementAndGet()).filter(__.limit(0)).iterate();

    assertThat(seen.get()).isEqualTo(TARGETS);
  }

  @Test
  void aFailStepNeverIsOptimizedAway() {
    final GraphTraversalSource g = graph.traversal();

    // BOTH PLACES A fail() CAN BE: IN FRONT OF THE FILTER AND INSIDE ITS CHILD. TINKERPOP RAISES THE EXCEPTION ON THE FIRST TRAVERSER
    assertThatThrownBy(() -> g.V(root.id()).out("High").fail().filter(__.limit(0)).toList()).isInstanceOf(RuntimeException.class)
        .hasMessageContaining("fail");
    assertThatThrownBy(() -> g.V(root.id()).filter(__.fail().limit(0)).toList()).isInstanceOf(RuntimeException.class)
        .hasMessageContaining("fail");
  }

  @Test
  void aMutationInsideTheFilterChildStillRunsForEveryTraverser() {
    final GraphTraversalSource g = graph.traversal();

    // THE CHILD'S STEPS IN FRONT OF ITS limit(0) RUN ONCE FOR EACH TRAVERSER THE FILTER IS OFFERED: ONE HERE, THE ROOT
    g.V(root.id()).filter(__.sideEffect(__.addV("Target").property("uid", -3L)).limit(0)).iterate();
    graph.tx().commit();

    assertThat(graph.traversal().V().hasLabel("Target").has("uid", -3L).count().next()).isEqualTo(1L);
  }

  @Test
  void aLambdaInsideTheFilterChildStillSeesEveryTraverser() {
    final GraphTraversalSource g = graph.traversal();
    final AtomicLong seenByFilter = new AtomicLong();
    final AtomicLong seenByWhere = new AtomicLong();

    g.V(root.id()).out("High").filter(__.sideEffect(t -> seenByFilter.incrementAndGet()).limit(0)).iterate();
    g.V(root.id()).out("High").where(__.sideEffect(t -> seenByWhere.incrementAndGet()).limit(0)).iterate();

    assertThat(seenByFilter.get()).isEqualTo(TARGETS);
    assertThat(seenByWhere.get()).isEqualTo(TARGETS);
  }

  @Test
  void aMutationNestedInFrontOfTheFilterStillRuns() {
    final GraphTraversalSource g = graph.traversal();

    g.V(root.id()).sideEffect(__.addV("Target").property("uid", -2L)).filter(__.limit(0)).iterate();
    graph.tx().commit();

    assertThat(graph.traversal().V().hasLabel("Target").has("uid", -2L).count().next()).isEqualTo(1L);
  }
}
