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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the index-backed rewrite. Ages are 10, 20, 30, 40, 50 and every range predicate is probed
 * AT a stored boundary value, so an inverted inclusive/exclusive flag on the index cursor changes
 * the result set and the differential comparison catches it. Also covers the multi-cursor
 * intersection path (including a case where the indexed predicate alone still leaves more than one
 * candidate row) and confirms the rewrite installs equally whether the traversal arrives via the
 * fluent Java API or is parsed from a string through gremlin-lang.
 */
class ArcadeFilterByIndexStepTest {

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-filterbyindex");
    final var personType = graph.getDatabase().getSchema().createVertexType("Person");
    personType.createProperty("age", Type.INTEGER);
    personType.createProperty("city", Type.STRING);
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "age");
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "city");

    graph.getDatabase().transaction(() -> {
      final int[] ages = { 10, 20, 30, 40, 50 };
      final String[] cities = { "Rome", "Rome", "Milan", "Milan", "Turin" };
      for (int i = 0; i < ages.length; i++) {
        final ArcadeVertex v = graph.addVertex("Person");
        v.property("name", "P" + ages[i]);
        v.property("age", ages[i]);
        v.property("city", cities[i]);
      }
    });
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void anIndexedEqualityInstallsTheIndexStep() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().hasLabel("Person").has("age", 30), ArcadeFilterByIndexStep.class))
        .as("plan was: %s",
            TraversalPlans.describe(graph.traversal().V().hasLabel("Person").has("age", 30)))
        .isTrue();
  }

  @Test
  void equalityMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", 30).values("name"));
  }

  @Test
  void greaterThanAtABoundaryValueMatchesTheUnoptimizedPath() {
    // Plan-shape guard: a future change that skipped the rewrite for non-eq predicates would leave the
    // differential assertion below green (both paths would run through the same unoptimized code) while
    // coverage of the index rewrite itself silently vanished. Assert the rewrite installs before
    // asserting its results agree.
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().hasLabel("Person").has("age", P.gt(30)), ArcadeFilterByIndexStep.class))
        .as("plan was: %s",
            TraversalPlans.describe(graph.traversal().V().hasLabel("Person").has("age", P.gt(30))))
        .isTrue();
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.gt(30)).values("name"));
  }

  @Test
  void greaterThanOrEqualAtABoundaryValueMatchesTheUnoptimizedPath() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().hasLabel("Person").has("age", P.gte(30)), ArcadeFilterByIndexStep.class))
        .as("plan was: %s",
            TraversalPlans.describe(graph.traversal().V().hasLabel("Person").has("age", P.gte(30))))
        .isTrue();
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.gte(30)).values("name"));
  }

  @Test
  void lessThanAtABoundaryValueMatchesTheUnoptimizedPath() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().hasLabel("Person").has("age", P.lt(30)), ArcadeFilterByIndexStep.class))
        .as("plan was: %s",
            TraversalPlans.describe(graph.traversal().V().hasLabel("Person").has("age", P.lt(30))))
        .isTrue();
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.lt(30)).values("name"));
  }

  @Test
  void lessThanOrEqualAtABoundaryValueMatchesTheUnoptimizedPath() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().hasLabel("Person").has("age", P.lte(30)), ArcadeFilterByIndexStep.class))
        .as("plan was: %s",
            TraversalPlans.describe(graph.traversal().V().hasLabel("Person").has("age", P.lte(30))))
        .isTrue();
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.lte(30)).values("name"));
  }

  @Test
  void greaterThanTheMaximumReturnsNothing() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.gt(50)).values("name"));
  }

  @Test
  void lessThanTheMinimumReturnsNothing() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.lt(10)).values("name"));
  }

  @Test
  void twoIndexedPredicatesIntersectCorrectly() {
    // Exercises the multi-cursor intersection loop, where cursor 0 seeds the set and later cursors
    // retainAll. A wrong intersection silently widens or empties the result.
    // Plan-shape guard first: see the comment on greaterThanAtABoundaryValueMatchesTheUnoptimizedPath.
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().hasLabel("Person").has("city", "Rome").has("age", 20), ArcadeFilterByIndexStep.class))
        .as("plan was: %s",
            TraversalPlans.describe(graph.traversal().V().hasLabel("Person").has("city", "Rome").has("age", 20)))
        .isTrue();
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("city", "Rome").has("age", 20).values("name"));
  }

  @Test
  void twoIndexedPredicatesWithAnEmptyIntersectionReturnNothing() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().hasLabel("Person").has("city", "Turin").has("age", 10), ArcadeFilterByIndexStep.class))
        .as("plan was: %s",
            TraversalPlans.describe(graph.traversal().V().hasLabel("Person").has("city", "Turin").has("age", 10)))
        .isTrue();
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("city", "Turin").has("age", 10).values("name"));
  }

  @Test
  void aRedundantUnindexedPredicateOnAnAlreadyUniqueIndexedMatchStillAgrees() {
    // age=30 alone already narrows to exactly one row (P30), so the unindexed name predicate that
    // follows is redundant here: it confirms the residual HasStep does not reject a genuine match, but
    // a residual HasStep that was silently dropped or broken would still pass this test, because the
    // index cursor alone already produced the right single row. See
    // anIndexedPredicateWithMultipleCandidatesPlusAnUnindexedPredicateDiscriminate for the case that
    // actually requires the unindexed predicate to do work.
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", 30).has("name", "P30").values("name"));
  }

  @Test
  void anIndexedPredicateWithMultipleCandidatesPlusAnUnindexedPredicateDiscriminate() {
    // city=Rome alone must leave more than one candidate (P10 and P20), otherwise the unindexed name
    // predicate that follows never gets a chance to discriminate and a broken or dropped residual
    // HasStep would go unnoticed, as it did in the test above.
    final List<Object> cityOnly = DifferentialTraversal.on(graph)
        .optimized(g -> g.V().hasLabel("Person").has("city", "Rome").values("name"));
    assertThat(cityOnly)
        .as("city=Rome must narrow to more than one row for this test to be meaningful")
        .containsExactlyInAnyOrder("P10", "P20");

    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("city", "Rome").has("name", "P20").values("name"));
  }

  @Test
  void aRangeAndAnEqualityCombineCorrectly() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().hasLabel("Person").has("city", "Milan").has("age", P.gte(30)), ArcadeFilterByIndexStep.class))
        .as("plan was: %s",
            TraversalPlans.describe(graph.traversal().V().hasLabel("Person").has("city", "Milan").has("age", P.gte(30))))
        .isTrue();
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("city", "Milan").has("age", P.gte(30)).values("name"));
  }

  @Test
  void theIndexRewriteEngagesWhenTheQueryArrivesAsAString() throws Exception {
    // graph.gremlin(String) parses through gremlin-lang, a different front end from the fluent Java
    // API every other test in this class uses. Task 4/5 found that the GAV VertexStep rewrite is blind
    // on this path: gremlin-lang builds a VertexStepPlaceholder for labeled hops that
    // GValueReductionStrategy only resolves after ArcadeTraversalStrategy has already run, so the
    // rewrite's "step instanceof VertexStep" check never matches. This test establishes, as a committed
    // regression guard rather than a deleted throwaway probe, that the INDEX rewrite does not share that
    // gap: it asserts ArcadeFilterByIndexStep is actually installed in the plan gremlin-lang builds.
    //
    // ArcadeGraph.getGremlinJavaEngine() is the same GremlinLangScriptEngine that
    // ArcadeGremlin.executeStatement() calls internally for the "java" engine (the default, and the
    // first one "auto" tries), so this test drives the traversal through the identical parsing entry
    // point graph.gremlin(String) uses in production. What it does NOT prove: it does not go through
    // ArcadeGremlin/ResultSet itself, so it does not exercise result serialization or the "auto"
    // fallback to Groovy.
    final GremlinLangScriptEngine engine = graph.getGremlinJavaEngine();
    final SimpleBindings bindings = new SimpleBindings();
    bindings.put("g", graph.traversal());
    final Object result = engine.eval("g.V().hasLabel('Person').has('age', 30)", bindings);

    assertThat(result).isInstanceOf(Traversal.class);
    final Traversal<?, ?> stringPathTraversal = (Traversal<?, ?>) result;

    assertThat(TraversalPlans.hasStepOfType(stringPathTraversal, ArcadeFilterByIndexStep.class))
        .as("plan was: %s", TraversalPlans.describe(stringPathTraversal))
        .isTrue();
  }

  @Test
  void theStringEntryPointProducesTheSameRowsAsTheFluentPath() {
    // Complements the plan-shape assertion above with an end-to-end behavioral check: executes the
    // query through the actual public graph.gremlin(String) entry point (unlike the test above, this
    // one does go through ArcadeGremlin/ResultSet) and compares against the fluent-API optimized path.
    // This does not by itself prove which step ran on the string path - the plan-shape test above is
    // what proves that - but it confirms the string path's answer is not merely plausible, it is
    // identical to the fluent path's.
    final List<Object> stringPathNames = new ArrayList<>();
    final ResultSet resultSet = graph.gremlin("g.V().hasLabel('Person').has('age', 30).values('name')").execute();
    while (resultSet.hasNext()) {
      final Result row = resultSet.next();
      stringPathNames.add(row.getProperty("result"));
    }

    final List<Object> fluentPathNames = DifferentialTraversal.on(graph)
        .optimized(g -> g.V().hasLabel("Person").has("age", 30).values("name"));

    assertThat(stringPathNames).containsExactlyInAnyOrderElementsOf(fluentPathNames);
  }
}
