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
import com.arcadedb.gremlin.support.TraversalPlans;
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression coverage for issue #5840: a Gremlin query submitted as a STRING -- the entry point used by
 * the HTTP endpoint, Studio, and the drivers ({@link ArcadeGraph#gremlin(String)}) -- must engage
 * GAV/CSR acceleration exactly like the fluent Java API does.
 * <p>
 * TinkerPop 3.8.1's {@code gremlin-lang} parser builds a {@code VertexStepPlaceholder} (not a
 * {@code VertexStep}) for any hop naming an edge label. {@code ArcadeTraversalStrategy} must run
 * strictly AFTER {@code GValueReductionStrategy} resolves those placeholders into concrete
 * {@code VertexStep}s, or the optimization silently never engages for labeled hops. The fluent Java API
 * (e.g. {@code graph.traversal().V().out("KNOWS")}) never builds a placeholder in the first place, which
 * is why the plan assertions in {@link ArcadeGAVStepsTest} and {@link GremlinGAVTest} did not catch this.
 */
class GremlinStringPathGAVTest {

  private ArcadeGraph         graph;
  private GraphAnalyticalView gav;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gav-string-path");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().getSchema().createEdgeType("KNOWS");
    graph.getDatabase().getSchema().createEdgeType("LIKES");

    // A -KNOWS-> B -KNOWS-> C, plus A -LIKES-> C.
    graph.getDatabase().transaction(() -> {
      final MutableVertex a = graph.getDatabase().newVertex("Person").set("name", "A").save();
      final MutableVertex b = graph.getDatabase().newVertex("Person").set("name", "B").save();
      final MutableVertex c = graph.getDatabase().newVertex("Person").set("name", "C").save();
      a.newEdge("KNOWS", b);
      b.newEdge("KNOWS", c);
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

  /**
   * Parses a query through the same {@link GremlinLangScriptEngine} the string entry point
   * ({@code ArcadeGremlin#executeStatement}) uses, without iterating it, so the compiled plan can be
   * inspected the same way the fluent-path tests inspect it via {@link TraversalPlans}.
   */
  private GraphTraversal<?, ?> parseWithoutExecuting(final String query) throws ScriptException {
    final GremlinLangScriptEngine engine = graph.getGremlinJavaEngine();
    final SimpleBindings bindings = new SimpleBindings();
    bindings.put("g", graph.traversal());
    final Object result = engine.eval(query, bindings);
    assertThat(result).isInstanceOf(GraphTraversal.class);
    return (GraphTraversal<?, ?>) result;
  }

  @Test
  void labeledOutHopSubmittedAsStringUsesTheGAVStep() throws ScriptException {
    final GraphTraversal<?, ?> traversal = parseWithoutExecuting("g.V().has('name','A').out('KNOWS')");
    assertThat(TraversalPlans.hasStepOfType(traversal, ArcadeGAVVertexStep.class))
        .as("plan was: %s", TraversalPlans.describe(traversal))
        .isTrue();
  }

  @Test
  void labeledInHopSubmittedAsStringUsesTheGAVStep() throws ScriptException {
    final GraphTraversal<?, ?> traversal = parseWithoutExecuting("g.V().has('name','B').in('KNOWS')");
    assertThat(TraversalPlans.hasStepOfType(traversal, ArcadeGAVVertexStep.class))
        .as("plan was: %s", TraversalPlans.describe(traversal))
        .isTrue();
  }

  @Test
  void labeledBothHopSubmittedAsStringUsesTheGAVStep() throws ScriptException {
    final GraphTraversal<?, ?> traversal = parseWithoutExecuting("g.V().has('name','B').both('KNOWS')");
    assertThat(TraversalPlans.hasStepOfType(traversal, ArcadeGAVVertexStep.class))
        .as("plan was: %s", TraversalPlans.describe(traversal))
        .isTrue();
  }

  @Test
  void labeledHopWithADifferentEdgeLabelSubmittedAsStringUsesTheGAVStep() throws ScriptException {
    final GraphTraversal<?, ?> traversal = parseWithoutExecuting("g.V().has('name','A').out('LIKES')");
    assertThat(TraversalPlans.hasStepOfType(traversal, ArcadeGAVVertexStep.class))
        .as("plan was: %s", TraversalPlans.describe(traversal))
        .isTrue();
  }

  @Test
  void labeledTwoHopChainSubmittedAsStringFusesIntoASingleStep() throws ScriptException {
    final GraphTraversal<?, ?> traversal = parseWithoutExecuting("g.V().has('name','A').out('KNOWS').out('KNOWS')");
    assertThat(TraversalPlans.hasStepOfType(traversal, ArcadeGAVFusedStep.class))
        .as("plan was: %s", TraversalPlans.describe(traversal))
        .isTrue();
  }

  @Test
  void labelLessMultiHopSubmittedAsStringFusesIntoASingleStep() throws ScriptException {
    // The one shape the issue observed already engaging ArcadeGAVFusedStep via the string path, but with
    // no committed test covering it.
    final GraphTraversal<?, ?> traversal = parseWithoutExecuting("g.V().has('name','A').out().out()");
    assertThat(TraversalPlans.hasStepOfType(traversal, ArcadeGAVFusedStep.class))
        .as("plan was: %s", TraversalPlans.describe(traversal))
        .isTrue();
  }

  @Test
  void labeledHopResultsMatchBetweenTheStringPathAndTheFluentPath() {
    final List<String> stringPathNames = new ArrayList<>();
    final var rs = graph.gremlin("g.V().has('name','A').out('KNOWS').values('name')").execute();
    while (rs.hasNext())
      stringPathNames.add(rs.next().getProperty("result"));

    final List<String> fluentPathNames = new ArrayList<>();
    graph.traversal().V().has("name", "A").out("KNOWS").values("name")
        .forEachRemaining(v -> fluentPathNames.add((String) v));

    assertThat(stringPathNames).containsExactlyInAnyOrderElementsOf(fluentPathNames);
    assertThat(stringPathNames).containsExactly("B");
  }
}
