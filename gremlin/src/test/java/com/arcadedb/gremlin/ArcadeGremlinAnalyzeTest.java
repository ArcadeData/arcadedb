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

import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers ArcadeGremlin.parse(). Its isIdempotent() verdict drives HA follower routing
 * (RaftReplicatedDatabase consults only isIdempotent()/isDDL(), never getOperationTypes()); the
 * placeholder step types gremlin-lang hands to parse() DO implement Mutating (via Writing/Deleting,
 * javap-verified), so isIdempotent() is correct and HA routing is unaffected. getOperationTypes() used to
 * be a separate story: it over-widened for addV/addE/property because the placeholder step types are not
 * subtypes of the concrete step classes parse() checked for (see
 * mutatingStepsSeenByAnalysisArePlaceholdersNotResolvedSteps) - fixed in #5838 by checking the
 * AddVertexStepContract/AddEdgeStepContract/AddPropertyStepContract interfaces both the placeholder and the
 * concrete steps implement, instead of the concrete classes alone.
 */
class ArcadeGremlinAnalyzeTest {

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-analyze");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().getSchema().createEdgeType("KNOWS");
    graph.getDatabase().transaction(() -> graph.addVertex("Person").property("name", "Alice"));
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  private QueryEngine.AnalyzedQuery analyze(final String query) {
    return graph.gremlin(query).parse();
  }

  /**
   * Reflects into the private {@code ArcadeGremlin.executeStatement(boolean)} to inspect exactly what
   * step types reach {@code parse()}'s instanceof checks, without triggering iteration (parse() itself
   * never calls hasNext()/next() before reading getSteps(), so this mirrors it precisely). This directly
   * confirms TinkerPop 3.8.1's gremlin-lang parser hands parse() placeholder step types (e.g.
   * AddVertexStartStepPlaceholder, which does NOT extend AddVertexStep) rather than the concrete classes.
   * The placeholders DO implement Mutating (via Writing/Deleting, javap-verified), so isIdempotent() is
   * correct and HA routing is unaffected; getOperationTypes() is fixed by checking against the
   * AddVertexStepContract/AddEdgeStepContract/AddPropertyStepContract interfaces the placeholders
   * implement (see addVertexOperationTypeIsExactlyCreate and siblings, and #5838).
   */
  private List<String> analysisStepClassNames(final String query) throws Exception {
    final ArcadeGremlin gremlin = graph.gremlin(query);
    final Method method = ArcadeGremlin.class.getDeclaredMethod("executeStatement", boolean.class);
    method.setAccessible(true);
    final DefaultGraphTraversal<?, ?> traversal = (DefaultGraphTraversal<?, ?>) method.invoke(gremlin, true);
    final List<String> names = new ArrayList<>();
    for (final Object step : traversal.getSteps())
      names.add(step.getClass().getSimpleName());
    return names;
  }

  @Test
  void aPlainTraversalIsIdempotentAndReadOnly() {
    final QueryEngine.AnalyzedQuery analyzed = analyze("g.V().hasLabel('Person').values('name')");
    assertThat(analyzed.isIdempotent()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.READ);
  }

  @Test
  void aCountIsIdempotent() {
    assertThat(analyze("g.V().count()").isIdempotent()).isTrue();
  }

  @Test
  // NOTE: .contains() cannot distinguish this from the widened [CREATE,UPDATE,DELETE] fallback;
  // see addVertexOperationTypeIsExactlyCreate for the narrow assertion.
  void addVertexIsNotIdempotentAndIsACreate() {
    final QueryEngine.AnalyzedQuery analyzed = analyze("g.addV('Person')");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE);
    assertThat(analyzed.getOperationTypes()).doesNotContain(OperationType.READ);
  }

  @Test
  // NOTE: .contains() cannot distinguish this from the widened [CREATE,UPDATE,DELETE] fallback;
  // see addEdgeOperationTypeIsExactlyCreate for the narrow assertion.
  void addEdgeIsACreate() {
    final QueryEngine.AnalyzedQuery analyzed =
        analyze("g.V().hasLabel('Person').as('a').addE('KNOWS').to('a')");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE);
  }

  @Test
  void dropIsADelete() {
    final QueryEngine.AnalyzedQuery analyzed = analyze("g.V().hasLabel('Person').drop()");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.DELETE);
  }

  @Test
  // NOTE: .contains() cannot distinguish this from the widened [CREATE,UPDATE,DELETE] fallback;
  // see addPropertyOperationTypeIsExactlyUpdate for the narrow assertion.
  void addPropertyIsAnUpdate() {
    final QueryEngine.AnalyzedQuery analyzed =
        analyze("g.V().hasLabel('Person').property('age', 30)");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.UPDATE);
  }

  @Test
  void analysisIsNeverDDL() {
    assertThat(analyze("g.addV('Person')").isDDL()).isFalse();
  }

  @Test
  void analysisToleratesUnboundParameters() {
    // Issue #5187: the analyze() path (HA follower idempotency check) does not receive parameter
    // bindings, so it must use the null-tolerant java engine and still classify the query.
    assertThat(analyze("g.V().has('name', name).values('name')").isIdempotent()).isTrue();
  }

  @Test
  void mutatingStepsSeenByAnalysisArePlaceholdersNotResolvedSteps() throws Exception {
    // TinkerPop 3.8.1's gremlin-lang parser builds GValue-based placeholder steps for addV/addE/property
    // (e.g. AddVertexStepPlaceholder, which does NOT extend AddVertexStep). They are only resolved into
    // the concrete step types ArcadeGremlin.parse() checks for (AddVertexStep, AddEdgeStep,
    // AddPropertyStep) by TinkerPop's GValueReductionStrategy, which parse() never runs (it reads
    // getSteps() straight after eval(), the same way production parse() does - see
    // analysisStepClassNames()). This locks in that fact: if a TinkerPop upgrade changes it, this test
    // breaks and the operation-type classification below must be re-checked. DropStep has no placeholder
    // variant in gremlin-core 3.8.1, so drop() alone reaches parse() already resolved.
    assertThat(analysisStepClassNames("g.addV('Person')"))
        .containsExactly("AddVertexStartStepPlaceholder");
    assertThat(analysisStepClassNames("g.V().hasLabel('Person').as('a').addE('KNOWS').to('a')"))
        .containsExactly("GraphStep", "HasStep", "AddEdgeStepPlaceholder");
    assertThat(analysisStepClassNames("g.V().hasLabel('Person').property('age', 30)"))
        .containsExactly("GraphStep", "HasStep", "AddPropertyStepPlaceholder");
    assertThat(analysisStepClassNames("g.V().hasLabel('Person').drop()"))
        .containsExactly("GraphStep", "HasStep", "DropStep");
  }

  @Test
  void dropOperationTypeIsExactlyDelete() {
    // DropStep has no placeholder variant, so parse() sees the real step and the instanceof check for
    // DropStep matches: the narrow, single-type classification the javadoc on parse() promises.
    assertThat(analyze("g.V().hasLabel('Person').drop()").getOperationTypes())
        .containsExactly(OperationType.DELETE);
  }

  @Test
  // FIXED (#5838): parse() now checks the AddVertexStepContract/AddEdgeStepContract/AddPropertyStepContract
  // interfaces, which both the GValue placeholder steps and the resolved concrete steps implement, instead of
  // the concrete classes alone. See the comment in ArcadeGremlin.parse() for the full explanation.
  void addVertexOperationTypeIsExactlyCreate() {
    assertThat(analyze("g.addV('Person')").getOperationTypes())
        .containsExactly(OperationType.CREATE);
  }

  @Test
  // FIXED (#5838): see addVertexOperationTypeIsExactlyCreate above.
  void addEdgeOperationTypeIsExactlyCreate() {
    assertThat(analyze("g.V().hasLabel('Person').as('a').addE('KNOWS').to('a')").getOperationTypes())
        .containsExactly(OperationType.CREATE);
  }

  @Test
  // FIXED (#5838): see addVertexOperationTypeIsExactlyCreate above.
  void addPropertyOperationTypeIsExactlyUpdate() {
    assertThat(analyze("g.V().hasLabel('Person').property('age', 30)").getOperationTypes())
        .containsExactly(OperationType.UPDATE);
  }
}
