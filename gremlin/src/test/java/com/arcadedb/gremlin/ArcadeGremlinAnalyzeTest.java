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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers ArcadeGremlin.parse(), whose idempotency verdict and OperationType set drive HA follower
 * routing. A read misclassified as a write, or a write as a read, routes the query to the wrong node.
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
   * answers whether TinkerPop 3.8.1's gremlin-lang parser hands parse() placeholder step types (e.g.
   * AddVertexStepPlaceholder, which does NOT extend AddVertexStep) that would be missed by the
   * instanceof checks in ArcadeGremlin.parse(), silently misrouting a write as idempotent/READ.
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
  void addVertexIsNotIdempotentAndIsACreate() {
    final QueryEngine.AnalyzedQuery analyzed = analyze("g.addV('Person')");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE);
    assertThat(analyzed.getOperationTypes()).doesNotContain(OperationType.READ);
  }

  @Test
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
  @Disabled("""
      BUG (test-coverage finding, HA routing defect, high severity): parse() sees AddVertexStartStepPlaceholder \
      for 'g.addV(\\'Person\\')', not the AddVertexStartStep its instanceof check tests for (TinkerPop 3.8.1's \
      gremlin-lang parser builds a GValue placeholder that GValueReductionStrategy - which parse() never runs - \
      would normally resolve). The instanceof chain in ArcadeGremlin.parse() misses it and falls into the \
      'unknown mutating step' branch, which adds ALL THREE write OperationTypes. Query: g.addV('Person'). \
      Expected getOperationTypes(): [CREATE]. Actual: [CREATE, UPDATE, DELETE]. isIdempotent() is still \
      correctly false, so this does not misroute the write to an HA follower, but any caller that keys \
      permission or routing decisions off the specific OperationType (e.g. denying DELETE-tagged operations \
      to a role that may only CREATE) will wrongly treat a plain addV() as also needing UPDATE and DELETE \
      rights.""")
  void addVertexOperationTypeIsExactlyCreate() {
    assertThat(analyze("g.addV('Person')").getOperationTypes())
        .containsExactly(OperationType.CREATE);
  }

  @Test
  @Disabled("""
      BUG (test-coverage finding, HA routing defect, high severity): parse() sees AddEdgeStepPlaceholder for \
      \"g.V().hasLabel('Person').as('a').addE('KNOWS').to('a')\", not the AddEdgeStep its instanceof check \
      tests for, for the same GValue-placeholder reason as addV (see \
      mutatingStepsSeenByAnalysisArePlaceholdersNotResolvedSteps). Falls into the 'unknown mutating step' \
      branch, which adds ALL THREE write OperationTypes. Expected getOperationTypes(): [CREATE]. Actual: \
      [CREATE, UPDATE, DELETE]. isIdempotent() is still correctly false.""")
  void addEdgeOperationTypeIsExactlyCreate() {
    assertThat(analyze("g.V().hasLabel('Person').as('a').addE('KNOWS').to('a')").getOperationTypes())
        .containsExactly(OperationType.CREATE);
  }

  @Test
  @Disabled("""
      BUG (test-coverage finding, HA routing defect, high severity): parse() sees AddPropertyStepPlaceholder \
      for \"g.V().hasLabel('Person').property('age', 30)\", not the AddPropertyStep its instanceof check \
      tests for, same GValue-placeholder reason as addV/addE. Falls into the 'unknown mutating step' branch, \
      which adds ALL THREE write OperationTypes. Expected getOperationTypes(): [UPDATE]. Actual: [CREATE, \
      UPDATE, DELETE]. isIdempotent() is still correctly false.""")
  void addPropertyOperationTypeIsExactlyUpdate() {
    assertThat(analyze("g.V().hasLabel('Person').property('age', 30)").getOperationTypes())
        .containsExactly(OperationType.UPDATE);
  }
}
