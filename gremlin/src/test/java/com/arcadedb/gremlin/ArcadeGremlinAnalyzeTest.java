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
 * Covers ArcadeGremlin.parse(). Its isIdempotent() verdict drives HA follower routing
 * (RaftReplicatedDatabase consults only isIdempotent()/isDDL(), never getOperationTypes()); the
 * placeholder step types gremlin-lang hands to parse() DO implement Mutating (via Writing/Deleting,
 * javap-verified), so isIdempotent() is correct and HA routing is unaffected. getOperationTypes() is a
 * separate story: it over-widens for addV/addE/property (see
 * mutatingStepsSeenByAnalysisArePlaceholdersNotResolvedSteps and the @Disabled tests below), and the
 * confirmed impact of that is MCP permission over-denial, not HA misrouting.
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
   * AddVertexStartStepPlaceholder, which does NOT extend AddVertexStep) that would be missed by the
   * instanceof checks in ArcadeGremlin.parse(). Confirmed impact: the placeholders DO implement
   * Mutating (via Writing/Deleting, javap-verified), so isIdempotent() still comes back correctly false
   * and HA routing is unaffected; what actually breaks is getOperationTypes(), which falls into the
   * "unknown mutating step" branch and widens to all three write types instead of the one that
   * actually applies - see mutatingStepsSeenByAnalysisArePlaceholdersNotResolvedSteps below and the
   * MCP permission over-denial impact documented on the @Disabled tests.
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
  // see addVertexOperationTypeIsExactlyCreate (disabled).
  void addVertexIsNotIdempotentAndIsACreate() {
    final QueryEngine.AnalyzedQuery analyzed = analyze("g.addV('Person')");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE);
    assertThat(analyzed.getOperationTypes()).doesNotContain(OperationType.READ);
  }

  @Test
  // NOTE: .contains() cannot distinguish this from the widened [CREATE,UPDATE,DELETE] fallback;
  // see addEdgeOperationTypeIsExactlyCreate (disabled).
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
  // see addPropertyOperationTypeIsExactlyUpdate (disabled).
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
      BUG (test-coverage finding): OperationType over-widening defect. getOperationTypes() only - \
      isIdempotent() is correct and HA routing (RaftReplicatedDatabase, which consults only isIdempotent()/ \
      isDDL(), never getOperationTypes()) is unaffected. The real, confirmed impact is MCP permission \
      over-denial: ExecuteCommandTool.execute() calls GremlinQueryEngine.analyze(), which delegates \
      straight to this parse() method, then checkPermission(Set<OperationType>, MCPPermissions) rejects \
      the whole command if ANY OperationType present lacks its matching permission bit (CREATE needs \
      isAllowInsert(), UPDATE needs isAllowUpdate(), DELETE needs isAllowDelete() - independent booleans \
      on MCPPermissions). Concrete effect: an MCP execute_command call with language "gremlin" and \
      command "g.addV('Person')", against a profile with allowInsert=true but allowUpdate=false (a \
      realistic, supported profile), is wrongly rejected with "Update operations are not allowed by MCP \
      configuration" even though the query is a pure insert. Tracked as issue #5838; full writeup: PR #5829. \
      Root cause: parse() sees AddVertexStartStepPlaceholder for 'g.addV(\\'Person\\')', not the \
      AddVertexStartStep its instanceof check tests for (TinkerPop 3.8.1's gremlin-lang parser builds a \
      GValue placeholder that GValueReductionStrategy - which parse() never runs - would normally resolve). \
      The instanceof chain in ArcadeGremlin.parse() misses it and falls into the 'unknown mutating step' \
      branch, which adds ALL THREE write OperationTypes. Query: g.addV('Person'). Expected \
      getOperationTypes(): [CREATE]. Actual: [CREATE, UPDATE, DELETE].""")
  void addVertexOperationTypeIsExactlyCreate() {
    assertThat(analyze("g.addV('Person')").getOperationTypes())
        .containsExactly(OperationType.CREATE);
  }

  @Test
  @Disabled("""
      BUG (test-coverage finding): OperationType over-widening defect. getOperationTypes() only - \
      isIdempotent() is correct and HA routing is unaffected (see the sibling addVertexOperationTypeIsExactlyCreate \
      for the full HA-routing analysis and the confirmed, already-shipping MCP permission over-denial \
      this causes; tracked as issue #5838, full writeup: PR #5829). Root cause: parse() sees \
      AddEdgeStepPlaceholder for \"g.V().hasLabel('Person').as('a').addE('KNOWS').to('a')\", not the \
      AddEdgeStep its instanceof check tests for, same GValue-placeholder reason as addV (see \
      mutatingStepsSeenByAnalysisArePlaceholdersNotResolvedSteps). Falls into the 'unknown mutating step' \
      branch, which adds ALL THREE write OperationTypes. Expected getOperationTypes(): [CREATE]. Actual: \
      [CREATE, UPDATE, DELETE].""")
  void addEdgeOperationTypeIsExactlyCreate() {
    assertThat(analyze("g.V().hasLabel('Person').as('a').addE('KNOWS').to('a')").getOperationTypes())
        .containsExactly(OperationType.CREATE);
  }

  @Test
  @Disabled("""
      BUG (test-coverage finding): OperationType over-widening defect. getOperationTypes() only - \
      isIdempotent() is correct and HA routing is unaffected (see the sibling addVertexOperationTypeIsExactlyCreate \
      for the full HA-routing analysis and the confirmed, already-shipping MCP permission over-denial \
      this causes; tracked as issue #5838, full writeup: PR #5829). Root cause: parse() sees \
      AddPropertyStepPlaceholder for \"g.V().hasLabel('Person').property('age', 30)\", not the \
      AddPropertyStep its instanceof check tests for, same GValue-placeholder reason as addV/addE. Falls \
      into the 'unknown mutating step' branch, which adds ALL THREE write OperationTypes. Expected \
      getOperationTypes(): [UPDATE]. Actual: [CREATE, UPDATE, DELETE].""")
  void addPropertyOperationTypeIsExactlyUpdate() {
    assertThat(analyze("g.V().hasLabel('Person').property('age', 30)").getOperationTypes())
        .containsExactly(OperationType.UPDATE);
  }
}
