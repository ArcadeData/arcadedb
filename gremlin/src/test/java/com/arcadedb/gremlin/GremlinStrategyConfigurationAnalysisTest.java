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
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The analysis of a Gremlin script reads what a traversal writes from its steps. A strategy added to the traversal source of the
 * script ({@code g.withStrategies(...)}) can be configured with a traversal, which joins the traversal as steps only when the
 * strategies are applied, at execution: the analysis, which builds the traversal and never runs it, has to read the traversals held
 * by the configuration of its strategies too, or what they write is classified as a read.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GremlinStrategyConfigurationAnalysisTest {
  private static final String SOURCE = "g.withStrategies(SubgraphStrategy(";

  private ArcadeGraph graph;
  private Database    database;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-strategy-configuration-analysis");
    database = (Database) graph.getDatabase();
    database.getSchema().createVertexType("Probe");
    database.getSchema().createEdgeType("Link");
    database.transaction(() -> {
      database.newVertex("Probe").set("name", "a").save();
      database.newVertex("Probe").set("name", "b").save();
    });
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  static Stream<Arguments> writingCriteria() {
    return Stream.of(
        Arguments.of(SOURCE + "vertices: __.addV('Probe'))).V().toList()", Set.of(OperationType.CREATE)),
        Arguments.of(SOURCE + "edges: __.addE('Link'))).E().toList()", Set.of(OperationType.CREATE)),
        Arguments.of(SOURCE + "vertexProperties: __.addV('Probe'))).V().properties().toList()", Set.of(OperationType.CREATE)),
        Arguments.of(SOURCE + "vertices: __.property('touched', true))).V().toList()", Set.of(OperationType.UPDATE)),
        Arguments.of(SOURCE + "vertices: __.drop())).V().toList()", Set.of(OperationType.DELETE)),
        Arguments.of(SOURCE + "edges: __.drop())).E().toList()", Set.of(OperationType.DELETE)),
        Arguments.of(SOURCE + "vertices: __.mergeV(['name': 'c']))).V().toList()", Set.of(OperationType.CREATE, OperationType.UPDATE)),
        Arguments.of(SOURCE + "vertices: __.coalesce(__.hasLabel('Probe'), __.addV('Probe')))).V().toList()", Set.of(OperationType.CREATE)),
        Arguments.of(SOURCE + "vertices: __.not(__.sideEffect(__.drop())))).V().toList()", Set.of(OperationType.DELETE)),
        Arguments.of(SOURCE + "vertices: __.hasLabel('Probe'), edges: __.drop())).V().toList()", Set.of(OperationType.DELETE)),
        Arguments.of("g.V().count().next(); " + SOURCE + "vertices: __.addV('Probe'))).V().toList()", Set.of(OperationType.CREATE)));
  }

  @ParameterizedTest
  @MethodSource("writingCriteria")
  void aWriteHeldByTheConfigurationOfAStrategyIsAnalysedAsAWrite(final String script, final Set<OperationType> expected) {
    // IN A TRANSACTION OF ITS OWN, WHERE WHAT THE ANALYSIS WROTE WOULD BE VISIBLE
    final QueryEngine.AnalyzedQuery analyzed;
    database.begin();
    try {
      analyzed = database.getQueryEngine("gremlin").analyze(script);

      assertThat(database.countType("Probe", true)).as(script).isEqualTo(2L);
      assertThat(database.countType("Link", true)).as(script).isZero();
      assertThat(database.query("sql", "select from Probe where touched = true").hasNext()).as(script).isFalse();
    } finally {
      database.rollback();
    }

    assertThat(analyzed.isIdempotent()).as(script).isFalse();
    assertThat(analyzed.getOperationTypes()).as(script).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(analyzed.isDDL()).isFalse();
  }

  @ParameterizedTest
  @MethodSource("writingCriteria")
  void theReadOnlyQueryPathRefusesAWriteHeldByTheConfigurationOfAStrategy(final String script, final Set<OperationType> expected) {
    assertThatThrownBy(() -> database.transaction(() -> database.query("gremlin", script).close())).as(script)
        .isInstanceOf(QueryNotIdempotentException.class);

    assertThat(database.countType("Probe", true)).as(script).isEqualTo(2L);
    assertThat(database.countType("Link", true)).as(script).isZero();
    assertThat(database.query("sql", "select from Probe where touched = true").hasNext()).as(script).isFalse();
  }

  @Test
  void aWriteHeldByTheConfigurationOfAStrategyIsCommittedByACommand() {
    // THE COMMAND OPENS THE TRANSACTION OF A STATEMENT THAT MAY WRITE: ONE CLASSIFIED AS A READ RAN WITHOUT, AND WHAT IT WROTE WAS TAKEN BACK
    database.command("gremlin", SOURCE + "vertices: __.property('touched', true))).V().toList()").close();

    assertThat(database.query("sql", "select count(*) as total from Probe where touched = true").next().<Long>getProperty("total"))
        .isEqualTo(2L);
  }

  @Test
  void aWriteHeldByTheConfigurationOfAStrategyOfAnArgumentIsAnalysedAsAWrite() {
    // THE ARGUMENT IS A TERMINATED TRAVERSAL: IT RUNS WHEN THE OUTER ONE IS BUILT, AND ITS WRITE IS REFUSED BEFORE IT RUNS
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("gremlin")
        .analyze("g.V(" + SOURCE + "vertices: __.drop())).V().id().next()).count()");

    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.DELETE).doesNotContain(OperationType.READ);
    assertThat(database.countType("Probe", true)).isEqualTo(2L);
  }

  @ParameterizedTest
  @MethodSource("readingCriteria")
  void aReadHeldByTheConfigurationOfAStrategyIsAnalysedAsARead(final String script) {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("gremlin").analyze(script);

    assertThat(analyzed.isIdempotent()).as(script).isTrue();
    assertThat(analyzed.getOperationTypes()).as(script).containsExactly(OperationType.READ);
  }

  static Stream<String> readingCriteria() {
    return Stream.of(
        SOURCE + "vertices: __.hasLabel('Probe'))).V().toList()",
        SOURCE + "vertices: __.has('name', 'a'), edges: __.hasLabel('Link'))).V().toList()",
        SOURCE + "vertexProperties: __.hasNot('hidden'))).V().properties().toList()",
        SOURCE + "vertices: __.or(__.has('name', 'a'), __.out('Link').count().is(0)))).V().toList()",
        "g.withStrategies(ReadOnlyStrategy).V().toList()",
        "g.withStrategies(SeedStrategy(seed: 7)).V().sample(1).toList()");
  }

  @Test
  void aWriteNestedInTheConfigurationOfAnyStrategyIsAnalysedAsAWrite() {
    final Map<String, Object> nested = new HashMap<>();
    nested.put("list", List.of(Map.of("entry", new Object[] { __.drop() })));
    nested.put("parameter", GValue.of("criterion", __.addV("Probe")));
    nested.put("literal", "text");
    nested.put("number", 3);
    nested.put("missing", null);

    final QueryEngine.AnalyzedQuery analyzed = GremlinStatementAnalyzer.analyze(
        List.of(graph.traversal().withStrategies(new ConfiguredStrategy(nested)).V().asAdmin()));

    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).containsExactlyInAnyOrder(OperationType.CREATE, OperationType.DELETE);
  }

  @Test
  void aConfigurationNestedTooDeepIsAnalysedAsAWrite() {
    Object value = "leaf";
    for (int i = 0; i < 100; i++)
      value = List.of(value);

    final QueryEngine.AnalyzedQuery analyzed = GremlinStatementAnalyzer.analyze(
        List.of(graph.traversal().withStrategies(new ConfiguredStrategy(Map.of("deep", value))).V().asAdmin()));

    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE);
  }

  @Test
  void aStrategyWithNoTraversalInItsConfigurationIsNotAWrite() {
    final QueryEngine.AnalyzedQuery analyzed = GremlinStatementAnalyzer.analyze(
        List.of(graph.traversal().withStrategies(new ConfiguredStrategy(Map.of("list", List.of("a", 1, true)))).V().asAdmin()));

    assertThat(analyzed.isIdempotent()).isTrue();
  }

  @Test
  void aStrategyWithAReadHeldByItsConfigurationStillRuns() {
    try (final var resultSet = database.query("gremlin", SOURCE + "vertices: __.has('name', 'a'))).V().values('name')")) {
      assertThat(resultSet.next().<String>getProperty("result")).isEqualTo("a");
      assertThat(resultSet.hasNext()).isFalse();
    }
  }

  /**
   * A strategy that does nothing and answers the values it was given as its configuration.
   */
  private static final class ConfiguredStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
      implements TraversalStrategy.DecorationStrategy {
    private final Map<String, Object> values;

    private ConfiguredStrategy(final Map<String, Object> values) {
      this.values = values;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
    }

    @Override
    public Configuration getConfiguration() {
      return new MapConfiguration(values);
    }
  }
}
