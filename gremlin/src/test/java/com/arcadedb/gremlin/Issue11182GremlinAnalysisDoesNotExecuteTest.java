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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11182: analysing a Gremlin command ran it. {@link ArcadeGremlin#parse()} built the traversal by evaluating the text, and the
 * gremlin-lang engine applies a terminal step ({@code iterate()}, {@code next()}, {@code toList()}...) while it evaluates, so the
 * write was applied by the analysis itself. On an HA follower, whose {@code command()} analyses a statement to decide whether to
 * forward it, one write sent to a follower was applied there, forwarded and applied again on the leader, failed the follower's
 * commit with a conflict, and was retried: one request created six vertices and still answered an error.
 * <p>
 * Analysis now builds every traversal of the script from the parse tree and never applies a terminal step, so nothing it is asked
 * about is executed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11182GremlinAnalysisDoesNotExecuteTest {

  private ArcadeGraph graph;
  private Database    database;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-11182-analysis");
    database = (Database) graph.getDatabase();
    database.getSchema().createVertexType("Probe");
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @ParameterizedTest
  @ValueSource(strings = { "iterate()", "next()", "toList()", "toSet()", "toBulkSet()", "hasNext()", "tryNext()", "next(2)" })
  void aTerminalStepIsNeverAppliedByTheAnalysis(final String terminal) {
    final QueryEngine.AnalyzedQuery analyzed = analyzeInATransaction("g.addV('Probe').property('via','analysis')." + terminal);

    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.CREATE);
    assertThat(database.countType("Probe", true)).as("the analysis of '%s' must not write", terminal).isZero();
  }

  @Test
  void aDropIsNeverAppliedByTheAnalysis() {
    database.transaction(() -> database.newVertex("Probe").set("name", "kept").save());

    final QueryEngine.AnalyzedQuery analyzed = analyzeInATransaction("g.V().hasLabel('Probe').drop().iterate()");

    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.DELETE);
    assertThat(database.countType("Probe", true)).isEqualTo(1L);
  }

  @Test
  void aReadEndingInATerminalStepIsARead() {
    // A TERMINAL STEP USED TO RETURN A VALUE, NOT A TRAVERSAL, AND parse() REFUSED IT AS A PARSING ERROR: A READ ENDING IN next()
    // SENT TO AN HA FOLLOWER FAILED BEFORE IT COULD RUN
    final QueryEngine.AnalyzedQuery analyzed = graph.gremlin("g.V().hasLabel('Probe').count().next()").parse();
    assertThat(analyzed.isIdempotent()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.READ);
  }

  @Test
  void everyStatementOfAScriptIsAnalysedNotOnlyTheLast() {
    // gremlin-lang ANSWERS ONLY THE LAST STATEMENT OF A SCRIPT: A WRITE BEFORE A READ WAS CLASSIFIED BY THE READ
    final QueryEngine.AnalyzedQuery analyzed = analyzeInATransaction("g.addV('Probe').iterate(); g.V().hasLabel('Probe').count()");

    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.CREATE);
    assertThat(database.countType("Probe", true)).isZero();
  }

  @Test
  void aScriptOfReadsIsARead() {
    final QueryEngine.AnalyzedQuery analyzed = graph.gremlin("g.V().count().next(); g.V().hasLabel('Probe').toList()").parse();
    assertThat(analyzed.isIdempotent()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.READ);
  }

  @Test
  void aWriteInAnArgumentIsNeverAppliedByTheAnalysis() {
    // A TERMINATED TRAVERSAL IN AN ARGUMENT IS EVALUATED WHILE THE OUTER ONE IS BUILT, SO THE ANALYSIS CANNOT BUILD THE OUTER ONE
    // WITHOUT RUNNING IT: A WRITE THERE IS REFUSED BEFORE IT RUNS, AND THE SCRIPT IS A WRITE
    final QueryEngine.AnalyzedQuery analyzed = analyzeInATransaction("g.V(g.addV('Probe').id().next()).count()");

    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE).doesNotContain(OperationType.READ);
    assertThat(database.countType("Probe", true)).isZero();
  }

  @Test
  void aReadInAnArgumentIsStillARead() {
    database.transaction(() -> database.newVertex("Probe").set("name", "arg").save());

    final QueryEngine.AnalyzedQuery analyzed = graph.gremlin("g.V(g.V().hasLabel('Probe').id().next()).values('name')").parse();
    assertThat(analyzed.isIdempotent()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.READ);

    // AND IT STILL RUNS
    try (final ResultSet rs = graph.gremlin("g.V(g.V().hasLabel('Probe').id().next()).values('name')").execute()) {
      assertThat(rs.next().<String>getProperty("result")).isEqualTo("arg");
    }
  }

  @Test
  void aTransactionControlIsNeverAppliedByTheAnalysis() {
    database.begin();
    try {
      final MutableVertex pending = database.newVertex("Probe").set("name", "pending").save();

      final QueryEngine.AnalyzedQuery commit = graph.gremlin("g.tx().commit()").parse();
      final QueryEngine.AnalyzedQuery rollback = graph.gremlin("g.tx().rollback()").parse();
      assertThat(commit.isIdempotent()).isTrue();
      assertThat(rollback.isIdempotent()).isTrue();

      assertThat(database.isTransactionActive()).as("the analysis must not end the caller's transaction").isTrue();
      assertThat(database.countType("Probe", true)).isEqualTo(1L);
      assertThat(pending.getIdentity()).isNotNull();
    } finally {
      database.rollback();
    }
    assertThat(database.countType("Probe", true)).isZero();
  }

  @Test
  void theReadOnlyQueryPathDoesNotCreateTheTypeOfANewLabel() {
    // THE MINOR HALF OF THE ISSUE: A WRITE SENT TO query() WAS REFUSED AS NOT IDEMPOTENT, AFTER ITS ANALYSIS HAD RUN IT. THE VERTEX
    // WAS ROLLED BACK, ITS TYPE WAS NOT (A SCHEMA CHANGE IS NOT TRANSACTIONAL)
    assertThatThrownBy(() -> database.transaction(() -> database.query("gremlin", "g.addV('Issue11182NewLabel').iterate()").close()))
        .isInstanceOf(QueryNotIdempotentException.class);

    assertThat(database.getSchema().existsType("Issue11182NewLabel")).isFalse();
  }

  @Test
  void theParametersOfTheQueryPathAreBound() {
    final ArcadeGremlin gremlin = graph.gremlin("g.addV(vertexLabel).property('name', name).next()");
    gremlin.setParameters(Map.of("vertexLabel", "Probe", "name", "p"));
    final QueryEngine.AnalyzedQuery analyzed = gremlin.parse();
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.CREATE);
    assertThat(database.countType("Probe", true)).isZero();
  }

  @Test
  void aSyntaxErrorIsStillAParsingError() {
    assertThatThrownBy(() -> graph.gremlin("g.V().filter{ it.get() }").parse()).isInstanceOf(CommandParsingException.class);
  }

  @Test
  void aGroovyScriptThatWritesIsNeverRunByTheAnalysis() {
    // WITH THE 'auto' ENGINE A SCRIPT THE gremlin-lang GRAMMAR CANNOT READ FALLS BACK TO GROOVY, WHICH IS CODE: ONLY EVALUATING IT
    // TELLS ITS TRAVERSAL. THE ANALYSIS EVALUATES IT WITH A SOURCE THAT STOPS A TRAVERSAL THAT WRITES BEFORE ITS FIRST STEP
    database.getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, "auto");

    final QueryEngine.AnalyzedQuery analyzed = analyzeInATransaction("def v = g.addV('Probe').next(); v");

    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE);
    assertThat(database.countType("Probe", true)).isZero();
  }

  @Test
  void aGroovyScriptThatReadsIsARead() {
    database.getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, "auto");
    database.transaction(() -> database.newVertex("Probe").set("name", "groovy").save());

    final QueryEngine.AnalyzedQuery ranARead = graph.gremlin("def c = g.V().hasLabel('Probe').count().next(); c").parse();
    assertThat(ranARead.isIdempotent()).isTrue();
    assertThat(ranARead.getOperationTypes()).containsExactly(OperationType.READ);

    final QueryEngine.AnalyzedQuery builtARead = graph.gremlin("def t = g.V().hasLabel('Probe'); t.values('name')").parse();
    assertThat(builtARead.isIdempotent()).isTrue();

    final QueryEngine.AnalyzedQuery builtAWrite = analyzeInATransaction("def t = g.V().hasLabel('Probe'); t.drop()");
    assertThat(builtAWrite.isIdempotent()).isFalse();
    assertThat(builtAWrite.getOperationTypes()).containsExactly(OperationType.DELETE);
    assertThat(database.countType("Probe", true)).isEqualTo(1L);
  }

  @Test
  void theCommandStillRunsItsTerminalStepOnce() {
    database.transaction(() -> database.command("gremlin", "g.addV('Probe').property('via','command').iterate()").close());
    assertThat(database.countType("Probe", true)).isEqualTo(1L);
  }

  /**
   * Analyses in a transaction that is rolled back afterwards, the shape of the HTTP handler's: a write the analysis applied would be
   * visible inside it, which is what the assertions read.
   */
  private QueryEngine.AnalyzedQuery analyzeInATransaction(final String query) {
    final long before = database.countType("Probe", true);
    database.begin();
    try {
      final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("gremlin").analyze(query);
      assertThat(database.countType("Probe", true)).as("written inside the analysing transaction by '%s'", query).isEqualTo(before);
      return analyzed;
    } finally {
      database.rollback();
    }
  }
}
