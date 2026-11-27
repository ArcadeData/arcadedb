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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The analysis of a Gremlin script reads every part of it that is built when it is evaluated, not only its traversals. A statement
 * that is a traversal source on its own ({@code g.withSideEffect(...)}, {@code g.withSack(...)}, {@code g.with(...)}), or a source
 * followed by a transaction control, has no traversal of its own, but the arguments of its configuration methods are built when the
 * statement is evaluated, and one of them can be a terminated traversal that runs at that moment. The analysis answered "read" for
 * such a statement whatever that argument did; it now answers by what the argument does, as it does for the arguments of a traversal.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinSourceStatementAnalysisTest {
  private static final String WRITING_ARGUMENT = "g.addV('Probe').next()";

  private ArcadeGraph graph;
  private Database    database;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-source-statement-analysis");
    database = (Database) graph.getDatabase();
    database.getSchema().createVertexType("Probe");
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @ParameterizedTest
  @ValueSource(strings = {//
      "g.withSideEffect('k', g.addV('Probe').next())",//
      "g.withSideEffect('k', g.addV('Probe').toList())",//
      "g.withSack(g.addV('Probe').next())",//
      "g.with('k', g.addV('Probe').next())",//
      "g.withSideEffect('a', 1).withSack(g.addV('Probe').next())",//
      "g.withSideEffect('k', g.addV('Probe').next()).toString()",//
      "g.withSideEffect('k', g.addV('Probe').property('name', 'p').hasNext())",//
      "g.withSideEffect('k', g.V().hasLabel('Probe').drop().iterate())" })
  void aStatementThatIsASourceWithAWritingArgumentIsAWrite(final String statement) {
    final QueryEngine.AnalyzedQuery analyzed = analyzeInATransaction(statement);

    assertThat(analyzed.isIdempotent()).as("'%s' writes", statement).isFalse();
    assertThat(analyzed.getOperationTypes()).doesNotContain(OperationType.READ).contains(OperationType.CREATE);
    assertThat(database.countType("Probe", true)).as("the analysis of '%s' must not write", statement).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = {//
      "g.withSideEffect('k', g.addV('Probe').next()).tx().commit()",//
      "g.withSack(g.addV('Probe').next()).tx().rollback()" })
  void aSourceWithAWritingArgumentFollowedByATransactionControlIsAWrite(final String statement) {
    final QueryEngine.AnalyzedQuery analyzed = analyzeInATransaction(statement);

    assertThat(analyzed.isIdempotent()).as("'%s' writes", statement).isFalse();
    assertThat(analyzed.getOperationTypes()).doesNotContain(OperationType.READ).contains(OperationType.CREATE);
    assertThat(database.countType("Probe", true)).isZero();
  }

  @Test
  void aWriteInAStatementThatIsASourceIsFoundAfterOtherStatements() {
    final QueryEngine.AnalyzedQuery analyzed = analyzeInATransaction(
        "g.V().hasLabel('Probe').count(); g.withSideEffect('k', " + WRITING_ARGUMENT + "); g.V().count()");

    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE);
    assertThat(database.countType("Probe", true)).isZero();
  }

  @Test
  void aSourceWithAReadingArgumentIsStillARead() {
    for (final String statement : new String[] { "g.withSideEffect('k', 1)", "g.withSideEffect('k', g.V().hasLabel('Probe').count().next())",
        "g.withSack(g.V().hasLabel('Probe').count().next())", "g.with('k', 'v')", "g.withSideEffect('k', __.addV('Probe'))",
        "g.withSideEffect('k', g.V().hasLabel('Probe').count().next()).tx().commit()", "g.tx().commit()" }) {
      final QueryEngine.AnalyzedQuery analyzed = analyzeInATransaction(statement);

      assertThat(analyzed.isIdempotent()).as("'%s' only reads", statement).isTrue();
      assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.READ);
    }
  }

  @Test
  void theReadOnlyQueryPathRefusesASourceWithAWritingArgument() {
    assertThatThrownBy(() -> database.query("gremlin", "g.withSideEffect('k', " + WRITING_ARGUMENT + ")").close())
        .isInstanceOf(QueryNotIdempotentException.class);

    assertThat(database.countType("Probe", true)).isZero();
  }

  @Test
  void theReadOnlyQueryPathRefusesASourceWithAWritingArgumentBeforeATransactionControl() {
    // THE TRANSACTION OF THE QUERY PATH IS ROLLED BACK WHEN ITS RESULT ENDS, UNLESS THE SCRIPT COMMITS IT ITSELF
    assertThatThrownBy(() -> database.query("gremlin", "g.withSideEffect('k', " + WRITING_ARGUMENT + "); g.tx().commit()").close())
        .isInstanceOf(QueryNotIdempotentException.class);

    assertThat(database.countType("Probe", true)).isZero();
  }

  @Test
  void aNestedTraversalPassedAsAValueIsNotRun() {
    // THE ANALYSIS ANSWERS "READ" FOR IT BECAUSE NOTHING RUNS IT: THE COMMAND DOES NOT EITHER
    database.transaction(() -> database.command("gremlin", "g.withSideEffect('k', __.addV('Probe')); g.withSack(__.addV('Probe'))").close());

    assertThat(database.countType("Probe", true)).isZero();
  }

  @Test
  void theCommandStillRunsASourceWithAWritingArgumentOnce() {
    database.transaction(() -> database.command("gremlin", "g.withSideEffect('k', " + WRITING_ARGUMENT + ")").close());

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
