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
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for issue #5187: parameterized Gremlin commands fail during idempotency analysis
 * because {@code QueryEngine.analyze(query)} never receives the parameter bindings. On an HA follower
 * this makes {@code RaftReplicatedDatabase.command()} throw {@code VariableResolverException: No variable found}
 * before the command is ever executed or forwarded.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinParameterizedAnalyzeTest {
  private static final String DB_PATH = "./target/testgremlin5187";

  @Test
  void analyzeParameterizedReadIsIdempotentWithoutBindings() {
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    try {
      graph.getDatabase().getSchema().createVertexType("Tenant");

      // ANALYSIS PATH (as invoked by RaftReplicatedDatabase.command() on a follower): NO PARAMETERS ARE SET.
      final ArcadeGremlin gremlin = graph.gremlin("g.V().hasLabel('Tenant').has('id', p0).as('v').select('v').valueMap()");

      assertThatCode(() -> {
        final QueryEngine.AnalyzedQuery analyzed = gremlin.parse();
        assertThat(analyzed.isIdempotent()).isTrue();
        assertThat(analyzed.getOperationTypes()).contains(OperationType.READ);
      }).doesNotThrowAnyException();
    } finally {
      graph.getDatabase().drop();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }

  @Test
  void analyzeParameterizedWriteIsNotIdempotentWithoutBindings() {
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    try {
      graph.getDatabase().getSchema().createVertexType("Tenant");

      final ArcadeGremlin gremlin = graph.gremlin("g.addV('Tenant').property('id', p0)");

      assertThatCode(() -> {
        final QueryEngine.AnalyzedQuery analyzed = gremlin.parse();
        assertThat(analyzed.isIdempotent()).isFalse();
        assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE);
      }).doesNotThrowAnyException();
    } finally {
      graph.getDatabase().drop();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }

  @Test
  void analyzeViaQueryEngineWithoutParameters() {
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    try {
      graph.getDatabase().getSchema().createVertexType("Tenant");

      final QueryEngine engine = ((Database) graph.getDatabase()).getQueryEngine("gremlin");

      assertThatCode(() -> {
        final QueryEngine.AnalyzedQuery analyzed = engine.analyze(
            "g.V().hasLabel('Tenant').has('id', p0).valueMap()");
        assertThat(analyzed.isIdempotent()).isTrue();
      }).doesNotThrowAnyException();
    } finally {
      graph.getDatabase().drop();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }
}
