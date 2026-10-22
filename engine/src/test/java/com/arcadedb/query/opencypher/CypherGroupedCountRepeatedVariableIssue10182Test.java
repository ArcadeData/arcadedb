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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #10182: a single hop whose two ends are the SAME node variable, grouped by a key of that node
 * and counted with count(*), was answered by the per-node count shortcut, which counts every edge of the node. The
 * pattern asks for the edges that close on the node, its self-loops. Each answer is checked against the same question
 * asked through {@code WITH}, which the shortcut does not claim.
 */
class CypherGroupedCountRepeatedVariableIssue10182Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("R");
    database.transaction(() -> {
      final MutableVertex v0 = database.newVertex("N").set("id", 0L).save();
      final MutableVertex v1 = database.newVertex("N").set("id", 1L).save();
      final MutableVertex v2 = database.newVertex("N").set("id", 2L).save();
      v0.newEdge("R", v0); // self-loop
      v0.newEdge("R", v1);
      v1.newEdge("R", v2);
      v2.newEdge("R", v2); // self-loop
    });
  }

  private Map<Long, Long> grouped(final String query) {
    final Map<Long, Long> out = new TreeMap<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.put(((Number) r.getProperty("id")).longValue(), ((Number) r.getProperty("n")).longValue());
      }
    }
    return out;
  }

  @Test
  void outgoingSelfLoopsPerNode() {
    assertThat(grouped("MATCH (a:N)-[:R]->(a) RETURN a.id AS id, count(*) AS n")).isEqualTo(Map.of(0L, 1L, 2L, 1L));
  }

  @Test
  void incomingSelfLoopsPerNode() {
    assertThat(grouped("MATCH (a:N)<-[:R]-(a) RETURN a.id AS id, count(*) AS n")).isEqualTo(Map.of(0L, 1L, 2L, 1L));
  }

  @Test
  void countOfTheRepeatedVariableAndUntypedRelationship() {
    assertThat(grouped("MATCH (a:N)-[:R]->(a) RETURN a.id AS id, count(a) AS n")).isEqualTo(Map.of(0L, 1L, 2L, 1L));
    assertThat(grouped("MATCH (a:N)-[]->(a) RETURN a.id AS id, count(*) AS n")).isEqualTo(Map.of(0L, 1L, 2L, 1L));
  }

  @Test
  void everyFormAgreesWithTheRecordPath() {
    final String[] patterns = { "(a:N)-[:R]->(a)", "(a:N)<-[:R]-(a)", "(a:N)-[:R]-(a)", "(a)-[:R]->(a)", "(a:N)-[]->(a)" };
    for (final String pattern : patterns)
      assertThat(grouped("MATCH " + pattern + " RETURN a.id AS id, count(*) AS n")).as(pattern)
          .isEqualTo(grouped("MATCH " + pattern + " WITH a RETURN a.id AS id, count(*) AS n"));
  }

  @Test
  void twoVariablesStillUseTheShortcutAndCountEveryEdge() {
    assertThat(grouped("MATCH (a:N)-[:R]->(b) RETURN a.id AS id, count(*) AS n")).isEqualTo(Map.of(0L, 2L, 1L, 1L, 2L, 1L));
    final String plan = database.query("opencypher", "EXPLAIN MATCH (a:N)-[:R]->(b) RETURN a.id AS id, count(*) AS n")
        .getExecutionPlan().get().prettyPrint(0, 2);
    assertThat(plan).contains("COUNT EDGES RETURN");
  }
}
