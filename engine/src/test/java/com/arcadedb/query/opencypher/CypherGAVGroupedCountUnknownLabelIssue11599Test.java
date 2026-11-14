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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11599: with a Graph Analytical View over the anchor, a grouped one-hop count whose counted node
 * carries a label that names no type, {@code MATCH (a:A)-[:R]->(b:Nope) RETURN a.id, count(b)}, failed with
 * "Type with name 'Nope' was not found". A label no type carries matches no node, so the statement returns no rows, as
 * it does without the view.
 */
class CypherGAVGroupedCountUnknownLabelIssue11599Test extends TestHelper {

  private static final String[] QUERIES = {
      "MATCH (a:A)-[:R]->(b:Nope) RETURN a.id AS k, count(b) AS c",
      "MATCH (a:A)-[:R]->(b:Nope) RETURN a.id AS k, count(*) AS c",
      "MATCH (b:Nope)<-[:R]-(a:A) RETURN a.id AS k, a.id + 1 AS k2, count(b) AS c" };

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1})-[:R]->(:A {id: 2})"));
  }

  @Test
  void unknownCountedLabelReturnsNoRowsWithTheView() {
    for (final String query : QUERIES)
      assertThat(rows(query)).as(query).isEmpty();

    createView();

    for (final String query : QUERIES) {
      assertThat(plan(query)).as(query).contains("COUNT EDGES RETURN");
      assertThat(rows(query)).as(query).isEmpty();
    }
  }

  @Test
  void knownCountedLabelStillCountedWithTheView() {
    createView();
    assertThat(rows("MATCH (a:A)-[:R]->(b:A) RETURN a.id AS k, count(b) AS c")).containsExactly(Map.of("k", 1L, "c", 1L));
  }

  private void createView() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav11599");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav11599");
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.onSpinWait();
    assertThat(view.isReady()).isTrue();
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private List<Map<String, Object>> rows(final String query) {
    final List<Map<String, Object>> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(Map.of("k", r.<Number>getProperty("k").longValue(), "c", r.<Number>getProperty("c").longValue()));
      }
    }
    return out;
  }
}
