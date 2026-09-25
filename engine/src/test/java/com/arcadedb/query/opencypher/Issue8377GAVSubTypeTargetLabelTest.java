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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8377: a Cypher label on a pattern node matches the vertices of its sub-types too. Once a Graph Analytical View
 * served the traversal, the fused chain filtered each hop's target on the label's OWN buckets and the deferred
 * expansion compared the exact type name, so every endpoint of a sub-type was silently dropped.
 * <p>
 * The view is a performance feature: the oracle is the answer the same statement gives without it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8377GAVSubTypeTargetLabelTest extends TestHelper {
  private static final String[] QUERIES = {
      // The issue's statements: a sub-typed intermediate and a sub-typed endpoint
      "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:C) RETURN count(*) AS n",
      "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:C) RETURN b.id AS bi, c.id AS ci ORDER BY bi",
      // The intermediate is not read downstream
      "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:C) RETURN c.id AS ci ORDER BY ci",
      "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:C) RETURN a.id AS ai, c.id AS ci ORDER BY ci",
      // A branching pattern is not fused: the unread intermediate's expansion defers its target load
      "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:C), (a)-[:S]->(x:D) RETURN c.id AS ci, x.id AS xi ORDER BY ci, xi",
      // Three hops, the sub-type in the middle
      "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:C)-[:R]->(d:D) RETURN a.id AS ai, d.id AS di ORDER BY di",
      // A label that names the sub-type alone still excludes its parent's own vertices
      "MATCH (a:A)-[:R]->(b:Bsub)-[:R]->(c:C) RETURN c.id AS ci ORDER BY ci",
      "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:Csub) RETURN b.id AS bi ORDER BY bi",
      // A label no type carries matches nothing
      "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:Missing) RETURN count(*) AS n" };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE VERTEX TYPE B");
    database.command("sql", "CREATE VERTEX TYPE Bsub EXTENDS B");
    database.command("sql", "CREATE VERTEX TYPE C");
    database.command("sql", "CREATE VERTEX TYPE Csub EXTENDS C");
    database.command("sql", "CREATE VERTEX TYPE D");
    database.command("sql", "CREATE EDGE TYPE R");
    database.command("sql", "CREATE EDGE TYPE S");

    database.transaction(() -> {
      final MutableVertex a1 = database.newVertex("A").set("id", 1).save();
      final MutableVertex bsub = database.newVertex("Bsub").set("id", 11).save();
      final MutableVertex b1 = database.newVertex("B").set("id", 12).save();
      final MutableVertex c1 = database.newVertex("C").set("id", 21).save();
      final MutableVertex csub = database.newVertex("Csub").set("id", 22).save();
      final MutableVertex d1 = database.newVertex("D").set("id", 31).save();
      a1.newEdge("R", bsub);
      bsub.newEdge("R", c1);
      a1.newEdge("R", b1);
      b1.newEdge("R", csub);
      csub.newEdge("R", d1);
      c1.newEdge("R", d1);
      a1.newEdge("S", d1);

      // Filler, so the optimizer anchors on A and fuses the chain
      for (int i = 0; i < 60; i++)
        database.newVertex("B").set("id", 1000 + i).save();
      for (int i = 0; i < 200; i++)
        database.newVertex("C").set("id", 2000 + i).save();
      for (int i = 0; i < 200; i++)
        database.newVertex("D").set("id", 3000 + i).save();
    });
  }

  @Test
  void subTypeEndpointsMatchTheParentLabelWithTheView() {
    final Map<String, List<String>> expected = new LinkedHashMap<>();
    for (final String query : QUERIES)
      expected.put(query, answer(query));

    // The oracle itself must see the sub-typed rows, or the comparison below proves nothing
    assertThat(expected.get(QUERIES[0])).containsExactly("n=2;");
    assertThat(expected.get(QUERIES[1])).containsExactly("bi=11;ci=21;", "bi=12;ci=22;");
    assertThat(expected.get(QUERIES[QUERIES.length - 1])).containsExactly("n=0;");

    createView("VERTEX TYPES (A, B, Bsub, C, Csub, D) EDGE TYPES (R, S) PROPERTIES (id)");

    // The deferred expansion is on the plan, not fused away
    assertThat(plan(QUERIES[4])).contains("GAVExpandAll(a)-[:R]->(b:B)");

    for (final String query : QUERIES) {
      assertThat(plan(query)).as(query).contains("provider=gav8377");
      assertThat(answer(query)).as(query).isEqualTo(expected.get(query));
    }
  }

  private void createView(final String definition) {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav8377 " + definition);
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav8377");
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

  private List<String> answer(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder line = new StringBuilder();
        for (final String name : row.getPropertyNames())
          line.append(name).append('=').append(row.<Object>getProperty(name)).append(';');
        rows.add(line.toString());
      }
    }
    return rows;
  }
}
