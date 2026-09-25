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
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8394: with a Graph Analytical View READY, a multi-hop Cypher pattern whose hops share an edge type bound the
 * same relationship twice in one MATCH, because the CSR holds adjacency ids, not edge identities. The view is a
 * performance feature, so the oracle is the answer without it: every query is answered first with no view, then with
 * one, and the two must match.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8394GAVRelationshipUniquenessTest extends TestHelper {
  private static final int VERTICES = 30;

  private static final String[] QUERIES = {
      // The issue's table
      "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:K]->(b:P), (a)-[:K]->(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P)-[:K]->(d:P) RETURN count(*) AS n",
      "MATCH (a:P {id:1})-[:K]-(b:P)-[:K]-(c:P) RETURN count(*) AS n",
      // Rows, not only counts, and grouped
      "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) RETURN a.id AS a, b.id AS b, c.id AS c ORDER BY a, b, c",
      "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) RETURN a.id AS a, count(*) AS n ORDER BY a",
      "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) RETURN a.id AS a, c.id AS c, count(*) AS n ORDER BY a, c",
      // Aggregated inside the fused chain
      "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WITH a, c, count(*) AS n RETURN a.id AS a, c.id AS c, n ORDER BY a, c",
      "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) WITH a, count(*) AS n RETURN a.id AS a, n ORDER BY a",
      // Walked backwards and mixed directions
      "MATCH (a:P)<-[:K]-(b:P)<-[:K]-(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:K]->(b:P)<-[:K]-(c:P) RETURN count(*) AS n",
      "MATCH (a:P)<-[:K]-(b:P)-[:K]->(c:P) RETURN count(*) AS n",
      // Closing a cycle: the last hop has both ends bound
      "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P)-[:K]->(a) RETURN count(*) AS n",
      "MATCH (a:P)-[:K]-(b:P)-[:K]-(a) RETURN count(*) AS n",
      "MATCH (a:P)-[:K]->(b:P)-[:K]->(a) RETURN count(*) AS n",
      // Several edge types: equal neighbours reached over different types are different relationships
      "MATCH (a:P)-[:K|L]->(b:P)-[:K|L]->(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:K|L]-(b:P)-[:K]-(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:K]->(b:P)-[:L]->(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[]->(b:P)-[]->(c:P) RETURN count(*) AS n",
      // Disconnected components of the same MATCH
      "MATCH (a:P {id:1})-[:K]->(b:P), (c:P {id:1})-[:K]->(d:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:K]->(b:P), (c:P)-[:K]->(d:P) WHERE a.id < 3 AND c.id < 3 RETURN count(*) AS n",
      // A named relationship beside anonymous ones, read and unread
      "MATCH (a:P)-[r:K]->(b:P)-[:K]->(c:P) RETURN count(r) AS n",
      "MATCH (a:P)-[r:K]->(b:P)-[:K]->(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:K]->(b:P)-[r:K]->(c:P) RETURN count(r) AS n",
      // A variable-length relationship in the same clause
      "MATCH (a:P)-[:K]->(b:P)-[:K*1..2]->(c:P) RETURN count(*) AS n",
      // Separate MATCH clauses may reuse a relationship
      "MATCH (a:P)-[:K]->(b:P) MATCH (b)<-[:K]-(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:K]-(b:P) MATCH (b)-[:K]-(c:P) RETURN count(*) AS n" };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE PROPERTY P.id INTEGER");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE L");

    final Random random = new Random(8394);
    database.transaction(() -> {
      final List<Vertex> vertices = new ArrayList<>();
      for (int i = 0; i < VERTICES; i++)
        vertices.add(database.newVertex("P").set("id", i).save());
      for (final Vertex v : vertices) {
        v.asVertex().modify().newEdge("K", v); // self-loop
        for (int k = 0; k < 3; k++)
          v.asVertex().modify().newEdge("K", vertices.get(random.nextInt(VERTICES)));
        // Parallel edges: the same pair joined twice by K, and once more by L
        if (random.nextInt(4) == 0) {
          final Vertex other = vertices.get(random.nextInt(VERTICES));
          v.asVertex().modify().newEdge("K", other);
          v.asVertex().modify().newEdge("K", other);
          v.asVertex().modify().newEdge("L", other);
        }
        if (random.nextInt(5) == 0)
          v.asVertex().modify().newEdge("L", v);
      }
    });
  }

  @Test
  void answersMatchTheQueriesWithoutTheView() {
    final Map<String, List<String>> expected = new LinkedHashMap<>();
    for (final String query : QUERIES)
      expected.put(query, answer(query));

    createView("VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id)");

    final List<String> failures = new ArrayList<>();
    for (final String query : QUERIES) {
      final List<String> actual = answer(query);
      if (!actual.equals(expected.get(query)))
        failures.add(query + "\n  expected " + abbreviate(expected.get(query)) + "\n  actual   " + abbreviate(actual) + "\n" + plan(query));
    }
    assertThat(failures).isEmpty();
  }

  @Test
  void collidingHopsStayOnTheView() {
    createView("VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id)");
    // The issue's shapes are still answered from the view, now binding relationship labels
    assertThat(plan(QUERIES[0])).contains("GAVFusedChain").contains("unique relationships");
    assertThat(plan(QUERIES[1])).contains("GAVFusedChain").contains("unique relationships");
    assertThat(plan(QUERIES[2])).contains("GAVExpandAll").contains("unique relationships");
    assertThat(plan(QUERIES[3])).contains("GAVFusedChain").contains("unique relationships");
    assertThat(plan("MATCH (a:P)-[:K]->(b:P)-[:K]->(a) RETURN count(*) AS n")).contains("GAVExpandInto")
        .doesNotContain("+ ExpandInto");
    assertThat(plan("MATCH (a:P)-[:K]->(b:P), (c:P)-[:K]->(d:P) RETURN count(*) AS n")).contains("GAVExpandAll")
        .contains("unique relationships");
    // Hops that cannot collide do not pay for the labels
    assertThat(plan("MATCH (a:P)-[:K]->(b:P)-[:L]->(c:P) RETURN count(*) AS n")).contains("GAVFusedChain")
        .doesNotContain("unique relationships");
    // A named relationship binds its edge record, so the whole clause walks the edge records
    assertThat(plan("MATCH (a:P)-[r:K]->(b:P)-[:K]->(c:P) RETURN count(r) AS n")).doesNotContain("GAV");
  }

  @Test
  void aChainSplitAcrossWorkerThreadsKeepsRelationshipsUnique() {
    // Enough sources for the fused chain to walk them in parallel chunks
    database.transaction(() -> {
      Vertex previous = null;
      for (int i = 0; i < 9_000; i++) {
        final Vertex v = database.newVertex("P").set("id", 1_000 + i).save();
        v.modify().newEdge("K", v);
        if (previous != null)
          previous.modify().newEdge("K", v);
        previous = v;
      }
    });
    final String query = "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) RETURN count(*) AS n";
    final String grouped = "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) WITH a, count(*) AS n RETURN sum(n) AS n";
    final List<String> expected = answer(query);

    createView("VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id)");
    assertThat(plan(query)).contains("GAVFusedChain").contains("unique relationships");
    assertThat(answer(query)).isEqualTo(expected);
    assertThat(answer(grouped)).isEqualTo(expected);
  }

  @Test
  void anEdgeSubTypeKeepsTheClauseOnTheRecords() {
    // A view builds a type's adjacency polymorphically: the parent's slice also holds the sub-type's edges, so one edge
    // could be labelled under both names. Such a clause walks the edge records instead
    database.command("sql", "CREATE EDGE TYPE KC EXTENDS K");
    database.transaction(() -> {
      final Vertex a = database.query("sql", "SELECT FROM P WHERE id = 1").next().getVertex().get();
      final Vertex b = database.query("sql", "SELECT FROM P WHERE id = 2").next().getVertex().get();
      a.modify().newEdge("KC", b);
      b.modify().newEdge("KC", a);
      a.modify().newEdge("KC", a);
    });
    final String[] queries = {
        "MATCH (a:P)-[:K]->(b:P)-[:KC]->(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:KC]-(b:P)-[:K]-(c:P) RETURN count(*) AS n",
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) RETURN count(*) AS n" };
    final Map<String, List<String>> expected = new LinkedHashMap<>();
    for (final String query : queries)
      expected.put(query, answer(query));

    createView("VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id)");
    for (final String query : queries) {
      assertThat(plan(query)).as(query).doesNotContain("unique relationships");
      assertThat(answer(query)).as(query + "\n" + plan(query)).isEqualTo(expected.get(query));
    }
  }

  @Test
  void answersMatchWithChangesServedFromTheOverlay() {
    createView("VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");

    // Parallel edges, self-loops and new vertices committed after the build are served from the view's delta overlay
    database.transaction(() -> {
      final Vertex x = database.newVertex("P").set("id", 100).save();
      final Vertex y = database.query("sql", "SELECT FROM P WHERE id = 1").next().getVertex().get();
      x.modify().newEdge("K", x);
      x.modify().newEdge("K", x);
      x.modify().newEdge("K", y);
      x.modify().newEdge("K", y);
      y.modify().newEdge("K", x);
      y.modify().newEdge("K", y);
    });

    final Map<String, List<String>> withView = new LinkedHashMap<>();
    for (final String query : QUERIES)
      withView.put(query, answer(query));
    assertThat(plan(QUERIES[0])).contains("provider=gav8394");

    database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav8394");
    for (final String query : QUERIES)
      assertThat(withView.get(query)).as(query).isEqualTo(answer(query));
  }

  @Test
  void aVertexTheViewDoesNotMapIsWalkedOnItsRecordsWithUniqueRelationships() {
    // Kept in use while stale: a vertex created after the build is expanded on its edge records
    createView("VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id) UPDATE MODE OFF");
    GraphAnalyticalViewRegistry.get(database, "gav8394").setUseWhenStale(true);
    database.transaction(() -> {
      final Vertex x = database.newVertex("P").set("id", 100).save();
      x.modify().newEdge("K", x);
      x.modify().newEdge("K", x);
    });

    // Two self-loops: each branch takes one, never both the same
    final String branching = "MATCH (a:P {id:100})-[:K]->(b:P), (a)-[:K]->(c:P) RETURN count(*) AS n";
    assertThat(plan(branching)).contains("GAVExpandAll").contains("unique relationships");
    assertThat(answer(branching)).containsExactly("n=2;");

    final String cycle = "MATCH (a:P {id:100})-[:K]->(b:P)-[:K]->(a) RETURN count(*) AS n";
    assertThat(plan(cycle)).contains("GAVExpandInto");
    assertThat(answer(cycle)).containsExactly("n=2;");
  }

  private static String abbreviate(final List<String> rows) {
    final String s = rows.toString();
    return s.length() > 200 ? rows.size() + " rows: " + s.substring(0, 200) + "..." : s;
  }

  private void createView(final String definition) {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav8394 " + definition);
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav8394");
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
          line.append(name).append('=').append((Object) row.getProperty(name)).append(';');
        rows.add(line.toString());
      }
    }
    return rows;
  }
}
