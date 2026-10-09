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
 * Issue #8426, follow-up to #8394/#8425: with {@code KC EXTENDS K}, a Graph Analytical View attributed every edge of a
 * polymorphic bucket to the last type listed, so the answer to {@code [:K]} depended on the order the view listed its
 * edge types, and the count push-down compared edge-type names only, so {@code [:K]} and {@code [:KC]} counted as
 * disjoint although a {@code KC} edge matches both. The view is a performance feature, so the oracle is the answer the
 * same statement gives without it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8426GAVEdgeSubTypeSlicesTest extends TestHelper {
  private static final int VERTICES = 30;

  private static final String[] QUERIES = {
      // The issue's repro
      "MATCH (a:P)-[:K]->(b:P)-[:KC]->(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:KC]->(b:P)-[:K]->(c:P) RETURN count(*) AS n",
      // A type answer is polymorphic, as on the record path
      "MATCH (a:P)-[:K]->(b:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:KC]->(b:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:K]-(b:P) RETURN count(*) AS n",
      "MATCH (a:P)-[]->(b:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:K|L]->(b:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:K]->(b:P)-[:L]->(c:P) RETURN count(*) AS n",
      "MATCH (a:P {id:1})-[:K]->(b:P) RETURN b.id AS b ORDER BY b",
      "MATCH (a:P {id:1})-[:KC]->(b:P) RETURN b.id AS b ORDER BY b",
      // The count push-down: overlapping types are not disjoint, and one inequality guards one pair of hops only
      "MATCH (a:P)-[:K]-(b:P)-[:KC]-(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:K]-(b:P)-[:KC]-(c:P) WHERE a <> c RETURN count(*) AS n",
      "MATCH (a:P)-[:K]-(b:P)-[:KC]-(c:P) WHERE a <> b RETURN count(*) AS n",
      "MATCH (a:P)-[:K]->(b:P)-[:KC]->(c:P) WHERE a <> c RETURN count(*) AS n",
      "MATCH (a:P)-[:K]-(b:P)-[:L]-(c:P)-[:K]-(d:P) WHERE a <> d RETURN count(*) AS n",
      "MATCH (a:P)-[:K]-(b:P)-[:L]-(c:P)-[:K]-(d:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:K]-(b:P)-[:L]-(c:P)-[:M]-(d:P) WHERE a <> c RETURN count(*) AS n",
      "MATCH (a:P)-[:K]-(b:P)-[:L]-(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:KC]-(b:P)-[:L]-(c:P) RETURN count(*) AS n",
      "MATCH (a:P)-[:K]-(b:P)-[:KC]-(c:P)-[:L]-(d:P) WHERE a <> c RETURN count(*) AS n" };

  private static final String[] VIEWS = {
      "VERTEX TYPES (P) EDGE TYPES (K, KC, L) PROPERTIES (id)",
      "VERTEX TYPES (P) EDGE TYPES (KC, K, L) PROPERTIES (id)",
      "VERTEX TYPES (P) EDGE TYPES (K, L) PROPERTIES (id)",
      "VERTEX TYPES (P) PROPERTIES (id)" };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE PROPERTY P.id INTEGER");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE KC EXTENDS K");
    database.command("sql", "CREATE EDGE TYPE L");
    database.command("sql", "CREATE EDGE TYPE M");

    // No self-loops: the count push-down counts an undirected self-loop twice, which is a separate defect and would
    // make the two sides of a comparison disagree for a reason that has nothing to do with sub-types
    final Random random = new Random(8426);
    database.transaction(() -> {
      final List<Vertex> vertices = new ArrayList<>();
      for (int i = 0; i < VERTICES; i++)
        vertices.add(database.newVertex("P").set("id", i).save());
      for (int i = 0; i < VERTICES; i++) {
        final Vertex v = vertices.get(i);
        for (int k = 0; k < 3; k++)
          v.asVertex().modify().newEdge("K", otherThan(vertices, i, random));
        if (random.nextInt(3) == 0)
          v.asVertex().modify().newEdge("KC", otherThan(vertices, i, random));
        if (random.nextInt(4) == 0) {
          final Vertex other = otherThan(vertices, i, random);
          v.asVertex().modify().newEdge("KC", other);
          v.asVertex().modify().newEdge("KC", other);
        }
        if (random.nextInt(3) == 0)
          v.asVertex().modify().newEdge("L", otherThan(vertices, i, random));
        v.asVertex().modify().newEdge("M", otherThan(vertices, i, random));
      }
    });
  }

  private static Vertex otherThan(final List<Vertex> vertices, final int index, final Random random) {
    int target;
    do {
      target = random.nextInt(vertices.size());
    } while (target == index);
    return vertices.get(target);
  }

  @Test
  void answersMatchTheQueriesWithoutTheViewWhateverOrderTheTypesAreListed() {
    final Map<String, List<String>> expected = oracle();

    for (final String definition : VIEWS) {
      createView(definition);
      final List<String> failures = new ArrayList<>();
      for (final String query : QUERIES) {
        final List<String> actual = answer(query);
        if (!actual.equals(expected.get(query)))
          failures.add(query + "\n  expected " + expected.get(query) + "\n  actual   " + actual + "\n" + plan(query));
      }
      assertThat(failures).as(definition).isEmpty();
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav8426");
    }
  }

  @Test
  void answersMatchWithChangesServedFromTheOverlayOnAViewListingTheSubType() {
    overlayChangesMatchTheOracle("VERTEX TYPES (P) EDGE TYPES (K, KC, L) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");
  }

  @Test
  void answersMatchWithChangesServedFromTheOverlayOnAnUnfilteredView() {
    overlayChangesMatchTheOracle("VERTEX TYPES (P) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");
  }

  private void overlayChangesMatchTheOracle(final String definition) {
    createView(definition);
    // Committed after the build, so served from the overlay
    database.transaction(() -> {
      final Vertex a = database.query("sql", "SELECT FROM P WHERE id = 1").next().getVertex().get();
      final Vertex b = database.query("sql", "SELECT FROM P WHERE id = 2").next().getVertex().get();
      a.modify().newEdge("KC", b);
      b.modify().newEdge("KC", a);
      a.modify().newEdge("K", b);
    });
    final Map<String, List<String>> withView = new LinkedHashMap<>();
    for (final String query : QUERIES)
      withView.put(query, answer(query));
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav8426");
    for (final String query : QUERIES)
      assertThat(withView.get(query)).as(query)
          .isEqualTo(answer(query.replace(" RETURN count(*) AS n", " RETURN sum(1) AS n")));
  }

  /** An edge type with no edges when the view was built has no slice, yet an untyped hop must still see its later edges. */
  @Test
  void anUntypedHopSeesTheEdgesOfATypeThatWasEmptyAtBuildTime() {
    database.command("sql", "CREATE EDGE TYPE E0");
    createView("VERTEX TYPES (P) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");
    database.transaction(() -> {
      final Vertex a = database.query("sql", "SELECT FROM P WHERE id = 3").next().getVertex().get();
      final Vertex b = database.query("sql", "SELECT FROM P WHERE id = 4").next().getVertex().get();
      a.modify().newEdge("E0", b);
      a.modify().newEdge("E0", b);
    });
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav8426");
    assertThat(view.getEdgeTypes()).contains("E0");
    assertThat(view.getMaterializedEdgeTypes()).contains("E0");

    final String[] queries = {
        "MATCH (a:P {id:3})-[]->(b:P) RETURN count(*) AS n",
        "MATCH (a:P {id:3})-[]->(b:P {id:4}) RETURN count(*) AS n",
        "MATCH (a:P)-[]-(b:P {id:4}) RETURN count(*) AS n" };
    final Map<String, List<String>> withView = new LinkedHashMap<>();
    for (final String query : queries)
      withView.put(query, answer(query));
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav8426");
    for (final String query : queries)
      assertThat(withView.get(query)).as(query).isEqualTo(answer(query));
  }

  /**
   * The self-loop subtraction of an inequality chain counted a returning path once per frontier entry and only tested
   * PRESENCE of the closing neighbour, so a pair joined by parallel edges closed one path instead of one per edge.
   */
  @Test
  void parallelEdgesEachCloseADistinctPathInTheInequalitySubtraction() {
    database.transaction(() -> {
      final Vertex x = database.newVertex("P").set("id", 900).save();
      final Vertex y = database.newVertex("P").set("id", 901).save();
      x.modify().newEdge("K", y);
      y.modify().newEdge("L", x);
      y.modify().newEdge("L", x);
      y.modify().newEdge("L", x);
      x.modify().newEdge("M", y);
    });
    final String query = "MATCH (a:P)-[:K]-(b:P)-[:L]-(c:P)-[:M]-(d:P) WHERE a <> c RETURN count(*) AS n";
    final List<String> expected = answer(query.replace(" RETURN count(*) AS n", " RETURN sum(1) AS n"));
    assertThat(answer(query)).isEqualTo(expected);

    createView("VERTEX TYPES (P) PROPERTIES (id)");
    assertThat(answer(query)).isEqualTo(expected);
  }

  @Test
  void theProviderResolvesATypeToItsSubTypes() {
    createView("VERTEX TYPES (P) EDGE TYPES (K, KC, L) PROPERTIES (id)");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav8426");
    assertThat(view.resolveEdgeTypes("K")).containsExactly("K", "KC");
    assertThat(view.resolveEdgeTypes("KC")).containsExactly("KC");
    assertThat(view.resolveEdgeTypes("L", "K")).containsExactly("L", "K", "KC");
    assertThat(view.coversEdgeType("KC")).isTrue();
  }

  /**
   * The answer the record path gives, which is what a view has to reproduce. A {@code count(*)} chain is answered by the
   * count push-down with or without a view, so the reference for those goes through {@code sum(1)}, which the push-down
   * does not claim.
   */
  /** getDegrees on a parent type sums its slices once each, on a two- and a three-level hierarchy, with and without an overlay. */
  @Test
  void degreesOfAParentTypeSumTheirSlicesOnce() {
    database.command("sql", "CREATE EDGE TYPE KCC EXTENDS KC");
    database.transaction(() -> {
      final Vertex a = database.query("sql", "SELECT FROM P WHERE id = 5").next().getVertex().get();
      final Vertex b = database.query("sql", "SELECT FROM P WHERE id = 6").next().getVertex().get();
      a.modify().newEdge("KCC", b);
      a.modify().newEdge("KCC", b);
    });
    createView("VERTEX TYPES (P) PROPERTIES (id) UPDATE MODE SYNCHRONOUS");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav8426");
    assertThat(view.resolveEdgeTypes("KC")).containsExactly("KC", "KCC");

    assertDegreesMatchTheRecords(view);

    database.transaction(() -> {
      final Vertex a = database.query("sql", "SELECT FROM P WHERE id = 5").next().getVertex().get();
      final Vertex b = database.query("sql", "SELECT FROM P WHERE id = 7").next().getVertex().get();
      a.modify().newEdge("KCC", b);
      a.modify().newEdge("KC", b);
    });
    assertDegreesMatchTheRecords(view);
  }

  private void assertDegreesMatchTheRecords(final GraphAnalyticalView view) {
    for (final String type : new String[] { "K", "KC", "KCC" }) {
      final int[] degrees = new int[view.getNodeIdUpperBound()];
      view.getDegrees(degrees, Vertex.DIRECTION.OUT, type);
      for (int id = 0; id < VERTICES; id++) {
        final Vertex v = database.query("sql", "SELECT FROM P WHERE id = " + id).next().getVertex().get();
        assertThat(degrees[view.getNodeId(v.getIdentity())]).as(type + " out-degree of " + id)
            .isEqualTo((int) v.countEdges(Vertex.DIRECTION.OUT, type));
      }
    }
  }

  private Map<String, List<String>> oracle() {
    final Map<String, List<String>> expected = new LinkedHashMap<>();
    for (final String query : QUERIES)
      expected.put(query, answer(query.replace(" RETURN count(*) AS n", " RETURN sum(1) AS n")));
    // The oracle has to see the sub-typed edges, or the comparison proves nothing
    assertThat(expected.get(QUERIES[2])).isNotEqualTo(expected.get(QUERIES[3]));
    return expected;
  }

  private void createView(final String definition) {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav8426 " + definition);
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav8426");
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
