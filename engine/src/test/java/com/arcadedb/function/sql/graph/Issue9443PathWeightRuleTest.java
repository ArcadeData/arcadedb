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
package com.arcadedb.function.sql.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.EdgeWeight;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression test for issue #9443: every weighted shortest-path finder reads an edge's weight the same way. An edge
 * without the weight property weighs 1 (it used to weigh 0 in {@code dijkstra()}, {@code astar()}, {@code algo.dijkstra}
 * and {@code algo.astar}, which made every unweighted edge free), and an edge whose weight is negative or NaN is not
 * walked (it used to be walked by A*, Dijkstra, {@code duanSSSP()}, Yen's {@code algo.kShortestPaths} and
 * {@code algo.steinerTree}, which breaks their invariant and answers an arbitrary path).
 * <p>
 * Three fixtures, each a triangle with a direct edge A-C against a two-hop A-B-C, built so that only the right rule
 * answers the direct edge:
 * <ul>
 *   <li>{@code M}: A-B and B-C have no weight, A-C weighs 1.5. By the 0 rule A-B-C is free; by the 1 rule it costs 2;</li>
 *   <li>{@code N}: A-B weighs 1, B-C weighs -1, A-C weighs 3. Walking the negative edge makes A-B-C cost 0;</li>
 *   <li>{@code X}: A-B weighs 1, B-C weighs NaN, A-C weighs 3.</li>
 * </ul>
 * Every finder is asked twice: on the records, and with a Graph Analytical View materializing the weight, so the
 * columnar arms (A*'s CSR neighbourhood, the {@code algo.dijkstra.singleSource} kernel, the shortest path finder's view
 * search) are held to the same rule.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9443PathWeightRuleTest {
  private static final String[] FIXTURES = { "M", "N", "X" };
  private static final double[] EXPECTED = { 1.5, 3.0, 3.0 };

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-9443-path-weights");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("node");
    database.getSchema().createEdgeType("road").createProperty("weight", Type.DOUBLE);
    database.getSchema().createEdgeType("rail").createProperty("weight", Type.DOUBLE);

    database.transaction(() -> {
      triangle("M", null, null, 1.5);
      triangle("N", 1.0, -1.0, 3.0);
      triangle("X", 1.0, Double.NaN, 3.0);

      // duanSSSP edge type filter: X-M-Y over rail costs 2, the direct X-Y over road costs 5
      final MutableVertex x = vertex("TX");
      final MutableVertex m = vertex("TM");
      final MutableVertex y = vertex("TY");
      x.newEdge("rail", m, true, new Object[] { "weight", 1.0 }).save();
      m.newEdge("rail", y, true, new Object[] { "weight", 1.0 }).save();
      x.newEdge("road", y, true, new Object[] { "weight", 5.0 }).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  @Test
  void theRuleItself() {
    assertThat(EdgeWeight.of(null)).isEqualTo(EdgeWeight.MISSING);
    assertThat(EdgeWeight.of("12")).isEqualTo(EdgeWeight.MISSING);
    assertThat(EdgeWeight.of(0)).isEqualTo(0.0);
    assertThat(EdgeWeight.of(2.5f)).isEqualTo(2.5);
    assertThat(EdgeWeight.of(7L)).isEqualTo(7.0);
    assertThat(EdgeWeight.isWalkable(EdgeWeight.of(-0.5))).isFalse();
    assertThat(EdgeWeight.isWalkable(EdgeWeight.of(Double.NaN))).isFalse();
    assertThat(EdgeWeight.isWalkable(EdgeWeight.of(Double.POSITIVE_INFINITY))).isFalse();
    assertThat(EdgeWeight.isWalkable(EdgeWeight.of(Double.NEGATIVE_INFINITY))).isFalse();
    assertThat(EdgeWeight.isWalkable(EdgeWeight.MISSING)).isTrue();
  }

  @Test
  void sqlFunctionsOnRecords() {
    assertSqlFunctions("records");
  }

  @Test
  void sqlFunctionsWithAView() {
    final GraphAnalyticalView view = weightedView();
    try {
      assertSqlFunctions("view");
    } finally {
      view.drop();
    }
  }

  @Test
  void cypherProceduresOnRecords() {
    assertCypherProcedures("records");
  }

  @Test
  void cypherProceduresWithAView() {
    final GraphAnalyticalView view = weightedView();
    try {
      assertCypherProcedures("view");
    } finally {
      view.drop();
    }
  }

  @Test
  void duanSSSPAcceptsAnEdgeTypeFilter() {
    assertThat(sqlPath("duanSSSP", "TX", "TY", "")).as("every type").containsExactly("TX", "TM", "TY");
    assertThat(sqlPath("duanSSSP", "TX", "TY", ", {edgeTypeNames: ['road']}")).containsExactly("TX", "TY");
    assertThat(sqlPath("duanSSSP", "TX", "TY", ", {edgeTypeNames: ['rail']}")).containsExactly("TX", "TM", "TY");
    assertThat(sqlPath("duanSSSP", "TY", "TX", ", {direction: 'IN', edgeTypeNames: ['road']}"))
        .containsExactly("TY", "TX");
    assertThat(sqlPath("duanSSSP", "TY", "TX", ", 'IN'")).as("the positional direction still works")
        .containsExactly("TY", "TM", "TX");
    assertThat(sqlPath("duanSSSP", "TX", "TY", ", {direction: 'IN'}")).as("against the edges").isEmpty();
  }

  @Test
  void dijkstraFallsBackToOneForAWeightPropertyNoEdgeHas() {
    // Every edge lacks 'cost', so every edge weighs 1 and the direct edge wins on hop count. By the 0 rule all paths
    // tied at 0 and the answer depended on which one the search happened to reach first.
    for (final String function : new String[] { "dijkstra", "astar", "duanSSSP", "cchShortestPath" })
      assertThat(namesOf(sqlRids("SELECT " + function + "(?, ?, 'cost') AS p", rid("NA"), rid("NC"))))
          .as(function).containsExactly("NA", "NC");
  }

  // ---------------------------------------------------------------------------------------------------------------

  private void assertSqlFunctions(final String mode) {
    // filtered on the type the view covers, so a view is found and its columnar arm answers in "view" mode
    for (final String function : new String[] { "dijkstra", "astar", "duanSSSP", "cchShortestPath" })
      for (final String fixture : FIXTURES)
        for (final String options : new String[] { "", ", {edgeTypeNames: ['road']}" })
          assertThat(sqlPath(function, fixture + "A", fixture + "C", options))
              .as("%s()%s on fixture %s (%s)", function, options, fixture, mode)
              .containsExactly(fixture + "A", fixture + "C");
  }

  private void assertCypherProcedures(final String mode) {
    for (int i = 0; i < FIXTURES.length; i++) {
      final String f = FIXTURES[i];
      final double expected = EXPECTED[i];

      for (final String call : new String[] {
          "algo.dijkstra(a, c, 'road', 'weight', 'OUT')",
          "algo.astar(a, c, 'road', 'weight')",
          "algo.cch.shortestPath(a, c, 'road', 'weight', 'OUT')",
          "algo.kShortestPaths(a, c, 1, 'road', 'weight')" }) {
        final Result row = single("""
            MATCH (a:node {name: $a}), (c:node {name: $c}) CALL %s YIELD path, weight RETURN path, weight"""
            .formatted(call), f);
        assertThat(pathNames(row.getProperty("path"))).as("%s on fixture %s (%s)", call, f, mode)
            .containsExactly(f + "A", f + "C");
        assertThat(((Number) row.getProperty("weight")).doubleValue()).as("%s weight on fixture %s (%s)", call, f, mode)
            .isCloseTo(expected, within(1e-9));
      }

      final Map<String, Double> costs = new HashMap<>();
      try (final ResultSet rs = database.query("opencypher", """
          MATCH (a:node {name: $a}) CALL algo.dijkstra.singleSource(a, 'road', 'weight') YIELD node, cost
          RETURN node.name AS name, cost""", Map.of("a", f + "A"))) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          costs.put(row.getProperty("name"), ((Number) row.getProperty("cost")).doubleValue());
        }
      }
      assertThat(costs.get(f + "C")).as("algo.dijkstra.singleSource cost of C on fixture %s (%s)", f, mode)
          .isCloseTo(expected, within(1e-9));
      // B is reached over A-B alone: 1 for the edge without a weight, as for the weighted ones
      assertThat(costs.get(f + "B")).as("algo.dijkstra.singleSource cost of B on fixture %s (%s)", f, mode)
          .isCloseTo(1.0, within(1e-9));

      final Result steiner = single("""
          MATCH (a:node {name: $a}), (c:node {name: $c}) CALL algo.steinerTree([a, c], 'road', 'weight')
          YIELD totalWeight RETURN totalWeight""", f);
      assertThat(((Number) steiner.getProperty("totalWeight")).doubleValue())
          .as("algo.steinerTree total weight on fixture %s (%s)", f, mode).isCloseTo(expected, within(1e-9));
    }
  }

  private void triangle(final String prefix, final Double ab, final Double bc, final double ac) {
    final MutableVertex a = vertex(prefix + "A");
    final MutableVertex b = vertex(prefix + "B");
    final MutableVertex c = vertex(prefix + "C");
    edge(a, b, ab);
    edge(b, c, bc);
    edge(a, c, ac);
  }

  private MutableVertex vertex(final String name) {
    return database.newVertex("node").set("name", name).save();
  }

  private void edge(final MutableVertex from, final MutableVertex to, final Double weight) {
    if (weight == null)
      from.newEdge("road", to).save();
    else
      from.newEdge("road", to, true, new Object[] { "weight", weight }).save();
  }

  private GraphAnalyticalView weightedView() {
    return GraphAnalyticalView.builder(database)
        .withName("issue-9443-view")
        .withVertexTypes("node")
        .withEdgeTypes("road")
        .withEdgeProperties("weight")
        .build();
  }

  private List<String> sqlPath(final String function, final String from, final String to, final String options) {
    return namesOf(sqlRids("SELECT " + function + "(?, ?, 'weight'" + options + ") AS p", rid(from), rid(to)));
  }

  private List<?> sqlRids(final String sql, final Object... args) {
    try (final ResultSet rs = database.query("sql", sql, args)) {
      final List<?> path = rs.next().getProperty("p");
      return path == null ? List.of() : path;
    }
  }

  private Result single(final String cypher, final String fixture) {
    try (final ResultSet rs = database.query("opencypher", cypher, Map.of("a", fixture + "A", "c", fixture + "C"))) {
      assertThat(rs.hasNext()).as(cypher).isTrue();
      return rs.next();
    }
  }

  private RID rid(final String name) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM node WHERE name = ?", name)) {
      return rs.next().getIdentity().get();
    }
  }

  private static List<String> pathNames(final Object path) {
    return namesOf((List<?>) ((Map<?, ?>) path).get("nodes"));
  }

  private static List<String> namesOf(final List<?> elements) {
    final List<String> names = new ArrayList<>(elements.size());
    for (final Object element : elements)
      names.add(((Vertex) ((Identifiable) element).getRecord()).getString("name"));
    return names;
  }
}
