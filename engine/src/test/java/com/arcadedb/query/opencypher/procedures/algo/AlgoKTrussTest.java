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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the algo.kTruss Cypher procedure.
 */
class AlgoKTrussTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-ktruss");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Triangle A-B-C (all connected to each other) plus dangling node D connected only to A
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      // Triangle edges (undirected represented as bidirectional directed)
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      b.newEdge("EDGE", a, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
      c.newEdge("EDGE", b, true, (Object[]) null).save();
      a.newEdge("EDGE", c, true, (Object[]) null).save();
      c.newEdge("EDGE", a, true, (Object[]) null).save();
      // Dangling edge
      a.newEdge("EDGE", d, true, (Object[]) null).save();
      d.newEdge("EDGE", a, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void kTrussReturnsOneRowPerVertex() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.kTruss() YIELD nodeId, trussNumber RETURN nodeId, trussNumber");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);
  }

  @Test
  void kTrussTriangleNodesHaveHigherTruss() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.kTruss() YIELD nodeId, trussNumber RETURN nodeId, trussNumber");

    int maxTruss = 0;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object val = r.getProperty("trussNumber");
      final int t = ((Number) val).intValue();
      if (t > maxTruss)
        maxTruss = t;
    }
    // Triangle nodes should be in at least a 3-truss
    assertThat(maxTruss).isGreaterThanOrEqualTo(3);
  }

  @Test
  void kTrussTrussNumbersNonNegative() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.kTruss() YIELD nodeId, trussNumber RETURN nodeId, trussNumber");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object val = r.getProperty("trussNumber");
      assertThat(((Number) val).intValue()).isGreaterThanOrEqualTo(0);
    }
  }

  @Test
  void kTrussWithRelType() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.kTruss('EDGE') YIELD nodeId, trussNumber RETURN nodeId, trussNumber");

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(4);
  }

  @Test
  void kTrussEmptyGraph() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-ktruss-empty");
    if (factory.exists())
      factory.open().drop();
    final Database emptyDb = factory.create();
    emptyDb.getSchema().createVertexType("Node");
    emptyDb.getSchema().createEdgeType("EDGE");

    final ResultSet rs = emptyDb.query("opencypher",
        "CALL algo.kTruss() YIELD nodeId, trussNumber RETURN nodeId, trussNumber");

    assertThat(rs.hasNext()).isFalse();
    emptyDb.drop();
  }

  /**
   * Issue #12303: {@code k} selects the k-truss. A call that passes it answers the vertices of the k-truss only (truss number at
   * least k), a call that omits it (or passes {@code null}) answers every vertex with its truss number, as before.
   * <p>
   * The fixture has every truss number in it: a K4 {@code A B C D} (truss 4), a triangle {@code D E F} hanging off it (E and F truss
   * 3, D keeps 4), a pendant {@code G} off F (truss 2) and an isolated {@code H} (truss 0). The edges are one per pair, in one
   * direction, which is the shape the decomposition reads (the simple undirected graph).
   */
  @Nested
  class Issue12303KSelectsTheKTruss {
    private Database graph;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-ktruss-12303");
      if (factory.exists())
        factory.open().drop();
      graph = factory.create();
      graph.getSchema().createVertexType("Node");
      graph.getSchema().createEdgeType("KNOWS");

      graph.transaction(() -> {
        final Map<String, MutableVertex> v = new TreeMap<>();
        for (final String name : List.of("A", "B", "C", "D", "E", "F", "G", "H"))
          v.put(name, graph.newVertex("Node").set("name", name).save());
        for (final String pair : List.of("AB", "AC", "AD", "BC", "BD", "CD", "DE", "DF", "EF", "FG"))
          v.get(pair.substring(0, 1)).newEdge("KNOWS", v.get(pair.substring(1, 2))).save();
      });
    }

    @AfterEach
    void teardown() {
      if (graph != null)
        graph.drop();
    }

    @Test
    void withoutKEveryVertexIsListed() {
      assertThat(trussByName("CALL algo.kTruss('KNOWS') YIELD nodeId, trussNumber RETURN nodeId, trussNumber")).isEqualTo(
          Map.of("A", 4, "B", 4, "C", 4, "D", 4, "E", 3, "F", 3, "G", 2, "H", 0));
    }

    @Test
    void anExplicitNullKIsAnOmittedOne() {
      assertThat(trussByName("CALL algo.kTruss('KNOWS', null) YIELD nodeId, trussNumber RETURN nodeId, trussNumber")).hasSize(8);
    }

    @Test
    void kSelectsTheVerticesOfTheKTruss() {
      assertThat(trussByName("CALL algo.kTruss('KNOWS', 2) YIELD nodeId, trussNumber RETURN nodeId, trussNumber")).isEqualTo(
          Map.of("A", 4, "B", 4, "C", 4, "D", 4, "E", 3, "F", 3, "G", 2));
      assertThat(trussByName("CALL algo.kTruss('KNOWS', 3) YIELD nodeId, trussNumber RETURN nodeId, trussNumber")).isEqualTo(
          Map.of("A", 4, "B", 4, "C", 4, "D", 4, "E", 3, "F", 3));
      assertThat(trussByName("CALL algo.kTruss('KNOWS', 4) YIELD nodeId, trussNumber RETURN nodeId, trussNumber")).isEqualTo(
          Map.of("A", 4, "B", 4, "C", 4, "D", 4));
      assertThat(trussByName("CALL algo.kTruss('KNOWS', 5) YIELD nodeId, trussNumber RETURN nodeId, trussNumber")).isEmpty();
      assertThat(trussByName("CALL algo.kTruss('KNOWS', 100) YIELD nodeId, trussNumber RETURN nodeId, trussNumber")).isEmpty();
    }

    @Test
    void kIsReadFromAParameterAndWithEveryRelationshipType() {
      assertThat(trussByName("CALL algo.kTruss(null, $k) YIELD nodeId, trussNumber RETURN nodeId, trussNumber", Map.of("k", 4)))
          .isEqualTo(Map.of("A", 4, "B", 4, "C", 4, "D", 4));
    }

    @Test
    void aKBelowTwoIsRefused() {
      // Every edge is in the 2-truss, so a smaller k names no truss: refused, rather than read as "every vertex"
      for (final int k : new int[] { 1, 0, -3 })
        assertThatThrownBy(() -> trussByName("CALL algo.kTruss('KNOWS', " + k + ") YIELD nodeId RETURN nodeId"))
            .hasStackTraceContaining("k must be at least 2");
    }

    @Test
    void aKThatIsNotANumberIsRefused() {
      assertThatThrownBy(() -> trussByName("CALL algo.kTruss('KNOWS', '4') YIELD nodeId RETURN nodeId"))
          .hasStackTraceContaining("k must be a number");
    }

    private Map<String, Integer> trussByName(final String query) {
      return trussByName(query, Map.of());
    }

    private Map<String, Integer> trussByName(final String query, final Map<String, Object> params) {
      final Map<String, Integer> result = new TreeMap<>();
      try (final ResultSet rs = graph.query("opencypher", query, params)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          final String name = graph.lookupByRID(r.getProperty("nodeId"), true).asVertex().getString("name");
          assertThat(result.put(name, ((Number) r.getProperty("trussNumber")).intValue())).as("vertex %s listed once", name).isNull();
        }
      }
      return result;
    }
  }
}
