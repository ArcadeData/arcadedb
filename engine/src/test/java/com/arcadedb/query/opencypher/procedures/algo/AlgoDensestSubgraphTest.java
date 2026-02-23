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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.densestSubgraph Cypher procedure.
 */
class AlgoDensestSubgraphTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-densest-subgraph");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Dense clique: A-B, A-C, B-C (triangle with density=1.5 for 3 nodes, 3 undirected edges)
    // Sparse spoke: D connected only to A (low density when included)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      a.newEdge("LINK", c, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      d.newEdge("LINK", a, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void densestSubgraphReturnsFourNodes() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.densestSubgraph() YIELD node, inDenseSubgraph, density RETURN node, inDenseSubgraph, density");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);
  }

  @Test
  void densestSubgraphFieldsNotNull() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.densestSubgraph() YIELD node, inDenseSubgraph, density RETURN node, inDenseSubgraph, density");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object node = r.getProperty("node");
      final Object inDenseSubgraph = r.getProperty("inDenseSubgraph");
      final Object density = r.getProperty("density");
      assertThat(node).isNotNull();
      assertThat(inDenseSubgraph).isNotNull();
      assertThat(density).isNotNull();
    }
  }

  @Test
  void densestSubgraphDensityIsNonNegative() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.densestSubgraph() YIELD node, density RETURN density");

    while (rs.hasNext()) {
      final Object densityObj = rs.next().getProperty("density");
      assertThat(((Number) densityObj).doubleValue()).isGreaterThanOrEqualTo(0.0);
    }
  }

  @Test
  void densestSubgraphAtLeastOneNodeInSubgraph() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.densestSubgraph() YIELD node, inDenseSubgraph RETURN node, inDenseSubgraph");

    boolean anyInSubgraph = false;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object inDenseObj = r.getProperty("inDenseSubgraph");
      if (Boolean.TRUE.equals(inDenseObj))
        anyInSubgraph = true;
    }

    assertThat(anyInSubgraph).isTrue();
  }

  @Test
  void densestSubgraphTriangleNodesAreInSubgraph() {
    // The triangle A-B-C is denser than the full graph including D,
    // so after peeling, the dense subgraph should contain A, B, C
    final ResultSet rs = database.query("opencypher",
        "CALL algo.densestSubgraph() YIELD node, inDenseSubgraph RETURN node, inDenseSubgraph");

    boolean inSubgraphA = false, inSubgraphB = false, inSubgraphC = false;
    int inSubgraphCount = 0;

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      final Object inDenseObj = r.getProperty("inDenseSubgraph");
      final boolean inSubgraph = Boolean.TRUE.equals(inDenseObj);
      if (inSubgraph)
        inSubgraphCount++;
      if (nodeObj instanceof Vertex v) {
        final String name = v.getString("name");
        switch (name) {
          case "A" -> inSubgraphA = inSubgraph;
          case "B" -> inSubgraphB = inSubgraph;
          case "C" -> inSubgraphC = inSubgraph;
        }
      }
    }

    // The triangle {A,B,C} is the densest part so all three should be in the subgraph
    assertThat(inSubgraphA).isTrue();
    assertThat(inSubgraphB).isTrue();
    assertThat(inSubgraphC).isTrue();
    assertThat(inSubgraphCount).isGreaterThanOrEqualTo(3);
  }
}
