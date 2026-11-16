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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
   * Issue #12173: the decomposition held an n x n neighbour bit matrix twice (2 MB each for these 4,000 nodes). On a
   * ring whose every node also links to the node two ahead, a ring edge is in two triangles and a chord in one: the
   * chords go at k = 4, which takes the ring edges' support down to 0, so every edge and every ring node has truss 3.
   */
  @Test
  void sparseGraphRunsWithinABudgetSmallerThanTheBitMatrix() {
    final int n = 4000;
    database.getSchema().createEdgeType("RING");
    database.transaction(() -> {
      final List<MutableVertex> ring = new ArrayList<>(n);
      for (int i = 0; i < n; i++)
        ring.add(database.newVertex("Node").set("name", "r" + i).save());
      for (int i = 0; i < n; i++) {
        ring.get(i).newEdge("RING", ring.get((i + 1) % n), (Object[]) null).save();
        ring.get(i).newEdge("RING", ring.get((i + 2) % n), (Object[]) null).save();
      }
    });
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1_800_000L);

    final ResultSet rs = database.query("opencypher",
        "CALL algo.kTruss('RING') YIELD nodeId, trussNumber RETURN nodeId.name AS name, trussNumber");
    int ringNodes = 0;
    while (rs.hasNext()) {
      final Result row = rs.next();
      if (row.<String>getProperty("name").startsWith("r")) {
        assertThat(row.<Integer>getProperty("trussNumber")).isEqualTo(3);
        ringNodes++;
      } else
        assertThat(row.<Integer>getProperty("trussNumber")).isZero();
    }
    assertThat(ringNodes).isEqualTo(n);
  }
}
