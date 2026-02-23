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
}
