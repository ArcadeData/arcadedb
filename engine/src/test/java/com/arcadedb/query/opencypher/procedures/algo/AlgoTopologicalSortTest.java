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
 * Tests for the algo.topologicalSort Cypher procedure.
 */
class AlgoTopologicalSortTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-toposort");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Diamond DAG: A->B, A->C, B->D, C->D
    // Valid topological order: A(0), B or C (1/2), D(3)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      a.newEdge("EDGE", c, true, (Object[]) null).save();
      b.newEdge("EDGE", d, true, (Object[]) null).save();
      c.newEdge("EDGE", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void topologicalSortAllNodesOrdered() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.topologicalSort() YIELD node, order RETURN node, order");

    while (rs.hasNext()) {
      final Result result = rs.next();
      final int order = ((Number) result.getProperty("order")).intValue();
      assertThat(order).isGreaterThanOrEqualTo(0);
    }
  }

  @Test
  void topologicalSortAFirst() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.topologicalSort() YIELD node, order RETURN node.name AS name, order");

    int orderA = -1;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String name = (String) result.getProperty("name");
      final int order = ((Number) result.getProperty("order")).intValue();
      if ("A".equals(name))
        orderA = order;
    }

    assertThat(orderA).isEqualTo(0);
  }

  @Test
  void topologicalSortDLast() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.topologicalSort() YIELD node, order RETURN node.name AS name, order");

    int orderD = -1;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String name = (String) result.getProperty("name");
      final int order = ((Number) result.getProperty("order")).intValue();
      if ("D".equals(name))
        orderD = order;
    }

    assertThat(orderD).isEqualTo(3);
  }
}
