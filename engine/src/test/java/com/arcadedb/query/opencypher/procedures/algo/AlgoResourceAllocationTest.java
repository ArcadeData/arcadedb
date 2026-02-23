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
 * Tests for the algo.resourceAllocation Cypher procedure.
 */
class AlgoResourceAllocationTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-resource-allocation");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // A->B, A->C: A's neighbors are B and C
    // E->B, E->C: E shares neighbors B and C with A
    // With BOTH direction: B has degree 2, C has degree 2
    // RA(A,E) = 1/deg(B) + 1/deg(C) = 1/2 + 1/2 = 1.0
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      a.newEdge("EDGE", c, true, (Object[]) null).save();
      e.newEdge("EDGE", b, true, (Object[]) null).save();
      e.newEdge("EDGE", c, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void resourceAllocationResultHasCorrectFields() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.resourceAllocation(a, null, 'BOTH', 0.0) " +
            "YIELD node1, node2, score " +
            "RETURN node1, node2, score");

    while (rs.hasNext()) {
      final Result result = rs.next();
      final Object node1 = result.getProperty("node1");
      final Object node2 = result.getProperty("node2");
      final Object score = result.getProperty("score");
      assertThat(node1).isNotNull();
      assertThat(node2).isNotNull();
      assertThat(score).isNotNull();
    }
  }

  @Test
  void resourceAllocationFindsSimilarNode() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.resourceAllocation(a, null, 'BOTH', 0.0) " +
            "YIELD node1, node2, score " +
            "RETURN node2.name AS name, score");

    boolean foundE = false;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String name = (String) result.getProperty("name");
      final double score = ((Number) result.getProperty("score")).doubleValue();
      if ("E".equals(name)) {
        assertThat(score).isGreaterThan(0.0);
        foundE = true;
      }
    }

    assertThat(foundE).isTrue();
  }

  @Test
  void resourceAllocationCutoffFilters() {
    // With a very high cutoff, no results should be returned
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.resourceAllocation(a, null, 'BOTH', 100.0) " +
            "YIELD node1, node2, score " +
            "RETURN node1, node2, score");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).isEmpty();
  }

  @Test
  void resourceAllocationScoreIsPositive() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.resourceAllocation(a, null, 'BOTH', 0.0) " +
            "YIELD node1, node2, score " +
            "RETURN score");

    while (rs.hasNext()) {
      final Object scoreObj = rs.next().getProperty("score");
      assertThat(((Number) scoreObj).doubleValue()).isGreaterThan(0.0);
    }
  }
}
