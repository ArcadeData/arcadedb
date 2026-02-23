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
 * Tests for the algo.commonNeighbors Cypher procedure.
 */
class AlgoCommonNeighborsTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-common-neighbors");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // A->B, A->C: A's neighbors are B and C
    // E->B, E->C: E shares neighbors B and C with A (commonNeighbors(A,E)=2)
    // D->B: D shares only B with A (commonNeighbors(A,D)=1)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      a.newEdge("EDGE", c, true, (Object[]) null).save();
      e.newEdge("EDGE", b, true, (Object[]) null).save();
      e.newEdge("EDGE", c, true, (Object[]) null).save();
      d.newEdge("EDGE", b, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void commonNeighborsResultHasCorrectFields() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.commonNeighbors(a, null, 'BOTH', 1) " +
            "YIELD node1, node2, commonNeighbors " +
            "RETURN node1, node2, commonNeighbors");

    while (rs.hasNext()) {
      final Result result = rs.next();
      final Object node1 = result.getProperty("node1");
      final Object node2 = result.getProperty("node2");
      final Object commonNeighbors = result.getProperty("commonNeighbors");
      assertThat(node1).isNotNull();
      assertThat(node2).isNotNull();
      assertThat(commonNeighbors).isNotNull();
    }
  }

  @Test
  void commonNeighborsFindsBestMatch() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.commonNeighbors(a, null, 'BOTH', 1) " +
            "YIELD node1, node2, commonNeighbors " +
            "RETURN node2.name AS name, commonNeighbors");

    boolean foundE = false;
    int countE = 0;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String name = (String) result.getProperty("name");
      final int count = ((Number) result.getProperty("commonNeighbors")).intValue();
      if ("E".equals(name)) {
        foundE = true;
        countE = count;
      }
    }

    assertThat(foundE).isTrue();
    assertThat(countE).isEqualTo(2);
  }

  @Test
  void commonNeighborsCutoffFilters() {
    // With cutoff=2, only E should appear (2 common neighbors with A)
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.commonNeighbors(a, null, 'BOTH', 2) " +
            "YIELD node1, node2, commonNeighbors " +
            "RETURN node2.name AS name, commonNeighbors");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(1);
    final Object nameObj = results.get(0).getProperty("name");
    assertThat(nameObj).isEqualTo("E");
  }

  @Test
  void commonNeighborsCountIsPositive() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name: 'A'}) " +
            "CALL algo.commonNeighbors(a, null, 'BOTH', 1) " +
            "YIELD node1, node2, commonNeighbors " +
            "RETURN commonNeighbors");

    while (rs.hasNext()) {
      final Object countObj = rs.next().getProperty("commonNeighbors");
      assertThat(((Number) countObj).intValue()).isGreaterThanOrEqualTo(1);
    }
  }
}
