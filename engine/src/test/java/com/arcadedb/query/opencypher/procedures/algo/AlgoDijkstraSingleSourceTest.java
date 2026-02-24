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
 * Tests for the algo.dijkstra.singleSource Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoDijkstraSingleSourceTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-dijkstra-ss");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("ROAD");

    // A --1--> B --2--> C
    //  \___3___________/
    // Shortest A→C = via A→B→C = 3; direct A→C = 3 (tie)
    // Shortest A→B = 1
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("City").set("name", "A").save();
      final MutableVertex b = database.newVertex("City").set("name", "B").save();
      final MutableVertex c = database.newVertex("City").set("name", "C").save();
      a.newEdge("ROAD", b, true, new Object[]{ "dist", 1.0 }).save();
      b.newEdge("ROAD", c, true, new Object[]{ "dist", 2.0 }).save();
      a.newEdge("ROAD", c, true, new Object[]{ "dist", 3.0 }).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void singleSourceReturnsAllReachableNodes() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (start:City {name: 'A'}) " +
            "CALL algo.dijkstra.singleSource(start, 'ROAD', 'dist') " +
            "YIELD node, cost RETURN node.name AS name, cost ORDER BY cost");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    // B and C reachable from A
    assertThat(results).hasSize(2);
  }

  @Test
  void singleSourceCorrectCosts() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (start:City {name: 'A'}) " +
            "CALL algo.dijkstra.singleSource(start, 'ROAD', 'dist') " +
            "YIELD node, cost RETURN node.name AS name, cost ORDER BY name");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
    // B: cost 1.0; C: cost 3.0
    final Result b = results.stream().filter(r -> "B".equals(r.getProperty("name"))).findFirst().orElseThrow();
    final Result c = results.stream().filter(r -> "C".equals(r.getProperty("name"))).findFirst().orElseThrow();
    assertThat(((Number) b.getProperty("cost")).doubleValue()).isEqualTo(1.0);
    assertThat(((Number) c.getProperty("cost")).doubleValue()).isEqualTo(3.0);
  }

  @Test
  void singleSourceAllCostsPositive() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (start:City {name: 'A'}) " +
            "CALL algo.dijkstra.singleSource(start, 'ROAD', 'dist') " +
            "YIELD node, cost RETURN cost");

    while (rs.hasNext())
      assertThat(((Number) rs.next().getProperty("cost")).doubleValue()).isGreaterThan(0.0);
  }
}
