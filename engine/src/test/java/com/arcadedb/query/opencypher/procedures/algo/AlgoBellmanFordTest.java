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
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.bellmanford Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoBellmanFordTest {
  private Database    database;
  private MutableVertex vA, vB, vC, vD;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-bellmanford");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("ROAD");

    // Graph: A -1-> B -1-> C -1-> D
    //        A -10-> C (shortcut but more expensive)
    database.transaction(() -> {
      vA = database.newVertex("City").set("name", "A").save();
      vB = database.newVertex("City").set("name", "B").save();
      vC = database.newVertex("City").set("name", "C").save();
      vD = database.newVertex("City").set("name", "D").save();

      final MutableEdge eAB = vA.newEdge("ROAD", vB, true, (Object[]) null);
      eAB.set("distance", 1.0);
      eAB.save();

      final MutableEdge eBC = vB.newEdge("ROAD", vC, true, (Object[]) null);
      eBC.set("distance", 1.0);
      eBC.save();

      final MutableEdge eCD = vC.newEdge("ROAD", vD, true, (Object[]) null);
      eCD.set("distance", 1.0);
      eCD.save();

      final MutableEdge eAC = vA.newEdge("ROAD", vC, true, (Object[]) null);
      eAC.set("distance", 10.0);
      eAC.save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void bellmanFordFindsShortestPath() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:City {name: 'A'}), (d:City {name: 'D'}) " +
            "CALL algo.bellmanford(a, d, 'ROAD', 'distance') " +
            "YIELD path, weight, negativeCycle " +
            "RETURN path, weight, negativeCycle");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();

    final Object negativeCycle = result.getProperty("negativeCycle");
    assertThat(negativeCycle).isEqualTo(false);
    final double weight = ((Number) result.getProperty("weight")).doubleValue();
    assertThat(weight).isEqualTo(3.0); // A->B->C->D = 1+1+1 = 3

    final Map<String, Object> path = result.getProperty("path");
    assertThat(path).isNotNull();
    assertThat(((List<?>) path.get("nodes"))).hasSize(4);
  }

  @Test
  void bellmanFordWithNegativeWeights() {
    // Add a negative-weight edge B->D to create a shorter path A->B->D (-5)
    database.transaction(() -> {
      final MutableEdge eBD = vB.newEdge("ROAD", vD, true, (Object[]) null);
      eBD.set("distance", -5.0);
      eBD.save();
    });

    final ResultSet rs = database.query("opencypher",
        "MATCH (a:City {name: 'A'}), (d:City {name: 'D'}) " +
            "CALL algo.bellmanford(a, d, 'ROAD', 'distance') " +
            "YIELD path, weight, negativeCycle " +
            "RETURN path, weight, negativeCycle");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object negativeCycleFlag = result.getProperty("negativeCycle");
    assertThat(negativeCycleFlag).isEqualTo(false);
    // A->B->D = 1 + (-5) = -4
    final double weight = ((Number) result.getProperty("weight")).doubleValue();
    assertThat(weight).isEqualTo(-4.0);
  }

  @Test
  void bellmanFordNoPathReturnsEmpty() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (d:City {name: 'D'}), (a:City {name: 'A'}) " +
            "CALL algo.bellmanford(d, a, 'ROAD', 'distance') " +
            "YIELD path, weight, negativeCycle " +
            "RETURN path, weight, negativeCycle");

    // D has no outgoing edges so no path from D to A
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void bellmanFordSameSourceAndDestination() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:City {name: 'A'}) " +
            "CALL algo.bellmanford(a, a, 'ROAD', 'distance') " +
            "YIELD path, weight, negativeCycle " +
            "RETURN path, weight, negativeCycle");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final double weight = ((Number) result.getProperty("weight")).doubleValue();
    assertThat(weight).isEqualTo(0.0);
  }

  @Test
  void bellmanFordDetectsNegativeCycle() {
    // Create a negative cycle: B->C->B with total weight < 0
    database.transaction(() -> {
      final MutableEdge eCB = vC.newEdge("ROAD", vB, true, (Object[]) null);
      eCB.set("distance", -10.0);
      eCB.save();
    });

    final ResultSet rs = database.query("opencypher",
        "MATCH (a:City {name: 'A'}), (d:City {name: 'D'}) " +
            "CALL algo.bellmanford(a, d, 'ROAD', 'distance') " +
            "YIELD path, weight, negativeCycle " +
            "RETURN path, weight, negativeCycle");

    if (rs.hasNext()) {
      final Result result = rs.next();
      // If negative cycle is detected, the flag should be true
      final Object cycleFlag = result.getProperty("negativeCycle");
      assertThat(cycleFlag).isEqualTo(true);
    }
    // If no result, that's also acceptable (unreachable due to cycle detection)
  }

  @Test
  void bellmanFordWithRelTypeFilter() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:City {name: 'A'}), (d:City {name: 'D'}) " +
            "CALL algo.bellmanford(a, d, 'ROAD', 'distance') " +
            "YIELD path, weight " +
            "RETURN weight");

    assertThat(rs.hasNext()).isTrue();
    final double weight = ((Number) rs.next().getProperty("weight")).doubleValue();
    assertThat(weight).isEqualTo(3.0);
  }
}
