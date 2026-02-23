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
 * Tests for the algo.mst Cypher procedure (Minimum Spanning Tree).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoMSTTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-mst");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("ROAD");

    // Graph: A-B(1), A-C(5), B-C(2), B-D(6), C-D(3)
    // MST (Kruskal): A-B(1), B-C(2), C-D(3) → totalWeight=6.0
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("ROAD", b, true, new Object[]{"w", 1}).save();
      a.newEdge("ROAD", c, true, new Object[]{"w", 5}).save();
      b.newEdge("ROAD", c, true, new Object[]{"w", 2}).save();
      b.newEdge("ROAD", d, true, new Object[]{"w", 6}).save();
      c.newEdge("ROAD", d, true, new Object[]{"w", 3}).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void mstHasThreeEdges() {
    // For 4 nodes, the MST must have exactly n-1 = 3 edges
    final ResultSet rs = database.query("opencypher",
        "CALL algo.mst('w') YIELD source, target, weight, totalWeight RETURN source, target, weight, totalWeight");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3);
  }

  @Test
  void mstTotalWeightIsSix() {
    // MST selects edges with weights 1+2+3=6
    final ResultSet rs = database.query("opencypher",
        "CALL algo.mst('w') YIELD source, target, weight, totalWeight RETURN source, target, weight, totalWeight");

    assertThat(rs.hasNext()).isTrue();
    final Result first = rs.next();
    final double totalWeight = ((Number) first.getProperty("totalWeight")).doubleValue();
    assertThat(totalWeight).isEqualTo(6.0);
  }
}
