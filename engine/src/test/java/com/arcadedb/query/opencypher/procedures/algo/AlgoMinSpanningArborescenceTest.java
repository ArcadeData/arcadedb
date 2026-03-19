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
 * Tests for the algo.msa Cypher procedure (Minimum Spanning Arborescence).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoMinSpanningArborescenceTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-msa");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("ROAD");

    // Directed graph: root=A, edges with weights
    // A→B (1), A→C (4), B→C (2), C→B (3)
    // MSA from A: A→B (1), B→C (2) — total 3
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("City").set("name", "A").save();
      final MutableVertex b = database.newVertex("City").set("name", "B").save();
      final MutableVertex c = database.newVertex("City").set("name", "C").save();
      a.newEdge("ROAD", b, true, new Object[]{ "dist", 1 }).save();
      a.newEdge("ROAD", c, true, new Object[]{ "dist", 4 }).save();
      b.newEdge("ROAD", c, true, new Object[]{ "dist", 2 }).save();
      c.newEdge("ROAD", b, true, new Object[]{ "dist", 3 }).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void msaReturnsNMinusOneEdges() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (r:City {name: 'A'}) CALL algo.msa(r, 'ROAD', 'dist') " +
            "YIELD source, target, weight, totalWeight RETURN source, target, weight, totalWeight");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    // 3 nodes → 2 MSA edges
    assertThat(results).hasSize(2);
  }

  @Test
  void msaTotalWeightIsCorrect() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (r:City {name: 'A'}) CALL algo.msa(r, 'ROAD', 'dist') " +
            "YIELD totalWeight RETURN totalWeight");

    assertThat(rs.hasNext()).isTrue();
    final double total = ((Number) rs.next().getProperty("totalWeight")).doubleValue();
    // Optimal arborescence: A→B (1) + B→C (2) = 3
    assertThat(total).isEqualTo(3.0);
  }

  @Test
  void msaEdgeWeightsArePositive() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (r:City {name: 'A'}) CALL algo.msa(r, 'ROAD', 'dist') " +
            "YIELD weight RETURN weight");

    while (rs.hasNext()) {
      final double w = ((Number) rs.next().getProperty("weight")).doubleValue();
      assertThat(w).isGreaterThan(0.0);
    }
  }
}
