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
 * Tests for the algo.localClusteringCoefficient Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoLocalClusteringCoefficientTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-lcc");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("KNOWS");

    // Triangle: A-B, B-C, A-C → all nodes have LCC = 1.0
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("KNOWS", b, true, (Object[]) null).save();
      b.newEdge("KNOWS", c, true, (Object[]) null).save();
      a.newEdge("KNOWS", c, true, (Object[]) null).save();
      // D is connected only to A (no triangle) → LCC = 0
      a.newEdge("KNOWS", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void lccReturnsEntryForEachNode() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.localClusteringCoefficient() YIELD node, localClusteringCoefficient RETURN node, localClusteringCoefficient");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);
  }

  @Test
  void lccValuesAreBetweenZeroAndOne() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.localClusteringCoefficient() YIELD node, localClusteringCoefficient RETURN localClusteringCoefficient");

    while (rs.hasNext()) {
      final double coeff = ((Number) rs.next().getProperty("localClusteringCoefficient")).doubleValue();
      assertThat(coeff).isBetween(0.0, 1.0);
    }
  }

  @Test
  void lccForTriangleIsOne() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.localClusteringCoefficient() YIELD node, localClusteringCoefficient " +
            "RETURN node.name AS name, localClusteringCoefficient ORDER BY name");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    // A is in a triangle with B and C, but also has D as a non-triangle neighbour
    // B and C are in the triangle only → LCC should be 1.0 for B and C
    for (final Result r : results) {
      final String name = (String) r.getProperty("name");
      final double coeff = ((Number) r.getProperty("localClusteringCoefficient")).doubleValue();
      if ("B".equals(name) || "C".equals(name))
        assertThat(coeff).isEqualTo(1.0);
      else if ("D".equals(name))
        assertThat(coeff).isEqualTo(0.0);
    }
  }
}
