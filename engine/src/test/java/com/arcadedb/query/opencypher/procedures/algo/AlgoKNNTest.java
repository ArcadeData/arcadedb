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
 * Tests for the algo.knn Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoKNNTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-knn");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    // Build graph with two dense groups sharing some members
    // Group 1: A, B, C all know each other
    // Group 2: D, E, F all know each other
    // Bridge: C knows D
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Person").set("name", "A").save();
      final MutableVertex b = database.newVertex("Person").set("name", "B").save();
      final MutableVertex c = database.newVertex("Person").set("name", "C").save();
      final MutableVertex d = database.newVertex("Person").set("name", "D").save();
      final MutableVertex e = database.newVertex("Person").set("name", "E").save();
      final MutableVertex f = database.newVertex("Person").set("name", "F").save();
      a.newEdge("KNOWS", b, true, (Object[]) null).save();
      a.newEdge("KNOWS", c, true, (Object[]) null).save();
      b.newEdge("KNOWS", c, true, (Object[]) null).save();
      d.newEdge("KNOWS", e, true, (Object[]) null).save();
      d.newEdge("KNOWS", f, true, (Object[]) null).save();
      e.newEdge("KNOWS", f, true, (Object[]) null).save();
      c.newEdge("KNOWS", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void knnReturnsPairs() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.knn(3, 'KNOWS', 'BOTH') YIELD node1, node2, similarity RETURN node1, node2, similarity");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).isNotEmpty();
    for (final Result r : results) {
      assertThat((Object) r.getProperty("node1")).isNotNull();
      assertThat((Object) r.getProperty("node2")).isNotNull();
      final double sim = ((Number) r.getProperty("similarity")).doubleValue();
      assertThat(sim).isBetween(0.0, 1.0);
    }
  }

  @Test
  void knnSimilarityIsPositive() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.knn(10) YIELD node1, node2, similarity RETURN similarity");

    while (rs.hasNext()) {
      final double sim = ((Number) rs.next().getProperty("similarity")).doubleValue();
      assertThat(sim).isGreaterThan(0.0);
    }
  }
}
