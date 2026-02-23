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
 * Tests for the algo.kShortestPaths Cypher procedure.
 */
class AlgoKShortestPathsTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-kshortest");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("ROAD");

    // Graph with multiple paths from A to D:
    // A -> B -> D (cost 3)
    // A -> C -> D (cost 5)
    // A -> B -> C -> D (cost 6)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("City").set("name", "A").save();
      final MutableVertex b = database.newVertex("City").set("name", "B").save();
      final MutableVertex c = database.newVertex("City").set("name", "C").save();
      final MutableVertex d = database.newVertex("City").set("name", "D").save();
      a.newEdge("ROAD", b, true, new Object[] { "dist", 1.0 }).save();
      b.newEdge("ROAD", d, true, new Object[] { "dist", 2.0 }).save();
      a.newEdge("ROAD", c, true, new Object[] { "dist", 2.0 }).save();
      c.newEdge("ROAD", d, true, new Object[] { "dist", 3.0 }).save();
      b.newEdge("ROAD", c, true, new Object[] { "dist", 2.0 }).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void kShortestPathsReturnsKPaths() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:City {name:'A'}), (d:City {name:'D'}) "
            + "CALL algo.kShortestPaths(a, d, 2, 'ROAD', 'dist') YIELD path, weight, rank "
            + "RETURN path, weight, rank ORDER BY rank ASC");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results.size()).isGreaterThanOrEqualTo(1);
    assertThat(results.size()).isLessThanOrEqualTo(2);
  }

  @Test
  void kShortestPathsRankedInOrder() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:City {name:'A'}), (d:City {name:'D'}) "
            + "CALL algo.kShortestPaths(a, d, 3, 'ROAD', 'dist') YIELD path, weight, rank "
            + "RETURN path, weight, rank ORDER BY rank ASC");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    double prevWeight = -1.0;
    for (final Result r : results) {
      final Object val = r.getProperty("weight");
      final double weight = ((Number) val).doubleValue();
      assertThat(weight).isGreaterThanOrEqualTo(prevWeight);
      prevWeight = weight;
    }
  }

  @Test
  void kShortestPathsFirstPathIsOptimal() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:City {name:'A'}), (d:City {name:'D'}) "
            + "CALL algo.kShortestPaths(a, d, 1, 'ROAD', 'dist') YIELD weight, rank "
            + "RETURN weight, rank");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("weight");
    // Shortest path A->B->D = 1+2 = 3
    assertThat(((Number) val).doubleValue()).isEqualTo(3.0);
  }

  @Test
  void kShortestPathsRankStartsAtOne() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:City {name:'A'}), (d:City {name:'D'}) "
            + "CALL algo.kShortestPaths(a, d, 2, 'ROAD', 'dist') YIELD weight, rank "
            + "RETURN weight, rank ORDER BY rank ASC");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("rank");
    assertThat(((Number) val).intValue()).isEqualTo(1);
  }
}
