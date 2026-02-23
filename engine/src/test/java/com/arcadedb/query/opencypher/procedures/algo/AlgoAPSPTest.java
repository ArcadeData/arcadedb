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
 * Tests for the algo.apsp Cypher procedure.
 */
class AlgoAPSPTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-apsp");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Weighted graph: A->B w=1, B->C w=2, A->C w=10, C->D w=3
    // Shortest A->C: via B = 1+2=3 (not direct 10)
    // Shortest A->D: via B->C = 1+2+3=6
    // D has no outgoing edges
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save().set("w", 1).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save().set("w", 2).save();
      a.newEdge("EDGE", c, true, (Object[]) null).save().set("w", 10).save();
      c.newEdge("EDGE", d, true, (Object[]) null).save().set("w", 3).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void apspDirectEdge() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.apsp('w') YIELD source, target, distance RETURN source.name AS src, target.name AS tgt, distance");

    double distAB = -1;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String src = (String) result.getProperty("src");
      final String tgt = (String) result.getProperty("tgt");
      if ("A".equals(src) && "B".equals(tgt))
        distAB = ((Number) result.getProperty("distance")).doubleValue();
    }

    assertThat(distAB).isEqualTo(1.0);
  }

  @Test
  void apspShortestPathThroughIntermediary() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.apsp('w') YIELD source, target, distance RETURN source.name AS src, target.name AS tgt, distance");

    double distAC = -1;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String src = (String) result.getProperty("src");
      final String tgt = (String) result.getProperty("tgt");
      if ("A".equals(src) && "C".equals(tgt))
        distAC = ((Number) result.getProperty("distance")).doubleValue();
    }

    // Shortest A->C is via B: 1+2=3, not the direct edge with weight 10
    assertThat(distAC).isEqualTo(3.0);
  }

  @Test
  void apspTransitivePath() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.apsp('w') YIELD source, target, distance RETURN source.name AS src, target.name AS tgt, distance");

    double distAD = -1;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String src = (String) result.getProperty("src");
      final String tgt = (String) result.getProperty("tgt");
      if ("A".equals(src) && "D".equals(tgt))
        distAD = ((Number) result.getProperty("distance")).doubleValue();
    }

    // Shortest A->D: A->B->C->D = 1+2+3=6
    assertThat(distAD).isEqualTo(6.0);
  }

  @Test
  void apspReturnsOnlyReachablePairs() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.apsp('w') YIELD source, target, distance RETURN source.name AS src, target.name AS tgt, distance");

    boolean foundDtoA = false;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String src = (String) result.getProperty("src");
      final String tgt = (String) result.getProperty("tgt");
      if ("D".equals(src) && "A".equals(tgt))
        foundDtoA = true;
    }

    // D has no outgoing edges, so D->A should not appear
    assertThat(foundDtoA).isFalse();
  }
}
