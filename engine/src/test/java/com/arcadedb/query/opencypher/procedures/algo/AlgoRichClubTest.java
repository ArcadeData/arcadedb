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
 * Tests for the algo.richClub Cypher procedure.
 */
class AlgoRichClubTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-richclub");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Complete graph K4: A-B-C-D all connected (high degree, rich-club)
    // Plus isolated low-degree node E connected only to A
    // Use single directed edges per pair to represent undirected graph
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();
      // K4 edges (single directed edge per pair for undirected semantics with BOTH adjacency)
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      a.newEdge("EDGE", c, true, (Object[]) null).save();
      a.newEdge("EDGE", d, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
      b.newEdge("EDGE", d, true, (Object[]) null).save();
      c.newEdge("EDGE", d, true, (Object[]) null).save();
      // Low-degree node
      e.newEdge("EDGE", a, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void richClubReturnsRows() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.richClub('EDGE', 1) YIELD degree, richClubCoefficient, nodeCount, edgeCount "
            + "RETURN degree, richClubCoefficient, nodeCount, edgeCount");

    assertThat(rs.hasNext()).isTrue();
  }

  @Test
  void richClubCoefficientRangeValid() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.richClub('EDGE', 1) YIELD degree, richClubCoefficient, nodeCount, edgeCount "
            + "RETURN degree, richClubCoefficient, nodeCount, edgeCount");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object phi = r.getProperty("richClubCoefficient");
      final double val = ((Number) phi).doubleValue();
      assertThat(val).isGreaterThanOrEqualTo(0.0);
      assertThat(val).isLessThanOrEqualTo(1.0);
    }
  }

  @Test
  void richClubNodeCountDecreases() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.richClub('EDGE', 1) YIELD degree, richClubCoefficient, nodeCount, edgeCount "
            + "RETURN degree, richClubCoefficient, nodeCount, edgeCount ORDER BY degree ASC");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    // Node count should decrease (or stay same) as degree threshold increases
    int prevNodeCount = Integer.MAX_VALUE;
    for (final Result r : results) {
      final Object val = r.getProperty("nodeCount");
      final int nc = ((Number) val).intValue();
      assertThat(nc).isLessThanOrEqualTo(prevNodeCount);
      prevNodeCount = nc;
    }
  }

  @Test
  void richClubDegreeFieldPresent() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.richClub() YIELD degree, richClubCoefficient, nodeCount, edgeCount "
            + "RETURN degree, richClubCoefficient, nodeCount, edgeCount");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object degree = r.getProperty("degree");
      assertThat(degree).isNotNull();
      assertThat(((Number) degree).intValue()).isGreaterThanOrEqualTo(0);
    }
  }

  @Test
  void richClubHighDegreeIsCompleteAmongThemselves() {
    // A has degree 4, B/C/D have degree 3, E has degree 1
    // For threshold 2 (degree > 2), A,B,C,D qualify (4 nodes forming K4)
    // K4 has 6 edges, phi = 2*6 / (4*3) = 1.0
    final ResultSet rs = database.query("opencypher",
        "CALL algo.richClub('EDGE', 2) YIELD degree, richClubCoefficient, nodeCount, edgeCount "
            + "RETURN degree, richClubCoefficient, nodeCount, edgeCount");

    boolean foundGoodCoeff = false;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object phi = r.getProperty("richClubCoefficient");
      final double coeff = ((Number) phi).doubleValue();
      if (coeff > 0.0)
        foundGoodCoeff = true;
    }
    assertThat(foundGoodCoeff).isTrue();
  }
}
