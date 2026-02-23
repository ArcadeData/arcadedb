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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.graphSummary Cypher procedure.
 */
class AlgoGraphSummaryTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-graphsummary");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // 4 nodes: A->B, A->C, B->C, D is isolated
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      database.newVertex("Node").set("name", "D").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      a.newEdge("EDGE", c, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void graphSummaryReturnsSingleRow() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.graphSummary() YIELD nodeCount, edgeCount, avgDegree, maxDegree, minDegree, density, isolatedNodes, selfLoops "
            + "RETURN nodeCount, edgeCount, avgDegree, maxDegree, minDegree, density, isolatedNodes, selfLoops");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(rs.hasNext()).isFalse();
    final Object nodeCount = result.getProperty("nodeCount");
    assertThat(nodeCount).isNotNull();
  }

  @Test
  void graphSummaryNodeCount() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.graphSummary() YIELD nodeCount RETURN nodeCount");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("nodeCount");
    assertThat(((Number) val).longValue()).isEqualTo(4L);
  }

  @Test
  void graphSummaryEdgeCount() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.graphSummary() YIELD edgeCount RETURN edgeCount");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("edgeCount");
    assertThat(((Number) val).longValue()).isEqualTo(3L);
  }

  @Test
  void graphSummaryIsolatedNodes() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.graphSummary() YIELD isolatedNodes RETURN isolatedNodes");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("isolatedNodes");
    // D has no OUT edges, C has no OUT edges, B has 1 OUT edge, A has 2 OUT edges
    // So isolatedNodes (out-degree == 0) = D and C = 2
    assertThat(((Number) val).longValue()).isEqualTo(2L);
  }

  @Test
  void graphSummaryMaxDegree() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.graphSummary() YIELD maxDegree RETURN maxDegree");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("maxDegree");
    // A has out-degree 2 (max)
    assertThat(((Number) val).longValue()).isEqualTo(2L);
  }

  @Test
  void graphSummaryDensityPositive() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.graphSummary() YIELD density RETURN density");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("density");
    assertThat(((Number) val).doubleValue()).isGreaterThan(0.0);
    assertThat(((Number) val).doubleValue()).isLessThanOrEqualTo(1.0);
  }

  @Test
  void graphSummaryNoSelfLoops() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.graphSummary() YIELD selfLoops RETURN selfLoops");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final Object val = result.getProperty("selfLoops");
    assertThat(((Number) val).longValue()).isEqualTo(0L);
  }
}
