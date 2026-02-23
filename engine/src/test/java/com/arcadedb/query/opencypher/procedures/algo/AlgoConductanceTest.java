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
 * Tests for the algo.conductance Cypher procedure.
 */
class AlgoConductanceTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-conductance");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Two communities: {A,B} community=0 and {C,D} community=1
    // Dense within, one cross-community edge
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").set("comm", 0).save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").set("comm", 0).save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").set("comm", 1).save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").set("comm", 1).save();
      // Within community 0
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      b.newEdge("EDGE", a, true, (Object[]) null).save();
      // Within community 1
      c.newEdge("EDGE", d, true, (Object[]) null).save();
      d.newEdge("EDGE", c, true, (Object[]) null).save();
      // Cross-community edge
      a.newEdge("EDGE", c, true, (Object[]) null).save();
      c.newEdge("EDGE", a, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void conductanceReturnsOneRowPerCommunity() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.conductance('comm') YIELD community, conductance, internalEdges, boundaryEdges, nodeCount "
            + "RETURN community, conductance, internalEdges, boundaryEdges, nodeCount");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    // Two distinct community values (0 and 1)
    assertThat(results).hasSize(2);
  }

  @Test
  void conductanceCommunityValuePresent() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.conductance('comm') YIELD community, conductance "
            + "RETURN community, conductance");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object community = r.getProperty("community");
      final Object conductance = r.getProperty("conductance");
      assertThat(community).isNotNull();
      assertThat(conductance).isNotNull();
    }
  }

  @Test
  void conductanceValueInRange() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.conductance('comm') YIELD conductance RETURN conductance");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object val = r.getProperty("conductance");
      final double c = ((Number) val).doubleValue();
      // Conductance is in [0, 1]
      assertThat(c).isGreaterThanOrEqualTo(0.0);
      assertThat(c).isLessThanOrEqualTo(1.0);
    }
  }

  @Test
  void conductanceInternalEdgesPositive() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.conductance('comm') YIELD community, internalEdges RETURN community, internalEdges");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object val = r.getProperty("internalEdges");
      assertThat(((Number) val).longValue()).isGreaterThanOrEqualTo(0L);
    }
  }

  @Test
  void conductanceNodeCountsCorrect() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.conductance('comm') YIELD nodeCount RETURN nodeCount");

    long totalNodes = 0;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object val = r.getProperty("nodeCount");
      totalNodes += ((Number) val).longValue();
    }
    // Total nodes across all communities should be 4
    assertThat(totalNodes).isEqualTo(4L);
  }

  @Test
  void conductanceWithRelType() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.conductance('comm', 'EDGE') YIELD community, conductance "
            + "RETURN community, conductance");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
  }

  @Test
  void conductanceWellSeparatedCommunityHasLowConductance() {
    // Perfect separation: two communities with no cross edges
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-conductance-sep");
    if (factory.exists())
      factory.open().drop();
    final Database separatedDb = factory.create();
    separatedDb.getSchema().createVertexType("Node");
    separatedDb.getSchema().createEdgeType("EDGE");

    separatedDb.transaction(() -> {
      final MutableVertex a = separatedDb.newVertex("Node").set("name", "A").set("grp", "X").save();
      final MutableVertex b = separatedDb.newVertex("Node").set("name", "B").set("grp", "X").save();
      final MutableVertex c = separatedDb.newVertex("Node").set("name", "C").set("grp", "Y").save();
      final MutableVertex d = separatedDb.newVertex("Node").set("name", "D").set("grp", "Y").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      b.newEdge("EDGE", a, true, (Object[]) null).save();
      c.newEdge("EDGE", d, true, (Object[]) null).save();
      d.newEdge("EDGE", c, true, (Object[]) null).save();
    });

    final ResultSet rs = separatedDb.query("opencypher",
        "CALL algo.conductance('grp') YIELD community, conductance, boundaryEdges "
            + "RETURN community, conductance, boundaryEdges");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object boundary = r.getProperty("boundaryEdges");
      final Object cond = r.getProperty("conductance");
      // No cross edges means conductance = 0
      assertThat(((Number) boundary).longValue()).isEqualTo(0L);
      assertThat(((Number) cond).doubleValue()).isEqualTo(0.0);
    }

    separatedDb.drop();
  }
}
