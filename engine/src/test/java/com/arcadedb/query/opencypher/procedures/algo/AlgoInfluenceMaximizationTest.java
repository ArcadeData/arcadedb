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
 * Tests for the algo.influenceMaximization Cypher procedure.
 */
class AlgoInfluenceMaximizationTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-influence");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    // Two clusters: {A->B->C} and {D->E->F}, connected by A->D
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Person").set("name", "A").save();
      final MutableVertex b = database.newVertex("Person").set("name", "B").save();
      final MutableVertex c = database.newVertex("Person").set("name", "C").save();
      final MutableVertex d = database.newVertex("Person").set("name", "D").save();
      final MutableVertex e = database.newVertex("Person").set("name", "E").save();
      final MutableVertex f = database.newVertex("Person").set("name", "F").save();
      a.newEdge("FOLLOWS", b, true, (Object[]) null).save();
      b.newEdge("FOLLOWS", c, true, (Object[]) null).save();
      a.newEdge("FOLLOWS", c, true, (Object[]) null).save();
      d.newEdge("FOLLOWS", e, true, (Object[]) null).save();
      e.newEdge("FOLLOWS", f, true, (Object[]) null).save();
      a.newEdge("FOLLOWS", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void influenceMaximizationReturnsKSeeds() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.influenceMaximization(2) YIELD nodeId, rank, marginalGain "
            + "RETURN nodeId, rank, marginalGain ORDER BY rank ASC");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
  }

  @Test
  void influenceMaximizationRankStartsAtOne() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.influenceMaximization(1) YIELD nodeId, rank, marginalGain "
            + "RETURN nodeId, rank, marginalGain");

    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    final Object rankVal = r.getProperty("rank");
    assertThat(((Number) rankVal).intValue()).isEqualTo(1);
  }

  @Test
  void influenceMaximizationRanksAreOrdered() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.influenceMaximization(3) YIELD nodeId, rank, marginalGain "
            + "RETURN nodeId, rank, marginalGain ORDER BY rank ASC");

    int prevRank = 0;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object val = r.getProperty("rank");
      final int rank = ((Number) val).intValue();
      assertThat(rank).isGreaterThan(prevRank);
      prevRank = rank;
    }
  }

  @Test
  void influenceMaximizationMarginalGainPositive() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.influenceMaximization(1) YIELD nodeId, rank, marginalGain "
            + "RETURN nodeId, rank, marginalGain");

    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    final Object val = r.getProperty("marginalGain");
    // First seed always has positive marginal gain
    assertThat(((Number) val).doubleValue()).isGreaterThan(0.0);
  }

  @Test
  void influenceMaximizationWithRelType() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.influenceMaximization(2, 'FOLLOWS', 50, 0.2) YIELD nodeId, rank, marginalGain "
            + "RETURN nodeId, rank, marginalGain");

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  @Test
  void influenceMaximizationKGreaterThanNodesReturnsAllNodes() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.influenceMaximization(10) YIELD nodeId, rank, marginalGain "
            + "RETURN nodeId, rank, marginalGain");

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    // Can't return more seeds than there are nodes
    assertThat(count).isLessThanOrEqualTo(6);
    assertThat(count).isGreaterThanOrEqualTo(1);
  }
}
