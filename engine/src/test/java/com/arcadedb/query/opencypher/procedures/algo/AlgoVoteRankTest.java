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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.voteRank Cypher procedure.
 */
class AlgoVoteRankTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-voterank");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // A points to B, C, D — A is the most influential hub
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      a.newEdge("EDGE", c, true, (Object[]) null).save();
      a.newEdge("EDGE", d, true, (Object[]) null).save();
      b.newEdge("EDGE", e, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void voteRankReturnsResults() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.voteRank() YIELD nodeId, rank RETURN nodeId, rank ORDER BY rank ASC");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).isNotEmpty();
  }

  @Test
  void voteRankRanksStartAtOne() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.voteRank() YIELD nodeId, rank RETURN nodeId, rank ORDER BY rank ASC");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    if (!results.isEmpty()) {
      final Object val = results.get(0).getProperty("rank");
      assertThat(((Number) val).intValue()).isEqualTo(1);
    }
  }

  @Test
  void voteRankTopKLimitsResults() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.voteRank('EDGE', 2) YIELD nodeId, rank RETURN nodeId, rank");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results.size()).isLessThanOrEqualTo(2);
  }

  @Test
  void voteRankRanksAreUnique() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.voteRank() YIELD nodeId, rank RETURN nodeId, rank");

    final List<Integer> ranks = new ArrayList<>();
    while (rs.hasNext()) {
      final Result result = rs.next();
      final Object val = result.getProperty("rank");
      final int rank = ((Number) val).intValue();
      assertThat(ranks).doesNotContain(rank);
      ranks.add(rank);
    }
  }

  /**
   * Issue #12170: an election now updates only the scores within two hops of the elected node. H (5 votes) goes first
   * and halves the voting ability of x and y; z then has 3 + 0.5 votes and x has 3 (it lost H's), so z, which tied x
   * at 4 before, is elected before x. Only the scores the two-hop update reaches decide that order.
   */
  @Test
  void electionUpdatesTheScoresTwoHopsAway() {
    database.getSchema().createEdgeType("VOTE");
    database.transaction(() -> {
      final Map<String, Vertex> v = new HashMap<>();
      for (final String name : List.of("H", "x", "y", "z", "a1", "a2", "a3", "a4", "a5", "b1", "b2", "b3", "b5", "c1", "c2",
          "c3"))
        v.put(name, database.newVertex("Node").set("name", name).save());
      final String[][] edges = { { "a1", "H" }, { "a2", "H" }, { "a3", "H" }, { "a4", "H" }, { "a5", "H" }, { "H", "x" },
          { "H", "y" }, { "b1", "x" }, { "b2", "x" }, { "b5", "x" }, { "b3", "y" }, { "x", "z" }, { "c1", "z" }, { "c2", "z" },
          { "c3", "z" } };
      for (final String[] edge : edges)
        v.get(edge[0]).newEdge("VOTE", v.get(edge[1]), (Object[]) null).save();
    });

    final ResultSet rs = database.query("opencypher",
        "CALL algo.voteRank('VOTE') YIELD nodeId, rank RETURN nodeId.name AS name ORDER BY rank");
    final List<String> order = new ArrayList<>();
    while (rs.hasNext())
      order.add(rs.next().getProperty("name"));

    assertThat(order).containsExactly("H", "z", "x", "y");
  }
}
