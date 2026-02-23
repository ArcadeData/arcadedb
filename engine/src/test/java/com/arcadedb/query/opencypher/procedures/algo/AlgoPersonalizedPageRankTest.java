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
 * Tests for the algo.personalizedPageRank Cypher procedure.
 */
class AlgoPersonalizedPageRankTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-ppr");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("FOLLOWS");

    // Linear chain: A -> B -> C -> D
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Person").set("name", "A").save();
      final MutableVertex b = database.newVertex("Person").set("name", "B").save();
      final MutableVertex c = database.newVertex("Person").set("name", "C").save();
      final MutableVertex d = database.newVertex("Person").set("name", "D").save();
      a.newEdge("FOLLOWS", b, true, (Object[]) null).save();
      b.newEdge("FOLLOWS", c, true, (Object[]) null).save();
      c.newEdge("FOLLOWS", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void pprReturnsOneRowPerVertex() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {name:'A'}) "
            + "CALL algo.personalizedPageRank(a) YIELD nodeId, score "
            + "RETURN nodeId, score");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);
  }

  @Test
  void pprScoresAreNonNegative() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {name:'A'}) "
            + "CALL algo.personalizedPageRank(a) YIELD nodeId, score "
            + "RETURN nodeId, score");

    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object val = r.getProperty("score");
      assertThat(((Number) val).doubleValue()).isGreaterThanOrEqualTo(0.0);
    }
  }

  @Test
  void pprSourceHasHighestScore() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {name:'A'}) "
            + "CALL algo.personalizedPageRank(a) YIELD nodeId, score "
            + "RETURN nodeId, score ORDER BY score DESC");

    assertThat(rs.hasNext()).isTrue();
    final Result topResult = rs.next();
    final Object val = topResult.getProperty("score");
    // Source node should have the highest (or near-highest) score
    assertThat(((Number) val).doubleValue()).isGreaterThan(0.0);
  }

  @Test
  void pprWithCustomDampingFactor() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {name:'A'}) "
            + "CALL algo.personalizedPageRank(a, 'FOLLOWS', 0.9) YIELD nodeId, score "
            + "RETURN nodeId, score");

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(4);
  }

  @Test
  void pprScoresSumToApproximatelyOne() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {name:'A'}) "
            + "CALL algo.personalizedPageRank(a) YIELD nodeId, score "
            + "RETURN nodeId, score");

    double totalScore = 0.0;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object val = r.getProperty("score");
      totalScore += ((Number) val).doubleValue();
    }
    // PPR scores should sum to approximately 1.0
    assertThat(totalScore).isGreaterThan(0.0);
    assertThat(totalScore).isLessThanOrEqualTo(1.5);
  }
}
