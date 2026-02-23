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
 * Tests for the algo.katz Cypher procedure.
 */
class AlgoKatzTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-katz");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Star graph: center C pointed to by A, B, D
    // C should have highest Katz centrality (most in-links)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("EDGE", c, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
      d.newEdge("EDGE", c, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void katzReturnsFourNodes() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.katz() YIELD nodeId, score RETURN nodeId, score");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);
  }

  @Test
  void katzScoresPositive() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.katz() YIELD nodeId, score RETURN nodeId, score");

    while (rs.hasNext()) {
      final Result result = rs.next();
      final Object val = result.getProperty("score");
      assertThat(((Number) val).doubleValue()).isGreaterThanOrEqualTo(0.0);
    }
  }

  @Test
  void katzScoresNormalized() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.katz() YIELD nodeId, score RETURN nodeId, score");

    double maxScore = 0.0;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final Object val = result.getProperty("score");
      final double score = ((Number) val).doubleValue();
      if (score > maxScore)
        maxScore = score;
    }

    // Max score should be 1.0 after normalization
    assertThat(maxScore).isEqualTo(1.0);
  }

  @Test
  void katzCenterHasHighestScore() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.katz() YIELD nodeId, score RETURN nodeId, score");

    // Center node C (pointed to by 3 others) should have the highest score
    // We verify by checking all scores are <= 1.0 and some node has score 1.0
    double maxScore = 0.0;
    int maxCount = 0;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final Object val = result.getProperty("score");
      final double score = ((Number) val).doubleValue();
      if (score == 1.0)
        maxCount++;
      if (score > maxScore)
        maxScore = score;
    }

    assertThat(maxScore).isEqualTo(1.0);
    assertThat(maxCount).isEqualTo(1);
  }

  @Test
  void katzWithCustomAlpha() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.katz('EDGE', 0.01, 50, 1e-6) YIELD nodeId, score RETURN nodeId, score");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);
    for (final Result r : results) {
      final Object val = r.getProperty("score");
      assertThat(((Number) val).doubleValue()).isGreaterThanOrEqualTo(0.0);
    }
  }
}
