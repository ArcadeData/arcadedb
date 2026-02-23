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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.closeness Cypher procedure (Closeness Centrality).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoClosenessCentralityTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-closeness");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Linear chain A-B-C-D with OUT edges
    // Using BOTH direction in queries so all nodes can reach all others
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      c.newEdge("LINK", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void closenessMiddleNodesRankHigher() {
    // In a linear chain A-B-C-D with BOTH direction, B and C are closer to all nodes than A and D
    final ResultSet rs = database.query("opencypher",
        "CALL algo.closeness(null, 'BOTH', true) YIELD node, score RETURN node, score");

    double scoreA = 0, scoreB = 0, scoreC = 0, scoreD = 0;
    int count = 0;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object nodeObj = r.getProperty("node");
      final double score = ((Number) r.getProperty("score")).doubleValue();
      if (nodeObj instanceof Vertex v) {
        final String name = v.getString("name");
        switch (name) {
          case "A" -> scoreA = score;
          case "B" -> scoreB = score;
          case "C" -> scoreC = score;
          case "D" -> scoreD = score;
        }
      }
      count++;
    }
    assertThat(count).isEqualTo(4);
    // Middle nodes B and C should have higher closeness than endpoints A and D
    assertThat(scoreB).isGreaterThan(scoreA);
    assertThat(scoreC).isGreaterThan(scoreD);
  }

  @Test
  void closenessScoresBetweenZeroAndOne() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.closeness(null, 'BOTH', true) YIELD node, score RETURN score");
    int count = 0;
    while (rs.hasNext()) {
      final double score = ((Number) rs.next().getProperty("score")).doubleValue();
      assertThat(score).isBetween(0.0, 1.0);
      count++;
    }
    assertThat(count).isEqualTo(4);
  }

  @Test
  void closenessAllFourNodes() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.closeness() YIELD node, score RETURN node, score");
    int count = 0;
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object node = r.getProperty("node");
      final Object score = r.getProperty("score");
      assertThat(node).isNotNull();
      assertThat(score).isNotNull();
      count++;
    }
    assertThat(count).isEqualTo(4);
  }

  @Test
  void closenessAllPositiveScores() {
    // With BOTH direction the chain is fully connected, so all scores should be > 0
    final ResultSet rs = database.query("opencypher",
        "CALL algo.closeness(null, 'BOTH', false) YIELD node, score RETURN score");
    final List<Double> scores = new ArrayList<>();
    while (rs.hasNext())
      scores.add(((Number) rs.next().getProperty("score")).doubleValue());
    assertThat(scores).hasSize(4);
    for (final double s : scores)
      assertThat(s).isGreaterThan(0.0);
  }
}
