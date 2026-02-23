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
 * Tests for the algo.betweenness Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoBetweennessTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-betweenness");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Linear graph: A -> B -> C -> D
    // B and C are bridges, so they have higher betweenness
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
      c.newEdge("EDGE", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void betweennessReturnsScoreForEachNode() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.betweenness() YIELD node, score RETURN node, score");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);
    for (final Result result : results) {
      final Object node = result.getProperty("node");
      assertThat(node).isNotNull();
      final Object score = result.getProperty("score");
      assertThat(score).isNotNull();
    }
  }

  @Test
  void betweennessEndpointsHaveLowerScore() {
    // In A->B->C->D, nodes A and D (endpoints) should have betweenness = 0
    // while B and C have higher betweenness
    final ResultSet rs = database.query("opencypher",
        "CALL algo.betweenness() YIELD node, score RETURN node.name AS name, score ORDER BY score DESC");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);

    // Find A and D scores - they are endpoints and should have lower betweenness
    double scoreA = 0, scoreB = 0, scoreC = 0, scoreD = 0;
    for (final Result r : results) {
      final String name = (String) r.getProperty("name");
      final double score = ((Number) r.getProperty("score")).doubleValue();
      switch (name) {
        case "A" -> scoreA = score;
        case "B" -> scoreB = score;
        case "C" -> scoreC = score;
        case "D" -> scoreD = score;
      }
    }

    assertThat(scoreB).isGreaterThan(scoreA);
    assertThat(scoreC).isGreaterThan(scoreD);
  }

  @Test
  void betweennessNormalized() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.betweenness({normalized: true}) YIELD node, score RETURN score");

    while (rs.hasNext()) {
      final Result result = rs.next();
      final double score = ((Number) result.getProperty("score")).doubleValue();
      assertThat(score).isBetween(0.0, 1.0);
    }
  }

  @Test
  void betweennessNonNormalized() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.betweenness({normalized: false}) YIELD node, score RETURN score");

    final List<Double> scores = new ArrayList<>();
    while (rs.hasNext()) {
      final Result result = rs.next();
      scores.add(((Number) result.getProperty("score")).doubleValue());
    }

    assertThat(scores).hasSize(4);
    // At least one node should have score > 1 (not normalized)
    assertThat(scores.stream().anyMatch(s -> s > 1.0)).isTrue();
  }
}
