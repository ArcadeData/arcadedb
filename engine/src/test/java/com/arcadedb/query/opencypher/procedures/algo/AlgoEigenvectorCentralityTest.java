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
 * Tests for the algo.eigenvector Cypher procedure.
 */
class AlgoEigenvectorCentralityTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-eigenvector");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");

    // Hub-and-spoke: A->B, A->C, A->D, B->C
    // A has the most connections (BOTH direction: connects to B, C, D = 3)
    // B connects to A and C (BOTH: 2)
    // C connects to A and B (BOTH: 2)
    // D connects only to A (BOTH: 1)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      a.newEdge("EDGE", c, true, (Object[]) null).save();
      a.newEdge("EDGE", d, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void eigenvectorReturnsFourNodes() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.eigenvector(null, 'BOTH') YIELD node, score RETURN node, score");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(4);
  }

  @Test
  void eigenvectorScoresBetweenZeroAndOne() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.eigenvector(null, 'BOTH') YIELD node, score RETURN node, score");

    while (rs.hasNext()) {
      final Result result = rs.next();
      final double score = ((Number) result.getProperty("score")).doubleValue();
      assertThat(score).isBetween(0.0, 1.0);
    }
  }

  @Test
  void eigenvectorHighlyConnectedNodeHigherScore() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.eigenvector(null, 'BOTH') YIELD node, score RETURN node.name AS name, score");

    double scoreA = 0, scoreB = 0, scoreC = 0, scoreD = 0;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final String name = (String) result.getProperty("name");
      final double score = ((Number) result.getProperty("score")).doubleValue();
      switch (name) {
        case "A" -> scoreA = score;
        case "B" -> scoreB = score;
        case "C" -> scoreC = score;
        case "D" -> scoreD = score;
      }
    }

    // A has the most connections, should have the highest eigenvector score
    assertThat(scoreA).isGreaterThan(scoreD);
    assertThat(scoreA).isGreaterThanOrEqualTo(scoreB);
    assertThat(scoreA).isGreaterThanOrEqualTo(scoreC);
  }
}
