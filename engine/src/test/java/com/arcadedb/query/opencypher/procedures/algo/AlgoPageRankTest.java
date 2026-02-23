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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the algo.pagerank Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoPageRankTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-pagerank");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Page");
    database.getSchema().createEdgeType("LINKS");

    // Create a small web graph:
    // A -> B, A -> C, B -> C, C -> A
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Page").set("name", "A").save();
      final MutableVertex b = database.newVertex("Page").set("name", "B").save();
      final MutableVertex c = database.newVertex("Page").set("name", "C").save();
      a.newEdge("LINKS", b, true, (Object[]) null).save();
      a.newEdge("LINKS", c, true, (Object[]) null).save();
      b.newEdge("LINKS", c, true, (Object[]) null).save();
      c.newEdge("LINKS", a, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void pageRankReturnsScoreForEachNode() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank() YIELD node, score RETURN node, score ORDER BY score DESC");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3);
    for (final Result result : results) {
      final Object node = result.getProperty("node");
      assertThat(node).isNotNull();
      final double score = ((Number) result.getProperty("score")).doubleValue();
      assertThat(score).isGreaterThan(0.0);
    }
  }

  @Test
  void pageRankScoresSumToApproximatelyOne() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank() YIELD node, score RETURN score");

    double sum = 0;
    while (rs.hasNext()) {
      final Result result = rs.next();
      sum += ((Number) result.getProperty("score")).doubleValue();
    }
    // PageRank scores should sum to approximately 1.0
    assertThat(sum).isBetween(0.9, 1.1);
  }

  @Test
  void pageRankWithCustomDampingFactor() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank({dampingFactor: 0.5}) YIELD node, score RETURN node, score");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3);
  }

  @Test
  void pageRankHigherScoreForHigherInDegree() {
    // Node C has 2 incoming edges (from A and B), so it should have higher rank
    final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank() YIELD node, score RETURN node.name AS name, score ORDER BY score DESC");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).isNotEmpty();
    // C has most incoming links (from A and B), should have higher score
    final String topNode = (String) results.getFirst().getProperty("name");
    assertThat(topNode).isEqualTo("C");
  }

  @Test
  void pageRankEmptyGraph() {
    final DatabaseFactory emptyFactory = new DatabaseFactory("./target/databases/test-algo-pagerank-empty");
    if (emptyFactory.exists())
      emptyFactory.open().drop();
    final Database emptyDb = emptyFactory.create();
    try {
      emptyDb.getSchema().createVertexType("Node");
      final ResultSet rs = emptyDb.query("opencypher", "CALL algo.pagerank() YIELD node, score RETURN node, score");
      // Empty graph returns no results
      assertThat(rs.hasNext()).isFalse();
    } finally {
      emptyDb.drop();
    }
  }
}
