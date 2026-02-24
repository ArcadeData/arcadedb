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
 * Tests for the algo.articlerank Cypher procedure.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoArticleRankTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-articlerank");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Page");
    database.getSchema().createEdgeType("LINKS");

    // Same graph as PageRank test: A -> B, A -> C, B -> C, C -> A
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
  void articleRankReturnsScoreForEachNode() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.articlerank() YIELD node, score RETURN node, score ORDER BY score DESC");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3);
    for (final Result result : results) {
      assertThat((Object) result.getProperty("node")).isNotNull();
      assertThat(((Number) result.getProperty("score")).doubleValue()).isGreaterThan(0.0);
    }
  }

  @Test
  void articleRankScoresArePositive() {
    // ArticleRank does not guarantee scores summing to 1 (unlike PageRank).
    // Each score should simply be positive.
    final ResultSet rs = database.query("opencypher",
        "CALL algo.articlerank() YIELD node, score RETURN score");

    while (rs.hasNext()) {
      final double score = ((Number) rs.next().getProperty("score")).doubleValue();
      assertThat(score).isGreaterThan(0.0);
    }
  }

  @Test
  void articleRankWithCustomConfig() {
    final ResultSet rs = database.query("opencypher",
        "CALL algo.articlerank({dampingFactor: 0.5, maxIterations: 10}) YIELD node, score RETURN node, score");

    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3);
  }

  @Test
  void articleRankEmptyGraph() {
    final DatabaseFactory emptyFactory = new DatabaseFactory("./target/databases/test-algo-articlerank-empty");
    if (emptyFactory.exists())
      emptyFactory.open().drop();
    final Database emptyDb = emptyFactory.create();
    try {
      emptyDb.getSchema().createVertexType("Node");
      final ResultSet rs = emptyDb.query("opencypher", "CALL algo.articlerank() YIELD node, score RETURN node, score");
      assertThat(rs.hasNext()).isFalse();
    } finally {
      emptyDb.drop();
    }
  }
}
