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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
  void pageRankCSRAndOLTPProduceIdenticalResults() {
    // Step 1: Run PageRank without GAV → OLTP path
    final Map<String, Double> oltpScores = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 20, tolerance: 0.0001}) " +
            "YIELD node, score RETURN node.name AS name, score")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        oltpScores.put((String) r.getProperty("name"), ((Number) r.getProperty("score")).doubleValue());
      }
    }
    assertThat(oltpScores).hasSize(3);

    // Step 2: Build a GAV so the CSR path is used
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("pagerank-csr")
        .withVertexTypes("Page")
        .withEdgeTypes("LINKS")
        .build();
    gav.awaitReady(10, TimeUnit.SECONDS);

    // Step 3: Run PageRank with GAV → CSR path
    final Map<String, Double> csrScores = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 20, tolerance: 0.0001}) " +
            "YIELD node, score RETURN node.name AS name, score")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        csrScores.put((String) r.getProperty("name"), ((Number) r.getProperty("score")).doubleValue());
      }
    }
    assertThat(csrScores).hasSize(3);

    // Step 4: Compare — CSR and OLTP paths accumulate floating-point differences
    // due to different iteration order and convergence behavior, so use 1e-4 tolerance
    for (final Map.Entry<String, Double> entry : oltpScores.entrySet()) {
      final String name = entry.getKey();
      assertThat(csrScores).containsKey(name);
      assertThat(csrScores.get(name)).isCloseTo(entry.getValue(), Offset.offset(1e-4));
    }

    gav.shutdown();
  }

  @Test
  void pageRankBothDirectionCSRAndOLTPProduceIdenticalResults() {
    // Step 1: Run PageRank with direction=BOTH without GAV → OLTP path
    final Map<String, Double> oltpScores = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 20, tolerance: 0.0, direction: 'BOTH'}) " +
            "YIELD node, score RETURN node.name AS name, score")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        oltpScores.put((String) r.getProperty("name"), ((Number) r.getProperty("score")).doubleValue());
      }
    }
    assertThat(oltpScores).hasSize(3);

    // Step 2: Build a GAV so the CSR path is used
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("pagerank-both-csr")
        .withVertexTypes("Page")
        .withEdgeTypes("LINKS")
        .build();
    gav.awaitReady(10, TimeUnit.SECONDS);

    // Step 3: Run PageRank with direction=BOTH with GAV → CSR path
    final Map<String, Double> csrScores = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank({dampingFactor: 0.85, maxIterations: 20, tolerance: 0.0, direction: 'BOTH'}) " +
            "YIELD node, score RETURN node.name AS name, score")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        csrScores.put((String) r.getProperty("name"), ((Number) r.getProperty("score")).doubleValue());
      }
    }
    assertThat(csrScores).hasSize(3);

    // Step 4: Compare — CSR and OLTP paths should match closely
    for (final Map.Entry<String, Double> entry : oltpScores.entrySet()) {
      final String name = entry.getKey();
      assertThat(csrScores).containsKey(name);
      assertThat(csrScores.get(name)).isCloseTo(entry.getValue(), Offset.offset(1e-4));
    }

    // Step 5: For undirected graph, scores should be more evenly distributed than directed
    double sum = 0;
    for (final double s : csrScores.values())
      sum += s;
    assertThat(sum).isBetween(0.9, 1.1);

    gav.shutdown();
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
