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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6065 (follow-up to #5924 / PR #6055).
 * <p>
 * Part A: an embedding-dimension-shaped parameter has no graph-derived bound to clamp against the way
 * a top-k parameter does, so a large but perfectly in-range {@code int} survived the #5924 overflow
 * check and went straight into a per-node {@code double[]} allocation - {@code new double[1][1000000000]}
 * is ~8 GB on a one-node graph. These are now bounded by
 * {@link AbstractAlgoProcedure#MAX_EMBEDDING_DIMENSION} and rejected, not silently clamped: there is no
 * "correct" dimension to fall back to.
 * </p>
 * <p>
 * Part B: a literal negative {@code k} used to reach the array/collection allocation and surface as a
 * bare {@code NegativeArraySizeException} / {@code IllegalArgumentException: Illegal Capacity} that
 * never named the offending parameter.
 * </p>
 */
class Issue6065NumericParameterBoundsTest {
  /** Comfortably in-range for an int, comfortably out of range for a heap. */
  private static final long HUGE_DIMENSION = 1_000_000_000L;

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6065-numeric-bounds");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Tiny path graph A -> B -> C -> D, plus the shortcut A -> D so kShortestPaths has two routes.
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      c.newEdge("LINK", d, true, (Object[]) null).save();
      a.newEdge("LINK", d, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  // ── Part A: embedding-dimension-shaped parameters are bounded ────────────────

  @Test
  void node2vecRejectsAnEmbeddingDimensionAboveTheCap() {
    assertThatThrownBy(() -> drain(
        "CALL algo.node2vec({embeddingDimension: $dim, walkLength: 3, walksPerNode: 1, seed: 1}) YIELD node RETURN node",
        Map.of("dim", HUGE_DIMENSION)))
        .hasStackTraceContaining("embeddingDimension")
        .hasStackTraceContaining(String.valueOf(AbstractAlgoProcedure.MAX_EMBEDDING_DIMENSION));
  }

  @Test
  void fastRPRejectsADimensionsValueAboveTheCap() {
    assertThatThrownBy(() -> drain(
        "CALL algo.fastrp({dimensions: $dim, iterations: 1, seed: 1}) YIELD node RETURN node",
        Map.of("dim", HUGE_DIMENSION)))
        .hasStackTraceContaining("dimensions")
        .hasStackTraceContaining(String.valueOf(AbstractAlgoProcedure.MAX_EMBEDDING_DIMENSION));
  }

  @Test
  void graphSageRejectsAnEmbeddingDimensionAboveTheCap() {
    assertThatThrownBy(() -> drain(
        "CALL algo.graphsage({embeddingDimension: $dim, layers: 1, seed: 1}) YIELD node RETURN node",
        Map.of("dim", HUGE_DIMENSION)))
        .hasStackTraceContaining("embeddingDimension")
        .hasStackTraceContaining(String.valueOf(AbstractAlgoProcedure.MAX_EMBEDDING_DIMENSION));
  }

  @Test
  void hashGnnRejectsAnEmbeddingDimensionAboveTheCap() {
    assertThatThrownBy(() -> drain(
        "CALL algo.hashgnn({embeddingDimension: $dim, iterations: 1, seed: 1}) YIELD node RETURN node",
        Map.of("dim", HUGE_DIMENSION)))
        .hasStackTraceContaining("embeddingDimension")
        .hasStackTraceContaining(String.valueOf(AbstractAlgoProcedure.MAX_EMBEDDING_DIMENSION));
  }

  @Test
  void aZeroEmbeddingDimensionIsRejectedRatherThanProducingEmptyVectors() {
    assertThatThrownBy(() -> drain(
        "CALL algo.fastrp({dimensions: 0, iterations: 1, seed: 1}) YIELD node RETURN node", Map.of()))
        .hasStackTraceContaining("dimensions")
        .hasStackTraceContaining("at least 1");
  }

  @Test
  void anEmbeddingDimensionExactlyAtTheCapIsStillAccepted() {
    // The cap must not narrow what already worked: 4096 doubles per node is 32 KB per row.
    final List<Result> results = drain(
        "CALL algo.fastrp({dimensions: $dim, iterations: 1, seed: 1}) YIELD node, embedding RETURN node, embedding",
        Map.of("dim", AbstractAlgoProcedure.MAX_EMBEDDING_DIMENSION));

    assertThat(results).hasSize(4);
    for (final Result r : results)
      assertThat((List<?>) r.getProperty("embedding")).hasSize(AbstractAlgoProcedure.MAX_EMBEDDING_DIMENSION);
  }

  @Test
  void maxKCutClampsAHugeKAgainstTheGraphSizeInsteadOfAllocatingForIt() {
    // Raised in review of PR #6214: issue #6065 asserted algo.maxKCut's k "already clamps against n
    // downstream" - it did not. Only `k < 2` was rejected, and `new double[k]` is allocated once per
    // node per local-search pass per restart, so a huge k was the same allocation-DoS shape this
    // issue closes for embeddingDimension. A k-cut into more parts than there are nodes can only
    // leave the surplus parts empty, so clamping (rather than rejecting) is the honest reading here.
    final List<Result> results = drain(
        "CALL algo.maxKCut($k, {seed: 42, restarts: 1, maxIterations: 1}) YIELD node, community RETURN node, community",
        Map.of("k", 2_000_000_000));

    assertThat(results).hasSize(4);
    for (final Result r : results)
      assertThat(((Number) r.getProperty("community")).intValue()).isBetween(0, 3);
  }

  @Test
  void maxKCutStillHonoursASmallK() {
    // Guards the clamp against over-reach: k below the node count is untouched.
    final List<Result> results = drain(
        "CALL algo.maxKCut(2, {seed: 42}) YIELD node, community RETURN node, community", Map.of());

    assertThat(results).hasSize(4);
    for (final Result r : results)
      assertThat(((Number) r.getProperty("community")).intValue()).isBetween(0, 1);
  }

  @Test
  void maxKCutOnASingleNodeGraphStillHonoursTheMinimumOfTwoPartitions() {
    // Raised in cycle 2 of the PR #6214 review: the clamp added in cycle 1 must not undercut the
    // k >= 2 contract that algo.maxKCut validates a few lines above it. On a one-node graph a bare
    // Math.min(rawK, n) yields k = 1 - a "cut" into a single partition - so the clamp carries a
    // floor of 2. The allocation stays bounded either way: max(2, min(rawK, n)).
    // algo.maxKCut loads the whole graph, so the single-node case needs its own database.
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6065-single-node");
    if (factory.exists())
      factory.open().drop();
    final Database single = factory.create();
    try {
      single.getSchema().createVertexType("Node");
      single.transaction(() -> single.newVertex("Node").set("name", "solo").save());

      final List<Result> results = new ArrayList<>();
      try (final ResultSet rs = single.query("opencypher",
          "CALL algo.maxKCut($k, {seed: 42, restarts: 1, maxIterations: 1}) YIELD node, community RETURN node, community",
          Map.of("k", 2_000_000_000))) {
        while (rs.hasNext())
          results.add(rs.next());
      }

      // The assertion has to be exact, not a range: the only observable of a one-node cut is the
      // single community id, and `isBetween(0, 1)` would pass for k = 1 too, so it could never fail.
      // The node's partition is the first draw of the seeded RNG, `new Random(42).nextInt(k)`, which
      // is 1 for k = 2 and can only ever be 0 for k = 1. Asserting 1 is therefore exactly the
      // assertion that distinguishes the floor being present from it being absent.
      assertThat(results).hasSize(1);
      assertThat(((Number) results.getFirst().getProperty("community")).intValue()).isEqualTo(1);
    } finally {
      single.drop();
    }
  }

  // ── Part B: a negative result-count bound is named, not thrown from an allocator ──

  @Test
  void knnRejectsANegativeKWithAMessageNamingTheParameter() {
    assertThatThrownBy(() -> drain("CALL algo.knn($k, 'LINK', 'BOTH') YIELD node1, node2 RETURN node1, node2",
        Map.of("k", -5)))
        .hasStackTraceContaining("algo.knn(): k must not be negative");
  }

  @Test
  void kShortestPathsRejectsANegativeKWithAMessageNamingTheParameter() {
    assertThatThrownBy(() -> drain("""
        MATCH (a:Node {name: 'A'}), (d:Node {name: 'D'}) \
        CALL algo.kShortestPaths(a, d, $k) YIELD path, weight, rank \
        RETURN rank, weight""", Map.of("k", -5)))
        .hasStackTraceContaining("algo.kShortestPaths(): k must not be negative");
  }

  @Test
  void kShortestPathsStillHonoursAPositiveK() {
    // Guards the negative-k rejection against over-reach: 2 is still 2.
    final List<Result> results = drain("""
        MATCH (a:Node {name: 'A'}), (d:Node {name: 'D'}) \
        CALL algo.kShortestPaths(a, d, 2) YIELD path, weight, rank \
        RETURN rank, weight""", Map.of());

    assertThat(results).hasSize(2);
  }

  private List<Result> drain(final String cypher, final Map<String, Object> params) {
    final List<Result> results = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher, params)) {
      while (rs.hasNext())
        results.add(rs.next());
    }
    return results;
  }
}
