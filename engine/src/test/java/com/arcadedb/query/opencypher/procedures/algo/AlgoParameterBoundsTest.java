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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.WorkCheckpoint;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.StallAwareStopwatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * How the {@code algo.*} procedures read and bound their arguments: numeric and work knobs, iteration knobs, direction and relationship-type arguments.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoParameterBoundsTest {
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
  @Nested
  class NumericParameterBounds {
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

  /**
   * Regression tests for issue #6216 - the CPU-shaped config knobs of the OpenCypher {@code algo.*} procedures.
   * <p>
   * Follow-up to #6065/#5924, which bounded the allocation-shaped knobs. The knobs covered here multiply loop
   * counts instead of a single allocation, so they are bounded by the resource each one actually consumes:
   * <ul>
   *   <li>the parameter's own domain - a walk count, restart count or simulation count below its minimum does
   *       not mean "a smaller run", it means an answer the algorithm cannot produce, and today those values are
   *       either silently absorbed or reach the allocator as a nameless NegativeArraySizeException;</li>
   *   <li>heap - {@code walksPerNode x nodeCount x walkLength} and {@code steps} size real buffers, checked in
   *       saturating long arithmetic against {@code arcadedb.cypher.algoMaxWorkingMemory} before allocating, which
   *       is also what catches the {@code n * walksPerNode} int overflow;</li>
   *   <li>time - no honest ceiling exists, so a long run is made abortable (thread interrupt and
   *       {@code arcadedb.command.timeout}) rather than forbidden.</li>
   * </ul>
   */
  @Nested
  class WorkKnobBounds {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6216-algo-work-knobs");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");

      // Directed cycle A→B→C→D→A: every node has an outgoing edge, so no walk ever dead-ends.
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Node").set("name", "A").save();
        final MutableVertex b = database.newVertex("Node").set("name", "B").save();
        final MutableVertex c = database.newVertex("Node").set("name", "C").save();
        final MutableVertex d = database.newVertex("Node").set("name", "D").save();
        a.newEdge("LINK", b, true, (Object[]) null).save();
        b.newEdge("LINK", c, true, (Object[]) null).save();
        c.newEdge("LINK", d, true, (Object[]) null).save();
        d.newEdge("LINK", a, true, (Object[]) null).save();
      });
    }

    @AfterEach
    void teardown() {
      if (database != null)
        database.drop();
    }

    // ── The walk buffers: int overflow and the heap budget ────────────────────

    @Test
    void node2VecRejectsAWalksPerNodeThatWrapsTheWalkCountProduct() {
      // 4 nodes x 2^30 walks each is exactly 2^32, so the old `int totalWalks = n * walksPerNode` wrapped to 0:
      // the matrix was allocated with zero rows and the generator died on `walks[wi++]` with a bare
      // ArrayIndexOutOfBoundsException that named nothing. The product is now computed in long.
      assertThatThrownBy(() -> drain("CALL algo.node2vec({walksPerNode: 1073741824, walkLength: 8}) YIELD node RETURN node"))
          .as("a walk count that wraps int must be rejected by name, not reach the allocator")
          .hasStackTraceContaining("walksPerNode");
    }

    @Test
    void node2VecRejectsAWalkMatrixLargerThanTheWalkMemoryBudget() {
      // Default knobs on a 4-node graph: 40 walks of 80 steps, roughly 14 KB of walk buffers. With the budget
      // set below that the call must be refused before allocating, naming both knobs and the setting to raise.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1024L);

      assertThatThrownBy(() -> drain("CALL algo.node2vec({embeddingDimension: 4}) YIELD node RETURN node"))
          .as("a walk matrix over the budget must be refused up front")
          .hasStackTraceContaining("walksPerNode=10 x walkLength=80 over 4 nodes")
          .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
    }

    @Test
    void node2VecRunsWhenTheWalkMatrixFitsTheBudget() {
      // Over-reach guard: the same call with a budget above the estimate must be untouched by the check.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1024L * 1024L);

      assertThat(drain("CALL algo.node2vec({embeddingDimension: 4, walkLength: 5, walksPerNode: 2, seed: 42}) "
          + "YIELD node RETURN node")).hasSize(4);
    }

    @Test
    void node2VecRejectsAWalkMatrixWhoseFootprintOverflowsEvenLongArithmetic() {
      // Both knobs at Integer.MAX_VALUE put the byte estimate itself past a long: 4 nodes x 2147483647 walks
      // of (36 + 4 x 2147483647) bytes each is about 7.4e19, against a long ceiling of 9.2e18. saturatingProduct
      // has to saturate rather than wrap, because a wrapped estimate can come back small enough to pass the
      // budget check - the precise failure the long arithmetic exists to prevent - and the message then says
      // "over" the ceiling instead of quoting a figure it cannot represent.
      assertThatThrownBy(
          () -> drain("CALL algo.node2vec({walksPerNode: 2147483647, walkLength: 2147483647}) YIELD node RETURN node"))
          .as("an estimate past Long.MAX_VALUE must saturate, never wrap into a passing value")
          .hasStackTraceContaining("over " + Long.MAX_VALUE + " bytes")
          .hasStackTraceContaining("walksPerNode=2147483647 x walkLength=2147483647 over 4 nodes");
    }

    @Test
    void node2VecRejectsMoreWalksThanAJavaArrayCanHoldEvenWithTheBudgetDisabled() {
      // The budget is what normally catches an oversized matrix, but the long product must still be refused on
      // its own account when the budget is switched off: 2^32 rows do not fit a Java array whatever the heap is.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);

      assertThatThrownBy(() -> drain("CALL algo.node2vec({walksPerNode: 1073741824, walkLength: 8}) YIELD node RETURN node"))
          .hasStackTraceContaining("4294967296 walks, more than the 2147483647 entries a Java array can hold");
    }

    @Test
    void randomWalkRejectsAStepCountThatWrapsTheWalkBuffer() {
      // `new int[steps + 1]` wrapped to Integer.MIN_VALUE at steps = Integer.MAX_VALUE and died with a bare
      // NegativeArraySizeException. 2147483647 walk entries are also 8 GB, well over the default budget.
      assertThatThrownBy(() -> drain("""
          MATCH (a:Node {name: 'A'}) \
          CALL algo.randomWalk(a, 2147483647) \
          YIELD path, steps \
          RETURN path, steps"""))
          .as("a step count that wraps the walk buffer must be rejected by name")
          .hasStackTraceContaining("steps=2147483647");
    }

    @Test
    void randomWalkRejectsAWalkLargerThanTheWalkMemoryBudget() {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1024L);

      assertThatThrownBy(() -> drain("""
          MATCH (a:Node {name: 'A'}) \
          CALL algo.randomWalk(a, 5000) \
          YIELD path, steps \
          RETURN path, steps"""))
          .as("a walk buffer over the budget must be refused up front")
          .hasStackTraceContaining("steps=5000")
          .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
    }

    @Test
    void randomWalkRejectsMoreStepsThanAJavaArrayCanHoldEvenWithTheBudgetDisabled() {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);

      assertThatThrownBy(() -> drain("""
          MATCH (a:Node {name: 'A'}) \
          CALL algo.randomWalk(a, 2147483647) \
          YIELD path, steps \
          RETURN path, steps"""))
          .hasStackTraceContaining("2147483648 walk entries, more than the 2147483647 a Java array can hold");
    }

    // ── The parameter domains ────────────────────────────────────────────────

    @Test
    void node2VecRejectsANonPositiveWalkLength() {
      // walkLength 0 sized the rows at zero and then wrote walk[0], an ArrayIndexOutOfBoundsException.
      assertThatThrownBy(() -> drain("CALL algo.node2vec({walkLength: 0}) YIELD node RETURN node"))
          .hasStackTraceContaining("walkLength must be at least 1, got 0");
    }

    @Test
    void node2VecRejectsANegativeWalksPerNode() {
      // walksPerNode -1 reached `new int[-4][...]` as a bare NegativeArraySizeException.
      assertThatThrownBy(() -> drain("CALL algo.node2vec({walksPerNode: -1}) YIELD node RETURN node"))
          .hasStackTraceContaining("walksPerNode must be at least 1, got -1");
    }

    @Test
    void node2VecRejectsNonPositiveIterations() {
      // iterations 0 skipped the training loop entirely and silently returned the untrained Xavier init.
      assertThatThrownBy(() -> drain("CALL algo.node2vec({iterations: 0}) YIELD node RETURN node"))
          .hasStackTraceContaining("iterations must be at least 1, got 0");
    }

    @Test
    void node2VecRejectsANonPositiveWindowSize() {
      // windowSize 0 leaves the Skip-gram context empty at every position: training becomes a no-op.
      assertThatThrownBy(() -> drain("CALL algo.node2vec({windowSize: 0}) YIELD node RETURN node"))
          .hasStackTraceContaining("windowSize must be at least 1, got 0");
    }

    @Test
    void node2VecRejectsNegativeNegSamples() {
      assertThatThrownBy(() -> drain("CALL algo.node2vec({negSamples: -3}) YIELD node RETURN node"))
          .hasStackTraceContaining("negSamples must be at least 0, got -3");
    }

    @Test
    void node2VecAcceptsZeroNegSamples() {
      // Over-reach guard: unlike the other knobs, 0 negative samples is a legitimate configuration
      // (plain Skip-gram without negative sampling) and must keep working.
      assertThat(drain("CALL algo.node2vec({embeddingDimension: 4, walkLength: 5, walksPerNode: 2, negSamples: 0, seed: 7}) "
          + "YIELD node RETURN node")).hasSize(4);
    }

    @Test
    void maxKCutRejectsNonPositiveRestarts() {
      // restarts 0 never entered the restart loop: every node came back in community 0 with cutWeight -1.0,
      // a wrong answer reported as a successful one.
      assertThatThrownBy(() -> drain("CALL algo.maxKCut(2, {restarts: 0}) YIELD node, community RETURN node, community"))
          .hasStackTraceContaining("restarts must be at least 1, got 0");
    }

    @Test
    void maxKCutRejectsNonPositiveMaxIterations() {
      // maxIterations 0 skipped the local search: the "maximum" cut was whatever the random init produced.
      assertThatThrownBy(
          () -> drain("CALL algo.maxKCut(2, {maxIterations: -5}) YIELD node, community RETURN node, community"))
          .hasStackTraceContaining("maxIterations must be at least 1, got -5");
    }

    @Test
    void influenceMaximizationRejectsNonPositiveSimulations() {
      // simulations 0 divided the accumulated spread by zero: every candidate scored NaN, no comparison ever
      // won, and the procedure returned an empty result set as if the graph had no influential node.
      assertThatThrownBy(() -> drain("CALL algo.influenceMaximization(2, 'LINK', 0) YIELD nodeId RETURN nodeId"))
          .hasStackTraceContaining("simulations must be at least 1, got 0");
    }

    @Test
    void influenceMaximizationRejectsANegativeK() {
      // k saturates upwards on purpose, but a negative k passed the Math.min(k, n) clamp untouched and reached
      // `new int[seedCount]` as a bare NegativeArraySizeException that never named k.
      assertThatThrownBy(() -> drain("CALL algo.influenceMaximization(-2, 'LINK', 5) YIELD nodeId RETURN nodeId"))
          .hasStackTraceContaining("k must not be negative, got -2");
    }

    @Test
    void node2VecStillCompletesAndTrainsWithLargeButLegitimateKnobs() {
      // The whole PR adds rejections and checkpoints to a hot path, so the risk it carries is refusing or
      // aborting a run that is merely big. Every other test here asserts that something is refused; this one
      // asserts the opposite at a scale the small-graph guards do not reach - 200 walks x 50 steps x 20 epochs,
      // two orders of magnitude past the defaults used elsewhere in this class - and checks the OUTPUT rather
      // than the absence of an exception: one embedding per node, of the requested width, finite, and L2
      // normalised, which is only true if the training loops actually ran to completion.
      final List<Result> results = drain("CALL algo.node2vec({embeddingDimension: 16, walkLength: 50, walksPerNode: 50, "
          + "iterations: 20, windowSize: 5, negSamples: 3, seed: 21}) YIELD node, embedding RETURN node, embedding");

      assertThat(results).hasSize(4);
      for (final Result r : results) {
        @SuppressWarnings("unchecked")
        final List<Double> embedding = (List<Double>) r.getProperty("embedding");
        assertThat(embedding).hasSize(16);
        double squaredNorm = 0.0;
        for (final Double v : embedding) {
          assertThat(Double.isFinite(v)).as("every component must be finite").isTrue();
          squaredNorm += v * v;
        }
        assertThat(Math.sqrt(squaredNorm)).as("embeddings are returned L2 normalised").isCloseTo(1.0, within(1e-9));
      }
    }

    // ── The context window clamp ─────────────────────────────────────────────

    @Test
    void node2VecClampsAWindowWiderThanTheWalkInsteadOfSkippingTraining() {
      // A window wider than the walk already spans the whole walk, so both runs must produce the SAME
      // embeddings. Before the clamp `pos + window` wrapped int for every position past the first, leaving
      // winEnd below winStart: the Skip-gram loop silently trained on almost nothing.
      final String query = "CALL algo.node2vec({embeddingDimension: 8, walkLength: 6, walksPerNode: 2, seed: 3, "
          + "windowSize: %d}) YIELD node, embedding RETURN node, embedding";

      final List<List<Double>> clamped = embeddings(String.format(query, 2147483647));
      final List<List<Double>> spanning = embeddings(String.format(query, 6));

      assertThat(clamped)
          .as("a window wider than the walk must behave exactly like a window spanning the walk")
          .isEqualTo(spanning);
    }

    // ── Cooperative abort: interrupt and command timeout ──────────────────────

    @Test
    void theWorkGuardAbortsAnInterruptedCallAndClearsTheFlag() {
      final CommandContext context = new BasicCommandContext().setDatabase(database);
      final WorkGuard guard = new AlgoMaxKCut().newWorkGuard(context);

      Thread.currentThread().interrupt();
      try {
        assertThatThrownBy(guard::check)
            .as("an interrupted algorithm must abort instead of running the loop to the end")
            .isInstanceOf(CommandExecutionException.class)
            .hasMessageContaining("algo.maxKCut() has been interrupted");
        assertThat(Thread.currentThread().isInterrupted())
            .as("the flag is consumed, so the pooled query thread is not left interrupted for the next task")
            .isFalse();
      } finally {
        Thread.interrupted();
      }
    }

    @Test
    void theWorkGuardAbortsOnceTheCommandDeadlineHasPassed() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1L);
      final CommandContext context = new BasicCommandContext().setDatabase(database);
      final WorkGuard guard = new AlgoMaxKCut().newWorkGuard(context);

      await(5);

      assertThatThrownBy(guard::check)
          .isInstanceOf(TimeoutException.class)
          .hasMessageContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    @Test
    void node2VecHonoursTheCommandTimeout() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1L);

      assertThatThrownBy(() -> drain("CALL algo.node2vec({embeddingDimension: 128, walkLength: 60, walksPerNode: 40, "
          + "iterations: 5, seed: 11}) YIELD node RETURN node"))
          .as("a long training run must give up at the command deadline instead of running to completion")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    @Test
    void node2VecHonoursTheCommandTimeoutInsideOneLongWalk() {
      // The case a checkpoint between walks cannot cover. One node and one walk per node means the training
      // loop reaches its per-walk checkpoint exactly once, before any work has happened; a window as wide as
      // the walk then makes that single walk O(walkLength x walkLength). Only a checkpoint inside the context
      // loop bounds the abort latency, so this run must give up in milliseconds rather than after the seconds
      // the whole walk takes. It needs its own single-node database: algo.node2vec loads the whole graph.
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6216-single-node");
      if (factory.exists())
        factory.open().drop();
      final Database single = factory.create();
      try {
        single.getSchema().createVertexType("Node");
        single.transaction(() -> single.newVertex("Node").set("name", "A").save());
        single.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1L);

        assertThatThrownBy(() -> {
          final ResultSet rs = single.query("opencypher",
              "CALL algo.node2vec({embeddingDimension: 8, walkLength: 8000, walksPerNode: 1, windowSize: 8000, "
                  + "negSamples: 1, iterations: 1, seed: 13}) YIELD node RETURN node");
          while (rs.hasNext())
            rs.next();
        }).as("one walk whose training is quadratic in walkLength must still hit the command deadline")
            .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
      } finally {
        single.drop();
      }
    }

    @Test
    void node2VecHonoursTheCommandTimeoutInsideOneNegativeSamplingLoop() {
      // negSamples is the innermost knob and the only one with neither a heap ceiling (the walk budget prices
      // the walk matrix, not the sampling) nor a maximum. One (position, context) pair costs negSamples x dim,
      // and the nearest enclosing checkpoint runs BEFORE the sampling starts.
      //
      // Unlike the single-long-walk case, the unguarded version does eventually throw - at the next context
      // checkpoint, once the whole sampling loop has run - so "it throws" proves nothing here and the assertion
      // has to be about latency. walkLength 2 with windowSize 1 gives one context pair per position, so the
      // unguarded run has to grind through 200 million x 128 operations before it can notice the deadline.
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1L);

      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> drain("CALL algo.node2vec({embeddingDimension: 128, walkLength: 2, walksPerNode: 1, "
          + "windowSize: 1, negSamples: 200000000, iterations: 1, seed: 17}) YIELD node RETURN node"))
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());

      // The bound comes from measurement, not taste: the whole test method runs in 0.33 s with the checkpoint
      // in place and 24.0 s with it removed, so 5 s sits roughly 16x above the passing case and 5x below the
      // failing one. Wide enough not to be a stopwatch on a slow machine, tight enough that it cannot pass
      // while the sampling loop is unabortable.
      //
      // A latency assertion is the CI-flakiest shape there is, so the margins are what matter: both numbers
      // scale together on a slower runner, and it takes a machine 15x slower than the reference to make the
      // passing case reach 5 s (where the failing case would be at 360 s), or 5x faster to bring the failing
      // case under it. If it ever does flake, raise the bound rather than dropping the assertion - "it throws"
      // alone passes with the checkpoint removed, which is how the first version of this test was wrong.
      stopwatch.assertStayedUnder(5_000L, "the deadline observed inside the sampling loop, not after it");
    }

    @Test
    void randomWalkHonoursTheCommandTimeoutEvenWithTheWalkMemoryBudgetDisabled() {
      // The budget is the only thing bounding `steps`, and it accepts "negative = no limit". With it disabled a
      // huge steps value is neither memory- nor - without a checkpoint in the step loop - time-bounded. The walk
      // is a directed cycle, so it never dead-ends and would otherwise run all 20 million steps.
      //
      // 20 million is "huge" for the deadline, not for the heap: the walk buffer is allocated up front at 4 bytes a
      // step, so 500 million asked for a 2 GB contiguous array and the test died with an OutOfMemoryError on any heap
      // that could not hand one over (a small -Xmx, or a fragmented one late in a full-suite run) instead of reaching
      // the checkpoint. 20 million still takes far longer than the 1 ms deadline to walk.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1L);

      assertThatThrownBy(() -> drain("""
          MATCH (a:Node {name: 'A'}) \
          CALL algo.randomWalk(a, 20000000) \
          YIELD path, steps \
          RETURN path, steps"""))
          .as("a memory bound is not a time bound: the walk must still give up at the command deadline")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    @Test
    void maxKCutHonoursTheCommandTimeout() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1L);

      assertThatThrownBy(() -> drain("CALL algo.maxKCut(2, {restarts: 20000000, seed: 5}) YIELD node RETURN node"))
          .as("a huge restart count must give up at the command deadline instead of running to completion")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    @Test
    void influenceMaximizationHonoursTheCommandTimeout() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1L);

      assertThatThrownBy(() -> drain("CALL algo.influenceMaximization(2, 'LINK', 10000000) YIELD nodeId RETURN nodeId"))
          .as("a huge simulation count must give up at the command deadline instead of running to completion")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private List<Result> drain(final String query) {
      final ResultSet rs = database.query("opencypher", query);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());
      return results;
    }

    @SuppressWarnings("unchecked")
    private List<List<Double>> embeddings(final String query) {
      final Map<String, List<Double>> byNode = new TreeMap<>();
      for (final Result r : drain(query))
        byNode.put(r.getProperty("node").toString(), (List<Double>) r.getProperty("embedding"));
      return new ArrayList<>(byNode.values());
    }

    private static void await(final long millis) {
      final long until = System.currentTimeMillis() + millis;
      while (System.currentTimeMillis() < until)
        Thread.onSpinWait();
    }
  }

  /**
   * Regression tests for issue #6264 - the iteration-shaped knob of the fourteen {@code algo.*} procedures that
   * #6216 left out of scope. The issue named thirteen; {@code algo.graphsage}'s {@code layers} is a fourteenth with
   * exactly the same shape, found in review.
   * <p>
   * #6216 established that such a knob needs two things, and gave both to {@code algo.node2vec},
   * {@code algo.maxKCut} and {@code algo.influenceMaximization} only:
   * <ul>
   *   <li><b>a domain minimum, rejected by name.</b> Below its minimum an iteration count does not mean "a smaller
   *       run", it means an answer the algorithm cannot produce - {@code algo.pageRank({maxIterations: 0})} returned
   *       the uniform initial rank vector as though it were a PageRank result, {@code algo.louvain} returned every
   *       node in its own community, {@code algo.fastrp} the untouched random projection. An unconverged centrality
   *       is not obviously wrong to a caller, unlike an exception, which is what makes the silent half the more
   *       serious one;</li>
   *   <li><b>a checkpoint inside the loop the knob drives,</b> so a large value is abortable by thread interrupt and
   *       by {@code arcadedb.command.timeout} rather than forbidden by a guessed cap. Six of the fourteen -
   *       pageRank on CSR, simRank, fastRP, hashGNN, graphSAGE and slpa - have no convergence test at all, so the
   *       knob alone decided when they stopped.</li>
   * </ul>
   * SLPA's {@code iterations} additionally buys heap rather than only time (one row of {@code iterations + 1} ints
   * per node), so it is priced against the same budget the walk buffers use.
   */
  @Nested
  class IterationKnobGuard {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6264-algo-iteration-knobs");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");

      // Directed cycle A→B→C→D→A: no node is a sink, so the iterative kernels always have work to do.
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Node").set("name", "A").save();
        final MutableVertex b = database.newVertex("Node").set("name", "B").save();
        final MutableVertex c = database.newVertex("Node").set("name", "C").save();
        final MutableVertex d = database.newVertex("Node").set("name", "D").save();
        a.newEdge("LINK", b, true, (Object[]) null).save();
        b.newEdge("LINK", c, true, (Object[]) null).save();
        c.newEdge("LINK", d, true, (Object[]) null).save();
        d.newEdge("LINK", a, true, (Object[]) null).save();
      });
    }

    @AfterEach
    void teardown() {
      // A test that arms the interrupt flag must not leave it set for whatever runs next on this thread.
      Thread.interrupted();
      if (database != null)
        database.drop();
    }

    // ── The parameter domains: fourteen knobs, one minimum ──────────────────

    /**
     * Every one of the fourteen extracted its knob with a plain {@code extractInt(n, name)} - no minimum - so a
     * non-positive value was absorbed in silence and came back as a result. The value is the trip count of a loop:
     * zero trips is not a cheaper answer, it is the initial state of the algorithm returned as its output.
     */
    @ParameterizedTest(name = "{0}({1})")
    @CsvSource(delimiter = '|', value = {
        "algo.pagerank             | maxIterations | CALL algo.pagerank({maxIterations: 0}) YIELD node RETURN node",
        "algo.articlerank          | maxIterations | CALL algo.articlerank({maxIterations: 0}) YIELD node RETURN node",
        "algo.personalizedPageRank | maxIterations | MATCH (a:Node {name: 'A'}) CALL algo.personalizedPageRank(a, 'LINK', 0.85, 0) YIELD nodeId RETURN nodeId",
        "algo.eigenvector          | maxIterations | CALL algo.eigenvector('LINK', 'BOTH', 0) YIELD node RETURN node",
        "algo.hits                 | maxIterations | CALL algo.hits('LINK', 0) YIELD node RETURN node",
        "algo.katz                 | maxIterations | CALL algo.katz('LINK', 0.005, 0) YIELD nodeId RETURN nodeId",
        "algo.louvain              | maxIterations | CALL algo.louvain({maxIterations: 0}) YIELD node RETURN node",
        "algo.leiden               | maxIterations | CALL algo.leiden('LINK', 0) YIELD nodeId RETURN nodeId",
        "algo.labelpropagation     | maxIterations | CALL algo.labelpropagation({maxIterations: 0}) YIELD node RETURN node",
        "algo.simRank              | maxIterations | MATCH (a:Node {name: 'A'}), (b:Node {name: 'C'}) CALL algo.simRank(a, b, 'LINK', 0.8, 0) YIELD similarity RETURN similarity",
        "algo.slpa                 | iterations    | CALL algo.slpa({iterations: 0}) YIELD node RETURN node",
        "algo.fastrp               | iterations    | CALL algo.fastrp({dimensions: 8, iterations: 0}) YIELD node RETURN node",
        "algo.hashgnn              | iterations    | CALL algo.hashgnn({embeddingDimension: 8, iterations: 0}) YIELD node RETURN node",
        "algo.graphsage            | layers        | CALL algo.graphsage({embeddingDimension: 8, layers: 0}) YIELD node RETURN node" })
    void everyIterationKnobRejectsZero(final String procedure, final String knob, final String query) {
      assertThatThrownBy(() -> drain(query))
          .as("%s must refuse %s 0 by name instead of returning its own initial state as a result", procedure, knob)
          .hasStackTraceContaining(procedure + "(): " + knob + " must be at least 1, got 0");
    }

    /**
     * A negative count reached the same loop and behaved exactly like zero, so it needs the same refusal: the two
     * differ only in that a negative one cannot even be read as "do nothing on purpose".
     * <p>
     * Three of the fourteen, sampled rather than exhaustive, and deliberately so: what distinguishes a negative
     * value from zero lives entirely in {@code extractInt(value, name, minimum)}, which all fourteen share, and the
     * per-procedure half - that the knob is extracted with a minimum at all - is what the zero case above covers
     * for every one of them. Fourteen more rows here would re-test one shared comparison fourteen times.
     */
    @ParameterizedTest(name = "{0}({1})")
    @CsvSource(delimiter = '|', value = {
        "algo.pagerank | maxIterations | CALL algo.pagerank({maxIterations: -7}) YIELD node RETURN node",
        "algo.hits     | maxIterations | CALL algo.hits('LINK', -7) YIELD node RETURN node",
        "algo.fastrp   | iterations    | CALL algo.fastrp({dimensions: 8, iterations: -7}) YIELD node RETURN node" })
    void anIterationKnobRejectsANegativeCount(final String procedure, final String knob, final String query) {
      assertThatThrownBy(() -> drain(query))
          .hasStackTraceContaining(procedure + "(): " + knob + " must be at least 1, got -7");
    }

    /**
     * Over-reach guard for the minimum. The risk a domain check carries is refusing a run that is merely small, so
     * every knob is exercised at exactly its minimum and has to produce a full result set - the boundary is
     * inclusive, and one iteration is a legitimate, if crude, run of each of these algorithms.
     */
    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
        "algo.pagerank             | 4 | CALL algo.pagerank({maxIterations: 1}) YIELD node RETURN node",
        "algo.articlerank          | 4 | CALL algo.articlerank({maxIterations: 1}) YIELD node RETURN node",
        "algo.personalizedPageRank | 4 | MATCH (a:Node {name: 'A'}) CALL algo.personalizedPageRank(a, 'LINK', 0.85, 1) YIELD nodeId RETURN nodeId",
        "algo.eigenvector          | 4 | CALL algo.eigenvector('LINK', 'BOTH', 1) YIELD node RETURN node",
        "algo.hits                 | 4 | CALL algo.hits('LINK', 1) YIELD node RETURN node",
        "algo.katz                 | 4 | CALL algo.katz('LINK', 0.005, 1) YIELD nodeId RETURN nodeId",
        "algo.louvain              | 4 | CALL algo.louvain({maxIterations: 1}) YIELD node RETURN node",
        "algo.leiden               | 4 | CALL algo.leiden('LINK', 1) YIELD nodeId RETURN nodeId",
        "algo.labelpropagation     | 4 | CALL algo.labelpropagation({maxIterations: 1}) YIELD node RETURN node",
        "algo.simRank              | 1 | MATCH (a:Node {name: 'A'}), (b:Node {name: 'C'}) CALL algo.simRank(a, b, 'LINK', 0.8, 1) YIELD similarity RETURN similarity",
        "algo.slpa                 | 4 | CALL algo.slpa({iterations: 1, seed: 1}) YIELD node RETURN node",
        "algo.fastrp               | 4 | CALL algo.fastrp({dimensions: 8, iterations: 1, seed: 1}) YIELD node RETURN node",
        "algo.hashgnn              | 4 | CALL algo.hashgnn({embeddingDimension: 8, iterations: 1, seed: 1}) YIELD node RETURN node",
        "algo.graphsage            | 4 | CALL algo.graphsage({embeddingDimension: 8, layers: 1, seed: 1}) YIELD node RETURN node" })
    void everyIterationKnobAcceptsItsMinimum(final String procedure, final int expectedRows, final String query) {
      assertThat(drain(query)).as("%s must still run at the smallest legal setting", procedure).hasSize(expectedRows);
    }

    // ── Cooperative abort: the checkpoint inside the loop ────────────────────

    /**
     * The interrupt half of the guard, on all fourteen. This is the deterministic one: the flag is armed before the
     * call, so the very first checkpoint the procedure reaches has to observe it, whatever the machine's speed.
     * <p>
     * The assertion is on the guard's own message rather than merely "something was thrown", because only
     * {@code WorkGuard.check()} produces it - nothing else in the query path reports an interrupt as
     * "{@code algo.x() has been interrupted}". Without the checkpoint the run simply completes and returns rows,
     * which is what this test is here to fail on.
     */
    @Timeout(120)
    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
        "algo.pagerank             | CALL algo.pagerank({maxIterations: 2000000000, tolerance: 0.0}) YIELD node RETURN node",
        "algo.articlerank          | CALL algo.articlerank({maxIterations: 2000000000, tolerance: 0.0}) YIELD node RETURN node",
        "algo.personalizedPageRank | MATCH (a:Node {name: 'A'}) CALL algo.personalizedPageRank(a, 'LINK', 0.85, 2000000000, 0.0) YIELD nodeId RETURN nodeId",
        "algo.eigenvector          | CALL algo.eigenvector('LINK', 'BOTH', 2000000000, 0.0) YIELD node RETURN node",
        "algo.hits                 | CALL algo.hits('LINK', 2000000000, 0.0) YIELD node RETURN node",
        "algo.katz                 | CALL algo.katz('LINK', 0.005, 2000000000, 0.0) YIELD nodeId RETURN nodeId",
        "algo.louvain              | CALL algo.louvain({maxIterations: 2000000000}) YIELD node RETURN node",
        "algo.leiden               | CALL algo.leiden('LINK', 2000000000) YIELD nodeId RETURN nodeId",
        "algo.labelpropagation     | CALL algo.labelpropagation({maxIterations: 2000000000}) YIELD node RETURN node",
        "algo.simRank              | MATCH (a:Node {name: 'A'}), (b:Node {name: 'C'}) CALL algo.simRank(a, b, 'LINK', 0.8, 2000000000) YIELD similarity RETURN similarity",
        "algo.slpa                 | CALL algo.slpa({iterations: 1000000, seed: 1}) YIELD node RETURN node",
        "algo.fastrp               | CALL algo.fastrp({dimensions: 8, iterations: 2000000000, seed: 1}) YIELD node RETURN node",
        "algo.hashgnn              | CALL algo.hashgnn({embeddingDimension: 8, iterations: 2000000000, seed: 1}) YIELD node RETURN node",
        "algo.graphsage            | CALL algo.graphsage({embeddingDimension: 8, layers: 2000000000, seed: 1}) YIELD node RETURN node" })
    void everyIterationLoopAbortsOnInterrupt(final String procedure, final String query) {
      Thread.currentThread().interrupt();
      try {
        assertThatThrownBy(() -> drain(query))
            .as("%s must abort at its checkpoint instead of running the whole knob out", procedure)
            .hasStackTraceContaining(procedure + "() has been interrupted");
        assertThat(Thread.currentThread().isInterrupted())
            .as("the flag is consumed, so the pooled query thread is not left interrupted for the next task")
            .isFalse();
      } finally {
        Thread.interrupted();
      }
    }

    /**
     * The deadline half of the guard: {@code arcadedb.command.timeout}, which before #6216 only the SQL SELECT
     * planner honoured.
     * <p>
     * The list is twelve of the fourteen rather than all: {@code algo.louvain} and {@code algo.leiden} both stop as
     * soon as no node changes community, and on this four-node cycle they settle in a handful of microseconds, so a
     * deadline test on them would assert nothing about a run that is over before the clock is read. Their checkpoint
     * is the same {@code WorkGuard.check()} call the other twelve use - it observes the deadline and the interrupt
     * in one place - and it is covered above.
     */
    @Timeout(120)
    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
        "algo.pagerank             | CALL algo.pagerank({maxIterations: 2000000000, tolerance: 0.0}) YIELD node RETURN node",
        "algo.articlerank          | CALL algo.articlerank({maxIterations: 2000000000, tolerance: 0.0}) YIELD node RETURN node",
        "algo.personalizedPageRank | MATCH (a:Node {name: 'A'}) CALL algo.personalizedPageRank(a, 'LINK', 0.85, 2000000000, 0.0) YIELD nodeId RETURN nodeId",
        "algo.eigenvector          | CALL algo.eigenvector('LINK', 'BOTH', 2000000000, 0.0) YIELD node RETURN node",
        "algo.hits                 | CALL algo.hits('LINK', 2000000000, 0.0) YIELD node RETURN node",
        "algo.katz                 | CALL algo.katz('LINK', 0.005, 2000000000, 0.0) YIELD nodeId RETURN nodeId",
        "algo.labelpropagation     | CALL algo.labelpropagation({maxIterations: 2000000000}) YIELD node RETURN node",
        "algo.simRank              | MATCH (a:Node {name: 'A'}), (b:Node {name: 'C'}) CALL algo.simRank(a, b, 'LINK', 0.8, 2000000000) YIELD similarity RETURN similarity",
        "algo.slpa                 | CALL algo.slpa({iterations: 1000000, seed: 1}) YIELD node RETURN node",
        "algo.fastrp               | CALL algo.fastrp({dimensions: 8, iterations: 2000000000, seed: 1}) YIELD node RETURN node",
        "algo.hashgnn              | CALL algo.hashgnn({embeddingDimension: 8, iterations: 2000000000, seed: 1}) YIELD node RETURN node",
        "algo.graphsage            | CALL algo.graphsage({embeddingDimension: 8, layers: 2000000000, seed: 1}) YIELD node RETURN node" })
    void everyIterationLoopHonoursTheCommandTimeout(final String procedure, final String query) {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1L);

      assertThatThrownBy(() -> drain(query))
          .as("%s must give up at the command deadline instead of running the whole knob out", procedure)
          .hasStackTraceContaining(procedure + "() exceeded the " + GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    /**
     * The checkpoint has to sit <em>inside</em> the per-node scan as well as around the iteration loop, and this is
     * the only test that can tell the two apart.
     * <p>
     * {@code maxIterations: 1} means the outer checkpoint runs exactly once, before any work has happened, so it
     * cannot be what fires. SimRank is O(n&sup2; x deg&sup2;) per iteration, so on an 800-node graph of degree 400 a
     * single iteration takes half a minute - and with only the outer checkpoint the call runs it to completion and
     * returns a similarity, no exception at all. It needs its own database because {@code algo.simRank} loads the
     * whole graph.
     * <p>
     * The deadline is 1.5 s rather than 1 ms on purpose: the guard starts its clock before the graph is loaded, and
     * a millisecond deadline would already have passed by the time the outer checkpoint runs, making this pass for
     * the wrong reason. 1.5 s is comfortably longer than loading 800 nodes (measured at ~0.5 s) and far shorter
     * than one iteration.
     */
    @Test
    @Tag("slow")
    @Timeout(300)
    void simRankHonoursTheCommandTimeoutInsideASingleIteration() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6264-simrank-single-pass");
      if (factory.exists())
        factory.open().drop();
      final Database dense = factory.create();
      try {
        dense.getSchema().createVertexType("Node");
        dense.getSchema().createEdgeType("LINK");

        final int nodeCount = 800;
        final int degree = 400;
        dense.transaction(() -> {
          final List<MutableVertex> nodes = new ArrayList<>(nodeCount);
          for (int i = 0; i < nodeCount; i++)
            nodes.add(dense.newVertex("Node").set("idx", i).save());
          for (int i = 0; i < nodeCount; i++)
            for (int k = 1; k <= degree; k++)
              nodes.get(i).newEdge("LINK", nodes.get((i + k) % nodeCount), true, (Object[]) null).save();
        });

        dense.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1_500L);

        final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
        assertThatThrownBy(() -> {
          final ResultSet rs = dense.query("opencypher", "MATCH (a:Node {idx: 0}), (b:Node {idx: 400}) "
              + "CALL algo.simRank(a, b, 'LINK', 0.8, 1) YIELD similarity RETURN similarity");
          while (rs.hasNext())
            rs.next();
        }).as("one iteration longer than the deadline must be abortable from inside, not only between iterations")
            .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());

        // The bound comes from measurement, not taste: the run gives up ~1.5 s after the deadline is armed, and the
        // same call with the per-node checkpoint removed grinds through the whole iteration in ~29 s and returns a
        // similarity instead of throwing. 8 s sits about 4x above the passing case and 3.5x below the failing one,
        // and both figures scale together on a slower runner. If this ever flakes, raise the bound rather than
        // dropping the assertion: the exception alone is already meaningful here (an unguarded run does not throw at
        // all), but the elapsed time is what says the abort came from inside the pass rather than after it.
        stopwatch.assertStayedUnder(8_000L, "the deadline observed inside one iteration, not after it");
      } finally {
        dense.drop();
      }
    }

    // ── The CSR kernels behind algo.pageRank and algo.labelPropagation ───────

    /**
     * {@code algo.pageRank} hands a CSR-backed graph straight to {@link GraphAlgorithms#pageRank}, which lives below
     * the query layer and knew nothing about deadlines. That kernel has no convergence test at all, so it always ran
     * the full {@code maxIterations}: the knob alone decided when it stopped, and nothing could interrupt it.
     * <p>
     * Since issue #6318 threaded {@code parallelForRangeCheckpointed} through pageRank's parallel phases (contrib,
     * pull, and dangling-node redistribution when the graph has dangling nodes), the checkpoint is called more than
     * once per iteration: the original once-per-iteration call at the top of the loop, plus one more per parallel
     * phase actually run - more still on a graph large enough for a phase to split into several batches. This
     * fixture is a directed cycle with no dangling nodes, so the dangling phase never runs and every iteration
     * checks in exactly 3 times: the loop-top call, then contrib, then pull.
     */
    @Test
    void thePageRankKernelCallsTheCheckpointAtLeastOncePerParallelPhaseEachIteration() {
      final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
          .withVertexTypes("Node").withEdgeTypes("LINK").build();

      final AtomicInteger calls = new AtomicInteger();
      GraphAlgorithms.pageRank(gav, 0.85, 7, Vertex.DIRECTION.OUT, calls::incrementAndGet, "LINK");

      assertThat(calls.get())
          .as("loop-top call + contrib phase + pull phase (no dangling nodes on this cycle), x 7 iterations")
          .isEqualTo(3 * 7);
    }

    @Test
    void thePageRankKernelPropagatesAnAbortFromTheCheckpoint() {
      final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
          .withVertexTypes("Node").withEdgeTypes("LINK").build();

      final AtomicInteger calls = new AtomicInteger();
      final WorkCheckpoint abortOnThird = () -> {
        if (calls.incrementAndGet() == 3)
          throw new IllegalStateException("aborted by the caller");
      };

      assertThatThrownBy(() -> GraphAlgorithms.pageRank(gav, 0.85, 1000, Vertex.DIRECTION.OUT, abortOnThird, "LINK"))
          .isInstanceOf(IllegalStateException.class).hasMessage("aborted by the caller");
      assertThat(calls.get()).as("the kernel stops at the checkpoint rather than finishing the run").isEqualTo(3);
    }

    @Test
    void theLabelPropagationKernelPropagatesAnAbortFromTheCheckpoint() {
      final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
          .withVertexTypes("Node").withEdgeTypes("LINK").build();

      final AtomicInteger calls = new AtomicInteger();
      final WorkCheckpoint abortOnSecond = () -> {
        if (calls.incrementAndGet() == 2)
          throw new IllegalStateException("aborted by the caller");
      };

      assertThatThrownBy(() -> GraphAlgorithms.labelPropagation(gav, 1000, abortOnSecond, "LINK"))
          .isInstanceOf(IllegalStateException.class).hasMessage("aborted by the caller");
      assertThat(calls.get()).isEqualTo(2);
    }

    /**
     * The same abort end to end, through Cypher, on the CSR path rather than the OLTP one.
     * <p>
     * The query deliberately leaves {@code tolerance} at its default: the OLTP path converges on this graph within a
     * handful of iterations and returns, so a run that still has to be aborted proves the CSR kernel - which ignores
     * tolerance entirely - is the one that executed.
     */
    @Test
    @Timeout(120)
    void pageRankOnACSRBackedGraphHonoursTheCommandTimeout() {
      GraphAnalyticalView.builder(database).withVertexTypes("Node").withEdgeTypes("LINK").build();
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1L);

      // tolerance 0 never converges: since #9791 the CSR kernel stops on the tolerance as the OLTP path does
      assertThatThrownBy(() -> drain("CALL algo.pagerank({maxIterations: 2000000000, tolerance: 0.0}) YIELD node RETURN node"))
          .as("with no tolerance to converge on, only the checkpoint can end this run")
          .hasStackTraceContaining("algo.pagerank() exceeded the " + GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    // ── SLPA: an iteration knob that buys heap as well as time ───────────────

    /**
     * Alone among the fourteen, SLPA's {@code iterations} sizes an allocation: every node keeps a label-memory row
     * of {@code iterations + 1} ints, so the matrix is {@code nodeCount x (iterations + 1)} and a value that merely
     * looks large reaches the allocator with nothing between it and the heap.
     */
    @Test
    void slpaRejectsALabelMemoryLargerThanTheBudget() {
      // The budget is set rather than left at its default because the default auto-scales with the JVM heap: on a
      // large-heap runner a value big enough to exceed it would be one this test then has to allocate to prove
      // nothing. 4 nodes x 1000001 ints is 16 MB against a 1 MB budget, and neither is ever reserved.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1024L * 1024L);

      assertThatThrownBy(() -> drain("CALL algo.slpa({iterations: 1000000}) YIELD node RETURN node"))
          .as("a label memory over the budget must be refused before the first row is allocated")
          .hasStackTraceContaining("label memory")
          .hasStackTraceContaining("iterations=1000000 over 4 nodes")
          .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
    }

    @Test
    void slpaRejectsMoreLabelEntriesThanAJavaArrayCanHoldEvenWithTheBudgetDisabled() {
      // The budget is what normally catches an oversized matrix, but it explicitly accepts "negative = no limit",
      // and `iterations + 1` at Integer.MAX_VALUE wrapped to Integer.MIN_VALUE: a bare NegativeArraySizeException
      // naming nothing. The capacity is computed in long and refused on its own account.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);

      assertThatThrownBy(() -> drain("CALL algo.slpa({iterations: 2147483647}) YIELD node RETURN node"))
          .hasStackTraceContaining("2147483648 label entries per node, more than the 2147483647 a Java array can hold");
    }

    @Test
    void slpaRunsWhenTheLabelMemoryFitsTheBudget() {
      // Over-reach guard: the same shape of call, under the budget, must be untouched by the check.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1024L * 1024L);

      assertThat(drain("CALL algo.slpa({iterations: 50, seed: 3}) YIELD node, communities RETURN node, communities"))
          .hasSize(4);
    }

    // ── Over-reach: the results themselves are unchanged ─────────────────────

    /**
     * The change adds a rejection and a checkpoint to every iterative kernel in the package, so the risk it carries
     * is a wrong answer rather than a refused one. PageRank at its defaults is the sharpest available check: on a
     * directed cycle every node is symmetric, so all four scores must be equal and sum to 1, which is only true if
     * the iteration loop ran unaltered.
     */
    @Test
    void pageRankStillConvergesToTheRightAnswerWithTheCheckpointInPlace() {
      final List<Result> results = drain("CALL algo.pagerank() YIELD node, score RETURN node, score");

      assertThat(results).hasSize(4);
      double sum = 0.0;
      for (final Result r : results) {
        final double score = ((Number) r.getProperty("score")).doubleValue();
        assertThat(score).as("every node of a directed cycle carries the same rank").isCloseTo(0.25, within(1e-6));
        sum += score;
      }
      assertThat(sum).isCloseTo(1.0, within(1e-6));
    }

    /**
     * The one place this PR changes what a kernel computes rather than only when it stops: SimRank's per-iteration
     * reset of the n x n similarity matrix became an {@code Arrays.fill} plus the diagonal, instead of an
     * element-by-element write of {@code i == j ? 1.0 : 0.0}. The two are equivalent, and this pins the value that
     * proves it.
     * <p>
     * The fixture is a hub pointing at two leaves, so the leaves share their only in-neighbour and
     * {@code sim(A, B) = decay x sim(hub, hub) = 0.8} - a value the four-node cycle cannot produce, since there
     * every SimRank of two distinct nodes is 0 and a broken reset would go unnoticed.
     * <p>
     * The assertion runs at more than one iteration count on purpose. The reset exists only for the diagonal (the
     * {@code u < v} loop writes every off-diagonal cell itself), and the buffers are swapped each round, so dropping
     * it leaves {@code sim(hub, hub)} at 0 from the second iteration onwards: one iteration still returns 0.8 and
     * only three reveals the difference.
     */
    @Test
    void simRankStillComputesTheSameSimilarityAfterTheMatrixResetRefactor() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6264-simrank-shared-parent");
      if (factory.exists())
        factory.open().drop();
      final Database shared = factory.create();
      try {
        shared.getSchema().createVertexType("Node");
        shared.getSchema().createEdgeType("LINK");
        shared.transaction(() -> {
          final MutableVertex hub = shared.newVertex("Node").set("name", "H").save();
          final MutableVertex a = shared.newVertex("Node").set("name", "A").save();
          final MutableVertex b = shared.newVertex("Node").set("name", "B").save();
          hub.newEdge("LINK", a, true, (Object[]) null).save();
          hub.newEdge("LINK", b, true, (Object[]) null).save();
        });

        for (final int iterations : new int[] { 1, 3 }) {
          final ResultSet rs = shared.query("opencypher", "MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) "
              + "CALL algo.simRank(a, b, 'LINK', 0.8, " + iterations + ") YIELD similarity RETURN similarity");
          assertThat(rs.hasNext()).isTrue();
          assertThat(((Number) rs.next().getProperty("similarity")).doubleValue())
              .as("two nodes sharing their only in-neighbour are decay-similar, at %d iterations", iterations)
              .isCloseTo(0.8, within(1e-9));
        }
      } finally {
        shared.drop();
      }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private List<Result> drain(final String query) {
      final ResultSet rs = database.query("opencypher", query);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());
      return results;
    }
  }

  /**
   * Regression test for issue #6976: {@link GraphEngine#parseDirection(String)} used to silently coerce any
   * unrecognised direction string to {@code BOTH} instead of rejecting it, so a typo like {@code 'INCOMING'} was
   * answered with a plausible-looking - but wrong - result instead of an error. This helper backs ~23
   * {@code algo.*}/path procedures; {@code algo.bfs} is exercised end to end here as a representative caller.
   */
  @Nested
  class UnknownDirectionRejected {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue6976-direction");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("CONNECTS");

      // A -CONNECTS-> B -CONNECTS-> C
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Node").set("name", "A").save();
        final MutableVertex b = database.newVertex("Node").set("name", "B").save();
        final MutableVertex c = database.newVertex("Node").set("name", "C").save();
        a.newEdge("CONNECTS", b, true, (Object[]) null).save();
        b.newEdge("CONNECTS", c, true, (Object[]) null).save();
      });
    }

    @AfterEach
    void teardown() {
      if (database != null)
        database.drop();
    }

    @Test
    void parseDirectionRejectsUnrecognisedValues() {
      for (final String bad : new String[] { "INCOMING", "OUTGOING", "nope", "in ", "" })
        assertThatThrownBy(() -> GraphEngine.parseDirection(bad))
            .as("direction '%s' must be rejected, not coerced to BOTH", bad)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("direction")
            .hasMessageContaining("OUT, IN or BOTH");
    }

    @Test
    void parseDirectionAcceptsValidValuesCaseInsensitively() {
      assertThat(GraphEngine.parseDirection("OUT")).isEqualTo(Vertex.DIRECTION.OUT);
      assertThat(GraphEngine.parseDirection("out")).isEqualTo(Vertex.DIRECTION.OUT);
      assertThat(GraphEngine.parseDirection("IN")).isEqualTo(Vertex.DIRECTION.IN);
      assertThat(GraphEngine.parseDirection("in")).isEqualTo(Vertex.DIRECTION.IN);
      assertThat(GraphEngine.parseDirection("BOTH")).isEqualTo(Vertex.DIRECTION.BOTH);
      assertThat(GraphEngine.parseDirection("both")).isEqualTo(Vertex.DIRECTION.BOTH);
    }

    @Test
    void parseDirectionDefaultsNullToBoth() {
      assertThat(GraphEngine.parseDirection(null)).isEqualTo(Vertex.DIRECTION.BOTH);
    }

    @Test
    void bfsRejectsAnUnrecognisedDirectionInsteadOfSilentlyTraversingBoth() {
      // Before the fix, 'INCOMING' silently became BOTH and algo.bfs returned all 3 nodes instead of erroring.
      assertThatThrownBy(() -> drain("CALL algo.bfs(start, null, 'INCOMING', 5) YIELD node RETURN node"))
          .as("'INCOMING' must be rejected, not coerced to BOTH")
          .hasStackTraceContaining("direction")
          .hasStackTraceContaining("OUT, IN or BOTH");

      // The three valid values, in either case, still work. Start node itself is never in the result, and A has
      // no incoming edges, so B and C (2 nodes) are reachable via OUT/BOTH, and none via IN.
      assertThat(drain("CALL algo.bfs(start, null, 'OUT', 5) YIELD node RETURN node")).isEqualTo(2);
      assertThat(drain("CALL algo.bfs(start, null, 'IN', 5) YIELD node RETURN node")).isEqualTo(0);
      assertThat(drain("CALL algo.bfs(start, null, 'both', 5) YIELD node RETURN node")).isEqualTo(2);
      // No direction arg at all still defaults to BOTH.
      assertThat(drain("CALL algo.bfs(start, null, null, 5) YIELD node RETURN node")).isEqualTo(2);
    }

    private int drain(final String query) {
      int count = 0;
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (start:Node {name: 'A'}) " + query)) {
        while (rs.hasNext()) {
          rs.next();
          count++;
        }
      }
      return count;
    }
  }

  /**
   * Regression tests for issue #9769: {@code algo.mst} and {@code algo.apsp} took {@code (weightProperty, relTypes)} while
   * every other weighted {@code algo.*} procedure takes {@code (relTypes, weightProperty)}, and a call written in the
   * family's order walked an empty graph and answered no rows. Both now take the family order, and a relationship-type
   * argument naming no edge type is refused by every {@code algo.*} procedure instead of selecting nothing.
   */
  @Nested
  class RelTypesArgument {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-9769-reltypes");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("N").createProperty("k", Type.INTEGER);
      database.getSchema().createEdgeType("R").createProperty("w", Type.DOUBLE);
      database.getSchema().createEdgeType("S");

      // Barbell: triangle {0,1,2}, connector 2->3, triangle {3,4,5}. The connector is the heaviest edge of its triangle
      // neighbours so a weighted MST differs from an unweighted one
      database.transaction(() -> {
        final List<MutableVertex> v = new ArrayList<>();
        for (int i = 0; i < 6; i++)
          v.add(database.newVertex("N").set("k", i).save());
        final int[][] edges = { { 0, 1, 1 }, { 1, 2, 1 }, { 2, 0, 5 }, { 2, 3, 2 }, { 3, 4, 1 }, { 4, 5, 1 }, { 5, 3, 5 } };
        for (final int[] e : edges)
          v.get(e[0]).newEdge("R", v.get(e[1]), "w", (double) e[2]).save();
      });
    }

    @AfterEach
    void teardown() {
      if (database != null)
        database.drop();
    }

    @Test
    void mstTakesRelTypesFirst() {
      final List<Result> rows = rows("CALL algo.mst('R', 'w') YIELD source, target, weight, totalWeight RETURN weight, totalWeight");
      assertThat(rows).hasSize(5);
      assertThat(rows.getFirst().<Double>getProperty("totalWeight")).isCloseTo(6.0, within(1e-9));

      assertThat(rows("CALL algo.mst(null, 'w') YIELD totalWeight RETURN totalWeight").getFirst().<Double>getProperty("totalWeight"))
          .isCloseTo(6.0, within(1e-9));
      assertThat(rows("CALL algo.mst('R') YIELD source RETURN source")).hasSize(5);
      assertThat(rows("CALL algo.mst() YIELD source RETURN source")).hasSize(5);
    }

    @Test
    void mstInTheOldOrderIsRefusedNotAnsweredEmpty() {
      assertThatThrownBy(() -> rows("CALL algo.mst('w', 'R') YIELD source RETURN source"))
          .hasStackTraceContaining("algo.mst(): relationship type 'w' does not exist")
          // 'w' is declared on R, so the message points at the swapped order
          .hasStackTraceContaining("'w' is a property of the edge type 'R': the relationship types come first, then the weight property");
      assertThatThrownBy(() -> rows("CALL algo.mst('w') YIELD source RETURN source"))
          .hasStackTraceContaining("relationship type 'w' does not exist");
    }

    @Test
    void apspTakesRelTypesFirst() {
      // Weighted 0 -> 5: 0-2 (via 1, cost 2), 2-3 (2), 3-5 (via 4, cost 2) = 6
      final List<Result> rows = rows("CALL algo.apsp('R', 'w') YIELD source, target, distance "
          + "WITH source, target, distance WHERE source.k = 0 AND target.k = 5 RETURN distance");
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().<Double>getProperty("distance")).isCloseTo(6.0, within(1e-9));

      assertThatThrownBy(() -> rows("CALL algo.apsp('w', 'R') YIELD distance RETURN distance"))
          .hasStackTraceContaining("algo.apsp(): relationship type 'w' does not exist");
    }

    @Test
    void oldOrderWithoutWeightIsRefusedToo() {
      // (null, 'R') used to mean "unit weights over R"; read in the new order R would be the weight property of every type
      assertThatThrownBy(() -> rows("CALL algo.apsp(null, 'R') YIELD distance RETURN distance"))
          .hasStackTraceContaining("algo.apsp(): 'R' is a relationship type, not a weight property");
      assertThatThrownBy(() -> rows("CALL algo.mst(null, 'R') YIELD source RETURN source"))
          .hasStackTraceContaining("algo.mst(): 'R' is a relationship type, not a weight property");
    }

    @Test
    void unknownRelTypeIsRefusedAcrossTheFamily() {
      // The ordinary typo, previously as silent as the swapped order
      assertThatThrownBy(() -> rows("CALL algo.bridges('RR') YIELD source RETURN source"))
          .hasStackTraceContaining("algo.bridges(): relationship type 'RR' does not exist");
      assertThatThrownBy(() -> rows("CALL algo.wcc('RR') YIELD node RETURN node"))
          .hasStackTraceContaining("relationship type 'RR' does not exist");
      // One bad name in a list is enough
      assertThatThrownBy(() -> rows("CALL algo.bridges(['R', 'RR']) YIELD source RETURN source"))
          .hasStackTraceContaining("relationship type 'RR' does not exist");
      assertThatThrownBy(() -> rows("CALL algo.bridges('R,RR') YIELD source RETURN source"))
          .hasStackTraceContaining("relationship type 'RR' does not exist");
      // A vertex type is not a relationship type
      assertThatThrownBy(() -> rows("CALL algo.bridges('N') YIELD source RETURN source"))
          .hasStackTraceContaining("'N' is not a relationship type, it is a vertex type");
      // A node-label filter is checked the same way, against the vertex types
      assertThatThrownBy(() -> rows("CALL algo.graphSummary(null, 'NOPE') YIELD nodeCount RETURN nodeCount"))
          .hasStackTraceContaining("algo.graphSummary(): node label 'NOPE' does not exist");
      assertThatThrownBy(() -> rows("CALL algo.graphSummary(null, 'R') YIELD nodeCount RETURN nodeCount"))
          .hasStackTraceContaining("'R' is not a node label, it is an edge type");
      assertThat(rows("CALL algo.graphSummary(null, 'N') YIELD nodeCount RETURN nodeCount").getFirst().<Number>getProperty("nodeCount")
          .intValue()).isEqualTo(6);
      // The types to skip are names too: a misspelt one would skip nothing
      assertThatThrownBy(() -> rows("MATCH (a:N {k: 0}), (b:N {k: 5}) "
          + "CALL algo.allSimplePaths(a, b, 'R', 5, {skipRelTypes: ['SS']}) YIELD path RETURN path"))
          .hasStackTraceContaining("algo.allsimplepaths(): relationship type 'SS' does not exist");
      assertThatThrownBy(() -> rows("MATCH (a:N {k: 0}), (b:N {k: 5}) "
          + "CALL algo.allSimplePaths(a, b, 'R', 5, {skipVertexTypes: 'NN'}) YIELD path RETURN path"))
          .hasStackTraceContaining("algo.allsimplepaths(): node label 'NN' does not exist");
      assertThatThrownBy(() -> rows("MATCH (a:N {k: 0}), (b:N {k: 5}) "
          + "CALL algo.allSimplePaths(a, b, 'R', 5, {skipVertexTypes: 'R'}) YIELD path RETURN path"))
          .hasStackTraceContaining("'R' is not a node label, it is an edge type");
      // The single-type procedures too
      assertThatThrownBy(() -> rows("MATCH (a:N {k: 0}), (b:N {k: 5}) CALL algo.dijkstra(a, b, 'RR', 'w') YIELD path RETURN path"))
          .hasStackTraceContaining("algo.dijkstra(): relationship type 'RR' does not exist");
    }

    @Test
    void existingAndAbsentFiltersStillWork() {
      assertThat(rows("CALL algo.bridges('R') YIELD source RETURN source")).hasSize(1);
      assertThat(rows("CALL algo.bridges(['R', 'S']) YIELD source RETURN source")).hasSize(1);
      // An existing type with no edges is a legitimate empty filter, not an error
      assertThat(rows("CALL algo.bridges('S') YIELD source RETURN source")).isEmpty();
      // A blank entry in a list is dropped like an empty part of 'R,,S'; a list of blanks means every type
      assertThat(rows("CALL algo.bridges(['R', '']) YIELD source RETURN source")).hasSize(1);
      assertThat(rows("CALL algo.bridges(['', ' ']) YIELD source RETURN source")).hasSize(1);
      // Blank and null mean every type
      assertThat(rows("CALL algo.bridges('') YIELD source RETURN source")).hasSize(1);
      assertThat(rows("CALL algo.bridges(null) YIELD source RETURN source")).hasSize(1);
    }

    private List<Result> rows(final String cypher) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }
  }
}
