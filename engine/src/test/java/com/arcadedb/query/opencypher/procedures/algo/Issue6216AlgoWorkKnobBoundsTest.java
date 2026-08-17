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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6216AlgoWorkKnobBoundsTest {
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
    final AbstractAlgoProcedure.WorkGuard guard = new AlgoMaxKCut().newWorkGuard(context);

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
    final AbstractAlgoProcedure.WorkGuard guard = new AlgoMaxKCut().newWorkGuard(context);

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
    // is a directed cycle, so it never dead-ends and would otherwise run all 500 million steps.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1L);

    assertThatThrownBy(() -> drain("""
        MATCH (a:Node {name: 'A'}) \
        CALL algo.randomWalk(a, 500000000) \
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
