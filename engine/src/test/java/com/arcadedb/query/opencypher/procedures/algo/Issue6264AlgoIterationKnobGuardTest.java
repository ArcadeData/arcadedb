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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAlgorithms;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.WorkCheckpoint;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6264AlgoIterationKnobGuardTest {
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
   */
  @Test
  void thePageRankKernelCallsTheCheckpointOncePerIteration() {
    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withVertexTypes("Node").withEdgeTypes("LINK").build();

    final AtomicInteger calls = new AtomicInteger();
    GraphAlgorithms.pageRank(gav, 0.85, 7, Vertex.DIRECTION.OUT, calls::incrementAndGet, "LINK");

    assertThat(calls.get()).as("one checkpoint per power iteration bounds abort latency by one graph sweep")
        .isEqualTo(7);
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

    assertThatThrownBy(() -> drain("CALL algo.pagerank({maxIterations: 2000000000}) YIELD node RETURN node"))
        .as("the CSR kernel has no convergence test, so only the checkpoint can end this run")
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
