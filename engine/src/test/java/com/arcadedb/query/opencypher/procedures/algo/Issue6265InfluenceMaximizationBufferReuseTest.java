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
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression tests for issue #6265 - {@code algo.influenceMaximization}'s {@code simulateIC} used to allocate
 * and zero-fill a {@code boolean[nodeCount]} and an {@code int[nodeCount]} on every one of its
 * {@code k x nodeCount x simulations} calls. Both buffers are now hoisted to the caller and reused across every
 * call, with {@code simulateIC} resetting only the entries it touched (recorded in {@code queue[0..tail)})
 * instead of reallocating.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6265InfluenceMaximizationBufferReuseTest {
  private Database database;

  @AfterEach
  void teardown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  // ── 1. Correctness: reused buffers must not leak activation state across calls ──────────────

  /**
   * A hub-and-spoke graph (H -> L1, H -> L2, H -> L3) with {@code propagationProbability = 1.0}, so every
   * cascade is fully deterministic: whichever node(s) a simulation starts from, the reachable set is exactly
   * what BFS from those nodes finds, with no randomness involved.
   * <p>
   * This is the shape that catches a broken reset: {@code simulateIC}'s very first call (candidate H in round
   * 1) activates all four nodes. If the shared {@code activated}/{@code queue} buffers were not cleared back to
   * their touched entries afterward, every following call would see H (and every other previously-visited node)
   * as already activated and skip re-adding it to the queue - silently undercounting every simulation after the
   * first, and producing a negative marginal gain in round 2 instead of the correct zero (the graph is already
   * fully covered by H alone, so adding any second seed contributes nothing).
   */
  @Test
  void reusedBuffersDoNotLeakActivationStateBetweenSimulations() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6265-buffer-reuse");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.transaction(() -> {
      final MutableVertex h = database.newVertex("Node").set("name", "H").save();
      final MutableVertex l1 = database.newVertex("Node").set("name", "L1").save();
      final MutableVertex l2 = database.newVertex("Node").set("name", "L2").save();
      final MutableVertex l3 = database.newVertex("Node").set("name", "L3").save();
      h.newEdge("LINK", l1, true, (Object[]) null).save();
      h.newEdge("LINK", l2, true, (Object[]) null).save();
      h.newEdge("LINK", l3, true, (Object[]) null).save();
    });

    // k = 4 (all nodes), a single simulation per candidate (p = 1.0 makes every cascade deterministic, so
    // more simulations would only repeat the same outcome), and propagationProbability = 1.0.
    final List<Result> rows = drain(influenceMaximization(4, "LINK", 1, 1.0));
    assertThat(rows).hasSize(4);

    // Round 1 must pick the hub: it is the only node whose cascade reaches all four nodes.
    assertThat(rows.get(0).<Object>getProperty("nodeId").toString()).contains(vertexRid("H"));
    assertThat(rows.get(0).<Number>getProperty("marginalGain").doubleValue()).isEqualTo(4.0);

    // Every following round adds a leaf to an already-fully-covered graph: the correct marginal gain is 0,
    // never negative. A leaked `activated` bit from an earlier call would undercount the cascade and drive
    // this negative instead.
    for (int i = 1; i < rows.size(); i++)
      assertThat(rows.get(i).<Number>getProperty("marginalGain").doubleValue())
          .as("round " + (i + 1) + " marginal gain must not be negative")
          .isEqualTo(0.0);
  }

  private String vertexRid(final String name) {
    return database.query("sql", "SELECT FROM Node WHERE name = ?", name).next().getElement().orElseThrow().getIdentity().toString();
  }

  // ── 2. Allocation: no more per-simulation churn ──────────────────────────────────────────────

  private static final int CHAIN_NODES = 300;
  private static final int SIMULATIONS = 300;

  private void openChainDatabase() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6265-allocation");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.transaction(() -> {
      MutableVertex previous = null;
      for (int i = 0; i < CHAIN_NODES; i++) {
        final MutableVertex v = database.newVertex("Node").set("name", "A" + i).save();
        if (previous != null)
          previous.newEdge("LINK", v, true, (Object[]) null).save();
        previous = v;
      }
    });
  }

  @Test
  @Tag("performance")
  void simulateICNoLongerAllocatesPerCall() {
    // Measured on the thread's own allocation counter, which no GC and no other test can move.
    //
    // k = 1 evaluates every one of the ~299 non-seed candidates, each run through 300 simulations: ~89,700
    // simulateIC calls. Before this fix each call allocated a boolean[300] (~324 bytes with header/padding)
    // plus an int[300] (~1216 bytes) - about 1500 bytes/call, ~134 MB total for this one procedure call. Now
    // both buffers are allocated once for the whole call and simulateIC only flips bits it already touched, so
    // the bound below separates "reused" from "reallocated every call" by close to two orders of magnitude
    // while leaving headroom for the rest of the procedure's one-time allocations (adjacency lists, Result
    // objects, boxed doubles for k=1 round's single output row).
    //
    // PR #6714 review: a 1% bound (1,345,500 bytes) passed locally across repeated runs but failed once in CI
    // at 1,443,080 bytes - about 7% over, still ~93x below the ~134MB unfixed-behaviour baseline it exists to
    // catch. The one-time overhead this measures is not itself bounded by anything the fix controls (JIT/JVM
    // allocation noise on the CI runner's shared hardware, not GC - the thread-local counter is immune to
    // that), so 5% keeps ~20x separation from the pathological case while absorbing that margin.
    final com.sun.management.ThreadMXBean threads = threadAllocationBean();
    assumeTrue(threads != null, "JVM does not expose per-thread allocation counters");

    openChainDatabase();

    // Warm up so class loading / JIT compilation is not attributed to the measured call.
    for (int i = 0; i < 2; i++)
      assertThat(drain(influenceMaximization(1, "LINK", SIMULATIONS, 0.1))).isNotEmpty();

    final long oldPerCallEstimate = 1500L * (long) (CHAIN_NODES - 1) * SIMULATIONS;
    final long allocated = measure(threads, () -> assertThat(drain(influenceMaximization(1, "LINK", SIMULATIONS, 0.1))).isNotEmpty());

    assertThat(allocated)
        .as("a boolean[n]+int[n] pair per simulateIC call is what this bound separates from buffers reused "
            + "across the whole procedure call (allocated=" + allocated + " bytes, old per-call estimate=" + oldPerCallEstimate + " bytes)")
        .isLessThan(oldPerCallEstimate / 20);
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

  private Stream<Result> influenceMaximization(final int k, final String relTypes, final int simulations, final double p) {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return new AlgoInfluenceMaximization().execute(new Object[] { k, relTypes, simulations, p }, null, context);
  }

  private static List<Result> drain(final Stream<Result> rows) {
    final List<Result> results = new ArrayList<>();
    for (final Iterator<Result> it = rows.iterator(); it.hasNext(); )
      results.add(it.next());
    return results;
  }

  private static long measure(final com.sun.management.ThreadMXBean threads, final Runnable body) {
    final long id = Thread.currentThread().getId();
    final long before = threads.getThreadAllocatedBytes(id);
    body.run();
    return threads.getThreadAllocatedBytes(id) - before;
  }

  private static com.sun.management.ThreadMXBean threadAllocationBean() {
    if (!(ManagementFactory.getThreadMXBean() instanceof final com.sun.management.ThreadMXBean bean))
      return null;
    if (!bean.isThreadAllocatedMemorySupported())
      return null;
    bean.setThreadAllocatedMemoryEnabled(true);
    return bean.isThreadAllocatedMemoryEnabled() ? bean : null;
  }
}
