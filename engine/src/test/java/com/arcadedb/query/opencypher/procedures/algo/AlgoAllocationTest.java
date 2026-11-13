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
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Allocation churn on the hot paths of the {@code algo.*} procedures: buffers reused and primitive arrays instead of boxed collections.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoAllocationTest {
  /**
   * Regression tests for issue #6289 - allocation churn on the dense {@code algo.*} paths.
   * <p>
   * Three findings from the review of PR #6285 (issue #6263), none of which raise a <em>peak</em> - which is why
   * the budget added there prices them correctly and none of them is a bug it could have caught:
   * <ol>
   *   <li>{@code algo.kShortestPaths} allocated a {@code nodeCount x nodeCount} removed-edge mask per spur node.
   *   Only one was ever live, so the peak was one; the churn was {@code k x pathLength} allocations of
   *   {@code nodeCount²} bytes - ~200 MB through the young generation for a single call at 1000 nodes. The mask
   *   never needed a row per source: every edge Yen's removes for a given spur node leaves that same node, so a
   *   single {@code boolean[nodeCount]} indexed by target says exactly as much, and it is cleared between spur
   *   nodes rather than reallocated.</li>
   *   <li>{@code algo.steinerTree} and {@code algo.mst} sorted their Kruskal edge indices through an
   *   {@code Integer[]} - 24 bytes per entry where the {@code int} occupies 4, plus two unboxings per comparison,
   *   over an entry count that is quadratic in a caller-supplied terminal list. Both now use a primitive stable
   *   index sort.</li>
   *   <li>{@code MemoryBudget.reserve()} added to the running total before checking it, so a refused reservation
   *   was recorded as granted.</li>
   * </ol>
   * <p>
   * The allocation assertions carry {@code @Tag("performance")} rather than {@code @Tag("benchmark")}, and that is
   * deliberate: {@code benchmark} is one of the three lanes CI <em>excludes</em> from the normal build
   * ({@code -DexcludedGroups=slow,benchmark,vector}), and a regression guard that never runs guards nothing. These
   * are not throughput measurements - they read {@code ThreadMXBean}'s per-thread allocation counter, which no GC
   * and no concurrent test can move - so they belong in the default lane, where {@code performance} leaves them.
   * The tag is a label for the reader, matching {@code RidScoreMinHeapTest}; the partition CLAUDE.md describes is
   * untouched by it.
   */
  @Nested
  class AllocationChurn {
    /**
     * Nodes that carry the paths: a chain {@code A0 -> A1 -> ... -> A20} doubled by a bypass node per hop, so
     * every hop offers two routes of different weight and Yen's has plenty of distinct paths to find - which is
     * what drives the spur-node loop the mask used to be reallocated in.
     */
    private static final int CHAIN_HOPS = 20;

    /**
     * Isolated vertices added on top. They take no part in any path, so they change neither the number of spur
     * nodes nor the work Dijkstra does - they change only {@code nodeCount}, which is exactly the dimension the
     * old per-spur-node mask was quadratic in.
     */
    private static final int PADDING_NODES = 460;

    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6289-algo-churn");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");

      database.transaction(() -> {
        final MutableVertex[] chain = new MutableVertex[CHAIN_HOPS + 1];
        for (int i = 0; i <= CHAIN_HOPS; i++)
          chain[i] = database.newVertex("Node").set("name", "A" + i).save();

        for (int i = 0; i < CHAIN_HOPS; i++) {
          // Direct hop, and a two-edge detour through a bypass node with a slightly different total weight so
          // that every combination of the two is a distinct path with a distinct cost.
          chain[i].newEdge("LINK", chain[i + 1], true, (Object[]) null).save().set("w", 10.0).save();
          final MutableVertex bypass = database.newVertex("Node").set("name", "B" + i).save();
          chain[i].newEdge("LINK", bypass, true, (Object[]) null).save().set("w", 5.0 + i * 0.01).save();
          bypass.newEdge("LINK", chain[i + 1], true, (Object[]) null).save().set("w", 5.0 + i * 0.01).save();
        }

        for (int i = 0; i < PADDING_NODES; i++)
          database.newVertex("Node").set("name", "P" + i).save();
      });
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }

    // ── 1. The per-spur-node mask ────────────────────────────────────────────

    @Test
    void kShortestPathsStillReturnsKDistinctPathsInAscendingWeight() {
      // The correctness counterweight to the mask change. Collapsing an n x n mask to one row is only valid
      // because every edge it removes leaves the spur node; if that were wrong, Yen's would stop excluding
      // previously-found paths and start returning the same path twice, or returning them out of order.
      final List<Result> paths = drain("""
          MATCH (a:Node {name: 'A0'}), (z:Node {name: 'A20'}) \
          CALL algo.kShortestPaths(a, z, 8, 'LINK', 'w') YIELD path, weight, rank \
          RETURN weight, rank""");

      assertThat(paths).hasSize(8);

      double previous = -1.0;
      for (int i = 0; i < paths.size(); i++) {
        final double weight = ((Number) paths.get(i).getProperty("weight")).doubleValue();
        assertThat(paths.get(i).<Number>getProperty("rank").intValue()).isEqualTo(i + 1);
        assertThat(weight).as("path " + (i + 1) + " must not be cheaper than the one before it").isGreaterThanOrEqualTo(previous);
        previous = weight;
      }
    }

    @Test
    void kShortestPathsFitsABudgetTheWeightMatrixDidNot() {
      // The reservation follows the allocation. Yen's used to hold a nodeCount x nodeCount weight matrix - 501 x 501
      // doubles, ~2 MB, on this graph - which 100 KB refused. Since #9914 it runs on the adjacency lists, and what it
      // holds beside them is node-sized: the search state and the two spur masks, ~9 KB here. The budget has to admit
      // the loaded graph, priced since #6317 at 501 x OLTP_VERTEX_BYTES, and now admits the whole call.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 100_000L);

      assertThat(drain("""
          MATCH (a:Node {name: 'A0'}), (z:Node {name: 'A20'}) \
          CALL algo.kShortestPaths(a, z, 3) YIELD weight \
          RETURN weight""")).hasSize(3);
    }

    @Test
    @Tag("performance")
    void kShortestPathsAllocatesTheWeightMatrixOnceAndNoMaskPerSpurNode() {
      // Measured on the thread's own allocation counter, which no GC and no other test can move.
      //
      // The call below walks ~150 spur nodes over a 501-node graph. Each used to allocate a
      // boolean[501][501] - ~264 KB - for ~40 MB of churn, against a weight matrix of 501 x 501 doubles
      // (~2 MB) allocated once. Measured: 4.4 MB after #6289 against 43.3 MB before, so a bound of six matrices
      // (12 MB) leaves room for Dijkstra's per-spur-node scratch on either side of it and still refuses
      // anything that reallocates a square mask per spur node. Since #9914 there is no matrix at all, and the bound
      // stays the size of six of the matrices it no longer allocates.
      final com.sun.management.ThreadMXBean threads = threadAllocationBean();
      assumeTrue(threads != null, "JVM does not expose per-thread allocation counters");

      database.begin();
      try {
        for (int i = 0; i < 2; i++)
          assertThat(drain(kShortestPaths())).isNotEmpty();

        final long weightMatrixBytes = (long) (CHAIN_HOPS * 2 + 1 + PADDING_NODES) * (CHAIN_HOPS * 2 + 1 + PADDING_NODES) * 8L;
        final long allocated = measure(threads, () -> assertThat(drain(kShortestPaths())).isNotEmpty());

        assertThat(allocated)
            .as("a square mask per spur node is what this bound separates from one weight matrix per call "
                + "(allocated=" + allocated + " bytes, weight matrix=" + weightMatrixBytes + " bytes)")
            .isLessThan(6 * weightMatrixBytes);
      } finally {
        database.rollback();
      }
    }

    // ── 2. The boxed Kruskal index sort ──────────────────────────────────────

    @Test
    void theIndexSortOrdersExactlyAsTheBoxedComparatorSortDid() {
      // The replacement has to be indistinguishable from Arrays.sort(Integer[], comparingDouble) - including
      // where the comparator's answers are least obvious. Duplicates decide stability, NaN and the infinities
      // decide the ordering Double.compare imposes, and -0.0 sorts BELOW 0.0 through Double.compare while
      // comparing equal through `<`, so a hand-rolled comparison on the raw doubles would quietly differ here.
      final double[] awkward = { 0.0, -0.0, Double.NaN, Double.POSITIVE_INFINITY, 1.0, Double.NEGATIVE_INFINITY,
          Double.NaN, 1.0, -0.0, Double.MAX_VALUE, 0.0 };
      assertThat(AbstractAlgoProcedure.sortedIndexesByWeight(awkward, awkward.length))
          .containsExactly(referenceOrder(awkward, awkward.length));

      final Random random = new Random(42);
      for (int count : new int[] { 0, 1, 2, 3, 7, 8, 9, 1000 }) {
        final double[] weights = new double[count];
        for (int i = 0; i < count; i++)
          // A small value range on purpose: ties are what a stable sort has to preserve the index order of.
          weights[i] = random.nextInt(5);
        assertThat(AbstractAlgoProcedure.sortedIndexesByWeight(weights, count))
            .as("order for " + count + " weights")
            .containsExactly(referenceOrder(weights, count));
      }
    }

    @Test
    void mstAndSteinerTreeAreUnchangedByThePrimitiveSort() {
      // Kruskal's picks edges in the order the sort produced, so a different order is a different tree. On the
      // chain-with-bypasses graph the minimum spanning forest is deterministic: every bypass detour (2 x ~5) is
      // cheaper than the direct hop (10), so the MST takes both bypass edges of every hop and no direct hop.
      final List<Result> mst = drain("CALL algo.mst(null, 'w') YIELD source, target, weight RETURN weight");
      assertThat(mst).hasSize(CHAIN_HOPS * 2);
      for (final Result edge : mst)
        assertThat(edge.<Number>getProperty("weight").doubleValue())
            .as("the MST must never take a 10.0 direct hop over the two ~5.0 bypass edges beside it")
            .isLessThan(10.0);

      // The Steiner tree over the two chain ends is their shortest path, and there the choice goes the other way:
      // a direct hop costs 10.0 against the 10.02 the two bypass edges cost together. Kruskal's over the terminal
      // pairs is what picks it, so a mis-ordered index sort shows up here as a different tree - and the two cases
      // together pin the ordering from both sides.
      final List<Result> steiner = drain("""
          MATCH (a:Node {name: 'A0'}), (z:Node {name: 'A20'}) \
          CALL algo.steinerTree([a, z], 'LINK', 'w') YIELD source, target, weight, totalWeight \
          RETURN weight, totalWeight""");
      assertThat(steiner).hasSize(CHAIN_HOPS);
      for (final Result edge : steiner) {
        assertThat(edge.<Number>getProperty("weight").doubleValue()).isEqualTo(10.0);
        assertThat(edge.<Number>getProperty("totalWeight").doubleValue()).isEqualTo(CHAIN_HOPS * 10.0);
      }
    }

    @Test
    @Tag("performance")
    void theIndexSortDoesNotBoxAnIndex() {
      // The point of the replacement, measured directly rather than inferred from a procedure call whose other
      // allocations would drown it. An Integer[] costs the 8-byte reference plus a 16-byte boxed object per
      // entry (the JVM's Integer cache covers only -128..127, which a million-entry index sort leaves at once);
      // two int[] cost 8. The bound below sits between them.
      final com.sun.management.ThreadMXBean threads = threadAllocationBean();
      assumeTrue(threads != null, "JVM does not expose per-thread allocation counters");

      final int count = 1_000_000;
      final double[] weights = new double[count];
      final Random random = new Random(7);
      for (int i = 0; i < count; i++)
        weights[i] = random.nextDouble();

      for (int i = 0; i < 3; i++)
        assertThat(AbstractAlgoProcedure.sortedIndexesByWeight(weights, count)).hasSize(count);

      long allocated = Long.MAX_VALUE;
      for (int round = 0; round < 3; round++)
        allocated = Math.min(allocated,
            measure(threads, () -> assertThat(AbstractAlgoProcedure.sortedIndexesByWeight(weights, count)).hasSize(count)));

      assertThat(allocated / (double) count)
          .as("bytes per index: two int[] is 8, an Integer[] was 24 (measured " + allocated + " bytes for " + count + ")")
          .isLessThan(12.0);
    }

    // ── 3. A refused reservation is not a granted one ────────────────────────

    @Test
    void aRefusedReservationIsNotAddedToTheRunningTotal() {
      // MemoryBudget.reserve() used to add to the total and then check it, so a refusal left the budget
      // recording heap nobody was holding: the next reservation was charged against a component the call had
      // just been refused, and the "on top of the N bytes this call already reserved" in its message quoted a
      // figure that included it. Harmless while the exception aborts the call - which is why it was filed as an
      // invariant rather than as a bug - and a trap for the first caller that survives a refusal.
      final AbstractAlgoProcedure procedure = new AlgoAPSP();
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1000L);
      final AbstractAlgoProcedure.MemoryBudget budget = procedure.newMemoryBudget(database);

      assertThatThrownBy(() -> budget.reserve(2000L, "an oversized component", "detail"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("more than the 1000 bytes allowed")
          .as("nothing was reserved yet, so the message must not claim a prior reservation")
          .hasMessageNotContaining("already reserved");

      // The refused 2000 bytes were never granted, so 500 still fits the 1000-byte budget.
      budget.reserve(500L, "a component that fits", "detail");

      assertThatThrownBy(() -> budget.reserve(600L, "one component too many", "detail"))
          .as("only the granted 500 bytes may be quoted as already reserved")
          .hasMessageContaining("on top of the 500 bytes this call already reserved");
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /** The reference the primitive sort has to reproduce: the boxed comparator sort it replaced. */
    private static int[] referenceOrder(final double[] weights, final int count) {
      final Integer[] boxed = new Integer[count];
      for (int i = 0; i < count; i++)
        boxed[i] = i;
      Arrays.sort(boxed, (a, b) -> Double.compare(weights[a], weights[b]));
      final int[] order = new int[count];
      for (int i = 0; i < count; i++)
        order[i] = boxed[i];
      return order;
    }

    private Stream<Result> kShortestPaths() {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final Object start = database.query("sql", "SELECT FROM Node WHERE name = 'A0'").next().getElement().orElseThrow();
      final Object end = database.query("sql", "SELECT FROM Node WHERE name = 'A20'").next().getElement().orElseThrow();
      return new AlgoKShortestPaths().execute(new Object[] { start, end, 8, "LINK", "w" }, null, context);
    }

    private static List<Result> drain(final Stream<Result> rows) {
      final List<Result> results = new ArrayList<>();
      for (final Iterator<Result> it = rows.iterator(); it.hasNext(); )
        results.add(it.next());
      return results;
    }

    private List<Result> drain(final String query) {
      final ResultSet rs = database.query("opencypher", query);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());
      return results;
    }

    private static long measure(final com.sun.management.ThreadMXBean threads, final Runnable body) {
      final long id = Thread.currentThread().threadId();
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

  /**
   * Regression tests for issue #6265 - {@code algo.influenceMaximization}'s {@code simulateIC} used to allocate
   * and zero-fill a {@code boolean[nodeCount]} and an {@code int[nodeCount]} on every one of its
   * {@code k x nodeCount x simulations} calls. Both buffers are now hoisted to the caller and reused across every
   * call, with {@code simulateIC} resetting only the entries it touched (recorded in {@code queue[0..tail)})
   * instead of reallocating.
   */
  @Nested
  class InfluenceMaximizationBufferReuse {
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
      assertThat(rows.getFirst().<Object>getProperty("nodeId").toString()).contains(vertexRid("H"));
      assertThat(rows.getFirst().<Number>getProperty("marginalGain").doubleValue()).isEqualTo(4.0);

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
      final long id = Thread.currentThread().threadId();
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

  /**
   * Issue #6718: {@code AlgoBetweenness.execute()}'s Brandes algorithm allocated {@code n} fresh
   * {@code ArrayList<Integer>} predecessor lists per source node, {@code n} times over - O(n^2) boxed-list
   * allocations. The fix replaces {@code predecessors}, and the equally boxed {@code stack}/{@code queue}, with
   * primitive {@code int[]} buffers allocated once and reused across every source, reset only over the nodes each
   * source's BFS actually touches.
   * <p>
   * Reusing per-source buffers across sources is exactly the kind of change that can leak stale state from one
   * source's round into the next if a reset is missed anywhere - a bug that would not show up on a small, fully
   * connected graph where every source touches every node anyway. So {@link #matchesNaiveReferenceOnAnIrregularGraph}
   * runs the algorithm on a graph deliberately shaped to touch very different, and sometimes disjoint, subsets of
   * nodes from one source to the next (a cycle, a path and a star bridged into one component, plus a node no other
   * source ever reaches) and compares every score against a naive reference Brandes implementation - the same
   * fresh-allocation-per-source shape the old code used, so it cannot share the bug being tested for - computed
   * independently in this test from the same edge list.
   */
  @Nested
  class BetweennessPrimitiveArrays {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6718-betweenness-primitive-arrays");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");
    }

    @AfterEach
    void teardown() {
      if (database != null)
        database.drop();
    }

    /**
     * Two triangles sharing vertex C ("bowtie"): every cross-triangle pair (A-D, A-E, B-D, B-E) has a unique
     * shortest path of length 2 through C, while every same-triangle pair is a direct edge. Hand-computable exactly:
     * C sits on 4 unordered pairs' unique shortest path; running Brandes from every node as source over a
     * bidirectionally-represented (undirected) graph counts each such pair twice (once per direction), so the raw,
     * non-normalized score for C is 8 and 0 for every other node.
     * <p>
     * Normalizing: {@link AlgoBetweenness} always runs a full, undeduped source iteration (every node as source,
     * directed adjacency) with no built-in undirected halving, so - reciprocal edges or not - the raw maximum a node
     * can reach is the ordered-pair count {@code (n-1)(n-2)}, and the matching normalization is
     * {@code 1/((n-1)(n-2))} (#6943) - not the {@code 2/((n-1)(n-2))} factor, which presumes a raw computation that
     * counts each unordered pair once. With n = 5 that gives {@code 8 * 1/(4*3) = 2/3}.
     */
    @Test
    void bowtieGraphMatchesHandComputedScores() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Node").set("name", "A").save();
        final MutableVertex b = database.newVertex("Node").set("name", "B").save();
        final MutableVertex c = database.newVertex("Node").set("name", "C").save();
        final MutableVertex d = database.newVertex("Node").set("name", "D").save();
        final MutableVertex e = database.newVertex("Node").set("name", "E").save();
        for (final MutableVertex[] edge : new MutableVertex[][] {
            { a, b }, { b, c }, { c, a }, { c, d }, { d, e }, { e, c } }) {
          edge[0].newEdge("LINK", edge[1]).save();
          edge[1].newEdge("LINK", edge[0]).save();
        }
      });

      final Map<String, Double> raw = scoresByName("CALL algo.betweenness({normalized: false}) YIELD node, score "
          + "RETURN node.name AS name, score");
      assertThat(raw.get("C")).isEqualTo(8.0);
      for (final String leaf : List.of("A", "B", "D", "E"))
        assertThat(raw.get(leaf)).isEqualTo(0.0);

      final Map<String, Double> normalized = scoresByName("CALL algo.betweenness({normalized: true}) YIELD node, score "
          + "RETURN node.name AS name, score");
      assertThat(normalized.get("C")).isCloseTo(2.0 / 3.0, offset(1e-9));
      for (final String leaf : List.of("A", "B", "D", "E"))
        assertThat(normalized.get(leaf)).isCloseTo(0.0, offset(1e-9));
    }

    /**
     * Cycle (0-5) bridged to a path (6-10) bridged to a star (center 11, leaves 12-16), plus node 17 which no edge
     * ever touches. 18 sources in total, with wildly different reachable sets and BFS-tree shapes between them -
     * exactly the condition under which a missed reset in the reused per-source buffers would surface as a wrong
     * score, not a crash.
     */
    @Test
    void matchesNaiveReferenceOnAnIrregularGraph() {
      final int n = 18;
      final List<int[]> edges = new ArrayList<>();
      // Cycle: 0-1-2-3-4-5-0
      for (int i = 0; i < 6; i++)
        edges.add(new int[] { i, (i + 1) % 6 });
      // Path: 6-7-8-9-10
      for (int i = 6; i < 10; i++)
        edges.add(new int[] { i, i + 1 });
      // Star: center 11, leaves 12-16
      for (int leaf = 12; leaf <= 16; leaf++)
        edges.add(new int[] { 11, leaf });
      // Bridges: cycle -> path, path -> star. Node 17 stays isolated.
      edges.add(new int[] { 3, 6 });
      edges.add(new int[] { 10, 11 });

      final MutableVertex[] vertices = new MutableVertex[n];
      database.transaction(() -> {
        for (int i = 0; i < n; i++)
          vertices[i] = database.newVertex("Node").set("name", "N" + i).save();
        for (final int[] edge : edges) {
          vertices[edge[0]].newEdge("LINK", vertices[edge[1]]).save();
          vertices[edge[1]].newEdge("LINK", vertices[edge[0]]).save();
        }
      });

      final int[][] adj = buildUndirectedAdjacency(n, edges);
      final double[] expected = naiveBrandes(n, adj);

      final Map<String, Double> actual = scoresByName("CALL algo.betweenness({normalized: false}) YIELD node, score "
          + "RETURN node.name AS name, score");
      assertThat(actual).hasSize(n);
      for (int i = 0; i < n; i++)
        assertThat(actual.get("N" + i)).as("score for N" + i).isCloseTo(expected[i], offset(1e-9));
    }

    private Map<String, Double> scoresByName(final String query) {
      final Map<String, Double> result = new HashMap<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          result.put(row.<String>getProperty("name"), row.<Number>getProperty("score").doubleValue());
        }
      }
      return result;
    }

    private static int[][] buildUndirectedAdjacency(final int n, final List<int[]> edges) {
      final int[] degree = new int[n];
      for (final int[] edge : edges) {
        degree[edge[0]]++;
        degree[edge[1]]++;
      }
      final int[][] adj = new int[n][];
      for (int i = 0; i < n; i++)
        adj[i] = new int[degree[i]];
      final int[] cursor = new int[n];
      for (final int[] edge : edges) {
        adj[edge[0]][cursor[edge[0]]++] = edge[1];
        adj[edge[1]][cursor[edge[1]]++] = edge[0];
      }
      return adj;
    }

    /**
     * Textbook Brandes, unoptimized on purpose: fresh {@code ArrayList}/{@code ArrayDeque}/{@code LinkedList} per
     * source, exactly the shape issue #6718 replaced in the production code - so this reference cannot share a
     * buffer-reuse bug with the code it is checking.
     */
    private static double[] naiveBrandes(final int n, final int[][] adj) {
      final double[] betweenness = new double[n];
      for (int s = 0; s < n; s++) {
        final Deque<Integer> stack = new ArrayDeque<>();
        final List<List<Integer>> predecessors = new ArrayList<>(n);
        for (int i = 0; i < n; i++)
          predecessors.add(new ArrayList<>());
        final double[] sigma = new double[n];
        final int[] dist = new int[n];
        sigma[s] = 1.0;
        for (int i = 0; i < n; i++)
          dist[i] = -1;
        dist[s] = 0;

        final Queue<Integer> queue = new LinkedList<>();
        queue.add(s);
        while (!queue.isEmpty()) {
          final int v = queue.poll();
          stack.push(v);
          for (final int w : adj[v]) {
            if (dist[w] < 0) {
              queue.add(w);
              dist[w] = dist[v] + 1;
            }
            if (dist[w] == dist[v] + 1) {
              sigma[w] += sigma[v];
              predecessors.get(w).add(v);
            }
          }
        }

        final double[] delta = new double[n];
        while (!stack.isEmpty()) {
          final int w = stack.pop();
          for (final int v : predecessors.get(w))
            delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w]);
          if (w != s)
            betweenness[w] += delta[w];
        }
      }
      return betweenness;
    }
  }
}
