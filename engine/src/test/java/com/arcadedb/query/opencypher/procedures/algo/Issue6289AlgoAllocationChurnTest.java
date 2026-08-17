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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6289AlgoAllocationChurnTest {
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
  void kShortestPathsPricesTwoNodeSizedMasksRatherThanASquareOne() {
    // The reservation follows the allocation: what is held beside the weight matrix is two boolean[nodeCount]
    // masks for the whole call, not a nodeCount x nodeCount matrix per spur node. On this graph that is
    // 501 x 501 doubles plus two 501-entry masks.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 100L);

    final int n = CHAIN_HOPS * 2 + 1 + PADDING_NODES;
    assertThatThrownBy(() -> drain("""
        MATCH (a:Node {name: 'A0'}), (z:Node {name: 'A20'}) \
        CALL algo.kShortestPaths(a, z, 3) YIELD weight \
        RETURN weight"""))
        .hasStackTraceContaining("the weight matrix and the spur masks would need")
        .hasStackTraceContaining("a double matrix of " + n + " x " + n + " nodes and two boolean masks of " + n + " nodes")
        .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  @Test
  @Tag("performance")
  void kShortestPathsAllocatesTheWeightMatrixOnceAndNoMaskPerSpurNode() {
    // Measured on the thread's own allocation counter, which no GC and no other test can move.
    //
    // The call below walks ~150 spur nodes over a 501-node graph. Each used to allocate a
    // boolean[501][501] - ~264 KB - for ~40 MB of churn, against a weight matrix of 501 x 501 doubles
    // (~2 MB) allocated once. Measured: 4.4 MB now against 43.3 MB before, so a bound of six matrices
    // (12 MB) leaves room for Dijkstra's per-spur-node scratch on either side of it and still refuses
    // anything that reallocates a square mask per spur node.
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
    final List<Result> mst = drain("CALL algo.mst('w') YIELD source, target, weight RETURN weight");
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
