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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression tests for issue #6296 - {@code algo.apsp} used to materialise every one of its {@code n² - n} rows
 * before the caller saw the first one.
 * <p>
 * #6263 bounded the <em>working set</em> of an {@code algo.*} call: {@code algo.apsp}'s {@code n x n} distance
 * matrix is now reserved against {@code arcadedb.cypher.algoMaxWorkingMemory} before it is allocated. That left
 * the larger allocation of the same call outside every budget. At the 64 MB floor of that setting the matrix
 * check admits about 2890 nodes, and a connected graph of 2890 nodes produced ~8.3 million {@code ResultInternal}
 * rows - each with a three-entry property map, all alive at once, well over a gigabyte - against the 64 MB the
 * budget had just finished enforcing on the matrix beside it. The budget did its job and the call still died, of
 * an allocation its error message would never have mentioned.
 * <p>
 * Nothing required the rows to exist together: Floyd-Warshall completes the matrix before the first row is
 * emitted, so the rows are a pure projection of it. They are now produced lazily, which makes the row-side
 * footprint O(1) and puts the decision of how many to hold where it belongs - with the consumer.
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
class Issue6296AlgoAPSPLazyRowsTest {
  /**
   * A directed cycle: the cheapest shape in edges that makes every ordered pair reachable, so {@code n} edges
   * buy the full {@code n² - n} result set the issue is about.
   */
  private static final int CYCLE_NODES = 200;

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6296-apsp-lazy-rows");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.transaction(() -> {
      final MutableVertex[] nodes = new MutableVertex[CYCLE_NODES];
      for (int i = 0; i < CYCLE_NODES; i++)
        nodes[i] = database.newVertex("Node").set("name", "N" + i).save();
      for (int i = 0; i < CYCLE_NODES; i++)
        nodes[i].newEdge("LINK", nodes[(i + 1) % CYCLE_NODES], true, (Object[]) null).save();
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

  @Test
  void everyReachablePairIsStillReturnedExactlyOnce() {
    // The counterweight to the laziness assertions: a stream that emits fewer rows, or the same row twice, is
    // not a cheaper version of the old behaviour. On a directed cycle every ordered pair (i != j) is reachable,
    // so the row count is exactly n² - n and each (source, target) appears once.
    final Set<String> pairs = new HashSet<>();
    int rows = 0;
    final ResultSet rs = database.query("opencypher",
        "CALL algo.apsp() YIELD source, target, distance RETURN source.name AS src, target.name AS tgt, distance");
    while (rs.hasNext()) {
      final Result result = rs.next();
      rows++;
      assertThat(pairs.add(result.getProperty("src") + "->" + result.getProperty("tgt")))
          .as("each ordered pair must be emitted once")
          .isTrue();
    }

    assertThat(rows).isEqualTo(CYCLE_NODES * CYCLE_NODES - CYCLE_NODES);
  }

  @Test
  void distancesAroundTheCycleAreUnchanged() {
    // Hop count around a directed cycle, which the eager and the lazy form must agree on: N0 -> Nk costs k.
    final ResultSet rs = database.query("opencypher", """
        CALL algo.apsp() YIELD source, target, distance \
        WITH source, target, distance WHERE source.name = 'N0' \
        RETURN target.name AS tgt, distance""");

    int seen = 0;
    while (rs.hasNext()) {
      final Result result = rs.next();
      final int target = Integer.parseInt(((String) result.getProperty("tgt")).substring(1));
      assertThat(((Number) result.getProperty("distance")).doubleValue())
          .as("distance from N0 to N" + target)
          .isEqualTo(target);
      seen++;
    }

    assertThat(seen).isEqualTo(CYCLE_NODES - 1);
  }

  @Test
  void aPartiallyReadStreamStillYieldsTheRestOnDemand() {
    // The shape of the fix without a measurement: one row is readable on its own, and the remaining
    // n² - n - 1 are still produced for a caller that goes on reading. A lazy stream that quietly drops its
    // tail would satisfy "cheap" and fail the procedure's contract.
    database.begin();
    try {
      final Iterator<Result> iterator = apspRows().iterator();

      assertThat(iterator.hasNext()).isTrue();
      final Result first = iterator.next();
      assertThat(first.<Object>getProperty("source")).isNotNull();
      assertThat(first.<Object>getProperty("target")).isNotNull();

      int remaining = 0;
      while (iterator.hasNext()) {
        iterator.next();
        remaining++;
      }
      assertThat(remaining).isEqualTo(CYCLE_NODES * CYCLE_NODES - CYCLE_NODES - 1);
    } finally {
      database.rollback();
    }
  }

  @Test
  void theRowsAreTheSameWhenTheGraphIsCSRBacked() {
    // The one thing making the rows lazy actually moves: `graph.getRID(i)` used to run inside execute() and
    // now runs as the row is read. On the OLTP-backed GraphData that is `vertices.get(i).getIdentity()`, an
    // in-memory field read that cannot care when it happens. On the CSR-backed one it goes through the
    // GraphAnalyticalView's node mapping, which takes a database reference - so that is the path worth
    // pinning, and it is the path the other tests here never take.
    //
    // Deferring it is safe because the stream is drained inside the same query execution that produced it
    // (CallStep keeps the iterator for a read procedure and the pipeline pulls it to completion before the
    // transaction is torn down), which is the contract every other lazily-returning algo.* procedure already
    // relies on - algo.mst, algo.fastrp and the rest have streamed from IntStream since before this change.
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("apsp-view")
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .build();
    try {
      database.begin();
      try {
        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        final Stream<Result> rows = new AlgoAPSP().execute(new Object[0], null, context);

        assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
            .as("the view must actually be used, or this test pins the OLTP path a second time")
            .isEqualTo(true);

        // Same shape as the OLTP runs above: every ordered pair once, at its hop distance round the cycle.
        final Set<String> pairs = new HashSet<>();
        int fromFirstNode = 0;
        for (final Iterator<Result> it = rows.iterator(); it.hasNext(); ) {
          final Result row = it.next();
          final String source = nameOf(row.getProperty("source"));
          final String target = nameOf(row.getProperty("target"));
          assertThat(pairs.add(source + "->" + target)).isTrue();
          if ("N0".equals(source)) {
            assertThat(((Number) row.getProperty("distance")).doubleValue())
                .as("distance from N0 to " + target)
                .isEqualTo(Integer.parseInt(target.substring(1)));
            fromFirstNode++;
          }
        }

        assertThat(pairs).hasSize(CYCLE_NODES * CYCLE_NODES - CYCLE_NODES);
        assertThat(fromFirstNode).isEqualTo(CYCLE_NODES - 1);
      } finally {
        database.rollback();
      }
    } finally {
      view.drop();
    }
  }

  @Test
  @Tag("performance")
  void buildingTheStreamCostsTheMatrixRatherThanTheRows() {
    // The measurement the issue asks for, taken from the thread's own allocation counter so that neither a GC
    // nor another test's work can move it.
    //
    // Two phases of one call: execute(), which runs Floyd-Warshall and hands back the stream, and the drain
    // that reads the rows out of it. Eagerly, the rows were built inside execute() and the drain allocated
    // nothing worth counting - the two numbers below were the other way round. Lazily, execute() pays for the
    // n x n matrix (320 KB here) and the drain pays for the ~40 000 rows, so the drain must dominate.
    final com.sun.management.ThreadMXBean threads = threadAllocationBean();
    assumeTrue(threads != null, "JVM does not expose per-thread allocation counters");

    database.begin();
    try {
      // Warm the whole path past the interpreter, so the first measured round is not measuring class loading.
      for (int i = 0; i < 3; i++)
        assertThat(drain(apspRows())).isPositive();

      final Stream<Result>[] held = newStreamHolder();
      final long buildBytes = measure(threads, () -> held[0] = apspRows());
      final long drainBytes = measure(threads, () -> assertThat(drain(held[0])).isPositive());

      assertThat(drainBytes)
          .as("the rows are the bulk of an apsp call and must be paid for as they are read, not up front "
              + "(build=" + buildBytes + " bytes, drain=" + drainBytes + " bytes)")
          .isGreaterThan(4 * buildBytes);
    } finally {
      database.rollback();
    }
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

  /**
   * Calls the procedure directly rather than through Cypher: what is under measurement is where the rows are
   * built, and the planner and the CALL pipeline would add allocation to both phases that has nothing to do
   * with it. Requires an active transaction, like every other direct use of the graph API.
   */
  private Stream<Result> apspRows() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return new AlgoAPSP().execute(new Object[0], null, context);
  }

  /** The name of the vertex a `source`/`target` row property points at, whichever form the RID arrives in. */
  private String nameOf(final Object rid) {
    return ((RID) rid).asVertex().getString("name");
  }

  private static int drain(final Stream<Result> rows) {
    int count = 0;
    for (final Iterator<Result> it = rows.iterator(); it.hasNext(); ) {
      it.next();
      count++;
    }
    return count;
  }

  @SuppressWarnings("unchecked")
  private static Stream<Result>[] newStreamHolder() {
    return new Stream[1];
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
