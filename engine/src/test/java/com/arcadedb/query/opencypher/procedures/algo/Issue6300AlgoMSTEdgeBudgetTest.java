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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression tests for issue #6300 - {@code algo.mst} was the one dense {@code algo.*} path the #6263
 * working-memory budget did not price.
 * <p>
 * #6263 reserved the dense working set of every procedure that builds one, choosing them by two criteria: sized by
 * a knob, or quadratic in the node count. {@code algo.mst} is neither. It is linear in the <em>edge</em> count -
 * three parallel arrays plus the index sort, 24 bytes per edge - which reads like the graph paying for itself.
 * But linear in the edge count is not small: the edge count is the largest linear dimension a graph has, usually
 * an order of magnitude above the node count, and 100M edges is ~2.4 GB requested with no check and no error
 * naming what asked for it. "Linear" was never the criterion; the criterion is whether the caller can predict a
 * ceiling, and here there is none.
 * <p>
 * Originally the reservation was made as a dedicated counting pass ran, rather than once it had finished, so a
 * refusal could stop before materialising a traversal it would then throw away. Issue #6316 moved edge collection
 * onto {@code GraphData.weightedAdjacency} - the shared helper every other weighted {@code algo.*} procedure
 * already reads edges through - and that dedicated pass went with it. A PR #6714 review round found this had
 * silently dropped the protection: {@code weightedAdjacency} itself reserved nothing, so the (potentially huge)
 * neighbour/weight arrays were fully materialised before {@code algo.mst}'s own {@code reserve()} call ever ran.
 * {@code weightedAdjacency} now reserves incrementally as it builds - a row-header reservation up front, then
 * per-edge-entry reservations as the walk proceeds, mirroring {@code GraphData.adjacency()}'s existing
 * {@code reserveAdjacency} pattern - so the refusal happens inside {@code weightedAdjacency} itself, and for a
 * graph this small it fires at the row-header reservation, before a single edge is read.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6300AlgoMSTEdgeBudgetTest {
  /** Ten edges, so a budget sized for four of them is refused while a default budget is not. */
  private static final int EDGE_COUNT = 10;

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6300-mst-edge-budget");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    // Declared so a Graph Analytical View can materialise "w" as an edge property column (Issue6301's setup
    // note: without a schema-known property the view falls back to edge records, which would leave the
    // columnar reservation path - weightedAdjacencyFromColumns - untested by
    // theWeightedAdjacencyRowHeadersAreRefusedOnTheCSRColumnarPathToo).
    database.getSchema().createEdgeType("LINK").createProperty("w", Type.DOUBLE);

    // A path of EDGE_COUNT + 1 nodes: connected, so the MST spans every node and no edge is redundant.
    database.transaction(() -> {
      final List<MutableVertex> nodes = new ArrayList<>(EDGE_COUNT + 1);
      for (int i = 0; i <= EDGE_COUNT; i++)
        nodes.add(database.newVertex("Node").set("idx", i).save());
      for (int i = 0; i < EDGE_COUNT; i++)
        nodes.get(i).newEdge("LINK", nodes.get(i + 1), true, new Object[] { "w", (double) (i + 1) }).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
          GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  @Test
  void theWeightedAdjacencyRowHeadersAreRefusedWhenTheyDoNotFitTheBudget() {
    // The graph is priced first since #6317 - 11 nodes at OLTP_VERTEX_BYTES is 1056 bytes. weightedAdjacency
    // reserves its row headers next, up front, before reading a single edge: 2 row headers per node (one for
    // neighbors[i], one for weights[i]) at MATRIX_ROW_OVERHEAD_BYTES=32 each, so 11 nodes cost 704 bytes -
    // 1056 + 704 = 1760, over the 1156-byte budget before any edge is even looked at.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1156L);

    assertThatThrownBy(() -> drain("CALL algo.mst('w') YIELD source, target RETURN source, target"))
        .as("the weighted adjacency list is the dense working set of an algo.mst call and must be priced like any other")
        .hasStackTraceContaining("algo.mst(): the weighted adjacency list would need")
        .hasStackTraceContaining("0 edge entries")
        .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  /**
   * PR #6714 review round 13: the tests around this one only exercise {@code weightedAdjacencyFromRecords}
   * (the OLTP fallback) since no Graph Analytical View is built - {@code weightedAdjacencyFromColumns}, the
   * path actually used once a view is ready and the one repeatedly patched for reservation-ordering bugs across
   * this PR's earlier review rounds, was never exercised by a budget-refusal test. {@code CSR_ACCELERATED_VAR}
   * confirms the columnar path was actually taken, not silently the OLTP one the test above already covers.
   * <p>
   * The budget cannot reuse {@link #theWeightedAdjacencyRowHeadersAreRefusedWhenTheyDoNotFitTheBudget}'s 1156:
   * a CSR-backed {@code GraphData} is built straight from the view's node count with no {@code loadVertices}
   * walk, so it never pays that test's 1056-byte OLTP_VERTEX_BYTES graph-loading charge. Only the row headers
   * (2 x 32 bytes/node) are common to both paths - 11 nodes is 704 bytes - so the budget here is sized to that
   * alone.
   */
  @Test
  void theWeightedAdjacencyRowHeadersAreRefusedOnTheCSRColumnarPathToo() {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("mst-budget-csr-view")
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .withEdgeProperties("w")
        .build();
    try {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 500L);

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      database.begin();
      try {
        assertThatThrownBy(() -> new AlgoMST().execute(new Object[] { "w" }, null, context))
            .as("weightedAdjacencyFromColumns must be priced the same way weightedAdjacencyFromRecords is")
            .hasStackTraceContaining("algo.mst(): the weighted adjacency list would need")
            .hasStackTraceContaining("0 edge entries")
            .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
        assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
            .as("the view must actually back the call, or this test pins nothing beyond the OLTP path already covered above")
            .isEqualTo(true);
      } finally {
        database.rollback();
      }
    } finally {
      view.drop();
    }
  }

  @Test
  void theRefusalNamesTheActualEdgeCountOnceTheRowHeadersFit() {
    // A budget wide enough for the graph (1056) plus the row headers (704) - 1760 - but not for the 10 edges'
    // entries on top of that (12 bytes each: an int neighbour id plus a double weight, 120 bytes total) fires the
    // second reservation, which quotes the edge count it actually reached.
    //
    // Issue #6795: before that fix, weightedAdjacencyFromRecords (the OLTP path this 11-node/no-view graph takes)
    // checkpointed only every 1024 nodes or 1_048_576 entries - neither ever reached here - so every edge was
    // already read into the arrays by the time the single post-loop reservation fired, reporting the full 10.
    // The fix makes it also consult capacityFor(), recomputed after every reservation exactly like the columnar
    // path already did: with this budget only 40 bytes remain once the row headers are admitted, so the interval
    // shrinks fast and the walk is refused after just 1 more edge entry - proof the checkpoint now fires mid-walk
    // instead of only once the whole adjacency list is already built.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1800L);

    // "1 edge entries" (not the full EDGE_COUNT): the record path must refuse well before materialising all 10
    // edges, mirroring the columnar path's own capacityFor-scaled checkpoint.
    assertThatThrownBy(() -> drain("CALL algo.mst('w') YIELD source RETURN source"))
        .hasStackTraceContaining("algo.mst(): the weighted adjacency list would need")
        .hasStackTraceContaining("more than the 1800 bytes allowed")
        .hasStackTraceContaining("1 edge entries");
  }

  @Test
  void aBudgetThatFitsLetsTheCallThrough() {
    // The counterweight: the reservation must not refuse a graph it can serve. 11 nodes cost 1056 bytes and
    // 10 edges at 24 bytes is 240, so 4 KB is comfortable, and the MST of a path is the path itself - every
    // edge, at its own weight.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 4096L);

    final List<Object> rows = drain("CALL algo.mst('w') YIELD source, weight, totalWeight RETURN source, weight, totalWeight");
    assertThat(rows).hasSize(EDGE_COUNT);
  }

  @Test
  void aDisabledBudgetPricesNothing() {
    // A negative limit means "no limit" throughout the budget, and it must reach the per-edge capacity too -
    // a capacity computed by dividing a negative limit would refuse every graph instead of accepting them all.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);

    assertThat(drain("CALL algo.mst('w') YIELD source RETURN source")).hasSize(EDGE_COUNT);
  }

  @Test
  void theMinimumSpanningTreeIsUnchanged() {
    // The reservation and the checkpoints must not move the answer. On a path every edge is a bridge, so the MST
    // is the whole path and its total weight is 1 + 2 + ... + EDGE_COUNT.
    final ResultSet rs = database.query("opencypher",
        "CALL algo.mst('w') YIELD weight, totalWeight RETURN weight, totalWeight");
    double sum = 0.0;
    int rows = 0;
    double reportedTotal = -1.0;
    while (rs.hasNext()) {
      final var row = rs.next();
      sum += ((Number) row.getProperty("weight")).doubleValue();
      reportedTotal = ((Number) row.getProperty("totalWeight")).doubleValue();
      rows++;
    }

    assertThat(rows).isEqualTo(EDGE_COUNT);
    assertThat(sum).isEqualTo(EDGE_COUNT * (EDGE_COUNT + 1) / 2.0);
    assertThat(reportedTotal).isEqualTo(sum);
  }

  private List<Object> drain(final String query) {
    final List<Object> rows = new ArrayList<>();
    final ResultSet rs = database.query("opencypher", query);
    while (rs.hasNext())
      rows.add(rs.next());
    return rows;
  }

  /**
   * PR #6714 review round 3 coverage gap: none of the tests above exercise a graph large enough for
   * {@code weightedAdjacency}'s incremental checkpoint (every 1024 nodes, or every {@code ADJACENCY_CHECKPOINT_ENTRIES}
   * entries) to fire mid-walk rather than only at the row-header reservation or the final one. A 3000-node chain
   * is enough: the checkpoint right after node 1023 reserves only the ~1024 edges read so far, and a budget sized
   * to admit the row headers plus that first batch but not the full 2999 edges refuses there - which this test
   * confirms not by timing but by measuring the call's own allocation (issue #6289's approach): if the refusal
   * actually happened partway through the walk rather than after materialising the whole graph, the allocated
   * bytes stay a small fraction (roughly 1024/3000) of what building the full {@code int[][]}/{@code double[][]}
   * pair for all 2999 edges would cost.
   */
  @Test
  @Tag("performance")
  void aLargeGraphIsRefusedPartwayThroughTheWalkNotAfterMaterialisingAllOfIt() {
    final com.sun.management.ThreadMXBean threads = threadAllocationBean();
    assumeTrue(threads != null, "JVM does not expose per-thread allocation counters");

    final int nodeCount = 3000;
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6300-mst-large-graph-refusal");
    if (factory.exists())
      factory.open().drop();
    final Database large = factory.create();
    try {
      large.getSchema().createVertexType("Node");
      large.getSchema().createEdgeType("LINK");
      large.transaction(() -> {
        final List<MutableVertex> nodes = new ArrayList<>(nodeCount);
        for (int i = 0; i < nodeCount; i++)
          nodes.add(large.newVertex("Node").set("idx", i).save());
        for (int i = 0; i < nodeCount - 1; i++)
          nodes.get(i).newEdge("LINK", nodes.get(i + 1), true, new Object[] { "w", (double) (i + 1) }).save();
      });

      // Graph load (96 bytes/node) plus weightedAdjacency's row headers (64 bytes/node) is 480 000 bytes; the
      // checkpoint right after node 1023 adds ~1024 edges x 12 bytes = ~12 288 more. 490 000 sits between the
      // two, so the call refuses right there - after ~1024 of 3000 nodes, not after all of them.
      final long refusalBudget = 490_000L;

      // Warm up both call shapes (JIT, class loading) before either measurement, so neither is biased by being
      // the first invocation.
      large.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, refusalBudget);
      assertThatThrownBy(() -> drain(large, "CALL algo.mst('w') YIELD source RETURN source"))
          .hasStackTraceContaining("algo.mst(): the weighted adjacency list would need");
      large.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);
      assertThat(drain(large, "CALL algo.mst('w') YIELD source RETURN source")).isNotEmpty();

      large.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);
      final long unboundedAllocated = measure(threads, () -> assertThat(drain(large, "CALL algo.mst('w') YIELD source RETURN source")).isNotEmpty());

      large.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, refusalBudget);
      final long refusedAllocated = measure(threads, () -> assertThatThrownBy(() -> drain(large, "CALL algo.mst('w') YIELD source RETURN source"))
          .hasStackTraceContaining("algo.mst(): the weighted adjacency list would need"));

      // A refusal partway through the walk must allocate much less than materialising the whole graph does - if
      // it did not stop mid-walk, the two would be close to equal instead.
      assertThat(refusedAllocated)
          .as("a refusal partway through the walk (refused=" + refusedAllocated + " bytes) must allocate much "
              + "less than materialising the whole graph does (unbounded=" + unboundedAllocated + " bytes)")
          .isLessThan(unboundedAllocated / 2);
    } finally {
      large.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
          GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
      large.drop();
    }
  }

  private static List<Object> drain(final Database db, final String query) {
    final List<Object> rows = new ArrayList<>();
    final ResultSet rs = db.query("opencypher", query);
    while (rs.hasNext())
      rows.add(rs.next());
    return rows;
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
