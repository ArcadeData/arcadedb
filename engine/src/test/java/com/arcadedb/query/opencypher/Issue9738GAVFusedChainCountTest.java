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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Regression test for GitHub issue #9738: with a Graph Analytical View, {@code MATCH (a)-[:K]->(b)-[:K]->(c) RETURN
 * count(*)} was planned as a fused chain feeding a plain aggregation, and the chain built a row for every path and held
 * them all before the aggregation read the first one, so the heap grew with the number of paths and the count ran out
 * of memory on graphs whose view fit in a fraction of it.
 * <p>
 * The count is now folded into the chain's walk, and the rows the chain still produces for everything else are handed
 * over in bounded rounds instead of all at once. The allocation tests run single-threaded (fewer sources than the
 * chain's parallel threshold), so the walk runs on the calling thread and its allocations are that thread's.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9738GAVFusedChainCountTest {
  private static final String DB_PATH  = "./target/databases/issue-9738-gav-fused-count";
  private static final String TWO_HOPS = "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) ";
  private static final String MIXED    = "MATCH (a:P)-[:K]->(b:P)-[:L]->(c:P) ";
  private static final long   MB       = 1024L * 1024L;

  private Database database;
  private int[]    destinations;
  private int      degree;

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void plainCountOverTwoHopsAllocatesNothingPerPath() {
    assumeAllocationCounters();
    // 5,000 sources x 20 x 20 = 2,000,000 paths: one row per path costs well over 100 MB of allocations
    buildGraph(5_000, 20);
    createView();
    final String query = TWO_HOPS + "RETURN count(*) AS n";
    assertThat(plan(query)).contains("GAVFusedChain").contains("fused count(*)");

    // Measured on the first run, before the JIT's escape analysis can hide an allocation per path
    final long before = allocatedBytes();
    final long n = scalar(query, "n");
    final long allocated = allocatedBytes() - before;

    assertThat(n).isEqualTo(twoHopPaths());
    assertThat(allocated).as("bytes allocated counting %d paths", n).isLessThan(16 * MB);
  }

  @Test
  void limitOverTwoHopsStopsWalkingOnceServed() {
    assumeAllocationCounters();
    buildGraph(5_000, 20);
    createView();
    final String query = TWO_HOPS + "RETURN a, c LIMIT 10";
    assertThat(plan(query)).contains("GAVFusedChain");

    final long before = allocatedBytes();
    final int rows = rows(query).size();
    final long allocated = allocatedBytes() - before;

    assertThat(rows).isEqualTo(10);
    assertThat(allocated).as("bytes allocated serving 10 of 2,000,000 paths").isLessThan(16 * MB);
  }

  @Test
  void parallelWalkAnswersEveryShape() {
    // More sources than the chain's parallel threshold, so the walk fans out over the query pool
    buildGraph(10_000, 3);
    final List<String> queries = List.of(//
        TWO_HOPS + "RETURN count(*) AS n",//
        TWO_HOPS + "RETURN count(c) AS n",//
        TWO_HOPS + "RETURN sum(c.pid) AS n",//
        TWO_HOPS + "RETURN count(*) AS n, sum(a.pid) AS s",//
        TWO_HOPS + "WHERE c.pid < 5000 RETURN count(*) AS n",//
        TWO_HOPS + "RETURN a.pid AS a, count(*) AS n ORDER BY a LIMIT 20",//
        TWO_HOPS + "WITH a, c, count(*) AS n RETURN sum(n) AS total, count(*) AS pairs",//
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P)-[:K]->(d:P) RETURN count(*) AS n");
    final List<List<String>> expected = new ArrayList<>();
    for (final String query : queries)
      expected.add(rows(query));

    createView();
    for (int i = 0; i < queries.size(); i++)
      assertThat(rows(queries.get(i))).as(queries.get(i)).isEqualTo(expected.get(i));
    assertThat(scalar(TWO_HOPS + "RETURN count(*) AS n", "n")).isEqualTo(twoHopPaths());

    // Every row of the parallel walk, each one exactly once
    final String allRows = TWO_HOPS + "RETURN a.pid AS a, c.pid AS c";
    assertThat(plan(allRows)).contains("GAVFusedChain");
    long checksum = 0;
    long count = 0;
    try (final ResultSet rs = database.query("opencypher", allRows)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        checksum += 1_000_003L * row.<Integer>getProperty("a") + row.<Integer>getProperty("c");
        ++count;
      }
    }
    assertThat(count).isEqualTo(twoHopPaths());
    assertThat(checksum).isEqualTo(twoHopChecksum());

    final List<String> limited = rows(allRows + " LIMIT 7");
    assertThat(limited).hasSize(7);
  }

  @Test
  void countShapesAnswerAsWithoutTheView() {
    buildSmallGraph();
    final List<String> queries = List.of(//
        TWO_HOPS + "RETURN count(*) AS n",//
        TWO_HOPS + "RETURN count(b) AS n",//
        // count of an expression counts its non-null values: never folded into the walk
        TWO_HOPS + "RETURN count(c.name) AS n",//
        TWO_HOPS + "RETURN count(DISTINCT c) AS n",//
        TWO_HOPS + "WHERE c.pid > 10 RETURN count(*) AS n",//
        "MATCH (a:P {pid: -1})-[:K]->(b:P)-[:K]->(c:P) RETURN count(*) AS n",//
        "MATCH (a:P)-[:K]->(b:Q)-[:K]->(c:P) RETURN count(*) AS n",//
        "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P) RETURN count(*) AS n",//
        "MATCH (a:P)-[r1:K]->(b:P)-[r2:K]->(c:P) RETURN count(*) AS n",//
        "MATCH (a:P)-[:K]->(b:P)<-[:K]-(c:P) RETURN count(*) AS n",//
        // Two edge types: no relationship to keep apart, the hops walk their slices untracked
        MIXED + "RETURN count(*) AS n",//
        MIXED + "WITH b, count(*) AS n RETURN b.pid AS b, n ORDER BY b",//
        MIXED + "RETURN a, b, count(*) AS n ORDER BY a.pid, b.pid",//
        MIXED + "RETURN c, count(*) AS n ORDER BY c.pid",//
        MIXED + "WHERE c.pid > 10 RETURN count(*) AS n",//
        "MATCH (a:P)-[:K]->(b:P)-[:L]->(c:Q) RETURN count(*) AS n",//
        "MATCH (a:P)-[:K]->(b:P)-[:L]-(c:P) RETURN count(*) AS n",//
        "MATCH (a:P)-[:L]->(b:P)<-[:L]-(c:P) RETURN count(*) AS n",//
        "MATCH (a:P)-[:K]->(b:P) MATCH (b)-[:L]->(c:P) RETURN count(*) AS n",//
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P)-[:L]->(d:P) RETURN a, count(*) AS n ORDER BY a.pid",//
        MIXED + "RETURN a.pid AS a, b.pid AS b, c.pid AS c ORDER BY a, b, c",//
        TWO_HOPS + "RETURN a.pid AS a, count(*) AS n ORDER BY a",//
        TWO_HOPS + "RETURN a, count(*) AS n ORDER BY n DESC, a.pid LIMIT 5",//
        TWO_HOPS + "RETURN a.pid AS a, c.pid AS c, count(*) AS n ORDER BY a, c",//
        TWO_HOPS + "WITH count(*) AS n RETURN n",//
        TWO_HOPS + "WITH a, count(*) AS n RETURN a.pid AS a, n ORDER BY a",//
        // A WITH between the chain and the aggregation: the chain's rows are not the aggregation's input
        TWO_HOPS + "WITH a, c WITH a, count(*) AS n RETURN a.pid AS a, n ORDER BY a",//
        TWO_HOPS + "WITH a, b, c WITH count(*) AS n RETURN n",//
        TWO_HOPS + "WITH DISTINCT a, c RETURN count(*) AS n",//
        TWO_HOPS + "RETURN a.pid AS a, b.pid AS b, c.pid AS c ORDER BY a, b, c",//
        // The RETURN's own modifiers still apply to the rows a fused count answers
        TWO_HOPS + "RETURN count(*) AS n SKIP 1",//
        TWO_HOPS + "RETURN count(*) AS n LIMIT 0",//
        TWO_HOPS + "RETURN DISTINCT count(*) AS n",//
        TWO_HOPS + "RETURN DISTINCT a, count(*) AS n ORDER BY n DESC, a.pid LIMIT 3",//
        TWO_HOPS + "RETURN a, count(*) AS n ORDER BY n, a.pid SKIP 2 LIMIT 4",//
        TWO_HOPS + "WITH a, count(*) AS n WHERE n > 20 RETURN a.pid AS a, n ORDER BY a",//
        // A cycle back to the source is never one fused chain: its last hop expands INTO a bound vertex
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(a) RETURN count(*) AS n");
    final List<List<String>> expected = new ArrayList<>();
    for (final String query : queries)
      expected.add(rows(query));

    createView();
    for (int i = 0; i < queries.size(); i++)
      assertThat(rows(queries.get(i))).as(queries.get(i)).isEqualTo(expected.get(i));
    assertThat(plan(TWO_HOPS + "RETURN count(*) AS n")).contains("fused count(*)");
    assertThat(plan(TWO_HOPS + "RETURN count(c.name) AS n")).doesNotContain("fused count");
    // A grouping key that is not a chain variable is a value the chain cannot pack: the aggregation step groups it
    assertThat(plan(TWO_HOPS + "RETURN a.pid AS a, count(*) AS n")).doesNotContain("fused count");
    // Two edge types keep no relationship apart: the fused count walks untracked hops (an ungrouped count of that shape
    // never reaches the chain, the CSR count push-down answers it first)
    assertThat(plan(MIXED + "RETURN a, b, count(*) AS n")).contains("fused count(*) AS n BY a, b")
        .doesNotContain("unique relationships");
    assertThat(plan(TWO_HOPS + "WITH count(*) AS n RETURN n")).contains("fused count(*) AS n");
    assertThat(plan(TWO_HOPS + "RETURN a, count(*) AS n")).contains("fused count(*) AS n BY a");
    // The chain feeds a projection, not the aggregation
    assertThat(plan(TWO_HOPS + "WITH a, c WITH a, count(*) AS n RETURN a, n")).doesNotContain("fused count");

    // Vertices and edges committed after the build reach the fused count too
    database.transaction(() -> {
      final Vertex first = database.query("sql", "SELECT FROM P WHERE pid = 0").next().getVertex().get();
      final MutableVertex x = database.newVertex("P").set("pid", 1000).save();
      final MutableVertex y = database.newVertex("P").set("pid", 1001).save();
      x.newEdge("K", y);
      y.newEdge("K", first);
      first.modify().newEdge("K", x);
    });
    final String count = TWO_HOPS + "RETURN count(*) AS n";
    final List<String> withView = rows(count);
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW v9738");
    assertThat(withView).isEqualTo(rows(count));
  }

  /**
   * A rebuild while the rows stream: the walk read the snapshot the statement pinned, so the rounds after the rebuild go on
   * reading it, and every row is right. It used to fail the rest of the walk with a {@link NeedRetryException} (issue #10415).
   */
  @Test
  void aRebuildWhileTheRowsStreamDoesNotMixNumberings() {
    // 2,000 x 10 x 10 = 200,000 paths: many rounds of the single walker
    buildGraph(2_000, 10);
    createView();
    final String query = TWO_HOPS + "RETURN a, c";
    assertThat(plan(query)).contains("GAVFusedChain");

    long rows = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      ++rows;
      // A rebuild numbers the nodes afresh: the walk's IDs would name other records in the new numbering
      database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW v9738");
      assertThat(GraphAnalyticalViewRegistry.get(database, "v9738").awaitReady(60, TimeUnit.SECONDS)).isTrue();
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<Vertex>getProperty("a").getTypeName()).isEqualTo("P");
        assertThat(row.<Vertex>getProperty("c").getTypeName()).isEqualTo("P");
        ++rows;
      }
    }
    assertThat(rows).isEqualTo(twoHopPaths());
  }

  @Test
  void aViewRestoredOnFirstUseAfterAReopenServesTheWalkWithoutARetry() {
    buildSmallGraph();
    final List<String> queries = List.of(//
        TWO_HOPS + "RETURN a, count(*) AS n ORDER BY a.pid",//
        TWO_HOPS + "RETURN a.pid AS a, c.pid AS c ORDER BY a, c");
    final List<List<String>> expected = new ArrayList<>();
    for (final String query : queries)
      expected.add(rows(query));
    createView();

    // Reopened, the view is restored from disk only when something first asks for it: the walk's numbering is the
    // restored one, never the empty view's
    database.close();
    database = new DatabaseFactory(DB_PATH).open();
    for (int i = 0; i < queries.size(); i++) {
      assertThat(plan(queries.get(i))).as(queries.get(i)).contains("GAVFusedChain");
      assertThat(rows(queries.get(i))).as(queries.get(i)).isEqualTo(expected.get(i));
    }
    assertThat(GraphAnalyticalViewRegistry.get(database, "v9738").isRestoredFromPersistedCsr()).isTrue();
  }

  @Test
  void bucketOfEveryNodeIsTheBucketOfItsRid() {
    buildSmallGraph();
    createView();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v9738");
    final int built = view.getNodeIdUpperBound();
    // Appended past the built mapping by the synchronous overlay
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newVertex(i % 2 == 0 ? "Q" : "P").set("pid", 700 + i).save();
    });
    final int bound = view.getNodeIdUpperBound();
    assertThat(bound).isGreaterThan(built);

    assertThat(view.getBucketId(-1)).isEqualTo(-1);
    // Past the bound too: no RID, no bucket
    for (int nodeId = 0; nodeId < bound + 3; nodeId++) {
      final RID rid = view.getRID(nodeId);
      assertThat(view.getBucketId(nodeId)).as("node %d", nodeId).isEqualTo(rid != null ? rid.getBucketId() : -1);
    }
  }

  @Test
  void sourcesTheViewDoesNotMapAreCountedOnTheirRecords() {
    buildSmallGraph();
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW v9738 VERTEX TYPES (P, Q) EDGE TYPES (K, L) PROPERTIES (pid) "
        + "UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v9738");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    // Kept in use while stale: the vertices created after the build are sources the view does not map
    view.setUseWhenStale(true);
    database.transaction(() -> {
      final List<Vertex> old = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM P WHERE pid < 6 ORDER BY pid")) {
        while (rs.hasNext())
          old.add(rs.next().getVertex().get());
      }
      MutableVertex previous = null;
      for (int i = 0; i < 4; i++) {
        final MutableVertex x = database.newVertex("P").set("pid", 500 + i).save();
        x.newEdge("K", old.get(i));
        x.newEdge("K", old.get(i + 1));
        if (previous != null)
          x.newEdge("K", previous);
        previous = x;
      }
    });

    final List<String> queries = List.of(//
        TWO_HOPS + "RETURN count(*) AS n",//
        TWO_HOPS + "WHERE c.pid > 10 RETURN count(*) AS n",//
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P)-[:K]->(d:P) RETURN count(*) AS n",//
        "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P)-[:K]->(d:P) WHERE d.pid < 30 RETURN count(*) AS n",//
        TWO_HOPS + "RETURN a.pid AS a, count(*) AS n ORDER BY a",//
        TWO_HOPS + "RETURN a.pid AS a, c.pid AS c ORDER BY a, c");
    assertThat(plan(queries.get(0))).contains("fused count(*)");
    final List<List<String>> withView = new ArrayList<>();
    for (final String query : queries)
      withView.add(rows(query));

    database.command("sql", "DROP GRAPH ANALYTICAL VIEW v9738");
    for (int i = 0; i < queries.size(); i++)
      assertThat(withView.get(i)).as(queries.get(i)).isEqualTo(rows(queries.get(i)));
  }

  private void buildGraph(final int vertices, final int degree) {
    createDatabase();
    this.degree = degree;
    final Random random = new Random(9738);
    destinations = new int[vertices * degree];
    for (int i = 0; i < vertices; i++)
      for (int k = 0; k < degree; k++)
        destinations[i * degree + k] = (i + 1 + random.nextInt(vertices - 1)) % vertices;

    final RID[] rids = new RID[vertices];
    try (final GraphBatch batch = database.batch().withBatchSize(50_000).withCommitEvery(50_000).build()) {
      final Object[][] properties = new Object[vertices][];
      for (int i = 0; i < vertices; i++)
        properties[i] = new Object[] { "pid", i };
      System.arraycopy(batch.createVertices("P", properties), 0, rids, 0, vertices);
      for (int i = 0; i < vertices; i++)
        for (int k = 0; k < degree; k++)
          batch.newEdge(rids[i], "K", rids[destinations[i * degree + k]]);
    }
  }

  /** A graph with self-loops, parallel edges, a second label and a property that is often missing. */
  private void buildSmallGraph() {
    createDatabase();
    final Random random = new Random(9738);
    database.transaction(() -> {
      final MutableVertex[] vertices = new MutableVertex[60];
      for (int i = 0; i < vertices.length; i++) {
        vertices[i] = database.newVertex(i % 7 == 0 ? "Q" : "P").set("pid", i);
        if (i % 3 != 0)
          vertices[i].set("name", "v" + i);
        vertices[i].save();
      }
      for (int i = 0; i < vertices.length; i++)
        for (int k = 0; k < 4; k++)
          vertices[i].newEdge("K", vertices[random.nextInt(vertices.length)]);
      vertices[5].newEdge("K", vertices[5]);
      vertices[6].newEdge("K", vertices[8]);
      vertices[6].newEdge("K", vertices[8]);
      // A second type, so a chain mixing the two has no relationship to keep apart: its last hop is counted whole
      for (int i = 0; i < vertices.length; i += 2)
        for (int k = 0; k < 3; k++)
          vertices[i].newEdge("L", vertices[random.nextInt(vertices.length)]);
      vertices[8].newEdge("L", vertices[8]);
      vertices[8].newEdge("L", vertices[9]);
      vertices[8].newEdge("L", vertices[9]);
    });
  }

  private void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("P");
    database.getSchema().createVertexType("Q").addSuperType("P");
    database.getSchema().createEdgeType("K");
    database.getSchema().createEdgeType("L");
  }

  private void createView() {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW v9738 VERTEX TYPES (P, Q) EDGE TYPES (K, L) PROPERTIES (pid) "
        + "UPDATE MODE SYNCHRONOUS");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v9738");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
  }

  private long twoHopPaths() {
    // Every edge a -> b continues through each of the degree edges leaving b
    return (long) destinations.length * degree;
  }

  private long twoHopChecksum() {
    long checksum = 0;
    for (int e = 0; e < destinations.length; e++) {
      final int a = e / degree;
      final int b = destinations[e];
      for (int k = 0; k < degree; k++)
        checksum += 1_000_003L * a + destinations[b * degree + k];
    }
    return checksum;
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
    }
  }

  private long scalar(final String query, final String column) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty(column)).longValue();
    }
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder sb = new StringBuilder();
        for (final String name : row.getPropertyNames()) {
          final Object value = row.getProperty(name);
          sb.append(name).append('=').append(value instanceof Vertex v ? v.getIdentity() : value).append(';');
        }
        rows.add(sb.toString());
      }
    }
    return rows;
  }

  /** The allocation tests read the per-thread allocation counter of HotSpot-derived JVMs; elsewhere they are skipped. */
  private static void assumeAllocationCounters() {
    assumeTrue(ManagementFactory.getThreadMXBean() instanceof ThreadMXBean bean && bean.isThreadAllocatedMemorySupported()
        && bean.isThreadAllocatedMemoryEnabled(), "per-thread allocation counters not available on this JVM");
  }

  private static long allocatedBytes() {
    final ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    return bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
  }
}
