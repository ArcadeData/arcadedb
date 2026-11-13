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
package com.arcadedb.graph.olap;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.graph.DenseNodeIdProvider;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex.DIRECTION;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests of GavViewConcurrency, one nested class per reported case.
 */
class GavViewConcurrencyTest {
  /**
   * Regression for #10189: an {@code ASYNCHRONOUS} Graph Analytical View answered every relevant commit with a full rebuild and reported
   * BUILDING while it ran, so it served no query for the whole rebuild, whatever the size of the change and even with
   * {@code gavUseWhenStale}. Under a steady stream of small commits almost no read used it. A commit now reaches the view as the
   * change it made, merged into the overlay off the committing thread as {@code SYNCHRONOUS} merges it on it; until the merge lands
   * the view is STALE, which a {@code gavUseWhenStale} view keeps serving, and a full rebuild happens only to compact the overlay,
   * in the background, while the view serves.
   * <p>
   * Full rebuilds are held off with the shared build permits, so nothing here depends on how fast a build is.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AsyncViewServesWhileUpdating {
    private static final String DB_PATH  = "./target/databases/test-issue-10189";
    private static final String QUERY    = "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) RETURN count(*) AS n";
    private static final int    VERTICES = 20;

    private Database database;
    private RID[]    rids;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("P");
      database.getSchema().createEdgeType("K");
      rids = new RID[VERTICES];
      database.transaction(() -> {
        final MutableVertex[] vertices = new MutableVertex[VERTICES];
        for (int i = 0; i < VERTICES; i++) {
          vertices[i] = database.newVertex("P").set("pid", i).save();
          rids[i] = vertices[i].getIdentity();
        }
        // a ring where each vertex links the next two: out-degree 2 and in-degree 2 everywhere
        for (int i = 0; i < VERTICES; i++) {
          vertices[i].newEdge("K", vertices[(i + 1) % VERTICES]);
          vertices[i].newEdge("K", vertices[(i + 2) % VERTICES]);
        }
      });
    }

    @AfterEach
    void teardown() {
      if (database != null && database.isOpen())
        database.drop();
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
    }

    @Test
    void aCommitReachesTheViewWithoutAFullRebuild() {
      final GraphAnalyticalView view = createView();
      assertThat(count()).isEqualTo(VERTICES * 4L);

      GraphAnalyticalView.acquireAllBuildPermitsForTest();
      try {
        // vertex 0 gains an out-edge to vertex 10: 2 more 2-hop paths starting with it (10 has out-degree 2), 2 more ending with it
        addEdge(0, 10);

        assertThat(view.awaitReady(30, TimeUnit.SECONDS)).as("no full rebuild is needed to take the commit in").isTrue();
        assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
        assertThat(plan()).as("served by the view right after the commit").contains("GAV");
        assertThat(count()).isEqualTo(VERTICES * 4L + 4);

        // a vertex and its edges, then a deletion
        database.transaction(() -> {
          final MutableVertex v = database.newVertex("P").set("pid", VERTICES).save();
          v.newEdge("K", rids[0].asVertex());
          rids[5].asVertex().modify().newEdge("K", v);
        });
        assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
        assertThat(view.getNodeCount()).isEqualTo(VERTICES + 1);
        assertThat(count()).isEqualTo(countOnRecords());

        database.transaction(() -> rids[3].asVertex().delete());
        assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
        assertThat(plan()).contains("GAV");
        assertThat(count()).isEqualTo(countOnRecords());
      } finally {
        GraphAnalyticalView.releaseAllBuildPermitsForTest();
      }
    }

    @Test
    void untilTheChangeIsMergedTheViewIsStaleAndServesOnlyWhenAllowedTo() throws Exception {
      final GraphAnalyticalView view = createView();
      final long before = count();

      // the merge runs off the committing thread under the view's monitor: holding it keeps the commit unmerged
      synchronized (view) {
        addEdge(0, 10);

        assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);
        assertThat(view.isStale()).isTrue();

        view.setUseWhenStale(false);
        assertThat(view.isReady()).as("a view behind a commit is not used by default").isFalse();
        // read-your-writes: the committing thread's next query sees its own change
        assertThat(count()).isEqualTo(before + 4);
        assertThat(plan()).doesNotContain("GAV");

        view.setUseWhenStale(true);
        assertThat(view.isReady()).as("gavUseWhenStale keeps serving the previous snapshot").isTrue();
        assertThat(plan()).contains("GAV");
        assertThat(count()).as("the snapshot before the commit").isEqualTo(before);
      }

      view.setUseWhenStale(null);
      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
      assertThat(view.isStale()).isFalse();
      assertThat(plan()).contains("GAV");
      assertThat(count()).isEqualTo(before + 4);
    }

    @Test
    void aCompactionRebuildsInTheBackgroundWhileTheViewServes() {
      final GraphAnalyticalView view = createView();
      view.setCompactionThreshold(3);

      GraphAnalyticalView.acquireAllBuildPermitsForTest();
      try {
        // past the threshold: the compaction is dispatched and waits for a permit, the view keeps serving its overlay
        for (int i = 0; i < 5; i++)
          addEdge(i, (i + 7) % VERTICES);
        assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
        assertThat(plan()).contains("GAV");
        assertThat(count()).isEqualTo(countOnRecords());
      } finally {
        GraphAnalyticalView.releaseAllBuildPermitsForTest();
      }

      addEdge(9, 3);
      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      // The released compaction swaps the CSR in: a query that walked the view across the swap is told to run again
      final long expected = countOnRecords();
      Awaitility.await("the compacted view answers").atMost(30, TimeUnit.SECONDS).ignoreExceptionsInstanceOf(NeedRetryException.class)
          .until(() -> count() == expected);
    }

    @Test
    void switchingToSynchronousMergesWhatIsPending() {
      final GraphAnalyticalView view = createView();
      final long before = count();

      synchronized (view) {
        addEdge(0, 10);
        assertThat(view.isStale()).isTrue();
      }
      view.setUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS);
      assertThat(view.isStale()).isFalse();
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
      assertThat(count()).isEqualTo(before + 4);
    }

    @Test
    void concurrentCommitsAllReachTheView() throws Exception {
      final GraphAnalyticalView view = createView();

      final int threads = 4;
      final int commitsPerThread = 25;
      final ExecutorService pool = Executors.newFixedThreadPool(threads);
      try {
        final List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
          final int offset = t;
          futures.add(pool.submit(() -> {
            for (int i = 0; i < commitsPerThread; i++) {
              final int from = (offset * commitsPerThread + i) % VERTICES;
              database.transaction(() -> {
                final MutableVertex v = database.newVertex("P").set("pid", -1).save();
                rids[from].asVertex().modify().newEdge("K", v);
                v.newEdge("K", rids[(from + 3) % VERTICES].asVertex());
              }, false, 10);
            }
          }));
        }
        for (final Future<?> future : futures)
          future.get(60, TimeUnit.SECONDS);
      } finally {
        pool.shutdownNow();
      }

      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      assertThat(view.getNodeCount()).isEqualTo(VERTICES + threads * commitsPerThread);
      assertThat(plan()).contains("GAV");
      assertThat(count()).isEqualTo(countOnRecords());
    }

    @Test
    void aQueueTheMergeCannotKeepUpWithRebuildsTheView() {
      final GraphAnalyticalView view = createView();
      view.setMaxQueuedAsyncDeltasForTest(2);

      // the merge waits on the view's monitor, so the third commit finds the queue full
      synchronized (view) {
        for (int i = 0; i < 3; i++)
          addEdge(i, (i + 9) % VERTICES);
        assertThat(view.isStale()).as("the full queue keeps the view stale until the merge task drops it").isTrue();
      }

      final long expected = countOnRecords();
      Awaitility.await("the rebuilt view answers").atMost(30, TimeUnit.SECONDS).ignoreExceptionsInstanceOf(NeedRetryException.class)
          .until(() -> view.awaitReady(1, TimeUnit.SECONDS) && plan().contains("GAV") && count() == expected);
    }

    @Test
    void concurrentCommitsOverflowingTheQueueRebuildOnce() throws Exception {
      final GraphAnalyticalView view = createView();
      view.setMaxQueuedAsyncDeltasForTest(3);

      final ExecutorService pool = Executors.newFixedThreadPool(4);
      try {
        // the merge waits on the view's monitor while 4 threads commit 40 changes: the queue overflows many times over
        synchronized (view) {
          final List<Future<?>> futures = new ArrayList<>();
          for (int t = 0; t < 4; t++) {
            final int offset = t;
            futures.add(pool.submit(() -> {
              for (int i = 0; i < 10; i++) {
                final int from = (offset * 10 + i) % VERTICES;
                database.transaction(() -> rids[from].asVertex().modify().newEdge("K", rids[(from + 7) % VERTICES].asVertex()),
                    false, 10);
              }
            }));
          }
          for (final Future<?> future : futures)
            future.get(60, TimeUnit.SECONDS);
          assertThat(view.isStale()).isTrue();
        }
      } finally {
        pool.shutdownNow();
      }

      final long expected = countOnRecords();
      Awaitility.await("the rebuilt view answers").atMost(30, TimeUnit.SECONDS).ignoreExceptionsInstanceOf(NeedRetryException.class)
          .until(() -> view.awaitReady(1, TimeUnit.SECONDS) && plan().contains("GAV") && count() == expected);
    }

    @Test
    void commitsRacingFullRebuildsAllReachTheView() throws Exception {
      final GraphAnalyticalView view = createView();

      final ExecutorService pool = Executors.newFixedThreadPool(3);
      try {
        final List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < 2; t++) {
          final int offset = t;
          futures.add(pool.submit(() -> {
            for (int i = 0; i < 40; i++) {
              final int from = (offset * 40 + i) % VERTICES;
              database.transaction(() -> rids[from].asVertex().modify().newEdge("K", rids[(from + 5) % VERTICES].asVertex()),
                  false, 10);
            }
          }));
        }
        futures.add(pool.submit(() -> {
          for (int i = 0; i < 5; i++)
            view.buildAsync();
        }));
        for (final Future<?> future : futures)
          future.get(60, TimeUnit.SECONDS);
      } finally {
        pool.shutdownNow();
      }

      final long expected = countOnRecords();
      Awaitility.await("the view answers what the records hold").atMost(30, TimeUnit.SECONDS)
          .ignoreExceptionsInstanceOf(NeedRetryException.class)
          .until(() -> view.awaitReady(1, TimeUnit.SECONDS) && count() == expected);
      assertThat(plan()).contains("GAV");
    }

    /**
     * A commit whose callback reaches a full build's watch just after it closed is refused by it and queued for the merge, without
     * the view's monitor the publication holds. The publication dropped every queued change after closing the watch, taking it for
     * one its scan had read, so a change committed after the scan, in neither the CSR nor the watch, was lost while the view
     * reported READY (the flaky {@code GavViewQueryTest.ViewVertexTypeAttachedAfterBuild} and {@code GavBuildTest.GAVBuildRacesCommits}).
     * The hook commits from inside that window, on another thread, and waits for its callback to have run.
     */
    @Test
    void aCommitRefusedByTheWatchOfAPublishingBuildReachesTheView() throws Exception {
      final GraphAnalyticalView view = createView();
      final AtomicBoolean fired = new AtomicBoolean();
      final AtomicBoolean committed = new AtomicBoolean();
      view.setAfterBuildWatchClosedForTest(() -> {
        if (!fired.compareAndSet(false, true))
          return;
        final Thread writer = new Thread(() -> {
          addEdge(0, 10);
          committed.set(true);
        });
        writer.start();
        try {
          writer.join(TimeUnit.SECONDS.toMillis(30));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      try {
        view.buildAsync();
        assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      } finally {
        view.setAfterBuildWatchClosedForTest(null);
      }
      assertThat(fired.get()).as("the build published through the hook").isTrue();
      assertThat(committed.get()).as("the commit ran inside the publication window").isTrue();

      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
      assertThat(plan()).contains("GAV");
      assertThat(count()).as("the view holds the edge committed while the build published").isEqualTo(countOnRecords());
    }

    @Test
    void aViewClosedRightAfterACommitReopensWithIt() {
      createView();
      addEdge(0, 10);
      final long expected = countOnRecords();

      database.close();
      database = new DatabaseFactory(DB_PATH).open();
      final GraphAnalyticalView reopened = GraphAnalyticalViewRegistry.get(database, "g");
      assertThat(reopened.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      assertThat(plan()).contains("GAV");
      assertThat(count()).isEqualTo(expected);
    }

    private GraphAnalyticalView createView() {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (P) EDGE TYPES (K) UPDATE MODE ASYNCHRONOUS");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      return view;
    }

    private void addEdge(final int from, final int to) {
      database.transaction(() -> rids[from].asVertex().modify().newEdge("K", rids[to].asVertex()));
    }

    private String plan() {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + QUERY)) {
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    private long count() {
      try (final ResultSet rs = database.query("opencypher", QUERY)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }

    /** The 2-hop count walked on the records with the vertex API, which no view answers. */
    private long countOnRecords() {
      long paths = 0;
      for (final Iterator<Record> it = database.iterateType("P", true); it.hasNext(); )
        for (final Vertex first : it.next().asVertex().getVertices(Vertex.DIRECTION.OUT, "K"))
          for (final Vertex ignored : first.getVertices(Vertex.DIRECTION.OUT, "K"))
            ++paths;
      return paths;
    }
  }

  /**
   * Regression for #10249: the changes of two transactions that conflict reach a Graph Analytical View in the order the transactions
   * committed, even when the thread of the second one gets to the view first.
   * <p>
   * The first transaction deletes an edge and is held after its commit, before it reports to the view; the second one creates an
   * edge in the slot the first freed (the same RID) and commits meanwhile. Reported in the opposite order, the creation replaces the
   * deleted edge in the view and the deletion that follows withdraws the new one. The randomized version of the scenario is
   * {@link GavViewConcurrencyTest.ConcurrentEdgeDeletes}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CommitOrder extends TestHelper {
    private static final String COUNT_EDGES = "MATCH (a:V)-[:E]->(b:V) RETURN count(*) AS n";

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void aCommitReachesTheViewAfterTheCommitsOfTheTransactionsItFollows(final String mode) throws Exception {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE E BUCKETS 1");
      final RID[] vertices = new RID[3];
      final RID[] edge = new RID[1];
      database.transaction(() -> {
        for (int i = 0; i < vertices.length; i++)
          vertices[i] = database.newVertex("V").save().getIdentity();
        edge[0] = vertices[0].asVertex().newEdge("E", vertices[1].asVertex()).save().getIdentity();
      });
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (V) EDGE TYPES (E) UPDATE MODE " + mode);
      awaitReady("g");

      final CountDownLatch deleteCommitted = new CountDownLatch(1);
      final CountDownLatch releaseDelete = new CountDownLatch(1);
      final ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        // A callback of the first transaction's own, registered before the view's, holds its thread after the commit, once the
        // locks are released and before the view hears of it: the window the commits race in
        final Future<?> deleter = executor.submit(() -> {
          database.begin();
          ((DatabaseInternal) database).getTransaction().addAfterCommitCallback(() -> {
            deleteCommitted.countDown();
            try {
              releaseDelete.await(1, TimeUnit.MINUTES);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
          edge[0].asEdge().delete();
          database.commit();
        });
        assertThat(deleteCommitted.await(1, TimeUnit.MINUTES)).as("the deletion committed").isTrue();

        // The slot the first transaction freed is the first one the bucket hands out again: one of the new edges has the RID of the
        // deleted one
        final Future<Boolean> creator = executor.submit(() -> {
          final boolean[] reused = new boolean[1];
          database.transaction(() -> {
            for (int i = 0; i < 3; i++)
              reused[0] |= vertices[0].asVertex().newEdge("E", vertices[2].asVertex()).save().getIdentity().equals(edge[0]);
          });
          return reused[0];
        });
        // It is committed, but its change is not given to the view ahead of the deletion's, which came first. A stall can only make
        // this wait longer
        Thread.sleep(300);
        assertThat(creator.isDone()).as("the commit that follows reaches the view after the one it follows").isFalse();

        releaseDelete.countDown();
        deleter.get(1, TimeUnit.MINUTES);
        assertThat(creator.get(1, TimeUnit.MINUTES)).as("an edge got the RID the deleted one freed").isTrue();
      } finally {
        releaseDelete.countDown();
        executor.shutdownNow();
      }

      awaitReady("g");
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + COUNT_EDGES)) {
        assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).as("the count is served by the view").contains("GAV");
      }
      final long edges;
      try (final ResultSet rs = database.query("sql", "SELECT count() AS n FROM E")) {
        edges = rs.next().<Number>getProperty("n").longValue();
      }
      assertThat(edges).isEqualTo(3);
      try (final ResultSet rs = database.query("opencypher", COUNT_EDGES)) {
        assertThat(rs.next().<Number>getProperty("n").longValue()).as("edges the view counts").isEqualTo(3);
      }
    }

    private void awaitReady(final String viewName) throws InterruptedException {
      final long deadline = System.currentTimeMillis() + 60_000;
      while (System.currentTimeMillis() < deadline) {
        try (final ResultSet rs = database.query("sql", "SELECT status FROM schema:graphAnalyticalViews WHERE name = ?", viewName)) {
          if (rs.hasNext() && "READY".equals(String.valueOf(rs.next().<Object>getProperty("status"))))
            return;
        }
        Thread.sleep(10);
      }
      throw new AssertionError("The view " + viewName + " is not READY");
    }
  }

  /**
   * Regression for #10249: when several threads commit at the same time and some of the commits delete edges, a Graph Analytical View
   * ended up a few edges short - once the writers were done and the view READY, the counts it served were lower than the same
   * queries without the view. A reopened database and a view built from scratch gave the right counts, so only the live view
   * was wrong.
   * <p>
   * Four threads each commit 250 transactions of 1 to 6 operations: edge inserts, deletes of edges the thread created itself
   * and updates of a vertex property. Edges of one thread end on vertices another thread's edges also end on, so the commits
   * of different threads share the adjacency of those vertices. The deterministic version of the scenario is
   * {@link GavViewConcurrencyTest.CommitOrder}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("slow")
  class ConcurrentEdgeDeletes extends TestHelper {
    private static final int      THREADS            = 4;
    private static final int      COMMITS_PER_THREAD = 250;
    private static final int      BASE_VERTICES      = 100;
    private static final String[] QUERIES            = {
        "MATCH (a:V)-[:E]->(b:V) RETURN count(*) AS n",
        "MATCH (a:V)-[:E]->(b:V) WHERE b.flag = 1 RETURN count(*) AS n",
        "MATCH (a:V)-[:E]->(b:V)-[:E]->(c:V) RETURN count(*) AS n",
        "MATCH (a:W)-[:E]->(b:W) RETURN count(*) AS n",
        "MATCH (a:V) WHERE a.flag = 1 RETURN count(*) AS n",
        "MATCH (a:V)<-[:E]-(b:V) WHERE a.flag = 0 AND b.flag = 1 RETURN count(*) AS n" };

    static Stream<Arguments> modesAndSeeds() {
      final List<Arguments> arguments = new ArrayList<>();
      for (final String mode : new String[] { "SYNCHRONOUS", "ASYNCHRONOUS" })
        for (long seed = 4; seed < 16; seed++)
          arguments.add(Arguments.of(mode, seed));
      return arguments.stream();
    }

    @ParameterizedTest(name = "{0} seed {1}")
    @MethodSource("modesAndSeeds")
    void liveViewAnswersLikeNoViewAfterConcurrentInsertsAndEdgeDeletes(final String mode, final long seed) throws Exception {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE VERTEX TYPE W EXTENDS V");
      database.command("sql", "CREATE EDGE TYPE E");
      database.command("sql", "CREATE PROPERTY V.flag INTEGER");

      final List<RID> base = Collections.synchronizedList(new ArrayList<>());
      final Random baseRandom = new Random(seed);
      database.transaction(() -> {
        for (int i = 0; i < BASE_VERTICES; i++)
          base.add(database.newVertex(i % 3 == 0 ? "W" : "V").set("flag", baseRandom.nextInt(2)).save().getIdentity());
      });

      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (V, W) EDGE TYPES (E) PROPERTIES (flag) UPDATE MODE " + mode);
      awaitReady("g");

      final AtomicInteger gaveUp = new AtomicInteger();
      final ExecutorService executor = Executors.newFixedThreadPool(THREADS);
      try {
        final List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
          final int thread = t;
          futures.add(executor.submit(() -> writer(thread, seed, base, gaveUp)));
        }
        for (final Future<?> future : futures)
          future.get(5, TimeUnit.MINUTES);
      } finally {
        executor.shutdownNow();
      }
      assertThat(gaveUp.get()).as("transactions that gave up on a conflict").isZero();

      awaitReady("g");
      int served = 0;
      for (final String query : QUERIES)
        if (servedByView(query))
          served++;
      assertThat(served).as("queries served by the view, or the comparison below would be vacuous").isPositive();

      final List<String> withView = answers();
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
      final List<String> withoutView = answers();
      assertThat(withView).as("the live view's answers against the answers without the view").isEqualTo(withoutView);

      // a view built from scratch over the same data must agree too
      database.command("sql",
          "CREATE GRAPH ANALYTICAL VIEW g2 VERTEX TYPES (V, W) EDGE TYPES (E) PROPERTIES (flag) UPDATE MODE SYNCHRONOUS");
      awaitReady("g2");
      assertThat(answers()).as("a fresh view's answers").isEqualTo(withoutView);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g2");
    }

    private Void writer(final int thread, final long seed, final List<RID> base, final AtomicInteger gaveUp) {
      final Random random = new Random(seed * 31 + thread);
      final List<RID> myVertices = new ArrayList<>();
      final List<RID> myEdges = new ArrayList<>();
      for (int commit = 0; commit < COMMITS_PER_THREAD; commit++) {
        for (int attempt = 0; attempt < 20; attempt++) {
          final List<RID> newEdges = new ArrayList<>();
          final List<RID> deletedEdges = new ArrayList<>();
          try {
            database.begin();
            final int operations = 1 + random.nextInt(6);
            for (int o = 0; o < operations; o++) {
              final int code = 3 + random.nextInt(5);
              if (code < 7) {
                final RID from = !myVertices.isEmpty() && random.nextBoolean() ?
                    myVertices.get(random.nextInt(myVertices.size())) :
                    base.get(thread * 25 + random.nextInt(25));
                final RID to = base.get(random.nextInt(base.size()));
                newEdges.add(from.asVertex().newEdge("E", to.asVertex()).save().getIdentity());
              } else if (!myEdges.isEmpty()) {
                final RID edge = myEdges.get(random.nextInt(myEdges.size()));
                if (!deletedEdges.contains(edge)) {
                  deletedEdges.add(edge);
                  edge.asEdge().delete();
                }
              } else
                base.get(thread * 25 + random.nextInt(25)).asVertex().modify().set("flag", random.nextInt(2)).save();
            }
            database.commit();
            myEdges.addAll(newEdges);
            myEdges.removeAll(deletedEdges);
            break;
          } catch (final Exception e) {
            if (database.isTransactionActive())
              database.rollback();
            if (attempt == 19)
              gaveUp.incrementAndGet();
          }
        }
      }
      return null;
    }

    private List<String> answers() {
      final List<String> answers = new ArrayList<>(QUERIES.length);
      for (final String query : QUERIES)
        try (final ResultSet rs = database.query("opencypher", query)) {
          answers.add(String.valueOf(rs.next().<Object>getProperty("n")));
        }
      return answers;
    }

    private boolean servedByView(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().get().prettyPrint(0, 2).contains("GAV");
      }
    }

    private void awaitReady(final String viewName) throws InterruptedException {
      final long deadline = System.currentTimeMillis() + 60_000;
      while (System.currentTimeMillis() < deadline) {
        try (final ResultSet rs = database.query("sql", "SELECT status FROM schema:graphAnalyticalViews WHERE name = ?", viewName)) {
          if (rs.hasNext() && "READY".equals(String.valueOf(rs.next().<Object>getProperty("status"))))
            return;
        }
        Thread.sleep(10);
      }
      throw new AssertionError("The view " + viewName + " is not READY");
    }
  }

  /**
   * Regression for #10338: transactions committed right after {@code CREATE GRAPH ANALYTICAL VIEW} (status BUILDING) were replayed over
   * the CSR the build published, and the replay of a vertex that took the slot of a deleted one read that node's edges from the
   * CSR with its absolute offsets passed to accessors that add the node's offset themselves. Past the first node that was an
   * {@code ArrayIndexOutOfBoundsException}, logged as "Cannot deliver a committed change" with the change dropped, or a wrong
   * count of the edges to subtract; a build that hit it never published.
   * <p>
   * Two things follow whatever the cause, and are pinned too: a committed change a SYNCHRONOUS view cannot merge makes it rebuild
   * instead of being dropped with a warning, and so does a buffered change the build cannot replay.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CommitRightAfterCreateView extends TestHelper {
    private static final String EDGE_TYPE = "L";
    private static final RID    SECOND    = new RID(1, 1);
    private static final RID    THIRD     = new RID(1, 2);

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE VERTEX TYPE Q");
      database.command("sql", "CREATE EDGE TYPE K");
      database.command("sql", "CREATE EDGE TYPE L");
    }

    // The three tests below replay a delta over hand-made CSRs: the RIDs are synthetic (one bucket, the position being the node id),
    // no database is involved

    /** The tail of a chain sits on the slot that is replayed: its in-edge starts past the first entry of the reverse index. */
    @Test
    void replayOfAReusedSlotWhoseInEdgeIsNotTheFirstEntry() {
      // 0 -> 1 -> 2, the node 2 (offset 1 in the reverse index) is the slot that was deleted and taken by a new vertex
      final NodeIdMapping mapping = mapping(3);
      final CSRAdjacencyIndex csr = new CSRAdjacencyIndex(new int[] { 0, 1, 2, 2 }, new int[] { 1, 2 }, new int[] { 0, 0, 1, 2 },
          new int[] { 0, 1 }, 3, 2);

      final DeltaOverlay overlay = replayReuse(mapping, csr, THIRD);

      assertThat(overlay.isDeleted(2)).as("the base node stays masked").isTrue();
      assertThat(csr.getEdgeCount() + overlay.getDeltaEdgeCount()).as("the edge 1 -> 2 of the scan is not counted").isEqualTo(1);
    }

    /** A slot in the middle of a ring has an in-edge and an out-edge, both past the start of their arrays. */
    @Test
    void replayOfAReusedSlotCountsItsOwnEdgesAndNotTheNeighboursOf() {
      // the ring 0 -> 1 -> 2 -> 3 -> 0: the node 1 is replayed, and the two edges it holds (0 -> 1, 1 -> 2) stop counting
      final NodeIdMapping mapping = mapping(4);
      final CSRAdjacencyIndex csr = new CSRAdjacencyIndex(new int[] { 0, 1, 2, 3, 4 }, new int[] { 1, 2, 3, 0 },
          new int[] { 0, 1, 2, 3, 4 }, new int[] { 3, 0, 1, 2 }, 4, 4);

      final DeltaOverlay overlay = replayReuse(mapping, csr, SECOND);

      assertThat(csr.getEdgeCount() + overlay.getDeltaEdgeCount()).as("2 -> 3 and 3 -> 0 are left").isEqualTo(2);
    }

    /** A self-loop is one edge, not one on each side. */
    @Test
    void replayOfAReusedSlotWithASelfLoopCountsItOnce() {
      // 0 -> 1, 1 -> 1, 1 -> 2: the node 1 holds three edges, one of them a loop
      final NodeIdMapping mapping = mapping(3);
      final CSRAdjacencyIndex csr = new CSRAdjacencyIndex(new int[] { 0, 1, 3, 3 }, new int[] { 1, 1, 2 }, new int[] { 0, 0, 2, 3 },
          new int[] { 0, 1, 1 }, 3, 3);

      final DeltaOverlay overlay = replayReuse(mapping, csr, SECOND);

      assertThat(csr.getEdgeCount() + overlay.getDeltaEdgeCount()).as("nothing is left").isZero();
    }

    /** The scenario of the issue, made deterministic: the commits land while the build waits for a permit, and are replayed. */
    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void commitsRightAfterCreateAreReplayedOnThePublishedBuild(final String mode) {
      final MutableVertex[] v = new MutableVertex[7];
      database.transaction(() -> {
        // an edge before the slot that is reused: its node does not start at the first offset of the CSR
        database.newVertex("P").set("id", 1L).save().newEdge("L", database.newVertex("P").set("id", 1L).save()).save();
        v[0] = database.newVertex("P").set("id", 2L).save();
        v[1] = database.newVertex("Q").set("id", 6L).save();
        v[2] = database.newVertex("P").set("id", 6L).save();
        v[0].newEdge("L", v[2]).save();
        v[1].newEdge("K", v[1]).save();
        v[5] = database.newVertex("Q").set("id", 6L).save();
        v[6] = database.newVertex("P").set("id", 5L).save();
        v[5].newEdge("K", v[6]).save();
      });

      final MutableVertex[] created = new MutableVertex[2];
      final int held = GraphAnalyticalView.BUILD_PERMITS.drainPermits();
      final GraphAnalyticalView view;
      try {
        database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P, Q) EDGE TYPES (K, L) PROPERTIES (id) UPDATE MODE " + mode)
            .close();
        view = GraphAnalyticalViewRegistry.get(database, "gav");
        assertThat(view.getStatus()).as("the build waits for a permit").isEqualTo(GraphAnalyticalView.Status.BUILDING);

        database.transaction(() -> v[0].delete());
        database.transaction(() -> created[0] = database.newVertex("P").set("id", 2L).save());
        database.transaction(() -> v[6].delete());
        database.transaction(() -> {
          created[1] = database.newVertex("P").set("id", 4L).save();
          created[0].newEdge("L", created[1]).save();
        });
      } finally {
        GraphAnalyticalView.BUILD_PERMITS.release(held);
      }

      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).as("the build publishes").isTrue();
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
      assertThat(edgesAnsweredByCypher("L")).as("the two L edges that are left").isEqualTo(2);
      assertThat(edgesAnsweredByCypher("K")).as("the K self-loop that is left").isEqualTo(1);
    }

    @Test
    void aCommittedChangeTheViewCannotMergeMakesItRebuildInsteadOfBeingDropped() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("P").set("id", 1L).save();
        a.newEdge("K", database.newVertex("P").set("id", 2L).save()).save();
      });
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P) EDGE TYPES (K) PROPERTIES (id) UPDATE MODE SYNCHRONOUS")
          .close();
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();

      // a change the overlay cannot take: a vertex without an identity
      final TxDelta unmergeable = new TxDelta();
      unmergeable.addedVertices.add(new TxDelta.VertexDelta(null, Map.of()));
      // The rebuild waits for a permit, so the view can be seen leaving READY: a change dropped with a warning would leave it there
      final int held = GraphAnalyticalView.BUILD_PERMITS.drainPermits();
      try {
        view.onCommittedDelta(unmergeable);
        assertThat(view.getStatus()).as("the unmergeable change asks for a rebuild").isNotEqualTo(GraphAnalyticalView.Status.READY);
      } finally {
        GraphAnalyticalView.BUILD_PERMITS.release(held);
      }

      // the graph is unchanged, so the rebuild the failure asked for publishes the same view
      awaitReadyAgain(view);
      assertThat(view.getEdgeCount()).isEqualTo(1);
      assertThat(view.getNodeCount()).isEqualTo(2);
    }

    /** A change buffered while the first build ran that the build cannot replay must not leave the view NOT_BUILT. */
    @Test
    void aBufferedChangeTheBuildCannotReplayDoesNotFailTheBuild() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("P").set("id", 1L).save();
        a.newEdge("K", database.newVertex("P").set("id", 2L).save()).save();
      });

      final int held = GraphAnalyticalView.BUILD_PERMITS.drainPermits();
      final GraphAnalyticalView view;
      try {
        database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P) EDGE TYPES (K) PROPERTIES (id) UPDATE MODE SYNCHRONOUS")
            .close();
        view = GraphAnalyticalViewRegistry.get(database, "gav");
        assertThat(view.getStatus()).as("the build waits for a permit").isEqualTo(GraphAnalyticalView.Status.BUILDING);

        final TxDelta unmergeable = new TxDelta();
        unmergeable.addedVertices.add(new TxDelta.VertexDelta(null, Map.of()));
        view.onCommittedDelta(unmergeable);
      } finally {
        GraphAnalyticalView.BUILD_PERMITS.release(held);
      }

      awaitReadyAgain(view);
      assertThat(view.getEdgeCount()).isEqualTo(1);
      assertThat(view.getNodeCount()).isEqualTo(2);
    }

    /** The view rebuilds after the failure: it is READY again, once the rebuild it asked for published. */
    private static void awaitReadyAgain(final GraphAnalyticalView view) {
      Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> view.getStatus() == GraphAnalyticalView.Status.READY);
    }

    /** The delete of the old vertex and the add of the new one that took its slot, replayed over a fresh base that holds both. */
    private DeltaOverlay replayReuse(final NodeIdMapping mapping, final CSRAdjacencyIndex csr, final RID reused) {
      final Map<String, CSRAdjacencyIndex> fresh = Map.of(EDGE_TYPE, csr);
      final TxDelta deleteOld = new TxDelta();
      deleteOld.deletedVertices.add(reused);
      final TxDelta addNew = new TxDelta();
      addNew.addedVertices.add(new TxDelta.VertexDelta(reused, Map.of()));

      return new DeltaOverlay(mapping.size()).merge(deleteOld, mapping, fresh, (type, src, tgt) -> 0)
          .merge(addNew, mapping, fresh, (type, src, tgt) -> 0);
    }

    private static NodeIdMapping mapping(final int nodes) {
      final NodeIdMapping mapping = new NodeIdMapping(1);
      final int bucket = mapping.registerBucket(1, "P", nodes);
      for (int i = 0; i < nodes; i++)
        mapping.addNode(bucket, i);
      mapping.compact();
      return mapping;
    }

    private long edgesAnsweredByCypher(final String edgeType) {
      try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r:" + edgeType + "]->() RETURN count(r) AS c")) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }
  }

  /**
   * Regression for #10350: right after a commit that deleted a vertex together with the edge it had just been given, {@code algo.wcc} and
   * {@code algo.scc} answered from a Graph Analytical View with one null node for a few milliseconds. Two defects met:
   * <ul>
   *   <li>the edge's {@code save()} reports a create and an update of it. The create never reaches the overlay, its source being
   *   deleted by the same transaction (#10339), and the update, finding no edge of the overlay to land on, marked the edge type's
   *   property columns out of date, which forces a full rebuild. An edge the view does not hold has no column to be out of date:
   *   the same happened for every edge to a vertex type the view does not cover, a rebuild per commit;</li>
   *   <li>the rebuild publishes a base numbered afresh, and an algorithm asks the view several questions - which ids are live, then
   *   the RID of each - each of them answered by the snapshot being served at that moment. One that started on the snapshot with
   *   the overlay and ended on the rebuilt one resolved ids of the first numbering in the second: one past its end came back null,
   *   the others as other vertices. An algorithm now reads one snapshot from start to end ({@link GraphTraversalProvider#pinned()}).</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ViewSnapshotSwap extends TestHelper {
    private static final int PAD = 2_000;

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE VERTEX TYPE Q");
      database.command("sql", "CREATE VERTEX TYPE R");
      database.command("sql", "CREATE EDGE TYPE K");
      database.command("sql", "CREATE EDGE TYPE L");
    }

    /** The reporter's sequence: the edge created and its source deleted by one transaction leaves the base in place. */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void anEdgeGoneWithItsSourceForcesNoRebuild(final String mode) {
      final RID[] ends = createPaddedGraph();
      final GraphAnalyticalView view = createView(mode);
      final Object numbering = view.getNodeIdSpace();

      final RID[] created = new RID[1];
      database.transaction(() -> {
        ends[1].asVertex().delete();
        created[0] = database.newVertex("P").set("id", 3L).save().getIdentity();
      });
      database.transaction(() -> {
        final Vertex c = created[0].asVertex();
        c.newEdge("L", ends[0]).save();
        c.delete();
      });
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();

      assertColumnsUpToDate(view);
      assertThat(view.getNodeIdSpace()).as("the base was not rebuilt").isSameAs(numbering);

      final Set<RID> live = liveVertices();
      assertThat(live).hasSize(PAD + 1);
      for (final String algorithm : new String[] { "wcc", "scc" })
        assertThat(nodesAnswered(algorithm)).as(algorithm).containsExactlyInAnyOrderElementsOf(live);
    }

    /** An edge to a vertex type the view does not cover is not in the view, created or updated: no column of it can be out of date. */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void anEdgeToAnUncoveredVertexForcesNoRebuild(final String mode) {
      final RID[] ends = createPaddedGraph();
      final GraphAnalyticalView view = createView(mode);
      final Object numbering = view.getNodeIdSpace();

      final RID[] edge = new RID[1];
      database.transaction(() -> {
        final MutableVertex outside = database.newVertex("R").set("id", 9L).save();
        edge[0] = ends[0].asVertex().newEdge("L", outside).set("ep", 1).save().getIdentity();
      });
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      assertColumnsUpToDate(view);

      database.transaction(() -> edge[0].asEdge().modify().set("ep", 2).save());
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      assertColumnsUpToDate(view);

      assertThat(view.getNodeIdSpace()).as("the base was not rebuilt").isSameAs(numbering);
    }

    /**
     * The race itself, made deterministic: a pinned view keeps answering from the snapshot it pinned after the view publishes a
     * base numbered afresh, which a provider asked one question at a time does not.
     */
    @Test
    void aPinnedViewAnswersFromItsSnapshotAcrossARebuild() {
      final RID[] ends = createPaddedGraph();
      final GraphAnalyticalView view = createView("SYNCHRONOUS");
      // a base vertex deleted and an overflow one created: the id space has a hole and an id past the base mapping
      database.transaction(() -> {
        ends[1].asVertex().delete();
        final MutableVertex c = database.newVertex("P").set("id", 3L).save();
        c.newEdge("K", ends[0]);
      });
      assertThat(view.hasPendingChanges()).isTrue();
      final Set<RID> live = liveVertices();

      final GraphTraversalProvider pinned = view.pinned();
      final GraphTraversalProvider dense = DenseNodeIdProvider.wrap(pinned);
      assertThat(dense).as("the overlay holds a deleted node: the ids are renumbered").isNotSameAs(pinned);

      view.build(); // publishes a compacted base: every id after the hole now names another vertex
      assertThat(view.hasPendingChanges()).isFalse();
      assertThat(pinned.hasPendingChanges()).as("the pinned snapshot still has its overlay").isTrue();
      assertThat(pinned.getNodeIdSpace()).isNotSameAs(view.getNodeIdSpace());

      final Set<RID> answered = new HashSet<>();
      for (int i = 0; i < dense.getNodeCount(); i++) {
        final RID rid = dense.getRID(i);
        assertThat(rid).as("dense id %d", i).isNotNull();
        assertThat(dense.getNodeId(rid)).isEqualTo(i);
        answered.add(rid);
      }
      assertThat(answered).containsExactlyInAnyOrderElementsOf(live);

      // the edge added in the overlay is still there, between the vertices it joins
      final int a = dense.getNodeId(ends[0]);
      final int[] in = dense.getNeighborIds(a, Vertex.DIRECTION.IN, "K");
      assertThat(in).hasSize(1);
      assertThat(dense.getRID(in[0]).asVertex().getInteger("id")).isEqualTo(3);
      assertThat(dense.countEdges(a, Vertex.DIRECTION.IN, "K")).isEqualTo(1);
    }

    /**
     * End to end: the view rebuilt while an algorithm streams its rows. The rows read after the rebuild still name the vertices of
     * the snapshot the algorithm ran on.
     */
    @ParameterizedTest(name = "{0}")
    @CsvSource({ "wcc", "scc", "pagerank", "labelpropagation" })
    void anAlgorithmStreamsItsAnswerFromOneSnapshot(final String algorithm) {
      final RID[] ends = createPaddedGraph();
      final GraphAnalyticalView view = createView("SYNCHRONOUS");
      database.transaction(() -> {
        ends[1].asVertex().delete();
        database.newVertex("P").set("id", 3L).save().newEdge("K", ends[0]);
      });
      assertThat(view.hasPendingChanges()).isTrue();
      final Set<RID> live = liveVertices();

      final List<RID> answered = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", "CALL algo." + algorithm + "() YIELD node RETURN node")) {
        answered.add(nodeOf(rs.next()));
        view.build(); // the base the algorithm's later rows are resolved against, unless it pinned its own
        while (rs.hasNext())
          answered.add(nodeOf(rs.next()));
      }
      assertThat(answered).doesNotContainNull().doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(live);
    }

    /**
     * Every question the view answers itself, rather than through the SPI's default, the pinned view answers from its snapshot
     * too: a default would fall back to whatever it is written against, and the pin would erode as the SPI grows.
     */
    @Test
    void aPinnedViewOverridesEveryMethodTheViewOverrides() throws NoSuchMethodException {
      final List<String> missing = new ArrayList<>();
      for (final Method spi : GraphTraversalProvider.class.getMethods()) {
        if (Modifier.isStatic(spi.getModifiers()))
          continue;
        final Method inView = GraphAnalyticalView.class.getMethod(spi.getName(), spi.getParameterTypes());
        final Method inPinned = GraphAnalyticalView.Pinned.class.getMethod(spi.getName(), spi.getParameterTypes());
        if (inView.getDeclaringClass() == GraphAnalyticalView.class && inPinned.getDeclaringClass() != GraphAnalyticalView.Pinned.class)
          missing.add(spi.getName());
      }
      assertThat(missing).as("SPI methods the view overrides and the pinned view leaves to the default").isEmpty();
    }

    // The four below merge synthetic deltas: one bucket, the position of a RID being its node id, no database involved

    /** An update to an edge whose end the view does not cover touches no column. */
    @Test
    void anUpdateToAnEdgeWithAnUncoveredEndDirtiesNothing() {
      final NodeIdMapping mapping = mapping(3);
      final DeltaOverlay overlay = new DeltaOverlay(mapping.size()).merge(updateOf(new RID(1, 0), new RID(9, 0)), mapping);
      assertThat(overlay.isEdgePropertiesDirty()).isFalse();
    }

    /** A base vertex deleted earlier: the view no longer serves its edges, so an update of one touches no column. */
    @Test
    void anUpdateToAnEdgeOfADeletedBaseVertexDirtiesNothing() {
      final NodeIdMapping mapping = mapping(3);
      final TxDelta delete = new TxDelta();
      delete.deletedVertices.add(new RID(1, 2));
      final DeltaOverlay overlay = new DeltaOverlay(mapping.size()).merge(delete, mapping)
          .merge(updateOf(new RID(1, 0), new RID(1, 2)), mapping);
      assertThat(overlay.isEdgePropertiesDirty()).isFalse();
    }

    /**
     * The slot of that deleted vertex taken by a new one: the RID names a vertex of the view again, in the overflow, so an edge
     * update the overlay cannot place is still one the columns may be out of step with.
     */
    @Test
    void anUpdateToAnEdgeOfAVertexOnAReusedSlotIsStillInTheView() {
      final NodeIdMapping mapping = mapping(3);
      final RID reused = new RID(1, 2);
      final TxDelta delete = new TxDelta();
      delete.deletedVertices.add(reused);
      final TxDelta add = new TxDelta();
      add.addedVertices.add(new TxDelta.VertexDelta(reused, Map.of()));
      final DeltaOverlay overlay = new DeltaOverlay(mapping.size()).merge(delete, mapping).merge(add, mapping)
          .merge(updateOf(new RID(1, 0), reused), mapping);
      assertThat(overlay.isDeleted(2)).as("precondition: the base node stays deleted").isTrue();
      assertThat(overlay.isEdgePropertiesDirty("L")).isTrue();
    }

    /** The reporter's delta: an edge created, saved and gone with its overflow source, all in one transaction. */
    @Test
    void anEdgeGoneWithItsOverflowSourceDirtiesNothing() {
      final NodeIdMapping mapping = mapping(3);
      final RID source = new RID(1, 5);
      final TxDelta create = new TxDelta();
      create.addedVertices.add(new TxDelta.VertexDelta(source, Map.of()));
      final TxDelta linkAndDelete = updateOf(source, new RID(1, 0));
      final TxDelta.EdgeDelta edge = linkAndDelete.updatedEdges.values().iterator().next();
      linkAndDelete.addedEdges.add(edge);
      linkAndDelete.deletedEdges.add(edge);
      linkAndDelete.deletedVertices.add(source);
      final DeltaOverlay overlay = new DeltaOverlay(mapping.size()).merge(create, mapping).merge(linkAndDelete, mapping);
      assertThat(overlay.isEdgePropertiesDirty()).isFalse();
      assertThat(overlay.getDeltaEdgeCount()).isZero();
    }

    private static TxDelta updateOf(final RID source, final RID target) {
      final TxDelta delta = new TxDelta();
      final RID edge = new RID(5, 0);
      delta.updatedEdges.put(edge, new TxDelta.EdgeDelta("L", source, target, edge));
      return delta;
    }

    private static NodeIdMapping mapping(final int nodes) {
      final NodeIdMapping mapping = new NodeIdMapping(1);
      final int bucket = mapping.registerBucket(1, "P", nodes);
      for (int i = 0; i < nodes; i++)
        mapping.addNode(bucket, i);
      mapping.compact();
      return mapping;
    }

    private static RID nodeOf(final Result row) {
      final Object node = row.getProperty("node");
      return node instanceof Identifiable identifiable ? identifiable.getIdentity() : null;
    }

    /** Padding Q vertices, then the P vertices {@code a} and {@code b}: returns their RIDs. */
    private RID[] createPaddedGraph() {
      final RID[] ends = new RID[2];
      database.transaction(() -> {
        for (int i = 0; i < PAD; i++)
          database.newVertex("Q").set("id", (long) i).save();
        ends[0] = database.newVertex("P").set("id", 1L).set("w", 0.5).set("s", "a").save().getIdentity();
        ends[1] = database.newVertex("P").set("id", 2L).set("w", 0.5).set("s", "a").save().getIdentity();
      });
      return ends;
    }

    private GraphAnalyticalView createView(final String mode) {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P, Q) EDGE TYPES (K, L) PROPERTIES (id, w, s) "
          + "EDGE PROPERTIES (ep) UPDATE MODE " + mode).close();
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      return view;
    }

    private static void assertColumnsUpToDate(final GraphAnalyticalView view) {
      final DeltaOverlay overlay = view.currentSnapshot().overlay;
      assertThat(overlay == null || !overlay.isEdgePropertiesDirty()).as("no edge column is out of date").isTrue();
    }

    private Set<RID> liveVertices() {
      final Set<RID> live = new HashSet<>();
      for (final String type : new String[] { "P", "Q" })
        database.iterateType(type, false).forEachRemaining(r -> live.add(r.getIdentity()));
      return live;
    }

    private List<RID> nodesAnswered(final String algorithm) {
      final List<RID> nodes = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", "CALL algo." + algorithm + "() YIELD node RETURN node")) {
        while (rs.hasNext())
          nodes.add(nodeOf(rs.next()));
      }
      return nodes;
    }
  }

  /**
   * Regression for #6719: three cheap O(n) sequential loops in {@link GraphAlgorithms} - {@code pageRank}'s dangling-node sum
   * accumulation, {@code lccBuildAndIntersect}'s {@code offsets}/{@code pos} array-fill loops and its final
   * {@code lcc[u]} coefficient-materialization loop - had no {@link WorkCheckpoint#check()} call at all, unlike every
   * other sequential pass in this file. Each is cheap per element (a double add or an int assign), so on a huge graph
   * it is not a correctness bug, just a small abortability gap: a caller that armed the checkpoint to stop a runaway
   * call could still be made to wait out one of these loops in full.
   * <p>
   * Both tests below rely on precise checkpoint-call accounting rather than timing: a graph shape and node count are
   * chosen so the checkpoint calls contributed by phases that already had a checkpoint before this fix are known
   * exactly, and the assertion is set just past that known baseline - so it can only be satisfied if the previously
   * unchecked loop calls the checkpoint too.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CheckpointGaps extends TestHelper {

    /**
     * {@code pageRank}'s dangling-sum loop, isolated with an all-dangling graph (no edges at all, so every node's
     * out-degree is zero) run for a single iteration. Without the loop's own checkpoint, one iteration makes exactly
     * 4 checkpoint calls (top-of-iteration, the contrib phase, the pull phase, the dangling-distribute phase - each
     * of the two {@code parallelForRangeCheckpointed} phases collapses to a single batch since {@code n} is below
     * {@code PARALLEL_THRESHOLD}), and {@code iterations == 1} means the loop never comes back around for a 5th. A
     * checkpoint that throws on its 5th call therefore only fires if the dangling-sum loop calls it too - which,
     * with 5000 dangling nodes and a periodic check every 1024 of them, it does well before the 4-call baseline
     * would otherwise finish the iteration.
     */
    @Test
    void danglingSumLoopIsCheckpointed() {
      final int n = 5000;
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");

      database.begin();
      for (int i = 0; i < n; i++)
        database.newVertex("Node").save();
      database.commit();

      final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
          .withVertexTypes("Node").withEdgeTypes("LINK").build();
      try {
        final AtomicInteger calls = new AtomicInteger();
        final WorkCheckpoint abortOn5thCall = () -> {
          if (calls.incrementAndGet() == 5)
            throw new RuntimeException("checkpoint reached - dangling-sum loop is checkpointed");
        };

        assertThatThrownBy(() -> GraphAlgorithms.pageRank(gav, 0.85, 1, DIRECTION.OUT, abortOn5thCall, "LINK"))
            .as("the dangling-sum loop must call the checkpoint before an all-dangling, single-iteration pageRank "
                + "run would otherwise finish (only 4 checkpoint calls without it)")
            .hasMessageContaining("dangling-sum loop is checkpointed");
      } finally {
        gav.drop();
      }
    }

    /**
     * The three previously-unchecked {@code lccBuildAndIntersect} loops (offsets build, pos build, final {@code
     * lcc[]} materialization), isolated together against the loops that already had a checkpoint before this fix.
     * <p>
     * One edge on an otherwise edgeless 5000-node graph keeps the CSR non-null (avoiding a null CSR on the
     * single-edge-type path) while leaving every degree at 0 or 1 - checkpoint call counts depend only on the node
     * count crossing the 1024-node periodic-check boundary, not on degree, so the single edge does not change the
     * count. For n = 5000 that boundary is crossed 4 times (at node indices 1023, 2047, 3071, 4095) in each of:
     * degree computation, the single-edge-type merge loop and the compact-in-place loop - all three checkpointed
     * before this fix, 12 calls total - plus one more from the triangle-counting phase's single batch, 13 without
     * the fix. The three newly-checkpointed loops each add 4 more, so a fixed run makes 25 calls; asserting at least
     * 25 fails against the pre-fix 13 and passes only once all three loops are checkpointed.
     */
    @Test
    void lccPrepAndFinalMaterializationLoopsAreCheckpointed() {
      final int n = 5000;
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");

      database.begin();
      MutableVertex first = null, second = null;
      for (int i = 0; i < n; i++) {
        final MutableVertex v = database.newVertex("Node").save();
        if (i == 0)
          first = v;
        else if (i == 1)
          second = v;
      }
      first.newEdge("LINK", second);
      database.commit();

      final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
          .withVertexTypes("Node").withEdgeTypes("LINK").build();
      try {
        final AtomicInteger calls = new AtomicInteger();
        final WorkCheckpoint counting = calls::incrementAndGet;

        final double[] lcc = GraphAlgorithms.localClusteringCoefficient(gav, counting, "LINK");

        assertThat(lcc).hasSize(n);
        assertThat(calls.get())
            .as("13 checkpoint calls come from the phases already checkpointed before issue #6719; the three "
                + "newly-checkpointed loops must contribute at least 4 more each")
            .isGreaterThanOrEqualTo(25);
      } finally {
        gav.drop();
      }
    }
  }

  /**
   * Regression for #10575: after a close and a reopen, a view that left its {@code gav-<name>.csr} behind is restored lazily (#6632): it is
   * marked READY and nothing is read. {@code schema:graphAnalyticalViews} took its "not built" branch for it and answered
   * {@code READY} with {@code nodeCount = 0} and {@code edgeCount = 0} for as long as no query used the view, so the documented way to
   * wait for a view (poll the row until READY) returned the zero row. Reading the row, like {@code GraphAnalyticalView.getStats()}, is
   * an explicit wait for the view, as {@code awaitReady} is: it dispatches the deferred restore, so the view reads BUILDING until
   * its counts are the real ones and no row claims READY before them. It holds whether the file turns out current (restored from
   * disk) or stale (the restore falls back to a rebuild).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GavRowPendingRestore extends TestHelper {
    private static final String VIEW     = "roads";
    private static final int    VERTICES = 300;

    private record Row(String status, int nodeCount, int edgeCount) {
    }

    @Test
    void theRowOfAViewWithAPendingRestoreNeverClaimsReadyWithZeroCounts() {
      final int edges = createGraphAndView();
      reopenDatabase();

      // Right after the reopen, before any query used the view: every row polled until READY carries the view's counts
      final List<Row> seen = new ArrayList<>();
      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(5, TimeUnit.MILLISECONDS).until(() -> {
        final Row row = readRow();
        seen.add(row);
        return "READY".equals(row.status());
      });

      for (final Row row : seen)
        if ("READY".equals(row.status()))
          assertThat(row).as("a READY row must carry the counts of the view").isEqualTo(new Row("READY", VERTICES, edges));
      assertThat(seen.get(seen.size() - 1)).isEqualTo(new Row("READY", VERTICES, edges));
    }

    @Test
    void readingTheRowRestoresTheCurrentFileInsteadOfRebuilding() {
      createGraphAndView();
      reopenDatabase();

      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, VIEW);
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
      assertThat(view.isBuilt()).as("nothing reads the file before something needs the view").isFalse();

      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(5, TimeUnit.MILLISECONDS)
          .until(() -> "READY".equals(readRow().status()) && view.isBuilt());

      assertThat(view.isRestoredFromPersistedCsr()).as("no commit since the close: the file is the view, no rebuild").isTrue();
    }

    @Test
    void aStaleFileIsRebuiltWhenTheRowAsksForTheViewAndNoRowClaimsReadyBeforeThen() {
      final int edges = createGraphAndView();
      // First reopen: the view is pending and the file is current. A commit now moves the database past the file's certificate and
      // the view, which was never queried, writes nothing at close: the file stays, stale
      reopenDatabase();
      final int extra = 25;
      addVertices(VERTICES, extra);
      reopenDatabase();

      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, VIEW);
      final List<Row> seen = new ArrayList<>();
      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(5, TimeUnit.MILLISECONDS).until(() -> {
        final Row row = readRow();
        seen.add(row);
        return "READY".equals(row.status());
      });

      // The rebuild the failed restore falls back to counts the vertices written since, and the ring is closed by the old edges only
      for (final Row row : seen)
        if ("READY".equals(row.status()))
          assertThat(row).isEqualTo(new Row("READY", VERTICES + extra, edges));
      assertThat(view.isRestoredFromPersistedCsr()).as("the stale file must not be the source").isFalse();
    }

    @Test
    void theStatsOfAViewWithAPendingRestoreNeverClaimReadyWithZeroCounts() {
      final int edges = createGraphAndView();
      reopenDatabase();

      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, VIEW);
      final List<Map<String, Object>> seen = new ArrayList<>();
      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(5, TimeUnit.MILLISECONDS).until(() -> {
        final Map<String, Object> stats = view.getStats();
        seen.add(stats);
        return "READY".equals(stats.get("status"));
      });

      for (final Map<String, Object> stats : seen)
        if ("READY".equals(stats.get("status"))) {
          assertThat(stats.get("nodeCount")).as("a READY view must report its counts").isEqualTo(VERTICES);
          assertThat(stats.get("edgeCount")).isEqualTo(edges);
        }
    }

    @Test
    void aViewThatIsQueriedFirstStillResolvesTheRestoreOnce() {
      final int edges = createGraphAndView();
      reopenDatabase();

      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, VIEW);
      assertThat(view.getNodeCount()).isEqualTo(VERTICES);
      assertThat(view.getEdgeCount()).isEqualTo(edges);
      assertThat(readRow()).isEqualTo(new Row("READY", VERTICES, edges));
    }

    /** @return the number of edges created */
    private int createGraphAndView() {
      database.getSchema().createVertexType("City");
      database.getSchema().createEdgeType("ROAD");
      addVertices(0, VERTICES);
      final int edges = addEdges();

      GraphAnalyticalView.builder(database)
          .withName(VIEW)
          .withVertexTypes("City")
          .withEdgeTypes("ROAD")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF)
          .build();
      assertThat(GraphAnalyticalViewRegistry.get(database, VIEW).getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
      return edges;
    }

    private void addVertices(final int from, final int count) {
      database.transaction(() -> {
        for (int i = from; i < from + count; i++)
          database.newVertex("City").set("id", i).save();
      });
    }

    /** A ring plus a chord every ten cities, so the graph is not a plain chain. */
    private int addEdges() {
      final List<RID> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM City ORDER BY id")) {
        while (rs.hasNext())
          ids.add(rs.next().getIdentity().orElseThrow());
      }
      final int[] created = new int[1];
      database.transaction(() -> {
        for (int i = 0; i < ids.size(); i++) {
          final MutableVertex from = ids.get(i).asVertex().modify();
          from.newEdge("ROAD", ids.get((i + 1) % ids.size()).asVertex());
          created[0]++;
          if (i % 10 == 0) {
            from.newEdge("ROAD", ids.get((i + 7) % ids.size()).asVertex());
            created[0]++;
          }
        }
      });
      return created[0];
    }

    private Row readRow() {
      try (final ResultSet rs = database.query("sql", "SELECT FROM schema:graphAnalyticalViews WHERE name = ?", VIEW)) {
        final Result row = rs.next();
        return new Row(row.getProperty("status"), ((Number) row.getProperty("nodeCount")).intValue(),
            ((Number) row.getProperty("edgeCount")).intValue());
      }
    }
  }

  /**
   * Regression for #7875.
   * <p>
   * Regression for #7875: {@code arcadedb.gavUseWhenStale} is {@code SCOPE.DATABASE}, so {@code ALTER DATABASE} on it must
   * reach every view of that database - views already built, and views restored when the database is reopened - while an
   * explicit per-view override still wins and survives the restart.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GAVUseWhenStaleDatabaseScope extends TestHelper {

    @Test
    void alterDatabaseReachesAnAlreadyBuiltView() {
      final GraphAnalyticalView gav = buildStaleView("gav7875", null);

      assertThat(gav.isStale()).isTrue();
      assertThat(gav.isUseWhenStale()).isFalse();
      assertThat(gav.isReady()).isFalse();
      assertThat(GraphTraversalProviderRegistry.findProvider(database, new String[] { "FOLLOWS" })).isNull();

      database.command("sql", "ALTER DATABASE `arcadedb.gavUseWhenStale` true").close();

      assertThat(gav.getUseWhenStaleOverride()).isNull();
      assertThat(gav.isUseWhenStale()).isTrue();
      assertThat(gav.isReady()).isTrue();
      assertThat(GraphTraversalProviderRegistry.findProvider(database, new String[] { "FOLLOWS" })).isNotNull();

      database.command("sql", "ALTER DATABASE `arcadedb.gavUseWhenStale` false").close();
      assertThat(gav.isReady()).isFalse();

      // The JVM-global value is untouched: the setting lives in this database's overlay only
      assertThat(GlobalConfiguration.GAV_USE_WHEN_STALE.getValueAsBoolean()).isFalse();
      gav.drop();
    }

    @Test
    void restoredViewFollowsTheDatabaseSetting() {
      database.command("sql", "ALTER DATABASE `arcadedb.gavUseWhenStale` true").close();
      buildStaleView("gav7875r", null);

      reopenDatabase();

      final GraphAnalyticalView restored = awaitRestored("gav7875r");
      assertThat(restored.getUseWhenStaleOverride()).isNull();
      assertThat(database.getConfiguration().getValueAsBoolean(GlobalConfiguration.GAV_USE_WHEN_STALE)).isTrue();
      assertThat(restored.isUseWhenStale()).isTrue();

      // Go stale again on the restored view: the database setting keeps it serving
      commitOneVertex();
      assertThat(restored.isStale()).isTrue();
      assertThat(restored.isReady()).isTrue();
      restored.drop();
    }

    @Test
    void explicitOverrideWinsAndSurvivesRestart() {
      database.command("sql", "ALTER DATABASE `arcadedb.gavUseWhenStale` true").close();
      final GraphAnalyticalView gav = buildStaleView("gav7875o", false);
      assertThat(gav.getUseWhenStaleOverride()).isFalse();
      assertThat(gav.isReady()).isFalse();

      reopenDatabase();

      final GraphAnalyticalView restored = awaitRestored("gav7875o");
      assertThat(restored.getUseWhenStaleOverride()).isFalse();
      commitOneVertex();
      assertThat(restored.isStale()).isTrue();
      assertThat(restored.isReady()).isFalse();

      // Clearing the override goes back to the database setting
      restored.setUseWhenStale(null);
      assertThat(restored.isReady()).isTrue();
      restored.drop();
    }

    private GraphAnalyticalView buildStaleView(final String name, final Boolean useWhenStale) {
      if (!database.getSchema().existsType("Person")) {
        database.getSchema().createVertexType("Person");
        database.getSchema().createEdgeType("FOLLOWS");
      }
      commitOneVertex();

      final GraphAnalyticalViewBuilder builder = GraphAnalyticalView.builder(database)
          .withName(name)
          .withVertexTypes("Person")
          .withEdgeTypes("FOLLOWS")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF);
      if (useWhenStale != null)
        builder.withUseWhenStale(useWhenStale);
      final GraphAnalyticalView gav = builder.build();
      assertThat(gav.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);

      // OFF mode: any commit marks the view STALE
      commitOneVertex();
      return gav;
    }

    private GraphAnalyticalView awaitRestored(final String name) {
      final GraphAnalyticalView restored = GraphAnalyticalViewRegistry.get(database, name);
      assertThat(restored).isNotNull();
      assertThat(restored.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      assertThat(restored.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
      return restored;
    }

    private void commitOneVertex() {
      database.transaction(() -> database.newVertex("Person").set("name", "p").save());
    }
  }
}
