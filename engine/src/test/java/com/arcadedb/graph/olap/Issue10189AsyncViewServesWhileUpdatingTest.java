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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10189: an {@code ASYNCHRONOUS} Graph Analytical View answered every relevant commit with a full rebuild and reported
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
class Issue10189AsyncViewServesWhileUpdatingTest {
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
