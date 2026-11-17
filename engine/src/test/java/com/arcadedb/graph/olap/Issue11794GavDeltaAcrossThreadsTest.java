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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #11794: a graph analytical view kept the changes of a transaction per THREAD, so a transaction whose
 * statements ran on different threads (an HTTP session: one request per statement, each on whichever worker is free) handed
 * the view only the changes of the thread that made the first one. The records were committed and the view never learnt
 * of the others. The changes now belong to the transaction, whichever thread makes them.
 * <p>
 * Each statement here runs on its own thread with the transaction handed over through {@link DatabaseContext}, the way
 * the HTTP server attaches a session transaction to the worker that serves a request, and the commit runs on yet another.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11794GavDeltaAcrossThreadsTest extends TestHelper {
  private static final int VERTICES = 6;

  private RID[]           sources;
  private RID[]           targets;
  private ExecutorService workers;

  @BeforeEach
  void setupGraph() {
    database.getSchema().createVertexType("A");
    database.getSchema().createVertexType("B");
    database.getSchema().createEdgeType("R");
    sources = new RID[VERTICES];
    targets = new RID[VERTICES];
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++) {
        sources[i] = database.newVertex("A").set("id", i).save().getIdentity();
        targets[i] = database.newVertex("B").set("id", i).save().getIdentity();
      }
    });
    // a thread per task: the pool creates a new one for every submission up to its core size
    workers = Executors.newFixedThreadPool(VERTICES + 2);
  }

  @AfterEach
  void stopWorkers() throws InterruptedException {
    workers.shutdownNow();
    assertThat(workers.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
  }

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void everyThreadOfOneTransactionReachesTheView(final GraphAnalyticalView.UpdateMode mode) throws Exception {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("gv").withVertexTypes("A", "B")
        .withEdgeTypes("R").withUpdateMode(mode).withCompactionThreshold(Integer.MAX_VALUE).build();
    final long builtAt = view.getBuildTimestamp();
    try {
      final DatabaseInternal db = (DatabaseInternal) database;
      database.begin();
      final TransactionContext tx = db.getTransaction();
      DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());

      // one statement per thread, one after the other, as the requests of one session
      for (int i = 0; i < VERTICES; i++) {
        final int index = i;
        workers.submit(() -> runInTransaction(db, tx, () -> {
          final MutableVertex source = sources[index].asVertex().modify();
          source.newEdge("R", targets[index].asVertex());
        })).get(30, TimeUnit.SECONDS);
      }
      // ... and a vertex created on yet another thread
      final RID[] created = new RID[1];
      workers.submit(() -> runInTransaction(db, tx, () -> created[0] = database.newVertex("A").set("id", 99).save().getIdentity()))
          .get(30, TimeUnit.SECONDS);

      // the commit on a thread that made no change at all
      workers.submit(() -> runInTransaction(db, tx, database::commit)).get(30, TimeUnit.SECONDS);

      assertThat(database.countType("R", false)).isEqualTo(VERTICES);

      Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
        assertThat(view.isStale()).as("the view caught up with the commit").isFalse();
        assertDeliveredAsDelta(view, builtAt);
        for (int i = 0; i < VERTICES; i++) {
          final int nodeId = view.getNodeId(sources[i]);
          assertThat(nodeId).isGreaterThanOrEqualTo(0);
          assertThat(view.countEdges(nodeId, Vertex.DIRECTION.OUT, "R"))
              .as("edge %d, created on its own thread, is in the view", i).isEqualTo(1L);
        }
        assertThat(view.getNodeId(created[0])).as("the vertex created on another thread is in the view")
            .isGreaterThanOrEqualTo(0);
      });
    } finally {
      view.drop();
    }
  }

  /**
   * The updates and deletions of one transaction made on different threads reach the view too, and an edge created on one thread
   * and deleted on another is no change at all: the deletion finds the creation in the same delta and takes it back.
   */
  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void updatesAndDeletionsOnOtherThreadsReachTheView(final GraphAnalyticalView.UpdateMode mode) throws Exception {
    // committed before the view is built: A0 -> B0, A1 -> B1, and A2 named "old"
    database.transaction(() -> {
      sources[0].asVertex().modify().newEdge("R", targets[0].asVertex());
      sources[1].asVertex().modify().newEdge("R", targets[1].asVertex());
      sources[2].asVertex().modify().set("name", "old").save();
    });
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("gv").withVertexTypes("A", "B")
        .withEdgeTypes("R").withProperties("name").withUpdateMode(mode).withCompactionThreshold(Integer.MAX_VALUE).build();
    final long builtAt = view.getBuildTimestamp();
    try {
      assertThat(view.countEdges(view.getNodeId(sources[0]), Vertex.DIRECTION.OUT, "R")).isEqualTo(1L);

      final DatabaseInternal db = (DatabaseInternal) database;
      database.begin();
      final TransactionContext tx = db.getTransaction();
      DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());

      // the first change of the transaction, on its own thread: an edge the transaction will take back
      final RID[] created = new RID[1];
      runOnItsOwnThread(db, tx,
          () -> created[0] = sources[3].asVertex().modify().newEdge("R", targets[3].asVertex()).getIdentity());
      // ... deleted on another thread
      runOnItsOwnThread(db, tx, () -> created[0].asEdge().delete());
      // a committed edge deleted
      runOnItsOwnThread(db, tx, () -> sources[0].asVertex().getEdges(Vertex.DIRECTION.OUT, "R").iterator().next().delete());
      // a committed vertex updated
      runOnItsOwnThread(db, tx, () -> sources[2].asVertex().modify().set("name", "new").save());
      // a committed vertex deleted, and its edge A1 -> B1 with it
      runOnItsOwnThread(db, tx, () -> targets[1].asVertex().delete());
      // the commit on a thread that made no change at all
      runOnItsOwnThread(db, tx, database::commit);

      assertThat(database.countType("R", false)).isEqualTo(0L);

      Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
        assertThat(view.isStale()).as("the view caught up with the commit").isFalse();
        assertDeliveredAsDelta(view, builtAt);
        assertThat(view.countEdges(view.getNodeId(sources[3]), Vertex.DIRECTION.OUT, "R"))
            .as("the edge created on one thread and deleted on another is not in the view").isEqualTo(0L);
        assertThat(view.countEdges(view.getNodeId(sources[0]), Vertex.DIRECTION.OUT, "R"))
            .as("the committed edge deleted on another thread is gone from the view").isEqualTo(0L);
        assertThat(view.countEdges(view.getNodeId(sources[1]), Vertex.DIRECTION.OUT, "R"))
            .as("the edge of the vertex deleted on another thread is gone from the view").isEqualTo(0L);
        final int deleted = view.getNodeId(targets[1]);
        assertThat(deleted < 0 || !view.isNodeLive(deleted)).as("the vertex deleted on another thread is gone from the view")
            .isTrue();
        assertThat(view.getProperty(view.getNodeId(sources[2]), "name"))
            .as("the update made on another thread is in the view").isEqualTo("new");
      });
    } finally {
      view.drop();
    }
  }

  /**
   * The change reached the view as the transaction's delta, merged into the overlay, and not through a full rebuild, which
   * would answer the same final state while hiding a delta that never arrived: compaction is off, the base was not rebuilt,
   * and the overlay holds the change.
   */
  private static void assertDeliveredAsDelta(final GraphAnalyticalView view, final long builtAt) {
    assertThat(view.getBuildTimestamp()).as("the view was not rebuilt").isEqualTo(builtAt);
    assertThat(view.hasActiveOverlay()).as("the change is in the overlay").isTrue();
  }

  private void runOnItsOwnThread(final DatabaseInternal db, final TransactionContext tx, final Runnable work) throws Exception {
    workers.submit(() -> runInTransaction(db, tx, work)).get(30, TimeUnit.SECONDS);
  }

  private void runInTransaction(final DatabaseInternal db, final TransactionContext tx, final Runnable work) {
    DatabaseContext.INSTANCE.init(db, tx);
    try {
      work.run();
    } finally {
      DatabaseContext.INSTANCE.removeContext(db.getDatabasePath());
    }
  }
}
