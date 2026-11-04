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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A compaction rebuilds the base CSR with a read-committed scan while the view keeps serving. A transaction's pages are
 * readable as soon as it releases its locks, but its change reaches the view only from its after-commit callback, later: a
 * commit the compaction scan read can be delivered after the compaction swapped its CSR in. Merged into the fresh base
 * unchecked, its edge was counted twice, for as long as that base was served (the intermittent failure of
 * {@code Issue10189AsyncViewServesWhileUpdatingTest.aCompactionRebuildsInTheBackgroundWhileTheViewServes}). The compaction
 * now watches its scan as a full build does, so a change delivered late is checked against what the scan read.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphAnalyticalViewCompactionLateDeliveryTest {
  private static final String DB_PATH  = "./target/databases/test-gav-compaction-late-delivery";
  private static final String QUERY    = "MATCH (a:P)-[:K]->(b:P) RETURN count(*) AS n";
  private static final int    VERTICES = 10;

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
      for (int i = 0; i < VERTICES; i++)
        vertices[i].newEdge("K", vertices[(i + 1) % VERTICES]);
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

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aCommitTheCompactionReadAndDeliveredAfterTheSwapIsNotCountedTwice(final GraphAnalyticalView.UpdateMode mode)
      throws Exception {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (P) EDGE TYPES (K) UPDATE MODE " + mode);
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    view.setCompactionThreshold(3);

    final CountDownLatch committed = new CountDownLatch(1);
    final CountDownLatch deliver = new CountDownLatch(1);
    final AtomicReference<Throwable> committerFailure = new AtomicReference<>();
    final Thread committer = new Thread(() -> {
      try {
        database.transaction(() -> {
          // Registered ahead of the view's callback, so it runs first: the pages are readable, the change is not delivered
          ((DatabaseInternal) database).getTransaction().addAfterCommitCallback(() -> {
            committed.countDown();
            try {
              deliver.await(30, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
          rids[7].asVertex().modify().newEdge("K", rids[2].asVertex());
        });
      } catch (final Throwable t) {
        committerFailure.set(t);
        committed.countDown();
      }
    });

    GraphAnalyticalView.acquireAllBuildPermitsForTest();
    try {
      // past the threshold: the compaction is dispatched and waits for a permit
      for (int i = 0; i < 4; i++)
        addEdge(i, (i + 5) % VERTICES);
      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      assertThat(overlayActive(view)).isTrue();

      // committed before the compaction scans, delivered once it has swapped
      committer.start();
      assertThat(committed.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(committerFailure.get()).isNull();
    } finally {
      GraphAnalyticalView.releaseAllBuildPermitsForTest();
    }

    try {
      // The scan read the new edge, and no change was delivered while it ran: the fresh base carries no overlay
      Awaitility.await("the compaction swapped its CSR in").atMost(30, TimeUnit.SECONDS).until(() -> !overlayActive(view));
    } finally {
      deliver.countDown();
      committer.join(TimeUnit.SECONDS.toMillis(30));
    }
    assertThat(committer.isAlive()).isFalse();
    assertThat(committerFailure.get()).isNull();

    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(plan()).contains("GAV");
    assertThat(countOnRecords()).isEqualTo(VERTICES + 5L);
    assertThat(count()).as("the late change is the edge the compaction already read").isEqualTo(VERTICES + 5L);
  }

  private static boolean overlayActive(final GraphAnalyticalView view) {
    return Boolean.TRUE.equals(view.getStats().get("overlayActive"));
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

  /** The edges counted on the records with the vertex API, which no view answers. */
  private long countOnRecords() {
    long edges = 0;
    for (final Iterator<Record> it = database.iterateType("P", true); it.hasNext(); )
      for (final Vertex ignored : it.next().asVertex().getVertices(Vertex.DIRECTION.OUT, "K"))
        ++edges;
    return edges;
  }
}
