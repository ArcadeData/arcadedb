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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9240, part 1: the planners and the graph functions ask the registry for a view with
 * {@link GraphTraversalProviderRegistry#findProvider}, and the first call after a reopen only dispatches the deferred restore of the
 * view and was answered "no view", so the first count push-down or {@code MATCH} after every restart took the record path although the
 * view was ready a fraction of a second later. The lookup now waits, within {@code arcadedb.gavQueryRestoreAwaitTimeout}, for a view
 * whose restore is in flight.
 * <p>
 * The restore is held in flight with the shared build permits, so every case is deterministic: nothing depends on how fast it is.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GavQueryRestoreAwaitTest {
  private static final String DB_PATH   = "./target/databases/test-gav-query-restore-await";
  private static final String VIEW_NAME = "query-restore-await-view";

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
    });
    GraphAnalyticalView.builder(database)
        .withName(VIEW_NAME)
        .withVertexTypes("Node")
        .withEdgeTypes("EDGE")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF)
        .build();
    database.close();
    database = new DatabaseFactory(DB_PATH).open();
  }

  @AfterEach
  void teardown() {
    if (database != null && database.isOpen())
      database.close();
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
  }

  @Test
  void theFirstLookupAfterAReopenWaitsForTheRestoreAndAnswersWithTheView() throws Exception {
    final GraphAnalyticalView restored = GraphAnalyticalViewRegistry.get(database, VIEW_NAME);
    assertThat(restored.isRestoring()).as("nothing has dispatched the deferred restore yet").isFalse();

    final GraphTraversalProvider provider;
    final StallAwareStopwatch stopwatch;
    GraphAnalyticalView.acquireAllBuildPermitsForTest();
    final Thread releaser = new Thread(() -> {
      try {
        Thread.sleep(300L);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        GraphAnalyticalView.releaseAllBuildPermitsForTest();
      }
    });
    try {
      releaser.start();
      stopwatch = StallAwareStopwatch.start();
      provider = GraphTraversalProviderRegistry.findProvider(database, "EDGE");
    } finally {
      releaser.join();
    }

    stopwatch.assertGaveUpWithin(30_000L, "a restore released after 300 ms versus the 5 s budget");
    assertThat(stopwatch.elapsedMs()).as("it waited for the restore").isGreaterThanOrEqualTo(250L);
    assertThat(provider).as("the view the restore made ready, not the record path").isSameAs(restored);
    assertThat(restored.isRestoredFromPersistedCsr()).isTrue();
  }

  @Test
  void aLookupWhoseRestoreIsAlreadyDoneDoesNotWait() {
    final GraphAnalyticalView restored = GraphAnalyticalViewRegistry.get(database, VIEW_NAME);
    assertThat(GraphTraversalProviderRegistry.findProvider(database, "EDGE")).isSameAs(restored);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    final GraphTraversalProvider again = GraphTraversalProviderRegistry.findProvider(database, "EDGE");

    stopwatch.assertGaveUpWithin(5_000L, "a ready view versus a wait");
    assertThat(again).isSameAs(restored);
  }

  @Test
  void aLookupGivesUpAtTheBudgetWhenTheRestoreNeverEnds() {
    database.getConfiguration().setValue(GlobalConfiguration.GAV_QUERY_RESTORE_AWAIT_TIMEOUT, 300L);

    final GraphTraversalProvider provider;
    final StallAwareStopwatch stopwatch;
    GraphAnalyticalView.acquireAllBuildPermitsForTest();
    try {
      stopwatch = StallAwareStopwatch.start();
      provider = GraphTraversalProviderRegistry.findProvider(database, "EDGE");
    } finally {
      GraphAnalyticalView.releaseAllBuildPermitsForTest();
    }

    stopwatch.assertGaveUpWithin(30_000L, "a 300 ms budget versus a restore that never ends");
    assertThat(stopwatch.elapsedMs()).as("it did wait for its budget").isGreaterThanOrEqualTo(250L);
    assertThat(provider).as("the record path, as before the setting existed").isNull();
  }

  @Test
  void aZeroBudgetDoesNotWaitAtAll() {
    database.getConfiguration().setValue(GlobalConfiguration.GAV_QUERY_RESTORE_AWAIT_TIMEOUT, 0L);

    final GraphTraversalProvider provider;
    GraphAnalyticalView.acquireAllBuildPermitsForTest();
    try {
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      provider = GraphTraversalProviderRegistry.findProvider(database, "EDGE");
      stopwatch.assertGaveUpWithin(30_000L, "no waiting versus a stalled restore that never ends");
    } finally {
      GraphAnalyticalView.releaseAllBuildPermitsForTest();
    }

    assertThat(provider).isNull();
  }

  @Test
  void aViewThatDoesNotCoverTheRequestIsNotWaitedFor() {
    database.getConfiguration().setValue(GlobalConfiguration.GAV_QUERY_RESTORE_AWAIT_TIMEOUT, 600_000L);

    GraphAnalyticalView.acquireAllBuildPermitsForTest();
    try {
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(database, "OTHER");
      stopwatch.assertGaveUpWithin(30_000L, "a view that cannot serve the request versus the 10 minute budget");
      assertThat(provider).isNull();
    } finally {
      GraphAnalyticalView.releaseAllBuildPermitsForTest();
    }
  }

  @Test
  void theFirstShortestPathAfterAReopenRunsOnTheRestoredView() throws Exception {
    final GraphAnalyticalView restored = GraphAnalyticalViewRegistry.get(database, VIEW_NAME);
    final RID from = nodeNamed("A");
    final RID to = nodeNamed("C");

    final int pathLength;
    GraphAnalyticalView.acquireAllBuildPermitsForTest();
    final Thread releaser = new Thread(() -> {
      try {
        Thread.sleep(300L);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        GraphAnalyticalView.releaseAllBuildPermitsForTest();
      }
    });
    try {
      releaser.start();
      try (final ResultSet rs = database.query("sql", "SELECT shortestPath(?, ?, 'OUT', 'EDGE') AS path", from, to)) {
        pathLength = rs.next().<List<?>>getProperty("path").size();
      }
    } finally {
      releaser.join();
    }

    assertThat(pathLength).isEqualTo(3);
    assertThat(restored.isRestoredFromPersistedCsr()).as("the first query waited for the restore of the view").isTrue();
  }

  private RID nodeNamed(final String name) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Node WHERE name = ?", name)) {
      return rs.next().getIdentity().get();
    }
  }
}
