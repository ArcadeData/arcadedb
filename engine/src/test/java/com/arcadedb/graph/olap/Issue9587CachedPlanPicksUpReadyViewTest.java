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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9587: an OpenCypher plan built while the Graph Analytical View that could serve it was not ready - its deferred restore
 * from disk still in flight after a reopen, its first build still running, or the view stale and not to be used stale - walks the
 * records, and the plan cache kept handing that plan out after the view turned READY. The view reported READY, EXPLAIN (which
 * plans afresh) showed the view, and every execution of the query still ran at the speed of no view at all, until a schema change
 * happened to flush the cache. A cached plan that passed over a view is now planned again once that view can serve it.
 * <p>
 * The view is held not ready with the shared build permits, so nothing depends on how fast it is.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9587CachedPlanPicksUpReadyViewTest {
  private static final String DB_PATH   = "./target/databases/test-issue-9587";
  private static final String VIEW_NAME = "g";
  private static final String QUERY     = "MATCH (a:P)-[:K]->(b:P)-[:K]->(c:P) RETURN count(*) AS n";
  // 6 vertices on a ring, each with an edge to the next two: every vertex has out-degree 2 and in-degree 2, so 6 * 2 * 2 paths
  private static final long   EXPECTED  = 24L;

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("K");
    database.transaction(() -> {
      final List<MutableVertex> vertices = new ArrayList<>();
      for (int i = 0; i < 6; i++)
        vertices.add(database.newVertex("P").set("pid", i).save());
      for (int i = 0; i < 6; i++) {
        vertices.get(i).newEdge("K", vertices.get((i + 1) % 6));
        vertices.get(i).newEdge("K", vertices.get((i + 2) % 6));
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
  void aPlanCachedWhileTheRestoreWasInFlightReadsTheRestoredView() {
    database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW " + VIEW_NAME + " VERTEX TYPES (P) EDGE TYPES (K) UPDATE MODE ASYNCHRONOUS");
    final GraphAnalyticalView built = GraphAnalyticalViewRegistry.get(database, VIEW_NAME);
    assertThat(built.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(count()).isEqualTo(EXPECTED);
    assertThat(cachedPlanReadsTheView()).as("a view built in the session is read").isTrue();

    database.close();
    database = new DatabaseFactory(DB_PATH).open();
    // the reporter's build predates the query-side restore wait: the first query plans without the view
    database.getConfiguration().setValue(GlobalConfiguration.GAV_QUERY_RESTORE_AWAIT_TIMEOUT, 0L);
    final GraphAnalyticalView restored = GraphAnalyticalViewRegistry.get(database, VIEW_NAME);

    GraphAnalyticalView.acquireAllBuildPermitsForTest();
    try {
      assertThat(count()).isEqualTo(EXPECTED);
      assertThat(restored.isRestoring()).as("the first query dispatched the restore, held by the permits").isTrue();
      assertThat(cachedPlanReadsTheView()).as("planned while the view was restoring").isFalse();
    } finally {
      GraphAnalyticalView.releaseAllBuildPermitsForTest();
    }

    assertThat(restored.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(restored.isRestoredFromPersistedCsr()).isTrue();

    assertThat(count()).isEqualTo(EXPECTED);
    assertThat(cachedPlanReadsTheView()).as("the restored view is READY, so the query reads it").isTrue();
  }

  @Test
  void aPlanCachedWhileTheFirstBuildRanReadsTheBuiltView() {
    final GraphAnalyticalView view;
    GraphAnalyticalView.acquireAllBuildPermitsForTest();
    try {
      view = GraphAnalyticalView.builder(database)
          .withName(VIEW_NAME)
          .withVertexTypes("P")
          .withEdgeTypes("K")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF)
          .buildAsync();
      assertThat(view.isReady()).isFalse();
      assertThat(count()).isEqualTo(EXPECTED);
      assertThat(cachedPlanReadsTheView()).as("planned while the view was building").isFalse();
    } finally {
      GraphAnalyticalView.releaseAllBuildPermitsForTest();
    }

    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();

    assertThat(count()).isEqualTo(EXPECTED);
    assertThat(cachedPlanReadsTheView()).as("the build is over, so the query reads the view").isTrue();
  }

  @Test
  void aPlanCachedWhileTheViewWasStaleReadsItOnceRefreshed() {
    database.getConfiguration().setValue(GlobalConfiguration.GAV_USE_WHEN_STALE, false);
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName(VIEW_NAME)
        .withVertexTypes("P")
        .withEdgeTypes("K")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF)
        .build();

    // a commit the view is not kept up to date with: it goes stale and is not to be used stale
    database.transaction(() -> database.newVertex("P").set("pid", 6).save());
    assertThat(view.isStale()).isTrue();
    assertThat(count()).isEqualTo(EXPECTED);
    assertThat(cachedPlanReadsTheView()).as("planned while the view was stale").isFalse();

    view.build();
    assertThat(view.isReady()).isTrue();

    assertThat(count()).isEqualTo(EXPECTED);
    assertThat(cachedPlanReadsTheView()).as("the view was rebuilt, so the query reads it").isTrue();
  }

  @Test
  void aPlanCachedWhileTheViewWasStaleReadsItOnceStaleUseIsAllowed() {
    database.getConfiguration().setValue(GlobalConfiguration.GAV_USE_WHEN_STALE, false);
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName(VIEW_NAME)
        .withVertexTypes("P")
        .withEdgeTypes("K")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF)
        .build();

    database.transaction(() -> database.newVertex("P").set("pid", 6).save());
    assertThat(count()).isEqualTo(EXPECTED);
    assertThat(cachedPlanReadsTheView()).as("planned while the view was stale").isFalse();

    // no status change at all: only the setting that decides whether a stale view serves queries
    view.setUseWhenStale(true);

    assertThat(count()).isEqualTo(EXPECTED);
    assertThat(cachedPlanReadsTheView()).as("a stale view may now serve the query").isTrue();
  }

  @Test
  void aViewThatStaysUnavailableKeepsTheCachedPlan() {
    database.getConfiguration().setValue(GlobalConfiguration.GAV_USE_WHEN_STALE, false);
    GraphAnalyticalView.builder(database)
        .withName(VIEW_NAME)
        .withVertexTypes("P")
        .withEdgeTypes("K")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF)
        .build();
    database.transaction(() -> database.newVertex("P").set("pid", 6).save());

    assertThat(count()).isEqualTo(EXPECTED);
    final PhysicalPlan cached = cachedPlan();
    assertThat(cached).isNotNull();

    assertThat(count()).isEqualTo(EXPECTED);
    assertThat(cachedPlan()).as("nothing changed for the view, so the plan is not built again").isSameAs(cached);
  }

  private long count() {
    try (final ResultSet rs = database.query("opencypher", QUERY)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }

  private PhysicalPlan cachedPlan() {
    return ((DatabaseInternal) database).getCypherPlanCache().get(QUERY);
  }

  private boolean cachedPlanReadsTheView() {
    final PhysicalPlan plan = cachedPlan();
    assertThat(plan).as("the query's plan is cached").isNotNull();
    return plan.getRootOperator().explain(0).contains("provider=" + VIEW_NAME);
  }
}
