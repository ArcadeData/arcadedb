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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10575: after a close and a reopen, a view that left its {@code gav-<name>.csr} behind is restored lazily (#6632): it is
 * marked READY and nothing is read. {@code schema:graphAnalyticalViews} took its "not built" branch for it and answered
 * {@code READY} with {@code nodeCount = 0} and {@code edgeCount = 0} for as long as no query used the view, so the documented way to
 * wait for a view (poll the row until READY) returned the zero row. Reading the row, like {@code GraphAnalyticalView.getStats()}, is
 * an explicit wait for the view, as {@code awaitReady} is: it dispatches the deferred restore, so the view reads BUILDING until
 * its counts are the real ones and no row claims READY before them. It holds whether the file turns out current (restored from
 * disk) or stale (the restore falls back to a rebuild).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10575GavRowPendingRestoreTest extends TestHelper {
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
