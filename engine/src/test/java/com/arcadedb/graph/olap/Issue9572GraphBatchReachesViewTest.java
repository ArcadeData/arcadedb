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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9572, the bulk side: {@link GraphBatch} writes its edges straight into the edge lists - a lightweight edge
 * has no record, and a regular one is created in bulk - so no record event reports them either.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9572GraphBatchReachesViewTest extends TestHelper {
  private static final String COUNT = "MATCH (a:P)-[:K]->(b:P) RETURN count(*) AS n";

  @Test
  void lightweightEdgesOfABatchReachASynchronousView() throws Exception {
    assertBatchReachesView("K LIGHTWEIGHT", "SYNCHRONOUS");
  }

  @Test
  void regularEdgesOfABatchReachASynchronousView() throws Exception {
    assertBatchReachesView("K", "SYNCHRONOUS");
  }

  @Test
  void lightweightEdgesOfABatchReachAnOffView() throws Exception {
    assertBatchReachesView("K LIGHTWEIGHT", "OFF");
  }

  @Test
  void regularEdgesOfABatchReachAnAsynchronousView() throws Exception {
    assertBatchReachesView("K", "ASYNCHRONOUS");
  }

  /**
   * A batch writing only a type the view does not cover leaves it alone; one writing that type and a covered one, in
   * either storage shape, rebuilds it.
   */
  @Test
  void onlyABatchWritingACoveredTypeRebuildsTheView() throws Exception {
    final RID[] v = createGraph("K", "SYNCHRONOUS");
    database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v1");
    final long builtAt = view.getBuildTimestamp();

    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      batch.newEdge(v[1], "L", v[0]);
    }
    assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
    assertThat(view.getBuildTimestamp()).as("no rebuild for an uncovered type").isEqualTo(builtAt);

    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      batch.newEdge(v[2], "L", v[1]);
      batch.newEdge(v[1], "K", v[0]);
    }
    assertThat(count()).isEqualTo(4L);
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertThat(view.getEdgeCount()).isEqualTo(4);
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW v1");
  }

  /** A listener that fails, told before the view, neither fails the batch nor keeps the view from rebuilding. */
  @Test
  void aFailingListenerDoesNotKeepTheViewFromTheBatch() throws Exception {
    final GraphEngine graphEngine = ((DatabaseInternal) database).getGraphEngine();
    final GraphEngine.EdgeWriteListener failing = new GraphEngine.EdgeWriteListener() {
      @Override
      public void onLightEdgeCreated(final Edge edge) {
      }

      @Override
      public void onEdgesWrittenInBulk(final Set<String> edgeTypeNames) {
        throw new IllegalStateException("listener failure the batch must survive");
      }
    };
    graphEngine.registerEdgeWriteListener(failing);
    try {
      final RID[] v = createGraph("K LIGHTWEIGHT", "SYNCHRONOUS");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v1");
      try (final GraphBatch batch = GraphBatch.builder(database).build()) {
        batch.newEdge(v[1], "K", v[0]);
      }
      assertThat(count()).isEqualTo(4L);
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      assertThat(view.getEdgeCount()).isEqualTo(4);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW v1");
    } finally {
      graphEngine.unregisterEdgeWriteListener(failing);
    }
  }

  /** A second batch on a view that a first batch sent rebuilding, before that rebuild is awaited. */
  @Test
  void twoBatchesInARowLeaveASynchronousViewComplete() throws Exception {
    final RID[] v = createGraph("K LIGHTWEIGHT", "SYNCHRONOUS");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v1");
    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      batch.newEdge(v[1], "K", v[0]);
    }
    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      batch.newEdge(v[2], "K", v[1]);
    }
    assertThat(count()).isEqualTo(5L);
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertThat(view.getEdgeCount()).isEqualTo(5);
    assertThat(count()).isEqualTo(5L);
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW v1");
  }

  private void assertBatchReachesView(final String edgeType, final String updateMode) throws Exception {
    final RID[] v = createGraph(edgeType, updateMode);
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "v1");

    // Every vertex already has an edge list with room in both directions, so the batch only appends to them: it
    // rewrites no vertex record, and no record event of any kind fires
    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      batch.newEdge(v[1], "K", v[0]);
      batch.newEdge(v[2], "K", v[1]);
    }

    // Right after the batch: no view may answer without its edges
    assertThat(count()).isEqualTo(5L);
    if ("OFF".equals(updateMode))
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);
    else {
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      assertThat(view.getEdgeCount()).isEqualTo(5);
      assertThat(count()).isEqualTo(5L);
    }

    database.command("sql", "DROP GRAPH ANALYTICAL VIEW v1");
    assertThat(count()).isEqualTo(5L);
  }

  /** A triangle a -> b -> c -> a: every vertex has an outgoing and an incoming edge list. */
  private RID[] createGraph(final String edgeType, final String updateMode) throws InterruptedException {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE " + edgeType);
    final RID[] v = new RID[3];
    database.transaction(() -> {
      for (int i = 0; i < v.length; i++)
        v[i] = database.newVertex("P").save().getIdentity();
      for (int i = 0; i < v.length; i++)
        v[i].asVertex().modify().newEdge("K", v[(i + 1) % v.length]);
    });
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW v1 VERTEX TYPES (P) EDGE TYPES (K) UPDATE MODE " + updateMode);
    assertThat(GraphAnalyticalViewRegistry.get(database, "v1").awaitReady(60, TimeUnit.SECONDS)).isTrue();
    return v;
  }

  private long count() {
    try (final ResultSet rs = database.query("opencypher", COUNT)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
