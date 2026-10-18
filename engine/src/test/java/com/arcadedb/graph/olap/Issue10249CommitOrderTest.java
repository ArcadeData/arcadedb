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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10249: the changes of two transactions that conflict reach a Graph Analytical View in the order the transactions
 * committed, even when the thread of the second one gets to the view first.
 * <p>
 * The first transaction deletes an edge and is held after its commit, before it reports to the view; the second one creates an
 * edge in the slot the first freed (the same RID) and commits meanwhile. Reported in the opposite order, the creation replaces the
 * deleted edge in the view and the deletion that follows withdraws the new one. The randomized version of the scenario is
 * {@link Issue10249ConcurrentEdgeDeletesTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10249CommitOrderTest extends TestHelper {
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
