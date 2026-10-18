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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10249: when several threads commit at the same time and some of the commits delete edges, a Graph Analytical View
 * ended up a few edges short - once the writers were done and the view READY, the counts it served were lower than the same
 * queries without the view. A reopened database and a view built from scratch gave the right counts, so only the live view
 * was wrong.
 * <p>
 * Four threads each commit 250 transactions of 1 to 6 operations: edge inserts, deletes of edges the thread created itself
 * and updates of a vertex property. Edges of one thread end on vertices another thread's edges also end on, so the commits
 * of different threads share the adjacency of those vertices.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10249ConcurrentEdgeDeletesTest extends TestHelper {
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
      // The first transaction deletes the edge. A callback of its own, registered before the view's, holds its thread after
      // the commit, once the locks are released and before the view hears of it: the window the commits race in
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

      // The second one creates edges, and the slot the first freed is the first one the bucket hands out again: one of the new
      // edges has the RID of the deleted one
      final Future<Boolean> creator = executor.submit(() -> {
        final boolean[] reused = new boolean[1];
        database.transaction(() -> {
          for (int i = 0; i < 3; i++)
            reused[0] |= vertices[0].asVertex().newEdge("E", vertices[2].asVertex()).save().getIdentity().equals(edge[0]);
        });
        return reused[0];
      });
      // It is committed, but its change is not given to the view ahead of the deletion's, which came first
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
    assertThat(servedByView(QUERIES[0])).as("the count is served by the view").isTrue();
    final long edges;
    try (final ResultSet rs = database.query("sql", "SELECT count() AS n FROM E")) {
      edges = rs.next().<Number>getProperty("n").longValue();
    }
    assertThat(edges).isEqualTo(3);
    assertThat(answers().getFirst()).as("edges the view counts").isEqualTo("3");
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
