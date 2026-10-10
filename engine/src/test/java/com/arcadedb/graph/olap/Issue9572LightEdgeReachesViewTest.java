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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9572: a lightweight edge has no record, so the record listeners a Graph Analytical View learns about changes
 * through never heard of its creation. A {@code SYNCHRONOUS} view answered as if the edge did not exist, and a
 * transaction that only created lightweight edges left an {@code OFF} view READY and an {@code ASYNCHRONOUS} one
 * without the rebuild it needed. Every assertion is checked against the same query once the view is dropped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9572LightEdgeReachesViewTest extends TestHelper {
  private static final String COUNT     = "MATCH (a:P)-[:K]->(b:P) RETURN count(*) AS n";
  private static final String ROW_COUNT = "MATCH (a:P)-[:K]->(b:P) WITH * RETURN count(*) AS n";

  @Test
  void synchronousViewSeesLightEdgesCreatedAfterTheBuild() throws Exception {
    createSchema("K");
    createView("SYNCHRONOUS");

    database.transaction(() -> {
      final MutableVertex c = database.newVertex("P").set("name", "c").save();
      vertex("a").newLightEdge("K", c);
      vertex("a").newEdge("K", c);
    });
    assertCounts(3);

    // A self loop, alone in its transaction
    database.transaction(() -> vertex("a").newLightEdge("K", vertex("a")));
    assertCounts(4);
    assertThat(view().getEdgeCount()).isEqualTo(4);
    assertThat(view().getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);

    dropViewAndAssertCounts(4);
  }

  @Test
  void synchronousViewSeesEdgesOfALightweightTypeFromEveryCreationPath() throws Exception {
    createSchema("K LIGHTWEIGHT");
    createView("SYNCHRONOUS");

    // Java API, SQL and Cypher all store the type's edges as lightweight ones
    database.transaction(() -> vertex("b").newEdge("K", vertex("a")));
    assertCounts(2);
    database.transaction(
        () -> database.command("sql", "CREATE EDGE K FROM (SELECT FROM P WHERE name = 'b') TO (SELECT FROM P WHERE name = 'b')"));
    assertCounts(3);
    database.transaction(() -> database.command("opencypher", "MATCH (a:P {name: 'a'}) CREATE (a)-[:K]->(:P {name: 'd'})"));
    assertCounts(4);

    dropViewAndAssertCounts(4);
  }

  @Test
  void synchronousViewDropsLightEdgesDeletedAfterTheBuild() throws Exception {
    createSchema("K LIGHTWEIGHT");
    createView("SYNCHRONOUS");

    database.transaction(() -> vertex("b").newEdge("K", vertex("a")));
    assertCounts(2);

    // One the overlay added, one from the base CSR
    database.transaction(() -> outEdges("b").getFirst().delete());
    assertCounts(1);
    database.transaction(() -> outEdges("a").getFirst().delete());
    assertCounts(0);
    assertThat(view().getEdgeCount()).isZero();

    dropViewAndAssertCounts(0);
  }

  /**
   * Two lightweight edges of one type over the same pair share their identity, which is the triple. Each is still an
   * edge of its own to a query (issue #9573), so the view has to count both, and a delete removes exactly one of them.
   */
  @Test
  void synchronousViewCountsEveryCopyOfADuplicatedLightEdge() throws Exception {
    createSchema("K");
    createView("SYNCHRONOUS");

    database.transaction(() -> {
      vertex("b").newLightEdge("K", vertex("a"));
      vertex("b").newLightEdge("K", vertex("a"));
    });
    assertCounts(3);
    database.transaction(() -> vertex("b").newLightEdge("K", vertex("a")));
    assertCounts(4);

    database.transaction(() -> outEdges("b").getFirst().delete());
    assertCounts(3);
    database.transaction(() -> {
      outEdges("b").getFirst().delete();
      outEdges("b").getFirst().delete();
    });
    assertCounts(1);

    // A copy added and deleted in one transaction is no change at all
    database.transaction(() -> {
      vertex("b").newLightEdge("K", vertex("a"));
      vertex("b").newLightEdge("K", vertex("a"));
      outEdges("b").getFirst().delete();
    });
    assertCounts(2);

    dropViewAndAssertCounts(2);
  }

  /** A twin of a base edge: deleting one copy, whichever the delete finds first, leaves exactly one. */
  @Test
  void synchronousViewKeepsTheSurvivingCopyOfABaseLightEdge() throws Exception {
    createSchema("K");
    database.transaction(() -> vertex("a").newLightEdge("K", vertex("b")));
    createView("SYNCHRONOUS");
    assertCounts(2);

    database.transaction(() -> vertex("a").newLightEdge("K", vertex("b")));
    assertCounts(3);
    database.transaction(() -> outEdges("a").getFirst().delete());
    assertCounts(2);
    database.transaction(() -> outEdges("a").getFirst().delete());
    assertCounts(1);

    dropViewAndAssertCounts(1);
  }

  /**
   * The light edge goes between vertices whose edge lists in both directions already exist: the append then rewrites
   * no vertex record, so no record event of any kind reaches the view and only the light-edge hook can tell it.
   */
  @Test
  void offViewGoesStaleOnATransactionThatOnlyCreatesLightEdges() throws Exception {
    createSchema("K LIGHTWEIGHT");
    database.transaction(() -> vertex("b").newEdge("K", vertex("a")));
    createView("OFF");

    database.transaction(() -> vertex("a").newEdge("K", vertex("a")));
    assertThat(view().getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);

    database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW v1");
    assertThat(view().awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertCounts(3);

    database.transaction(() -> outEdges("b").getFirst().delete());
    assertThat(view().getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);

    dropViewAndAssertCounts(2);
  }

  @Test
  void asynchronousViewRebuildsOnATransactionThatOnlyCreatesLightEdges() throws Exception {
    createSchema("K LIGHTWEIGHT");
    database.transaction(() -> vertex("b").newEdge("K", vertex("a")));
    createView("ASYNCHRONOUS");

    database.transaction(() -> vertex("a").newEdge("K", vertex("a")));
    awaitEdgeCount(3);
    assertCounts(3);

    database.transaction(() -> outEdges("b").getFirst().delete());
    awaitEdgeCount(2);
    assertCounts(2);

    dropViewAndAssertCounts(2);
  }

  /**
   * The case the reconciliation cannot settle by counting, driven through a real rebuild: a duplicate of a copy older
   * than the build, created by a transaction that reports it before the scan reads its source and commits after the
   * rebuild published. The counts take the new copy as one the scan read; the view checks the pair against the graph,
   * finds one copy missing and rebuilds, so it never stays READY one copy short.
   */
  @Test
  void aDuplicateRacingARebuildIsNotLeftOutOfTheView() throws Exception {
    createSchema("K");
    database.transaction(() -> vertex("a").newLightEdge("K", vertex("b")));
    createView("SYNCHRONOUS");
    assertCounts(2);

    final CountDownLatch reported = new CountDownLatch(1);
    final CountDownLatch commit = new CountDownLatch(1);
    final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        database.transaction(() -> {
          vertex("a").newLightEdge("K", vertex("b"));
          reported.countDown();
          try {
            commit.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
      } catch (final Throwable t) {
        writerFailure.set(t);
        reported.countDown();
      }
    }, "issue9572-duplicate-writer");

    final GraphAnalyticalView view = view();
    view.setBeforeBuildScanForTest(() -> {
      writer.start();
      try {
        assertThat(reported.await(60, TimeUnit.SECONDS)).isTrue();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    try {
      view.build();
    } finally {
      view.setBeforeBuildScanForTest(null);
      commit.countDown();
    }
    writer.join(60_000);
    assertThat(writerFailure.get()).isNull();

    final long deadline = System.currentTimeMillis() + 60_000;
    while (!(view.getStatus() == GraphAnalyticalView.Status.READY && view.getEdgeCount() == 3)
        && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(view.getEdgeCount()).as("the view must end up with every copy").isEqualTo(3);
    assertCounts(3);
    dropViewAndAssertCounts(3);
  }

  /** A dropped view stops listening for light edges, as it stops listening for records. */
  @Test
  void droppingTheViewUnregistersItsLightEdgeListener() throws Exception {
    createSchema("K LIGHTWEIGHT");
    final int before = edgeWriteListeners();
    createView("SYNCHRONOUS");
    assertThat(edgeWriteListeners()).isEqualTo(before + 1);

    database.command("sql", "ALTER GRAPH ANALYTICAL VIEW v1 UPDATE MODE OFF");
    assertThat(edgeWriteListeners()).as("a mode change replaces the listener").isEqualTo(before + 1);

    dropViewAndAssertCounts(1);
    assertThat(edgeWriteListeners()).isEqualTo(before);
  }

  private int edgeWriteListeners() {
    return ((DatabaseInternal) database).getGraphEngine().getEdgeWriteListenerCount();
  }

  /** A rolled back light edge never reaches the view. */
  @Test
  void rolledBackLightEdgeLeavesTheViewUntouched() throws Exception {
    createSchema("K LIGHTWEIGHT");
    createView("SYNCHRONOUS");

    database.begin();
    vertex("b").newEdge("K", vertex("a"));
    database.rollback();
    assertCounts(1);

    database.transaction(() -> vertex("a").newEdge("K", vertex("a")));
    assertCounts(2);

    dropViewAndAssertCounts(2);
  }

  /** A compaction folds the overlay's light edges into a fresh base, which must then hold each of them once. */
  @Test
  void lightEdgesAddedAfterTheBuildSurviveACompaction() throws Exception {
    createSchema("K LIGHTWEIGHT");
    createView("SYNCHRONOUS");
    view().setCompactionThreshold(1);

    database.transaction(() -> {
      vertex("b").newEdge("K", vertex("a"));
      vertex("b").newEdge("K", vertex("b"));
    });
    assertCounts(3);

    final long deadline = System.currentTimeMillis() + 60_000;
    while (Boolean.TRUE.equals(view().getStats().get("overlayActive")) && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(view().getStats().get("overlayActive")).as("the compaction must have folded the overlay").isEqualTo(false);
    assertCounts(3);
    assertThat(view().getEdgeCount()).isEqualTo(3);
    view().setCompactionThreshold(GraphAnalyticalView.DEFAULT_COMPACTION_THRESHOLD);

    database.transaction(() -> outEdges("b").getFirst().delete());
    assertCounts(2);

    dropViewAndAssertCounts(2);
  }

  private void createSchema(final String edgeTypeDeclaration) {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE PROPERTY P.name STRING");
    database.command("sql", "CREATE EDGE TYPE " + edgeTypeDeclaration);
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").set("name", "a").save();
      final MutableVertex b = database.newVertex("P").set("name", "b").save();
      a.newEdge("K", b);
    });
  }

  private void createView(final String updateMode) throws InterruptedException {
    database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW v1 VERTEX TYPES (P) EDGE TYPES (K) UPDATE MODE " + updateMode);
    assertThat(view().awaitReady(60, TimeUnit.SECONDS)).isTrue();
  }

  private GraphAnalyticalView view() {
    return GraphAnalyticalViewRegistry.get(database, "v1");
  }

  private void awaitEdgeCount(final int expected) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 60_000;
    while (System.currentTimeMillis() < deadline) {
      final GraphAnalyticalView view = view();
      if (view.getStatus() == GraphAnalyticalView.Status.READY && view.getEdgeCount() == expected)
        return;
      Thread.sleep(20);
    }
    assertThat(view().getEdgeCount()).as("the asynchronous rebuild must pick the light edges up").isEqualTo(expected);
  }

  private void assertCounts(final long expected) {
    assertThat(count(COUNT)).as(COUNT).isEqualTo(expected);
    assertThat(count(ROW_COUNT)).as(ROW_COUNT).isEqualTo(expected);
  }

  private void dropViewAndAssertCounts(final long expected) {
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW v1");
    assertCounts(expected);
  }

  private long count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private List<Edge> outEdges(final String name) {
    final List<Edge> edges = new ArrayList<>();
    for (final Edge e : vertex(name).getEdges(Vertex.DIRECTION.OUT, "K"))
      edges.add(e);
    return edges;
  }

  private MutableVertex vertex(final String name) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM P WHERE name = ?", name)) {
      return rs.next().getVertex().get().modify();
    }
  }
}
