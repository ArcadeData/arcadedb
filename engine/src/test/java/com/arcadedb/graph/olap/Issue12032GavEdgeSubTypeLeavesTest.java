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
import com.arcadedb.graph.DenseNodeIdProvider;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12032: a Graph Analytical View over an edge type {@code E0} kept the edges of a sub-type {@code E1} after
 * {@code DROP TYPE E1 UNSAFE} or {@code ALTER TYPE E1 SUPERTYPE -E0}. It stayed READY with the old edge count, and a Cypher
 * {@code count(*)} it answered still counted them: the renumbered neighbour views kept with the snapshot were held by the type
 * name asked rather than by the slices it resolves to, and nothing told the view a slice it holds had left its types.
 */
class Issue12032GavEdgeSubTypeLeavesTest extends TestHelper {
  private static final String COUNT       = "MATCH (a:V0)-[e:E0]->(b:V0) RETURN count(*) AS c";
  private static final String SUM         = "MATCH (a:V0)-[e:E0]->(b:V0) RETURN sum(1) AS c";
  private static final String CHAIN_COUNT = "MATCH (a:V)-[e:E0]->(b:V) RETURN count(*) AS c";
  private static final String VIEW_HOP    = "GAVExpandAll";

  /** DROP TYPE ... UNSAFE leaves the vertices' edge lists pointing at the dropped edges, by design. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @ParameterizedTest
  @ValueSource(strings = { "DROP TYPE E1 UNSAFE", "ALTER TYPE E1 SUPERTYPE -E0" })
  void theCountAnsweredFromTheViewLeavesOutTheEdgesOfASubTypeThatLeft(final String change) {
    final GraphAnalyticalView view = overlayWithHoles();
    database.query("opencypher", "MATCH (a:W)-[e:E0]->(b:W) RETURN count(*) AS c").close();
    assertThat(cypherPlan(COUNT)).contains(VIEW_HOP);
    assertThat(count(COUNT)).isEqualTo(1L);

    database.command("sql", change).close();

    assertThat(sqlCount("SELECT count(*) AS c FROM E0")).isZero();
    assertThat(count(SUM)).isZero();
    assertThat(count(COUNT)).isZero();

    Awaitility.await("the rebuild").atMost(30, TimeUnit.SECONDS).until(() -> view.awaitReady(1, TimeUnit.SECONDS) && view.isReady());
    assertThat(view.getEdgeCount()).isZero();
    assertThat(count(COUNT)).isZero();
  }

  @Test
  void aRenumberedViewKeptWithTheSnapshotFollowsTheSubTypesOfTheTypeAsked() {
    final GraphAnalyticalView view = overlayWithHoles();
    final GraphAnalyticalView.Pinned pinned = view.pinned();
    final GraphTraversalProvider dense = DenseNodeIdProvider.wrap(pinned);
    assertThat(dense).as("a deleted vertex leaves a hole: the ids are renumbered").isInstanceOf(DenseNodeIdProvider.class);
    assertThat(entries(dense.getNeighborView(Vertex.DIRECTION.OUT, "E0"))).isEqualTo(1);

    // NO COMMIT: THE SNAPSHOT, AND THE RENUMBERING KEPT WITH IT, STAY
    database.command("sql", "DROP TYPE E1 UNSAFE").close();
    final GraphTraversalProvider again = DenseNodeIdProvider.wrap(pinned);
    assertThat(again).isSameAs(dense);
    assertThat(entries(pinned.getNeighborView(Vertex.DIRECTION.OUT, "E0"))).isZero();
    assertThat(entries(again.getNeighborView(Vertex.DIRECTION.OUT, "E0"))).isZero();
  }

  @ParameterizedTest
  @CsvSource({ "DROP TYPE E1 UNSAFE, SYNCHRONOUS", "ALTER TYPE E1 SUPERTYPE -E0, SYNCHRONOUS", "DROP TYPE E1 UNSAFE, OFF",
      "ALTER TYPE E1 SUPERTYPE -E0, OFF" })
  void aViewHoldingTheEdgesOfASubTypeThatLeftIsNotServedUntilRebuilt(final String change, final String mode) {
    final GraphAnalyticalView view = edgesOfASubTypeInTheBaseCsr(mode);

    final CountDownLatch gate = new CountDownLatch(1);
    view.setBeforeBuildScanForTest(() -> {
      try {
        gate.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    try {
      database.command("sql", change).close();
      assertThat(view.isReady()).as("the CSR still holds E1's edges").isFalse();
      final Result row = schemaRow();
      assertThat(row.<String>getProperty("status")).as("not READY with E1's edges counted").isNotEqualTo("READY");
      assertThat(cypherPlan(CHAIN_COUNT)).doesNotContain(VIEW_HOP);
      assertThat(count(CHAIN_COUNT)).isEqualTo(3L);
    } finally {
      gate.countDown();
      view.setBeforeBuildScanForTest(null);
    }

    if ("OFF".equals(mode)) {
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.STALE);
      database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW G").close();
    }
    Awaitility.await("the rebuild").atMost(30, TimeUnit.SECONDS).until(() -> view.awaitReady(1, TimeUnit.SECONDS) && view.isReady());
    assertThat(view.getEdgeCount()).isEqualTo(3);
    assertThat(schemaRow().<Number>getProperty("edgeCount").intValue()).isEqualTo(3);
    assertThat(cypherPlan(CHAIN_COUNT)).contains(VIEW_HOP);
    assertThat(count(CHAIN_COUNT)).isEqualTo(3L);
    assertThat(count("MATCH (a:V)-[e:E0]->(b:V) RETURN sum(1) AS c")).isEqualTo(3L);
  }

  /**
   * A DDL statement in an open transaction changes the types at once and saves the schema, which moves its version, at the commit:
   * a verdict "every slice is still covered" taken before it must not be served until then.
   */
  @ParameterizedTest
  @CsvSource({ "ALTER TYPE E1 SUPERTYPE -E0, 3", "DROP TYPE E1 UNSAFE, 3", "ALTER TYPE E1 NAME E1R, 6" })
  void aChangeInsideAnOpenTransactionIsNotAnsweredFromTheVerdictOfBefore(final String change, final long edgesOfE0AndItsSubTypes) {
    final GraphAnalyticalView view = edgesOfASubTypeInTheBaseCsr("SYNCHRONOUS");
    assertThat(view.isReady()).as("every slice is covered: the verdict the change must not be answered from").isTrue();

    database.begin();
    try {
      database.command("sql", change).close();
      assertThat(view.isReady()).as("the CSR holds a slice named E1, which is no longer a type of the view").isFalse();
      assertThat(count(CHAIN_COUNT)).isEqualTo(edgesOfE0AndItsSubTypes);
    } finally {
      database.commit();
    }

    Awaitility.await("the rebuild").atMost(30, TimeUnit.SECONDS).until(() -> view.awaitReady(1, TimeUnit.SECONDS) && view.isReady());
    assertThat(view.getEdgeCount()).isEqualTo(edgesOfE0AndItsSubTypes);
    assertThat(count(CHAIN_COUNT)).isEqualTo(edgesOfE0AndItsSubTypes);
  }

  /** E0 and its sub-type E1, 3 edges each, in the base CSR of a view over E0 (the table 2 of the issue). */
  private GraphAnalyticalView edgesOfASubTypeInTheBaseCsr(final String mode) {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.command("sql", "CREATE EDGE TYPE E0").close();
    database.command("sql", "CREATE EDGE TYPE E1 EXTENDS E0").close();
    for (int i = 0; i < 4; i++)
      write("INSERT INTO V SET i = " + i);
    for (int i = 0; i < 3; i++) {
      write("CREATE EDGE E0 FROM (SELECT FROM V WHERE i = " + i + ") TO (SELECT FROM V WHERE i = " + (i + 1) + ")");
      write("CREATE EDGE E1 FROM (SELECT FROM V WHERE i = " + (i + 1) + ") TO (SELECT FROM V WHERE i = " + i + ")");
    }
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW G VERTEX TYPES (V) EDGE TYPES (E0) UPDATE MODE " + mode).close();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "G");
    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(view.getEdgeCount()).isEqualTo(6);
    return view;
  }

  /**
   * The issue's minimized trace: an E1 edge only the overlay holds, and a vertex inserted and deleted, which leaves a hole in the
   * view's ids so the count reads the renumbered views kept with the snapshot.
   */
  private GraphAnalyticalView overlayWithHoles() {
    for (final String statement : new String[] { "CREATE VERTEX TYPE V0", "CREATE VERTEX TYPE V1 EXTENDS V0",
        "CREATE VERTEX TYPE V3 EXTENDS V0", "CREATE VERTEX TYPE W", "CREATE EDGE TYPE E0", "CREATE EDGE TYPE E1 EXTENDS E0" })
      database.command("sql", statement).close();
    write("INSERT INTO V1 SET uid = 'a'");
    write("INSERT INTO V0 SET uid = 'b'");
    write("INSERT INTO W SET uid = 'w'");
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW G0 VERTEX TYPES (V0, W) EDGE TYPES (E0) UPDATE MODE SYNCHRONOUS").close();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "G0");
    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    write("CREATE EDGE E1 FROM (SELECT FROM V1 WHERE uid = 'a') TO (SELECT FROM V0 WHERE uid = 'b')");
    write("INSERT INTO V3 SET uid = 'c'");
    write("DELETE VERTEX FROM V3 WHERE uid = 'c'");
    return view;
  }

  /** One statement in its own transaction, committed before the next: what the view hears one commit at a time. */
  private void write(final String statement) {
    database.transaction(() -> database.command("sql", statement).close());
  }

  private static int entries(final NeighborView view) {
    if (view == null)
      return 0;
    int entries = 0;
    for (int v = 0; v < view.nodeCount(); v++)
      entries += view.degree(v);
    return entries;
  }

  private Result schemaRow() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:graphAnalyticalViews")) {
      return rs.next();
    }
  }

  private long count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      final Number n = rs.next().getProperty("c");
      return n == null ? 0L : n.longValue();
    }
  }

  private long sqlCount(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private String cypherPlan(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + cypher)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
