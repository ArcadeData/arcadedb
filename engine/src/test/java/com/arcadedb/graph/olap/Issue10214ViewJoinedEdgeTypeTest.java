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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10214: a Graph Analytical View listing edge type {@code E} claimed an edge type {@code G} attached to {@code E} after the
 * scan ({@code ALTER TYPE G SUPERTYPE +E}), whose edges its CSR does not hold: a hop on {@code E}, which reads its sub-types'
 * slices, missed them. Such a type is now held only when the scan materialized it or proves it held no edge the CSR lacks; otherwise
 * the view is not served until it has rebuilt (OFF: it turns STALE).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10214ViewJoinedEdgeTypeTest {
  private static final String DB_PATH = "./target/databases/test-issue-10214";
  private static final String OUT_E   = "SELECT expand(out('E')) FROM V";
  private static final String GAV     = "CSR-accelerated via Graph Analytical View";

  private Database            database;
  private List<MutableVertex> vertices;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      vertices = new ArrayList<>();
      for (int i = 0; i < 4; i++)
        vertices.add(database.newVertex("V").set("pid", i).save());
      for (int i = 0; i < 4; i++)
        vertices.get(i).newEdge("E", vertices.get((i + 1) % 4));
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
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "OFF", "SYNCHRONOUS", "ASYNCHRONOUS" })
  void anExistingEdgeTypeWithEdgesAttachedAfterTheBuildIsNotServedUntilRebuilt(final GraphAnalyticalView.UpdateMode mode) {
    database.getSchema().createEdgeType("G");
    addEdges("G", 3);
    final GraphAnalyticalView view = createView(mode);
    assertServedAndRight(view, 4);

    final CountDownLatch gate = holdRebuilds(view);
    database.command("sql", "ALTER TYPE G SUPERTYPE +E").close();
    assertNotServedUntilRebuilt(view, mode, gate, 7);
  }

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aLightweightEdgeTypeAttachedAfterTheBuildIsNotServedUntilRebuilt(final GraphAnalyticalView.UpdateMode mode) {
    // NO RECORD TO COUNT: ONLY THE SCAN, WHICH MET ITS EDGES IN THE LISTS IT WALKED, KNOWS
    database.command("sql", "CREATE EDGE TYPE G LIGHTWEIGHT").close();
    addEdges("G", 2);
    final GraphAnalyticalView view = createView(mode);
    assertServedAndRight(view, 4);

    final CountDownLatch gate = holdRebuilds(view);
    database.command("sql", "ALTER TYPE G SUPERTYPE +E").close();
    assertNotServedUntilRebuilt(view, mode, gate, 6);
  }

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void edgesWrittenAfterTheBuildWhileTheTypeWasNotCoveredAreNotClaimed(final GraphAnalyticalView.UpdateMode mode) {
    // CREATED AFTER THE VIEW STARTED LISTENING: THE SCAN MEETS NONE OF ITS EDGES, ONLY THE LISTENERS HEAR THE ONES WRITTEN SINCE
    final GraphAnalyticalView view = createView(mode);
    database.getSchema().createEdgeType("G");
    addEdges("G", 2);
    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertServedAndRight(view, 4);

    final CountDownLatch gate = holdRebuilds(view);
    database.command("sql", "ALTER TYPE G SUPERTYPE +E").close();
    assertNotServedUntilRebuilt(view, mode, gate, 6);
  }

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "OFF", "SYNCHRONOUS", "ASYNCHRONOUS" })
  void anEmptyEdgeTypeThatExistedWhenTheViewStartedIsRebuiltOnceWhenAttached(final GraphAnalyticalView.UpdateMode mode) {
    // EMPTY AS FAR AS ANY COUNT SHOWS, BUT IT EXISTED WHEN THE LISTENERS WERE ARMED: A TRANSACTION OPEN THEN MAY HOLD ITS EDGES
    database.getSchema().createEdgeType("G");
    final GraphAnalyticalView view = createView(mode);

    final CountDownLatch gate = holdRebuilds(view);
    database.command("sql", "ALTER TYPE G SUPERTYPE +E").close();
    assertNotServedUntilRebuilt(view, mode, gate, 4);

    if (mode != GraphAnalyticalView.UpdateMode.OFF) {
      // ITS EDGES WRITTEN FROM NOW ON REACH THE OVERLAY
      addEdges("G", 2);
      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      assertServedAndRight(view, 6);
    }
  }

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "OFF", "SYNCHRONOUS", "ASYNCHRONOUS" })
  void anEmptyEdgeTypeCreatedAfterTheViewStartedIsServedAtOnceWhenAttached(final GraphAnalyticalView.UpdateMode mode) {
    final GraphAnalyticalView view = createView(mode);
    database.getSchema().createEdgeType("G");

    database.command("sql", "ALTER TYPE G SUPERTYPE +E").close();
    assertThat(view.isReady()).as("every edge of G would have been heard: nothing to rebuild").isTrue();
    assertServedAndRight(view, 4);
  }

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS" })
  void anEdgeCreatedInATransactionOpenBeforeTheViewExistedIsNotClaimed(final GraphAnalyticalView.UpdateMode mode)
      throws Exception {
    database.getSchema().createEdgeType("G");
    final CountDownLatch created = new CountDownLatch(1);
    final CountDownLatch built = new CountDownLatch(1);
    // CREATED BEFORE ANY LISTENER EXISTS, COMMITTED AFTER THE VIEW'S SCAN: NEITHER THE SCAN NOR THE LISTENERS SEE IT
    final Thread writer = new Thread(() -> database.transaction(() -> {
      final Vertex from = database.select().fromType("V").where().property("pid").eq().value(0).vertices().next();
      final Vertex to = database.select().fromType("V").where().property("pid").eq().value(2).vertices().next();
      from.newEdge("G", to);
      created.countDown();
      try {
        built.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }));
    writer.start();
    assertThat(created.await(30, TimeUnit.SECONDS)).isTrue();
    final GraphAnalyticalView view = createView(mode);
    built.countDown();
    writer.join(30_000);

    final CountDownLatch gate = holdRebuilds(view);
    database.command("sql", "ALTER TYPE G SUPERTYPE +E").close();
    assertNotServedUntilRebuilt(view, mode, gate, 5);
  }

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void anEdgeSubTypeCreatedAfterTheBuildIsServedWithoutARebuild(final GraphAnalyticalView.UpdateMode mode) {
    final GraphAnalyticalView view = createView(mode);

    database.command("sql", "CREATE EDGE TYPE K EXTENDS E").close();
    addEdges("K", 3);
    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(view.isReady()).isTrue();
    assertServedAndRight(view, 7);
  }

  /** Holds every rebuild of {@code view} before its scan until the latch returned is released, so its old CSR is what is asked. */
  private static CountDownLatch holdRebuilds(final GraphAnalyticalView view) {
    final CountDownLatch gate = new CountDownLatch(1);
    view.setBeforeBuildScanForTest(() -> {
      try {
        gate.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    return gate;
  }

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "OFF", "SYNCHRONOUS" })
  void aViewRestoredFromDiskStillHoldsTheSubTypesItsScanCovered(final GraphAnalyticalView.UpdateMode mode) {
    // K IS EMPTY WHEN SCANNED, SO THE CSR HAS NO SLICE FOR IT: ONLY THE PERSISTED SCAN COVERAGE SAYS IT WAS READ
    database.command("sql", "CREATE EDGE TYPE K EXTENDS E").close();
    createView(mode);
    database.close();
    database = new DatabaseFactory(DB_PATH).open();

    final GraphAnalyticalView restored = GraphAnalyticalViewRegistry.get(database, "g");
    assertThat(restored.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(restored.isRestoredFromPersistedCsr()).isTrue();
    assertThat(restored.isReady()).as("K was scanned: no rebuild").isTrue();
    assertServedAndRight(restored, 4);
    assertThat(restored.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
  }

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS" })
  void aViewRestoredFromDiskStillRefusesAnAttachedTypeItsScanMet(final GraphAnalyticalView.UpdateMode mode) {
    // G'S EDGES ARE IN THE LISTS THE SCAN WALKED: THE PERSISTED FILE REMEMBERS THEIR BUCKETS
    database.getSchema().createEdgeType("G");
    addEdges("G", 2);
    createView(mode);
    database.close();
    database = new DatabaseFactory(DB_PATH).open();
    final GraphAnalyticalView restored = GraphAnalyticalViewRegistry.get(database, "g");
    assertThat(restored.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(restored.isRestoredFromPersistedCsr()).isTrue();

    final CountDownLatch gate = holdRebuilds(restored);
    database.command("sql", "ALTER TYPE G SUPERTYPE +E").close();
    assertNotServedUntilRebuilt(restored, mode, gate, 6);
  }

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS" })
  void aTypeWrittenWhileNotCoveredAfterTheScanIsNotClaimedAfterAReopen(final GraphAnalyticalView.UpdateMode mode) {
    database.getSchema().createEdgeType("G");
    createView(mode);
    // WRITTEN AFTER THE SCAN, WHILE G IS NOT COVERED: THE COMMIT VOIDS THE PERSISTED CSR, SO THE REOPEN SCANS AND MEETS THEM
    addEdges("G", 2);
    database.close();
    database = new DatabaseFactory(DB_PATH).open();
    final GraphAnalyticalView restored = GraphAnalyticalViewRegistry.get(database, "g");
    assertThat(restored.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(restored.isRestoredFromPersistedCsr()).isFalse();

    final CountDownLatch gate = holdRebuilds(restored);
    database.command("sql", "ALTER TYPE G SUPERTYPE +E").close();
    assertNotServedUntilRebuilt(restored, mode, gate, 6);
  }

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS" })
  void anEdgeCreatedBeforeTheScanButCommittedAfterItIsNotClaimed(final GraphAnalyticalView.UpdateMode mode) throws Exception {
    // A VIEW TRACKING CHANGES FIRST, AND G CREATED AFTER IT, SO THE CREATION OF THE G EDGE BELOW IS HEARD BEFORE THE REBUILD
    // SAMPLES ITS MARK: ONLY THE COMMIT, AFTER THAT SCAN, CAN TELL IT WAS NOT READ
    final GraphAnalyticalView view = createView(mode);
    database.getSchema().createEdgeType("G");

    final CountDownLatch created = new CountDownLatch(1);
    final CountDownLatch scanned = new CountDownLatch(1);
    final Thread writer = new Thread(() -> database.transaction(() -> {
      final Vertex from = database.select().fromType("V").where().property("pid").eq().value(0).vertices().next();
      final Vertex to = database.select().fromType("V").where().property("pid").eq().value(2).vertices().next();
      from.newEdge("G", to);
      created.countDown();
      try {
        // COMMITTED ONLY ONCE THE REBUILD HAS SCANNED: THAT SCAN CANNOT HAVE READ THE EDGE
        scanned.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }));
    writer.start();
    assertThat(created.await(30, TimeUnit.SECONDS)).isTrue();
    database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW g").close();
    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    scanned.countDown();
    writer.join(30_000);

    final CountDownLatch gate = holdRebuilds(view);
    database.command("sql", "ALTER TYPE G SUPERTYPE +E").close();
    assertNotServedUntilRebuilt(view, mode, gate, 5);
  }

  private void assertNotServedUntilRebuilt(final GraphAnalyticalView view, final GraphAnalyticalView.UpdateMode mode,
      final CountDownLatch gate, final long expectedEdges) {
    try {
      assertThat(view.coversEdgeType("G")).as("the schema-only check is unchanged").isTrue();
      assertThat(view.isReady()).as("the CSR holds none of G's edges").isFalse();
      assertThat(view.coversEveryEdge()).isFalse();
      assertThat(outE()).as("answered from the records").isEqualTo(expectedEdges);
    } finally {
      gate.countDown();
      view.setBeforeBuildScanForTest(null);
    }

    if (mode == GraphAnalyticalView.UpdateMode.OFF) {
      Awaitility.await("the OFF view turns STALE").atMost(30, TimeUnit.SECONDS)
          .until(() -> view.getStatus() == GraphAnalyticalView.Status.STALE);
      database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW g").close();
    }
    Awaitility.await("the rebuild read G").atMost(30, TimeUnit.SECONDS).until(() -> view.awaitReady(1, TimeUnit.SECONDS) && view.isReady());
    assertServedAndRight(view, expectedEdges);
  }

  private void assertServedAndRight(final GraphAnalyticalView view, final long expectedEdges) {
    assertThat(view.isReady()).isTrue();
    try (final ResultSet rs = database.query("sql", OUT_E)) {
      long rows = 0;
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
      assertThat(rows).isEqualTo(expectedEdges);
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains(GAV);
    }
  }

  private long outE() {
    try (final ResultSet rs = database.query("sql", OUT_E)) {
      long rows = 0;
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).doesNotContain(GAV);
      return rows;
    }
  }

  /** {@code count} edges of {@code type} between the V vertices, each from vertex i to vertex i + 2. */
  private void addEdges(final String type, final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final Vertex from = database.select().fromType("V").where().property("pid").eq().value(i).vertices().next();
        final Vertex to = database.select().fromType("V").where().property("pid").eq().value((i + 2) % 4).vertices().next();
        from.newEdge(type, to);
      }
    });
  }

  private GraphAnalyticalView createView(final GraphAnalyticalView.UpdateMode mode) {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (V) EDGE TYPES (E) UPDATE MODE " + mode).close();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    return view;
  }
}
