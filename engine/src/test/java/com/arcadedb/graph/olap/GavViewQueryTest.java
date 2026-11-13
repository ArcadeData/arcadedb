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
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.NodeLocalFiles;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry.ViewsPassedOver;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.GraphViewPins;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.optimizer.plan.PhysicalPlan;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.CreateGraphAnalyticalViewStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests of GavViewQuery, one nested class per reported case.
 */
class GavViewQueryTest {
  /**
   * Regression for #10208: a Graph Analytical View declared over a vertex type left that type's sub-types out of its CSR, while an edge type
   * brought its sub-types along. Once a sub-type held a vertex the view no longer covered every vertex, so the planner never handed
   * it a query: the view was built and kept up to date for nothing. A vertex type now brings its sub-types, as an edge type does and
   * as a query on the type does on the record path.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ViewVertexSubtypes {
    private static final String DB_PATH = "./target/databases/test-issue-10208";
    private static final String ONE_HOP = "MATCH (a:V)-[:E]->(b:V) RETURN count(*) AS n";
    // Over the leaf edge type F: two hops on a type with sub-types are kept off the view for relationship uniqueness (#8394)
    private static final String TWO_HOP = "MATCH (a:V)-[:F]->(b:V)-[:F]->(c:V) RETURN count(*) AS n";

    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("V");
      database.getSchema().createVertexType("W").addSuperType("V");
      database.getSchema().createEdgeType("E");
      database.getSchema().createEdgeType("F").addSuperType("E");
      database.transaction(() -> {
        final List<MutableVertex> vertices = new ArrayList<>();
        for (int i = 0; i < 4; i++)
          vertices.add(database.newVertex("V").set("pid", i).save());
        for (int i = 4; i < 6; i++)
          vertices.add(database.newVertex("W").set("pid", i).save());
        for (int i = 0; i < 6; i++) {
          vertices.get(i).newEdge("E", vertices.get((i + 1) % 6));
          vertices.get(i).newEdge(i % 2 == 0 ? "F" : "E", vertices.get((i + 2) % 6));
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

    @ParameterizedTest
    @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void aViewOverASuperTypeHoldsItsSubTypesAndServesQueries(final GraphAnalyticalView.UpdateMode mode) {
      final GraphAnalyticalView view = createView(mode);

      assertThat(view.getNodeCount()).as("the 4 V and the 2 W vertices").isEqualTo(6);
      assertThat(view.coversVertexType("W")).isTrue();
      assertThat(view.coversVertexType(null)).isTrue();

      assertServedByTheViewAndRight();
    }

    @ParameterizedTest
    @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void changesToASubTypeReachTheView(final GraphAnalyticalView.UpdateMode mode) {
      final GraphAnalyticalView view = createView(mode);

      // a vertex of the existing sub-type, and one of a sub-type created after the build
      database.getSchema().createVertexType("X").addSuperType("W");
      database.transaction(() -> {
        final MutableVertex w = database.newVertex("W").set("pid", 6).save();
        final MutableVertex x = database.newVertex("X").set("pid", 7).save();
        final MutableVertex first = database.select().fromType("V").where().property("pid").eq().value(0).vertices().next()
            .modify();
        first.newEdge("E", w);
        w.newEdge("F", x);
        x.newEdge("E", first);
      });
      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();

      assertThat(view.getNodeCount()).isEqualTo(8);
      assertThat(view.coversVertexType("X")).isTrue();
      assertServedByTheViewAndRight();

      // and the deletion of a sub-type's vertex
      database.transaction(() -> database.select().fromType("W").where().property("pid").eq().value(4).vertices().next().delete());
      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      assertServedByTheViewAndRight();
    }

    @ParameterizedTest
    @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void aTypeWithSeveralSuperTypesIsHeldThroughTheListedOne(final GraphAnalyticalView.UpdateMode mode) {
      // M is a V (through W) and an unlisted Other; Deep is three levels below V
      database.getSchema().createVertexType("Other");
      database.getSchema().createVertexType("M").addSuperType("W").addSuperType("Other");
      database.getSchema().createVertexType("Deep").addSuperType("M");
      database.transaction(() -> {
        final MutableVertex m = database.newVertex("M").set("pid", 10).save();
        final MutableVertex deep = database.newVertex("Deep").set("pid", 11).save();
        final MutableVertex first = database.select().fromType("V").where().property("pid").eq().value(0).vertices().next()
            .modify();
        first.newEdge("E", m);
        m.newEdge("F", deep);
        deep.newEdge("E", first);
      });
      final GraphAnalyticalView view = createView(mode);

      assertThat(view.getNodeCount()).isEqualTo(8);
      assertThat(view.coversVertexType("M")).isTrue();
      assertThat(view.coversVertexType("Deep")).isTrue();
      assertThat(view.coversVertexType("Other")).isFalse();
      assertThat(view.coversVertexType(null)).as("Other holds no vertex of its own").isTrue();
      assertServedByTheViewAndRight();
    }

    @ParameterizedTest
    @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS" })
    void aViewOverTheSubTypeAloneLeavesTheSuperTypeOut(final GraphAnalyticalView.UpdateMode mode) {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (W) EDGE TYPES (E) UPDATE MODE " + mode);
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();

      assertThat(view.getNodeCount()).isEqualTo(2);
      assertThat(view.coversVertexType("W")).isTrue();
      assertThat(view.coversVertexType("V")).isFalse();
      // V holds vertices the view does not hold
      assertThat(view.coversVertexType(null)).isFalse();
      assertThat(count(ONE_HOP)).isEqualTo(expectedPaths("E", 1));
    }

    @ParameterizedTest
    @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS" })
    void aViewRestoredFromDiskHoldsTheSubTypes(final GraphAnalyticalView.UpdateMode mode) {
      createView(mode);
      database.close();
      database = new DatabaseFactory(DB_PATH).open();

      final GraphAnalyticalView restored = GraphAnalyticalViewRegistry.get(database, "g");
      assertThat(restored.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      assertThat(restored.isRestoredFromPersistedCsr()).isTrue();
      assertThat(restored.getNodeCount()).isEqualTo(6);
      assertServedByTheViewAndRight();
    }

    @ParameterizedTest
    @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void aSubTypeCreatedAfterTheBuildSurvivesAReopen(final GraphAnalyticalView.UpdateMode mode) {
      createView(mode);
      database.getSchema().createVertexType("X").addSuperType("W");
      database.transaction(() -> {
        final MutableVertex x = database.newVertex("X").set("pid", 7).save();
        final MutableVertex first = database.select().fromType("V").where().property("pid").eq().value(0).vertices().next()
            .modify();
        first.newEdge("F", x);
        x.newEdge("F", first);
      });
      assertThat(GraphAnalyticalViewRegistry.get(database, "g").awaitReady(30, TimeUnit.SECONDS)).isTrue();

      database.close();
      database = new DatabaseFactory(DB_PATH).open();
      final GraphAnalyticalView reopened = GraphAnalyticalViewRegistry.get(database, "g");
      assertThat(reopened.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      assertThat(reopened.coversVertexType("X")).isTrue();
      assertThat(reopened.getNodeCount()).isEqualTo(7);
      assertServedByTheViewAndRight();

      // closed again with nothing changed: this time the CSR is restored from disk, sub-type included
      database.close();
      database = new DatabaseFactory(DB_PATH).open();
      final GraphAnalyticalView restored = GraphAnalyticalViewRegistry.get(database, "g");
      assertThat(restored.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      assertThat(restored.isRestoredFromPersistedCsr()).isTrue();
      assertThat(restored.getNodeCount()).isEqualTo(7);
      assertServedByTheViewAndRight();
    }

    @ParameterizedTest
    @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "OFF", "SYNCHRONOUS", "ASYNCHRONOUS" })
    void anExistingTypeAttachedAfterTheBuildIsNotClaimedUntilRebuilt(final GraphAnalyticalView.UpdateMode mode) {
      // Y holds vertices and edges before the view exists, and is not a V yet
      database.getSchema().createVertexType("Y");
      database.transaction(() -> {
        final MutableVertex y = database.newVertex("Y").set("pid", 8).save();
        final MutableVertex first = database.select().fromType("V").where().property("pid").eq().value(0).vertices().next()
            .modify();
        first.newEdge("E", y);
        y.newEdge("E", first);
      });
      final GraphAnalyticalView view = createView(mode);
      assertThat(view.getNodeCount()).isEqualTo(6);

      // The first question that finds Y dispatches the rebuild that takes it in: held before its scan, or a scan of seven vertices
      // can publish before the next question is asked, which then rightly finds Y held
      final CountDownLatch gate = new CountDownLatch(1);
      view.setBeforeBuildScanForTest(() -> {
        try {
          gate.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      try {
        database.getSchema().getType("Y").addSuperType("V");

        assertThat(view.coversVertexType("Y")).as("the CSR holds none of Y's vertices").isFalse();
        assertThat(view.coversVertexType(null)).isFalse();
        assertThat(count(ONE_HOP)).as("answered from the records").isEqualTo(expectedPaths("E", 1));
      } finally {
        gate.countDown();
        view.setBeforeBuildScanForTest(null);
      }

      if (mode == GraphAnalyticalView.UpdateMode.OFF) {
        // turned STALE off the query thread that found Y
        Awaitility.await("the OFF view turns STALE").atMost(30, TimeUnit.SECONDS)
            .until(() -> view.getStatus() == GraphAnalyticalView.Status.STALE);
        database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW g");
      }
      // the incremental modes dispatch the rebuild off the query thread: until it starts, the view still reads READY
      Awaitility.await("the rebuild read Y").atMost(30, TimeUnit.SECONDS)
          .until(() -> view.awaitReady(1, TimeUnit.SECONDS) && view.getNodeCount() == 7);
      assertThat(view.getNodeCount()).as("the rebuild read Y").isEqualTo(7);
      assertThat(view.coversVertexType("Y")).isTrue();
      assertServedByTheViewAndRight();
    }

    @ParameterizedTest
    @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS" })
    void aCsrPersistedInTheFormerFormatIsRebuilt(final GraphAnalyticalView.UpdateMode mode) throws Exception {
      createView(mode);
      database.close();

      // the file a release before #10208 wrote: the listed types alone, format version 2 right after the magic number
      final File file = new File(DB_PATH, NodeLocalFiles.GAV_CSR_FILE_PREFIX + "g" + NodeLocalFiles.GAV_CSR_FILE_EXTENSION);
      assertThat(file).as("the CSR was persisted on close").exists();
      try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
        raf.seek(Integer.BYTES);
        raf.writeInt(2);
      }

      database = new DatabaseFactory(DB_PATH).open();
      final GraphAnalyticalView reopened = GraphAnalyticalViewRegistry.get(database, "g");
      assertThat(reopened.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      assertThat(reopened.isRestoredFromPersistedCsr()).as("a former-format file is rebuilt, not restored").isFalse();
      assertThat(reopened.getNodeCount()).isEqualTo(6);
      assertServedByTheViewAndRight();
    }

    private GraphAnalyticalView createView(final GraphAnalyticalView.UpdateMode mode) {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (V) EDGE TYPES (E) UPDATE MODE " + mode);
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      return view;
    }

    private void assertServedByTheViewAndRight() {
      assertThat(cypherPlan(ONE_HOP)).as("1-hop served by the view").contains("GAV");
      assertThat(cypherPlan(TWO_HOP)).as("2-hop served by the view").contains("GAV");
      assertThat(count(ONE_HOP)).isEqualTo(expectedPaths("E", 1));
      assertThat(count(TWO_HOP)).isEqualTo(expectedPaths("F", 2));

      try (final ResultSet rs = database.query("sql", "SELECT expand(out('E')) FROM V")) {
        long rows = 0;
        while (rs.hasNext()) {
          rs.next();
          ++rows;
        }
        assertThat(rows).isEqualTo(expectedPaths("E", 1));
        assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("CSR-accelerated via Graph Analytical View");
      }
    }

    private String cypherPlan(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }

    /** The number of {@code edgeType} paths of {@code hops} hops between V vertices, sub-types included, counted on the records. */
    private long expectedPaths(final String edgeType, final int hops) {
      final DocumentType v = database.getSchema().getType("V");
      final Map<RID, List<RID>> out = new HashMap<>();
      for (final Iterator<Record> it = database.iterateType(edgeType, true); it.hasNext(); ) {
        final Edge edge = it.next().asEdge();
        if (edge.getOutVertex().getType().instanceOf(v.getName()) && edge.getInVertex().getType().instanceOf(v.getName()))
          out.computeIfAbsent(edge.getOut(), k -> new ArrayList<>()).add(edge.getIn());
      }
      long paths = 0;
      for (final Map.Entry<RID, List<RID>> entry : out.entrySet())
        for (final RID second : entry.getValue())
          if (hops == 1)
            ++paths;
          else
            paths += out.getOrDefault(second, List.of()).size();
      return paths;
    }
  }

  /**
   * Regression for #10214: a Graph Analytical View listing edge type {@code E} claimed an edge type {@code G} attached to {@code E} after the
   * scan ({@code ALTER TYPE G SUPERTYPE +E}), whose edges its CSR does not hold: a hop on {@code E}, which reads its sub-types'
   * slices, missed them. Such a type is now held only when the scan materialized it or proves it held no edge the CSR lacks; otherwise
   * the view is not served until it has rebuilt (OFF: it turns STALE).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ViewJoinedEdgeType {
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

  /**
   * Regression for #10415 (#10365, #10393): the Cypher MATCH operators that use a Graph Analytical View read whichever snapshot the view
   * served at each call. An operator maps a vertex to its node id, reads the neighbours, maps them back, and hands the ids on to
   * the next operator, while the rows stream out in batches: a rebuild between two batches - a compaction, an explicit REBUILD -
   * numbers the nodes afresh, and the later batches answered the old ids as other vertices, without failing. Every operator of a
   * statement now reads one pin of the view, taken when the statement first reads it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ViewPinnedPerStatement extends TestHelper {
    private static final int PAD     = 2_000;
    // More neighbours than a batch of rows, and more two-hop paths than a round of the fused walk: the ids an operator read
    // before the rebuild are still being consumed after it
    private static final int TARGETS = 20_000;

    private final List<RID> sources = new ArrayList<>();
    private       RID       deleted;

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE VERTEX TYPE Q");
      database.command("sql", "CREATE EDGE TYPE K");
      database.command("sql", "CREATE EDGE TYPE L");
      database.transaction(() -> {
        // padding first, so the base numbers the hub's targets after it and a deletion shifts them on the next build
        for (int i = 0; i < PAD; i++)
          database.newVertex("Q").set("id", 100_000 + i).save();
        deleted = database.newVertex("P").set("id", -1).save().getIdentity();
        final MutableVertex hub = database.newVertex("P").set("id", 0).save();
        sources.add(hub.getIdentity());
        sources.add(database.newVertex("P").set("id", 1).save().getIdentity());
        for (int i = 0; i < TARGETS; i++) {
          final MutableVertex middle = database.newVertex("Q").set("id", 1_000_000 + i).save();
          final MutableVertex end = database.newVertex("Q").set("id", 2_000_000 + i).save();
          hub.newEdge("K", middle);
          middle.newEdge("L", end);
        }
      });
    }

    /** The reproduction of #10365: a view-served MATCH streams its rows, and the view is rebuilt after the first one. */
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "MATCH (a:P)-[:K]->(b:Q) RETURN a.id AS a, b.id AS b | GAVExpandAll",
        "MATCH (a:P)-[:K]->(b:Q)-[:L]->(c:Q) RETURN a.id AS a, c.id AS b | GAVFusedChain",
        "MATCH (a:P)-[:K]->(b:Q) WITH a, b MATCH (b)-[:L]->(c:Q) RETURN a.id AS a, c.id AS b | MATCH RELATIONSHIP" })
    void aRebuildWhileTheRowsStreamDoesNotChangeTheVerticesTheyName(final String query, final String operator) {
      final GraphAnalyticalView view = createView();
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).as("the view-served operator under test").contains(operator);
      }
      // an overlay deletion, so the rebuild below numbers every node after it afresh
      database.transaction(() -> deleted.asVertex().delete());
      assertThat(view.hasPendingChanges()).isTrue();

      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        rows.add(pair(rs.next()));
        view.build();
        assertThat(view.hasPendingChanges()).as("the rebuild published a new base").isFalse();
        while (rs.hasNext())
          rows.add(pair(rs.next()));
      }

      final List<String> expected = new ArrayList<>(TARGETS);
      final int offset = query.contains(":L") ? 2_000_000 : 1_000_000;
      for (int i = 0; i < TARGETS; i++)
        expected.add("0->" + (offset + i));
      assertThat(rows).containsExactlyInAnyOrderElementsOf(expected);
    }

    /** The operators of one statement share the pin: one snapshot per view per statement. */
    @Test
    void aStatementPinsEachViewOnce() {
      final GraphAnalyticalView view = createView();
      final BasicCommandContext statement = new BasicCommandContext();
      statement.openGraphViewPins();
      final GraphTraversalProvider first = GraphViewPins.pin(statement, view);
      assertThat(first).isInstanceOf(GraphAnalyticalView.Pinned.class);

      final BasicCommandContext derived = new BasicCommandContext();
      derived.setParent(statement);
      assertThat(GraphViewPins.pin(derived, view)).as("a sub-query of the statement").isSameAs(first);
      assertThat(GraphViewPins.pin(statement.copy(), view)).as("a parallel worker of the statement").isSameAs(first);
      assertThat(GraphViewPins.pin(statement, first)).as("a pin pinned again").isSameAs(first);

      // another statement, and a caller outside any statement, pin for themselves
      final BasicCommandContext next = new BasicCommandContext();
      next.openGraphViewPins();
      assertThat(GraphViewPins.pin(next, view)).isNotSameAs(first);
      assertThat(GraphViewPins.pin(new BasicCommandContext(), view)).isNotSameAs(first).isInstanceOf(GraphAnalyticalView.Pinned.class);

      // two statements of one script: the script's context opens no scope, so each statement opens its own and the second does
      // not inherit the first one's pins, while what the script itself evaluates outside a statement holds none
      final BasicCommandContext script = new BasicCommandContext();
      final BasicCommandContext firstStatement = new BasicCommandContext();
      firstStatement.setParentWithoutOverridingChild(script);
      firstStatement.openGraphViewPins();
      final BasicCommandContext secondStatement = new BasicCommandContext();
      secondStatement.setParentWithoutOverridingChild(script);
      secondStatement.openGraphViewPins();
      assertThat(script.getGraphViewPins()).isNull();
      assertThat(secondStatement.getGraphViewPins()).isNotNull().isNotSameAs(firstStatement.getGraphViewPins());
      assertThat(GraphViewPins.pin(secondStatement, view)).isNotSameAs(GraphViewPins.pin(firstStatement, view));
    }

    /**
     * A view with no snapshot yet has nothing to pin: the statement is handed the view itself, every read of which fails, and the
     * fallback is not kept, so the same statement pins the view once it is built - and keeps that pin from then on.
     */
    @Test
    void aViewBuiltDuringTheStatementIsPinnedFromItsFirstSnapshot() {
      final GraphAnalyticalView unbuilt = new GraphAnalyticalView(database);
      try {
        final BasicCommandContext statement = new BasicCommandContext();
        statement.openGraphViewPins();
        assertThat(GraphViewPins.pin(statement, unbuilt)).isSameAs(unbuilt);
        assertThatThrownBy(() -> unbuilt.getNodeId(sources.get(0))).isInstanceOf(IllegalStateException.class);

        unbuilt.build();
        final GraphTraversalProvider pinned = GraphViewPins.pin(statement, unbuilt);
        assertThat(pinned).isInstanceOf(GraphAnalyticalView.Pinned.class);
        unbuilt.build();
        assertThat(GraphViewPins.pin(statement, unbuilt)).as("the statement keeps the first snapshot it pinned").isSameAs(pinned);
      } finally {
        unbuilt.shutdown();
      }
    }

    /**
     * A statement of a script pins for itself: the one that follows a COMMIT reads what was committed, not the snapshot an earlier
     * statement of the script read.
     */
    @Test
    void aScriptStatementAfterACommitReadsWhatWasCommitted() {
      createView();
      final String count = "SELECT sum(out('K').size()) AS n FROM P";
      try (final ResultSet rs = database.command("sqlscript", count + ";\n"
          + "BEGIN;\n"
          + "CREATE EDGE K FROM " + sources.get(0) + " TO " + sources.get(1) + ";\n"
          + "COMMIT;\n"
          + count + ";")) {
        assertThat(rs.next().<Number>getProperty("n").longValue()).isEqualTo(TARGETS + 1L);
      }
    }

    private static String pair(final Result row) {
      return row.<Number>getProperty("a").intValue() + "->" + row.<Number>getProperty("b").intValue();
    }

    private GraphAnalyticalView createView() {
      database.command("sql",
          "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P, Q) EDGE TYPES (K, L) PROPERTIES (id) UPDATE MODE SYNCHRONOUS").close();
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      return view;
    }
  }

  /**
   * Regression for #10434, the report's own three-vertex chain: {@code WHERE b.flag} on the middle node of a two-hop chain over a Graph
   * Analytical View. The plan is asserted too, so the answers cannot come from a walk over the records: the chain is the fused one,
   * with the middle node bound (a predicate whose node went unseen deferred its load, the {@code b*} of the plan) and not given up on.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ViewBooleanPredicateReportedQuery extends TestHelper {
    private static final String CHAIN = "MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N) ";

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE N").close();
      database.command("sql", "CREATE PROPERTY N.flag BOOLEAN").close();
      database.command("sql", "CREATE EDGE TYPE R").close();
      database.transaction(() -> database.command("opencypher", "CREATE (a:N {flag: false})-[:R]->(b:N {flag: true})-[:R]->(c:N {flag: false})")
          .close());
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (N) EDGE TYPES (R) PROPERTIES (flag) UPDATE MODE SYNCHRONOUS").close();
      assertThat(GraphAnalyticalViewRegistry.get(database, "g").awaitReady(30, TimeUnit.SECONDS)).isTrue();
    }

    private long count(final String where) {
      try (final ResultSet rs = database.query("opencypher", CHAIN + where + " RETURN count(*) AS c")) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

    @Test
    void theMiddleNodeIsBoundForABarePredicate() {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + CHAIN + "WHERE b.flag RETURN count(*) AS c")) {
        final String plan = rs.next().getProperty("executionPlanAsString");
        assertThat(plan).contains("GAVFusedChain").contains("(b)-[:R]->(c*)").doesNotContain("(b*)");
      }
    }

    @Test
    void theReportedQueryFindsTheMiddleNodeWithTheFlag() {
      assertThat(count("WHERE b.flag")).isEqualTo(1L);
      assertThat(count("WHERE b.flag = true")).isEqualTo(1L);
      assertThat(count("WHERE NOT b.flag")).isZero();
      assertThat(count("WHERE c.flag")).isZero();
      assertThat(count("WHERE NOT c.flag")).isEqualTo(1L);
      assertThat(count("WHERE coalesce(b.flag, false)")).isEqualTo(1L);
    }
  }

  /**
   * Regression for #10434: with a Graph Analytical View, an openCypher two-hop chain whose {@code WHERE} held a boolean property of the
   * middle or last node as a predicate on its own ({@code WHERE b.flag}, {@code WHERE NOT b.flag},
   * {@code WHERE coalesce(b.flag, false)}, {@code WHERE c.flag}, also as one side of an {@code AND}) returned no rows. The
   * conjunct was taken for one that reads no node, so it was decided on the source alone, before the node it reads was bound. The
   * same query without the view, {@code WHERE b.flag = true}, a predicate on the first node, a comparison or {@code IS NULL} all
   * answered correctly. Every query is answered with the view and again after {@code DROP GRAPH ANALYTICAL VIEW}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ViewBooleanPredicate extends TestHelper {

    private static final String CHAIN = "MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N) ";

    private static final String[] QUERIES = {//
        CHAIN + "WHERE b.flag RETURN count(*) AS c",//
        CHAIN + "WHERE b.flag = true RETURN count(*) AS c",//
        CHAIN + "WHERE NOT b.flag RETURN count(*) AS c",//
        CHAIN + "WHERE b.flag = false RETURN count(*) AS c",//
        CHAIN + "WHERE a.flag RETURN count(*) AS c",//
        CHAIN + "WHERE c.flag RETURN count(*) AS c",//
        CHAIN + "WHERE NOT c.flag RETURN count(*) AS c",//
        "MATCH (a:N)-[:R]->(b:N) WHERE b.flag RETURN count(*) AS c",//
        "MATCH (a:N)-[:R]->(b:N) WHERE a.flag RETURN count(*) AS c",//
        CHAIN + "WHERE b.flag RETURN a.id AS x, c.id AS y",//
        CHAIN + "WHERE b.flag AND a.id <> c.id RETURN count(*) AS c",//
        CHAIN + "WHERE b.i > 0 RETURN count(*) AS c",//
        CHAIN + "WHERE b.flag IS NULL RETURN count(*) AS c",//
        CHAIN + "WHERE b.flag IS NOT NULL RETURN count(*) AS c",//
        CHAIN + "WHERE coalesce(b.flag, false) RETURN count(*) AS c",//
        CHAIN + "WHERE b.flag OR b.i = 1 RETURN count(*) AS c",//
        CHAIN + "WHERE b.flag XOR false RETURN count(*) AS c",//
        CHAIN + "WHERE b.flag AND c.flag RETURN count(*) AS c",//
        CHAIN + "WHERE b.flag AND NOT c.flag RETURN count(*) AS c",//
        CHAIN + "WHERE NOT b.flag AND NOT c.flag RETURN count(*) AS c",//
        CHAIN + "RETURN count(b.flag) AS c",//
        CHAIN + "WHERE b.i = 1 AND b.flag RETURN count(*) AS c",//
        CHAIN + "WHERE true RETURN count(*) AS c",//
        CHAIN + "WHERE false RETURN count(*) AS c",//
        "MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N)-[:R]->(d:N) WHERE c.flag RETURN count(*) AS c",//
        "MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N)-[:R]->(d:N) WHERE NOT d.flag RETURN count(*) AS c",//
    };

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE N").close();
      database.command("sql", "CREATE PROPERTY N.id LONG").close();
      database.command("sql", "CREATE PROPERTY N.flag BOOLEAN").close();
      database.command("sql", "CREATE PROPERTY N.i LONG").close();
      database.command("sql", "CREATE PROPERTY N.s STRING").close();
      database.command("sql", "CREATE EDGE TYPE R").close();

      final Random random = new Random(5);
      final List<RID> vertices = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < 40; i++) {
          final MutableVertex vertex = database.newVertex("N").set("id", (long) i).set("i", (long) (i % 3)).set("s", i % 2 == 0 ? "x" : "y");
          // true, false or absent
          if (i % 3 < 2)
            vertex.set("flag", i % 3 == 0);
          vertices.add(vertex.save().getIdentity());
        }
        for (int i = 0; i < 120; i++)
          vertices.get(random.nextInt(40)).asVertex().newEdge("R", vertices.get(random.nextInt(40)).asVertex());
      });
    }

    private String answer(final String query) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          rows.add(row.getPropertyNames().stream().sorted().map(name -> String.valueOf(row.<Object>getProperty(name))).toList().toString());
        }
      }
      Collections.sort(rows);
      return rows.toString();
    }

    private void createView(final String properties, final String mode) {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (N) EDGE TYPES (R) PROPERTIES " + properties + " UPDATE MODE " + mode)
          .close();
      assertThat(GraphAnalyticalViewRegistry.get(database, "g").awaitReady(30, TimeUnit.SECONDS)).isTrue();
    }

    @ParameterizedTest
    @CsvSource({ "'(id, flag, i, s)', SYNCHRONOUS", "'(id, i, s)', SYNCHRONOUS", "'(id)', SYNCHRONOUS", "'(id, i, s)', OFF" })
    void everyBooleanPredicateAnswersAsItDoesWithoutTheView(final String properties, final String mode) {
      createView(properties, mode);
      final String[] withView = new String[QUERIES.length];
      for (int i = 0; i < QUERIES.length; i++)
        withView[i] = answer(QUERIES[i]);

      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g").close();

      for (int i = 0; i < QUERIES.length; i++)
        assertThat(withView[i]).as(QUERIES[i]).isEqualTo(answer(QUERIES[i]));
    }
  }

  /**
   * Regression for #10435: SQL MATCH over a Graph Analytical View holding several vertex types returned rows that break the pattern in
   * chains of three or more hops. The fused walk of the consecutive hops bound the nodes it reached without looking at the type the
   * pattern gave them, so {@code {type: C}} on the third node or a later one let other types through (the hops before it were
   * checked by the regular step); since #10212 gave a view over a type its sub-types, a typed second node of a three-hop chain
   * returned paths that do not exist. Every chain is answered with the view and again after {@code DROP GRAPH ANALYTICAL VIEW},
   * and the rows must be the same.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SqlMatchViewMultiType extends TestHelper {

    private static final String[] QUERIES = {//
        "MATCH {type: V, as: a}.out('R'){type: C, as: b} RETURN a, b",//
        "MATCH {type: V, as: a}.out('R'){as: b}.out('R'){type: C, as: c} RETURN a, b, c",//
        "MATCH {type: V, as: a}.out('R'){as: b}.out('R'){as: c}.out('R'){type: C, as: d} RETURN a, b, c, d",//
        "MATCH {type: V, as: a}.out('R'){as: b}.out('R'){as: c}.out('R'){as: d}.out('R'){type: C, as: e} RETURN a, b, c, d, e",//
        "MATCH {type: V, as: a}.out('R'){type: C, as: b}.out('R'){as: c}.out('R'){as: d} RETURN a, b, c, d",//
        "MATCH {type: V, as: a}.out('R'){as: b}.out('R'){type: C, as: c}.out('R'){as: d} RETURN a, b, c, d",//
        "MATCH {type: V, as: a}.in('R'){as: b}.in('R'){as: c}.in('R'){type: C, as: d} RETURN a, b, c, d",//
        "MATCH {type: V, as: a}.both('R'){as: b}.both('R'){as: c}.both('R'){type: C, as: d} RETURN a, b, c, d",//
        "MATCH {type: V, as: a}.out('R'){as: b}.out('R'){as: c}.out('R'){type: A, as: d} RETURN a, b, c, d",//
        "MATCH {type: V, as: a}.out('R'){as: b}.out('R'){as: c}.out('R'){as: d, where: (k = 1)} RETURN a, b, c, d",//
        "MATCH {type: C, as: a}.out('R'){type: C, as: b}.out('R'){type: C, as: c}.out('R'){type: C, as: d} RETURN a, b, c, d",//
        "MATCH {type: A, as: a}.out('R'){type: B, as: b}.out('R'){type: V, as: c}.out('R'){type: C, as: d} RETURN a, b, c, d",//
    };

    private void schema(final boolean subTypes) {
      if (subTypes) {
        sql("CREATE VERTEX TYPE V");
        sql("CREATE VERTEX TYPE A EXTENDS V");
        sql("CREATE VERTEX TYPE B EXTENDS A");
        sql("CREATE VERTEX TYPE C EXTENDS V");
      } else {
        sql("CREATE VERTEX TYPE V");
        sql("CREATE VERTEX TYPE A");
        sql("CREATE VERTEX TYPE B");
        sql("CREATE VERTEX TYPE C");
      }
      sql("CREATE PROPERTY V.k LONG");
      sql("CREATE EDGE TYPE R");
    }

    private void sql(final String statement) {
      database.command("sql", statement).close();
    }

    private void fill() {
      final String[] types = { "V", "A", "B", "C" };
      final Random random = new Random(3);
      final List<RID> vertices = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < 60; i++)
          vertices.add(database.newVertex(types[i % 4]).set("k", (long) (i % 3)).save().getIdentity());
        for (int i = 0; i < 180; i++)
          vertices.get(random.nextInt(60)).asVertex().newEdge("R", vertices.get(random.nextInt(60)).asVertex());
      });
    }

    private void createView(final String vertexTypes, final String mode) {
      sql("CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES " + vertexTypes + " EDGE TYPES (R) PROPERTIES (k) UPDATE MODE " + mode);
      assertThat(GraphAnalyticalViewRegistry.get(database, "g").awaitReady(60, TimeUnit.SECONDS)).isTrue();
    }

    /** The rows of a chain, each as the RIDs of its aliases, sorted. */
    private List<String> rows(final String query) {
      final String[] aliases = query.substring(query.indexOf("RETURN ") + "RETURN ".length()).split(",\\s*");
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final StringBuilder sb = new StringBuilder();
          for (final String alias : aliases)
            sb.append(((Identifiable) row.getProperty(alias.trim())).getIdentity()).append(' ');
          rows.add(sb.toString());
        }
      }
      Collections.sort(rows);
      return rows;
    }

    private void assertSameWithAndWithoutTheView(final String vertexTypes, final String mode) {
      createView(vertexTypes, mode);
      final List<List<String>> withView = new ArrayList<>();
      for (final String query : QUERIES)
        withView.add(rows(query));

      sql("DROP GRAPH ANALYTICAL VIEW g");

      for (int i = 0; i < QUERIES.length; i++)
        assertThat(withView.get(i)).as(QUERIES[i]).isEqualTo(rows(QUERIES[i]));
    }

    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "OFF" })
    void unrelatedTypesAllInTheView(final String mode) {
      schema(false);
      fill();
      assertSameWithAndWithoutTheView("(V, A, B, C)", mode);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "OFF" })
    void subTypesBroughtByTheViewOverTheSuperType(final String mode) {
      schema(true);
      fill();
      assertSameWithAndWithoutTheView("(V)", mode);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "OFF" })
    void subTypesListedOneByOne(final String mode) {
      schema(true);
      fill();
      assertSameWithAndWithoutTheView("(V, A, B, C)", mode);
    }

    /** The report's smallest case: the one {@code C} vertex is no reason for the three-hop chain to end on an {@code A}. */
    @Test
    void aTypeOnTheLastNodeOfAThreeHopChainIsRespected() {
      sql("CREATE VERTEX TYPE V");
      sql("CREATE VERTEX TYPE A");
      sql("CREATE VERTEX TYPE C");
      sql("CREATE EDGE TYPE R");
      database.transaction(() -> database.command("opencypher",
          "CREATE (v1:V), (a1:A), (a2:A), (a3:A), (c:C), (v1)-[:R]->(a1), (a1)-[:R]->(a2), (a2)-[:R]->(a3)").close());
      createView("(V, A, C)", "SYNCHRONOUS");

      final String chain = "MATCH {type: V, as: a}.out('R'){as: b}.out('R'){as: c}.out('R'){type: C, as: d} RETURN a, b, c, d";
      assertThat(rows(chain)).as("d is an A, not a C").isEmpty();
      assertThat(rows("MATCH {type: V, as: a}.out('R'){as: b}.out('R'){as: c}.out('R'){type: A, as: d} RETURN a, b, c, d")).hasSize(1);
    }
  }

  /**
   * Regression for #10491: after {@code ALTER TYPE U SUPERTYPE +Base}, where {@code Base} is a vertex type a Graph Analytical View lists and
   * {@code U} already holds vertices, a vertex inserted into {@code U} right after the change was missing from the view and stayed
   * missing, while the view reported READY and {@code algo.wcc} answered one node fewer than the same call without the view. The
   * type attached after the scan is claimed only if it was scanned or held nothing, and otherwise the view rebuilds; here the check
   * took the type for empty, so it was claimed. Every run is compared with the answer the same call gives once the view is dropped.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ViewVertexTypeAttachedAfterBuild {
    private static final String DB_PATH = "./target/databases/test-issue-10491";
    private static final int    REPS    = 12;

    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
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
    void aVertexInsertedRightAfterAnExistingTypeJoinedIsInTheViewAnswer(final GraphAnalyticalView.UpdateMode mode) throws Exception {
      for (int rep = 0; rep < REPS; rep++)
        runOnce(mode, 4, 1, true);
    }

    @ParameterizedTest
    @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void severalVerticesInsertedRightAfterTheTypeJoinedAreInTheViewAnswer(final GraphAnalyticalView.UpdateMode mode)
        throws Exception {
      for (int rep = 0; rep < REPS; rep++)
        runOnce(mode, 4, 2, true);
    }

    @ParameterizedTest
    @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void aVertexWithNoEdgeInsertedRightAfterTheTypeJoinedIsInTheViewAnswer(final GraphAnalyticalView.UpdateMode mode)
        throws Exception {
      for (int rep = 0; rep < REPS; rep++)
        runOnce(mode, 4, 1, false);
    }

    @ParameterizedTest
    @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void aTypeThatWasEmptyWhenItJoinedKeepsBeingServed(final GraphAnalyticalView.UpdateMode mode) throws Exception {
      for (int rep = 0; rep < REPS; rep++)
        runOnce(mode, 0, 1, true);
    }

    /** The delete of a vertex of the joined type meets the same fate as an insert: the listener dropped it, the rebuild kept the vertex. */
    @ParameterizedTest
    @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void aVertexDeletedRightAfterTheTypeJoinedIsNotInTheViewAnswer(final GraphAnalyticalView.UpdateMode mode) throws Exception {
      for (int rep = 0; rep < REPS; rep++)
        runOnce(mode, 4, 0, false, "DELETE FROM U WHERE id = 30");
    }

    /**
     * The update of a vertex of the joined type meets the same fate as an insert: the listener dropped it, and the rebuild it
     * dispatched read the old value. Read through the view itself, once it holds the type.
     */
    @ParameterizedTest
    @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void aPropertyUpdatedRightAfterTheTypeJoinedIsWhatTheViewHoldsOnceItHoldsTheType(final GraphAnalyticalView.UpdateMode mode)
        throws Exception {
      for (int rep = 0; rep < REPS; rep++) {
        database = new DatabaseFactory(DB_PATH).create();
        try {
          database.command("sql", "CREATE VERTEX TYPE Base").close();
          database.command("sql", "CREATE VERTEX TYPE U").close();
          database.command("sql", "CREATE EDGE TYPE E1").close();
          database.transaction(() -> {
            for (int k = 0; k < 4; k++) {
              database.command("sql", "INSERT INTO Base SET id = " + k).close();
              database.command("sql", "INSERT INTO U SET id = " + (30 + k)).close();
            }
          });
          database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (Base) EDGE TYPES (E1) UPDATE MODE " + mode).close();
          awaitReady();

          database.command("sql", "ALTER TYPE U SUPERTYPE +Base").close();
          database.transaction(() -> database.command("sql", "UPDATE U SET id = 999 WHERE id = 30").close());
          awaitReady();

          final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
          Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(20, TimeUnit.MILLISECONDS)
              .until(() -> view.isReady() && view.coversVertexType("U"));

          final RID rid;
          try (final ResultSet rs = database.query("sql", "SELECT @rid AS r FROM U WHERE id = 999")) {
            rid = rs.next().<RID>getProperty("r");
          }
          assertThat(((Number) view.getProperty(view.getNodeId(rid), "id")).intValue()).as("%s, rep %d", mode, rep).isEqualTo(999);
        } finally {
          if (database.isOpen())
            database.drop();
        }
      }
    }

    private void runOnce(final GraphAnalyticalView.UpdateMode mode, final int existing, final int inserts, final boolean edge)
        throws Exception {
      runOnce(mode, existing, inserts, edge, null);
    }

    /**
     * Base and U are vertex types, E1 an edge type; a view over Base and E1; {@code ALTER TYPE U SUPERTYPE +Base} with
     * {@code existing} vertices in U; then {@code inserts} new U vertices, each with an E1 edge from a Base vertex when
     * {@code edge}; the nodes {@code algo.wcc(['E1'])} answers with the view and after it is dropped must be the same.
     */
    private void runOnce(final GraphAnalyticalView.UpdateMode mode, final int existing, final int inserts, final boolean edge,
        final String afterJoin) throws Exception {
      database = new DatabaseFactory(DB_PATH).create();
      try {
        database.command("sql", "CREATE VERTEX TYPE Base").close();
        database.command("sql", "CREATE VERTEX TYPE U").close();
        database.command("sql", "CREATE EDGE TYPE E1").close();
        database.transaction(() -> {
          for (int k = 0; k < 4; k++)
            database.command("sql", "INSERT INTO Base SET id = " + k).close();
          for (int k = 0; k < existing; k++)
            database.command("sql", "INSERT INTO U SET id = " + (30 + k)).close();
          for (int k = 0; k < 3; k++)
            database.command("sql",
                "CREATE EDGE E1 FROM (SELECT FROM Base WHERE id = " + k + ") TO (SELECT FROM Base WHERE id = " + (k + 1) + ")").close();
        });
        database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (Base) EDGE TYPES (E1) UPDATE MODE " + mode).close();
        awaitReady();

        database.command("sql", "ALTER TYPE U SUPERTYPE +Base").close();
        if (afterJoin != null)
          database.transaction(() -> database.command("sql", afterJoin).close());
        for (int k = 0; k < inserts; k++) {
          final int id = 130 + k;
          database.transaction(() -> {
            database.command("sql", "INSERT INTO U SET id = " + id).close();
            if (edge)
              database.command("sql",
                  "CREATE EDGE E1 FROM (SELECT FROM Base WHERE id = 0) TO (SELECT FROM U WHERE id = " + id + ")").close();
          });
        }
        awaitReady();

        final Set<String> withView = wccNodes();
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW g").close();
        final Set<String> withoutView = wccNodes();

        assertThat(withView).as("nodes of algo.wcc with the view (%s, existing=%d, inserts=%d, edge=%s)", mode, existing, inserts, edge)
            .isEqualTo(withoutView);
      } finally {
        if (database.isOpen())
          database.drop();
      }
    }

    private void awaitReady() {
      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(20, TimeUnit.MILLISECONDS).until(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT status FROM schema:graphAnalyticalViews WHERE name = 'g'")) {
          return rs.hasNext() && "READY".equals(String.valueOf(rs.next().<Object>getProperty("status")));
        }
      });
    }

    private Set<String> wccNodes() {
      final Set<String> nodes = new TreeSet<>();
      try (final ResultSet rs = database.query("opencypher",
          "CALL algo.wcc(['E1']) YIELD node, componentId RETURN node.id AS pid, componentId AS c")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          nodes.add(String.valueOf(row.<Object>getProperty("pid")));
        }
      }
      return nodes;
    }
  }

  /**
   * Regression for #9301.
   * <p>
   * Regression test for #9301: a view over some of the vertex types was used for every traversal over its edge types,
   * also when the traversal reached a vertex type the view does not hold, so SQL out(), SQL MATCH and openCypher MATCH
   * answered 0 where the edges exist. A view only accelerates: a statement answers the same with and without it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PartialVertexCoverageView extends TestHelper {

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("A");
      database.getSchema().createVertexType("B");
      database.getSchema().createVertexType("C");
      database.getSchema().createEdgeType("E");
      database.transaction(() -> {
        final MutableVertex a1 = database.newVertex("A").set("id", 1).save();
        final MutableVertex b1 = database.newVertex("B").set("id", 11).save();
        final MutableVertex b2 = database.newVertex("B").set("id", 12).save();
        final MutableVertex c1 = database.newVertex("C").set("id", 21).save();
        final MutableVertex c2 = database.newVertex("C").set("id", 22).save();
        a1.newEdge("E", b1);
        a1.newEdge("E", b2);
        b1.newEdge("E", c1);
        b2.newEdge("E", c2);
      });
    }

    private long count(final String language, final String query) {
      return database.query(language, query).next().<Number>getProperty("n").longValue();
    }

    private void assertSameAnswers(final String state) {
      assertThat(count("sql", "SELECT count(*) AS n FROM (SELECT expand(out('E')) FROM B)")).as(state + " out()").isEqualTo(2);
      assertThat(count("sql", "SELECT sum(out('E').size()) AS n FROM B")).as(state + " sum(out().size())").isEqualTo(2);
      assertThat(count("sql", "MATCH {type: A, as: a}.out('E'){type: B, as: b}.out('E'){type: C, as: c} RETURN count(*) AS n"))
          .as(state + " SQL MATCH").isEqualTo(2);
      assertThat(count("opencypher", "MATCH (a:A)-[:E]->(b:B)-[:E]->(c:C) RETURN count(*) AS n")).as(state + " Cypher chain").isEqualTo(2);
      assertThat(count("opencypher", "MATCH (b:B)-[:E]->(c:C) RETURN count(*) AS n")).as(state + " Cypher hop").isEqualTo(2);
      assertThat(count("opencypher", "MATCH (c:C)<-[:E]-(b:B)<-[:E]-(a:A) RETURN count(*) AS n")).as(state + " Cypher reverse").isEqualTo(2);
      assertThat(database.query("opencypher", "MATCH (b:B)-[:E]->(c:C) RETURN c.id AS id ORDER BY id").stream()
          .map(r -> r.<Number>getProperty("id").intValue()).toList()).as(state + " Cypher ids").containsExactly(21, 22);
    }

    private void withView(final String vertexTypes, final boolean coversAll, final String state) throws Exception {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g9301 VERTEX TYPES (" + vertexTypes + ") EDGE TYPES (E) UPDATE MODE OFF");
      try {
        final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g9301");
        assertThat(view.awaitReady(60, TimeUnit.SECONDS)).as(state + " ready").isTrue();
        // the registry hands a view to a walk only when it can answer for every vertex the walk reaches
        assertThat(GraphTraversalProviderRegistry.findProvider(database, "E")).as(state + " findProvider")
            .isEqualTo(coversAll ? view : null);
        assertThat(GraphTraversalProviderRegistry.findProviderAllowingPartialVertexCoverage(database, "E")).as(state + " partial")
            .isSameAs(view);
        assertSameAnswers(state);
        // the one-hop scan, the one path that accepts a partial view, answers the same
        assertThat(count("opencypher", "MATCH (a:A)-[:E]->(b:B) RETURN count(*) AS n")).as(state + " Cypher one hop").isEqualTo(2);
        assertThat(count("opencypher", "MATCH (a:A)-[:E]->(b:B) RETURN max(b.id) AS n")).as(state + " Cypher one hop max").isEqualTo(12);
        if ("A, B".equals(vertexTypes))
          assertThat(database.query("opencypher", "PROFILE MATCH (a:A)-[:E]->(b:B) RETURN max(b.id) AS n").getExecutionPlan().get()
              .prettyPrint(0, 2)).as(state + " one hop plan").contains("GAV ONE-HOP SCAN");
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9301");
      }
    }

    @Test
    void viewOverSomeVertexTypesAnswersLikeNoView() throws Exception {
      assertSameAnswers("no view");
      withView("A, B", false, "view (A, B)");
      withView("A, C", false, "view (A, C)");
      withView("A, B, C", true, "view (A, B, C)");
      assertSameAnswers("view dropped");
    }

    @Test
    void aVertexOfANewTypeMakesTheViewPartial() throws Exception {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g9301 VERTEX TYPES (A, B, C) EDGE TYPES (E) UPDATE MODE OFF");
      try {
        final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g9301");
        assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
        assertThat(view.coversVertexType(null)).isTrue();
        database.getSchema().createVertexType("D");
        assertThat(view.coversVertexType(null)).as("a vertex type the view does not hold appeared, with no vertex yet").isTrue();
        database.transaction(() -> database.newVertex("D").set("id", 41).save());
        assertThat(view.coversVertexType(null)).as("a vertex the view does not hold appeared").isFalse();
        assertThat(GraphTraversalProviderRegistry.findProvider(database, "E")).isNull();
        database.getSchema().dropType("D");
        assertThat(view.coversVertexType(null)).as("the uncovered vertex type is gone").isTrue();
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9301");
      }
    }

    @Test
    void anEmptyVertexTypeTheViewDoesNotListKeepsItFull() throws Exception {
      // the LSQB shape: Post and Comment extend Message, which holds no vertex of its own, and the view lists the concrete
      // types only. No walk can reach a vertex of a type that has none, so the view still answers for every vertex
      database.getSchema().createVertexType("M");
      database.getSchema().getType("C").addSuperType("M");
      database.getSchema().createVertexType("Unused");
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g9301 VERTEX TYPES (A, B, C) EDGE TYPES (E) UPDATE MODE OFF");
      try {
        final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g9301");
        assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
        assertThat(view.coversVertexType(null)).as("the unlisted types hold no vertex").isTrue();
        assertThat(GraphTraversalProviderRegistry.findProvider(database, "E")).isSameAs(view);
        assertSameAnswers("view (A, B, C) with M and Unused empty");
        assertThat(count("opencypher", "MATCH (b:B)-[:E]->(m:M) RETURN count(*) AS n")).isEqualTo(2);

        // a counter not known yet (fresh open with no statistics, unclean shutdown) still reads a bucket never written as empty
        for (final Bucket bucket : database.getSchema().getType("Unused").getBuckets(false))
          ((LocalBucket) bucket).setCachedRecordCount(-1);
        assertThat(view.coversVertexType(null)).as("unknown counter, no page written").isTrue();

        // an uncommitted vertex of an unlisted type, linked to a listed one: the view cannot see it, so no walk gets the view
        database.begin();
        try {
          final MutableVertex pending = database.newVertex("M").set("id", 32).save();
          database.query("sql", "SELECT FROM B WHERE id = 11").next().getVertex().get().newEdge("E", pending);
          assertThat(GraphTraversalProviderRegistry.findProvider(database, "E")).as("transaction holds changes").isNull();
          assertThat(count("opencypher", "MATCH (b:B)-[:E]->(m:M) RETURN count(*) AS n")).as("own write seen").isEqualTo(3);
          assertThat(count("sql", "SELECT count(*) AS n FROM (SELECT expand(out('E')) FROM B)")).as("own write seen").isEqualTo(3);
        } finally {
          database.rollback();
        }
        assertThat(GraphTraversalProviderRegistry.findProvider(database, "E")).as("rolled back").isSameAs(view);

        // a vertex of an unlisted type makes the view partial again, with no schema change to notice it
        final RID[] m1 = new RID[1];
        database.transaction(() -> m1[0] = database.newVertex("M").set("id", 31).save().getIdentity());
        assertThat(view.coversVertexType(null)).as("M holds a vertex").isFalse();
        assertThat(GraphTraversalProviderRegistry.findProvider(database, "E")).isNull();
        database.transaction(() -> m1[0].asVertex().delete());
        assertThat(view.coversVertexType(null)).as("M is empty again").isTrue();

        // emptied, then its counter lost: not provable without a scan, so partial until a count(*) recounts the bucket
        for (final Bucket bucket : database.getSchema().getType("M").getBuckets(false))
          ((LocalBucket) bucket).setCachedRecordCount(-1);
        assertThat(view.coversVertexType(null)).as("unknown counter, pages written").isFalse();
        assertThat(count("sql", "SELECT count(*) AS n FROM M")).as("C's vertices, polymorphic").isEqualTo(2);
        assertThat(view.coversVertexType(null)).as("recounted").isTrue();
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9301");
      }
    }

    /**
     * The coverage check trusts the committed record counter of the unlisted buckets: every way of creating a vertex has to
     * move it, or the view would read as full while a vertex it does not hold exists, and the wrong answers of #9301 would
     * be back. One unlisted type per path, so no path can hide behind another's vertex.
     */
    @Test
    void everyVertexCreatePathMakesTheViewPartial() throws Exception {
      final String[] paths = { "SqlInsert", "SqlCreateVertex", "CypherCreate", "ApiAsync", "GraphBatch", "GraphBatchProps" };
      for (final String type : paths)
        database.getSchema().createVertexType(type);
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g9301 VERTEX TYPES (A, B, C) EDGE TYPES (E) UPDATE MODE OFF");
      try {
        final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g9301");
        assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();

        final Runnable[] creates = {
            () -> database.command("sql", "INSERT INTO SqlInsert SET id = 51"),
            () -> database.command("sql", "CREATE VERTEX SqlCreateVertex SET id = 52"),
            () -> database.command("opencypher", "CREATE (:CypherCreate {id: 53})"),
            () -> {
              database.async().createRecord(database.newVertex("ApiAsync").set("id", 54), null);
              database.async().waitCompletion();
            },
            () -> {
              try (final GraphBatch batch = database.batch().build()) {
                batch.createVertices("GraphBatch", 2);
              }
            },
            () -> {
              try (final GraphBatch batch = database.batch().build()) {
                batch.createVertices("GraphBatchProps", new Object[][] { { "id", 56 } });
              }
            } };

        for (int i = 0; i < paths.length; i++) {
          assertThat(view.coversVertexType(null)).as("before " + paths[i]).isTrue();
          if (paths[i].startsWith("Sql") || paths[i].startsWith("Cypher"))
            database.transaction(creates[i]::run);
          else
            creates[i].run();
          assertThat(count("sql", "SELECT count(@rid) AS n FROM " + paths[i])).as(paths[i] + " created").isGreaterThan(0);
          assertThat(view.coversVertexType(null)).as("after " + paths[i]).isFalse();
          final String emptied = paths[i];
          database.transaction(() -> database.command("sql", "DELETE FROM " + emptied));
        }
        assertThat(view.coversVertexType(null)).as("all emptied again").isTrue();
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9301");
      }
    }

    @Test
    void oneHopScanSkipsAViewThatLacksAnEndpointForALaterOne() throws Exception {
      // the first view holds A and C, the second A and B: only the second can answer A -> B
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g9301ac VERTEX TYPES (A, C) EDGE TYPES (E) UPDATE MODE OFF");
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g9301ab VERTEX TYPES (A, B) EDGE TYPES (E) UPDATE MODE OFF");
      try {
        assertThat(GraphAnalyticalViewRegistry.get(database, "g9301ac").awaitReady(60, TimeUnit.SECONDS)).isTrue();
        assertThat(GraphAnalyticalViewRegistry.get(database, "g9301ab").awaitReady(60, TimeUnit.SECONDS)).isTrue();
        assertThat(count("opencypher", "MATCH (a:A)-[:E]->(b:B) RETURN max(b.id) AS n")).isEqualTo(12);
        assertThat(database.query("opencypher", "PROFILE MATCH (a:A)-[:E]->(b:B) RETURN max(b.id) AS n").getExecutionPlan().get()
            .prettyPrint(0, 2)).contains("GAV ONE-HOP SCAN");
        assertSameAnswers("two partial views");
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9301ac");
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW g9301ab");
      }
    }
  }

  /**
   * Regression for #9587: an OpenCypher plan built while the Graph Analytical View that could serve it was not ready - its deferred restore
   * from disk still in flight after a reopen, its first build still running, or the view stale and not to be used stale - walks the
   * records, and the plan cache kept handing that plan out after the view turned READY. The view reported READY, EXPLAIN (which
   * plans afresh) showed the view, and every execution of the query still ran at the speed of no view at all, until a schema change
   * happened to flush the cache. A cached plan that passed over a view is now planned again once that view can serve it.
   * <p>
   * The view is held not ready with the shared build permits, so nothing depends on how fast it is.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CachedPlanPicksUpReadyView {
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

    @Test
    void aViewThatTurnsReadyIsPlannedAgainOnceNotOnEveryExecution() {
      database.getConfiguration().setValue(GlobalConfiguration.GAV_USE_WHEN_STALE, false);
      // registered first, so the lookup prefers it once it is ready
      final GraphAnalyticalView first = GraphAnalyticalView.builder(database)
          .withName("first")
          .withVertexTypes("P")
          .withEdgeTypes("K")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF)
          .build();
      database.transaction(() -> database.newVertex("P").set("pid", 6).save());
      assertThat(first.isReady()).isFalse();
      // built after the commit: ready, while the first one is stale
      GraphAnalyticalView.builder(database)
          .withName("second")
          .withVertexTypes("P")
          .withEdgeTypes("K")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF)
          .build();

      assertThat(count()).isEqualTo(EXPECTED);
      final PhysicalPlan readsTheSecond = cachedPlan();
      assertThat(readsTheSecond.getRootOperator().explain(0)).contains("provider=second");
      assertThat(count()).isEqualTo(EXPECTED);
      assertThat(cachedPlan()).as("the first view is still stale: the plan is kept").isSameAs(readsTheSecond);

      first.build();

      assertThat(count()).isEqualTo(EXPECTED);
      final PhysicalPlan readsTheFirst = cachedPlan();
      assertThat(readsTheFirst).isNotSameAs(readsTheSecond);
      assertThat(readsTheFirst.getRootOperator().explain(0)).as("planned again, onto the view the lookup prefers")
          .contains("provider=first");
      assertThat(count()).isEqualTo(EXPECTED);
      assertThat(cachedPlan()).as("a ready view is selected, never recorded: no re-plan on every execution").isSameAs(readsTheFirst);
    }

    @Test
    void aPlanBuiltInsideAWriteTransactionIsNotCached() {
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName(VIEW_NAME)
          .withVertexTypes("P")
          .withEdgeTypes("K")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
          .build();
      assertThat(view.isReady()).isTrue();

      // the view is withheld from a transaction holding uncommitted changes: its plan walks the records and must not be cached
      database.transaction(() -> {
        database.newVertex("P").set("pid", 6).save();
        assertThat(count()).isEqualTo(EXPECTED);
        assertThat(cachedPlan()).isNull();
      });

      assertThat(count()).isEqualTo(EXPECTED);
      assertThat(cachedPlanReadsTheView()).as("the first plan after the commit reads the view").isTrue();
    }

    @Test
    void droppingAViewAPlanPassedOverEmptiesTheCache() {
      database.getConfiguration().setValue(GlobalConfiguration.GAV_USE_WHEN_STALE, false);
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName(VIEW_NAME)
          .withVertexTypes("P")
          .withEdgeTypes("K")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF)
          .build();
      database.transaction(() -> database.newVertex("P").set("pid", 6).save());
      assertThat(count()).isEqualTo(EXPECTED);
      assertThat(cachedPlan()).isNotNull();

      // the plan holds the view it passed over: the drop, a schema change, must not leave it reachable from the cache
      view.drop();

      assertThat(cachedPlan()).isNull();
    }

    @Test
    void aNestedRecordingHandsWhatItSawToTheEnclosingOne() {
      database.getConfiguration().setValue(GlobalConfiguration.GAV_USE_WHEN_STALE, false);
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName(VIEW_NAME)
          .withVertexTypes("P")
          .withEdgeTypes("K")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF)
          .build();
      database.transaction(() -> database.newVertex("P").set("pid", 6).save());
      assertThat(view.isReady()).isFalse();

      try (final ViewsPassedOver outer = GraphTraversalProviderRegistry.recordViewsPassedOver()) {
        try (final ViewsPassedOver inner = GraphTraversalProviderRegistry.recordViewsPassedOver()) {
          assertThat(GraphTraversalProviderRegistry.findProvider(database, "K")).isNull();
          assertThat(GraphTraversalProviderRegistry.findProvider(database, "K")).isNull();
          assertThat(inner.getViews()).as("recorded once however often it is passed over").containsExactly(view);
          assertThat(outer.getViews()).isEmpty();
        }
        assertThat(outer.getViews()).as("handed over when the inner recording closes").containsExactly(view);
      }

      // no recording open any more: a lookup records nowhere
      try (final ViewsPassedOver later = GraphTraversalProviderRegistry.recordViewsPassedOver()) {
        assertThat(later.getViews()).isEmpty();
      }
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

  /**
   * Regression for #9831: {@code CREATE GRAPH ANALYTICAL VIEW ... PROPERTIES ()} creates a view without any vertex property, as
   * {@code GraphAnalyticalViewBuilder.withProperties()} with an empty array does. No {@code PROPERTIES} clause still
   * copies every vertex property.
   */
  @Nested
  class EmptyPropertiesView extends TestHelper {
    private RID a;

    @BeforeEach
    void createGraph() {
      database.getSchema().createVertexType("P");
      // an edge property reaches the view only when the schema declares it
      database.getSchema().createEdgeType("K").createProperty("w", Type.DOUBLE);
      database.transaction(() -> {
        final MutableVertex va = database.newVertex("P").set("name", "a", "age", 1).save();
        final MutableVertex vb = database.newVertex("P").set("name", "b", "age", 2).save();
        final MutableVertex vc = database.newVertex("P").set("name", "c", "age", 3).save();
        va.newEdge("K", vb, "w", 1.5);
        vb.newEdge("K", vc, "w", 2.5);
        a = va.getIdentity();
      });
    }

    @Test
    void emptyPropertiesCreatesAViewWithoutPropertyColumns() {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW v PROPERTIES () UPDATE MODE OFF");
      final GraphAnalyticalView view = awaitView("v");

      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
      assertThat(view.getNodeCount()).isEqualTo(3);
      assertThat(view.getEdgeCount()).isEqualTo(2);
      assertThat(view.getPropertyFilter()).isEmpty();
      assertThat(view.getColumnStore()).isNull();
      assertThat(view.getProperty(view.getNodeId(a), "name")).isNull();

      try (final ResultSet rs = database.query("sql", "SELECT FROM schema:graphAnalyticalViews WHERE name = 'v'")) {
        final List<?> propertyFilter = rs.next().getProperty("propertyFilter");
        assertThat(propertyFilter).isNotNull().isEmpty();
      }
    }

    @Test
    void noClauseCopiesEveryPropertyAndAListCopiesOnlyItsOwn() {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW vAll");
      final GraphAnalyticalView all = awaitView("vAll");
      assertThat(all.getPropertyFilter()).isNull();
      assertThat(all.getProperty(all.getNodeId(a), "name")).isEqualTo("a");
      assertThat(all.getProperty(all.getNodeId(a), "age")).isEqualTo(1);

      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW vName PROPERTIES (name)");
      final GraphAnalyticalView name = awaitView("vName");
      assertThat(name.getPropertyFilter()).containsExactly("name");
      assertThat(name.getProperty(name.getNodeId(a), "name")).isEqualTo("a");
      assertThat(name.getProperty(name.getNodeId(a), "age")).isNull();
    }

    @Test
    void emptyPropertiesSurviveAReopen() {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW v PROPERTIES () UPDATE MODE OFF");
      awaitView("v");

      reopenDatabase();

      final GraphAnalyticalView restored = awaitView("v");
      assertThat(restored.isRestoredFromPersistedCsr()).isTrue();
      assertThat(restored.getNodeCount()).isEqualTo(3);
      assertThat(restored.getEdgeCount()).isEqualTo(2);
      assertThat(restored.getPropertyFilter()).isEmpty();
      assertThat(restored.getColumnStore()).isNull();
    }

    @Test
    void emptyPropertiesRoundTripsThroughTheParser() {
      final String sql = "CREATE GRAPH ANALYTICAL VIEW v PROPERTIES () EDGE PROPERTIES (w)";
      final Statement statement = new SQLAntlrParser(null).parse(sql);
      assertThat(statement).isInstanceOf(CreateGraphAnalyticalViewStatement.class);
      assertThat(((CreateGraphAnalyticalViewStatement) statement).properties).isEmpty();
      assertThat(statement.toString()).isEqualTo(sql);

      final Statement reparsed = new SQLAntlrParser(null).parse(statement.toString());
      assertThat(reparsed).isEqualTo(statement);
      assertThat(statement.copy()).isEqualTo(statement);
      assertThat(statement.copy().toString()).isEqualTo(sql);

      // an empty list and no list at all are different statements, the statement cache must not mix them up
      final Statement noClause = new SQLAntlrParser(null).parse("CREATE GRAPH ANALYTICAL VIEW v EDGE PROPERTIES (w)");
      assertThat(((CreateGraphAnalyticalViewStatement) noClause).properties).isNull();
      assertThat(noClause).isNotEqualTo(statement);
    }

    @Test
    void edgePropertiesStillWorkNextToEmptyProperties() {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW v PROPERTIES () EDGE PROPERTIES (w)");
      final GraphAnalyticalView view = awaitView("v");

      assertThat(view.getColumnStore()).isNull();
      assertThat(view.getEdgePropertyFilter()).containsExactly("w");
      assertThat(view.getEdgeColumnStore("K").getColumn("w")).isNotNull();
    }

    @Test
    void algorithmRunsOnAViewWithoutProperties() {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW v PROPERTIES () UPDATE MODE OFF");
      awaitView("v");

      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "CALL algo.pagerank() YIELD node, score RETURN node.name AS name ORDER BY score DESC")) {
        while (rs.hasNext())
          names.add(rs.next().getProperty("name"));
      }
      assertThat(names).containsExactlyInAnyOrder("a", "b", "c");

      try (final ResultSet rs = database.command("opencypher",
          "PROFILE CALL algo.pagerank() YIELD node, score RETURN node.name AS name, score")) {
        assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("CSR-accelerated via Graph Analytical View");
      }
    }

    private GraphAnalyticalView awaitView(final String name) {
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, name);
      assertThat(view).isNotNull();
      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      return view;
    }
  }

  /**
   * Regression for #8939.
   * <p>
   * Regression test for #8939: {@code in()} and {@code both()} over a UNIDIRECTIONAL edge type answered the incoming
   * vertices from the analytical view's reverse index, so the same query returned different rows with and without a view
   * and disagreed with {@code inE()} and the vertex API. A view only accelerates: called on its own the functions answer
   * what the vertex stores, a pattern answers the incoming side, in both states.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UnidirectionalInWithView extends TestHelper {

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("V");
      database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL");
      database.getSchema().createEdgeType("B");
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("V").set("name", "a").save();
        final MutableVertex b = database.newVertex("V").set("name", "b").save();
        a.newEdge("U", b);
        a.newEdge("B", b);
      });
    }

    private long sql(final String projection) {
      return ((Number) database.query("sql", "SELECT " + projection + " AS n FROM V WHERE name = 'b'").next().getProperty("n")).longValue();
    }

    private void assertSameAnswers(final String state) {
      assertThat(sql("in('U').size()")).as(state + " in('U')").isZero();
      assertThat(sql("both('U').size()")).as(state + " both('U')").isZero();
      assertThat(sql("inE('U').size()")).as(state + " inE('U')").isZero();
      // a bidirectional type is answered as ever
      assertThat(sql("in('B').size()")).as(state + " in('B')").isEqualTo(1);
      assertThat(sql("both('B').size()")).as(state + " both('B')").isEqualTo(1);

      final Vertex b = database.query("sql", "SELECT FROM V WHERE name = 'b'").next().getVertex().get();
      long api = 0;
      for (final Vertex ignored : b.getVertices(Vertex.DIRECTION.IN, "U"))
        api++;
      assertThat(api).as(state + " API").isZero();

      // a pattern asks which edges end in the vertex, with or without a view
      assertThat(database.query("opencypher", "MATCH (b:V {name: 'b'})<-[:U]-(a) RETURN count(*) AS n").next().<Number>getProperty("n").longValue())
          .as(state + " Cypher").isEqualTo(1);
      assertThat(database.query("sql", "MATCH {type: V, as: b, where: (name = 'b')}.in('U'){as: a} RETURN a.name AS n").next().<String>getProperty("n"))
          .as(state + " SQL MATCH").isEqualTo("a");
    }

    @Test
    void inAndBothAnswerTheSameWithAndWithoutAView() throws Exception {
      assertSameAnswers("no view");

      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g8939 VERTEX TYPES (V) EDGE TYPES (U, B)");
      try {
        GraphAnalyticalViewRegistry.get(database, "g8939").awaitReady(60, TimeUnit.SECONDS);
        assertSameAnswers("with view");
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW g8939");
      }

      assertSameAnswers("view dropped");
    }
  }
}
