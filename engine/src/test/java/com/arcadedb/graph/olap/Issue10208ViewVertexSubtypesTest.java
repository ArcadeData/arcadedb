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
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.NodeLocalFiles;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10208: a Graph Analytical View declared over a vertex type left that type's sub-types out of its CSR, while an edge type
 * brought its sub-types along. Once a sub-type held a vertex the view no longer covered every vertex, so the planner never handed
 * it a query: the view was built and kept up to date for nothing. A vertex type now brings its sub-types, as an edge type does and
 * as a query on the type does on the record path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10208ViewVertexSubtypesTest {
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

    database.getSchema().getType("Y").addSuperType("V");

    assertThat(view.coversVertexType("Y")).as("the CSR holds none of Y's vertices").isFalse();
    assertThat(view.coversVertexType(null)).isFalse();
    assertThat(count(ONE_HOP)).as("answered from the records").isEqualTo(expectedPaths("E", 1));

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
