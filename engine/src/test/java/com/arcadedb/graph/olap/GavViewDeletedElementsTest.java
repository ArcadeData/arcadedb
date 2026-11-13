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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeLinkedList;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests of GavViewDeletedElements, one nested class per reported case.
 */
class GavViewDeletedElementsTest {
  /**
   * Regression for #10305: a transaction that created an edge and deleted it again reported the deletion of that edge's RID to the view. When
   * another transaction had committed an edge into the very same slot of the edge bucket's page in the meantime (the first one holds
   * its own copy of the page, so it handed the slot out again), the view applied the deletion to the other transaction's edge and
   * lost it, while the records and both vertices' edge lists kept it. A record created and deleted by one transaction is no change
   * to the committed state, so it is no change to the view either. Vertices share the slot allocation, and so the shape.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ViewSameSlotCreateDelete extends TestHelper {

    private final List<RID> vertices = new ArrayList<>();

    private void sql(final String statement) {
      database.command("sql", statement).close();
    }

    private RID edge(final int from, final int to) {
      return vertices.get(from).asVertex().newEdge("E", vertices.get(to).asVertex()).save().getIdentity();
    }

    private void awaitView(final String name) {
      assertThat(GraphAnalyticalViewRegistry.get(database, name).awaitReady(60, TimeUnit.SECONDS)).isTrue();
    }

    private long id(final RID rid) {
      return ((Number) rid.asVertex().get("id")).longValue();
    }

    private List<String> recordEdges() {
      final List<String> edges = new ArrayList<>();
      for (final Iterator<?> it = database.iterateType("E", true); it.hasNext(); ) {
        final Edge edge = ((Record) it.next()).asEdge();
        edges.add(id(edge.getOut()) + "->" + id(edge.getIn()));
      }
      Collections.sort(edges);
      return edges;
    }

    private List<String> viewEdges() {
      final List<String> edges = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", "MATCH (a:V)-[:E]->(b:V) RETURN a.id AS a, b.id AS b")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          edges.add(row.<Object>getProperty("a") + "->" + row.<Object>getProperty("b"));
        }
      }
      Collections.sort(edges);
      return edges;
    }

    private long viewCount() {
      try (final ResultSet rs = database.query("opencypher", "MATCH (a:V)-[:E]->(b:V) RETURN count(*) AS c")) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

    @ParameterizedTest
    @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void anEdgeCreatedAndDeletedInASlotAnotherTransactionTookKeepsTheOthersEdge(final GraphAnalyticalView.UpdateMode mode) throws Exception {
      sql("CREATE VERTEX TYPE V");
      sql("CREATE EDGE TYPE E BUCKETS 1");
      sql("CREATE PROPERTY V.id INTEGER");
      database.transaction(() -> {
        for (int i = 0; i < 8; i++)
          vertices.add(database.newVertex("V").set("id", i).save().getIdentity());
      });
      sql("CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (V) EDGE TYPES (E) UPDATE MODE " + mode);
      awaitView("gv");

      final RID[] frees = new RID[2];
      // the endpoints' edge segments exist, so later edges append to them
      database.transaction(() -> {
        edge(2, 3);
        edge(4, 5);
        edge(0, 1);
      });
      database.transaction(() -> {
        frees[0] = edge(6, 7);
        frees[1] = edge(0, 1);
      });
      // frees the slot of the edge 6->7 on the edge bucket's page
      database.transaction(() -> frees[0].asEdge().delete());
      awaitView("gv");

      final CountDownLatch bTookItsPage = new CountDownLatch(1);
      final CountDownLatch aCommitted = new CountDownLatch(1);
      final AtomicReference<RID> x = new AtomicReference<>();
      final AtomicReference<RID> y = new AtomicReference<>();
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      final Thread b = new Thread(() -> {
        try {
          database.begin();
          // B now holds its own copy of the page
          frees[1].asEdge().delete();
          bTookItsPage.countDown();
          assertThat(aCommitted.await(60, TimeUnit.SECONDS)).isTrue();
          // takes the slot A's edge took, in B's copy, and gives it back
          y.set(edge(4, 5));
          y.get().asEdge().delete();
          database.commit();
        } catch (final Throwable t) {
          failure.set(t);
          if (database.isTransactionActive())
            database.rollback();
        }
      });
      final Thread a = new Thread(() -> {
        try {
          assertThat(bTookItsPage.await(60, TimeUnit.SECONDS)).isTrue();
          database.begin();
          x.set(edge(2, 3));
          database.commit();
        } catch (final Throwable t) {
          failure.set(t);
          if (database.isTransactionActive())
            database.rollback();
        } finally {
          aCommitted.countDown();
        }
      });
      b.start();
      a.start();
      a.join(60_000);
      b.join(60_000);
      assertThat(failure.get()).isNull();
      // Issue #11498: B, alone on the page with an image of it older than A's commit, used to be handed the slot A's edge took, and
      // its insert then failed at commit or, created and deleted, left the view to tell the two apart. The allocator now checks the
      // committed page: B gets another slot, and the view must hold what the records hold all the same
      assertThat(y.get()).as("B is not handed the slot A's edge took").isNotEqualTo(x.get());

      awaitView("gv");
      assertThat(recordEdges()).containsExactly("0->1", "2->3", "2->3", "4->5");
      assertThat(viewEdges()).as("the view holds what the records hold").isEqualTo(recordEdges());
      assertThat(viewCount()).isEqualTo(4L);
    }

    @ParameterizedTest
    @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void aVertexCreatedAndDeletedInASlotAnotherTransactionTookKeepsTheOthersVertex(final GraphAnalyticalView.UpdateMode mode) throws Exception {
      sql("CREATE VERTEX TYPE V BUCKETS 1");
      sql("CREATE EDGE TYPE E");
      sql("CREATE PROPERTY V.id INTEGER");
      final RID[] frees = new RID[3];
      database.transaction(() -> {
        for (int i = 0; i < 3; i++)
          frees[i] = database.newVertex("V").set("id", i).save().getIdentity();
      });
      sql("CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (V) EDGE TYPES (E) UPDATE MODE " + mode);
      awaitView("gv");
      // frees a slot of the vertex bucket's page
      database.transaction(() -> frees[0].asVertex().delete());
      awaitView("gv");

      final CountDownLatch bTookItsPage = new CountDownLatch(1);
      final CountDownLatch aCommitted = new CountDownLatch(1);
      final AtomicReference<RID> x = new AtomicReference<>();
      final AtomicReference<RID> y = new AtomicReference<>();
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      final Thread b = new Thread(() -> {
        try {
          database.begin();
          frees[1].asVertex().delete();
          bTookItsPage.countDown();
          assertThat(aCommitted.await(60, TimeUnit.SECONDS)).isTrue();
          y.set(database.newVertex("V").set("id", 100).save().getIdentity());
          y.get().asVertex().delete();
          database.commit();
        } catch (final Throwable t) {
          failure.set(t);
          if (database.isTransactionActive())
            database.rollback();
        }
      });
      final Thread a = new Thread(() -> {
        try {
          assertThat(bTookItsPage.await(60, TimeUnit.SECONDS)).isTrue();
          database.begin();
          x.set(database.newVertex("V").set("id", 50).save().getIdentity());
          database.commit();
        } catch (final Throwable t) {
          failure.set(t);
          if (database.isTransactionActive())
            database.rollback();
        } finally {
          aCommitted.countDown();
        }
      });
      b.start();
      a.start();
      a.join(60_000);
      b.join(60_000);
      assertThat(failure.get()).isNull();
      // Issue #11498: see the edge test above, B gets another slot than the one A's vertex took
      assertThat(y.get()).as("B is not handed the slot A's vertex took").isNotEqualTo(x.get());

      awaitView("gv");
      assertThat(database.countType("V", true)).isEqualTo(2L);
      try (final ResultSet rs = database.query("opencypher", "MATCH (v:V) RETURN v.id AS id ORDER BY id")) {
        final List<Long> ids = new ArrayList<>();
        while (rs.hasNext())
          ids.add(((Number) rs.next().getProperty("id")).longValue());
        assertThat(ids).as("the view holds what the records hold").containsExactly(2L, 50L);
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH (v:V) RETURN count(v) AS c")) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2L);
      }
    }

    /** A lightweight edge has no RID of its own to take back: its creation and its deletion are both reported, and cancel out in the view. */
    @Test
    void aLightweightEdgeCreatedAndDeletedInOneTransactionLeavesTheViewAlone() {
      sql("CREATE VERTEX TYPE V");
      sql("CREATE EDGE TYPE L LIGHTWEIGHT");
      sql("CREATE PROPERTY V.id INTEGER");
      database.transaction(() -> {
        for (int i = 0; i < 4; i++)
          vertices.add(database.newVertex("V").set("id", i).save().getIdentity());
        vertices.get(0).asVertex().newEdge("L", vertices.get(1).asVertex());
      });
      sql("CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (V) EDGE TYPES (L) UPDATE MODE SYNCHRONOUS");
      awaitView("gv");

      database.transaction(() -> {
        vertices.get(2).asVertex().newEdge("L", vertices.get(3).asVertex());
        for (final Edge e : vertices.get(2).asVertex().getEdges(Vertex.DIRECTION.OUT, "L"))
          e.delete();
      });
      awaitView("gv");

      try (final ResultSet rs = database.query("opencypher", "MATCH (a:V)-[:L]->(b:V) RETURN a.id AS a, b.id AS b")) {
        final List<String> edges = new ArrayList<>();
        while (rs.hasNext()) {
          final Result row = rs.next();
          edges.add(row.<Object>getProperty("a") + "->" + row.<Object>getProperty("b"));
        }
        assertThat(edges).containsExactly("0->1");
      }
    }

    @Test
    void anEdgeCreatedAndDeletedInOneTransactionLeavesTheViewAlone() {
      sql("CREATE VERTEX TYPE V");
      sql("CREATE EDGE TYPE E");
      sql("CREATE PROPERTY V.id INTEGER");
      database.transaction(() -> {
        for (int i = 0; i < 4; i++)
          vertices.add(database.newVertex("V").set("id", i).save().getIdentity());
        edge(0, 1);
      });
      sql("CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (V) EDGE TYPES (E) UPDATE MODE SYNCHRONOUS");
      awaitView("gv");

      database.transaction(() -> {
        // created and deleted again, twice over, beside an edge that stays and one that goes
        edge(1, 2).asEdge().delete();
        edge(2, 3).asEdge().delete();
        edge(2, 3);
        for (final Edge e : vertices.get(0).asVertex().getEdges(Vertex.DIRECTION.OUT, "E"))
          e.delete();
      });
      awaitView("gv");
      assertThat(recordEdges()).containsExactly("2->3");
      assertThat(viewEdges()).isEqualTo(recordEdges());
    }
  }

  /**
   * Regression for #10335: a vertex property removed after a Graph Analytical View was built kept its old value in the view until the
   * database was reopened. The vertex's new values were recorded from the names the record still has, so a removed property was
   * not in the map and the overlay, which applied the map over the old values, left the old one in place. A property set to null
   * stays among the record's names, which is why that edit was right.
   * <p>
   * The values a vertex is reported with are its complete state, so the overlay takes them as that: whatever the vertex no longer
   * has reads as missing, whatever the view's property filter is.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ViewRemovedProperty extends TestHelper {
    private static final String ROWS = "MATCH (x:P)-[:K]->(y:P) RETURN x.id AS xi, y.id AS yi";

    private final RID[] rids = new RID[3];

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE EDGE TYPE K");
      // a path 2 -> 3 -> 4
      database.transaction(() -> {
        final MutableVertex[] vertices = new MutableVertex[3];
        for (int i = 0; i < 3; i++) {
          vertices[i] = database.newVertex("P").set("id", (long) (2 + i)).set("name", "n" + i).save();
          rids[i] = vertices[i].getIdentity();
        }
        vertices[0].newEdge("K", vertices[1]).save();
        vertices[1].newEdge("K", vertices[2]).save();
      });
    }

    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void removalThroughTheJavaApi(final String mode) {
      final GraphAnalyticalView view = createView(mode, "PROPERTIES (id)");
      database.transaction(() -> removeId(rids[1]));
      awaitMerged(view);

      assertRemoved(view);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void removalThroughSql(final String mode) {
      final GraphAnalyticalView view = createView(mode, "PROPERTIES (id)");
      database.transaction(() -> database.command("sql", "UPDATE " + rids[1] + " REMOVE id").close());
      awaitMerged(view);

      assertRemoved(view);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void removalThroughCypher(final String mode) {
      final GraphAnalyticalView view = createView(mode, "PROPERTIES (id)");
      database.transaction(() -> database.command("opencypher", "MATCH (y:P) WHERE id(y) = '" + rids[1] + "' REMOVE y.id").close());
      awaitMerged(view);

      assertRemoved(view);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void removalOnAViewWithNoPropertyFilter(final String mode) {
      // a view that materialises every property has no list of names to walk to find the removed one
      final GraphAnalyticalView view = createView(mode, "");
      database.transaction(() -> removeId(rids[1]));
      awaitMerged(view);

      assertRemoved(view);
    }

    @Test
    void aRemovalKeepsTheVertexOtherProperties() {
      final GraphAnalyticalView view = createView("SYNCHRONOUS", "PROPERTIES (id, name)");
      database.transaction(() -> removeId(rids[1]));

      assertThat(view.getProperty(view.getNodeId(rids[1]), "id")).isNull();
      assertThat(view.getProperty(view.getNodeId(rids[1]), "name")).isEqualTo("n1");
      // the neighbours are untouched
      assertThat(view.getProperty(view.getNodeId(rids[0]), "id")).isEqualTo(2L);
      assertThat(view.getProperty(view.getNodeId(rids[2]), "id")).isEqualTo(4L);
    }

    @Test
    void anUpdateOfOnePropertyKeepsTheOthersOfAVertexWithSeveralInTheView() {
      final GraphAnalyticalView view = createView("SYNCHRONOUS", "PROPERTIES (id, name)");
      database.transaction(() -> {
        final MutableVertex vertex = rids[1].asVertex().modify();
        vertex.set("name", "renamed");
        vertex.save();
      });

      assertThat(view.getProperty(view.getNodeId(rids[1]), "id")).isEqualTo(3L);
      assertThat(view.getProperty(view.getNodeId(rids[1]), "name")).isEqualTo("renamed");
      assertThat(rows()).containsExactlyInAnyOrder("[xi=2, yi=3]", "[xi=3, yi=4]");
    }

    @Test
    void aRemovedPropertyCanBeSetAgain() {
      final GraphAnalyticalView view = createView("SYNCHRONOUS", "PROPERTIES (id)");
      database.transaction(() -> removeId(rids[1]));
      assertThat(view.getProperty(view.getNodeId(rids[1]), "id")).isNull();

      database.transaction(() -> rids[1].asVertex().modify().set("id", 10L).save());
      assertThat(view.getProperty(view.getNodeId(rids[1]), "id")).isEqualTo(10L);
      assertThat(rows()).containsExactlyInAnyOrder("[xi=2, yi=10]", "[xi=10, yi=4]");
    }

    @Test
    void aRemovalOnAVertexCreatedAfterTheBuild() {
      final GraphAnalyticalView view = createView("SYNCHRONOUS", "PROPERTIES (id)");
      final RID[] created = new RID[1];
      database.transaction(() -> {
        final MutableVertex v = database.newVertex("P").set("id", 7L).set("name", "late").save();
        created[0] = v.getIdentity();
        rids[2].asVertex().modify().newEdge("K", v).save();
      });
      assertThat(view.getProperty(view.getNodeId(created[0]), "id")).isEqualTo(7L);

      database.transaction(() -> removeId(created[0]));

      assertThat(view.getProperty(view.getNodeId(created[0]), "id")).as("a vertex of the overlay").isNull();
      assertThat(view.getProperty(view.getNodeId(created[0]), "name")).isEqualTo("late");
      assertThat(rows()).containsExactlyInAnyOrder("[xi=2, yi=3]", "[xi=3, yi=4]", "[xi=4, yi=null]");
    }

    @Test
    void aRemovalSurvivesAnotherUpdateOfTheSameVertex() {
      final GraphAnalyticalView view = createView("SYNCHRONOUS", "PROPERTIES (id)");
      database.transaction(() -> removeId(rids[1]));
      database.transaction(() -> rids[1].asVertex().modify().set("name", "renamed").save());

      assertThat(view.getProperty(view.getNodeId(rids[1]), "id")).isNull();
      assertThat(view.getProperty(view.getNodeId(rids[1]), "name")).isEqualTo("renamed");
    }

    @Test
    void aPropertyOutsideTheFilterRemovedLeavesTheFilteredOnesAlone() {
      final GraphAnalyticalView view = createView("SYNCHRONOUS", "PROPERTIES (id)");
      database.transaction(() -> {
        final MutableVertex vertex = rids[1].asVertex().modify();
        vertex.remove("name");
        vertex.save();
      });

      assertThat(view.getProperty(view.getNodeId(rids[1]), "id")).isEqualTo(3L);
      assertThat(view.getProperty(view.getNodeId(rids[1]), "name")).isNull();
      assertThat(rows()).containsExactlyInAnyOrder("[xi=2, yi=3]", "[xi=3, yi=4]");
    }

    /** The path 2 -> 3 -> 4 after the middle vertex lost its id: [2 -> null] and [null -> 4], and no pair compares as less than. */
    private void assertRemoved(final GraphAnalyticalView view) {
      assertThat(view.getProperty(view.getNodeId(rids[1]), "id")).as("the view's own value").isNull();
      assertThat(rows()).containsExactlyInAnyOrder("[xi=2, yi=null]", "[xi=null, yi=4]");

      try (final ResultSet rs = database.query("opencypher", "MATCH (x:P)-[:K]->(y:P) WHERE x.id < y.id RETURN count(*) AS n")) {
        assertThat(rs.next().<Number>getProperty("n").longValue()).isZero();
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH (y:P)<-[:K]-(:P) WHERE y.id = 3 RETURN count(*) AS n")) {
        assertThat(rs.next().<Number>getProperty("n").longValue()).isZero();
      }
    }

    private void removeId(final RID rid) {
      final MutableVertex vertex = rid.asVertex().modify();
      vertex.remove("id");
      vertex.save();
    }

    private List<String> rows() {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", ROWS)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          rows.add("[xi=" + r.getProperty("xi") + ", yi=" + r.getProperty("yi") + "]");
        }
      }
      Collections.sort(rows);
      return rows;
    }

    private GraphAnalyticalView createView(final String mode, final String properties) {
      database.command("sql",
          "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P) EDGE TYPES (K) " + properties + " UPDATE MODE " + mode).close();
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      return view;
    }

    /** An ASYNCHRONOUS view merges a commit off the committing thread: wait until it has. */
    private void awaitMerged(final GraphAnalyticalView view) {
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    }
  }

  /**
   * Regression for #10339: an edge created in the transaction that deletes its target stayed in a Graph Analytical View after the commit
   * when the target sat on the record slot of an earlier deleted vertex. The target's slot still names the deleted base node, so
   * once the target left the overflow the edge was added to the overlay against that node, while its deletion, resolved through
   * the overflow vertex the same delta deleted, named the overflow node: the two never met, and the deletion of a lightweight edge,
   * which has no identity to withdraw the addition by, left it in the view. A regular edge was withdrawn by its RID and went
   * unnoticed.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ViewEdgeOfDeletedTarget extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE EDGE TYPE L");
    }

    @ParameterizedTest(name = "{0}, lightweight={1}, reused slot: {2}")
    @CsvSource({ "SYNCHRONOUS,true,TARGET", "SYNCHRONOUS,true,SOURCE", "SYNCHRONOUS,true,NONE", "SYNCHRONOUS,false,TARGET",
        "SYNCHRONOUS,false,SOURCE", "SYNCHRONOUS,false,NONE", "ASYNCHRONOUS,true,TARGET", "ASYNCHRONOUS,true,SOURCE",
        "ASYNCHRONOUS,true,NONE", "ASYNCHRONOUS,false,TARGET", "ASYNCHRONOUS,false,SOURCE", "ASYNCHRONOUS,false,NONE" })
    void anEdgeCreatedInTheTransactionThatDeletesItsTargetIsNotInTheView(final String mode, final boolean lightweight,
        final String reused) {
      final Graph graph = new Graph(mode, reused);

      database.transaction(() -> {
        link(graph, lightweight);
        graph.target.delete();
      });
      assertThat(graph.view.awaitReady(60, TimeUnit.SECONDS)).as("the change is merged").isTrue();

      assertThat(edgesAnsweredByCypher()).as("count(r) of the view").isZero();
      assertThat(graph.view.getEdgeCount()).as("the edges the view holds").isZero();
      assertNoNegativeDegree();
    }

    /** The same sequence with the target left alive: the edge is in the view, on every kind of slot. */
    @ParameterizedTest(name = "{0}, lightweight={1}, reused slot: {2}")
    @CsvSource({ "SYNCHRONOUS,true,TARGET", "SYNCHRONOUS,true,SOURCE", "SYNCHRONOUS,true,NONE", "SYNCHRONOUS,false,TARGET",
        "ASYNCHRONOUS,true,TARGET", "ASYNCHRONOUS,false,SOURCE" })
    void anEdgeToASurvivingTargetIsInTheView(final String mode, final boolean lightweight, final String reused) {
      final Graph graph = new Graph(mode, reused);

      database.transaction(() -> link(graph, lightweight));
      assertThat(graph.view.awaitReady(60, TimeUnit.SECONDS)).as("the change is merged").isTrue();

      assertThat(edgesAnsweredByCypher()).isEqualTo(1);
      assertThat(graph.view.getEdgeCount()).isEqualTo(1);

      // and leaves it with the vertex, in a later transaction
      database.transaction(() -> graph.target.delete());
      assertThat(graph.view.awaitReady(60, TimeUnit.SECONDS)).as("the change is merged").isTrue();

      assertThat(edgesAnsweredByCypher()).isZero();
      assertThat(graph.view.getEdgeCount()).isZero();
      assertNoNegativeDegree();
    }

    /** The edge created in the transaction that deletes its SOURCE: the same slot, the other end. */
    @ParameterizedTest(name = "{0}, lightweight={1}, reused slot: {2}")
    @CsvSource({ "SYNCHRONOUS,true,SOURCE", "SYNCHRONOUS,false,SOURCE", "SYNCHRONOUS,true,TARGET", "ASYNCHRONOUS,true,SOURCE",
        "ASYNCHRONOUS,false,TARGET" })
    void anEdgeCreatedInTheTransactionThatDeletesItsSourceIsNotInTheView(final String mode, final boolean lightweight,
        final String reused) {
      final Graph graph = new Graph(mode, reused);

      database.transaction(() -> {
        link(graph, lightweight);
        graph.source.delete();
      });
      assertThat(graph.view.awaitReady(60, TimeUnit.SECONDS)).as("the change is merged").isTrue();

      assertThat(edgesAnsweredByCypher()).isZero();
      assertThat(graph.view.getEdgeCount()).isZero();
      assertNoNegativeDegree();
    }

    private void link(final Graph graph, final boolean lightweight) {
      if (lightweight)
        graph.source.newLightEdge("L", graph.target);
      else
        graph.source.newEdge("L", graph.target).save();
    }

    /**
     * Two vertices created after the view was built, one of them on the slot of a base vertex deleted before ({@code reused}:
     * SOURCE or TARGET, NONE keeps the base vertex).
     */
    private final class Graph {
      final GraphAnalyticalView view;
      final MutableVertex       source;
      final MutableVertex       target;

      Graph(final String mode, final String reused) {
        final RID[] first = new RID[1];
        database.transaction(() -> first[0] = database.newVertex("P").set("id", 3L).save().getIdentity());

        database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P) EDGE TYPES (L) PROPERTIES (id) UPDATE MODE " + mode)
            .close();
        view = GraphAnalyticalViewRegistry.get(database, "gav");
        assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();

        final MutableVertex[] created = new MutableVertex[2];
        if (reused.equals("SOURCE")) {
          database.transaction(() -> first[0].asVertex().modify().delete());
          database.transaction(() -> created[0] = database.newVertex("P").set("id", 7L).save());
          database.transaction(() -> created[1] = database.newVertex("P").set("id", 9L).save());
          assertThat(created[0].getIdentity()).as("precondition: the source takes the freed slot").isEqualTo(first[0]);
        } else if (reused.equals("TARGET")) {
          // a slot freed by a transaction is not reused by that same transaction: the source is created beside the delete, the
          // target in the next one
          database.transaction(() -> {
            first[0].asVertex().modify().delete();
            created[0] = database.newVertex("P").set("id", 7L).save();
          });
          database.transaction(() -> created[1] = database.newVertex("P").set("id", 9L).save());
          assertThat(created[1].getIdentity()).as("precondition: the target takes the freed slot").isEqualTo(first[0]);
        } else {
          database.transaction(() -> created[0] = database.newVertex("P").set("id", 7L).save());
          database.transaction(() -> created[1] = database.newVertex("P").set("id", 9L).save());
          assertThat(created[0].getIdentity()).isNotEqualTo(first[0]);
          assertThat(created[1].getIdentity()).isNotEqualTo(first[0]);
        }
        source = created[0];
        target = created[1];
      }
    }

    private long edgesAnsweredByCypher() {
      try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r:L]->() RETURN count(r) AS c")) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    private void assertNoNegativeDegree() {
      try (final ResultSet rs = database.query("opencypher",
          "CALL algo.degree() YIELD node, inDegree, outDegree, degree RETURN inDegree AS i, outDegree AS o, degree AS d")) {
        while (rs.hasNext()) {
          final var r = rs.next();
          assertThat(r.<Number>getProperty("i").longValue()).isGreaterThanOrEqualTo(0);
          assertThat(r.<Number>getProperty("o").longValue()).isGreaterThanOrEqualTo(0);
          assertThat(r.<Number>getProperty("d").longValue()).isGreaterThanOrEqualTo(0);
        }
      }
    }
  }

  /**
   * Regression for #10340: {@code algo.degree()} on a Graph Analytical View answered a negative out-degree for the source of an edge
   * created in the transaction that deleted its target, when the target had been created after the view was built. The edge was
   * never added to the overlay (its target had left the overflow by the time the added edges were resolved), but its cascaded
   * deletion was then recorded against the base CSR as the deletion of an edge the overlay holds, and the degree read off the
   * view subtracted an edge it had never counted.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ViewDegreeOfDeletedTarget extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE VERTEX TYPE Q");
      database.command("sql", "CREATE EDGE TYPE L");
    }

    /** The reproduction of the issue: the view on the empty database, so every vertex is an overflow node. */
    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void sourceAndTargetOnlyInTheOverlay(final String mode) {
      final GraphAnalyticalView view = createView(mode);

      final MutableVertex[] v = new MutableVertex[4];
      database.transaction(() -> {
        v[0] = database.newVertex("P").set("id", 3L).save();
        v[3] = database.newVertex("P").set("id", 2L).save();
      });
      database.transaction(() -> v[1] = database.newVertex("P").set("w", 0.2).save());
      database.transaction(() -> {
        v[0].newEdge("L", v[3]);
        v[2] = database.newVertex("Q").set("id", 5L).save();
      });
      database.transaction(() -> {
        v[1].newEdge("L", v[2]);
        v[2].delete();
      });
      awaitMerged(view);

      assertDegreesMatchTheRecords(view, 1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void sourceInTheBaseTargetOnlyInTheOverlay(final String mode) {
      final RID[] source = new RID[1];
      database.transaction(() -> source[0] = database.newVertex("P").set("id", 1L).save().getIdentity());
      final GraphAnalyticalView view = createView(mode);

      database.transaction(() -> {
        final MutableVertex target = database.newVertex("Q").set("id", 2L).save();
        source[0].asVertex().modify().newEdge("L", target);
        target.delete();
      });
      awaitMerged(view);

      assertDegreesMatchTheRecords(view, 0);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void sourceOnlyInTheOverlayTargetInTheBase(final String mode) {
      final RID[] target = new RID[1];
      database.transaction(() -> target[0] = database.newVertex("Q").set("id", 2L).save().getIdentity());
      final GraphAnalyticalView view = createView(mode);

      database.transaction(() -> {
        final MutableVertex source = database.newVertex("P").set("id", 1L).save();
        source.newEdge("L", target[0]);
        target[0].asVertex().delete();
      });
      awaitMerged(view);

      assertDegreesMatchTheRecords(view, 0);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void sourceAndTargetInTheBase(final String mode) {
      final RID[] rids = new RID[2];
      database.transaction(() -> {
        rids[0] = database.newVertex("P").set("id", 1L).save().getIdentity();
        rids[1] = database.newVertex("Q").set("id", 2L).save().getIdentity();
      });
      final GraphAnalyticalView view = createView(mode);

      database.transaction(() -> {
        rids[0].asVertex().modify().newEdge("L", rids[1]);
        rids[1].asVertex().delete();
      });
      awaitMerged(view);

      assertDegreesMatchTheRecords(view, 0);
    }

    /** A self-loop is an edge of an overflow vertex at both ends: it goes with the vertex, and so does the edge to its neighbour. */
    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void aSelfLoopOnAnOverflowVertexDeletedInTheSameTransaction(final String mode) {
      final GraphAnalyticalView view = createView(mode);

      final MutableVertex[] v = new MutableVertex[2];
      database.transaction(() -> {
        v[0] = database.newVertex("P").set("id", 1L).save();
        v[1] = database.newVertex("P").set("id", 2L).save();
      });
      database.transaction(() -> {
        v[0].newEdge("L", v[0]);
        v[0].newEdge("L", v[1]);
        v[0].delete();
      });
      awaitMerged(view);

      assertDegreesMatchTheRecords(view, 0);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void aSelfLoopOnAnOverflowVertexIsCountedOnceAndLeavesWithIt(final String mode) {
      final GraphAnalyticalView view = createView(mode);

      final MutableVertex[] v = new MutableVertex[1];
      database.transaction(() -> v[0] = database.newVertex("P").set("id", 1L).save());
      database.transaction(() -> v[0].newEdge("L", v[0]));
      awaitMerged(view);

      assertDegreesMatchTheRecords(view, 1);

      database.transaction(() -> v[0].delete());
      awaitMerged(view);

      assertDegreesMatchTheRecords(view, 0);
    }

    /** The edges of the overlay deleted on their own, in a later transaction than the one that created them, are withdrawn. */
    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void anOverlayEdgeDeletedInALaterTransactionIsWithdrawn(final String mode) {
      final RID[] base = new RID[1];
      database.transaction(() -> base[0] = database.newVertex("P").set("id", 1L).save().getIdentity());
      final GraphAnalyticalView view = createView(mode);

      final RID[] edges = new RID[2];
      database.transaction(() -> {
        final MutableVertex b = database.newVertex("Q").set("id", 2L).save();
        final MutableVertex c = database.newVertex("Q").set("id", 3L).save();
        // a base source to an overflow target, and two overflow vertices
        edges[0] = base[0].asVertex().modify().newEdge("L", b).save().getIdentity();
        edges[1] = b.newEdge("L", c).save().getIdentity();
      });
      awaitMerged(view);

      assertDegreesMatchTheRecords(view, 2);

      database.transaction(() -> edges[0].asEdge().delete());
      awaitMerged(view);

      assertDegreesMatchTheRecords(view, 1);

      database.transaction(() -> edges[1].asEdge().delete());
      awaitMerged(view);

      assertDegreesMatchTheRecords(view, 0);
    }

    /** A target that survives keeps the edge, whichever side of the build it was created on. */
    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
    void aSurvivingTargetKeepsTheEdge(final String mode) {
      final GraphAnalyticalView view = createView(mode);

      final MutableVertex[] v = new MutableVertex[2];
      database.transaction(() -> {
        v[0] = database.newVertex("P").set("id", 1L).save();
        v[1] = database.newVertex("Q").set("id", 2L).save();
      });
      database.transaction(() -> v[0].newEdge("L", v[1]));
      awaitMerged(view);

      assertDegreesMatchTheRecords(view, 1);

      database.transaction(() -> v[1].delete());
      awaitMerged(view);

      assertDegreesMatchTheRecords(view, 0);
    }

    /** Every degree the view answers is the one the records give, and the view counts the edges that are left. */
    private void assertDegreesMatchTheRecords(final GraphAnalyticalView view, final int expectedEdges) {
      int nodes = 0;
      try (final ResultSet rs = database.query("opencypher",
          "CALL algo.degree() YIELD node, inDegree, outDegree, degree RETURN node AS n, inDegree AS i, outDegree AS o, degree AS d")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          final Vertex vertex = r.<Identifiable>getProperty("n").asVertex();
          final long out = vertex.countEdges(Vertex.DIRECTION.OUT, "L");
          final long in = vertex.countEdges(Vertex.DIRECTION.IN, "L");
          assertThat(r.<Number>getProperty("o").longValue()).as("out-degree of " + vertex.getIdentity()).isEqualTo(out);
          assertThat(r.<Number>getProperty("i").longValue()).as("in-degree of " + vertex.getIdentity()).isEqualTo(in);
          assertThat(r.<Number>getProperty("d").longValue()).as("degree of " + vertex.getIdentity()).isEqualTo(out + in);
          nodes++;
        }
      }
      assertThat(nodes).as("every live vertex is answered").isEqualTo(countVertices());
      assertThat(view.getEdgeCount()).as("the edges the view holds").isEqualTo(expectedEdges);
    }

    private long countVertices() {
      long count = 0;
      for (final String type : new String[] { "P", "Q" })
        try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + type)) {
          count += rs.next().<Number>getProperty("c").longValue();
        }
      return count;
    }

    private GraphAnalyticalView createView(final String mode) {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P, Q) EDGE TYPES (L) PROPERTIES (id) UPDATE MODE " + mode)
          .close();
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      return view;
    }

    private void awaitMerged(final GraphAnalyticalView view) {
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    }
  }

  /**
   * Regression for #10369: deleting a vertex that carries a LIGHTWEIGHT self-loop reported the self-loop deleted twice, once from each
   * of the vertex's two edge lists. A record-backed edge is reported by its RID, so the overlay of a Graph Analytical View
   * absorbed the second report; a lightweight one has no identity of its own, so the second report spent a second deletion
   * against the base run of the pair and the view's edge count drifted one lower per self-loop, down to negative numbers.
   * The engine now walks a self-loop once: the incoming walk of a deleted vertex skips the self-loops the outgoing walk
   * already deleted.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ViewSelfLoopOfDeletedVertex extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("P");
        database.getSchema().createEdgeType("R");
        database.getSchema().buildEdgeType().withName("W").withLightweight(true).create();
      });
    }

    /** The reproduction of the issue, in every cell of the matrix: edge kind x where the self-loop is x update mode. */
    @ParameterizedTest
    @CsvSource({
        "W, true, SYNCHRONOUS", "W, false, SYNCHRONOUS", "R, true, SYNCHRONOUS", "R, false, SYNCHRONOUS",
        "W, true, ASYNCHRONOUS", "W, false, ASYNCHRONOUS", "R, true, ASYNCHRONOUS", "R, false, ASYNCHRONOUS" })
    void aDeletedVertexTakesItsSelfLoopOnce(final String edgeType, final boolean selfLoopInTheBase, final String mode) {
      final RID[] v = new RID[3];
      database.transaction(() -> {
        for (int i = 0; i < 3; i++)
          v[i] = database.newVertex("P").set("id", i).save().getIdentity();
        // a control edge the delete must not touch
        v[1].asVertex().modify().newEdge(edgeType, v[2]);
      });

      if (selfLoopInTheBase)
        createSelfLoop(v[0], edgeType);
      final GraphAnalyticalView view = createView(edgeType, mode);
      if (!selfLoopInTheBase) {
        createSelfLoop(v[0], edgeType);
        awaitMerged(view);
      }
      assertThat(view.getEdgeCount()).as("the self-loop and the control edge").isEqualTo(2);

      database.transaction(() -> v[0].asVertex().delete());
      awaitMerged(view);

      assertThat(view.getEdgeCount()).as("only the control edge is left").isEqualTo(1);
      assertDegreesMatchTheRecords(view, edgeType);
    }

    /** The drift accumulated one edge per self-loop: five of them took the count of a one-edge graph to -4. */
    @ParameterizedTest
    @CsvSource({ "W, SYNCHRONOUS", "W, ASYNCHRONOUS", "R, SYNCHRONOUS", "R, ASYNCHRONOUS" })
    void manySelfLoopsDoNotDrift(final String edgeType, final String mode) {
      final RID[] v = new RID[7];
      database.transaction(() -> {
        for (int i = 0; i < v.length; i++)
          v[i] = database.newVertex("P").set("id", i).save().getIdentity();
        v[5].asVertex().modify().newEdge(edgeType, v[6]);
        for (int i = 0; i < 5; i++)
          v[i].asVertex().modify().newEdge(edgeType, v[i]);
      });
      final GraphAnalyticalView view = createView(edgeType, mode);
      assertThat(view.getEdgeCount()).isEqualTo(6);

      database.transaction(() -> {
        for (int i = 0; i < 5; i++)
          v[i].asVertex().delete();
      });
      awaitMerged(view);

      assertThat(view.getEdgeCount()).isEqualTo(1);
      try (final ResultSet rs = database.query("sql", "SELECT edgeCount FROM schema:graphAnalyticalViews WHERE name = 'gav'")) {
        assertThat(rs.next().<Number>getProperty("edgeCount").longValue()).isEqualTo(1L);
      }
      assertDegreesMatchTheRecords(view, edgeType);
    }

    /** A self-loop deleted directly, and one of two parallel self-loops, were already right: they stay so. */
    @ParameterizedTest
    @CsvSource({ "W, SYNCHRONOUS", "R, SYNCHRONOUS", "W, ASYNCHRONOUS", "R, ASYNCHRONOUS" })
    void parallelSelfLoopsLeaveWithTheirVertex(final String edgeType, final String mode) {
      final RID[] v = new RID[2];
      database.transaction(() -> {
        v[0] = database.newVertex("P").set("id", 0).save().getIdentity();
        v[1] = database.newVertex("P").set("id", 1).save().getIdentity();
        final MutableVertex loop = v[0].asVertex().modify();
        loop.newEdge(edgeType, loop);
        loop.newEdge(edgeType, loop);
        loop.newEdge(edgeType, v[1]);
      });
      final GraphAnalyticalView view = createView(edgeType, mode);
      assertThat(view.getEdgeCount()).isEqualTo(3);

      database.transaction(() -> v[0].asVertex().delete());
      awaitMerged(view);

      assertThat(view.getEdgeCount()).isEqualTo(0);
      assertDegreesMatchTheRecords(view, edgeType);
    }

    private void createSelfLoop(final RID vertex, final String edgeType) {
      database.transaction(() -> {
        final MutableVertex loop = vertex.asVertex().modify();
        loop.newEdge(edgeType, loop);
      });
    }

    /** Every live vertex's degrees, read every way the view answers them, are the ones its records give. */
    private void assertDegreesMatchTheRecords(final GraphAnalyticalView view, final String edgeType) {
      final int bound = view.getNodeIdUpperBound();
      final int[] outDegrees = new int[bound];
      final int[] inDegrees = new int[bound];
      view.getDegrees(outDegrees, Vertex.DIRECTION.OUT, edgeType);
      view.getDegrees(inDegrees, Vertex.DIRECTION.IN, edgeType);
      database.transaction(() -> {
        for (int nodeId = 0; nodeId < bound; nodeId++) {
          if (!view.isNodeLive(nodeId)) {
            assertThat(outDegrees[nodeId]).as("out-degree of the deleted node " + nodeId).isZero();
            assertThat(inDegrees[nodeId]).as("in-degree of the deleted node " + nodeId).isZero();
            assertThat(view.countEdges(nodeId, Vertex.DIRECTION.BOTH, edgeType)).as("edges of the deleted node " + nodeId).isZero();
            continue;
          }
          final Vertex vertex = view.getRID(nodeId).asVertex();
          final long out = vertex.countEdges(Vertex.DIRECTION.OUT, edgeType);
          final long in = vertex.countEdges(Vertex.DIRECTION.IN, edgeType);
          assertThat(outDegrees[nodeId]).as("out-degree of " + vertex.getIdentity()).isEqualTo(out);
          assertThat(inDegrees[nodeId]).as("in-degree of " + vertex.getIdentity()).isEqualTo(in);
          assertThat(view.countEdges(nodeId, Vertex.DIRECTION.OUT, edgeType)).isEqualTo(out);
          assertThat(view.countEdges(nodeId, Vertex.DIRECTION.IN, edgeType)).isEqualTo(in);
        }
      });
    }

    private GraphAnalyticalView createView(final String edgeType, final String mode) {
      database.command("sql",
          "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P) EDGE TYPES (" + edgeType + ") PROPERTIES (id) UPDATE MODE " + mode).close();
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      return view;
    }

    private void awaitMerged(final GraphAnalyticalView view) {
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    }
  }

  /**
   * Regression for #8948.
   * <p>
   * Regression test for issue #8948: in a SYNCHRONOUS view, a new vertex taking the RID of a deleted base vertex
   * was skipped as "already in base" and so stayed marked deleted, hiding its edges from the Cypher count paths.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GAVReusedVertexRid extends TestHelper {

    @Test
    void reusedVertexRidKeepsItsEdgesInCounts() throws Exception {
      createSchemaAndView();

      final RID c = vertex("c").getIdentity();
      database.transaction(() -> vertex("c").delete());
      database.transaction(() -> {
        final MutableVertex nv = database.newVertex("V").set("name", "d").set("age", 20).save();
        assertThat(nv.getIdentity()).as("precondition: the new vertex reuses the deleted RID").isEqualTo(c);
        vertex("a").newEdge("K", nv);
        nv.newEdge("K", vertex("a"));
      });

      assertCounts();
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
      assertCounts();
    }

    /** Guard test: no RID reuse here, it pins that cascaded edge deletions of a plain base vertex are still recorded. */
    @Test
    void deletingVertexWithEdgesKeepsEdgeCountInStep() throws Exception {
      createSchemaAndView();
      database.transaction(() -> {
        vertex("a").newEdge("K", vertex("c"));
        vertex("c").newEdge("K", vertex("a"));
      });
      final GraphAnalyticalView gav = GraphAnalyticalViewRegistry.get(database, "g");
      assertThat(gav.getEdgeCount()).isEqualTo(2);
      database.transaction(() -> vertex("c").delete());
      assertThat(gav.getEdgeCount()).as("cascaded edge deletions must still be recorded").isEqualTo(0);
      assertThat(count("MATCH (x:V)-[:K]->(y:V) RETURN count(*) AS n")).isEqualTo(0L);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
    }

    @Test
    void reusedVertexPropertyUpdateAndDelete() throws Exception {
      createSchemaAndView();
      final RID c = vertex("c").getIdentity();
      database.transaction(() -> vertex("c").delete());
      database.transaction(() -> {
        final MutableVertex nv = database.newVertex("V").set("name", "d").set("age", 20).save();
        assertThat(nv.getIdentity()).as("precondition: the new vertex reuses the deleted RID").isEqualTo(c);
        vertex("a").newEdge("K", nv);
        nv.newEdge("K", vertex("a"));
      });
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
      assertThat(view.getNodeId(c)).as("the reused vertex is an overflow node").isGreaterThanOrEqualTo(2);
      assertThat(view.getEdgeCount()).isEqualTo(2);
      database.transaction(() -> vertex("d").set("age", 50).save());
      assertThat(count("MATCH (x:V)-[:K]->(y:V) WHERE y.age = 50 RETURN count(*) AS n")).isEqualTo(1L);
      database.transaction(() -> vertex("d").delete());
      assertThat(count("MATCH (x:V)-[:K]->(y:V) RETURN count(*) AS n")).isEqualTo(0L);
      assertThat(count("MATCH (y:V) RETURN count(*) AS n")).isEqualTo(1L);
      assertThat(GraphAnalyticalViewRegistry.get(database, "g").getEdgeCount()).isEqualTo(0);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
    }

    @Test
    void slotReusedTwice() throws Exception {
      createSchemaAndView();
      final RID c = vertex("c").getIdentity();
      database.transaction(() -> vertex("c").delete());
      database.transaction(() -> {
        final MutableVertex d = database.newVertex("V").set("name", "d").set("age", 20).save();
        assertThat(d.getIdentity()).as("precondition: d reuses the deleted RID").isEqualTo(c);
        vertex("a").newEdge("K", d);
      });
      database.transaction(() -> vertex("d").delete());
      database.transaction(() -> {
        final MutableVertex e = database.newVertex("V").set("name", "e").set("age", 40).save();
        assertThat(e.getIdentity()).as("precondition: e reuses the same RID again").isEqualTo(c);
        vertex("a").newEdge("K", e);
      });
      assertThat(count("MATCH (x:V)-[:K]->(y:V) RETURN count(*) AS n")).isEqualTo(1L);
      assertThat(count("MATCH (x:V)-[:K]->(y:V) WHERE y.age = 40 RETURN count(*) AS n")).isEqualTo(1L);
      assertThat(GraphAnalyticalViewRegistry.get(database, "g").getEdgeCount()).isEqualTo(1);
      database.transaction(() -> vertex("e").delete());
      assertThat(count("MATCH (x:V)-[:K]->(y:V) RETURN count(*) AS n")).isEqualTo(0L);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
    }

    /** Pins the invariant DeltaOverlay.merge() relies on: a TxDelta never carries both the delete and the reuse of a slot. */
    @Test
    void deleteAndCreateInOneTransaction() throws Exception {
      createSchemaAndView();
      final RID c = vertex("c").getIdentity();
      final RID[] reused = new RID[1];
      database.transaction(() -> {
        vertex("c").delete();
        final MutableVertex d = database.newVertex("V").set("name", "d").set("age", 20).save();
        reused[0] = d.getIdentity();
        vertex("a").newEdge("K", d);
      });
      assertThat(reused[0]).as("a slot freed by a transaction is not reused by that same transaction").isNotEqualTo(c);
      assertThat(count("MATCH (x:V)-[:K]->(y:V) RETURN count(*) AS n")).isEqualTo(1L);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
    }

    /** A plain new vertex (no slot reuse) must take later property updates too. */
    @Test
    void propertyUpdateOfPlainNewVertex() throws Exception {
      createSchemaAndView();
      database.transaction(() -> vertex("a").newEdge("K", database.newVertex("V").set("name", "n").set("age", 20).save()));
      database.transaction(() -> vertex("n").set("age", 60).save());
      assertThat(count("MATCH (x:V)-[:K]->(y:V) WHERE y.age = 60 RETURN count(*) AS n")).isEqualTo(1L);
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
    }

    private void createSchemaAndView() throws Exception {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE PROPERTY V.name STRING");
      database.command("sql", "CREATE PROPERTY V.age INTEGER");
      database.command("sql", "CREATE EDGE TYPE K");
      database.transaction(() -> {
        database.newVertex("V").set("name", "a").set("age", 30).save();
        database.newVertex("V").set("name", "c").set("age", 10).save();
      });
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (V) EDGE TYPES (K) PROPERTIES (age) UPDATE MODE SYNCHRONOUS");
      GraphAnalyticalViewRegistry.get(database, "g").awaitReady(60, TimeUnit.SECONDS);
    }

    private void assertCounts() {
      assertThat(count("MATCH (x:V)-[:K]->(y:V) RETURN count(*) AS n")).isEqualTo(2L);
      assertThat(count("MATCH (x:V)-[:K]->(y:V) WHERE y.age > 15 RETURN count(*) AS n")).isEqualTo(2L);
      final StringBuilder groups = new StringBuilder();
      database.query("opencypher", "MATCH (x:V)-[:K]->(y:V) RETURN y.age AS age, count(*) AS n ORDER BY age").stream()
          .forEach(r -> groups.append("(").append(r.<Object>getProperty("age")).append(",").append(r.<Object>getProperty("n")).append(")"));
      assertThat(groups.toString()).isEqualTo("(20,1)(30,1)");
    }

    private long count(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }

    private MutableVertex vertex(final String name) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM V WHERE name = ?", name)) {
        return rs.next().getVertex().get().modify();
      }
    }
  }

  /**
   * Regression for #8948, post-compaction replay: the fresh base already holds the new vertex sitting on a reused slot (and its
   * edge), then the buffered "delete old vertex" and "add new vertex + edge" deltas are replayed over it. The edge must
   * be visible exactly once, and deleting the new vertex afterwards must leave nothing behind.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ReplayReusedVertexRid {
    private static final String EDGE_TYPE = "K";
    private static final RID    A      = new RID(1, 0);
    private static final RID    REUSED = new RID(1, 1);
    private static final RID    EDGE   = new RID(2, 20);

    @Test
    void replayedReuseOverAFreshBaseThatAlreadySawTheNewVertexCountsTheEdgeOnce() {
      final NodeIdMapping mapping = new NodeIdMapping(1);
      final int bucket = mapping.registerBucket(1, "V", 2);
      mapping.addNode(bucket, 0);
      mapping.addNode(bucket, 1);
      mapping.compact();
      // fresh base: a(0) -> new vertex(1), captured by the scan
      final CSRAdjacencyIndex csr = new CSRAdjacencyIndex(new int[] { 0, 1, 1 }, new int[] { 1 }, new int[] { 0, 0, 1 }, new int[] { 0 }, 2, 1);
      final Map<String, CSRAdjacencyIndex> fresh = Map.of(EDGE_TYPE, csr);
      final DeltaOverlay.PreCompactionPairCount preCount = (type, src, tgt) -> 0;

      final TxDelta deleteOld = new TxDelta();
      deleteOld.deletedVertices.add(REUSED);
      final TxDelta addNew = new TxDelta();
      addNew.addedVertices.add(new TxDelta.VertexDelta(REUSED, Map.of()));
      addNew.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, A, REUSED, EDGE));

      DeltaOverlay overlay = new DeltaOverlay(mapping.size());
      overlay = overlay.merge(deleteOld, mapping, fresh, preCount);
      overlay = overlay.merge(addNew, mapping, fresh, preCount);

      final int newId = overlay.resolveNodeId(REUSED, mapping);
      assertThat(newId).as("the new vertex is an overflow node").isGreaterThanOrEqualTo(mapping.size());
      assertThat(overlay.isDeleted(1)).as("the base node stays masked, hiding the scan's copy of the edge").isTrue();
      assertThat(overlay.getAddedOutNeighbors(0, EDGE_TYPE)).as("the edge is re-created once, on the overflow node").containsExactly(newId);

      assertThat(csr.getEdgeCount() + overlay.getDeltaEdgeCount()).as("the view's edge count: one edge, counted once").isEqualTo(1);

      final TxDelta deleteNew = new TxDelta();
      deleteNew.deletedVertices.add(REUSED);
      deleteNew.deletedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, A, REUSED, EDGE));
      final DeltaOverlay afterDelete = overlay.merge(deleteNew, mapping, fresh, preCount);

      assertThat(afterDelete.getAddedOutNeighbors(0, EDGE_TYPE)).isEmpty();
      assertThat(csr.getEdgeCount() + afterDelete.getDeltaEdgeCount()).as("no edge left after the delete").isZero();
    }

    /** The scan ran after the delete committed: the fresh base lacks the old vertex, so only the add is buffered. */
    @Test
    void replayedAddAloneOverAFreshBaseThatLacksTheOldVertexLandsInTheOverflow() {
      final NodeIdMapping mapping = new NodeIdMapping(1);
      final int bucket = mapping.registerBucket(1, "V", 1);
      mapping.addNode(bucket, 0);
      mapping.compact();
      final CSRAdjacencyIndex csr = new CSRAdjacencyIndex(new int[] { 0, 0 }, new int[0], new int[] { 0, 0 }, new int[0], 1, 0);
      final Map<String, CSRAdjacencyIndex> fresh = Map.of(EDGE_TYPE, csr);

      final TxDelta addNew = new TxDelta();
      addNew.addedVertices.add(new TxDelta.VertexDelta(REUSED, Map.of("age", 20)));
      addNew.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, A, REUSED, EDGE));
      final DeltaOverlay overlay = new DeltaOverlay(mapping.size()).merge(addNew, mapping, fresh, (type, src, tgt) -> 0);

      final int newId = overlay.resolveNodeId(REUSED, mapping);
      assertThat(newId).isGreaterThanOrEqualTo(mapping.size());
      assertThat(overlay.getAddedOutNeighbors(0, EDGE_TYPE)).containsExactly(newId);
      assertThat(overlay.getPropertyOverride(newId, "age")).isEqualTo(20);

      final TxDelta update = new TxDelta();
      update.updatedProperties.put(REUSED, Map.of("age", 21));
      assertThat(overlay.merge(update, mapping, fresh, (type, src, tgt) -> 0).getPropertyOverride(newId, "age")).isEqualTo(21);
    }

    /** Merging the same add delta twice must not subtract the fresh base's edges twice. */
    @Test
    void replayingTheSameAddTwiceIsIdempotent() {
      final NodeIdMapping mapping = new NodeIdMapping(1);
      final int bucket = mapping.registerBucket(1, "V", 2);
      mapping.addNode(bucket, 0);
      mapping.addNode(bucket, 1);
      mapping.compact();
      final CSRAdjacencyIndex csr = new CSRAdjacencyIndex(new int[] { 0, 1, 1 }, new int[] { 1 }, new int[] { 0, 0, 1 }, new int[] { 0 }, 2, 1);
      final Map<String, CSRAdjacencyIndex> fresh = Map.of(EDGE_TYPE, csr);
      final DeltaOverlay.PreCompactionPairCount preCount = (type, src, tgt) -> 0;

      final TxDelta deleteOld = new TxDelta();
      deleteOld.deletedVertices.add(REUSED);
      final TxDelta addNew = new TxDelta();
      addNew.addedVertices.add(new TxDelta.VertexDelta(REUSED, Map.of()));

      final DeltaOverlay once = new DeltaOverlay(mapping.size()).merge(deleteOld, mapping, fresh, preCount).merge(addNew, mapping, fresh, preCount);
      final DeltaOverlay twice = once.merge(addNew, mapping, fresh, preCount);

      assertThat(twice.getDeltaEdgeCount()).isEqualTo(once.getDeltaEdgeCount());
    }

    /** An edge joining two reused slots, both replayed, is subtracted from the fresh base's count once, not twice. */
    @Test
    void anEdgeBetweenTwoReusedSlotsIsSubtractedOnce() {
      final RID second = new RID(1, 2);
      final NodeIdMapping mapping = new NodeIdMapping(1);
      final int bucket = mapping.registerBucket(1, "V", 3);
      for (int i = 0; i < 3; i++)
        mapping.addNode(bucket, i);
      mapping.compact();
      // fresh base: one edge 1 -> 2 between the two reused slots
      final CSRAdjacencyIndex csr = new CSRAdjacencyIndex(new int[] { 0, 0, 1, 1 }, new int[] { 2 }, new int[] { 0, 0, 0, 1 }, new int[] { 1 }, 3, 1);
      final Map<String, CSRAdjacencyIndex> fresh = Map.of(EDGE_TYPE, csr);
      final DeltaOverlay.PreCompactionPairCount preCount = (type, src, tgt) -> 0;

      final TxDelta deleteOld = new TxDelta();
      deleteOld.deletedVertices.add(REUSED);
      deleteOld.deletedVertices.add(second);
      final TxDelta addNew = new TxDelta();
      addNew.addedVertices.add(new TxDelta.VertexDelta(REUSED, Map.of()));
      addNew.addedVertices.add(new TxDelta.VertexDelta(second, Map.of()));
      addNew.addedEdges.add(new TxDelta.EdgeDelta(EDGE_TYPE, REUSED, second, EDGE));

      final DeltaOverlay overlay = new DeltaOverlay(mapping.size()).merge(deleteOld, mapping, fresh, preCount).merge(addNew, mapping, fresh, preCount);

      assertThat(csr.getEdgeCount() + overlay.getDeltaEdgeCount()).as("one edge, counted once").isEqualTo(1);
    }
  }

  /**
   * Regression for #9715: an edge list may hold an empty segment between two non-empty ones (a GraphBatch prepends a new head over the
   * head a delete emptied). The (edge, vertex) entry iterator stopped at the first empty segment it reached, so a Graph
   * Analytical View built from it missed every edge past that segment, and CHECK DATABASE reported those edges as missing from
   * their outgoing vertex's list.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EmptyEdgeSegment extends TestHelper {
    private static final String COUNT     = "MATCH (a:P {name: 'A'})-[:K]->(b:P) RETURN count(*) AS n";
    // every matched row materialized, so no count push-down can answer it from a counter
    private static final String ROW_COUNT = "MATCH (a:P {name: 'A'})-[r:K]->(b:P) WITH a, r, b RETURN count(r) AS n";

    private RID   a;
    private RID[] targets;

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("P");
      database.getSchema().createEdgeType("K");
      database.transaction(() -> {
        a = database.newVertex("P").set("name", "A").save().getIdentity();
        targets = new RID[7];
        for (int i = 0; i < targets.length; i++)
          targets[i] = database.newVertex("P").set("name", "T" + i).save().getIdentity();
      });

      // one edge per batch, then the newest one deleted: the head segment is left empty
      for (int i = 0; i < 2; i++)
        try (final GraphBatch batch = database.batch().build()) {
          batch.newEdge(a, "K", targets[i]);
        }
      database.transaction(() -> {
        for (final Edge e : vertexA().getEdges(Vertex.DIRECTION.OUT, "K"))
          if (e.getIn().equals(targets[1]))
            e.delete();
      });
    }

    @Test
    void anEmptySegmentLeftBetweenTwoBatchesHidesNoEdge() throws Exception {
      try (final GraphBatch batch = database.batch().build()) {
        batch.newEdge(a, "K", targets[2]);
        batch.newEdge(a, "K", targets[3]);
      }
      assertEveryReaderSeesEdges(3);
    }

    @Test
    void consecutiveEmptySegmentsHideNoEdge() throws Exception {
      // a second batch-and-delete round empties the next head too: two empty segments in a row between non-empty ones
      try (final GraphBatch batch = database.batch().build()) {
        batch.newEdge(a, "K", targets[2]);
        batch.newEdge(a, "K", targets[3]);
      }
      database.transaction(() -> {
        for (final Edge e : vertexA().getEdges(Vertex.DIRECTION.OUT, "K"))
          if (e.getIn().equals(targets[2]) || e.getIn().equals(targets[3]))
            e.delete();
      });
      try (final GraphBatch batch = database.batch().build()) {
        for (int i = 4; i < 7; i++)
          batch.newEdge(a, "K", targets[i]);
      }
      assertThat(longestRunOfEmptySegments()).as("empty segments in a row in the OUT list").isGreaterThanOrEqualTo(2);
      assertEveryReaderSeesEdges(4);
    }

    /**
     * CHECK DATABASE FIX prunes a dangling entry through the iterator's remove(). A hasNext() between next() and remove() can
     * already have stepped over the empty segment into an older one: the entry must still be taken out of the segment it came
     * from, and the older segment left alone.
     */
    @Test
    void removeAfterHasNextSteppedOverAnEmptySegmentPrunesTheReturnedEntry() throws Exception {
      try (final GraphBatch batch = database.batch().build()) {
        batch.newEdge(a, "K", targets[2]);
        batch.newEdge(a, "K", targets[3]);
      }

      // the head segment holds two entries: the second one returned becomes dangling, its edge record deleted behind the list
      Iterator<Pair<RID, RID>> it = outList().entryIterator();
      it.next();
      final RID dangling = it.next().getFirst();
      final RID danglingTarget = database.lookupByRID(dangling, true).asEdge().getIn();
      database.transaction(() -> database.getSchema().getBucketById(dangling.getBucketId()).deleteRecord(dangling));

      database.transaction(() -> {
        final Iterator<Pair<RID, RID>> walk = outList().entryIterator();
        walk.next();
        assertThat(walk.next().getFirst()).isEqualTo(dangling);
        assertThat(walk.hasNext()).as("the oldest segment, past the empty one").isTrue();
        walk.remove();
        assertThat(walk.next().getSecond()).isEqualTo(targets[0]);
        assertThat(walk.hasNext()).isFalse();
      });

      // the dangling entry is gone, every other one is still there
      final Set<RID> neighbours = new HashSet<>();
      for (it = outList().entryIterator(); it.hasNext(); )
        neighbours.add(it.next().getSecond());
      final Set<RID> expected = new HashSet<>(List.of(targets[0], targets[2], targets[3]));
      expected.remove(danglingTarget);
      assertThat(neighbours).isEqualTo(expected);
      // only the list walked is pruned: the far vertex's IN list still names the deleted record, for CHECK DATABASE to report
      assertThat(vertexA().countEdges(Vertex.DIRECTION.OUT, "K")).isEqualTo(2);

      // which the fix prunes, leaving the database clean for the integrity check after the test
      database.command("sql", "CHECK DATABASE FIX").close();
      assertEveryReaderSeesEdges(2);
    }

    private EdgeLinkedList outList() {
      return ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk((VertexInternal) vertexA(), Vertex.DIRECTION.OUT);
    }

    @Test
    void aTransactionAfterTheDeleteHidesNoEdgeEither() throws Exception {
      database.transaction(() -> {
        vertexA().modify().newEdge("K", targets[2]);
        vertexA().modify().newEdge("K", targets[3]);
      });
      assertEveryReaderSeesEdges(3);
    }

    private int longestRunOfEmptySegments() {
      int longest = 0;
      int run = 0;
      for (EdgeSegment segment = (EdgeSegment) database.lookupByRID(((VertexInternal) vertexA()).getOutEdgesHeadChunk(), true);
           segment != null; segment = segment.getPrevious()) {
        run = segment.isEmpty() ? run + 1 : 0;
        longest = Math.max(longest, run);
      }
      return longest;
    }

    private void assertEveryReaderSeesEdges(final int expected) throws Exception {
      assertThat(vertexA().countEdges(Vertex.DIRECTION.OUT, "K")).isEqualTo(expected);
      assertThat(count()).as("MATCH without a view").isEqualTo(expected);

      // the iterator the view build and CHECK DATABASE read the list through
      final EdgeLinkedList list = ((DatabaseInternal) database).getGraphEngine()
          .getEdgeHeadChunk((VertexInternal) vertexA(), Vertex.DIRECTION.OUT);
      int entries = 0;
      for (final Iterator<Pair<RID, RID>> it = list.entryIterator(); it.hasNext(); it.next())
        ++entries;
      assertThat(entries).as("entries of the OUT list").isEqualTo(expected);
      int typedEntries = 0;
      for (final Iterator<Pair<RID, RID>> it = list.entryIterator("K"); it.hasNext(); it.next())
        ++typedEntries;
      assertThat(typedEntries).as("entries of type K of the OUT list").isEqualTo(expected);

      final Map<String, Object> check = new DatabaseChecker(database).setVerboseLevel(0).check();
      assertThat(check.get("edgesMissingOutReference")).isEqualTo(0L);
      assertThat(check.get("edgesMissingInReference")).isEqualTo(0L);
      checkDatabaseIntegrity();

      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (P) EDGE TYPES (K) UPDATE MODE OFF");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      assertThat(view.getEdgeCount()).isEqualTo(expected);
      assertThat(count()).as("MATCH with the view").isEqualTo(expected);

      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g");
      assertThat(count()).as("MATCH once the view is dropped").isEqualTo(expected);
    }

    private Vertex vertexA() {
      return database.lookupByRID(a, true).asVertex();
    }

    /** The count of matched edges, asserted equal whether a counter or the matched rows answer it. */
    private long count() {
      final long counted = count(COUNT);
      assertThat(count(ROW_COUNT)).as(ROW_COUNT).isEqualTo(counted);
      return counted;
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }

  /**
   * Regression for #10368: a Graph Analytical View stores its vertex and edge types by name, in the schema extension it is restored from
   * on every open, yet DROP TYPE and ALTER TYPE ... NAME of one of them went through: the live view kept answering from a CSR
   * that no longer matched the schema, and the next open's build failed on the old name and left the view unbuilt for good, with
   * REBUILD refused because the registry no longer held it. A view is now a dependant like a materialized view: a type it lists
   * cannot be dropped, and a rename of one is carried to its definition and the view rebuilt from it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ViewSourceTypeDropAndRename extends TestHelper {
    private final RID[] vertices = new RID[2];

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE VERTEX TYPE Q");
      database.command("sql", "CREATE VERTEX TYPE R");
      database.command("sql", "CREATE VERTEX TYPE Unlisted");
      database.command("sql", "CREATE EDGE TYPE L");
      database.command("sql", "CREATE EDGE TYPE M");
      database.transaction(() -> {
        final MutableVertex p = database.newVertex("P").set("id", 1).save();
        final MutableVertex q = database.newVertex("Q").set("id", 2).save();
        p.newEdge("L", q);
        vertices[0] = p.getIdentity();
        vertices[1] = q.getIdentity();
      });
    }

    @ParameterizedTest
    @ValueSource(strings = { "M", "R", "P", "L" })
    void aTypeTheViewListsCannotBeDropped(final String typeName) {
      createView("SYNCHRONOUS");

      // UNSAFE, so that P and L, which hold records, reach the guard rather than the refusal of a non-empty type
      assertThatThrownBy(() -> database.command("sql", "DROP TYPE " + typeName + " UNSAFE"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("graph analytical view 'gav'")
          .hasMessageContaining("DROP GRAPH ANALYTICAL VIEW gav");
      assertThat(database.getSchema().existsType(typeName)).isTrue();
    }

    @Test
    void anAliasDoesNotWalkPastTheGuard() {
      database.command("sql", "ALTER TYPE M ALIASES Mx");
      createView("SYNCHRONOUS");

      assertThatThrownBy(() -> database.command("sql", "DROP TYPE Mx")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("edge type of graph analytical view 'gav'");
      assertThat(database.getSchema().existsType("M")).isTrue();
    }

    @Test
    void aTypeTheViewDoesNotListCanBeDropped() {
      createView("SYNCHRONOUS");
      database.command("sql", "DROP TYPE Unlisted");
      assertThat(database.getSchema().existsType("Unlisted")).isFalse();
    }

    @Test
    void droppingTheViewReleasesItsTypes() {
      createView("SYNCHRONOUS");
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav");
      database.command("sql", "DROP TYPE M");
      database.command("sql", "DROP TYPE R");
      assertThat(database.getSchema().existsType("M")).isFalse();
      assertThat(database.getSchema().existsType("R")).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS", "OFF" })
    void aRenamedVertexTypeIsCarriedToTheViewAndSurvivesAReopen(final String mode) {
      createView(mode);

      database.command("sql", "ALTER TYPE Q NAME Q2");

      assertThat(definedTypes("vertexTypes")).containsExactly("P", "Q2", "R");
      GraphAnalyticalView view = awaitView();
      assertThat(view.coversVertexType("Q2")).isTrue();
      assertThat(view.coversVertexType("Q")).isFalse();
      assertThat(view.getEdgeCount()).isEqualTo(1);

      // a vertex of the renamed type and an edge to it reach the view
      database.transaction(() -> vertices[0].asVertex().modify().newEdge("L", database.newVertex("Q2").set("id", 3).save()));
      if (!"OFF".equals(mode)) {
        view = awaitView();
        assertThat(view.getEdgeCount()).isEqualTo(2);
      }

      reopenDatabase();
      view = awaitView();
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
      assertThat(view.coversVertexType("Q2")).isTrue();
      assertThat(view.getEdgeCount()).isEqualTo(2);
      assertThat(view.countEdges(view.getNodeId(vertices[0]), Vertex.DIRECTION.OUT, "L")).isEqualTo(2);
    }

    @Test
    void aRenamedEdgeTypeIsCarriedToTheViewAndSurvivesAReopen() {
      createView("SYNCHRONOUS");

      database.command("sql", "ALTER TYPE L NAME L2");

      assertThat(definedTypes("edgeTypes")).containsExactly("L2", "M");
      GraphAnalyticalView view = awaitView();
      assertThat(view.coversEdgeType("L2")).isTrue();
      assertThat(view.countEdges(view.getNodeId(vertices[0]), Vertex.DIRECTION.OUT, "L2")).isEqualTo(1);

      database.transaction(() -> vertices[1].asVertex().modify().newEdge("L2", vertices[0]));
      view = awaitView();
      assertThat(view.getEdgeCount()).isEqualTo(2);

      reopenDatabase();
      view = awaitView();
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
      assertThat(view.getEdgeCount()).isEqualTo(2);
      try (final ResultSet rs = database.query("opencypher", "MATCH (a:P)-[:L2]->(b) RETURN count(*) AS n")) {
        assertThat(rs.next().<Number>getProperty("n").longValue()).isEqualTo(1L);
      }
    }

    /** A rename of a type the view does not list leaves the view as it is: the same instance, still ready. */
    @Test
    void aRenameOfAnotherTypeLeavesTheViewAlone() {
      final GraphAnalyticalView view = createView("SYNCHRONOUS");
      database.command("sql", "ALTER TYPE Unlisted NAME Unlisted2");
      assertThat(GraphAnalyticalViewRegistry.get(database, "gav")).isSameAs(view);
      assertThat(view.isReady()).isTrue();
    }

    /**
     * A definition left naming a type that no longer exists - by a build before this fix - fails safe: a drop cannot be told
     * apart from the drop of the type it meant, so every drop is refused, and REBUILD, which used to be refused because the view
     * was not loaded, reports the reason the build fails.
     */
    @Test
    void aDanglingDefinitionFailsSafe() {
      createView("SYNCHRONOUS");
      final JSONObject all = database.getSchema().getExtension(GraphAnalyticalViewPersistence.EXTENSION_KEY);
      all.getJSONObject("gav").put("edgeTypes", new JSONArray(List.of("L", "Gone")));
      database.getSchema().setExtension(GraphAnalyticalViewPersistence.EXTENSION_KEY, all);
      GraphAnalyticalViewRegistry.get(database, "gav").shutdown();

      assertThatThrownBy(() -> database.command("sql", "DROP TYPE Unlisted")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("graph analytical view 'gav'").hasMessageContaining("'Gone'").hasMessageContaining("does not exist");

      assertThatThrownBy(() -> database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW gav"))
          .hasMessageContaining("Gone");

      database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav");
      database.command("sql", "DROP TYPE Unlisted");
      assertThatThrownBy(() -> database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW gav"))
          .isInstanceOf(SchemaException.class).hasMessageContaining("does not exist");
    }

    /** REBUILD of a view whose build failed on open builds it again from its definition. */
    @Test
    void rebuildRestoresAViewTheRegistryDropped() {
      createView("SYNCHRONOUS");
      GraphAnalyticalViewRegistry.get(database, "gav").shutdown();
      assertThat(GraphAnalyticalViewRegistry.get(database, "gav")).isNull();

      database.command("sql", "REBUILD GRAPH ANALYTICAL VIEW gav").close();

      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
      assertThat(view).isNotNull();
      assertThat(view.getStatus()).isEqualTo(GraphAnalyticalView.Status.READY);
      assertThat(view.getEdgeCount()).isEqualTo(1);
    }

    private List<Object> definedTypes(final String key) {
      return database.getSchema().getExtension(GraphAnalyticalViewPersistence.EXTENSION_KEY).getJSONObject("gav").getJSONArray(key)
          .toList();
    }

    private GraphAnalyticalView createView(final String mode) {
      database.command("sql",
          "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P, Q, R) EDGE TYPES (L, M) PROPERTIES (id) UPDATE MODE " + mode).close();
      return awaitView();
    }

    private GraphAnalyticalView awaitView() {
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
      assertThat(view).isNotNull();
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      return view;
    }
  }
}
