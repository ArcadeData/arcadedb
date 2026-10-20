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
import com.arcadedb.database.Record;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10305: a transaction that created an edge and deleted it again reported the deletion of that edge's RID to the view. When
 * another transaction had committed an edge into the very same slot of the edge bucket's page in the meantime (the first one holds
 * its own copy of the page, so it handed the slot out again), the view applied the deletion to the other transaction's edge and
 * lost it, while the records and both vertices' edge lists kept it. A record created and deleted by one transaction is no change
 * to the committed state, so it is no change to the view either. Vertices share the slot allocation, and so the shape.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10305ViewSameSlotCreateDeleteTest extends TestHelper {

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
    assertThat(y.get()).as("B was handed the slot A's edge took").isEqualTo(x.get());

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
    assertThat(y.get()).as("B was handed the slot A's vertex took").isEqualTo(x.get());

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
