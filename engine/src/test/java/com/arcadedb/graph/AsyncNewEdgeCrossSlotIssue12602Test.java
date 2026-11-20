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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.Bucket;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12602: {@code database.async().newEdge(source, type, destination, light, callback, props...)} on a
 * bidirectional type appended the edge TWICE to the IN list of the destination when the two vertices map to different async
 * slots. {@code CreateEdgeAsyncTask} runs in the source's slot and wires both sides (the type decides the direction since #2094),
 * then the cascade scheduled in the destination's slot appended the IN entry once more. When both vertices share a slot only the
 * first task runs, which is why {@link EdgeCreationEntryPointsContractTest} (one bucket) never saw it. The record count and CHECK
 * DATABASE do not notice the second entry, so the test reads the adjacency lists themselves.
 * <p>
 * The cascade also ignored an {@code @in} among the edge properties (issue #12247): it wired the destination given to {@code
 * newEdge} instead of the vertex the edge record names, in the slot of the former.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class AsyncNewEdgeCrossSlotIssue12602Test extends TestHelper {
  // THE FILE IDS OF THE BUCKETS OF A TYPE ARE 3 APART (BUCKET + ITS OUT AND IN EDGE BUCKETS): A POOL OF 3 WOULD SERVE THEM ALL ON ONE WORKER
  private static final int SLOTS = 4;

  private DatabaseAsyncExecutorImpl async;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("V").withTotalBuckets(9).create();
      database.getSchema().buildEdgeType().withName("F").create();
      database.getSchema().buildEdgeType().withName("Light").withLightweight(true).create();
      database.getSchema().buildEdgeType().withName("OneWay").withBidirectional(false).create();
    });
    async = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) database).async();
    async.setParallelLevel(SLOTS);
  }

  @Test
  @Timeout(120)
  void aBidirectionalEdgeBetweenTwoSlotsIsInTheDestinationListOnce() {
    final RID a = newVertexInSlot(0);
    final RID b = newVertexInSlot(1);

    final Edge edge = createEdge(a, "F", b, false);

    assertWiredOnce(edge, "F", a, b);
  }

  @Test
  @Timeout(120)
  void aBidirectionalEdgeCreatedWithTheLightFlagIsInTheDestinationListOnce() {
    final RID a = newVertexInSlot(0);
    final RID b = newVertexInSlot(2);

    final Edge edge = createEdge(a, "F", b, true);

    assertThat(edge).isInstanceOf(LightEdge.class);
    assertThat(outEdges(a, "F")).hasSize(1);
    assertThat(inEdges(b, "F")).hasSize(1);
    assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "F")).isEqualTo(1L);
    assertThat(b.asVertex().countEdges(Vertex.DIRECTION.IN, "F")).isEqualTo(1L);
  }

  @Test
  @Timeout(120)
  void aLightweightTypeEdgeBetweenTwoSlotsIsInTheDestinationListOnce() {
    final RID a = newVertexInSlot(1);
    final RID b = newVertexInSlot(2);

    final Edge edge = createEdge(a, "Light", b, false);

    assertThat(edge).isInstanceOf(LightEdge.class);
    assertThat(database.countType("Light", true)).isZero();
    assertThat(outEdges(a, "Light")).hasSize(1);
    assertThat(inEdges(b, "Light")).hasSize(1);
    assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "Light")).isEqualTo(1L);
    assertThat(b.asVertex().countEdges(Vertex.DIRECTION.IN, "Light")).isEqualTo(1L);
  }

  @Test
  @Timeout(120)
  void aUnidirectionalEdgeBetweenTwoSlotsHasNoIncomingSide() {
    final RID a = newVertexInSlot(0);
    final RID b = newVertexInSlot(1);

    final Edge edge = createEdge(a, "OneWay", b, false);

    assertThat(edge).isNotNull();
    assertThat(outEdges(a, "OneWay")).containsExactly(edge.getIdentity());
    assertThat(inEdges(b, "OneWay")).isEmpty();
    assertThat(b.asVertex().countEdges(Vertex.DIRECTION.IN, "OneWay")).isZero();
  }

  @Test
  @Timeout(120)
  void anEdgeBetweenVerticesOfTheSameSlotIsStillWiredOnce() {
    final RID a = newVertexInSlot(1);
    final RID b = newVertexInSlot(1);

    final Edge edge = createEdge(a, "F", b, false);

    assertWiredOnce(edge, "F", a, b);
  }

  @Test
  @Timeout(120)
  void theCallbackIsCalledOnceWithTheEdgeOnceBothSidesAreWired() {
    final RID a = newVertexInSlot(0);
    final RID b = newVertexInSlot(1);
    final List<Edge> called = new CopyOnWriteArrayList<>();
    final List<Long> incomingSeenByTheCallback = new CopyOnWriteArrayList<>();

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    async.onError((ErrorCallback) failure::set);
    try {
      async.newEdge(a.asVertex(), "F", b, false, (edge, createdSource, createdDestination) -> {
        called.add(edge);
        incomingSeenByTheCallback.add(b.asVertex().countEdges(Vertex.DIRECTION.IN, "F"));
      });
      async.waitCompletion();
    } finally {
      async.onError((ErrorCallback) null);
    }

    assertThat(failure.get()).isNull();
    assertThat(called).hasSize(1);
    assertThat(incomingSeenByTheCallback).containsExactly(1L);
    assertWiredOnce(called.getFirst(), "F", a, b);
  }

  @Test
  @Timeout(120)
  void aNamedDestinationInAnotherSlotGetsTheIncomingEntryAndTheGivenOneDoesNot() {
    final RID a = newVertexInSlot(0);
    final RID given = newVertexInSlot(1);
    final RID named = newVertexInSlot(2);

    final Edge edge = createEdge(a, "F", given, false, "@in", named);

    assertThat(edge.getIn()).isEqualTo(named);
    assertThat(outEdges(a, "F")).containsExactly(edge.getIdentity());
    assertThat(inEdges(named, "F")).containsExactly(edge.getIdentity());
    assertThat(named.asVertex().countEdges(Vertex.DIRECTION.IN, "F")).isEqualTo(1L);
    assertThat(inEdges(given, "F")).isEmpty();
    assertThat(given.asVertex().countEdges(Vertex.DIRECTION.IN, "F")).isZero();
    assertCheckDatabaseClean();
  }

  @Test
  @Timeout(120)
  void aNamedDestinationInTheSlotOfTheGivenOneGetsTheIncomingEntryAndTheGivenOneDoesNot() {
    final RID a = newVertexInSlot(0);
    final RID given = newVertexInSlot(1);
    final RID named = newVertexInSlot(1);

    final Edge edge = createEdge(a, "F", given, false, "@in", named);

    assertThat(inEdges(named, "F")).containsExactly(edge.getIdentity());
    assertThat(named.asVertex().countEdges(Vertex.DIRECTION.IN, "F")).isEqualTo(1L);
    assertThat(inEdges(given, "F")).isEmpty();
    assertCheckDatabaseClean();
  }

  @Test
  @Timeout(120)
  void aNamedDestinationInTheSlotOfTheSourceGetsTheIncomingEntryAndTheGivenOneDoesNot() {
    final RID a = newVertexInSlot(0);
    final RID given = newVertexInSlot(1);
    final RID named = newVertexInSlot(0);

    final Edge edge = createEdge(a, "F", given, false, "@in", named);

    assertThat(outEdges(a, "F")).containsExactly(edge.getIdentity());
    assertThat(inEdges(named, "F")).containsExactly(edge.getIdentity());
    assertThat(named.asVertex().countEdges(Vertex.DIRECTION.IN, "F")).isEqualTo(1L);
    assertThat(inEdges(given, "F")).isEmpty();
    assertCheckDatabaseClean();
  }

  @Test
  @Timeout(120)
  void aNamedDestinationOnAUnidirectionalTypeGetsNoIncomingEntry() {
    final RID a = newVertexInSlot(0);
    final RID given = newVertexInSlot(1);
    final RID named = newVertexInSlot(2);

    final Edge edge = createEdge(a, "OneWay", given, false, "@in", named);

    assertThat(edge.getIn()).isEqualTo(named);
    assertThat(outEdges(a, "OneWay")).containsExactly(edge.getIdentity());
    assertThat(inEdges(given, "OneWay")).isEmpty();
    assertThat(inEdges(named, "OneWay")).isEmpty();
  }

  @Test
  @Timeout(120)
  void newEdgeByKeysBetweenTwoSlotsIsInTheDestinationListOnce() {
    database.transaction(() -> {
      database.getSchema().getType("V").createProperty("id", Integer.class);
      database.getSchema().getType("V").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    });
    final RID a = newVertexInSlot(0, 1);
    final RID b = newVertexInSlot(1, 2);

    final List<Edge> called = new CopyOnWriteArrayList<>();
    async.newEdgeByKeys("V", new String[] { "id" }, new Object[] { 1 }, "V", new String[] { "id" }, new Object[] { 2 }, false, "F",
        true, false, (edge, createdSource, createdDestination) -> called.add(edge));
    async.waitCompletion();

    assertThat(called).hasSize(1);
    assertWiredOnce(called.getFirst(), "F", a, b);
  }

  @Test
  @Timeout(300)
  void everyEdgeOfAManyEdgesLoadIsInTheListsOfItsEndpointsOnce() {
    final int vertices = 60;
    final int edges = 400;
    final List<RID> rids = new ArrayList<>(vertices);
    final List<Bucket> buckets = database.getSchema().getType("V").getBuckets(false);
    database.transaction(() -> {
      for (int i = 0; i < vertices; i++)
        rids.add(database.newVertex("V").save(buckets.get(i % buckets.size()).getName()).getIdentity());
    });

    final Random random = new Random(12602L);
    final Map<RID, Long> expectedOut = new HashMap<>();
    final Map<RID, Long> expectedIn = new HashMap<>();
    int crossSlot = 0;
    for (int i = 0; i < edges; i++) {
      final RID from = rids.get(random.nextInt(vertices));
      final RID to = rids.get(random.nextInt(vertices));
      if (async.getSlot(from.getBucketId()) != async.getSlot(to.getBucketId()))
        ++crossSlot;
      expectedOut.merge(from, 1L, Long::sum);
      expectedIn.merge(to, 1L, Long::sum);
      async.newEdge(from.asVertex(), "F", to, false, null);
    }
    async.waitCompletion();

    assertThat(crossSlot).as("the load must cross slots to mean anything").isGreaterThan(edges / 4);
    for (final RID rid : rids) {
      assertThat(rid.asVertex().countEdges(Vertex.DIRECTION.OUT, "F")).as("OUT of " + rid).isEqualTo(expectedOut.getOrDefault(rid, 0L));
      assertThat(rid.asVertex().countEdges(Vertex.DIRECTION.IN, "F")).as("IN of " + rid).isEqualTo(expectedIn.getOrDefault(rid, 0L));
      assertThat(inEdges(rid, "F")).as("IN edges of " + rid).hasSize(expectedIn.getOrDefault(rid, 0L).intValue()).doesNotHaveDuplicates();
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM F")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo((long) edges);
    }
  }

  private void assertWiredOnce(final Edge edge, final String type, final RID from, final RID to) {
    assertThat(edge).isNotNull();
    assertThat(edge.getOut()).isEqualTo(from);
    assertThat(edge.getIn()).isEqualTo(to);
    assertThat(from.asVertex().countEdges(Vertex.DIRECTION.OUT, type)).isEqualTo(1L);
    assertThat(to.asVertex().countEdges(Vertex.DIRECTION.IN, type)).isEqualTo(1L);
    assertThat(outEdges(from, type)).containsExactly(edge.getIdentity());
    assertThat(inEdges(to, type)).containsExactly(edge.getIdentity());
    assertThat(from.asVertex().countEdges(Vertex.DIRECTION.IN, type)).isZero();
    assertThat(to.asVertex().countEdges(Vertex.DIRECTION.OUT, type)).isZero();
    assertCheckDatabaseClean();
  }

  private void assertCheckDatabaseClean() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result check = rs.next();
      assertThat(check.<Long>getProperty("totalWarnings")).isZero();
      assertThat(check.<Long>getProperty("totalErrors")).isZero();
    }
  }

  /** Creates one edge through the async executor, waits for it and answers the edge the callback was given. */
  private Edge createEdge(final RID from, final String type, final RID to, final boolean light, final Object... properties) {
    final AtomicReference<Edge> created = new AtomicReference<>();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    async.onError((ErrorCallback) failure::set);
    try {
      async.newEdge(from.asVertex(), type, to, light, (edge, createdSource, createdDestination) -> created.set(edge), properties);
      async.waitCompletion();
    } finally {
      async.onError((ErrorCallback) null);
    }
    // AN ASYNC FAILURE DOES NOT REACH THE CALLER: SURFACE IT HERE, OR THE TEST WOULD ONLY SEE A MISSING EDGE
    assertThat(failure.get()).isNull();
    assertThat(created.get()).isNotNull();
    return created.get();
  }

  private static List<RID> edgeRids(final Iterable<Edge> edges) {
    final List<RID> list = new ArrayList<>();
    for (final Edge edge : edges)
      list.add(edge.getIdentity());
    return list;
  }

  private List<RID> outEdges(final RID vertex, final String type) {
    return edgeRids(vertex.asVertex().getEdges(Vertex.DIRECTION.OUT, type));
  }

  private List<RID> inEdges(final RID vertex, final String type) {
    return edgeRids(vertex.asVertex().getEdges(Vertex.DIRECTION.IN, type));
  }

  private RID newVertexInSlot(final int slot) {
    return newVertexInSlot(slot, null);
  }

  /** Persists a vertex in a bucket whose async slot is {@code slot}: two buckets of the type that share one are the same worker. */
  private RID newVertexInSlot(final int slot, final Integer id) {
    for (final Bucket bucket : database.getSchema().getType("V").getBuckets(false))
      if (async.getSlot(bucket.getFileId()) == slot) {
        final RID[] created = new RID[1];
        database.transaction(() -> {
          final MutableVertex vertex = database.newVertex("V");
          if (id != null)
            vertex.set("id", id);
          created[0] = vertex.save(bucket.getName()).getIdentity();
        });
        assertThat(async.getSlot(created[0].getBucketId())).isEqualTo(slot);
        return created[0];
      }
    throw new AssertionError("no bucket of V is served by the async slot " + slot + " of " + SLOTS);
  }
}
