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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8417: a type-filtered walk over a vertex whose edge list is dominated by ANOTHER edge type ("a hub for one
 * type, a handful of another") must reject the foreign entries on the raw bucket number stored in the segment, without
 * decoding each one into a pair of {@link RID} objects first.
 * <p>
 * The counting tests drive the filtered iterators over a segment proxy that counts {@link EdgeSegment#getRID} calls:
 * before the fix every entry of the list cost two decodes whatever its type, so a list of {@code HUB} foreign entries
 * plus one match cost {@code 2 * (HUB + 1)}; after it only the match is decoded.
 */
class Issue8417TypeFilteredScanTest {
  private static final String DB_PATH = "./target/databases/issue8417TypeFilteredScan";
  private static final int    HUB     = 500;

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> {
      database.getSchema().createVertexType("Unit");
      database.getSchema().createVertexType("Case");
      database.getSchema().createEdgeType("Case_unit");
      database.getSchema().createEdgeType("Parent");
      database.getSchema().createEdgeType("SubParent").addSuperType("Parent");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void filteredRidWalkDecodesOnlyTheMatchingEntry() {
    final RID[] ids = buildHub(HUB, false);
    database.transaction(() -> {
      final AtomicInteger decodes = new AtomicInteger();
      final EdgeSegment head = countingHead(ids[0], decodes);

      final List<RID> found = new ArrayList<>();
      new RIDIteratorFilter((DatabaseInternal) database, head, new String[] { "Parent" }).forEachRemaining(found::add);

      assertThat(found).containsExactly(ids[1]);
      // EDGE + VERTEX OF THE ONE MATCH. BEFORE #8417 THIS WAS 2 * (HUB + 1)
      assertThat(decodes.get()).isEqualTo(2);
    });
  }

  @Test
  void filteredVertexAndEdgeWalksDecodeOnlyTheMatchingEntry() {
    final RID[] ids = buildHub(HUB, false);
    database.transaction(() -> {
      final AtomicInteger decodes = new AtomicInteger();

      final List<RID> vertices = new ArrayList<>();
      new VertexIteratorFilter((DatabaseInternal) database, countingHead(ids[0], decodes), new String[] { "Parent" })
          .forEachRemaining(v -> vertices.add(v.getIdentity()));
      assertThat(vertices).containsExactly(ids[1]);
      assertThat(decodes.get()).isEqualTo(2);

      decodes.set(0);
      final Vertex unit = ids[0].asVertex();
      final List<RID> edges = new ArrayList<>();
      new EdgeIteratorFilter((DatabaseInternal) database, unit, Vertex.DIRECTION.IN, countingHead(ids[0], decodes),
          new String[] { "Parent" }).forEachRemaining(e -> edges.add(e.getIdentity()));
      assertThat(edges).containsExactly(ids[2]);
      assertThat(decodes.get()).isEqualTo(2);

      decodes.set(0);
      final List<Pair<RID, RID>> entries = new ArrayList<>();
      new EdgeVertexIteratorFilter((DatabaseInternal) database, countingHead(ids[0], decodes), new String[] { "Parent" })
          .forEachRemaining(entries::add);
      assertThat(entries).hasSize(1);
      assertThat(entries.getFirst().getFirst()).isEqualTo(ids[2]);
      assertThat(entries.getFirst().getSecond()).isEqualTo(ids[1]);
      assertThat(decodes.get()).isEqualTo(2);
    });
  }

  @Test
  void publicApiReturnsExactlyTheRequestedTypeAcrossChunks() {
    final RID[] ids = buildHub(HUB, false);
    database.transaction(() -> {
      final Vertex unit = ids[0].asVertex();

      assertThat(ridsOf(unit.getVertices(Vertex.DIRECTION.IN, "Parent").iterator())).containsExactly(ids[1]);
      assertThat(ridsOf(unit.getEdges(Vertex.DIRECTION.IN, "Parent").iterator())).containsExactly(ids[2]);
      assertThat(unit.countEdges(Vertex.DIRECTION.IN, "Parent")).isEqualTo(1);
      // THE DOMINANT TYPE STILL COMES BACK WHOLE: THE SKIP MUST NOT LOSE MATCHES WHEN ALMOST EVERYTHING MATCHES
      assertThat(ridsOf(unit.getVertices(Vertex.DIRECTION.IN, "Case_unit").iterator())).hasSize(HUB);
      assertThat(ridsOf(unit.getEdges(Vertex.DIRECTION.IN, "Case_unit", "Parent").iterator())).hasSize(HUB + 1);

      try (final ResultSet rs = database.query("sql", "SELECT expand(in('Parent')) FROM " + ids[0])) {
        assertThat(rs.next().getIdentity().get()).isEqualTo(ids[1]);
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  @Test
  void polymorphicTypeMatchesItsSubtypeBuckets() {
    final RID[] ids = buildHub(HUB, true);
    database.transaction(() -> {
      final Vertex unit = ids[0].asVertex();
      // ids[3] IS A SubParent EDGE: ASKING FOR "Parent" MUST STILL FIND IT, ASKING FOR "SubParent" ONLY IT
      assertThat(ridsOf(unit.getEdges(Vertex.DIRECTION.IN, "Parent").iterator())).containsExactlyInAnyOrder(ids[2], ids[3]);
      assertThat(ridsOf(unit.getEdges(Vertex.DIRECTION.IN, "SubParent").iterator())).containsExactly(ids[3]);
    });
  }

  @Test
  void lightweightEdgesAreFilteredOnTheirTypeBucket() {
    final RID[] unit = new RID[1];
    final RID[] parent = new RID[1];
    database.transaction(() -> {
      final MutableVertex u = database.newVertex("Unit").save();
      unit[0] = u.getIdentity();
      for (int i = 0; i < HUB; i++)
        database.newVertex("Case").save().newLightEdge("Case_unit", u);
      final MutableVertex p = database.newVertex("Unit").save();
      parent[0] = p.getIdentity();
      p.newLightEdge("Parent", u);
      for (int i = 0; i < 10; i++)
        database.newVertex("Case").save().newLightEdge("Case_unit", u);
    });
    database.transaction(() -> {
      final Vertex u = unit[0].asVertex();
      assertThat(ridsOf(u.getVertices(Vertex.DIRECTION.IN, "Parent").iterator())).containsExactly(parent[0]);
      final List<Edge> edges = new ArrayList<>();
      u.getEdges(Vertex.DIRECTION.IN, "Parent").forEach(edges::add);
      assertThat(edges).hasSize(1);
      assertThat(edges.getFirst().getOut()).isEqualTo(parent[0]);
    });
  }

  @Test
  void neighbourFilterStillAppliesAfterTheBucketSkip() {
    final RID[] ids = buildHub(HUB, false);
    database.transaction(() -> {
      final Vertex unit = ids[0].asVertex();
      final GraphEngine engine = ((DatabaseInternal) database).getGraphEngine();
      assertThat(ridsOf(engine.getEdgesConnectedTo((VertexInternal) unit, Vertex.DIRECTION.IN, ids[1], "Parent")))
          .containsExactly(ids[2]);
      // A Case_unit NEIGHBOUR ASKED FOR WITH THE Parent FILTER: THE BUCKET SKIP DROPS IT, NOTHING COMES BACK
      final RID caseVertex = unit.getVertices(Vertex.DIRECTION.IN, "Case_unit").iterator().next().getIdentity();
      assertThat(ridsOf(engine.getEdgesConnectedTo((VertexInternal) unit, Vertex.DIRECTION.IN, caseVertex, "Parent")))
          .isEmpty();
    });
  }

  @Test
  void removeThroughTheFilteredIteratorRemovesTheMatchedEntry() {
    final RID[] ids = buildHub(HUB, false);
    database.transaction(() -> {
      final RID headRID = ((VertexInternal) ids[0].asVertex()).getInEdgesHeadChunk();
      final VertexIteratorFilter it = new VertexIteratorFilter((DatabaseInternal) database,
          (EdgeSegment) database.lookupByRID(headRID, true), new String[] { "Parent" });
      // remove() unlinks the entry the iterator returned last, which the skip must keep pointing at
      assertThat(it.next().getIdentity()).isEqualTo(ids[1]);
      it.remove();
      assertThat(it.hasNext()).isFalse();
    });
    database.transaction(() -> {
      final Vertex unit = ids[0].asVertex();
      assertThat(unit.countEdges(Vertex.DIRECTION.IN, "Parent")).isZero();
      assertThat(unit.countEdges(Vertex.DIRECTION.IN, "Case_unit")).isEqualTo(HUB);
    });
  }

  @Test
  void bucketMaskMatchesExactlyTheRequestedBucketsAcrossAGap() {
    database.transaction(() -> {
      // DOCUMENT TYPES CREATED BETWEEN Parent AND ITS LATE SUBTYPE OPEN A GAP OF FOREIGN BUCKETS INSIDE THE MASK'S RANGE
      for (int i = 0; i < 5; i++)
        database.getSchema().createDocumentType("Filler" + i);
      database.getSchema().createEdgeType("LateParent").addSuperType("Parent");
    });

    final DatabaseInternal db = (DatabaseInternal) database;
    final EdgeBucketMask mask = EdgeBucketMask.of(db, new String[] { "Parent" });
    final List<Integer> accepted = database.getSchema().getType("Parent").getBucketIds(true);
    for (final Integer bucketId : accepted)
      assertThat(mask.matches(bucketId)).isTrue();
    for (int i = 0; i < 5; i++)
      for (final Integer bucketId : database.getSchema().getType("Filler" + i).getBucketIds(false))
        assertThat(mask.matches(bucketId)).isFalse();
    for (final Integer bucketId : database.getSchema().getType("Case_unit").getBucketIds(false))
      assertThat(mask.matches(bucketId)).isFalse();
    assertThat(mask.matches(-1)).isFalse();
    assertThat(mask.matches(Integer.MAX_VALUE)).isFalse();
    assertThat(mask.matches(Long.MAX_VALUE)).isFalse();

    // NAMES THAT ARE NOT EDGE TYPES MATCH NOTHING (#5194), AND ALONE THEY LEAVE NO MASK AT ALL
    assertThat(EdgeBucketMask.of(db, new String[] { "Unit", "NoSuchType" })).isNull();
    final EdgeBucketMask mixed = EdgeBucketMask.of(db, new String[] { "Unit", "Case_unit" });
    for (final Integer bucketId : database.getSchema().getType("Unit").getBucketIds(false))
      assertThat(mixed.matches(bucketId)).isFalse();
  }

  /**
   * Builds the shape from the issue: one Unit with {@code hub} incoming Case_unit edges, one incoming Parent edge in the
   * middle of the list, and optionally one SubParent edge. Returns [unit, parentVertex, parentEdge, subParentEdge].
   */
  private RID[] buildHub(final int hub, final boolean withSubParent) {
    final RID[] ids = new RID[4];
    database.transaction(() -> {
      final MutableVertex unit = database.newVertex("Unit").set("name", "local").save();
      ids[0] = unit.getIdentity();
      for (int i = 0; i < hub / 2; i++)
        database.newVertex("Case").save().newEdge("Case_unit", unit);
      final MutableVertex region = database.newVertex("Unit").set("name", "region").save();
      ids[1] = region.getIdentity();
      ids[2] = region.newEdge("Parent", unit).getIdentity();
      if (withSubParent)
        ids[3] = database.newVertex("Unit").save().newEdge("SubParent", unit).getIdentity();
      for (int i = hub / 2; i < hub; i++)
        database.newVertex("Case").save().newEdge("Case_unit", unit);
    });
    return ids;
  }

  /** The head chunk of the vertex's IN list, wrapped so every {@link EdgeSegment#getRID} call (on any chunk) is counted. */
  private EdgeSegment countingHead(final RID vertex, final AtomicInteger decodes) {
    final RID headRID = ((VertexInternal) vertex.asVertex()).getInEdgesHeadChunk();
    return counting((EdgeSegment) database.lookupByRID(headRID, true), decodes);
  }

  private static EdgeSegment counting(final EdgeSegment delegate, final AtomicInteger decodes) {
    return (EdgeSegment) Proxy.newProxyInstance(EdgeSegment.class.getClassLoader(), new Class<?>[] { EdgeSegment.class },
        (proxy, method, args) -> {
          if (method.getName().equals("getRID"))
            decodes.incrementAndGet();
          try {
            final Object result = method.invoke(delegate, args);
            if (method.getName().equals("getPrevious") && result != null)
              return counting((EdgeSegment) result, decodes);
            return result;
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  private static List<RID> ridsOf(final Iterator<? extends Identifiable> it) {
    final List<RID> rids = new ArrayList<>();
    while (it.hasNext())
      rids.add(it.next().getIdentity());
    return rids;
  }
}
