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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests of the EdgeChunkChain area, one nested class per former stand-alone regression class.
 */
class EdgeChunkChainTest {
  /**
   * Regression test for issue #5771: {@code reload()} on an {@link ImmutableVertex} kept the edge pointers it had parsed
   * out of the previous buffer, so a vertex reloaded to pick up another transaction's changes went on reporting its
   * pre-reload edges.
   * <p>
   * The pointers are re-derived from the buffer by {@code parseRecordPrefix()}, which
   * {@link com.arcadedb.database.BaseDocument#reload()} calls on the buffer it has just installed. The test holds the
   * immutable instance across the transaction that adds the edge, which is the only shape that meets the defect: an edge
   * added through a freshly looked-up vertex never sees a stale parse.
   */
  @Nested
  class VertexReloadEdgePointer extends BaseGraphTest {

    @Test
    void reloadRefreshesOutEdgesHeadChunk() {
      final RID[] rids = createTwoUnconnectedVertices();
      final RID sourceRID = rids[0];
      final RID targetRID = rids[1];

      // MATERIALISE THE IMMUTABLE VERTEX WHILE IT STILL HAS NO EDGES: THIS IS WHAT PARSES THE PREFIX THE FIX HAS TO REDO
      database.begin();
      final ImmutableVertex source = (ImmutableVertex) database.lookupByRID(sourceRID, true);
      assertThat(source.getOutEdgesHeadChunk()).isNull();
      database.commit();

      database.transaction(() -> database.lookupByRID(sourceRID, true).asVertex()
          .newEdge(EDGE1_TYPE_NAME, database.lookupByRID(targetRID, true), "name", "reloaded"));

      database.begin();
      try {
        source.reload();

        assertThat(source.getOutEdgesHeadChunk()).as("reload() must re-read the out-edge head chunk from the fresh buffer")
            .isEqualTo(((VertexInternal) database.lookupByRID(sourceRID, true)).getOutEdgesHeadChunk());
        assertThat(source.countEdges(Vertex.DIRECTION.OUT, EDGE1_TYPE_NAME)).isEqualTo(1L);
      } finally {
        database.commit();
      }
    }

    @Test
    void reloadRefreshesInEdgesHeadChunk() {
      final RID[] rids = createTwoUnconnectedVertices();
      final RID sourceRID = rids[0];
      final RID targetRID = rids[1];

      database.begin();
      final ImmutableVertex target = (ImmutableVertex) database.lookupByRID(targetRID, true);
      assertThat(target.getInEdgesHeadChunk()).isNull();
      database.commit();

      database.transaction(() -> database.lookupByRID(sourceRID, true).asVertex()
          .newEdge(EDGE1_TYPE_NAME, database.lookupByRID(targetRID, true), "name", "reloaded"));

      database.begin();
      try {
        target.reload();

        assertThat(target.getInEdgesHeadChunk()).as("reload() must re-read the in-edge head chunk from the fresh buffer")
            .isEqualTo(((VertexInternal) database.lookupByRID(targetRID, true)).getInEdgesHeadChunk());
        assertThat(target.countEdges(Vertex.DIRECTION.IN, EDGE1_TYPE_NAME)).isEqualTo(1L);
      } finally {
        database.commit();
      }
    }

    /**
     * The properties have to survive the re-parse: the prefix is read again to find where they start, and getting that
     * wrong reads the properties from the middle of the edge pointers instead.
     */
    @Test
    void reloadKeepsReadingTheVertexProperties() {
      final RID[] rids = createTwoUnconnectedVertices();
      final RID sourceRID = rids[0];
      final RID targetRID = rids[1];

      database.begin();
      final ImmutableVertex source = (ImmutableVertex) database.lookupByRID(sourceRID, true);
      assertThat(source.getString("name")).isEqualTo("source");
      database.commit();

      database.transaction(() -> {
        database.lookupByRID(sourceRID, true).asVertex().newEdge(EDGE1_TYPE_NAME, database.lookupByRID(targetRID, true));
        database.lookupByRID(sourceRID, true).asVertex().modify().set("name", "renamed").save();
      });

      database.begin();
      try {
        source.reload();

        assertThat(source.getString("name")).isEqualTo("renamed");
        assertThat(source.getPropertyNames()).contains("name");
        assertThat(source.getOutEdgesHeadChunk()).isNotNull();
      } finally {
        database.commit();
      }
    }

    /**
     * A vertex that never gains an edge must come back from a reload with both pointers still null, and must not blow up
     * on the re-parse of a prefix whose RIDs are the {@code -1} markers.
     */
    @Test
    void reloadOfAVertexWithoutEdgesKeepsBothPointersNull() {
      final RID[] rids = createTwoUnconnectedVertices();
      final RID isolatedRID = rids[0];

      database.begin();
      final ImmutableVertex isolated = (ImmutableVertex) database.lookupByRID(isolatedRID, true);
      assertThat(isolated.getOutEdgesHeadChunk()).isNull();
      assertThat(isolated.getInEdgesHeadChunk()).isNull();
      database.commit();

      database.begin();
      try {
        isolated.reload();

        assertThat(isolated.getOutEdgesHeadChunk()).isNull();
        assertThat(isolated.getInEdgesHeadChunk()).isNull();
        assertThat(isolated.getString("name")).isEqualTo("source");
      } finally {
        database.commit();
      }
    }

    /**
     * The edge shape of the same hook: {@link ImmutableEdge} re-derives its out/in RIDs and its properties offset from
     * the reloaded buffer rather than from the fields the previous buffer left behind.
     */
    @Test
    void reloadOfAnEdgeKeepsItsEndpointsAndSeesTheNewProperty() {
      final RID[] rids = createTwoUnconnectedVertices();
      final RID sourceRID = rids[0];
      final RID targetRID = rids[1];

      final RID[] edgeRID = new RID[1];
      database.transaction(() -> edgeRID[0] = database.lookupByRID(sourceRID, true).asVertex()
          .newEdge(EDGE1_TYPE_NAME, database.lookupByRID(targetRID, true), "name", "before").getIdentity());

      database.begin();
      final ImmutableEdge edge = (ImmutableEdge) database.lookupByRID(edgeRID[0], true);
      assertThat(edge.getString("name")).isEqualTo("before");
      database.commit();

      database.transaction(() -> database.lookupByRID(edgeRID[0], true).asEdge().modify().set("name", "after").save());

      database.begin();
      try {
        edge.reload();

        assertThat(edge.getOut()).isEqualTo(sourceRID);
        assertThat(edge.getIn()).isEqualTo(targetRID);
        assertThat(edge.getString("name")).isEqualTo("after");
      } finally {
        database.commit();
      }
    }

    /**
     * Creates a source and a target vertex with no edge between them. {@code BaseGraphTest} wires its own {@code root}
     * vertex up with edges in the fixture, so a test about "the pointers were null when the vertex was parsed" needs
     * vertices of its own.
     *
     * @return the source RID at index 0 and the target RID at index 1
     */
    private RID[] createTwoUnconnectedVertices() {
      final RID[] rids = new RID[2];
      database.transaction(() -> {
        final MutableVertex source = database.newVertex(VERTEX1_TYPE_NAME);
        source.set("name", "source");
        source.save();

        final MutableVertex target = database.newVertex(VERTEX2_TYPE_NAME);
        target.set("name", "target");
        target.save();

        rids[0] = source.getIdentity();
        rids[1] = target.getIdentity();
      });
      return rids;
    }
  }

  /**
   * Regression test for issue #6278.
   * <p>
   * {@code EdgeVertexIterator.hasNext()} gained a guard against a chunk whose {@code previous} pointer names itself
   * in #6276 (issue #6062), because that iterator backs {@code EdgeLinkedList.entryIterator()}, which is what
   * {@code CHECK DATABASE}'s adjacency probe cache walks a vertex's own edge list through. The guard was added only
   * there - every other chunk-hopping iterator behind an ordinary graph traversal ({@link EdgeIterator},
   * {@link VertexIterator}, {@link RIDIterator}, and the {@code *IteratorFilter} family built on
   * {@link IteratorFilterBase}) still lacked it, so the same corruption that #6276 made {@code CHECK DATABASE}
   * survive still hung {@code Vertex.getEdges()}, {@code getVertices()} and {@code GraphEngine.getConnectedVertexRIDs()}
   * in an ordinary request thread.
   * <p>
   * The fix lifts the guard into {@code ResettableIteratorBase.moveToPreviousChunk()}, the one chunk-hop every one of
   * these iterators now goes through. Each test here plants a self-referencing head chunk - the same
   * {@code setPrevious(itself)} trick as {@code EdgeStripeDirectoryTest.AdjacencyProbeCache
   * .aSelfReferencingChunkEndsTheWalkInsteadOfFeedingItForever} - and walks one iterator family under a HARD CAP
   * rather than a timeout: an unterminating walk does not fail a test, it hangs the build, and a regression has to
   * come back as a red test, not a red build.
   */
  @Nested
  class SelfReferencingChunkTraversalIterators extends TestHelper {
    private static final String VERTEX_TYPE = "Issue6278Node";
    private static final String EDGE_TYPE   = "Issue6278Link";
    private static final int    DEGREE      = 50;
    private static final int    CAP         = 8 * DEGREE;

    /**
     * Every test here deliberately plants a self-referencing chunk - that is the corruption under measurement - so
     * the blanket teardown check would only re-assert the same database with the opposite expectation.
     */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType(VERTEX_TYPE);
        database.getSchema().buildEdgeType().withName(EDGE_TYPE).withBidirectional(true).create();
      });
    }

    @Test
    void edgeIteratorEndsTheWalkInsteadOfFeedingItForever() {
      final RID hub = createHubWithDistinctSpokesAndSelfLoopHead();

      final int[] walked = new int[1];
      database.transaction(() -> {
        final Iterator<Edge> edges = edgeLinkedListFor(hub).edgeIterator();
        while (walked[0] <= CAP && edges.hasNext()) {
          edges.next();
          ++walked[0];
        }
      });

      assertThat(walked[0])
          .as("EdgeIterator must stop on a self-referencing chunk; without the guard it yields the chunk's entries forever")
          .isLessThanOrEqualTo(DEGREE);
    }

    @Test
    void vertexIteratorEndsTheWalkInsteadOfFeedingItForever() {
      final RID hub = createHubWithDistinctSpokesAndSelfLoopHead();

      final int[] walked = new int[1];
      database.transaction(() -> {
        final Iterator<Vertex> vertices = edgeLinkedListFor(hub).vertexIterator();
        while (walked[0] <= CAP && vertices.hasNext()) {
          vertices.next();
          ++walked[0];
        }
      });

      assertThat(walked[0])
          .as("VertexIterator must stop on a self-referencing chunk; without the guard it yields the chunk's entries forever")
          .isLessThanOrEqualTo(DEGREE);
    }

    @Test
    void ridIteratorEndsTheWalkInsteadOfFeedingItForever() {
      final RID hub = createHubWithDistinctSpokesAndSelfLoopHead();

      final int[] walked = new int[1];
      database.transaction(() -> {
        final Iterator<RID> rids = edgeLinkedListFor(hub).ridIterator();
        while (walked[0] <= CAP && rids.hasNext()) {
          rids.next();
          ++walked[0];
        }
      });

      assertThat(walked[0])
          .as("RIDIterator must stop on a self-referencing chunk; without the guard it yields the chunk's entries forever")
          .isLessThanOrEqualTo(DEGREE);
    }

    @Test
    void edgeIteratorFilterEndsTheWalkInsteadOfFeedingItForever() {
      final RID hub = createHubWithDistinctSpokesAndSelfLoopHead();

      final int[] walked = new int[1];
      database.transaction(() -> {
        // Passing an edge type routes EdgeLinkedList.edgeIterator() to EdgeIteratorFilter, which is what exercises
        // the shared chunk-hop through IteratorFilterBase rather than through EdgeIterator directly.
        final Iterator<Edge> edges = edgeLinkedListFor(hub).edgeIterator(EDGE_TYPE);
        while (walked[0] <= CAP && edges.hasNext()) {
          edges.next();
          ++walked[0];
        }
      });

      assertThat(walked[0])
          .as(
              "EdgeIteratorFilter (IteratorFilterBase) must stop on a self-referencing chunk; without the guard it yields the chunk's entries forever")
          .isLessThanOrEqualTo(DEGREE);
    }

    /** One hub with {@code DEGREE} distinct spokes, its IN head chunk pointing at itself. */
    private RID createHubWithDistinctSpokesAndSelfLoopHead() {
      final RID[] hub = new RID[1];
      database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());
      database.transaction(() -> {
        final MutableVertex target = hub[0].asVertex(true).modify();
        for (int i = 0; i < DEGREE; i++)
          database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, target);
      });

      // Point the hub's IN head chunk at itself.
      database.transaction(() -> {
        final RID head = ((VertexInternal) hub[0].asVertex(true)).getInEdgesHeadChunk();
        final MutableEdgeSegment segment = (MutableEdgeSegment) database.lookupByRID(head, true);
        segment.setPrevious(segment);
        ((DatabaseInternal) database).updateRecord(segment);
      });

      return hub[0];
    }

    private EdgeLinkedList edgeLinkedListFor(final RID hub) {
      return ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk((VertexInternal) hub.asVertex(true), Vertex.DIRECTION.IN);
    }
  }

  /**
   * An edge list must render every entry it holds, and an edge segment must honour {@code includeMetadata} (issue #8554).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EdgeSegmentToJson extends TestHelper {

    @Test
    void edgeListSerializesItsEntries() {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");

      final Set<String> expectedEdges = new HashSet<>();
      final RID[] source = new RID[1];
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("V").save();
        source[0] = a.getIdentity();
        for (int i = 0; i < 3; i++) {
          final MutableVertex b = database.newVertex("V").save();
          expectedEdges.add(a.newEdge("E", b).getIdentity().toString());
        }
      });

      final VertexInternal vertex = (VertexInternal) database.lookupByRID(source[0], true);
      final EdgeLinkedList out = ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk(vertex, Vertex.DIRECTION.OUT);
      assertThat(out.count()).isEqualTo(3);

      final JSONArray json = out.toJSON();
      assertThat(json.length()).isEqualTo(3);
      final Set<String> edges = new HashSet<>();
      for (int i = 0; i < json.length(); i++) {
        final JSONObject entry = json.getJSONObject(i);
        edges.add(entry.getString("edge"));
        assertThat(entry.getString("vertex")).startsWith("#");
      }
      assertThat(edges).isEqualTo(expectedEdges);
    }

    @Test
    void segmentHonoursIncludeMetadata() {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
      final RID[] source = new RID[1];
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("V").save();
        source[0] = a.getIdentity();
        a.newEdge("E", database.newVertex("V").save());
      });

      final VertexInternal vertex = (VertexInternal) database.lookupByRID(source[0], true);
      final EdgeSegment segment = (EdgeSegment) database.lookupByRID(vertex.getOutEdgesHeadChunk(), true);
      assertThat(segment.toJSON(false).has("@rid")).isFalse();
      assertThat(segment.toJSON(true).has("@rid")).isTrue();
      assertThat(segment.toJSON(false).getJSONArray("entries").length()).isEqualTo(1);
    }
  }

  /**
   * Regression test for issue #8568: {@code EdgeLinkedList.count()} walked the chunk chain with no guard against a chunk
   * whose previous pointer names itself, so on that corruption - which every other walker survives - a degree query
   * never returned. Every walk of the class now hops through one guarded helper; {@code toJSON()}, the removal walks and
   * {@code RIDIteratorFilter} were unguarded too.
   * <p>
   * A regression must come back as a red test rather than a hung build, so each walk runs on a daemon thread and the
   * test waits for it with a bound far above what the walk needs: it is a hang detector, not a latency bound.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EdgeListCountSelfLoop extends TestHelper {
    private static final String VERTEX_TYPE = "Issue8568Node";
    private static final String EDGE_TYPE   = "Issue8568Link";
    private static final int    DEGREE      = 50;
    private static final int    CAP         = 8 * DEGREE;

    /**
     * Every test plants a self-referencing chunk on purpose, so the teardown integrity check would only re-assert it.
     */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType(VERTEX_TYPE);
        database.getSchema().buildEdgeType().withName(EDGE_TYPE).withBidirectional(true).create();
      });
    }

    @Test
    void countEndsOnASelfReferencingChunk() throws Exception {
      final RID hub = createHubWithSelfLoopHead();

      final Long total = runBounded(() -> inTx(() -> edgeLinkedListFor(hub).count()));
      assertThat(total).isBetween(1L, (long) DEGREE);

      final Long filtered = runBounded(() -> inTx(() -> edgeLinkedListFor(hub).count(EDGE_TYPE)));
      assertThat(filtered).isEqualTo(total);

      // The query-facing path: the vertex degree
      final Long degree = runBounded(() -> inTx(() -> hub.asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE)));
      assertThat(degree).isEqualTo(total);
    }

    @Test
    void toJSONEndsOnASelfReferencingChunk() throws Exception {
      final RID hub = createHubWithSelfLoopHead();

      final JSONArray json = runBounded(() -> inTx(() -> edgeLinkedListFor(hub).toJSON()));
      // Its content is a separate defect (the entries are not serialized); what matters here is that the walk returns
      assertThat(json.length()).isLessThanOrEqualTo(DEGREE);
    }

    @Test
    void ridIteratorFilterEndsOnASelfReferencingChunk() throws Exception {
      final RID hub = createHubWithSelfLoopHead();

      final Integer walked = runBounded(() -> inTx(() -> {
        // Passing an edge type routes ridIterator() to RIDIteratorFilter, which hopped chunks without the guard
        final Iterator<RID> rids = edgeLinkedListFor(hub).ridIterator(EDGE_TYPE);
        int count = 0;
        while (count <= CAP && rids.hasNext()) {
          rids.next();
          ++count;
        }
        return count;
      }));
      assertThat(walked).isLessThanOrEqualTo(DEGREE);
    }

    @Test
    void removalWalksEndOnASelfReferencingChunk() throws Exception {
      final RID hub = createHubWithSelfLoopHead();
      final RID stranger = inTx(() -> database.newVertex(VERTEX_TYPE).save().getIdentity());

      // A vertex and an edge the list does not hold: the removal walks visit every chunk and must still terminate
      runBounded(() -> {
        database.transaction(() -> edgeLinkedListFor(hub).removeVertex(stranger));
        return null;
      });
      runBounded(() -> {
        database.transaction(() -> edgeLinkedListFor(hub).removeEdgeRID(new RID(0, 0)));
        return null;
      });
      final Boolean contains = runBounded(() -> inTx(() -> edgeLinkedListFor(hub).containsVertex(stranger, null)));
      assertThat(contains).isFalse();
    }

    /**
     * A self-referencing chunk that is NOT the head and that a removal empties must not be deleted and relinked around:
     * the chunk in front of it would be left pointing at the deleted record.
     */
    @Test
    void emptiedSelfReferencingChunkIsNotDeletedAndRelinkedAround() throws Exception {
      final RID[] hub = new RID[1];
      final List<RID> spokes = new ArrayList<>();
      database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());
      database.transaction(() -> {
        final MutableVertex target = hub[0].asVertex(true).modify();
        for (int i = 0; i < DEGREE; i++) {
          final MutableVertex spoke = database.newVertex(VERTEX_TYPE).set("i", i).save();
          spoke.newEdge(EDGE_TYPE, target);
          spokes.add(spoke.getIdentity());
        }
      });

      // Point the chunk BEHIND the head at itself
      final RID second = inTx(() -> {
        final RID head = ((VertexInternal) hub[0].asVertex(true)).getInEdgesHeadChunk();
        final RID behind = ((EdgeSegment) database.lookupByRID(head, true)).getPreviousRID();
        final MutableEdgeSegment segment = (MutableEdgeSegment) database.lookupByRID(behind, true);
        segment.setPrevious(segment);
        ((DatabaseInternal) database).updateRecord(segment);
        return behind;
      });
      assertThat(second).isNotNull();

      runBounded(() -> {
        database.transaction(() -> {
          for (final RID spoke : spokes)
            edgeLinkedListFor(hub[0]).removeVertex(spoke);
        });
        return null;
      });

      database.transaction(() -> {
        final RID head = ((VertexInternal) hub[0].asVertex(true)).getInEdgesHeadChunk();
        final RID behind = ((EdgeSegment) database.lookupByRID(head, true)).getPreviousRID();
        assertThat(behind).isEqualTo(second);
        assertThat(database.existsRecord(behind)).as("the head must not point at a deleted chunk").isTrue();
      });
    }

    private <T> T inTx(final Supplier<T> read) {
      final List<T> result = new ArrayList<>(1);
      database.transaction(() -> result.add(read.get()));
      return result.getFirst();
    }

    private <T> T runBounded(final Callable<T> walk) throws Exception {
      final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        final Thread thread = new Thread(r, "Issue8568-walk");
        thread.setDaemon(true);
        return thread;
      });
      try {
        final Future<T> future = executor.submit(walk);
        // Hang detector, not a latency bound: the walk covers a single chunk
        return future.get(60, TimeUnit.SECONDS);
      } finally {
        executor.shutdownNow();
      }
    }

    /**
     * One hub with {@code DEGREE} spokes, its IN head chunk pointing at itself.
     */
    private RID createHubWithSelfLoopHead() {
      final RID[] hub = new RID[1];
      database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());
      database.transaction(() -> {
        final MutableVertex target = hub[0].asVertex(true).modify();
        for (int i = 0; i < DEGREE; i++)
          database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, target);
      });

      database.transaction(() -> {
        final RID head = ((VertexInternal) hub[0].asVertex(true)).getInEdgesHeadChunk();
        final MutableEdgeSegment segment = (MutableEdgeSegment) database.lookupByRID(head, true);
        segment.setPrevious(segment);
        ((DatabaseInternal) database).updateRecord(segment);
      });

      return hub[0];
    }

    private EdgeLinkedList edgeLinkedListFor(final RID hub) {
      return ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk((VertexInternal) hub.asVertex(true), Vertex.DIRECTION.IN);
    }
  }

  /**
   * Issue #8713: the chunk-chain walks stopped only on a chunk pointing at ITSELF, so a two-chunk cycle still hung
   * {@code count()}, {@code toJSON()}, the removal walks and the iterators. Each walk runs on a daemon thread so a
   * regression is a red test rather than a hung build; the bound is a hang detector, not a latency bound.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ChunkChainCycle extends TestHelper {
    private static final String VERTEX_TYPE = "Issue8713Node";
    private static final String EDGE_TYPE   = "Issue8713Link";
    private static final int    DEGREE      = 400;

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType(VERTEX_TYPE);
        database.getSchema().buildEdgeType().withName(EDGE_TYPE).withBidirectional(true).create();
      });
    }

    @Test
    void walksEndOnATwoChunkCycle() throws Exception {
      final RID hub = createHubWithTwoChunkCycle();
      final RID stranger = inTx(() -> database.newVertex(VERTEX_TYPE).save().getIdentity());

      assertThat(runBounded(() -> inTx(() -> edgeLinkedListFor(hub).count()))).isPositive();
      assertThat(runBounded(() -> inTx(() -> edgeLinkedListFor(hub).count(EDGE_TYPE)))).isPositive();
      assertThat(runBounded(() -> inTx(() -> edgeLinkedListFor(hub).toJSON()))).isNotNull();
      assertThat(runBounded(() -> inTx(() -> edgeLinkedListFor(hub).containsVertex(stranger, null)))).isFalse();
      assertThat(runBounded(() -> inTx(() -> edgeLinkedListFor(hub).containsEdge(new RID(0, 0))))).isFalse();

      runBounded(() -> {
        database.transaction(() -> edgeLinkedListFor(hub).removeVertex(stranger));
        return null;
      });
      runBounded(() -> {
        database.transaction(() -> edgeLinkedListFor(hub).removeEdgeRID(new RID(0, 0)));
        return null;
      });
    }

    /**
     * A walk that deletes what it visits must not meet a chunk it already deleted: with the A to B to A cycle it would
     * resolve A again after deleting it.
     */
    @Test
    void deleteAllSurvivesATwoChunkCycle() throws Exception {
      final RID hub = createHubWithTwoChunkCycle();

      runBounded(() -> {
        database.transaction(() -> edgeLinkedListFor(hub).deleteAll());
        return null;
      });
    }

    @Test
    void iteratorsEndOnATwoChunkCycle() throws Exception {
      final RID hub = createHubWithTwoChunkCycle();
      final int cap = 100 * DEGREE;

      for (final String[] types : new String[][] { {}, { EDGE_TYPE } }) {
        final Integer walked = runBounded(() -> inTx(() -> {
          final Iterator<RID> rids = edgeLinkedListFor(hub).ridIterator(types);
          int count = 0;
          while (count <= cap && rids.hasNext()) {
            rids.next();
            ++count;
          }
          return count;
        }));
        assertThat(walked).isLessThanOrEqualTo(cap);
      }

      final Long degree = runBounded(() -> inTx(() -> hub.asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE)));
      assertThat(degree).isPositive();
    }

    @Test
    void aLongHealthyChainIsNotTruncated() {
      final RID hub = createHub();
      final long[] counted = new long[1];
      database.transaction(() -> counted[0] = edgeLinkedListFor(hub).count());
      assertThat(counted[0]).isEqualTo(DEGREE);
    }

    private <T> T inTx(final Supplier<T> read) {
      final List<T> result = new ArrayList<>(1);
      database.transaction(() -> result.add(read.get()));
      return result.getFirst();
    }

    private <T> T runBounded(final Callable<T> walk) throws Exception {
      final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        final Thread thread = new Thread(r, "Issue8713-walk");
        thread.setDaemon(true);
        return thread;
      });
      try {
        final Future<T> future = executor.submit(walk);
        return future.get(60, TimeUnit.SECONDS);
      } finally {
        executor.shutdownNow();
      }
    }

    private RID createHub() {
      final RID[] hub = new RID[1];
      database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());
      database.transaction(() -> {
        final MutableVertex target = hub[0].asVertex(true).modify();
        for (int i = 0; i < DEGREE; i++)
          database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, target);
      });
      return hub[0];
    }

    /**
     * One hub whose IN chain has at least two chunks, the oldest pointing back at the head: head -> ... -> tail -> head.
     */
    private RID createHubWithTwoChunkCycle() {
      final RID hub = createHub();
      database.transaction(() -> {
        final RID head = ((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk();
        final MutableEdgeSegment headSegment = (MutableEdgeSegment) database.lookupByRID(head, true);
        final RID behind = headSegment.getPreviousRID();
        assertThat(behind).as("the chain needs at least two chunks").isNotNull();
        final MutableEdgeSegment tail = (MutableEdgeSegment) database.lookupByRID(behind, true);
        tail.setPrevious(headSegment);
        ((DatabaseInternal) database).updateRecord(tail);
      });
      return hub;
    }

    private EdgeLinkedList edgeLinkedListFor(final RID hub) {
      return ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk((VertexInternal) hub.asVertex(true), Vertex.DIRECTION.IN);
    }
  }

  /**
   * #9985: the truncated buffer guard #4432 added protected {@link ImmutableVertex} only. {@link ImmutableEdge}, whose
   * prefix has no fixed length and which is on the live read path, and both mutable shapes through
   * {@code setBuffer()}, let a bare {@code BufferUnderflowException} with a null message escape instead, naming neither
   * the record nor the file.
   * <p>
   * The guarantee is the one #4432 could have made and did not: a buffer cut anywhere inside the prefix, fed to ALL FOUR
   * record shapes, raises a {@link SerializationException} naming the record. The cut is made twice - on an array of
   * the cut length, which underflows, and on the full array with a shorter declared size, which reads past that size
   * without failing at all unless the parser compares against it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TruncatedRecordPrefix extends TestHelper {
    private static final String VERTEX_TYPE = "Issue9985Vertex";
    private static final String EDGE_TYPE   = "Issue9985Edge";

    private RID    vertexRid;
    private RID    edgeRid;
    private byte[] vertexContent;
    private byte[] edgeContent;

    @Override
    protected void beginTest() {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);

      database.transaction(() -> {
        // Vertex positions past the single-byte varint range in every bucket of the type, so each compressed RID of the
        // edge prefix spans several bytes and a cut can land inside one of them
        for (int i = 0; i < 2_000; i++)
          database.newVertex(VERTEX_TYPE).set("i", i).save();
        final MutableVertex a = database.newVertex(VERTEX_TYPE).set("name", "a").save();
        final MutableVertex b = database.newVertex(VERTEX_TYPE).set("name", "b").save();
        final MutableEdge edge = a.newEdge(EDGE_TYPE, b).set("weight", 7).save();
        vertexRid = a.getIdentity();
        edgeRid = edge.getIdentity();
      });

      vertexContent = recordContent(vertexRid);
      edgeContent = recordContent(edgeRid);
    }

    private byte[] recordContent(final RID rid) {
      final Binary buffer = database.getSchema().getBucketById(rid.getBucketId()).getRecord(rid);
      return buffer.toByteArray();
    }

    private VertexType vertexType() {
      return (VertexType) database.getSchema().getType(VERTEX_TYPE);
    }

    private EdgeType edgeType() {
      return (EdgeType) database.getSchema().getType(EDGE_TYPE);
    }

    @Test
    void theUntruncatedRecordsStillReadInEveryShape() {
      final ImmutableVertex immutableVertex = new ImmutableVertex(database, vertexType(), vertexRid, new Binary(vertexContent.clone()));
      assertThat(immutableVertex.getString("name")).isEqualTo("a");
      assertThat(immutableVertex.getOutEdgesHeadChunk()).isNotNull();
      final ImmutableEdge immutableEdge = new ImmutableEdge(database, edgeType(), edgeRid, new Binary(edgeContent.clone()));
      assertThat(immutableEdge.getInteger("weight")).isEqualTo(7);
      assertThat(immutableEdge.getOut()).isEqualTo(vertexRid);

      // setBuffer() installs the prefix; the property map of a mutable record is left alone by design
      final MutableVertex vertex = new MutableVertex(database, vertexType(), vertexRid);
      vertex.setBuffer(new Binary(vertexContent.clone()));
      assertThat(vertex.getOutEdgesHeadChunk()).isEqualTo(immutableVertex.getOutEdgesHeadChunk());

      final MutableEdge edge = new MutableEdge(database, edgeType(), edgeRid);
      edge.setBuffer(new Binary(edgeContent.clone()));
      assertThat(edge.getOut()).isEqualTo(vertexRid);
      assertThat(edge.getIn()).isEqualTo(immutableEdge.getIn());
    }

    @Test
    void immutableVertexNamesTheRecord() {
      assertEveryCutIsNamed(vertexRid, vertexContent, 25, buffer -> new ImmutableVertex(database, vertexType(), vertexRid, buffer));
    }

    @Test
    void immutableEdgeNamesTheRecord() {
      assertEveryCutIsNamed(edgeRid, edgeContent, edgePrefixSize(), buffer -> new ImmutableEdge(database, edgeType(), edgeRid, buffer));
    }

    @Test
    void mutableVertexSetBufferNamesTheRecord() {
      assertEveryCutIsNamed(vertexRid, vertexContent, 25, buffer -> {
        final MutableVertex vertex = new MutableVertex(database, vertexType(), vertexRid);
        vertex.setBuffer(buffer);
        return vertex;
      });
    }

    @Test
    void mutableEdgeSetBufferNamesTheRecord() {
      assertEveryCutIsNamed(edgeRid, edgeContent, edgePrefixSize(), buffer -> {
        final MutableEdge edge = new MutableEdge(database, edgeType(), edgeRid);
        edge.setBuffer(buffer);
        return edge;
      });
    }

    /** The secondary half: a reload of the mutable shapes re-derives the prefix from the buffer it installs. */
    @Test
    void aReloadOfTheMutableShapesReadsThePrefixOfTheNewBuffer() {
      final MutableVertex vertex = new MutableVertex(database, vertexType(), vertexRid);
      vertex.setBuffer(new Binary(vertexContent.clone()));
      final RID outHead = vertex.getOutEdgesHeadChunk();
      vertex.setOutEdgesHeadChunk(null);
      vertex.reload();
      assertThat(vertex.getOutEdgesHeadChunk()).isEqualTo(outHead);
      assertThat(vertex.getString("name")).isEqualTo("a");

      final MutableEdge edge = new MutableEdge(database, edgeType(), edgeRid, vertexRid, vertexRid);
      edge.reload();
      assertThat(edge.getIn()).isNotEqualTo(vertexRid);
      assertThat(edge.getOut()).isEqualTo(vertexRid);
      assertThat(edge.getInteger("weight")).isEqualTo(7);
    }

    private int edgePrefixSize() {
      final Binary buffer = new Binary(edgeContent.clone());
      buffer.position(1);
      final DatabaseInternal internal = (DatabaseInternal) database;
      internal.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID, null);
      internal.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID, null);
      // the fixture's RIDs need more than one byte each, so a cut can fall inside a RID and not only between them
      assertThat(buffer.position()).isGreaterThan(5);
      return buffer.position();
    }

    private static void assertEveryCutIsNamed(final RID rid, final byte[] content, final int prefixSize,
        final Function<Binary, Object> shape) {
      for (int cut = 0; cut < prefixSize; cut++) {
        final int size = cut;
        // an array of the cut length: the read underflows
        assertThatThrownBy(() -> shape.apply(new Binary(Arrays.copyOf(content, size))))
            .as("cut at %d of a %d byte prefix", size, prefixSize)
            .isInstanceOf(SerializationException.class)
            .hasMessageContaining(rid.toString())
            .hasMessageContaining("truncated or corrupted");
        // the whole array with a shorter declared size: the read does not underflow, it reads past the size
        assertThatThrownBy(() -> shape.apply(new Binary(content.clone(), size)))
            .as("declared size %d of a %d byte prefix", size, prefixSize)
            .isInstanceOf(SerializationException.class)
            .hasMessageContaining(rid.toString())
            .hasMessageContaining("truncated or corrupted");
      }
    }
  }
}
