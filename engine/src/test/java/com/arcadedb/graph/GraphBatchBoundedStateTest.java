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
import com.arcadedb.database.RID;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5664: {@code GraphBatch} kept two kinds of state proportional to the
 * <b>lifetime</b> of the batch rather than to {@code batchSize}:
 * <ul>
 *   <li>{@code outChunkRIDCache} / {@code inChunkRIDCache} were never cleared, so every distinct vertex an
 *       edge touched added an entry that stayed until {@link GraphBatch#close()}.</li>
 *   <li>The deferred incoming-edge buffer was only drained at {@link GraphBatch#close()}, so its primitive
 *       arrays (and the doubling copy when they grow) scaled with the total edge count of the whole stream.</li>
 * </ul>
 * Both are now bounded: the RID caches are LRUs with a configurable capacity, and the incoming-edge buffer
 * drains early once it crosses a configurable cap. Both call sites fall back to reading from disk on a cache
 * miss, so eviction and early draining must not change the graph the batch produces.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphBatchBoundedStateTest extends TestHelper {

  private static final String VERTEX_TYPE = "BoundedPerson";
  private static final String EDGE_TYPE   = "BOUNDED_KNOWS";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
    });
  }

  /**
   * A stream touching far more distinct vertices than the configured cache capacity must keep the OUT/IN
   * head-chunk RID caches bounded by that capacity, not by the number of vertices touched, while still
   * producing a fully correct graph (sequential flush path).
   */
  @Test
  void headChunkCachesStayBoundedSequential() {
    assertBoundedOverLongStream(false);
  }

  /**
   * Same bound must hold on the parallel flush path, where each async slot merges its own local cache into
   * the shared one via {@code putAll()}.
   */
  @Test
  void headChunkCachesStayBoundedParallel() {
    assertBoundedOverLongStream(true);
  }

  private void assertBoundedOverLongStream(final boolean parallelFlush) {
    final int vertices                 = 6_000;
    final int edges                    = vertices - 1;
    final int chunkCacheCapacity       = 200;
    final int maxDeferredIncomingEdges = 500;

    final RID[] vertexRIDs = new RID[vertices];
    database.transaction(() -> {
      for (int i = 0; i < vertices; i++) {
        final MutableVertex v = database.newVertex(VERTEX_TYPE);
        v.set("id", i);
        v.save();
        vertexRIDs[i] = v.getIdentity();
      }
    });

    final GraphBatch importer = GraphBatch.builder(database)
        .withBatchSize(200)
        .withEdgeListInitialSize(256)
        .withChunkCacheCapacity(chunkCacheCapacity)
        .withMaxDeferredIncomingEdges(maxDeferredIncomingEdges)
        .withParallelFlush(parallelFlush)
        .build();

    try {
      // A distinct src->dst pair per edge: every vertex participates as both a source and a destination, so
      // both caches receive an insertion far past their configured capacity.
      for (int i = 0; i < edges; i++)
        importer.newEdge(vertexRIDs[i], EDGE_TYPE, vertexRIDs[i + 1]);

      importer.flush();

      assertThat(importer.getOutChunkRIDCacheSize())
          .as("OUT head-chunk RID cache must stay bounded by the configured capacity, not by vertex count (%d touched)",
              vertices)
          .isLessThanOrEqualTo(chunkCacheCapacity);
      assertThat(importer.getInChunkRIDCacheSize())
          .as("IN head-chunk RID cache must stay bounded by the configured capacity, not by vertex count (%d touched)",
              vertices)
          .isLessThanOrEqualTo(chunkCacheCapacity);
      assertThat(importer.getDeferredIncomingEdgeCount())
          .as("the deferred incoming-edge buffer must have been drained well before the full edge count piled up")
          .isLessThan(edges);
    } finally {
      importer.close();
    }

    // Cache eviction / early draining must be invisible to correctness: every edge is still traversable in
    // both directions, because a cache miss falls back to reading the vertex's head chunk from disk.
    database.transaction(() -> {
      long totalOut = 0;
      long totalIn = 0;
      for (int i = 0; i < vertices; i++) {
        final Vertex v = vertexRIDs[i].asVertex();
        for (final Edge ignored : v.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE))
          totalOut++;
        for (final Edge ignored : v.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE))
          totalIn++;
      }
      assertThat(totalOut).isEqualTo(edges);
      assertThat(totalIn).isEqualTo(edges);
    });
  }

  /**
   * A cap of 0 on the deferred incoming-edge buffer opts back into the pre-#5664 behavior of deferring
   * everything to {@link GraphBatch#close()} - the buffer must not be drained early in that case.
   */
  @Test
  void zeroCapDisablesEarlyDrain() {
    final int vertices = 300;
    final int edges    = vertices - 1;

    final RID[] vertexRIDs = new RID[vertices];
    database.transaction(() -> {
      for (int i = 0; i < vertices; i++) {
        final MutableVertex v = database.newVertex(VERTEX_TYPE);
        v.set("id", i);
        v.save();
        vertexRIDs[i] = v.getIdentity();
      }
    });

    final GraphBatch importer = GraphBatch.builder(database)
        .withBatchSize(50)
        .withEdgeListInitialSize(256)
        .withMaxDeferredIncomingEdges(0)
        .withParallelFlush(false)
        .build();

    try {
      for (int i = 0; i < edges; i++)
        importer.newEdge(vertexRIDs[i], EDGE_TYPE, vertexRIDs[i + 1]);

      importer.flush();

      assertThat(importer.getDeferredIncomingEdgeCount())
          .as("with the early drain disabled, every incoming edge buffered so far must still be pending")
          .isEqualTo(edges);
    } finally {
      importer.close();
    }
  }

  @Test
  void builderRejectsNonPositiveChunkCacheCapacity() {
    assertThatThrownBy(() -> GraphBatch.builder(database).withChunkCacheCapacity(0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> GraphBatch.builder(database).withChunkCacheCapacity(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void builderRejectsNegativeMaxDeferredIncomingEdges() {
    assertThatThrownBy(() -> GraphBatch.builder(database).withMaxDeferredIncomingEdges(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Regression test for the PR #5950 review finding: bounding {@code outChunkRIDCache}/
   * {@code inChunkRIDCache} with an LRU broke an invariant the "known-new vertex" fast path
   * ({@code knownNewVertexKeys}, populated only by {@link GraphBatch#createVertices}) relied on -
   * that a cache miss for a known-new vertex could only mean "no segment exists yet". Once the RID
   * cache entry for such a vertex is evicted (easy on a large stream once more than
   * {@code chunkCacheCapacity} *other* distinct vertices are touched between two edges of the same
   * vertex), touching that vertex again wrongly took the "assume brand new" branch and created a
   * second, unlinked segment that overwrote the pointer to the first - permanently orphaning the
   * first segment's already-committed edges once the batch closes.
   * <p>
   * This must reproduce with {@link GraphBatch#createVertices}, not plain {@code database.newVertex()}
   * / {@code save()}, since only {@code createVertices} populates {@code knownNewVertexKeys} - the
   * existing bounded-cache tests in this class do not exercise this path at all.
   */
  @Test
  void knownNewVertexSurvivesCacheEvictionBetweenItsOwnEdgesSequential() {
    assertKnownNewVertexSurvivesCacheEviction(false);
  }

  /**
   * Same scenario on the parallel flush path, which reads/writes the caches through
   * {@code connectOutEdgesRangeLocal}/{@code connectIncomingEdgesRangeLocal} rather than the
   * {@code getOrCreate*SegmentDeferred} helpers, but is vulnerable to the exact same eviction race.
   */
  @Test
  void knownNewVertexSurvivesCacheEvictionBetweenItsOwnEdgesParallel() {
    assertKnownNewVertexSurvivesCacheEviction(true);
  }

  private void assertKnownNewVertexSurvivesCacheEviction(final boolean parallelFlush) {
    final int chunkCacheCapacity = 20;
    final int evictionPairs      = 200; // far more than chunkCacheCapacity distinct OTHER vertices
    final int namedVertices      = 6;   // v0, d0, targetA, targetB, srcA, srcB
    final int vertices           = namedVertices + 2 * evictionPairs;

    final GraphBatch importer = GraphBatch.builder(database)
        .withBatchSize(1_000)
        .withEdgeListInitialSize(256)
        .withChunkCacheCapacity(chunkCacheCapacity)
        .withMaxDeferredIncomingEdges(1) // drain IN edges on every flush(), for precise eviction control
        .withParallelFlush(parallelFlush)
        .build();

    final RID[] vertexRIDs;
    try {
      // createVertices() (unlike database.newVertex()/save()) populates knownNewVertexKeys for every
      // one of these vertices - the exact fast path the LRU eviction bug trips over.
      vertexRIDs = importer.createVertices(VERTEX_TYPE, vertices);

      final RID v0     = vertexRIDs[0]; // OUT-direction: source whose head-chunk cache entry gets evicted
      final RID d0     = vertexRIDs[1]; // IN-direction: destination whose head-chunk cache entry gets evicted
      final RID targetA = vertexRIDs[2];
      final RID targetB = vertexRIDs[3];
      final RID srcA     = vertexRIDs[4];
      final RID srcB     = vertexRIDs[5];

      // Step 1: v0's and d0's FIRST edge - each gets a brand-new segment, cached in both the bounded
      // RID cache and the unbounded deferred head map.
      importer.newEdge(v0, EDGE_TYPE, targetA);
      importer.newEdge(srcA, EDGE_TYPE, d0);
      importer.flush();

      // Step 2: touch evictionPairs*2 = 400 other distinct vertices (200 distinct OUT sources, 200
      // distinct IN destinations), far more than chunkCacheCapacity=20, guaranteeing v0's OUT cache
      // entry and d0's IN cache entry are evicted by the time this flush returns.
      for (int i = 0; i < evictionPairs; i++)
        importer.newEdge(vertexRIDs[namedVertices + i], EDGE_TYPE, vertexRIDs[namedVertices + evictionPairs + i]);
      importer.flush();

      // Step 3: v0's and d0's SECOND edge. The RID cache misses (evicted in step 2); knownNewVertexKeys
      // still contains v0/d0, so the buggy code assumed "no segment yet" and created a second, unlinked
      // segment - silently orphaning the step-1 edge. The fix consults deferredOutHead/deferredInHead
      // (unbounded, always accurate) before making that assumption.
      importer.newEdge(v0, EDGE_TYPE, targetB);
      importer.newEdge(srcB, EDGE_TYPE, d0);
      importer.flush();
    } finally {
      importer.close();
    }

    database.transaction(() -> {
      final Vertex v0Vertex = vertexRIDs[0].asVertex();
      int outCount = 0;
      for (final Edge ignored : v0Vertex.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE))
        outCount++;
      assertThat(outCount)
          .as("v0's first outgoing edge must survive its OUT head-chunk cache entry being evicted "
              + "between its own two edges (parallelFlush=%s)", parallelFlush)
          .isEqualTo(2);

      final Vertex d0Vertex = vertexRIDs[1].asVertex();
      int inCount = 0;
      for (final Edge ignored : d0Vertex.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE))
        inCount++;
      assertThat(inCount)
          .as("d0's first incoming edge must survive its IN head-chunk cache entry being evicted "
              + "between its own two edges (parallelFlush=%s)", parallelFlush)
          .isEqualTo(2);
    });
  }

  /**
   * Regression test for the second, broader shape of the same PR #5950 review finding: a vertex that
   * was NOT created via {@link GraphBatch#createVertices} (so it is never in {@code knownNewVertexKeys})
   * is equally vulnerable. Its first segment, created mid-batch, is only tracked in the bounded RID
   * cache and in {@code deferredOutHead}/{@code deferredInHead} - the on-disk {@code getOutEdgesHeadChunk()}
   * stays {@code null} until {@link GraphBatch#close()} persists it. If the RID cache entry is evicted
   * before the vertex's second edge, the {@code !knownNewVertexKeys.contains(vertexKey)} branch reads the
   * still-null on-disk head, falls through, and creates a second, unlinked segment - the same silent
   * orphaning as the known-new case, just reached through the other branch.
   */
  @Test
  void preExistingVertexSurvivesCacheEvictionBetweenItsOwnEdgesSequential() {
    assertPreExistingVertexSurvivesCacheEviction(false);
  }

  /** Same scenario on the parallel flush path. */
  @Test
  void preExistingVertexSurvivesCacheEvictionBetweenItsOwnEdgesParallel() {
    assertPreExistingVertexSurvivesCacheEviction(true);
  }

  private void assertPreExistingVertexSurvivesCacheEviction(final boolean parallelFlush) {
    final int chunkCacheCapacity = 20;
    final int evictionPairs      = 200; // far more than chunkCacheCapacity distinct OTHER vertices

    // v0/d0 exist BEFORE the batch starts (plain database.newVertex()/save()), so GraphBatch never adds
    // them to knownNewVertexKeys - this is the branch the known-new eviction test above does NOT cover.
    final RID[] named = new RID[6];
    database.transaction(() -> {
      for (int i = 0; i < named.length; i++) {
        final MutableVertex v = database.newVertex(VERTEX_TYPE);
        v.save();
        named[i] = v.getIdentity();
      }
    });
    final RID v0     = named[0]; // OUT-direction: source whose head-chunk cache entry gets evicted
    final RID d0     = named[1]; // IN-direction: destination whose head-chunk cache entry gets evicted
    final RID targetA = named[2];
    final RID targetB = named[3];
    final RID srcA     = named[4];
    final RID srcB     = named[5];

    final RID[] evictionVertices = new RID[2 * evictionPairs];
    database.transaction(() -> {
      for (int i = 0; i < evictionVertices.length; i++) {
        final MutableVertex v = database.newVertex(VERTEX_TYPE);
        v.save();
        evictionVertices[i] = v.getIdentity();
      }
    });

    final GraphBatch importer = GraphBatch.builder(database)
        .withBatchSize(1_000)
        .withEdgeListInitialSize(256)
        .withChunkCacheCapacity(chunkCacheCapacity)
        .withMaxDeferredIncomingEdges(1) // drain IN edges on every flush(), for precise eviction control
        .withParallelFlush(parallelFlush)
        .build();

    try {
      // Step 1: v0's and d0's FIRST edge via the batch - each gets a brand-new segment, cached in both
      // the bounded RID cache and the unbounded deferred head map. On-disk head chunk stays null until
      // close().
      importer.newEdge(v0, EDGE_TYPE, targetA);
      importer.newEdge(srcA, EDGE_TYPE, d0);
      importer.flush();

      // Step 2: touch evictionPairs*2 other distinct pre-existing vertices, far more than
      // chunkCacheCapacity=20, guaranteeing v0's OUT cache entry and d0's IN cache entry are evicted.
      for (int i = 0; i < evictionPairs; i++)
        importer.newEdge(evictionVertices[i], EDGE_TYPE, evictionVertices[evictionPairs + i]);
      importer.flush();

      // Step 3: v0's and d0's SECOND edge. The RID cache misses (evicted in step 2); v0/d0 are not in
      // knownNewVertexKeys, so the buggy code read the still-null on-disk head and assumed "no segment
      // yet" again, creating a second, unlinked segment. The fix consults deferredOutHead/deferredInHead
      // unconditionally before falling back to the on-disk read.
      importer.newEdge(v0, EDGE_TYPE, targetB);
      importer.newEdge(srcB, EDGE_TYPE, d0);
      importer.flush();
    } finally {
      importer.close();
    }

    database.transaction(() -> {
      final Vertex v0Vertex = v0.asVertex();
      int outCount = 0;
      for (final Edge ignored : v0Vertex.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE))
        outCount++;
      assertThat(outCount)
          .as("pre-existing v0's first outgoing edge must survive its OUT head-chunk cache entry being "
              + "evicted between its own two edges (parallelFlush=%s)", parallelFlush)
          .isEqualTo(2);

      final Vertex d0Vertex = d0.asVertex();
      int inCount = 0;
      for (final Edge ignored : d0Vertex.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE))
        inCount++;
      assertThat(inCount)
          .as("pre-existing d0's first incoming edge must survive its IN head-chunk cache entry being "
              + "evicted between its own two edges (parallelFlush=%s)", parallelFlush)
          .isEqualTo(2);
    });
  }
}
