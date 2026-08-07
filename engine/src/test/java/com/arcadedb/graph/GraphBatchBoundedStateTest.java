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
}
