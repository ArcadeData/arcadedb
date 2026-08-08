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
import com.arcadedb.exception.ConcurrentModificationException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the PR #5950 review cycle 4 finding on the OUTGOING edge direction.
 * <p>
 * {@code connectOutgoingEdgesParallel} schedules each source bucket as {@code async.transaction(..., 3, ...)},
 * and {@code DatabaseAsyncTransaction.execute()} retries the <b>same lambda</b> up to three times on a
 * {@link ConcurrentModificationException}, rolling back and re-invoking {@code connectOutEdgesRangeLocal}
 * over the same range. Page-level CME is a normal, expected occurrence on this exact code path (two edges
 * appended into the same edge-list page), which is precisely why the async layer retries it transparently.
 * <p>
 * {@code getOrCreateOutEdgeChunk()} used to persist a brand-new segment AND immediately write its RID into
 * the class-level {@code outChunkRIDCache} (plus flip the vertex head pointer) before the surrounding
 * transaction committed. That cache write survived the rollback, so the retry read the cache first and
 * dereferenced a RID belonging to a transaction that had been rolled back - turning a transparently
 * retryable transient conflict into a hard {@code RecordNotFoundException} batch failure.
 * <p>
 * The fix routes new-segment creation through the per-bucket local maps that are merged into the shared
 * caches only after the bucket's commit succeeds, mirroring what {@code connectIncomingEdgesRangeLocal}
 * already does for the IN direction.
 * <p>
 * The source vertices here are created with plain {@code database.newVertex()} rather than
 * {@code GraphBatch.createVertices()} on purpose: only then are they absent from
 * {@code knownNewVertexKeys}, which is what routes them into the vertex-loading fallback branch that
 * called {@code getOrCreateOutEdgeChunk()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5950OutgoingEdgeRetryCacheTest extends TestHelper {

  private static final String VERTEX_TYPE = "Issue5950RetrySrc";
  private static final String EDGE_TYPE   = "ISSUE5950_RETRY_KNOWS";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
    });
  }

  @AfterEach
  void clearHook() {
    GraphBatch.TEST_BEFORE_OUTGOING_EDGE_COMMIT_HOOK = null;
  }

  /**
   * A CME on the first attempt of an outgoing-edge bucket task must be absorbed by the async layer's own
   * retry and produce a complete, correct graph - not a {@code RecordNotFoundException} from the retry
   * reading a rolled-back segment RID out of the shared head-chunk cache.
   */
  @Test
  void concurrentModificationRetryDoesNotReadRolledBackSegmentRID() {
    final int vertices = 8;

    // Plain newVertex(), NOT GraphBatch.createVertices(): keeps these out of knownNewVertexKeys so the
    // edge connect takes the vertex-loading fallback branch this fix changed.
    final RID[] rids = new RID[vertices];
    database.transaction(() -> {
      for (int i = 0; i < vertices; i++) {
        final MutableVertex v = database.newVertex(VERTEX_TYPE);
        v.set("id", i);
        v.save();
        rids[i] = v.getIdentity();
      }
    });

    final AtomicInteger calls = new AtomicInteger();
    GraphBatch.TEST_BEFORE_OUTGOING_EDGE_COMMIT_HOOK = attempt -> {
      calls.incrementAndGet();
      // Fail the first bucket attempt only; the async executor's CME retry must then succeed.
      if (attempt == 1)
        throw new ConcurrentModificationException("simulated page conflict on the first attempt");
    };

    try (final GraphBatch importer = GraphBatch.builder(database)
        .withBatchSize(1_000_000)
        .withParallelFlush(true)
        .build()) {
      // A chain, so every vertex is a source exactly once.
      for (int i = 0; i < vertices - 1; i++)
        importer.newEdge(rids[i], EDGE_TYPE, rids[i + 1]);
    }

    assertThat(calls.get())
        .as("the injected CME plus at least one retry attempt must have run")
        .isGreaterThanOrEqualTo(2);

    // Every edge must be present exactly once in both directions: the rolled-back attempt must have left
    // nothing behind, and the retry must have recreated the segment cleanly.
    database.transaction(() -> {
      int totalOut = 0;
      int totalIn = 0;
      for (int i = 0; i < vertices; i++) {
        final Vertex v = rids[i].asVertex();
        for (final Edge ignored : v.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE))
          totalOut++;
        for (final Edge ignored : v.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE))
          totalIn++;
      }
      assertThat(totalOut).as("every outgoing edge exactly once after the CME retry").isEqualTo(vertices - 1);
      assertThat(totalIn).as("every incoming edge exactly once after the CME retry").isEqualTo(vertices - 1);
    });
  }
}
