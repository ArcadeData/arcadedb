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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Issue #9575, discussion #9559: the edge pass of a GraphImporter load over 209M vertices stopped for 15 minutes every
 * 500,000 edges. Each flush read, for every distinct vertex it connected, the vertex record itself to find the edge list to
 * append to - one random disk access per vertex on a graph far larger than the memory - although the importer had created
 * every one of those vertices without edges and knew it. {@link GraphBatch#newEdgeWithHints} lets a caller say so.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9575GraphBatchEndpointHintsTest extends TestHelper {
  private static final int VERTICES   = 3_000;
  private static final int EDGES      = 20_000;
  private static final int BATCH_SIZE = 2_000;

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void hintedEndpointsAreNeverReadAndKeepEveryEdge(final boolean parallelFlush) {
    final RID[] rids = createVertices();
    final int[] outDegree = new int[VERTICES];
    final int[] inDegree = new int[VERTICES];

    // the premise: without hints, the batch reads the vertices it connects
    final long readsWithoutHints = load(rids, parallelFlush, 0, outDegree, inDegree, null, null);
    assertThat(readsWithoutHints).isGreaterThan(0L);

    // a second type, untouched: hinted the way GraphImporter hints, the first time a vertex is handed to a batch per direction
    final RID[] fresh = createVertices();
    final int[] freshOut = new int[VERTICES];
    final int[] freshIn = new int[VERTICES];
    final long readsWithHints = load(fresh, parallelFlush, 1, freshOut, freshIn, new boolean[VERTICES], new boolean[VERTICES]);
    assertThat(readsWithHints).as("vertices read by a load whose every endpoint was hinted").isZero();
    assertDegrees(fresh, freshOut, freshIn);

    // a second batch over the same vertices: they have edges now, so no hint, and the edges of the first batch survive
    final long readsOfSecondBatch = load(fresh, parallelFlush, 2, freshOut, freshIn, null, null);
    assertThat(readsOfSecondBatch).isGreaterThan(0L);
    assertDegrees(fresh, freshOut, freshIn);
  }

  /**
   * A wrong hint would orphan the edge list the vertex has. Under {@code -ea}, as every test suite runs, the batch checks the
   * hint against the vertex and refuses it before writing anything over the list.
   */
  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void aWrongHintIsCaughtUnderAssertions(final boolean parallelFlush) {
    boolean assertions = false;
    assert assertions = true;
    assumeTrue(assertions, "the check runs under -ea only");

    final RID[] rids = createVertices();
    final int[] outDegree = new int[VERTICES];
    load(rids, parallelFlush, 0, outDegree, new int[VERTICES], null, null);
    int withEdges = 0;
    while (outDegree[withEdges] == 0)
      ++withEdges;

    final GraphBatch batch = database.batch().withBatchSize(BATCH_SIZE).withBidirectional(true).withWAL(false)
        .withParallelFlush(parallelFlush).build();
    batch.newEdgeWithHints(rids[withEdges], "E", rids[1], GraphBatch.SOURCE_HAS_NO_OUT_EDGES);
    // the parallel flush reports what failed in a worker wrapped, hence the stack trace rather than the type
    assertThatThrownBy(batch::close).hasStackTraceContaining("hinted to have no outgoing edges");
    if (database.isTransactionActive())
      database.rollback();
  }

  @ParameterizedTest
  @ValueSource(ints = { 4, -1, 0x100 })
  void unknownHintBitsAreRefused(final int hints) {
    final RID[] rids = createVertices();
    try (final GraphBatch batch = database.batch().withBidirectional(true).withWAL(false).build()) {
      assertThatThrownBy(() -> batch.newEdgeWithHints(rids[0], "E", rids[1], hints)).isInstanceOf(IllegalArgumentException.class);
    }
  }

  private RID[] createVertices() {
    final RID[] rids = new RID[VERTICES];
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        rids[i] = database.newVertex("V").set("i", i).save().getIdentity();
    });
    return rids;
  }

  /**
   * Loads EDGES random edges. With {@code outTouched}/{@code inTouched} given, an endpoint is hinted the first time it is
   * handed to the batch in that direction, as GraphImporter does.
   */
  private long load(final RID[] rids, final boolean parallelFlush, final long seed, final int[] outDegree, final int[] inDegree,
      final boolean[] outTouched, final boolean[] inTouched) {
    final Random rnd = new Random(seed);
    final GraphBatch batch = database.batch().withBatchSize(BATCH_SIZE).withBidirectional(true).withWAL(false)
        .withParallelFlush(parallelFlush).withCommitEvery(500).build();
    try {
      for (int e = 0; e < EDGES; e++) {
        final int s = rnd.nextInt(VERTICES);
        final int d = rnd.nextInt(VERTICES);
        int hints = 0;
        if (outTouched != null && !outTouched[s]) {
          outTouched[s] = true;
          hints |= GraphBatch.SOURCE_HAS_NO_OUT_EDGES;
        }
        if (inTouched != null && !inTouched[d]) {
          inTouched[d] = true;
          hints |= GraphBatch.DESTINATION_HAS_NO_IN_EDGES;
        }
        batch.newEdgeWithHints(rids[s], "E", rids[d], hints);
        outDegree[s]++;
        inDegree[d]++;
      }
    } finally {
      batch.close();
    }
    // read after close(): the incoming side is connected there
    return batch.getVertexHeadReads();
  }

  private void assertDegrees(final RID[] rids, final int[] outDegree, final int[] inDegree) {
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++) {
        final Vertex v = rids[i].asVertex();
        assertThat(v.countEdges(Vertex.DIRECTION.OUT, "E")).as("out edges of vertex %d", i).isEqualTo(outDegree[i]);
        assertThat(v.countEdges(Vertex.DIRECTION.IN, "E")).as("in edges of vertex %d", i).isEqualTo(inDegree[i]);
      }
    });
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
  }
}
