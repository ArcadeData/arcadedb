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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collection;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9575, discussion #9559, 4th report: the edge pass of a GraphImporter load ran at 460,000 edges/s until 172M edges,
 * then fell to 1,400/s. The edge file listed each work's citations together, so the destinations came in order and the sources
 * at random: every flush met again most of the sources it connected, and read the head segment of each one's edge list from
 * the disk to append to it. A segment this batch wrote is sized to its content, so the append never fit: the read only told
 * the batch to chain a new segment, which needs nothing but the head's RID - and that the batch keeps in memory. Once the
 * segments outgrew the memory, each of those reads was a random disk read, ~0.7 ms an edge.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9575GraphBatchSegmentHeadReadsTest extends TestHelper {
  private static final int VERTICES   = 2_000;
  private static final int EDGES      = 30_000;
  private static final int BATCH_SIZE = 1_000;

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void segmentsThisBatchWroteAreChainedWithoutBeingRead(final boolean parallelFlush) {
    final RID[] rids = createVertices();
    final int[] outDegree = new int[VERTICES];
    final int[] inDegree = new int[VERTICES];

    // random sources AND destinations, small flushes and drains: every vertex comes back in many of them, both directions
    final long reads = load(rids, parallelFlush, 1, outDegree, inDegree, new boolean[VERTICES], new boolean[VERTICES]);
    assertThat(reads).as("edge-list segments read back by a batch that wrote every one of them").isZero();
    assertDegrees(rids, outDegree, inDegree);
    assertDatabaseIsConsistent();
  }

  /**
   * A segment the batch did not write may have room left - a pre-allocated one, or the head of a list an earlier load wrote -
   * so it is still read and filled before anything is chained to it.
   */
  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void aHeadThisBatchDidNotWriteIsStillReadAndKeepsItsEdges(final boolean parallelFlush) {
    final RID[] rids = createVertices();
    final int[] outDegree = new int[VERTICES];
    final int[] inDegree = new int[VERTICES];
    load(rids, parallelFlush, 1, outDegree, inDegree, new boolean[VERTICES], new boolean[VERTICES]);

    final long reads = load(rids, parallelFlush, 2, outDegree, inDegree, null, null);
    assertThat(reads).isGreaterThan(0L);
    assertDegrees(rids, outDegree, inDegree);
    assertDatabaseIsConsistent();
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
   * Loads EDGES random edges and answers the edge-list segments the batch read. With {@code outTouched}/{@code inTouched} given,
   * an endpoint is hinted the first time it is handed to the batch in that direction, as GraphImporter does.
   */
  private long load(final RID[] rids, final boolean parallelFlush, final long seed, final int[] outDegree, final int[] inDegree,
      final boolean[] outTouched, final boolean[] inTouched) {
    final Random rnd = new Random(seed);
    final GraphBatch batch = database.batch().withBatchSize(BATCH_SIZE).withBidirectional(true).withWAL(false)
        .withParallelFlush(parallelFlush).withCommitEvery(300).withMaxDeferredIncomingEdges(BATCH_SIZE * 3).build();
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
    return batch.getSegmentHeadReads();
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

  private void assertDatabaseIsConsistent() {
    try (final ResultSet result = database.command("sql", "CHECK DATABASE")) {
      final Result row = result.next();
      assertThat(row.<Collection<?>>getProperty("corruptedRecords")).as("corruptedRecords: %s", row.toJSON()).isEmpty();
      assertThat(((Number) row.getProperty("invalidLinks")).longValue()).as("invalidLinks: %s", row.toJSON()).isZero();
    }
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
  }
}
