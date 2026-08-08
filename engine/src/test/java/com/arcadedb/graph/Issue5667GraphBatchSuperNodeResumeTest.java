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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5667: {@link GraphBatch} used to hard-fail with an {@link IllegalStateException}
 * (sequential flush, via {@code getOrCreateOutSegmentDeferred}/{@code getOrCreateInSegmentDeferred}) or a raw
 * {@link ClassCastException} (parallel flush, via {@code getOrCreateOutEdgeChunk}/{@code getOrCreateInEdgeChunk})
 * as soon as it touched a vertex already promoted to the super-node striped layout (#5156).
 * <p>
 * A bulk load resuming over a graph that already has hub vertices - created either through the standard API
 * before the load ran, or by a previous, non-bulk write - must instead succeed: this class now routes edges for
 * an already-promoted vertex through the standard {@link StripedEdgeList} write path (see
 * {@code GraphBatch#addGroupThroughStripedEdgeList}) instead of throwing.
 * <p>
 * GraphBatch itself still never PROMOTES a vertex during bulk load (see the {@link GraphBatch} class javadoc) -
 * that is out of scope for this fix and is not asserted here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5667GraphBatchSuperNodeResumeTest extends TestHelper {
  private int savedThreshold;
  private int savedStripes;

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub");
      database.getSchema().createVertexType("Leaf", 8);
      database.getSchema().createEdgeType("LINK", 8);
    });
  }

  private RID createHub() {
    final MutableVertex[] holder = new MutableVertex[1];
    database.transaction(() -> {
      holder[0] = database.newVertex("Hub");
      holder[0].save();
    });
    return holder[0].getIdentity();
  }

  /** Promotes {@code hubRID}'s OUT list by creating {@code count} Hub->Leaf edges via the standard API. */
  private void promoteOutList(final RID hubRID, final int count) {
    for (int i = 0; i < count; i++)
      database.transaction(() -> {
        final MutableVertex leaf = database.newVertex("Leaf");
        leaf.save();
        hubRID.asVertex(true).modify().newEdge("LINK", leaf);
      });
  }

  /** Promotes {@code hubRID}'s IN list by creating {@code count} Leaf->Hub edges via the standard API. */
  private void promoteInList(final RID hubRID, final int count) {
    for (int i = 0; i < count; i++)
      database.transaction(() -> {
        final MutableVertex leaf = database.newVertex("Leaf");
        leaf.save();
        leaf.newEdge("LINK", hubRID);
      });
  }

  private Record loadOutHead(final RID hubRID) {
    final Record[] head = new Record[1];
    database.transaction(() -> {
      final RID headRID = ((VertexInternal) hubRID.asVertex(true)).getOutEdgesHeadChunk();
      head[0] = database.lookupByRID(headRID, true);
    });
    return head[0];
  }

  private Record loadInHead(final RID hubRID) {
    final Record[] head = new Record[1];
    database.transaction(() -> {
      final RID headRID = ((VertexInternal) hubRID.asVertex(true)).getInEdgesHeadChunk();
      head[0] = database.lookupByRID(headRID, true);
    });
    return head[0];
  }

  private RID createLeaf() {
    final MutableVertex[] holder = new MutableVertex[1];
    database.transaction(() -> {
      holder[0] = database.newVertex("Leaf");
      holder[0].save();
    });
    return holder[0].getIdentity();
  }

  /**
   * Sequential flush ({@code parallelFlush=false}): exercises {@code getOrCreateOutSegmentDeferred} and
   * {@code getOrCreateInSegmentDeferred}, which used to throw {@link IllegalStateException}.
   */
  @Test
  void sequentialFlushResumesOverPromotedHub() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    final RID hubRID = createHub();

    promoteOutList(hubRID, 200);
    promoteInList(hubRID, 200);
    assertThat(loadOutHead(hubRID)).isInstanceOf(StripeDirectory.class);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    final RID newDst = createLeaf();
    final RID newSrc = createLeaf();

    try (final GraphBatch batch = GraphBatch.builder(database)
        .withBatchSize(10)
        .withParallelFlush(false)
        .build()) {
      // Hub is the SOURCE (exercises the promoted OUT path)
      batch.newEdge(hubRID, "LINK", newDst);
      // Hub is the DESTINATION (exercises the promoted IN path, connected at close())
      batch.newEdge(newSrc, "LINK", hubRID);
    }

    // NEITHER LIST WAS CORRUPTED: BOTH STILL A DIRECTORY
    assertThat(loadOutHead(hubRID)).isInstanceOf(StripeDirectory.class);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    database.transaction(() -> {
      final Vertex hub = hubRID.asVertex(true);
      assertThat(hub.countEdges(Vertex.DIRECTION.OUT, "LINK")).isEqualTo(201);
      assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(201);
      assertThat(hub.isConnectedTo(newDst, Vertex.DIRECTION.OUT)).isTrue();
      assertThat(hub.isConnectedTo(newSrc, Vertex.DIRECTION.IN)).isTrue();
    });
  }

  /**
   * Parallel flush ({@code parallelFlush=true}, the default): exercises {@code connectOutEdgesRangeLocal} /
   * {@code connectIncomingEdgesRangeLocal}'s fallback to {@code getOrCreateOutEdgeChunk} /
   * {@code getOrCreateInEdgeChunk}, which used to fail with a raw {@link ClassCastException} (a StripeDirectory
   * cast to EdgeSegment) rather than even the old, documented {@link IllegalStateException}.
   */
  @Test
  void parallelFlushResumesOverPromotedHub() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    final RID hubRID = createHub();

    promoteOutList(hubRID, 200);
    promoteInList(hubRID, 200);
    assertThat(loadOutHead(hubRID)).isInstanceOf(StripeDirectory.class);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    final RID newDst = createLeaf();
    final RID newSrc = createLeaf();

    try (final GraphBatch batch = GraphBatch.builder(database)
        .withBatchSize(10)
        .withParallelFlush(true)
        .build()) {
      batch.newEdge(hubRID, "LINK", newDst);
      batch.newEdge(newSrc, "LINK", hubRID);
    }

    assertThat(loadOutHead(hubRID)).isInstanceOf(StripeDirectory.class);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    database.transaction(() -> {
      final Vertex hub = hubRID.asVertex(true);
      assertThat(hub.countEdges(Vertex.DIRECTION.OUT, "LINK")).isEqualTo(201);
      assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(201);
      assertThat(hub.isConnectedTo(newDst, Vertex.DIRECTION.OUT)).isTrue();
      assertThat(hub.isConnectedTo(newSrc, Vertex.DIRECTION.IN)).isTrue();
    });
  }
}
