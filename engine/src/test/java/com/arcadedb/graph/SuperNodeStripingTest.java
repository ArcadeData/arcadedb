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
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Super-node striped edge list (#5156): when a vertex crosses GRAPH_SUPERNODE_THRESHOLD its edge list is
 * promoted to a {@link StripeDirectory} + N per-stripe chains hosted in a per-type bucket pool, so concurrent
 * appends stop contending on one page/file. These tests cover promotion, correctness of every edge-list
 * operation across the promotion boundary, persistence across reopen, disabled mode, and concurrency.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SuperNodeStripingTest extends TestHelper {
  private int savedThreshold;
  private int savedStripes;
  private int savedRetryDelay;

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
    savedRetryDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
    GlobalConfiguration.TX_RETRY_DELAY.setValue(savedRetryDelay);
  }

  private void createSchema() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub", 1);
      database.getSchema().createVertexType("Src", 8);
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

  /** Inserts {@code count} edges Src->Hub, one transaction each, returning the source vertex RIDs in order. */
  private List<RID> insertEdges(final RID hubRID, final int count) {
    final List<RID> sources = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final RID[] srcHolder = new RID[1];
      database.transaction(() -> {
        final MutableVertex src = database.newVertex("Src");
        src.save();
        src.newEdge("LINK", hubRID);
        srcHolder[0] = src.getIdentity();
      });
      sources.add(srcHolder[0]);
    }
    return sources;
  }

  private Record loadInHead(final RID hubRID) {
    final Record[] head = new Record[1];
    database.transaction(() -> {
      final RID headRID = ((VertexInternal) hubRID.asVertex(true)).getInEdgesHeadChunk();
      head[0] = database.lookupByRID(headRID, true);
    });
    return head[0];
  }

  @Test
  void promotionAtThresholdKeepsEveryEdgeAndEveryOperation() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    createSchema();
    final RID hubRID = createHub();

    final int total = 300;
    final List<RID> sources = insertEdges(hubRID, total);

    // THE HUB'S IN LIST MUST HAVE BEEN PROMOTED TO THE STRIPED LAYOUT
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);
    // THE SOURCES (LOW DEGREE) MUST STAY CLASSIC
    database.transaction(() -> {
      final RID srcOutHead = ((VertexInternal) sources.getFirst().asVertex(true)).getOutEdgesHeadChunk();
      assertThat(database.lookupByRID(srcOutHead, true)).isInstanceOf(MutableEdgeSegment.class);
    });

    database.transaction(() -> {
      final Vertex hub = hubRID.asVertex(true);

      // COUNT: EVERY EDGE, ACROSS GENERATION 0 (PRE-PROMOTION) AND THE STRIPES
      assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(total);

      // ITERATION: NO EDGE LOST, NO EDGE DUPLICATED
      final Set<RID> found = new HashSet<>();
      for (final Iterator<Vertex> it = hub.getVertices(Vertex.DIRECTION.IN, "LINK").iterator(); it.hasNext(); )
        assertThat(found.add(it.next().getIdentity())).isTrue();
      assertThat(found).hasSize(total);
      assertThat(found).containsAll(sources);

      // CONNECTIVITY: LOCALISED LOOKUPS WORK FOR PRE-PROMOTION AND POST-PROMOTION NEIGHBOURS
      assertThat(hub.isConnectedTo(sources.getFirst(), Vertex.DIRECTION.IN)).isTrue();
      assertThat(hub.isConnectedTo(sources.getLast(), Vertex.DIRECTION.IN)).isTrue();
      assertThat(hub.isConnectedTo(hubRID, Vertex.DIRECTION.IN)).isFalse();
    });
  }

  @Test
  void belowThresholdStaysClassic() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(10_000);
    createSchema();
    final RID hubRID = createHub();

    insertEdges(hubRID, 200);

    assertThat(loadInHead(hubRID)).isInstanceOf(MutableEdgeSegment.class);
    database.transaction(() -> assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(200));
  }

  @Test
  void zeroThresholdDisablesPromotion() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(0);
    createSchema();
    final RID hubRID = createHub();

    insertEdges(hubRID, 300);

    assertThat(loadInHead(hubRID)).isInstanceOf(MutableEdgeSegment.class);
    // NO STRIPE BUCKETS MUST HAVE BEEN CREATED
    assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("Hub", 0))).isFalse();
  }

  @Test
  void promotedVertexSurvivesReopen() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    createSchema();
    final RID hubRID = createHub();

    final int total = 300;
    final List<RID> sources = insertEdges(hubRID, total);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    reopenDatabase();

    // REBUILD PLAIN RIDS: THE CAPTURED ONES ARE BOUND TO THE CLOSED DATABASE INSTANCE
    final RID hub2 = new RID(hubRID.getBucketId(), hubRID.getPosition());
    final RID firstSrc = new RID(sources.getFirst().getBucketId(), sources.getFirst().getPosition());
    final RID lastSrc = new RID(sources.getLast().getBucketId(), sources.getLast().getPosition());

    assertThat(loadInHead(hub2)).isInstanceOf(StripeDirectory.class);
    database.transaction(() -> {
      final Vertex hub = hub2.asVertex(true);
      assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(total);
      assertThat(hub.isConnectedTo(firstSrc, Vertex.DIRECTION.IN)).isTrue();
      assertThat(hub.isConnectedTo(lastSrc, Vertex.DIRECTION.IN)).isTrue();
    });

    // AND THE PROMOTED VERTEX MUST STILL ACCEPT NEW EDGES AFTER THE REOPEN
    insertEdges(hub2, 10);
    database.transaction(() -> assertThat(hub2.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(total + 10));
  }

  @Test
  void removeAndDeleteOnPromotedVertex() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    createSchema();
    final RID hubRID = createHub();

    final int total = 300;
    final List<RID> sources = insertEdges(hubRID, total);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    // DELETE A PRE-PROMOTION EDGE (GENERATION 0) AND A POST-PROMOTION ONE (STRIPES)
    database.transaction(() -> {
      sources.getFirst().asVertex(true).getEdges(Vertex.DIRECTION.OUT, "LINK").iterator().next().delete();
      sources.getLast().asVertex(true).getEdges(Vertex.DIRECTION.OUT, "LINK").iterator().next().delete();
    });

    database.transaction(() -> {
      final Vertex hub = hubRID.asVertex(true);
      assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(total - 2);
      assertThat(hub.isConnectedTo(sources.getFirst(), Vertex.DIRECTION.IN)).isFalse();
      assertThat(hub.isConnectedTo(sources.getLast(), Vertex.DIRECTION.IN)).isFalse();
      assertThat(hub.isConnectedTo(sources.get(1), Vertex.DIRECTION.IN)).isTrue();
    });

    // DELETING THE PROMOTED VERTEX MUST DROP THE DIRECTORY AND EVERY STRIPE CHAIN (integrity checked at teardown)
    database.transaction(() -> hubRID.asVertex(true).delete());
    database.transaction(() -> assertThat(database.countType("Hub", false)).isEqualTo(0));
  }

  @Test
  void dropTypeRemovesStripePool() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    createSchema();
    final RID hubRID = createHub();

    insertEdges(hubRID, 300);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);
    assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("Hub", 0))).isTrue();

    // DELETE THE HUB FIRST (WITH ITS EDGES): DROPPING A TYPE NEVER CASCADES ON CONNECTED EDGES
    database.transaction(() -> hubRID.asVertex(true).delete());

    database.getSchema().dropType("Hub");

    for (int i = 0; i < GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger(); i++)
      assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("Hub", i))).isFalse();
  }

  @Tag("slow")
  @Test
  void concurrentAppendsOnPromotedVertex() throws InterruptedException {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    GlobalConfiguration.TX_RETRY_DELAY.setValue(1);
    createSchema();
    final RID hubRID = createHub();

    // PROMOTE FIRST WITH A SINGLE THREAD, THEN HAMMER THE STRIPES CONCURRENTLY
    final int warmup = 200;
    insertEdges(hubRID, warmup);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    final int threads = 6;
    final int perThread = 500;
    final AtomicLong failures = new AtomicLong();
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    for (int t = 0; t < threads; t++) {
      new Thread(() -> {
        try {
          start.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          done.countDown();
          return;
        }
        for (int i = 0; i < perThread; i++) {
          try {
            database.transaction(() -> {
              final MutableVertex src = database.newVertex("Src");
              src.save();
              src.newEdge("LINK", hubRID);
            }, false, 10_000);
          } catch (final Exception e) {
            failures.incrementAndGet();
          }
        }
        done.countDown();
      }).start();
    }
    start.countDown();
    done.await();

    assertThat(failures.get()).isEqualTo(0);
    database.transaction(() -> assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"))
        .isEqualTo(warmup + threads * perThread));
  }
}
