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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11591: a transaction appended an edge to the incoming edge list of a vertex that a concurrent transaction then deleted,
 * and a third transaction gave another vertex its first incoming edge list chunk in the slot the deleted chunk had freed. The
 * commit replayed the append onto whatever chunk the slot held by then, so the edge was committed pointing at the deleted vertex
 * and listed in the incoming edges of the other one. The commit must refuse it with a retryable conflict instead.
 */
class Issue11591EdgeAppendOntoReusedChunkTest extends TestHelper {

  @Test
  void anEdgeToAVertexDeletedWhileItWasAppendedIsRefusedWhenItsChunkSlotIsReused() throws Exception {
    assertDanglingEdgeRefused(false);
  }

  @Test
  void aLightweightEdgeToAVertexDeletedWhileItWasAppendedIsRefusedWhenItsChunkSlotIsReused() throws Exception {
    assertDanglingEdgeRefused(true);
  }

  /** The merge the check guards keeps working: a concurrent append to the same chunk of a vertex still alive. */
  @Test
  void anAppendToTheChunkOfALiveVertexIsStillMerged() throws Exception {
    final List<RID> k = createGraph(false);
    final long mergesBefore = ((DatabaseInternal) database).getPageManager().getStats().edgeAppendMerges;

    final Throwable failure = appendWhile(k.get(1), k.get(6),
        () -> database.transaction(() -> vertex(k.get(3)).newEdge("E", vertex(k.get(6)))));

    assertThat(failure).isNull();
    assertThat(((DatabaseInternal) database).getPageManager().getStats().edgeAppendMerges).isGreaterThan(mergesBefore);
    database.transaction(
        () -> assertThat(neighbours(k.get(6), Vertex.DIRECTION.IN)).containsExactlyInAnyOrder(k.get(1), k.get(2), k.get(3)));
  }

  /** And on a super-node, whose chunks the check finds through the stripe directory. */
  @Test
  void anAppendToAStripeChunkOfALiveSuperNodeIsStillMerged() throws Exception {
    final int savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(256);
    try {
      final List<RID> k = createGraph(false);
      final RID hub = k.get(6);
      for (int i = 0; i < 600; i++) {
        final String key = "s" + i;
        database.transaction(() -> database.newVertex("T").set("k", key).save().newEdge("E", vertex(hub)));
      }
      // the chunk of the stripe k2 hashes to, with room left for the two appends below
      database.transaction(() -> vertex(k.get(2)).newEdge("E", vertex(hub)));
      database.transaction(() -> assertThat(database.lookupByRID(((VertexInternal) vertex(hub)).getInEdgesHeadChunk(), true))
          .as("the hub must have been promoted to a super-node").isInstanceOf(StripeDirectory.class));
      final long mergesBefore = ((DatabaseInternal) database).getPageManager().getStats().edgeAppendMerges;

      final Throwable failure = appendWhile(k.get(2), hub, () -> database.transaction(() -> vertex(k.get(2)).newEdge("E", vertex(hub))));

      assertThat(failure).isNull();
      // one merge for the outgoing chunk of k2, one for the stripe chunk of the hub
      assertThat(((DatabaseInternal) database).getPageManager().getStats().edgeAppendMerges).isGreaterThanOrEqualTo(mergesBefore + 2);
      database.transaction(() -> {
        assertThat(vertex(hub).countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(604);
        assertThat(vertex(k.get(2)).countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(4);
      });
    } finally {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    }
  }

  private void assertDanglingEdgeRefused(final boolean lightweight) throws Exception {
    final List<RID> k = createGraph(lightweight);
    final RID deletedChunk = compute(() -> ((VertexInternal) vertex(k.get(6))).getInEdgesHeadChunk());

    final Throwable failure = appendWhile(k.get(1), k.get(6), () -> {
      database.transaction(() -> vertex(k.get(6)).delete());
      database.transaction(() -> vertex(k.get(2)).newEdge("E", vertex(k.get(7))));
    });

    assertThat(compute(() -> ((VertexInternal) vertex(k.get(7))).getInEdgesHeadChunk()))
        .as("the first incoming chunk of k7 took the slot of the chunk freed with k6, which is the case of the issue")
        .isEqualTo(deletedChunk);
    assertThat(failure).isInstanceOf(ConcurrentModificationException.class).isInstanceOf(NeedRetryException.class);
    database.transaction(() -> {
      assertThat(neighbours(k.get(7), Vertex.DIRECTION.IN)).containsExactly(k.get(2));
      assertThat(neighbours(k.get(1), Vertex.DIRECTION.OUT)).isEmpty();
      assertThat(vertex(k.get(1)).countEdges(Vertex.DIRECTION.OUT, "E")).isZero();
    });
  }

  /** Vertices k0..k9 and the edge k2 -> k6, which gives k6 an incoming chunk with room left for a plain append. */
  private List<RID> createGraph(final boolean lightweight) {
    database.transaction(() -> {
      database.getSchema().createVertexType("T", 1).createProperty("k", Type.STRING);
      database.getSchema().buildEdgeType().withName("E").withTotalBuckets(1).withLightweight(lightweight).create();
    });
    final List<RID> k = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final String key = "k" + i;
      k.add(compute(() -> database.newVertex("T").set("k", key).save().getIdentity()));
    }
    database.transaction(() -> vertex(k.get(2)).newEdge("E", vertex(k.get(6))));
    return k;
  }

  /**
   * Creates {@code from -> to} in a transaction that commits after {@code meanwhile} ran its own transactions, and returns what
   * the commit threw, or null.
   */
  private Throwable appendWhile(final RID from, final RID to, final Runnable meanwhile) throws InterruptedException {
    final CountDownLatch appended = new CountDownLatch(1);
    final CountDownLatch othersCommitted = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread appender = new Thread(() -> {
      try {
        database.begin();
        try {
          vertex(from).newEdge("E", vertex(to));
        } finally {
          appended.countDown();
        }
        othersCommitted.await();
        database.commit();
      } catch (final Throwable e) {
        failure.set(e);
        if (database.isTransactionActive())
          database.rollback();
      }
    }, "appender");
    appender.start();
    appended.await();
    try {
      meanwhile.run();
    } finally {
      othersCommitted.countDown();
      appender.join();
    }
    return failure.get();
  }

  private <T> T compute(final Supplier<T> supplier) {
    final AtomicReference<T> result = new AtomicReference<>();
    database.transaction(() -> result.set(supplier.get()));
    return result.get();
  }

  private Vertex vertex(final RID rid) {
    return database.lookupByRID(rid, true).asVertex();
  }

  private List<RID> neighbours(final RID rid, final Vertex.DIRECTION direction) {
    final List<RID> result = new ArrayList<>();
    for (final Vertex v : vertex(rid).getVertices(direction, "E"))
      result.add(v.getIdentity());
    return result;
  }
}
