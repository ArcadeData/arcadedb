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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #5147: concurrent edge insertion into the SAME super-node (hot) vertex must not drop
 * edges. The head chunk was read via an immutable lookup that (under READ_COMMITTED) did not retain the page,
 * and the page was only captured at the deferred {@code updateRecord} - at a newer version if a concurrent
 * append committed in between. The commit-time MVCC check then compared matching versions, found no conflict,
 * and the stale chunk buffer silently overwrote the concurrent append, so an edge record survived with no
 * back-reference from the hub (a lost update / dropped edge). {@code GraphEngine} now anchors the head chunk's
 * page in the transaction at read time, so the conflict is detected and the transaction retries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue5147SuperNodeChunkRaceTest extends TestHelper {

  private static final int THREADS          = 8;
  private static final int EDGES_PER_THREAD = 4000;
  private static final int TOTAL            = THREADS * EDGES_PER_THREAD;

  @Test
  void concurrentAppendsToSuperNodeDoNotLoseEdges() throws Exception {
    final int savedRetryDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    GlobalConfiguration.TX_RETRY_DELAY.setValue(1);
    try {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub", 1);
        // Spread the source vertices / edge records across many buckets so their insertion is not the
        // bottleneck: the contention we exercise is the hub's single edge-list head chunk.
        database.getSchema().createVertexType("Src", 16);
        database.getSchema().createEdgeType("LINK", 16);
      });
      final MutableVertex[] hubHolder = new MutableVertex[1];
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex("Hub");
        hub.save();
        hubHolder[0] = hub;
      });
      final RID hubRID = hubHolder[0].getIdentity();

      final AtomicLong committed = new AtomicLong();
      final CountDownLatch start = new CountDownLatch(1);
      final CountDownLatch done = new CountDownLatch(THREADS);
      for (int t = 0; t < THREADS; t++) {
        new Thread(() -> {
          try {
            start.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            done.countDown();
            return;
          }
          for (int i = 0; i < EDGES_PER_THREAD; i++) {
            database.transaction(() -> {
              final MutableVertex src = database.newVertex("Src");
              src.save();
              src.newEdge("LINK", hubRID);
            }, false, 10_000);
            committed.incrementAndGet();
          }
          done.countDown();
        }).start();
      }
      start.countDown();
      done.await();

      assertThat(committed.get()).isEqualTo(TOTAL);

      // Every committed edge must have its back-reference in the hub IN list (no lost update).
      final long[] inDegree = new long[1];
      database.transaction(() -> inDegree[0] = hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"));
      assertThat(inDegree[0]).isEqualTo(TOTAL);

      // And the integrity check must be clean (no missingReferenceBack). This is additionally enforced by the
      // TestHelper end-of-test "check database" assertion.
    } finally {
      GlobalConfiguration.TX_RETRY_DELAY.setValue(savedRetryDelay);
    }
  }
}
