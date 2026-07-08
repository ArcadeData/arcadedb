/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.server.ArcadeDBServer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Deterministic HA (3-node Raft) correctness gate for the commutative edge-append merge: many threads append
 * edges to the SAME super-node through the leader, and afterwards every replica must hold the exact same,
 * complete edge list. This protects the "the leader replicates the merged page, so replicas stay identical"
 * claim in CI (the {@code SuperNodeConcurrentAppendHABenchmark} exercises the same path but is benchmark-tagged
 * and measures throughput). Small on purpose so it can gate every build. Also covers the #5147/#5153
 * lost-update fixes under replication: no committed edge may be dropped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SuperNodeAppendHAConsistencyIT extends BaseRaftHATest {

  private static final int THREADS          = 6;
  private static final int EDGES_PER_THREAD = 200;
  private static final int TOTAL_EDGES      = THREADS * EDGES_PER_THREAD;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void concurrentSuperNodeAppendsReplicateWithoutLoss() throws InterruptedException {
    final int savedRetryDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    GlobalConfiguration.TX_RETRY_DELAY.setValue(1);
    try {
      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
          .until(() -> findLeaderIndex() >= 0);
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).as("a leader must be elected").isGreaterThanOrEqualTo(0);
      final ArcadeDBServer leaderServer = getServer(leaderIndex);
      final Database leaderDB = leaderServer.getDatabase(getDatabaseName());

      leaderDB.transaction(() -> {
        leaderDB.command("sql", "CREATE VERTEX TYPE Hub BUCKETS 1");
        leaderDB.command("sql", "CREATE VERTEX TYPE Src BUCKETS 8");
        leaderDB.command("sql", "CREATE EDGE TYPE LINK BUCKETS 8");
      });
      final RID[] hubHolder = new RID[1];
      leaderDB.transaction(() -> {
        final MutableVertex hub = leaderDB.newVertex("Hub");
        hub.save();
        hubHolder[0] = hub.getIdentity();
      });
      final RID hubRID = hubHolder[0];
      waitForReplicationIsCompleted(leaderIndex);

      final AtomicLong committed = new AtomicLong();
      final CountDownLatch start = new CountDownLatch(1);
      final CountDownLatch done = new CountDownLatch(THREADS);
      for (int t = 0; t < THREADS; t++) {
        final int threadId = t;
        new Thread(() -> {
          try {
            start.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            done.countDown();
            return;
          }
          for (int i = 0; i < EDGES_PER_THREAD; i++) {
            leaderDB.transaction(() -> {
              final MutableVertex src = leaderDB.newVertex("Src");
              src.set("thread", threadId);
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

      assertThat(committed.get()).isEqualTo(TOTAL_EDGES);
      waitForReplicationIsCompleted(leaderIndex);

      // Every replica holds the exact, complete edge list (no lost update, replicas identical).
      for (int s = 0; s < getServerCount(); s++) {
        final Database db = getServerDatabase(s, getDatabaseName());
        final long[] deg = new long[1];
        db.transaction(() -> deg[0] = hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"));
        assertThat(deg[0]).as("server %d hub IN-degree", s).isEqualTo(TOTAL_EDGES);
      }
    } finally {
      GlobalConfiguration.TX_RETRY_DELAY.setValue(savedRetryDelay);
    }
  }
}
