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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PageManager;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * HA (3-node Raft) counterpart of {@code SuperNodeConcurrentAppendBenchmark}. Many threads concurrently insert
 * an edge into the SAME super-node (hub) vertex through the LEADER, the way a payments cluster funnels
 * transactions into a central account. Every append to the hub's shared IN edge-list head chunk collides under
 * page-level MVCC; without the commutative edge-append merge each collision fails the whole transaction and the
 * retry re-runs the ENTIRE Raft replication round (this is exactly the timeout cascade reported in the field).
 * With the merge the appends are replayed on the newer chunk version on the leader and the merged page is
 * replicated once, so the number of full-transaction (re-replicated) retries collapses.
 * <p>
 * Reports full-tx retries, leader-side append merges (from the JVM-wide {@link PageManager} stats), per-commit
 * latency, throughput, and verifies every replica converges to the same hub degree.
 * <p>
 * Tagged {@code benchmark} so it is skipped from regular CI builds. Run explicitly, e.g.:
 * {@code mvn -pl ha-raft test -Dtest=SuperNodeConcurrentAppendHABenchmark -DexcludedGroups=}
 * and again with {@code -Darcadedb.graph.edgeAppendMerge=false} for the baseline.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class SuperNodeConcurrentAppendHABenchmark extends BaseRaftHATest {

  private static final int THREADS          = 8;
  private static final int EDGES_PER_THREAD = 500;
  private static final int TOTAL_EDGES      = THREADS * EDGES_PER_THREAD;
  private static final int BUCKETS          = 16;
  private static final int MAX_RETRIES      = 10_000;

  @Override
  protected int getServerCount() {
    return 3;
  }

  // Note: the base class also runs checkDatabasesAreIdentical() at teardown, verifying the three replicas
  // converged to the SAME state (the leader replicates its merged pages, so followers apply them verbatim).
  // The merge's own correctness is gated by the embedded ConcurrentEdgeAppendMergeTest; the #5147 lost-update
  // fix (folded into this PR) means the super-node workload now keeps every committed edge.

  @Test
  void concurrentAppendToSuperNodeUnderHA() throws InterruptedException {
    // Toggle the feature from a plain system property so the baseline run is reliable regardless of how the
    // surefire fork forwards -D flags: -DedgeAppendMerge=false for the OFF comparison.
    // Control switch: -DsuperNode=false makes each edge target a fresh per-transaction vertex instead of the
    // shared hub, removing ALL edge-list contention. If throughput is the same as the super-node run, the
    // ceiling is HA replication itself, not the hot vertex.
    final boolean superNode = Boolean.parseBoolean(System.getProperty("superNode", "true"));
    final boolean appendMerge = Boolean.parseBoolean(System.getProperty("edgeAppendMerge", "true"));
    final boolean savedMerge = GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.getValueAsBoolean();
    GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.setValue(appendMerge);
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
        leaderDB.command("sql", "CREATE VERTEX TYPE Src BUCKETS " + BUCKETS);
        leaderDB.command("sql", "CREATE EDGE TYPE LINK BUCKETS " + BUCKETS);
      });

      final RID[] hubHolder = new RID[1];
      leaderDB.transaction(() -> {
        final MutableVertex hub = leaderDB.newVertex("Hub");
        hub.set("name", "treasury");
        hub.save();
        hubHolder[0] = hub.getIdentity();
      });
      final RID hubRID = hubHolder[0];
      waitForReplicationIsCompleted(leaderIndex);

      final PageManager pageManager = ((DatabaseInternal) leaderDB).getPageManager();
      final long conflictsBefore = pageManager.getStats().concurrentModificationExceptions;
      final long mergesBefore = pageManager.getStats().edgeAppendMerges;

      final AtomicLong committed = new AtomicLong();
      final AtomicLong attempts = new AtomicLong();
      final AtomicLong totalLatencyNs = new AtomicLong();
      final AtomicLong maxLatencyNs = new AtomicLong();
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
            final long t0 = System.nanoTime();
            leaderDB.transaction(() -> {
              attempts.incrementAndGet();
              final MutableVertex src = leaderDB.newVertex("Src");
              src.set("thread", threadId);
              src.save();
              if (superNode)
                src.newEdge("LINK", hubRID);
              else {
                final MutableVertex dst = leaderDB.newVertex("Src");
                dst.save();
                src.newEdge("LINK", dst);
              }
            }, false, MAX_RETRIES);
            final long dt = System.nanoTime() - t0;
            committed.incrementAndGet();
            totalLatencyNs.addAndGet(dt);
            maxLatencyNs.accumulateAndGet(dt, Math::max);
          }
          done.countDown();
        }, "ha-append-worker-" + threadId).start();
      }

      final long begin = System.currentTimeMillis();
      start.countDown();
      done.await();
      final long elapsed = System.currentTimeMillis() - begin;

      final long conflicts = pageManager.getStats().concurrentModificationExceptions - conflictsBefore;
      final long merges = pageManager.getStats().edgeAppendMerges - mergesBefore;
      final long retries = attempts.get() - committed.get();

      waitForReplicationIsCompleted(leaderIndex);

      // Every replica must converge to the same hub degree, and to the exact committed total (#5147 fixed).
      final long[] perServerDegree = new long[getServerCount()];
      if (superNode)
        for (int s = 0; s < getServerCount(); s++) {
          final Database db = getServerDatabase(s, getDatabaseName());
          final long[] deg = new long[1];
          db.transaction(() -> deg[0] = hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"));
          perServerDegree[s] = deg[0];
        }

      final String report = """
          ======== Super-node concurrent-append HA benchmark (3 nodes) ========
          workload             : %s
          append-merge enabled : %b
          threads              : %d
          edges committed       : %d (target %d)
          per-server IN-degree  : %s
          tx attempts          : %d
          full-tx retries      : %d  (%.1f%% of commits, each a re-replicated Raft round)
          leader append merges : %d
          MVCC conflicts       : %d
          elapsed              : %d ms
          throughput           : %.0f edges/s
          avg commit latency   : %.2f ms
          max commit latency   : %.2f ms
          =====================================================================""".formatted(
          superNode ? "super-node (shared hub)" : "control (distinct targets)",
          GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.getValueAsBoolean(), THREADS, committed.get(), TOTAL_EDGES,
          java.util.Arrays.toString(perServerDegree), attempts.get(), retries, 100.0 * retries / TOTAL_EDGES, merges,
          conflicts, elapsed, TOTAL_EDGES / (elapsed / 1000.0), totalLatencyNs.get() / 1e6 / TOTAL_EDGES,
          maxLatencyNs.get() / 1e6);
      LogManager.instance().log(this, Level.INFO, report);
      try {
        final java.io.File out = new java.io.File("./target/supernode-append-ha-benchmark.txt");
        java.nio.file.Files.writeString(out.toPath(), report + System.lineSeparator(),
            java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
      } catch (final java.io.IOException ignore) {
        // best-effort reporting only
      }

      assertThat(committed.get()).isEqualTo(TOTAL_EDGES);
      if (superNode) {
        // All replicas agree (replication is consistent) ...
        for (int s = 1; s < getServerCount(); s++)
          assertThat(perServerDegree[s]).as("server %d degree vs leader", s).isEqualTo(perServerDegree[0]);
        // ... and every committed edge survived (#5147 fixed: no lost update on the hub head chunk).
        assertThat(perServerDegree[0]).isEqualTo(TOTAL_EDGES);
      }
    } finally {
      GlobalConfiguration.TX_RETRY_DELAY.setValue(savedRetryDelay);
      GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.setValue(savedMerge);
    }
  }
}
