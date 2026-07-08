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
import com.arcadedb.engine.PageManager;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Type;
import com.sun.management.ThreadMXBean;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures the write-contention cost of many threads concurrently inserting an edge into the SAME super-node
 * (hub) vertex, the way a payments graph funnels transactions into a central account. Each transaction creates
 * its own source vertex + edge record (spread across many buckets, so record insertion is NOT the bottleneck)
 * and appends the edge to the hub's shared IN edge-list head chunk. Page-level MVCC turns each concurrent
 * append to that one chunk into a {@link com.arcadedb.exception.ConcurrentModificationException}.
 * <p>
 * With the commutative edge-append merge the appends are replayed on the newer chunk version instead of
 * failing the whole transaction, so the number of FULL-transaction retries (re-running the vertex + edge
 * creation, and under HA the whole Raft round) collapses. The benchmark reports those retries plus the number
 * of resolved merges.
 * <p>
 * Tagged {@code benchmark} so it is skipped from regular CI builds. Run explicitly with:
 * {@code mvn -pl engine test -Dtest=SuperNodeConcurrentAppendBenchmark -DexcludedGroups=}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class SuperNodeConcurrentAppendBenchmark extends TestHelper {

  private static final int THREADS          = 8;
  private static final int EDGES_PER_THREAD = 4000;
  private static final int TOTAL_EDGES      = THREADS * EDGES_PER_THREAD;
  private static final int BUCKETS          = 16;
  private static final int MAX_RETRIES      = 10_000;

  /**
   * Single-threaded, zero-contention edge append. There are no conflicts and therefore no merges, so the only
   * cost the append-merge adds here is its per-append TRACKING. Reports bytes allocated per edge (via the
   * HotSpot per-thread allocation counter) so the tracking overhead can be compared on vs off:
   * {@code -Darcadedb.graph.edgeAppendMerge=false}. After the allocation-free rework (segment-keyed, primitive
   * buffers, lazy PageId) the on/off allocation-per-edge should be essentially identical.
   */
  @Test
  void singleThreadedAppendTrackingOverhead() {
    final int edges = 200_000;
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub", 1);
      database.getSchema().createVertexType("Src", BUCKETS);
      database.getSchema().createEdgeType("LINK", BUCKETS);
    });
    final MutableVertex[] hubHolder = new MutableVertex[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Hub");
      hub.save();
      hubHolder[0] = hub;
    });
    final RID hubRID = hubHolder[0].getIdentity();

    final ThreadMXBean threadBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final long tid = Thread.currentThread().threadId();

    // One big transaction: every edge appends to the hub's IN head chunk (tracked) with no other thread, so no
    // conflict, no merge - pure tracking cost.
    final long allocBefore = threadBean.getThreadAllocatedBytes(tid);
    final long begin = System.currentTimeMillis();
    database.transaction(() -> {
      for (int i = 0; i < edges; i++) {
        final MutableVertex src = database.newVertex("Src");
        src.save();
        src.newEdge("LINK", hubRID);
      }
    });
    final long elapsed = System.currentTimeMillis() - begin;
    final long allocated = threadBean.getThreadAllocatedBytes(tid) - allocBefore;

    final String report = """
        ======== Single-threaded append tracking overhead ========
        append-merge enabled : %b
        edges                : %d
        elapsed              : %d ms
        throughput           : %.0f edges/s
        allocated total      : %.1f MB
        allocated / edge     : %d bytes
        ==========================================================""".formatted(
        GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.getValueAsBoolean(), edges, elapsed,
        edges / (elapsed / 1000.0), allocated / (1024.0 * 1024.0), allocated / edges);
    LogManager.instance().log(this, Level.INFO, report);
    try {
      Files.writeString(new File("./target/supernode-append-overhead.txt").toPath(),
          report + System.lineSeparator(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } catch (final IOException ignore) {
      // best-effort reporting only
    }
  }

  @Test
  void concurrentAppendToSuperNode() throws InterruptedException {
    // Keep retry back-off tiny so the measurement reflects wasted work (conflicts), not sleep time.
    final int savedRetryDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    GlobalConfiguration.TX_RETRY_DELAY.setValue(1);
    try {
      database.transaction(() -> {
        final var hub = database.getSchema().createVertexType("Hub", 1);
        hub.createProperty("name", Type.STRING);
        // Spread the per-transaction source vertices and edge records across many buckets so their INSERTION
        // is not the dominant conflict: what we want to measure is contention on the hub's single edge list.
        database.getSchema().createVertexType("Src", BUCKETS);
        database.getSchema().createEdgeType("LINK", BUCKETS);
      });

      final RID hubRID;
      {
        final MutableVertex[] hubHolder = new MutableVertex[1];
        database.transaction(() -> {
          final MutableVertex hub = database.newVertex("Hub");
          hub.set("name", "treasury");
          hub.save();
          hubHolder[0] = hub;
        });
        hubRID = hubHolder[0].getIdentity();
      }

      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final long conflictsBefore = pageManager.getStats().concurrentModificationExceptions;
      final long mergesBefore = pageManager.getStats().edgeAppendMerges;

      final AtomicLong committed = new AtomicLong();
      final AtomicLong attempts = new AtomicLong();
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
            database.transaction(() -> {
              attempts.incrementAndGet();
              final MutableVertex src = database.newVertex("Src");
              src.set("thread", threadId);
              src.save();
              src.newEdge("LINK", hubRID);
            }, false, MAX_RETRIES);
            committed.incrementAndGet();
          }
          done.countDown();
        }, "append-worker-" + threadId).start();
      }

      final long begin = System.currentTimeMillis();
      start.countDown();
      done.await();
      final long elapsed = System.currentTimeMillis() - begin;

      final long conflicts = pageManager.getStats().concurrentModificationExceptions - conflictsBefore;
      final long merges = pageManager.getStats().edgeAppendMerges - mergesBefore;
      final long retries = attempts.get() - committed.get();

      final long[] inDegree = new long[1];
      database.transaction(() -> inDegree[0] = hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"));

      final String report = """
          ======== Super-node concurrent-append benchmark ========
          append-merge enabled : %b
          threads              : %d
          edges committed      : %d (target %d)
          hub IN-degree        : %d
          tx attempts          : %d
          full-tx retries      : %d  (%.1f%% of commits)
          append merges        : %d
          MVCC conflicts       : %d
          elapsed              : %d ms
          throughput           : %.0f edges/s
          ========================================================"""
          .formatted(GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.getValueAsBoolean(), THREADS, committed.get(), TOTAL_EDGES,
              inDegree[0], attempts.get(), retries, 100.0 * retries / TOTAL_EDGES, merges, conflicts, elapsed,
              TOTAL_EDGES / (elapsed / 1000.0));
      LogManager.instance().log(this, Level.INFO, report);
      // Persist too: the test log level is SEVERE, so the INFO report above is invisible in a normal run.
      try {
        final File out = new File("./target/supernode-append-benchmark.txt");
        Files.writeString(out.toPath(), report + System.lineSeparator(),
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
      } catch (final IOException ignore) {
        // best-effort reporting only
      }

      assertThat(committed.get()).isEqualTo(TOTAL_EDGES);
      // Every committed edge survives (#5147/#5153 fixed: no lost update on the hub head chunk).
      assertThat(inDegree[0]).isEqualTo(TOTAL_EDGES);
    } finally {
      GlobalConfiguration.TX_RETRY_DELAY.setValue(savedRetryDelay);
    }
  }
}
