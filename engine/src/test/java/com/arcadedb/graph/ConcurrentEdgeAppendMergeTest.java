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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Correctness of the commutative edge-append merge (GRAPH_EDGE_APPEND_MERGE): many threads appending edges to
 * one super-node must all survive with no lost/duplicated edge and a clean integrity check.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class ConcurrentEdgeAppendMergeTest extends TestHelper {

  private static final int THREADS          = 8;
  private static final int EDGES_PER_THREAD = 4000;
  private static final int TOTAL            = THREADS * EDGES_PER_THREAD;

  @Test
  void concurrentAppendsAllSurviveAndIntegrityIsClean() throws InterruptedException {
    final int savedRetryDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    GlobalConfiguration.TX_RETRY_DELAY.setValue(1);
    try {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub").createProperty("name", Type.STRING);
        database.getSchema().createVertexType("Spoke");
        database.getSchema().createEdgeType("LINK");
      });

      final MutableVertex[] hubHolder = new MutableVertex[1];
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex("Hub");
        hub.set("name", "treasury");
        hub.save();
        hubHolder[0] = hub;
      });
      final RID hubRID = hubHolder[0].getIdentity();

      final AtomicLong failures = new AtomicLong();
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
            try {
              database.transaction(() -> {
                final MutableVertex spoke = database.newVertex("Spoke");
                spoke.save();
                spoke.newEdge("LINK", hubRID);
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

      // Every appended edge survives, exactly once.
      final long[] inDegree = new long[1];
      database.transaction(() -> inDegree[0] = hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"));
      assertThat(inDegree[0]).isEqualTo(TOTAL);

      // Integrity check must be clean: no auto-fixed problems and no detected errors on any bucket.
      try (final ResultSet rs = database.command("sql", "check database")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat((Long) row.getProperty("autoFix")).as("check database: " + row.toJSON()).isEqualTo(0L);
          assertThat((Long) row.getProperty("totalErrors")).as("check database: " + row.toJSON()).isEqualTo(0L);
        }
      }
    } finally {
      GlobalConfiguration.TX_RETRY_DELAY.setValue(savedRetryDelay);
    }
  }

  /**
   * Stresses the poison guards AND the #5153 removal-path anchor: each transaction, on the same hot vertex,
   * BOTH appends a new edge (a tracked, rebasable operation) AND deletes a pre-existing one (a non-commutative
   * edge-list update that must poison its page and, on the remove path, anchor the modified chunk). If a
   * remove failed to poison a page carrying a tracked append, the commit-time rebase would drop it; if the
   * removal path failed to anchor, a concurrent modification of the same chunk would silently overwrite it.
   * Either way an edge count would drift. Net degree is invariant (one add + one remove per iteration), so the
   * final IN-degree must equal the pre-created pool size and the integrity check must be clean.
   */
  @Test
  void concurrentAppendsAndRemovesStayConsistent() throws InterruptedException {
    final int threads = 6;
    final int perThread = 500;
    final int pool = threads * perThread; // exactly one removal per iteration drains the pool

    final int savedRetryDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    GlobalConfiguration.TX_RETRY_DELAY.setValue(1);
    try {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub", 1);
        database.getSchema().createVertexType("Src", 16);
        database.getSchema().createEdgeType("LINK", 16);
      });
      final MutableVertex[] hubHolder = new MutableVertex[1];
      database.transaction(() -> {
        hubHolder[0] = database.newVertex("Hub");
        hubHolder[0].save();
      });
      final RID hubRID = hubHolder[0].getIdentity();

      // Pre-create the removable pool: `pool` edges into the hub, collecting their RIDs.
      final java.util.concurrent.ConcurrentLinkedQueue<RID> removable = new java.util.concurrent.ConcurrentLinkedQueue<>();
      for (int i = 0; i < pool; i++) {
        final RID[] edgeHolder = new RID[1];
        database.transaction(() -> {
          final MutableVertex src = database.newVertex("Src");
          src.save();
          edgeHolder[0] = src.newEdge("LINK", hubRID).getIdentity();
        });
        removable.add(edgeHolder[0]);
      }

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
            final RID toRemove = removable.poll();
            try {
              database.transaction(() -> {
                if (toRemove != null)
                  toRemove.asEdge().delete();
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

      final long[] inDegree = new long[1];
      database.transaction(() -> inDegree[0] = hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"));
      assertThat(inDegree[0]).isEqualTo(pool);

      try (final ResultSet rs = database.command("sql", "check database")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat((Long) row.getProperty("autoFix")).as("check database: " + row.toJSON()).isEqualTo(0L);
          assertThat((Long) row.getProperty("totalErrors")).as("check database: " + row.toJSON()).isEqualTo(0L);
        }
      }
    } finally {
      GlobalConfiguration.TX_RETRY_DELAY.setValue(savedRetryDelay);
    }
  }
}
