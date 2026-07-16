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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Commutative edge-append merge (GRAPH_EDGE_APPEND_MERGE, new in 26.7.2) under concurrent ADD + REMOVE on the
 * same hot vertex, on the CLASSIC (non-striped) layout - i.e. the exact configuration of a 26.7.2 deployment.
 * <p>
 * The merge rebases a conflicting edge-list page by re-applying this transaction's tracked appends on top of
 * the current committed page. Correctness depends on every NON-append write to that page poisoning it first
 * (removal relink, chunk allocation, chunk delete). A gap there would let a rebase re-derive a page from
 * committed-state + appends and drop a concurrent removal's relink - producing a chain whose bytes no longer
 * parse, which surfaces later as a truncated read (BufferUnderflowException) on any traversal of the vertex.
 */
@Tag("slow")
class EdgeAppendMergeConcurrentStressTest extends TestHelper {
  private static final int THREADS         = 8;
  private static final int EDGES_PER_THREAD = 400;

  private int     savedThreshold;
  private boolean savedMerge;

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    savedMerge = GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.getValueAsBoolean();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.setValue(savedMerge);
  }

  @Test
  void concurrentAddAndRemoveOnHotVertexKeepsChainReadable() throws Exception {
    // 26.7.2 SHAPE: the striped super-node layout did not exist, so the hot vertex keeps ONE classic chain and
    // every writer contends on its head chunk page - which is precisely what the merge was built to rebase.
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(0);
    GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.setValue(true);

    database.transaction(() -> {
      database.getSchema().createVertexType("Account", 4).createProperty("number", Type.INTEGER);
      database.getSchema().createEdgeType("TRANSFERS", 8);
    });

    final RID[] hubHolder = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Account");
      hub.set("number", 0);
      hub.save();
      hubHolder[0] = hub.getIdentity();
    });
    final RID hubRID = hubHolder[0];

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final List<RID> added = Collections.synchronizedList(new ArrayList<>());
    final AtomicInteger removed = new AtomicInteger();
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();

    for (int t = 0; t < THREADS; t++) {
      final int threadId = t;
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < EDGES_PER_THREAD; i++) {
            final int n = threadId * EDGES_PER_THREAD + i;

            // ADD: a tracked, rebasable in-chunk append onto the hot vertex's head chunk.
            final RID[] srcHolder = new RID[1];
            database.transaction(() -> {
              final MutableVertex src = database.newVertex("Account");
              src.set("number", n + 1);
              src.save();
              src.newEdge("TRANSFERS", hubRID);
              srcHolder[0] = src.getIdentity();
            }, true, 50);
            added.add(srcHolder[0]);

            // REMOVE: a NON-commutative write (relink / possible chunk delete) that must poison the page,
            // racing the other threads' appends onto the same chunks.
            if (i % 7 == 3) {
              RID victim = null;
              synchronized (added) {
                if (added.size() > 20)
                  victim = added.remove(added.size() / 2);
              }
              if (victim != null) {
                final RID v = victim;
                database.transaction(() -> v.asVertex(true).delete(), true, 50);
                removed.incrementAndGet();
              }
            }

            // READ: traverse the hot vertex while it is being mutated - this is where a page whose bytes no
            // longer parse shows up as a BufferUnderflowException.
            if (i % 11 == 5)
              database.transaction(() -> hubRID.asVertex(true).countEdges(Vertex.DIRECTION.IN, "TRANSFERS"), true, 50);
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "writer-" + t);
      threads.add(thread);
      thread.start();
    }

    start.countDown();
    for (final Thread thread : threads)
      thread.join();

    if (!errors.isEmpty()) {
      for (final Throwable e : errors)
        e.printStackTrace();
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());
    }

    final int expected = THREADS * EDGES_PER_THREAD - removed.get();
    final long merges = ((DatabaseInternal) database).getPageManager().getStats().edgeAppendMerges;

    // The whole point of this test is the REBASE path: if no merge ever fired, a green result proves nothing.
    assertThat(merges).as("edge-append rebase must actually have fired").isGreaterThan(0);

    // THE CHAIN MUST STILL PARSE AND COUNT EXACTLY.
    database.transaction(() -> {
      assertThat(hubRID.asVertex(true).countEdges(Vertex.DIRECTION.IN, "TRANSFERS")).isEqualTo(expected);
      try (final ResultSet rs = database.query("SQL",
          "SELECT number, both().size() FROM Account ORDER BY both().size() DESC LIMIT 5")) {
        final Result top = rs.next();
        assertThat(top.<Number>getProperty("both().size()").intValue()).isEqualTo(expected);
      }
      // EVERY SURVIVING EDGE MUST STILL BE REACHABLE (no append lost to a rebase).
      int seen = 0;
      for (final Vertex ignored : hubRID.asVertex(true).getVertices(Vertex.DIRECTION.IN, "TRANSFERS"))
        seen++;
      assertThat(seen).isEqualTo(expected);
    });
  }
}
