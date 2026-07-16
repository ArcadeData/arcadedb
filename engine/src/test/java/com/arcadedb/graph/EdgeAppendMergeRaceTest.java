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
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Adversarial race between the commutative edge-append merge (GRAPH_EDGE_APPEND_MERGE, new in 26.7.2) and the
 * structural writes it must never rebase over: edge REMOVAL (chunk relink + empty-chunk delete) on the classic
 * layout, with dedicated adder / remover / reader threads all hitting one hot vertex at once.
 * <p>
 * If a structural write ever failed to poison its page, a concurrent rebase would re-derive that page from
 * committed-state + appends and drop the relink, leaving a chain whose bytes no longer parse - the truncated
 * read surfaces as java.nio.BufferUnderflowException on the next traversal (the shape reported in #565).
 */
@Tag("slow")
class EdgeAppendMergeRaceTest extends TestHelper {
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
  void addersRemoversAndReadersOnOneHotVertex() throws Exception {
    // 26.7.2 SHAPE: no striped layout, so every writer contends on the hot vertex's single head chunk.
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

    final int ADDERS = 8, REMOVERS = 4, READERS = 4, PER_ADDER = 1500;

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final ConcurrentLinkedQueue<RID> live = new ConcurrentLinkedQueue<>();
    final AtomicInteger addedCount = new AtomicInteger();
    final AtomicInteger removedCount = new AtomicInteger();
    final AtomicBoolean addersDone = new AtomicBoolean();
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();

    for (int t = 0; t < ADDERS; t++) {
      final int id = t;
      threads.add(new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < PER_ADDER; i++) {
            final int n = id * PER_ADDER + i;
            final RID[] holder = new RID[1];
            database.transaction(() -> {
              final MutableVertex src = database.newVertex("Account");
              src.set("number", n + 1);
              src.save();
              src.newEdge("TRANSFERS", hubRID);
              holder[0] = src.getIdentity();
            }, true, 100);
            // Only half are ever eligible for removal: the chain must stay long (many chunks) while the
            // removers churn it, instead of draining to empty.
            if ((n & 1) == 0)
              live.add(holder[0]);
            addedCount.incrementAndGet();
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "adder-" + t));
    }

    // REMOVERS: delete source vertices, which relinks (and may delete) chunks in the hub's chain while the
    // adders are appending to it - the structural-vs-append race the merge must never smooth over.
    for (int t = 0; t < REMOVERS; t++) {
      threads.add(new Thread(() -> {
        try {
          start.await();
          while (!addersDone.get() || !live.isEmpty()) {
            final RID victim = live.poll();
            if (victim == null) {
              Thread.yield();
              continue;
            }
            database.transaction(() -> victim.asVertex(true).delete(), true, 100);
            removedCount.incrementAndGet();
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "remover-" + t));
    }

    // READERS: traverse the hot vertex continuously; a chain whose bytes stopped parsing shows up here.
    for (int t = 0; t < READERS; t++) {
      threads.add(new Thread(() -> {
        try {
          start.await();
          while (!addersDone.get()) {
            database.transaction(() -> {
              final Vertex hub = hubRID.asVertex(true);
              hub.countEdges(Vertex.DIRECTION.IN, "TRANSFERS");
              for (final Vertex ignored : hub.getVertices(Vertex.DIRECTION.IN, "TRANSFERS")) {
                // full traversal: forces every chunk in the chain to be parsed
              }
            }, true, 100);
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "reader-" + t));
    }

    threads.forEach(Thread::start);
    start.countDown();
    for (int i = 0; i < ADDERS; i++)
      threads.get(i).join();
    addersDone.set(true);
    for (final Thread thread : threads)
      thread.join();

    final long merges = ((DatabaseInternal) database).getPageManager().getStats().edgeAppendMerges;

    if (!errors.isEmpty()) {
      errors.getFirst().printStackTrace();
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());
    }
    assertThat(merges).as("edge-append rebase must actually have fired").isGreaterThan(0);

    final int expected = addedCount.get() - removedCount.get();
    database.transaction(() -> {
      assertThat(hubRID.asVertex(true).countEdges(Vertex.DIRECTION.IN, "TRANSFERS")).isEqualTo(expected);
      int seen = 0;
      for (final Vertex ignored : hubRID.asVertex(true).getVertices(Vertex.DIRECTION.IN, "TRANSFERS"))
        seen++;
      assertThat(seen).as("every surviving edge still reachable").isEqualTo(expected);
    });
  }
}
