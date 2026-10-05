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

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9208: threads deleting vertices that share no record failed with a
 * {@code ConcurrentModificationException} when their edge list segments shared pages, because neither the edge-append
 * merge nor the disjoint-slot merge could rebase a page that holds an edge removal or a deleted segment.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue9208ConcurrentEdgeDeleteTest extends TestHelper {
  private static final int THREADS = 4;
  private static final int PER_THREAD = 1500;
  private static final int HUBS = 100;

  @Test
  void concurrentDeletesOfVerticesSharingEdgeListPagesAreMerged() throws Exception {
    database.transaction(() -> {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
    });

    final RID[][] hubs = new RID[THREADS][HUBS];
    final RID[][] victims = new RID[THREADS][PER_THREAD];

    database.transaction(() -> {
      for (int i = 0; i < HUBS; i++)
        for (int t = 0; t < THREADS; t++)
          hubs[t][i] = database.newVertex("V").save().getIdentity();
    });

    final SplittableRandom random = new SplittableRandom(42);
    for (int start = 0; start < PER_THREAD; start += 500) {
      final int from = start;
      database.transaction(() -> {
        for (int i = from; i < Math.min(PER_THREAD, from + 500); i++)
          for (int t = 0; t < THREADS; t++) {
            victims[t][i] = database.newVertex("V").save().getIdentity();
            hubs[t][random.nextInt(HUBS)].asVertex().newEdge("E", victims[t][i]);
          }
      });
    }

    final AtomicLong failedToCaller = new AtomicLong();
    final long mergesBefore = PageManager.INSTANCE.getStats().txPageSlotMerges;

    final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < THREADS; t++) {
        final int thread = t;
        futures.add(pool.submit(() -> {
          for (int i = 0; i < PER_THREAD; i++) {
            final RID rid = victims[thread][i];
            try {
              database.transaction(() -> database.lookupByRID(rid, true).delete());
            } catch (final ConcurrentModificationException e) {
              failedToCaller.incrementAndGet();
            }
          }
        }));
      }
      for (final Future<?> f : futures)
        f.get(5, TimeUnit.MINUTES);
    } finally {
      pool.shutdownNow();
    }

    assertThat(failedToCaller.get()).as("deletes of unrelated vertices that failed after their retries").isZero();
    assertThat(PageManager.INSTANCE.getStats().txPageSlotMerges - mergesBefore).as("page conflicts rebased by the slot merge")
        .isPositive();
    assertThat(database.countType("V", false)).isEqualTo((long) THREADS * HUBS);
    for (int t = 0; t < THREADS; t++)
      for (int i = 0; i < HUBS; i++)
        assertThat(hubs[t][i].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isZero();
  }

  @Test
  void concurrentAppendsAndDeletesOnSharedPagesKeepEveryEdgeList() throws Exception {
    database.transaction(() -> {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
    });

    final int perThread = 600;
    final RID[][] hubs = new RID[THREADS][HUBS];
    final RID[][] victims = new RID[THREADS][perThread];
    final int[][] expected = new int[THREADS][HUBS];

    database.transaction(() -> {
      for (int i = 0; i < HUBS; i++)
        for (int t = 0; t < THREADS; t++)
          hubs[t][i] = database.newVertex("V").save().getIdentity();
    });
    final SplittableRandom random = new SplittableRandom(7);
    final int[][] hubOf = new int[THREADS][perThread];
    for (int start = 0; start < perThread; start += 300) {
      final int from = start;
      database.transaction(() -> {
        for (int i = from; i < Math.min(perThread, from + 300); i++)
          for (int t = 0; t < THREADS; t++) {
            victims[t][i] = database.newVertex("V").save().getIdentity();
            hubOf[t][i] = random.nextInt(HUBS);
            hubs[t][hubOf[t][i]].asVertex().newEdge("E", victims[t][i]);
            expected[t][hubOf[t][i]]++;
          }
      });
    }

    final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < THREADS; t++) {
        final int thread = t;
        futures.add(pool.submit(() -> {
          final SplittableRandom r = new SplittableRandom(thread);
          for (int idx = 0; idx < perThread; idx++) {
            final int i = idx;
            final int newHub = r.nextInt(HUBS);
            try {
              // One transaction both deletes a vertex (removing an edge) and appends a new edge to the same hub list
              database.transaction(() -> {
                database.lookupByRID(victims[thread][i], true).delete();
                final MutableVertex created = database.newVertex("V").save();
                hubs[thread][newHub].asVertex().newEdge("E", created);
              });
              expected[thread][hubOf[thread][i]]--;
              expected[thread][newHub]++;
            } catch (final ConcurrentModificationException e) {
              // The whole transaction was rolled back: nothing to account for
            }
          }
        }));
      }
      for (final Future<?> f : futures)
        f.get(5, TimeUnit.MINUTES);
    } finally {
      pool.shutdownNow();
    }

    for (int t = 0; t < THREADS; t++)
      for (int h = 0; h < HUBS; h++)
        assertThat(hubs[t][h].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).as("hub %d of thread %d", h, t)
            .isEqualTo(expected[t][h]);

    final ResultSet check = database.command("sql", "CHECK DATABASE");
    assertThat(check.hasNext()).isTrue();
    assertThat(((Number) check.next().getProperty("totalErrors")).longValue()).isZero();
  }
}
