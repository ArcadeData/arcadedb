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
package com.arcadedb.index.sparsevector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regressions for #9209 (a query that starts between a memtable flush's swap and the segment's publication misses the
 * flushed postings) and #9210 (a query that starts while a flush or compaction runs waits for it to finish).
 * <p>
 * Both freeze the engine inside a mutation through its test hook, then query from another thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9209And9210SparseQueryDuringMutationTest extends TestHelper {

  private static final int DIM = 7;

  private static PaginatedSparseVectorEngine newEngine(final DatabaseInternal db, final String name) {
    return new PaginatedSparseVectorEngine(db, name, SegmentParameters.defaults());
  }

  private static void put(final PaginatedSparseVectorEngine engine, final long from, final long to) {
    for (long i = from; i < to; i++)
      engine.put(DIM, new RID(0, i), 1f);
  }

  private static List<RidScore> query(final PaginatedSparseVectorEngine engine, final int k) throws Exception {
    return engine.topK(new int[] { DIM }, new float[] { 1f }, k);
  }

  /** Runs {@code mutation} on its own thread, frozen at the hook until the returned latch is released. */
  private static final class FrozenMutation {
    final CountDownLatch    inside  = new CountDownLatch(1);
    final CountDownLatch    release = new CountDownLatch(1);
    CompletableFuture<Long> result;

    void hook() {
      inside.countDown();
      try {
        release.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Test
  void queryDuringFlushSeesTheMemtableBeingFlushed() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    try (final PaginatedSparseVectorEngine engine = newEngine(db, "Issue9209Flush")) {
      put(engine, 0, 50);
      engine.flush(); // one published segment, so the refresh has something to compare against
      put(engine, 50, 100);

      final FrozenMutation frozen = new FrozenMutation();
      engine.setMutationHookForTest(frozen::hook);
      frozen.result = CompletableFuture.supplyAsync(() -> {
        return engine.flush();
      });
      try {
        if (!frozen.inside.await(30, TimeUnit.SECONDS))
          frozen.result.get(1, TimeUnit.SECONDS); // surfaces why the flush never got there
        assertThat(frozen.inside.getCount()).as("the flush reached the hook").isZero();

        // The memtable is swapped out and its segment is not published yet: the answer must still hold all 100, and it
        // must come back while the flush is frozen rather than after it.
        final CompletableFuture<List<RidScore>> q = CompletableFuture.supplyAsync(() -> {
          try {
            return query(engine, 1000);
          } catch (final Exception e) {
            throw new IllegalStateException(e);
          }
        });
        assertThat(q.get(20, TimeUnit.SECONDS)).hasSize(100);
      } finally {
        frozen.release.countDown();
      }
      assertThat(frozen.result.get(30, TimeUnit.SECONDS)).isGreaterThan(0L);
      assertThat(query(engine, 1000)).hasSize(100);
    }
  }

  @Test
  void aFlushThatFailsKeepsItsPostingsForTheNextFlush() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    try (final PaginatedSparseVectorEngine engine = newEngine(db, "Issue9209Failure")) {
      put(engine, 0, 50);
      engine.setMutationHookForTest(() -> {
        throw new IllegalStateException("injected");
      });
      assertThatThrownBy(engine::flush).isInstanceOf(IllegalStateException.class);
      engine.setMutationHookForTest(null);

      // The postings of the failed flush stay readable, and the next flush persists them.
      put(engine, 100, 120);
      assertThat(query(engine, 1000)).hasSize(70);
      assertThat(engine.flush()).isGreaterThan(0L);
      assertThat(query(engine, 1000)).hasSize(70);
      assertThat(engine.flush()).isGreaterThan(0L); // the postings written since
      assertThat(query(engine, 1000)).hasSize(70);
    }
  }

  @Test
  void queryDuringCompactionDoesNotWaitForIt() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    try (final PaginatedSparseVectorEngine engine = newEngine(db, "Issue9210Compact")) {
      put(engine, 0, 50);
      engine.flush();
      put(engine, 50, 100);
      engine.flush();

      final FrozenMutation frozen = new FrozenMutation();
      engine.setMutationHookForTest(frozen::hook);
      frozen.result = CompletableFuture.supplyAsync(engine::compactAll);
      try {
        assertThat(frozen.inside.await(30, TimeUnit.SECONDS)).as("the compaction reached the hook").isTrue();

        // The compaction registered its output file and holds the mutator lock. The query must answer from the
        // published segments instead of queueing behind it.
        final CompletableFuture<List<RidScore>> q = CompletableFuture.supplyAsync(() -> {
          try {
            return query(engine, 1000);
          } catch (final Exception e) {
            throw new IllegalStateException(e);
          }
        });
        assertThat(q.get(20, TimeUnit.SECONDS)).hasSize(100);
      } finally {
        frozen.release.countDown();
      }
      assertThat(frozen.result.get(30, TimeUnit.SECONDS)).isGreaterThan(0L);
      assertThat(query(engine, 1000)).hasSize(100);
    }
  }
}
