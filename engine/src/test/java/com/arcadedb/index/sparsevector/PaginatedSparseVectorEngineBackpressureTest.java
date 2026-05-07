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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Backpressure regression for {@link PaginatedSparseVectorEngine#put} (Tier 2 follow-up to
 * #4068). When the memtable grows past 2x the flush threshold, {@code put} must briefly take
 * the engine's mutator lock so an in-progress flush has a chance to swap the memtable out
 * before the put adds another entry. Without this, sustained write rate exceeding flush rate
 * lets the memtable grow unbounded between {@link PaginatedSparseVectorEngine#maybeFlush}
 * calls and eventually OOMs.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PaginatedSparseVectorEngineBackpressureTest extends TestHelper {

  /**
   * Below the hard limit, put must not block - the hot path must be lock-free. Sanity baseline
   * before the contended-lock test below.
   */
  @Test
  void putIsLockFreeBelowHardLimit() {
    final DatabaseInternal db = (DatabaseInternal) database;
    try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine(
        db, "BackpressureNoOpTest", SegmentParameters.defaults(), /* threshold */ 100L)) {
      final ReentrantLock mutatorLock = engine.mutatorLockForTest();
      // Hold the mutator lock from this thread; a put below the hard limit must not even try
      // to acquire it, so the call must return without observable delay.
      mutatorLock.lock();
      try {
        final long t0 = System.nanoTime();
        engine.put(0, new RID(0, 1L), 0.5f);
        final long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
        assertThat(engine.memtablePostings()).isEqualTo(1L);
        assertThat(elapsedMs)
            .as("put below the hard limit must not contend the mutator lock")
            .isLessThan(50L);
      } finally {
        mutatorLock.unlock();
      }
    }
  }

  /**
   * Once the memtable crosses the hard limit (2x the flush threshold), put must block on the
   * mutator lock so any in-progress flush completes before the put adds another entry. We
   * simulate "flush in progress" by holding the lock from a worker thread for a known wait,
   * then verify the put thread observed at least that wait.
   */
  @Test
  @Tag("slow")
  void putBlocksOnMutatorLockOnceMemtableExceedsHardLimit() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final long flushThreshold = 5L;
    try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine(
        db, "BackpressureBlockTest", SegmentParameters.defaults(), flushThreshold)) {
      // Fill the memtable above the hard limit (= 2 * flushThreshold = 10) without contending the
      // lock. The pre-fill itself goes through the same put path; backpressure on those calls is
      // a no-op because the lock is uncontended.
      for (int i = 0; i < 12; i++)
        engine.put(0, new RID(0, 100L + i), 0.1f * (i + 1));
      assertThat(engine.memtablePostings()).isGreaterThanOrEqualTo(2 * flushThreshold);

      final ReentrantLock mutatorLock = engine.mutatorLockForTest();
      final CountDownLatch lockHeld = new CountDownLatch(1);
      final CountDownLatch releaseSignal = new CountDownLatch(1);
      final long holdMs = 200L;

      // Worker thread takes the lock and holds it for `holdMs`, simulating a slow flush.
      final Thread holder = new Thread(() -> {
        mutatorLock.lock();
        try {
          lockHeld.countDown();
          try {
            releaseSignal.await(2L, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        } finally {
          mutatorLock.unlock();
        }
      }, "backpressure-lock-holder");
      holder.setDaemon(true);
      holder.start();

      assertThat(lockHeld.await(2L, TimeUnit.SECONDS))
          .as("worker thread should acquire the mutator lock").isTrue();

      // The next put must observe contention because the memtable is over hardLimit. Time it.
      final AtomicLong elapsedMs = new AtomicLong();
      final Thread putter = new Thread(() -> {
        final long t0 = System.nanoTime();
        engine.put(1, new RID(0, 9999L), 0.42f);
        elapsedMs.set((System.nanoTime() - t0) / 1_000_000L);
      }, "backpressure-putter");
      putter.start();

      // Let the put queue on the lock for a portion of the hold window, then release.
      Thread.sleep(holdMs);
      releaseSignal.countDown();
      putter.join(2_000L);

      assertThat(putter.isAlive()).as("put must have unblocked once the lock was released").isFalse();
      assertThat(elapsedMs.get())
          .as("put crossing the hard limit must have waited at least the hold window")
          .isGreaterThanOrEqualTo(holdMs - 50L); // small tolerance for scheduling jitter
      assertThat(engine.memtablePostings())
          .as("the put eventually landed in the memtable")
          .isEqualTo(13L);
    }
  }

}
