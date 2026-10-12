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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9745: the windowed {@code SELECT count(*) FROM T WHERE ts >= X} is answered by
 * {@code TimeSeriesEngine.aggregateMulti}, which reads a shard's sealed blocks and its mutable bucket inside one
 * compaction read-lock window. That window is only consistent if NOTHING changes either layer while a reader holds it,
 * which means every operation that publishes into the sealed store or clears the mutable bucket has to do it under the
 * same lock's write side.
 * <p>
 * Issue #9716's test pins this for the compaction's Phase 4c (swap then clear). This one pins it for every other
 * publisher a standalone database runs: {@code COMPACT TIMESERIES TYPE} is a compaction followed by a merge of small
 * blocks, and the maintenance scheduler adds retention and downsampling. Each operation is started while the test
 * holds the shard's compaction read lock - exactly what a running aggregation holds - and must leave both layers and the
 * windowed count unchanged until that lock is released. A publisher that skipped the write lock would finish (or
 * publish) inside the window, and the layers read under it would no longer match the snapshot.
 * <p>
 * One shard on purpose: the lock under test is per shard, and with one shard the aggregation reads it on the calling
 * thread, which is what lets this thread re-enter the read lock it already holds to take the windowed count.
 */
class Issue9745LayerPublishersWaitForReadersTest extends TestHelper {

  private static final long T0              = 1_767_225_600_000L;
  private static final int  ROWS_PER_BLOCK  = 1_000;
  private static final int  SEALED_ROUNDS   = 3;
  private static final int  MUTABLE_ROWS    = 500;
  private static final long WINDOW_START_TS = T0 + 500_000L;

  @Test
  void compactionWaitsForAReaderHoldingTheCompactionReadLock() throws Exception {
    fill();
    final State after = runWhileAReaderHoldsTheLock(TimeSeriesEngine::compactAll);
    assertThat(after.mutableSamples()).as("the compaction moved the mutable rows once the reader let go").isZero();
    assertThat(after.totalSamples()).isEqualTo(totalRows());
  }

  @Test
  void mergeOfSmallBlocksWaitsForAReaderHoldingTheCompactionReadLock() throws Exception {
    fill();
    final int blocksBefore = shard().getSealedStore().getBlockCount();
    assertThat(blocksBefore).as("one small block per compaction round").isGreaterThan(1);
    final State after = runWhileAReaderHoldsTheLock(engine -> engine.mergeSmallBlocks(1));
    assertThat(after.sealedBlocks()).as("the merge folded the small blocks once the reader let go").isLessThan(blocksBefore);
    assertThat(after.totalSamples()).isEqualTo(totalRows());
  }

  @Test
  void retentionWaitsForAReaderHoldingTheCompactionReadLock() throws Exception {
    fill();
    final long sealedBefore = shard().getSealedStore().getTotalSampleCount();
    // The cutoff is the first timestamp of the second round, so it is past every sample of the first round's block (the
    // last one is a second earlier) and before every other block: retention drops whole blocks whose samples are all
    // older than the cutoff, which here is exactly that one
    final State after = runWhileAReaderHoldsTheLock(engine -> engine.applyRetention(T0 + ROWS_PER_BLOCK * 1000L));
    assertThat(after.sealedSamples()).as("retention dropped the oldest block once the reader let go")
        .isEqualTo(sealedBefore - ROWS_PER_BLOCK);
    assertThat(after.mutableSamples()).as("retention does not touch the mutable bucket").isEqualTo(MUTABLE_ROWS);
  }

  @Test
  void downsamplingWaitsForAReaderHoldingTheCompactionReadLock() throws Exception {
    fill();
    final long sealedBefore = shard().getSealedStore().getTotalSampleCount();
    // One sample per second downsampled to one per minute, for every sealed block
    final State after = runWhileAReaderHoldsTheLock(
        engine -> engine.applyDownsampling(List.of(new DownsamplingTier(1L, 60_000L)), Long.MAX_VALUE / 2));
    assertThat(after.sealedSamples()).as("downsampling reduced the sealed samples once the reader let go").isLessThan(sealedBefore)
        .isGreaterThan(0L);
    assertThat(after.mutableSamples()).as("downsampling does not touch the mutable bucket").isEqualTo(MUTABLE_ROWS);
  }

  @FunctionalInterface
  private interface Maintenance {
    void run(TimeSeriesEngine engine) throws Exception;
  }

  private record State(int sealedBlocks, long sealedSamples, long mutableSamples, long windowedCount) {
    long totalSamples() {
      return sealedSamples + mutableSamples;
    }
  }

  /**
   * Runs {@code maintenance} on another thread while this thread holds the shard's compaction read lock, and checks that
   * neither layer nor the windowed count moved before the lock is released. The operation is observed parked on the
   * lock, not timed: the 30 s bounds are hang guards only.
   */
  private State runWhileAReaderHoldsTheLock(final Maintenance maintenance) throws Exception {
    final TimeSeriesEngine engine = engine();
    assertThat(shard().getCompactionLock()).as("the queue checks below need the JDK's ReentrantReadWriteLock")
        .isInstanceOf(ReentrantReadWriteLock.class);
    final ReentrantReadWriteLock lock = (ReentrantReadWriteLock) shard().getCompactionLock();
    final AtomicReference<Thread> maintenanceThread = new AtomicReference<>();
    final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
      final Thread thread = new Thread(r, "issue9745-maintenance");
      maintenanceThread.set(thread);
      return thread;
    });
    try {
      final Future<?> running;
      lock.readLock().lock();
      try {
        final State before = state();
        running = executor.submit(() -> {
          maintenance.run(engine);
          return null;
        });
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!isQueued(lock, maintenanceThread.get()) && !running.isDone() && System.nanoTime() < deadline)
          LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));

        assertThat(running.isDone()).as("the operation waits for the reader instead of publishing past it").isFalse();
        // Not done AND queued: a publisher that skipped the write lock fails here, whether it already finished or is
        // still running unlocked, before state() is even asked
        assertThat(isQueued(lock, maintenanceThread.get())).as("the operation is parked on the compaction lock").isTrue();
        // state() runs the windowed count on THIS thread, which already holds the read lock: ReentrantReadWriteLock lets a
        // reentrant read through even with a writer queued. aggregateMulti reads a single shard on the calling thread; if it
        // ever read it from another thread, that read would queue behind the parked writer and this would hang up to the
        // 30 s bound below
        assertThat(state()).as("nothing changed under the reader's compaction read lock").isEqualTo(before);
      } finally {
        lock.readLock().unlock();
      }
      running.get(30, TimeUnit.SECONDS);
      final State after = state();
      assertThat(after.windowedCount()).as("the windowed count agrees with the layers it was read from")
          .isEqualTo(sqlCount("SELECT count(*) AS c FROM T WHERE ts >= " + WINDOW_START_TS));
      return after;
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  private static boolean isQueued(final ReentrantReadWriteLock lock, final Thread thread) {
    return thread != null && lock.hasQueuedThread(thread);
  }

  /**
   * One small sealed block per round, then rows left in the mutable bucket, so every operation has something to act on.
   */
  private void fill() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    int row = 0;
    for (int round = 0; round < SEALED_ROUNDS; round++) {
      insert(row, ROWS_PER_BLOCK);
      row += ROWS_PER_BLOCK;
      engine().compactAll();
    }
    insert(row, MUTABLE_ROWS);
    assertThat(shard().getMutableBucket().getSampleCount()).isEqualTo(MUTABLE_ROWS);
    assertThat(sqlCount("SELECT count(*) AS c FROM T")).isEqualTo(totalRows());
  }

  private void insert(final int first, final int count) {
    database.transaction(() -> {
      for (int i = first; i < first + count; i++)
        database.command("sql", "INSERT INTO T SET ts = ?, host = ?, v = ?", T0 + i * 1000L, "h" + (i % 4), (double) i);
    });
  }

  private static long totalRows() {
    return (long) SEALED_ROUNDS * ROWS_PER_BLOCK + MUTABLE_ROWS;
  }

  private State state() throws Exception {
    final TimeSeriesShard shard = shard();
    return new State(shard.getSealedStore().getBlockCount(), shard.getSealedStore().getTotalSampleCount(),
        shard.getMutableBucket().getSampleCount(), sqlCount("SELECT count(*) AS c FROM T WHERE ts >= " + WINDOW_START_TS));
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine();
  }

  private TimeSeriesShard shard() {
    return engine().getShard(0);
  }

  private long sqlCount(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
