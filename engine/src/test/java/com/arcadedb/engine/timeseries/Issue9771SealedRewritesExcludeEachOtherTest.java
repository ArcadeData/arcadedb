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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #9771: every operation that rewrites a shard's sealed file shares one temp path ({@code .ts.sealed.tmp}), and the rule
 * that keeps them apart was stated on a mutex that only compaction and the merge of small blocks took. Retention and
 * downsampling took the shard's compaction READ/WRITE lock instead - which compaction deliberately does not hold for
 * Phases 3 and 4b, the two that own the temp file. A retention pass could therefore run from beginning to end inside a
 * compaction's Phase 3 to Phase 4c window, write its own temp file over the one the compaction was using, rename it onto the
 * live sealed file, and leave the compaction to find its temp file gone. What the database was left with: a live store whose
 * in-memory directory disagreed with the file (every read of a block decoded another block's bytes), and, after a reopen,
 * the retention undone.
 * <p>
 * The race is made deterministic with {@link TimeSeriesShard#TEST_PRE_PHASE4C_HOOK}, which parks the compaction inside the
 * window; the second rewrite is started from ANOTHER thread, as it is in production (the maintenance tick against a
 * {@code COMPACT TIMESERIES TYPE} from a session). On the compaction's own thread the mutex is re-entrant and would let it
 * through, which is not a state production can reach.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
// a hang detector for a deadlock between the compaction and the rewrite it is meant to exclude, not a latency bound: the class runs in about 2 seconds
@Timeout(180)
class Issue9771SealedRewritesExcludeEachOtherTest extends TestHelper {
  private static final String TYPE_NAME = "Issue9771Ts";
  private static final long   T0        = 1_700_000_000_000L;
  private static final long   STEP_MS   = 1_000L;
  private static final int    PER_BLOCK = 100;
  /** The sealed blocks are 100 samples a second apart, one per pass, 200 s apart. */
  private static final long   BLOCK_SPAN_MS = 200_000L;

  private enum SecondRewrite {
    RETENTION, DOWNSAMPLING, MERGE
  }

  @AfterEach
  void clearHooks() {
    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = null;
    TimeSeriesShard.TEST_PRE_PHASE4B_HOOK = null;
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType(TYPE_NAME)).getEngine();
  }

  private void createType() {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE_NAME + " TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
  }

  /** One pass worth of samples, starting at the pass's own slot so that passes never overlap. */
  private void feed(final TimeSeriesEngine engine, final int pass) throws Exception {
    final long[] timestamps = new long[PER_BLOCK];
    final Object[] tags = new Object[PER_BLOCK];
    final Object[] values = new Object[PER_BLOCK];
    for (int i = 0; i < PER_BLOCK; i++) {
      timestamps[i] = T0 + pass * BLOCK_SPAN_MS + i * STEP_MS;
      tags[i] = "s" + (i % 3);
      values[i] = (double) (pass * PER_BLOCK + i);
    }
    engine.appendSamples(timestamps, tags, values);
  }

  /** Six sealed blocks, then a seventh batch left in the mutable bucket for the compaction under test to seal. */
  private void sealSixBlocksAndLeaveOneBatch(final TimeSeriesEngine engine) throws Exception {
    for (int pass = 0; pass < 6; pass++) {
      feed(engine, pass);
      engine.compactAll();
    }
    feed(engine, 6);
    assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(6);
  }

  private static void runOn(final DatabaseInternal db, final Thread[] holder, final AtomicReference<Throwable> failure,
      final ThrowingRunnable work) {
    final Thread thread = new Thread(() -> {
      DatabaseContext.INSTANCE.init(db);
      try {
        work.run();
      } catch (final Throwable t) {
        failure.set(t);
      } finally {
        DatabaseContext.INSTANCE.removeContext(db.getDatabasePath());
      }
    }, "Issue9771-second-rewrite");
    holder[0] = thread;
    thread.start();
  }

  /**
   * Waits until the thread has finished or is queued on the shard's rewrite mutex, and says which. A thread that is merely parked
   * on something else would not do: the claim is that the second rewrite reached the mutex and waits there. The bound is a hang
   * detector, not a latency claim.
   */
  private static boolean awaitQueuedOnTheMutex(final TimeSeriesShard shard, final Thread thread) throws InterruptedException {
    final long deadline = System.nanoTime() + 20_000_000_000L;
    while (thread.isAlive() && System.nanoTime() < deadline) {
      if (shard.hasRewriteWaiting())
        return true;
      Thread.sleep(5);
    }
    return false;
  }

  private record Snapshot(int blocks, long samples, int rows, List<String> problems) {
  }

  private Snapshot snapshot() throws Exception {
    final TimeSeriesEngine engine = engine();
    final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
    final List<String> problems = new ArrayList<>();
    sealed.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems().forEach(p -> problems.add(p.toString()));
    return new Snapshot(sealed.getBlockCount(), sealed.getTotalSampleCount(),
        engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null).size(), problems);
  }

  @ParameterizedTest
  @EnumSource(SecondRewrite.class)
  void aRewriteStartedInsideACompactionWaitsForItAndLeavesASoundStore(final SecondRewrite second) throws Exception {
    createType();
    final TimeSeriesEngine engine = engine();
    sealSixBlocksAndLeaveOneBatch(engine);

    final Thread[] holder = new Thread[1];
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicBoolean queuedOnTheMutex = new AtomicBoolean();
    final DatabaseInternal db = (DatabaseInternal) database;
    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = () -> {
      TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = null;
      runOn(db, holder, failure, () -> {
        switch (second) {
        case RETENTION -> engine.applyRetention(T0 + 3 * BLOCK_SPAN_MS);
        case DOWNSAMPLING -> engine.applyDownsampling(List.of(new DownsamplingTier(1L, 10_000L, 0L)), T0 + 100 * BLOCK_SPAN_MS);
        case MERGE -> engine.mergeSmallBlocks();
        }
      });
      try {
        queuedOnTheMutex.set(awaitQueuedOnTheMutex(engine.getShard(0), holder[0]));
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    engine.compactAll();

    assertThat(holder[0]).as("the hook must have started the second rewrite").isNotNull();
    assertThat(queuedOnTheMutex.get()).as("the second rewrite must wait for the compaction on the shard's rewrite mutex").isTrue();
    holder[0].join(60_000);
    assertThat(holder[0].isAlive()).as("the second rewrite must finish once the compaction is done").isFalse();
    assertThat(failure.get()).as("the second rewrite must not fail").isNull();

    final Snapshot live = snapshot();
    assertThat(live.problems()).as("the live store must agree with its own file").isEmpty();
    switch (second) {
    case RETENTION -> {
      // 3 blocks retired, the compaction's own block added: the order the two ran in, not the one they interleaved in
      assertThat(live.blocks()).isEqualTo(4);
      assertThat(live.samples()).isEqualTo(4L * PER_BLOCK);
      assertThat(live.rows()).isEqualTo(4 * PER_BLOCK);
    }
    case DOWNSAMPLING -> assertThat(live.rows()).as("seven batches were sealed, then reduced").isLessThan(7 * PER_BLOCK);
    case MERGE -> assertThat(live.rows()).isEqualTo(7 * PER_BLOCK);
    }

    // the retention must not be undone by what the compaction installs, or by a restart
    reopenDatabase();
    final Snapshot reopened = snapshot();
    assertThat(reopened.problems()).isEmpty();
    assertThat(reopened.blocks()).as("a restart sees the store the live node was serving").isEqualTo(live.blocks());
    assertThat(reopened.samples()).isEqualTo(live.samples());
    assertThat(reopened.rows()).isEqualTo(live.rows());
  }

  /** The same rule for two retention passes: the second must not start writing while the first still owns the temp path. */
  @Test
  void concurrentMaintenancePassesLeaveASoundStore() throws Exception {
    createType();
    final TimeSeriesEngine engine = engine();
    for (int pass = 0; pass < 12; pass++) {
      feed(engine, pass);
      engine.compactAll();
    }

    final DatabaseInternal db = (DatabaseInternal) database;
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final List<Thread> threads = new ArrayList<>();
    final ThrowingRunnable[] works = {
        () -> engine.applyRetention(T0 + 2 * BLOCK_SPAN_MS),
        () -> engine.applyRetention(T0 + 4 * BLOCK_SPAN_MS),
        () -> engine.applyDownsampling(List.of(new DownsamplingTier(1L, 20_000L, 0L)), T0 + 100 * BLOCK_SPAN_MS),
        engine::mergeSmallBlocks,
        engine::compactAll };
    for (int round = 0; round < 20; round++)
      for (final ThrowingRunnable work : works) {
        final Thread[] holder = new Thread[1];
        runOn(db, holder, failure, work);
        threads.add(holder[0]);
      }
    for (final Thread thread : threads) {
      thread.join(120_000);
      assertThat(thread.isAlive()).isFalse();
    }
    assertThat(failure.get()).isNull();

    final Snapshot live = snapshot();
    assertThat(live.problems()).isEmpty();
    reopenDatabase();
    final Snapshot reopened = snapshot();
    assertThat(reopened.problems()).isEmpty();
    assertThat(reopened.blocks()).isEqualTo(live.blocks());
    assertThat(reopened.samples()).isEqualTo(live.samples());
    assertThat(reopened.rows()).isEqualTo(live.rows());
  }

  /**
   * The window that produced the leftover of #9770: a compaction that fails between writing its temp file and swapping it
   * (Phase 4a, Phase 4b) used to leave the file in place, with the crash-recovery flag set, for the next writer to find.
   */
  @Test
  void aCompactionFailingInPhase4bLeavesNoTempFileAndNoFlag() throws Exception {
    createType();
    final TimeSeriesEngine engine = engine();
    sealSixBlocksAndLeaveOneBatch(engine);
    final TimeSeriesShard shard = engine.getShard(0);
    final File temp = new File(getDatabasePath(), shard.getSealedStore().getSealedFileName() + ".tmp");

    TimeSeriesShard.TEST_PRE_PHASE4B_HOOK = () -> {
      assertThat(temp).as("the temp file exists once Phase 3 is done").exists();
      throw new IllegalStateException("injected Phase 4b failure");
    };
    assertThatThrownBy(engine::compactAll).isInstanceOf(IllegalStateException.class).hasMessageContaining("injected Phase 4b");
    TimeSeriesShard.TEST_PRE_PHASE4B_HOOK = null;

    assertThat(temp).as("a failed compaction leaves no temp file behind").doesNotExist();
    assertThat(shard.getMutableBucket().isCompactionInProgress()).as("and no crash-recovery flag").isFalse();

    // the next rewrite starts from a clean slate and the data is all still there
    engine.applyRetention(T0 + 3 * BLOCK_SPAN_MS);
    assertThat(shard.getSealedStore().getBlockCount()).isEqualTo(3);
    engine.compactAll();
    assertThat(snapshot().problems()).isEmpty();
    assertThat(snapshot().rows()).isEqualTo(4 * PER_BLOCK);
  }

  /**
   * A read lock cannot be upgraded to the write lock a rewrite takes, so a rewrite started by a thread that holds the compaction
   * read lock would wait for itself for ever. It is refused up front, before the rewrite mutex is taken. The timeout is a hang
   * detector for the failure this guards against, not a latency claim.
   */
  @Test
  @Timeout(60)
  void aRewriteStartedWhileHoldingTheCompactionReadLockIsRefusedInsteadOfDeadlocking() throws Exception {
    createType();
    final TimeSeriesEngine engine = engine();
    final TimeSeriesShard shard = engine.getShard(0);

    shard.getCompactionLock().readLock().lock();
    try {
      assertThatThrownBy(() -> engine.applyRetention(T0)).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("compaction read lock");
    } finally {
      shard.getCompactionLock().readLock().unlock();
    }

    // and nothing was left held: the same call works once the lock is given back
    engine.applyRetention(T0);
    assertThat(shard.hasRewriteWaiting()).isFalse();
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
