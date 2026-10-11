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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9716: a count of a TimeSeries type taken while a compaction publishes its sealed blocks
 * must not see the compacted rows twice - once in the new sealed blocks and once more in the mutable bucket the same
 * compaction has not cleared yet.
 * <p>
 * The race is made deterministic with {@link TimeSeriesShard#TEST_PHASE4C_AFTER_SWAP_HOOK}, which parks the compaction
 * inside Phase 4c exactly between the sealed-file swap and the mutable-bucket clear: the state the reporter's
 * {@code count(*)} landed in. The count is issued from another thread while the compaction is parked there; it either
 * answers at once (and, before the fix, answered both layers) or waits for the compaction to finish, and in both cases
 * its answer must be the number of rows inserted.
 */
class Issue9716CountDuringCompactionTest extends TestHelper {

  private static final long T0   = 1_767_225_600_000L;
  private static final int  ROWS = 3_000;

  @AfterEach
  void clearHook() {
    TimeSeriesShard.TEST_PHASE4C_AFTER_SWAP_HOOK = null;
  }

  @Test
  void sqlCountDuringCompactionSwapDoesNotDoubleCount() throws Exception {
    createAndFill(1);
    final long during = readDuringCompactionSwap(() -> sqlCount("SELECT count(*) AS c FROM T"));
    assertThat(during).isEqualTo(ROWS);
    assertThat(sqlCount("SELECT count(*) AS c FROM T")).isEqualTo(ROWS);
  }

  @Test
  void countTypeDuringCompactionSwapDoesNotDoubleCount() throws Exception {
    createAndFill(1);
    final long during = readDuringCompactionSwap(() -> database.countType("T", true));
    assertThat(during).isEqualTo(ROWS);
  }

  @Test
  void countOfAMultiShardTypeDuringCompactionSwapDoesNotDoubleCount() throws Exception {
    createAndFill(2);
    final long during = readDuringCompactionSwap(() -> sqlCount("SELECT count(*) AS c FROM T"));
    assertThat(during).isEqualTo(ROWS);
  }

  /** The windowed form went through the aggregate push-down, which already held the lock: pinned so it stays that way. */
  @Test
  void windowedCountDuringCompactionSwapDoesNotDoubleCount() throws Exception {
    createAndFill(1);
    final long during = readDuringCompactionSwap(() -> sqlCount("SELECT count(*) AS c FROM T WHERE ts >= " + T0));
    assertThat(during).isEqualTo(ROWS);
  }

  @Test
  void schemaTypesRecordsDuringCompactionSwapDoesNotDoubleCount() throws Exception {
    createAndFill(1);
    final Result during = readDuringCompactionSwap(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM schema:types WHERE name = 'T'")) {
        return rs.next();
      }
    });
    assertThat(((Number) during.getProperty("records")).longValue()).isEqualTo(ROWS);
    final Result shard = during.<List<Result>>getProperty("shards").getFirst();
    assertThat(((Number) shard.getProperty("totalSamples")).longValue()).isEqualTo(ROWS);
    assertThat(((Number) shard.getProperty("sealedSamples")).longValue()
        + ((Number) shard.getProperty("mutableSamples")).longValue()).isEqualTo(ROWS);
  }

  private void createAndFill(final int shards) {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS " + shards);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.command("sql", "INSERT INTO T SET ts = ?, host = ?, v = ?", T0 + i * 1000L, "h" + (i % 4), (double) i);
    });
    assertThat(sqlCount("SELECT count(*) AS c FROM T")).isEqualTo(ROWS);
  }

  private long sqlCount(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  /**
   * Compacts the type and runs {@code read} on another thread while the first shard's compaction is parked between the
   * sealed swap and the mutable clear. The hook holds the compaction there until the read has either answered (the
   * unfixed behavior: it read both layers without waiting) or parked on a shard's compaction lock (the fixed behavior:
   * it waits for this compaction and answers once it has finished). Both are observed, not timed, so a slow runner
   * cannot make the read miss the window. The 30 s bound is a hang guard only.
   */
  private <T> T readDuringCompactionSwap(final Callable<T> read) throws Exception {
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine();
    final ExecutorService reader = Executors.newSingleThreadExecutor();
    final AtomicReference<Future<T>> pending = new AtomicReference<>();
    try {
      TimeSeriesShard.TEST_PHASE4C_AFTER_SWAP_HOOK = () -> {
        if (pending.get() != null)
          return;
        final Future<T> future = reader.submit(read);
        pending.set(future);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!future.isDone() && !readerParkedOnACompactionLock(engine) && System.nanoTime() < deadline)
          LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
      };

      engine.compactAll();
      TimeSeriesShard.TEST_PHASE4C_AFTER_SWAP_HOOK = null;

      assertThat(pending.get()).as("the compaction reached the hook").isNotNull();
      return pending.get().get(30, TimeUnit.SECONDS);
    } finally {
      reader.shutdownNow();
    }
  }

  /**
   * Whether any thread waits on a shard's compaction lock. While the hook runs, the compaction holding the write lock
   * is the only other user of these locks in this test, so a queued thread is the reader; a test that adds other
   * readers or writers of the type would need to tell them apart.
   */
  private static boolean readerParkedOnACompactionLock(final TimeSeriesEngine engine) {
    for (int s = 0; s < engine.getShardCount(); s++)
      if (((ReentrantReadWriteLock) engine.getShard(s).getCompactionLock()).hasQueuedThreads())
        return true;
    return false;
  }
}
