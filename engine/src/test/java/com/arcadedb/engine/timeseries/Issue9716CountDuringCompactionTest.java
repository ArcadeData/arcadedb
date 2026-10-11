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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

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
   * sealed swap and the mutable clear. The hook gives the read a second to answer before letting the compaction go on;
   * a read that waits on the compaction (the fixed behavior) answers once it has finished.
   */
  private <T> T readDuringCompactionSwap(final Callable<T> read) throws Exception {
    final ExecutorService reader = Executors.newSingleThreadExecutor();
    final AtomicReference<Future<T>> pending = new AtomicReference<>();
    try {
      TimeSeriesShard.TEST_PHASE4C_AFTER_SWAP_HOOK = () -> {
        if (pending.get() != null)
          return;
        final Future<T> future = reader.submit(read);
        pending.set(future);
        try {
          future.get(1, TimeUnit.SECONDS);
        } catch (final TimeoutException e) {
          // The read waits for this compaction: let it finish
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (final Exception e) {
          // Surfaced by the get() below
        }
      };

      ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine().compactAll();
      TimeSeriesShard.TEST_PHASE4C_AFTER_SWAP_HOOK = null;

      assertThat(pending.get()).as("the compaction reached the hook").isNotNull();
      return pending.get().get(30, TimeUnit.SECONDS);
    } finally {
      reader.shutdownNow();
    }
  }
}
