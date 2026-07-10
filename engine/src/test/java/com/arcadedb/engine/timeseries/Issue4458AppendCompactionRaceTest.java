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
import com.arcadedb.schema.LocalTimeSeriesType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4458: concurrent appends must not lose samples when they race
 * against compaction Phase 4c on an embedded (standalone) database.
 * <p>
 * NOTE: in standalone mode {@code appendSamples()} holds {@code compactionLock.readLock()} through
 * {@code db.commit()} (the {@code db.isReplicated()} early-release branch is not taken), so the
 * HA-only CME-retry path is NOT exercised here. This test verifies that the standalone locking
 * path remains correct - concurrent appends + compaction lose no data. The actual CME-retry path
 * introduced by the fix is covered by the HA integration test {@code Issue4458TsWalVersionGapIT}.
 */
@Tag("slow")
class Issue4458AppendCompactionRaceTest extends TestHelper {

  @AfterEach
  void cleanupHook() {
    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = null;
  }

  @Test
  void concurrentAppendsRacingPhase4cRetainAllSamples() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE sensor TIMESTAMP ts FIELDS (v DOUBLE) SHARDS 1");

    // Insert enough initial data to make compaction non-trivial.
    for (int i = 0; i < 300; i++) {
      final long ts = i;
      database.transaction(() ->
          database.command("sql", "INSERT INTO sensor SET ts = ?, v = ?", ts, (double) ts));
    }

    final int appendThreads = 8;
    final int appendsPerThread = 200;
    final int initialCount = 300;
    final int expectedTotal = initialCount + appendThreads * appendsPerThread;

    // Latch: fired when TEST_PRE_PHASE4C_HOOK runs, signalling append threads to start.
    final CountDownLatch hookFired = new CountDownLatch(1);
    // Latch: each append thread counts this down from INSIDE its first transaction (after the
    // append write, before commit), so when the hook proceeds the threads are guaranteed to have
    // an open transaction in flight - not merely scheduled - maximising the append/compaction
    // overlap regardless of CI runner scheduling latency.
    final CountDownLatch appendsInFlight = new CountDownLatch(appendThreads);

    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = () -> {
      hookFired.countDown();
      // Wait until append threads actually have a transaction in flight, then let Phase 4c run.
      try {
        appendsInFlight.await(10, TimeUnit.SECONDS);
        Thread.sleep(50);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final List<Throwable> errors = new ArrayList<>();
    final ExecutorService executor = Executors.newFixedThreadPool(appendThreads + 1);

    // Append threads: wait for the hook to fire, then insert as fast as possible.
    for (int t = 0; t < appendThreads; t++) {
      final int ti = t;
      executor.submit(() -> {
        try {
          hookFired.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        final long base = 1_000_000L + (long) ti * 100_000L;
        for (int i = 0; i < appendsPerThread; i++) {
          final long ts = base + i;
          final double val = i;
          final boolean firstInsert = i == 0;
          try {
            database.transaction(() -> {
              database.command("sql", "INSERT INTO sensor SET ts = ?, v = ?", ts, val);
              // Signal in-flight status from within the transaction, before it commits.
              if (firstInsert)
                appendsInFlight.countDown();
            });
          } catch (final Throwable e) {
            synchronized (errors) {
              errors.add(e);
            }
          }
        }
      });
    }

    // Compaction thread: triggers while append threads are racing.
    executor.submit(() -> {
      try {
        ((LocalTimeSeriesType) database.getSchema().getType("sensor")).getEngine().compactAll();
      } catch (final Throwable e) {
        synchronized (errors) {
          errors.add(e);
        }
      }
    });

    executor.shutdown();
    assertThat(executor.awaitTermination(2, TimeUnit.MINUTES))
        .as("all tasks complete within timeout").isTrue();

    assertThat(errors)
        .as("no errors during concurrent append+compaction: %s", errors).isEmpty();

    final long actualCount = ((Number) database.command("sql", "SELECT count(*) AS cnt FROM sensor")
        .next().getProperty("cnt")).longValue();
    assertThat(actualCount)
        .as("all %d samples (initial + concurrent) present after compaction race", expectedTotal)
        .isEqualTo(expectedTotal);
  }
}
