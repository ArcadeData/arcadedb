/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.engine.timeseries.TimeSeriesShard;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4458: WAL page-version gap on TimeSeries shard page-0 when
 * compaction Phase 4c races with concurrent Raft-replicated appends.
 * <p>
 * The root cause was that {@code appendSamples()} held {@code compactionLock.readLock()} through
 * {@code db.commit()}. On the leader, {@code commit()} calls {@code waitForActiveRecordingSession()}
 * which waits for the compaction recording session to end. The session ends only after Phase 4c
 * (which needs the write lock), but Phase 4c cannot acquire the write lock while any thread holds
 * the read lock - a deadlock resolved only by {@code waitForActiveRecordingSession()}'s timeout.
 * When the timeout fires, the append ships its {@code TX_ENTRY} before the buffered
 * {@code SCHEMA_ENTRY}, causing a WAL page-version gap on the follower.
 * <p>
 * The fix releases the read lock before {@code commit()}. This test uses
 * {@link TimeSeriesShard#TEST_PRE_PHASE4C_HOOK} to inject a brief delay just before Phase 4c
 * acquires the write lock. During this delay, append threads start and reach
 * {@code waitForActiveRecordingSession()} (where they wait without holding the read lock).
 * Phase 4c then runs unblocked, {@code replicateSchema()} ships the {@code SCHEMA_ENTRY},
 * the session ends, and appends ship their {@code TX_ENTRY} afterward - in the correct order.
 */
@Tag("slow")
class Issue4458TsWalVersionGapIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @AfterEach
  void cleanupHooks() {
    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = null;
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = null;
  }

  @Test
  void noWalVersionGapWhenAppendsRacePhase4c() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql",
        "CREATE TIMESERIES TYPE sensor TIMESTAMP ts FIELDS (v DOUBLE) SHARDS 1");
    waitForAllServers();

    // Insert initial data so the shard has enough to compact.
    for (int i = 0; i < 250; i++) {
      final long ts = i;
      leaderDb.command("sql", "INSERT INTO sensor SET ts = ?, v = ?", ts, (double) i);
    }

    // Install WAL gap counter before starting the race.
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

    final int appendThreads = 4;
    final int appendsPerThread = 60;

    // The hook fires just before Phase 4c acquires the write lock.
    // Append threads wait for the hook signal, then start sending INSERTs.
    // A brief sleep in the hook gives the append threads time to reach their
    // waitForActiveRecordingSession() call (where they wait without holding the read lock).
    // Phase 4c then proceeds unblocked - if the fix is correct, no timeout fires.
    final CountDownLatch hookFired = new CountDownLatch(1);

    TimeSeriesShard.TEST_PRE_PHASE4C_HOOK = () -> {
      hookFired.countDown();
      // Sleep long enough for append threads to reach waitForActiveRecordingSession().
      // They will block there (recording session is active) without holding the read lock,
      // allowing Phase 4c to acquire the write lock immediately after this sleep.
      try {
        Thread.sleep(200);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final List<Throwable> errors = new ArrayList<>();
    final ExecutorService exec = Executors.newFixedThreadPool(appendThreads + 1);

    for (int t = 0; t < appendThreads; t++) {
      final int ti = t;
      exec.submit(() -> {
        try {
          // Wait until Phase 4c is imminent, then flood concurrent INSERTs.
          // These INSERTs will reach waitForActiveRecordingSession() and wait
          // there (without the compaction read lock) until the session ends.
          hookFired.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        final long base = 1_000_000L + (long) ti * 100_000L;
        for (int i = 0; i < appendsPerThread; i++) {
          final long ts = base + i;
          final double v = i;
          try {
            leaderDb.command("sql", "INSERT INTO sensor SET ts = ?, v = ?", ts, v);
          } catch (final Exception e) {
            synchronized (errors) {
              errors.add(e);
            }
          }
        }
      });
    }

    // Compaction thread: triggers Phase 0, 1, 2, 3, 4a, 4b, hook, Phase 4c.
    exec.submit(() -> {
      try {
        ((LocalTimeSeriesType) leaderDb.getSchema().getType("sensor")).getEngine().compactAll();
      } catch (final Throwable e) {
        synchronized (errors) {
          errors.add(e);
        }
      }
    });

    exec.shutdown();
    assertThat(exec.awaitTermination(3, TimeUnit.MINUTES))
        .as("all tasks complete within timeout").isTrue();

    assertThat(errors)
        .as("no ingest errors during compaction race: %s", errors).isEmpty();

    // The critical assertion: no WAL version gap on the follower.
    final AtomicInteger gapCounter = ArcadeStateMachine.TEST_WAL_GAP_COUNTER;
    assertThat(gapCounter.get())
        .as("follower must see zero WAL version gaps during compaction race").isZero();

    // Data-integrity: the leader must not have lost samples, and both nodes must converge to the
    // same count. The lower-bound check guards against the degenerate case where the leader itself
    // dropped data (e.g. retry exhaustion) and the follower merely agrees on the lower count.
    final long expectedMinimum = 250L + (long) appendThreads * appendsPerThread;
    final long leaderCount = ((Number) leaderDb.command("sql", "SELECT count(*) AS cnt FROM sensor")
        .next().getProperty("cnt")).longValue();
    assertThat(leaderCount)
        .as("no samples lost on the leader during the compaction race").isEqualTo(expectedMinimum);
    final int followerIndex = 1 - leaderIndex;
    final Database followerDb = getServerDatabase(followerIndex, getDatabaseName());
    final long deadline = System.currentTimeMillis() + 30_000;
    long followerCount = -1;
    while (System.currentTimeMillis() < deadline) {
      followerCount = ((Number) followerDb.command("sql", "SELECT count(*) AS cnt FROM sensor")
          .next().getProperty("cnt")).longValue();
      if (followerCount == leaderCount)
        break;
      Thread.sleep(500);
    }
    assertThat(followerCount)
        .as("follower converges to leader count after compaction race").isEqualTo(leaderCount);
  }
}
