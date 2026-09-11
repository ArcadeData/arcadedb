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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7337: on an HA follower the sealed store is not rewritten by local compaction - it is REPLACED by the
 * leader's blob - and that path took only the store's own {@code directoryLock}, so
 * {@link TimeSeriesCompactionPause} did not exclude it.
 * <p>
 * The tear that produced is invisible. The leader ships the rewritten sealed bytes and the WAL that clears the
 * mutable bucket in ONE Raft entry, so they are atomic with respect to each other; a backup or a snapshot ship
 * running on that follower can still capture a page image from before the entry (samples in the mutable bucket)
 * and a sealed image from after it (the same samples, now sealed). Restored, every one of those samples appears
 * twice, and nothing reports an error. The {@code compactionInProgress} watermark that repairs a torn LOCAL
 * compaction is never set for a shipped blob, so open-time recovery has nothing to key on either.
 * <p>
 * {@link TimeSeriesSealedInstallLock} is what makes the pause mean on a follower what it already means on a
 * standalone database: the install takes the same shard lock a local compaction takes. These tests hold the two
 * halves of that - a held pause really does block an install, and a held install really does block a pause -
 * plus the resolution and ordering rules the lock has to follow to be safe to take on the Raft apply thread.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7337">issue #7337</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7337FollowerSealedInstallLockTest extends TestHelper {

  private static final long BASE_TS = 1_700_000_000_000L;
  /** A wait that is EXPECTED to expire: it IS the assertion. A stall can only make it more true. */
  private static final long BLOCKED_PROBE_MS = 2_000L;

  @Override
  protected void beginTest() {
    database.command("sql",
        "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
  }

  /**
   * The fix, stated directly: a backup holding the compaction pause excludes a follower's sealed-store install,
   * which is exactly what it did not do before. Without the lock the install would run straight through the
   * pause and the archive could pair a pre-clear page image with a post-install sealed one.
   */
  @Test
  void aHeldCompactionPauseBlocksAFollowerSealedInstall() throws Exception {
    ingest(2_000);

    final CountDownLatch installed = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
      final Thread applier = new Thread(() -> {
        try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
            List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 60_000L)) {
          installed.countDown();
        } catch (final Throwable e) {
          failure.set(e);
          installed.countDown();
        }
      }, "issue7337-applier");
      applier.setDaemon(true);
      applier.start();

      assertThat(installed.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS))
          .as("a sealed install must not get through a held pause: that is the whole tear")
          .isFalse();

      pause.close();

      assertThat(installed.await(60, TimeUnit.SECONDS))
          .as("and it must proceed the moment the pause is released, or a backup would stall replication")
          .isTrue();
      assertThat(failure.get()).isNull();
      applier.join(60_000);
    }
  }

  /** The other direction: a pause cannot start while an install is in flight, so it never sees a half-applied entry. */
  @Test
  void aHeldSealedInstallBlocksTheCompactionPause() throws Exception {
    ingest(2_000);

    final CountDownLatch paused = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
        List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 30_000L)) {

      assertThat(lock.getLockedShards()).isEqualTo(1);

      final Thread backup = new Thread(() -> {
        try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 60_000L)) {
          paused.countDown();
        } catch (final Throwable e) {
          failure.set(e);
          paused.countDown();
        }
      }, "issue7337-backup");
      backup.setDaemon(true);
      backup.start();

      assertThat(paused.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS))
          .as("a copy of the database must wait for an install in flight rather than photograph it halfway")
          .isFalse();

      lock.close();

      assertThat(paused.await(60, TimeUnit.SECONDS)).isTrue();
      assertThat(failure.get()).isNull();
      backup.join(60_000);
    }
  }

  /** A shard the entry does not name is left alone, so an unrelated compaction is not held up by the apply. */
  @Test
  void onlyTheShardsTheEntryNamesAreLocked() {
    try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
        List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 1)), 30_000L)) {
      assertThat(lock.getLockedShards()).isEqualTo(1);
      // Shard 0 is untouched, so a second lock on it is free.
      try (final TimeSeriesSealedInstallLock other = TimeSeriesSealedInstallLock.acquire(database,
          List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 5_000L)) {
        assertThat(other.getLockedShards()).isEqualTo(1);
      }
    }
  }

  /**
   * A reference that resolves to nothing is skipped rather than refused. The apply path has its own diagnostic
   * for an unknown type, a non-TimeSeries one and an engine that never started; a shard that does not exist
   * cannot be torn, so failing the whole entry here would only turn a logged skip into a halted replica.
   */
  @Test
  void anUnresolvableShardReferenceLocksNothingRatherThanFailing() {
    database.command("sql", "CREATE DOCUMENT TYPE Plain");

    try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database, List.of(
        new TimeSeriesSealedInstallLock.ShardRef("NoSuchType", 0),
        new TimeSeriesSealedInstallLock.ShardRef("Plain", 0),
        new TimeSeriesSealedInstallLock.ShardRef("Reading", 99)), 30_000L)) {
      assertThat(lock.getLockedShards()).isZero();
    }

    assertThat(TimeSeriesSealedInstallLock.acquire(database, List.of(), 30_000L).getLockedShards()).isZero();
    assertThat(TimeSeriesSealedInstallLock.acquire(database, null, 30_000L).getLockedShards()).isZero();
  }

  /**
   * The budget is a hang detector: when it expires, everything already taken is released, so a failed apply
   * never leaves a shard locked for the life of the node.
   */
  @Test
  void aTimedOutAcquisitionReleasesWhateverItHadTaken() throws Exception {
    ingest(1_000);

    try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
      assertThatThrownBy(() -> TimeSeriesSealedInstallLock.acquire(database, List.of(
          new TimeSeriesSealedInstallLock.ShardRef("Reading", 0),
          new TimeSeriesSealedInstallLock.ShardRef("Reading", 1)), 200L))
          .isInstanceOf(TimeoutException.class)
          .hasMessageContaining("Reading");
    }

    // Nothing was left held: the pause is takeable again, and so is the install lock.
    try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 5_000L)) {
      assertThat(pause.getPausedShards()).isEqualTo(2);
    }
    try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database, List.of(
        new TimeSeriesSealedInstallLock.ShardRef("Reading", 0),
        new TimeSeriesSealedInstallLock.ShardRef("Reading", 1)), 5_000L)) {
      assertThat(lock.getLockedShards()).isEqualTo(2);
    }
  }

  /** Closing twice is a no-op, so the explicit early release and the try-with-resources can both fire. */
  @Test
  void closingTheLockTwiceIsANoOp() {
    final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
        List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 30_000L);
    lock.close();
    lock.close();

    try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 5_000L)) {
      assertThat(pause.getPausedShards()).isEqualTo(2);
    }
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType("Reading")).getEngine();
  }

  private void ingest(final int samples) throws Exception {
    final long[] timestamps = new long[samples];
    final Object[] hosts = new Object[samples];
    final Object[] values = new Object[samples];
    for (int i = 0; i < samples; i++) {
      timestamps[i] = BASE_TS + i * 1_000L;
      hosts[i] = "host_" + (i % 4);
      values[i] = (double) i;
    }
    engine().appendBatch(timestamps, new Object[][] { hosts, values });
  }
}
