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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7475: a sealed-store repair on a type whose engine never loaded was excluded by neither
 * {@link TimeSeriesCompactionPause} nor {@link TimeSeriesSealedInstallLock}, because both walked the per-shard
 * compaction locks and a type with no engine has no shard to take one from.
 * <p>
 * The tests drive the type into that state with {@link LocalTimeSeriesType#close()}, which is the same
 * "engine == null" the schema loader leaves behind for issue #6356, and check both directions of the exclusion
 * through the lock that now belongs to the TYPE ({@link LocalTimeSeriesType#getEngineLifecycleLock()}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7475EngineLessTypeLockTest extends TestHelper {
  /** A wait that is EXPECTED to expire: it IS the assertion. A stall can only make it more true. */
  private static final long BLOCKED_PROBE_MS = 1_500L;
  private static final long PARK_WAIT_MS     = 30_000L;

  @Override
  protected void beginTest() {
    database.command("sql",
        "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 2");
  }

  /** The type is left without an engine on purpose, which the after-test integrity check rightly reports. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  private LocalTimeSeriesType engineLessType() {
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("Reading");
    type.close();
    assertThat(type.isEngineAvailable()).as("the state this test starts from: no engine, so no shard").isFalse();
    return type;
  }

  /** The reported case: a backup pause is held, and the repair's install lock must wait for it. */
  @Test
  void aHeldPauseBlocksTheInstallLockOfAnEngineLessType() throws Exception {
    engineLessType();

    final CountDownLatch locked = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
      assertThat(pause.getPausedShards())
          .as("there is no shard to count: it is the type's lifecycle lock that makes the pause cover it").isZero();

      final Thread repair = new Thread(() -> {
        try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
            List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 60_000L)) {
          locked.countDown();
        } catch (final Throwable e) {
          failure.set(e);
          locked.countDown();
        }
      }, "issue7475-repair");
      repair.setDaemon(true);
      repair.start();

      awaitParkedIn(repair, "TimeSeriesSealedInstallLock", "acquire");
      assertThat(locked.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS))
          .as("a repair must not run through a held pause: that is the duplicated-samples restore").isFalse();

      pause.close();

      assertThat(locked.await(60, TimeUnit.SECONDS))
          .as("and it must proceed the moment the pause is released, or the test proves nothing").isTrue();
      assertThat(failure.get()).isNull();
      repair.join(60_000);
    }
  }

  /** The other direction: a pause cannot start while a repair holds the type. */
  @Test
  void aHeldInstallLockOfAnEngineLessTypeBlocksThePause() throws Exception {
    engineLessType();

    final CountDownLatch paused = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
        List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 30_000L)) {
      assertThat(lock.getLockedShards()).as("no shard exists yet").isZero();

      final Thread backup = new Thread(() -> {
        try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 60_000L)) {
          paused.countDown();
        } catch (final Throwable e) {
          failure.set(e);
          paused.countDown();
        }
      }, "issue7475-backup");
      backup.setDaemon(true);
      backup.start();

      awaitParkedIn(backup, "TimeSeriesCompactionPause", "acquire");
      assertThat(paused.await(BLOCKED_PROBE_MS, TimeUnit.MILLISECONDS))
          .as("a copy of the database must wait for a repair in flight rather than photograph it halfway").isFalse();

      lock.close();

      assertThat(paused.await(60, TimeUnit.SECONDS)).isTrue();
      assertThat(failure.get()).isNull();
      backup.join(60_000);
    }
  }

  /**
   * A repair that brings the engine up while a pause was waiting must not leave the pause covering nothing: the
   * pause takes the type's lock first and only then reads the engine, so the shards the repair created are locked
   * too. Without the re-read the pause would be granted when the repair ends and hold no shard, and a whole
   * compaction could start and finish inside the window it believes closed.
   */
  @Test
  void aPauseGrantedAfterTheEngineCameUpAlsoHoldsItsShards() throws Exception {
    final LocalTimeSeriesType type = engineLessType();

    final AtomicReference<Integer> pausedShards = new AtomicReference<>();
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread backup;
    try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
        List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 30_000L)) {
      // The pause is taken and closed on ITS thread: the read locks are released by the thread that took them.
      backup = new Thread(() -> {
        try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 60_000L)) {
          pausedShards.set(pause.getPausedShards());
        } catch (final Throwable e) {
          failure.set(e);
        }
      }, "issue7475-late-backup");
      backup.setDaemon(true);
      backup.start();
      awaitParkedIn(backup, "TimeSeriesCompactionPause", "acquire");

      // The repair: the engine comes up while the pause waits.
      try {
        type.initEngine();
      } catch (final Exception e) {
        throw new AssertionError(e);
      }
    }

    backup.join(60_000);
    assertThat(failure.get()).isNull();
    assertThat(pausedShards.get()).as("both shards that appeared while it waited").isEqualTo(2);
  }

  /** Nothing is left held when a budget runs out on an engine-less type. */
  @Test
  void aTimedOutAcquisitionOnAnEngineLessTypeReleasesEverything() {
    engineLessType();

    try (final TimeSeriesCompactionPause pause = TimeSeriesCompactionPause.acquire(database, 30_000L)) {
      org.assertj.core.api.Assertions.assertThatThrownBy(() -> TimeSeriesSealedInstallLock.acquire(database,
          List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 200L))
          .isInstanceOf(com.arcadedb.exception.TimeoutException.class).hasMessageContaining("Reading");
    }

    try (final TimeSeriesSealedInstallLock lock = TimeSeriesSealedInstallLock.acquire(database,
        List.of(new TimeSeriesSealedInstallLock.ShardRef("Reading", 0)), 5_000L)) {
      assertThat(lock.getLockedShards()).isZero();
    }
  }

  private static void awaitParkedIn(final Thread thread, final String className, final String methodName)
      throws InterruptedException {
    final long deadline = System.currentTimeMillis() + PARK_WAIT_MS;
    while (System.currentTimeMillis() < deadline) {
      final Thread.State state = thread.getState();
      if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING)
        for (final StackTraceElement frame : thread.getStackTrace())
          if (frame.getClassName().endsWith(className) && methodName.equals(frame.getMethodName()))
            return;
      if (state == Thread.State.TERMINATED)
        break;
      Thread.sleep(10);
    }
    throw new AssertionError("thread '" + thread.getName() + "' never parked in " + className + "." + methodName
        + " (state=" + thread.getState() + "); the block assertion that follows would have been vacuous");
  }
}
