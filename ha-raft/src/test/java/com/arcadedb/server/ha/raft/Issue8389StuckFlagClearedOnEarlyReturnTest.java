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

import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8389: the debounced stuck-at-stale-term flag, and the observation streaks behind it, must only describe ticks
 * that actually evaluated the follower checks. {@code tick()} returns early on a requested shutdown, on a
 * CLOSED/EXCEPTION division and on a failed Raft log writer, and each of those ticks skips
 * {@code checkStaleFollower()}/{@code checkStuckFollower()}. A flag left standing across them kept
 * {@code GET /api/v1/cluster} answering {@code localStuckAtStaleTerm: true} with a critical alert promising a
 * self-heal the same early returns made unreachable; a streak left standing across them fired a reformat (or a
 * snapshot re-arm) on the very first tick back, without the fresh second observation the streak exists to demand.
 */
class Issue8389StuckFlagClearedOnEarlyReturnTest {

  private static final long DURATION_MS = 5_000L;

  static final class FakeTarget implements HealthMonitor.HealthTarget {
    final    AtomicReference<LifeCycle.State> state                = new AtomicReference<>(LifeCycle.State.RUNNING);
    final    AtomicInteger                    restarts             = new AtomicInteger();
    final    AtomicInteger                    divergenceRecover    = new AtomicInteger();
    final    AtomicInteger                    persistentLagRecover = new AtomicInteger();
    volatile boolean                          shutdownRequested    = false;
    volatile boolean                          stuckDiverged        = false;
    volatile boolean                          lagging              = false;
    volatile String                           logFailure           = null;
    volatile boolean                          storageWritable      = true;

    @Override
    public LifeCycle.State getRaftLifeCycleState() {
      return state.get();
    }

    @Override
    public boolean isShutdownRequested() {
      return shutdownRequested;
    }

    @Override
    public void restartRatisIfNeeded() {
      restarts.incrementAndGet();
      logFailure = null; // a restart builds a fresh state machine, which is what clears the log-writer mark
    }

    @Override
    public boolean isFollowerStuckDiverged() {
      return stuckDiverged;
    }

    @Override
    public void recoverFromDivergence() {
      divergenceRecover.incrementAndGet();
    }

    @Override
    public boolean isFollowerLaggingBeyond(final long lagThreshold) {
      return lagging;
    }

    @Override
    public void recoverFromPersistentLag() {
      persistentLagRecover.incrementAndGet();
    }

    @Override
    public String getRaftLogFailure() {
      return logFailure;
    }

    @Override
    public boolean isRaftStorageWritable() {
      return storageWritable;
    }
  }

  private static HealthMonitor monitor(final FakeTarget target, final AtomicLong clock, final boolean divergedRecovery,
      final long lagThreshold, final int restartThreshold) {
    final HealthMonitor monitor = new HealthMonitor(target, 1000L, lagThreshold, DURATION_MS, divergedRecovery, 0,
        restartThreshold);
    monitor.setClock(clock::get);
    return monitor;
  }

  /** Two ticks on the stuck signature: the flag is confirmed, which is the precondition of every scenario below. */
  private static void confirmStuck(final HealthMonitor monitor, final FakeTarget target, final AtomicLong clock) {
    target.stuckDiverged = true;
    monitor.tick();
    clock.addAndGet(1_000L);
    monitor.tick();
    assertThat(monitor.isFollowerStuckDivergedConfirmed()).as("precondition: stuck confirmed on two ticks").isTrue();
  }

  @Test
  void aFailedLogWriterWaitingForSpaceDropsTheConfirmedFlag() {
    final FakeTarget target = new FakeTarget();
    final AtomicLong clock = new AtomicLong(0L);
    final HealthMonitor monitor = monitor(target, clock, true, 0L, 10);
    confirmStuck(monitor, target, clock);

    target.logFailure = "at index 4946: java.io.IOException: No space left on device";
    target.storageWritable = false;
    clock.addAndGet(1_000L);
    monitor.tick();

    assertThat(target.restarts.get()).as("the restart is deferred, so nothing resets the streaks on the way").isZero();
    assertThat(monitor.isFollowerStuckDivergedConfirmed())
        .as("a tick that returned before checkStuckFollower() observed nothing and must not leave the flag standing")
        .isFalse();
  }

  @Test
  void aFailedLogWriterWithItsRestartBudgetSpentDropsTheConfirmedFlag() {
    final FakeTarget target = new FakeTarget();
    final AtomicLong clock = new AtomicLong(0L);
    final HealthMonitor monitor = monitor(target, clock, true, 0L, 1);

    // Spend the one-restart budget: the restart clears the mark, and the episode stays open while the writer has been
    // healthy for less than LOG_FAILURE_EPISODE_RESET_MS.
    target.logFailure = "at index 12: java.io.IOException: Input/output error";
    monitor.tick();
    assertThat(target.restarts.get()).isEqualTo(1);

    clock.addAndGet(1_000L);
    confirmStuck(monitor, target, clock);

    // The failure comes straight back: the budget is spent, so handleFailedLogWriter() gives up and returns.
    target.logFailure = "at index 13: java.io.IOException: Input/output error";
    clock.addAndGet(1_000L);
    monitor.tick();

    assertThat(target.restarts.get()).as("budget spent: no second restart").isEqualTo(1);
    assertThat(monitor.isFollowerStuckDivergedConfirmed()).isFalse();
  }

  @Test
  void aRequestedShutdownDropsTheConfirmedFlag() {
    final FakeTarget target = new FakeTarget();
    final AtomicLong clock = new AtomicLong(0L);
    final HealthMonitor monitor = monitor(target, clock, true, 0L, 10);
    confirmStuck(monitor, target, clock);

    target.shutdownRequested = true;
    clock.addAndGet(1_000L);
    monitor.tick();

    assertThat(monitor.isFollowerStuckDivergedConfirmed()).isFalse();
  }

  @Test
  void anUnhealthyDivisionDropsTheConfirmedFlag() {
    // Pins the CLOSED/EXCEPTION arm too: today the restart and escalation paths reach resetStreaksAfterRestart(), but
    // the flag must not depend on which sub-arm of handleUnhealthyState() a tick happens to take.
    for (final LifeCycle.State unhealthy : new LifeCycle.State[] { LifeCycle.State.CLOSED, LifeCycle.State.EXCEPTION }) {
      final FakeTarget target = new FakeTarget();
      final AtomicLong clock = new AtomicLong(0L);
      final HealthMonitor monitor = monitor(target, clock, true, 0L, 1);
      confirmStuck(monitor, target, clock);

      target.state.set(unhealthy);
      for (int i = 0; i < 5; i++) {
        clock.addAndGet(1_000L);
        monitor.tick();
        assertThat(monitor.isFollowerStuckDivergedConfirmed()).as("%s tick %d", unhealthy, i).isFalse();
      }
      assertThat(monitor.isCrashLoopEscalated()).as("%s: the crash loop reached its escalated arm", unhealthy).isTrue();
    }
  }

  @Test
  void aStuckStreakDoesNotSurviveAnEarlyReturnAndFireAReformatOnTheFirstTickBack() {
    final FakeTarget target = new FakeTarget();
    final AtomicLong clock = new AtomicLong(0L);
    final HealthMonitor monitor = monitor(target, clock, true, 0L, 10);
    confirmStuck(monitor, target, clock); // streak started at t=0

    // A log-writer incident well past the recovery duration, during which no tick evaluates the stuck signature.
    target.logFailure = "at index 4946: java.io.IOException: No space left on device";
    target.storageWritable = false;
    for (int i = 0; i < 10; i++) {
      clock.addAndGet(1_000L);
      monitor.tick();
    }

    // The writer is healthy again and the follower still looks stuck. This is ONE observation: it may start a streak,
    // not act on one that began before the checks went blind.
    target.logFailure = null;
    clock.addAndGet(1_000L);
    monitor.tick();

    assertThat(target.divergenceRecover.get()).as("no reformat on the first tick back").isZero();
    assertThat(monitor.isFollowerStuckDivergedConfirmed()).as("a single observation is never confirmed").isFalse();

    // The streak re-arms normally: a second observation confirms it, and the reformat waits out a full duration.
    clock.addAndGet(1_000L);
    monitor.tick();
    assertThat(monitor.isFollowerStuckDivergedConfirmed()).isTrue();
    assertThat(target.divergenceRecover.get()).isZero();

    clock.addAndGet(DURATION_MS);
    monitor.tick();
    assertThat(target.divergenceRecover.get()).isEqualTo(1);
  }

  @Test
  void aLagStreakDoesNotSurviveAnEarlyReturnAndFireASnapshotReArmOnTheFirstTickBack() {
    final FakeTarget target = new FakeTarget();
    final AtomicLong clock = new AtomicLong(0L);
    final HealthMonitor monitor = monitor(target, clock, false, 100L, 10);

    target.lagging = true;
    monitor.tick(); // lag streak starts at t=0
    assertThat(target.persistentLagRecover.get()).isZero();

    target.logFailure = "at index 4946: java.io.IOException: No space left on device";
    target.storageWritable = false;
    for (int i = 0; i < 10; i++) {
      clock.addAndGet(1_000L);
      monitor.tick();
    }

    target.logFailure = null;
    clock.addAndGet(1_000L);
    monitor.tick();
    assertThat(target.persistentLagRecover.get()).as("no snapshot re-arm on the first tick back").isZero();

    clock.addAndGet(DURATION_MS);
    monitor.tick();
    assertThat(target.persistentLagRecover.get()).as("a fresh streak still recovers once it has persisted").isEqualTo(1);
  }
}
