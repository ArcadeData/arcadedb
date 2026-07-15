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

class HealthMonitorTest {

  static final class FakeHealthTarget implements HealthMonitor.HealthTarget {
    final    AtomicReference<LifeCycle.State> state                = new AtomicReference<>(LifeCycle.State.RUNNING);
    final    AtomicInteger                    recoveryCalls        = new AtomicInteger();
    final    AtomicInteger                    persistentLagRecover = new AtomicInteger();
    final    AtomicInteger                    divergenceRecover    = new AtomicInteger();
    final    AtomicInteger                    allowlistRefreshes   = new AtomicInteger();
    volatile boolean                          shutdownRequested    = false;
    volatile boolean                          lagging              = false;
    volatile boolean                          stuckDiverged        = false;

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
      recoveryCalls.incrementAndGet();
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
    public boolean isFollowerStuckDiverged() {
      return stuckDiverged;
    }

    @Override
    public void recoverFromDivergence() {
      divergenceRecover.incrementAndGet();
    }

    @Override
    public void refreshPeerAllowlist() {
      allowlistRefreshes.incrementAndGet();
    }
  }

  @Test
  void tickInvokesReportResyncProgress() {
    final AtomicInteger reportCalls = new AtomicInteger();
    final HealthMonitor.HealthTarget target = new HealthMonitor.HealthTarget() {
      @Override public LifeCycle.State getRaftLifeCycleState() {
        return LifeCycle.State.RUNNING;
      }
      @Override public boolean isShutdownRequested() { return false; }
      @Override public void restartRatisIfNeeded() { }
      @Override public void reportResyncProgress() { reportCalls.incrementAndGet(); }
    };
    final HealthMonitor monitor = new HealthMonitor(target, 1000L);
    monitor.tick();
    assertThat(reportCalls.get()).isEqualTo(1);
  }

  @Test
  void tickDoesNothingWhenStateIsRunning() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    final HealthMonitor monitor = new HealthMonitor(fake, 0);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isZero();
  }

  @Test
  void tickRefreshesPeerAllowlistEveryTickRegardlessOfState() {
    // Issue #4696: the allowlist must reconcile proactively on every tick, on every node, so a peer that
    // restarted with a new pod IP is admitted without first being rejected. It must run even while the
    // local Ratis server is healthy (RUNNING) - the previous behaviour refreshed only on a rejected
    // inbound connection.
    final FakeHealthTarget fake = new FakeHealthTarget();
    final HealthMonitor monitor = new HealthMonitor(fake, 0);
    monitor.tick();
    monitor.tick();
    assertThat(fake.allowlistRefreshes.get()).isEqualTo(2);

    // ...and also when the local server is CLOSED (the recovery branch returns early, but the allowlist
    // refresh happens before the lifecycle-state check).
    fake.state.set(LifeCycle.State.CLOSED);
    monitor.tick();
    assertThat(fake.allowlistRefreshes.get()).isEqualTo(3);
  }

  @Test
  void tickSkipsAllowlistRefreshWhenShutdownRequested() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.shutdownRequested = true;
    final HealthMonitor monitor = new HealthMonitor(fake, 0);
    monitor.tick();
    assertThat(fake.allowlistRefreshes.get()).isZero();
  }

  @Test
  void tickTriggersRecoveryOnClosed() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.state.set(LifeCycle.State.CLOSED);
    final HealthMonitor monitor = new HealthMonitor(fake, 0);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isEqualTo(1);
  }

  @Test
  void tickTriggersRecoveryOnException() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.state.set(LifeCycle.State.EXCEPTION);
    final HealthMonitor monitor = new HealthMonitor(fake, 0);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isEqualTo(1);
  }

  @Test
  void tickSkipsRecoveryWhenShutdownRequested() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.state.set(LifeCycle.State.CLOSED);
    fake.shutdownRequested = true;
    final HealthMonitor monitor = new HealthMonitor(fake, 0);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isZero();
  }

  @Test
  void tickSkipsRecoveryOnNewState() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.state.set(LifeCycle.State.NEW);
    final HealthMonitor monitor = new HealthMonitor(fake, 0);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isZero();
  }

  @Test
  void tickSkipsRecoveryOnStartingState() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.state.set(LifeCycle.State.STARTING);
    final HealthMonitor monitor = new HealthMonitor(fake, 0);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isZero();
  }

  // --- Stale-follower recovery (issue #3893) ---

  private static HealthMonitor staleMonitor(final FakeHealthTarget fake, final AtomicLong clock, final long lagThreshold,
      final long durationMs) {
    final HealthMonitor monitor = new HealthMonitor(fake, 1000, lagThreshold, durationMs);
    monitor.setClock(clock::get);
    return monitor;
  }

  @Test
  void staleFollowerDisabledWhenThresholdZero() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.lagging = true;
    final AtomicLong clock = new AtomicLong(0);
    final HealthMonitor monitor = staleMonitor(fake, clock, 0, 1000); // threshold 0 = disabled
    for (int i = 0; i < 5; i++) {
      clock.addAndGet(10_000);
      monitor.tick();
    }
    assertThat(fake.persistentLagRecover.get()).isZero();
  }

  @Test
  void staleFollowerNotTriggeredOnFirstObservation() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.lagging = true;
    final AtomicLong clock = new AtomicLong(0);
    final HealthMonitor monitor = staleMonitor(fake, clock, 1000, 5000);
    monitor.tick(); // first observation only starts the streak
    assertThat(fake.persistentLagRecover.get()).isZero();
  }

  @Test
  void staleFollowerTriggersAfterDuration() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.lagging = true;
    final AtomicLong clock = new AtomicLong(0);
    final HealthMonitor monitor = staleMonitor(fake, clock, 1000, 5000);

    monitor.tick();                 // t=0: start streak
    clock.set(3000);
    monitor.tick();                 // t=3000: still within duration
    assertThat(fake.persistentLagRecover.get()).isZero();

    clock.set(6000);
    monitor.tick();                 // t=6000: lag persisted >= 5000ms -> recover
    assertThat(fake.persistentLagRecover.get()).isEqualTo(1);
  }

  @Test
  void staleFollowerStreakResetsWhenLagClears() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.lagging = true;
    final AtomicLong clock = new AtomicLong(0);
    final HealthMonitor monitor = staleMonitor(fake, clock, 1000, 5000);

    monitor.tick();                 // t=0: start streak
    clock.set(3000);
    fake.lagging = false;           // transient catch-up: lag gone
    monitor.tick();                 // resets streak
    fake.lagging = true;
    clock.set(7000);
    monitor.tick();                 // restarts streak (only 0ms elapsed since restart)
    assertThat(fake.persistentLagRecover.get()).isZero();

    clock.set(13_000);
    monitor.tick();                 // 6000ms since restart -> recover
    assertThat(fake.persistentLagRecover.get()).isEqualTo(1);
  }

  @Test
  void staleFollowerSkippedWhenShutdownRequested() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.lagging = true;
    fake.shutdownRequested = true;
    final AtomicLong clock = new AtomicLong(0);
    final HealthMonitor monitor = staleMonitor(fake, clock, 1000, 0);
    monitor.tick();
    clock.set(10_000);
    monitor.tick();
    assertThat(fake.persistentLagRecover.get()).isZero();
  }

  @Test
  void staleFollowerStreakResetByUnhealthyState() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.lagging = true;
    final AtomicLong clock = new AtomicLong(0);
    final HealthMonitor monitor = staleMonitor(fake, clock, 1000, 5000);

    monitor.tick();                 // t=0: start streak
    clock.set(3000);
    fake.state.set(LifeCycle.State.CLOSED);
    monitor.tick();                 // unhealthy: restart path runs, streak reset
    assertThat(fake.recoveryCalls.get()).isEqualTo(1);

    fake.state.set(LifeCycle.State.RUNNING);
    clock.set(7000);
    monitor.tick();                 // streak restarts here, not enough elapsed
    assertThat(fake.persistentLagRecover.get()).isZero();
  }

  // --- Stuck-divergence recovery (issue #4741) ---

  private static HealthMonitor stuckMonitor(final FakeHealthTarget fake, final AtomicLong clock, final long durationMs,
      final boolean enabled) {
    return stuckMonitor(fake, clock, durationMs, enabled, 0);
  }

  private static HealthMonitor stuckMonitor(final FakeHealthTarget fake, final AtomicLong clock, final long durationMs,
      final boolean enabled, final int maxReformats) {
    final HealthMonitor monitor = new HealthMonitor(fake, 1000, 0L, durationMs, enabled, maxReformats);
    monitor.setClock(clock::get);
    return monitor;
  }

  /** Drives one full reformat cycle: one tick to arm the streak, then advance past the duration and tick to act. */
  private static void runStuckCycle(final HealthMonitor monitor, final AtomicLong clock, final long durationMs) {
    monitor.tick();
    clock.addAndGet(durationMs + 1000);
    monitor.tick();
  }

  @Test
  void divergenceRecoveryDisabledByLegacyConstructor() {
    // The 4-arg constructor (used before #4741) must leave diverged-follower recovery OFF.
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.stuckDiverged = true;
    final AtomicLong clock = new AtomicLong(0);
    final HealthMonitor monitor = new HealthMonitor(fake, 1000, 0L, 1000L);
    monitor.setClock(clock::get);
    for (int i = 0; i < 5; i++) {
      clock.addAndGet(10_000);
      monitor.tick();
    }
    assertThat(fake.divergenceRecover.get()).isZero();
  }

  @Test
  void divergenceRecoveryDisabledWhenFlagFalse() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.stuckDiverged = true;
    final AtomicLong clock = new AtomicLong(0);
    final HealthMonitor monitor = stuckMonitor(fake, clock, 1000, false);
    for (int i = 0; i < 5; i++) {
      clock.addAndGet(10_000);
      monitor.tick();
    }
    assertThat(fake.divergenceRecover.get()).isZero();
  }

  @Test
  void divergenceNotTriggeredOnFirstObservation() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.stuckDiverged = true;
    final AtomicLong clock = new AtomicLong(0);
    final HealthMonitor monitor = stuckMonitor(fake, clock, 5000, true);
    monitor.tick(); // first observation only starts the streak
    assertThat(fake.divergenceRecover.get()).isZero();
  }

  @Test
  void divergenceTriggersAfterDuration() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.stuckDiverged = true;
    final AtomicLong clock = new AtomicLong(0);
    final HealthMonitor monitor = stuckMonitor(fake, clock, 5000, true);

    monitor.tick();                 // t=0: start streak
    clock.set(3000);
    monitor.tick();                 // t=3000: still within duration
    assertThat(fake.divergenceRecover.get()).isZero();

    clock.set(6000);
    monitor.tick();                 // t=6000: divergence persisted >= 5000ms -> recover
    assertThat(fake.divergenceRecover.get()).isEqualTo(1);
  }

  @Test
  void divergenceStreakResetsWhenConditionClears() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.stuckDiverged = true;
    final AtomicLong clock = new AtomicLong(0);
    final HealthMonitor monitor = stuckMonitor(fake, clock, 5000, true);

    monitor.tick();                 // t=0: start streak
    clock.set(3000);
    fake.stuckDiverged = false;     // transient: divergence cleared (e.g. brief election window)
    monitor.tick();                 // resets streak
    fake.stuckDiverged = true;
    clock.set(7000);
    monitor.tick();                 // restarts streak (0ms elapsed since restart)
    assertThat(fake.divergenceRecover.get()).isZero();

    clock.set(13_000);
    monitor.tick();                 // 6000ms since restart -> recover
    assertThat(fake.divergenceRecover.get()).isEqualTo(1);
  }

  @Test
  void divergenceStreakResetByUnhealthyState() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.stuckDiverged = true;
    final AtomicLong clock = new AtomicLong(0);
    final HealthMonitor monitor = stuckMonitor(fake, clock, 5000, true);

    monitor.tick();                 // t=0: start streak
    clock.set(3000);
    fake.state.set(LifeCycle.State.CLOSED);
    monitor.tick();                 // unhealthy: restart path runs, streak reset
    assertThat(fake.recoveryCalls.get()).isEqualTo(1);

    fake.state.set(LifeCycle.State.RUNNING);
    clock.set(7000);
    monitor.tick();                 // streak restarts here, not enough elapsed
    assertThat(fake.divergenceRecover.get()).isZero();
  }

  @Test
  void divergenceSkippedWhenShutdownRequested() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.stuckDiverged = true;
    fake.shutdownRequested = true;
    final AtomicLong clock = new AtomicLong(0);
    final HealthMonitor monitor = stuckMonitor(fake, clock, 0, true);
    monitor.tick();
    clock.set(10_000);
    monitor.tick();
    assertThat(fake.divergenceRecover.get()).isZero();
  }

  @Test
  void divergenceReformatBudgetCapsRepeatedReformats() {
    // A node whose divergence keeps reproducing must not reformat forever: the budget caps it.
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.stuckDiverged = true; // stays stuck across every cycle (pathological re-divergence)
    final AtomicLong clock = new AtomicLong(0);
    final HealthMonitor monitor = stuckMonitor(fake, clock, 5000, true, 2);

    for (int i = 0; i < 6; i++)
      runStuckCycle(monitor, clock, 5000);

    assertThat(fake.divergenceRecover.get())
        .as("reformats must be capped at the configured maximum")
        .isEqualTo(2);
  }

  @Test
  void divergenceUnboundedWhenMaxReformatsZero() {
    // max=0 disables the breaker: every persisted episode reformats.
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.stuckDiverged = true;
    final AtomicLong clock = new AtomicLong(0);
    final HealthMonitor monitor = stuckMonitor(fake, clock, 5000, true, 0);

    for (int i = 0; i < 4; i++)
      runStuckCycle(monitor, clock, 5000);

    assertThat(fake.divergenceRecover.get()).isEqualTo(4);
  }

  // --- Crash-loop escalation (issue #5291) ---

  private static HealthMonitor crashLoopMonitor(final FakeHealthTarget fake, final boolean divergedEnabled,
      final int crashLoopThreshold) {
    // interval 0 so tick() runs synchronously; no clock needed - crash-loop escalation counts ticks, not time.
    return new HealthMonitor(fake, 1000, 0L, 5000L, divergedEnabled, 2, crashLoopThreshold);
  }

  @Test
  void crashLoopDisabledWhenThresholdZero() {
    // Threshold 0 preserves the legacy behaviour: every CLOSED tick just restarts, forever, no escalation.
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.state.set(LifeCycle.State.CLOSED);
    final HealthMonitor monitor = crashLoopMonitor(fake, true, 0);
    for (int i = 0; i < 20; i++)
      monitor.tick();
    assertThat(fake.recoveryCalls.get()).isEqualTo(20);
    assertThat(fake.divergenceRecover.get()).isZero();
  }

  @Test
  void crashLoopEscalatesToReformatThenGivesUp() {
    // A CLOSED restart that never sticks: after the threshold, escalate once to a reformat; if it still
    // crash-loops, stop restarting entirely (give up) instead of looping forever.
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.state.set(LifeCycle.State.CLOSED);
    final HealthMonitor monitor = crashLoopMonitor(fake, true, 3);

    // Ticks 1..3 restart normally (streak <= threshold).
    monitor.tick();
    monitor.tick();
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isEqualTo(3);
    assertThat(fake.divergenceRecover.get()).isZero();

    // Tick 4 crosses the threshold: escalate to a one-shot reformat (no plain restart this tick).
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isEqualTo(3);
    assertThat(fake.divergenceRecover.get()).isEqualTo(1);

    // The reformat reset the streak; ticks 5..7 restart normally again while it fails to stick.
    monitor.tick();
    monitor.tick();
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isEqualTo(6);

    // Tick 8 crosses the threshold again with the reformat already spent: give up (SEVERE, once).
    monitor.tick();
    assertThat(fake.divergenceRecover.get()).as("reformat is one-shot per episode").isEqualTo(1);

    // All further ticks are no-ops: neither restart nor reformat - the churn is stopped.
    for (int i = 0; i < 10; i++)
      monitor.tick();
    assertThat(fake.recoveryCalls.get()).as("no more restarts after give-up").isEqualTo(6);
    assertThat(fake.divergenceRecover.get()).isEqualTo(1);
  }

  @Test
  void crashLoopGivesUpDirectlyWhenDivergenceRecoveryDisabled() {
    // With divergence recovery disabled there is no reformat step: escalation goes straight to give-up.
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.state.set(LifeCycle.State.CLOSED);
    final HealthMonitor monitor = crashLoopMonitor(fake, false, 3);

    for (int i = 0; i < 3; i++)
      monitor.tick(); // normal restarts
    assertThat(fake.recoveryCalls.get()).isEqualTo(3);

    monitor.tick(); // crosses threshold -> give up (no reformat available)
    for (int i = 0; i < 5; i++)
      monitor.tick();
    assertThat(fake.recoveryCalls.get()).as("stopped restarting at give-up").isEqualTo(3);
    assertThat(fake.divergenceRecover.get()).isZero();
  }

  @Test
  void crashLoopStreakResetsWhenHealthyAgain() {
    // A restart that sticks (node returns to RUNNING) before the threshold must never escalate: the streak
    // resets, so a later genuinely-new incident starts counting from zero.
    final FakeHealthTarget fake = new FakeHealthTarget();
    final HealthMonitor monitor = crashLoopMonitor(fake, true, 3);

    for (int cycle = 0; cycle < 5; cycle++) {
      fake.state.set(LifeCycle.State.CLOSED);
      monitor.tick();
      monitor.tick(); // two CLOSED ticks (below threshold 3)
      fake.state.set(LifeCycle.State.RUNNING);
      monitor.tick(); // recovered -> streak resets
    }
    assertThat(fake.recoveryCalls.get()).as("every CLOSED tick restarted, none escalated").isEqualTo(10);
    assertThat(fake.divergenceRecover.get()).isZero();
  }

  @Test
  void divergenceBudgetReArmsAfterHealthyPeriod() {
    // Once the follower looks healthy for 5x the duration, the prior episode is forgotten and the
    // budget re-arms for a genuinely new divergence later.
    final FakeHealthTarget fake = new FakeHealthTarget();
    fake.stuckDiverged = true;
    final AtomicLong clock = new AtomicLong(0);
    final long duration = 5000;
    final HealthMonitor monitor = stuckMonitor(fake, clock, duration, true, 2);

    runStuckCycle(monitor, clock, duration);
    runStuckCycle(monitor, clock, duration);
    assertThat(fake.divergenceRecover.get()).as("budget consumed").isEqualTo(2);

    // Follower becomes healthy; stay healthy past the reset window (5x duration = 25000ms).
    fake.stuckDiverged = false;
    monitor.tick();                                   // start healthy streak
    clock.addAndGet(duration * 5 + 1000);
    monitor.tick();                                   // healthy long enough -> budget re-arms

    // A new divergence reformats again.
    fake.stuckDiverged = true;
    runStuckCycle(monitor, clock, duration);
    assertThat(fake.divergenceRecover.get())
        .as("budget re-armed after a sustained healthy period")
        .isEqualTo(3);
  }
}
