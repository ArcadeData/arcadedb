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
    volatile boolean                          shutdownRequested    = false;
    volatile boolean                          lagging              = false;

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
  }

  @Test
  void tickDoesNothingWhenStateIsRunning() {
    final FakeHealthTarget fake = new FakeHealthTarget();
    final HealthMonitor monitor = new HealthMonitor(fake, 0);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).isZero();
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
}
