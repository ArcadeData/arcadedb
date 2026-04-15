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
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class HealthMonitorTest {

  static final class FakeHealthTarget implements HealthMonitor.HealthTarget {
    final    AtomicReference<LifeCycle.State> state             = new AtomicReference<>(LifeCycle.State.RUNNING);
    final    AtomicInteger                    recoveryCalls     = new AtomicInteger();
    volatile boolean                          shutdownRequested = false;

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
}
