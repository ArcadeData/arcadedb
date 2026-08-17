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
package com.arcadedb.server.ha.raft;

import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@link HealthMonitor} tick must drive the bootstrap-divergence verification (issue #6124).
 * <p>
 * A node the bootstrap overwrite guard left with its own copy of a database is not lagging and not
 * diverged in the Raft sense - it applies every entry it is sent and reports zero lag - so none of the
 * monitor's other checks can ever see it. Without its own hook on the tick, nothing would re-examine
 * that copy for the life of the process.
 */
class HealthMonitorBootstrapDivergenceTest {

  private static final class CountingTarget implements HealthMonitor.HealthTarget {
    final AtomicReference<LifeCycle.State> state             = new AtomicReference<>(LifeCycle.State.RUNNING);
    final AtomicInteger                    verifications     = new AtomicInteger();
    volatile boolean                       shutdownRequested = false;

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
    }

    @Override
    public void verifyBootstrapDivergence() {
      verifications.incrementAndGet();
    }
  }

  @Test
  void everyTickVerifiesBootstrapDivergence() {
    final CountingTarget target = new CountingTarget();
    final HealthMonitor monitor = new HealthMonitor(target, 0);

    monitor.tick();
    monitor.tick();

    assertThat(target.verifications.get()).isEqualTo(2);
  }

  @Test
  void anUnhealthyRatisLifecycleStillVerifiesBootstrapDivergence() {
    // The check runs before the lifecycle branch returns early: a node whose Ratis server is CLOSED is
    // exactly the kind that has been sitting on a diverged copy unnoticed.
    final CountingTarget target = new CountingTarget();
    target.state.set(LifeCycle.State.CLOSED);
    final HealthMonitor monitor = new HealthMonitor(target, 0);

    monitor.tick();

    assertThat(target.verifications.get()).isEqualTo(1);
  }

  @Test
  void aShutdownRequestSkipsTheVerification() {
    final CountingTarget target = new CountingTarget();
    target.shutdownRequested = true;
    final HealthMonitor monitor = new HealthMonitor(target, 0);

    monitor.tick();

    assertThat(target.verifications.get()).isZero();
  }
}
