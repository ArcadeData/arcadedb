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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8652: a {@code StateMachineUpdater} that died from any throwable was only noticed through
 * the lifecycle state it left behind, which a zombie corrupts (the closing division stayed {@code CLOSING}, or the node
 * reported {@code RUNNING} again after rejoining while it rejected every append). The state machine now says directly
 * that the thread which applies entries has terminated, and the health monitor restarts the node in place when it sees
 * that on consecutive ticks.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8652DeadStateMachineUpdaterTest {

  // -- the state machine names a dead updater ---------------------------------------------------------------------

  @Test
  void aStateMachineThatHasSeenNoUpdaterReportsNoneDead() {
    assertThat(new ArcadeStateMachine().describeDeadApplyThread()).isNull();
  }

  @Test
  void aLiveUpdaterIsNotReportedDead() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final Object release = new Object();
    final Thread updater = new Thread(() -> {
      sm.notifyTermIndexUpdated(1L, 1L);
      synchronized (release) {
        try {
          release.wait();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }, "test-updater-alive");
    updater.start();
    try {
      while (sm.getLastAppliedTermIndex() == null)
        Thread.sleep(5);
      assertThat(sm.describeDeadApplyThread()).isNull();
    } finally {
      updater.interrupt();
      updater.join();
    }
  }

  @Test
  void anUpdaterThatDiedIsReportedByName() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final Thread updater = new Thread(() -> sm.notifyTermIndexUpdated(1L, 1L), "test-updater-dead");
    updater.start();
    updater.join();

    assertThat(sm.describeDeadApplyThread()).contains("test-updater-dead").contains("terminated");
  }

  // -- the health monitor recovers it ----------------------------------------------------------------------------

  @Test
  void aDeadUpdaterSeenOnConsecutiveTicksRestartsRatis() {
    final HealthMonitorTest.FakeHealthTarget target = new HealthMonitorTest.FakeHealthTarget();
    target.deadUpdater = "the Ratis StateMachineUpdater thread 'x' has terminated";
    final HealthMonitor monitor = new HealthMonitor(target, 0);

    monitor.tick();
    assertThat(target.recoveryCalls.get()).as("one sighting is a shutdown racing the tick").isZero();

    monitor.tick();
    assertThat(target.recoveryCalls.get()).isEqualTo(1);
  }

  @Test
  void aDeadUpdaterSeenOnceAndThenGoneIsForgotten() {
    final HealthMonitorTest.FakeHealthTarget target = new HealthMonitorTest.FakeHealthTarget();
    final HealthMonitor monitor = new HealthMonitor(target, 0);

    target.deadUpdater = "gone";
    monitor.tick();
    target.deadUpdater = null;
    monitor.tick();
    target.deadUpdater = "gone";
    monitor.tick();

    assertThat(target.recoveryCalls.get()).as("the streak restarts after a healthy tick").isZero();
  }

  @Test
  void aDeadUpdaterOnAHealthyLookingNodeSuppressesTheOtherChecksThatTick() {
    final HealthMonitorTest.FakeHealthTarget target = new HealthMonitorTest.FakeHealthTarget();
    target.deadUpdater = "gone";
    target.lagging = true;
    final HealthMonitor monitor = new HealthMonitor(target, 0);

    monitor.tick();
    monitor.tick();

    assertThat(target.persistentLagRecover.get()).as("a resync cannot help a node that applies nothing").isZero();
    assertThat(target.recoveryCalls.get()).isEqualTo(1);
  }
}
