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

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #7037 (follow-up to #5345): a follower whose Raft log writer failed persistently -
 * {@code No space left on device} being the reported cause - stays {@code RUNNING} while rejecting every append,
 * and nothing recovered it without an operator restart. The state machine now keeps the failure Ratis reports
 * through {@code notifyLogFailed}, and the health monitor restarts the server in place once the volume has room,
 * bounded per episode so a failure that is not about space does not restart the node on every tick forever.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7037LogWriterRecoveryTest {

  /** A target whose log writer failure and volume state the test drives; a restart clears the failure unless pinned. */
  static final class FakeTarget implements HealthMonitor.HealthTarget {
    final    AtomicInteger restarts        = new AtomicInteger();
    volatile String        logFailure      = null;
    volatile boolean       storageWritable = true;
    volatile boolean       failureSurvivesRestart = false;

    @Override
    public LifeCycle.State getRaftLifeCycleState() {
      return LifeCycle.State.RUNNING;
    }

    @Override
    public boolean isShutdownRequested() {
      return false;
    }

    @Override
    public void restartRatisIfNeeded() {
      restarts.incrementAndGet();
      if (!failureSurvivesRestart)
        logFailure = null; // a restart builds a fresh state machine, which is what clears the mark
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

  private static HealthMonitor monitor(final FakeTarget target, final int restartThreshold, final AtomicLong clock) {
    final HealthMonitor monitor = new HealthMonitor(target, 1000L, 0L, 0L, false, 0, restartThreshold);
    monitor.setClock(clock::get);
    return monitor;
  }

  @Test
  void healthyLogWriterNeverRestarts() {
    final FakeTarget target = new FakeTarget();
    final HealthMonitor monitor = monitor(target, 10, new AtomicLong(1_000L));

    monitor.tick();
    monitor.tick();

    assertThat(target.restarts.get()).isZero();
  }

  @Test
  void restartIsDeferredWhileTheVolumeIsStillFullAndFiredOnceItHasRoom() {
    final FakeTarget target = new FakeTarget();
    final AtomicLong clock = new AtomicLong(1_000L);
    final HealthMonitor monitor = monitor(target, 10, clock);

    target.logFailure = "at index 4946: java.io.IOException: No space left on device";
    target.storageWritable = false;
    monitor.tick();
    monitor.tick();
    assertThat(target.restarts.get()).as("a restart on a full volume wedges again at the first append").isZero();

    // The periodic compaction freed the segments below the applied index.
    target.storageWritable = true;
    monitor.tick();
    assertThat(target.restarts.get()).isEqualTo(1);
    assertThat(target.logFailure).isNull();

    monitor.tick();
    assertThat(target.restarts.get()).as("the fresh state machine carries no failure: nothing more to do").isEqualTo(1);
  }

  @Test
  void restartsAreBoundedPerEpisodeWhenTheFailureIsNotAboutSpace() {
    final FakeTarget target = new FakeTarget();
    final AtomicLong clock = new AtomicLong(1_000L);
    final HealthMonitor monitor = monitor(target, 2, clock);

    target.logFailure = "at index 12: java.io.IOException: Input/output error";
    target.failureSurvivesRestart = true; // a dying disk: every restart fails the same way

    for (int i = 0; i < 6; i++) {
      monitor.tick();
      clock.addAndGet(1_000L);
    }
    assertThat(target.restarts.get()).as("the crash-loop budget bounds the in-place restarts").isEqualTo(2);

    // After a quiet window the episode is over and recovery is attempted again.
    clock.addAndGet(HealthMonitor.LOG_FAILURE_EPISODE_RESET_MS);
    monitor.tick();
    assertThat(target.restarts.get()).isEqualTo(3);
  }

  @Test
  void aZeroThresholdKeepsTheLegacyUnboundedBehaviour() {
    final FakeTarget target = new FakeTarget();
    final HealthMonitor monitor = monitor(target, 0, new AtomicLong(1_000L));

    target.logFailure = "at index 12: java.io.IOException: Input/output error";
    target.failureSurvivesRestart = true;
    for (int i = 0; i < 4; i++)
      monitor.tick();

    assertThat(target.restarts.get()).isEqualTo(4);
  }

  @Test
  void stateMachineKeepsTheFirstFailureRatisReports() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    assertThat(sm.getRaftLogFailure()).isNull();

    sm.notifyLogFailed(new IOException("No space left on device"), LogEntryProto.newBuilder().setTerm(3).setIndex(4946).build());
    final ArcadeStateMachine.RaftLogFailure first = sm.getRaftLogFailure();
    assertThat(first).isNotNull();
    assertThat(first.index()).isEqualTo(4946L);
    assertThat(first.describe()).contains("at index 4946").contains("No space left on device");

    // Ratis reports every later task against the pinned exception; the first failure is the one that matters.
    sm.notifyLogFailed(new IOException("Log already failed at index 4946"), LogEntryProto.newBuilder().setTerm(3).setIndex(4947).build());
    assertThat(sm.getRaftLogFailure()).isSameAs(first);

    // A whole-segment failure carries no entry.
    final ArcadeStateMachine other = new ArcadeStateMachine();
    other.notifyLogFailed(new IOException("No space left on device"), null);
    assertThat(other.getRaftLogFailure().index()).isEqualTo(-1L);
    assertThat(other.getRaftLogFailure().describe()).startsWith("on a log segment");
  }
}
