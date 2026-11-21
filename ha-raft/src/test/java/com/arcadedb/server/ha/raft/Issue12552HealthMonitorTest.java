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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Regression tests for issue #12552: after an {@code OutOfMemoryError} the HealthMonitor logged "recovery failed (attempt
 * 1/10)" and then nothing for an hour, while the node stayed up out of the cluster and {@code /api/v1/ready} kept
 * answering 204.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12552HealthMonitorTest {

  /** An error whose report fails as well, as an {@code OutOfMemoryError} does when the logging that reports it allocates. */
  private static final class ErrorWhoseReportFails extends Error {
    @Override
    public String getMessage() {
      throw new OutOfMemoryError("Java heap space (thrown again while reporting the first one)");
    }
  }

  /** The scheduled loop: one throwable escaping a run cancels every later run of scheduleWithFixedDelay. */
  @Nested
  @Timeout(value = 2, unit = TimeUnit.MINUTES) // hang detector only
  class TickLoopSurvivesErrors {

    // Regression for #12552: the tick loop died with the second error, and attempts 2..N never ran
    @Test
    void attemptsKeepRunningAfterATickWhoseErrorCannotBeReported() {
      final AtomicBoolean thrown = new AtomicBoolean();
      final HealthMonitorTest.FakeHealthTarget target = new HealthMonitorTest.FakeHealthTarget() {
        @Override
        public LifeCycle.State getRaftLifeCycleState() {
          if (thrown.compareAndSet(false, true))
            throw new ErrorWhoseReportFails();
          return LifeCycle.State.CLOSED;
        }
      };
      final HealthMonitor monitor = new HealthMonitor(target, 20L);
      monitor.start();
      try {
        await().atMost(30, TimeUnit.SECONDS).until(() -> target.recoveryCalls.get() >= 3);
        assertThat(thrown.get()).as("the first tick did throw").isTrue();
        assertThat(monitor.describeTickLoopFailure(60_000L)).as("the loop is alive").isNull();
      } finally {
        monitor.stop();
      }
    }

    // Regression for #12552
    @Test
    void aTickWhoseErrorCannotBeReportedStillReturnsAndRecordsItsEnd() {
      final HealthMonitorTest.FakeHealthTarget target = new HealthMonitorTest.FakeHealthTarget() {
        @Override
        public LifeCycle.State getRaftLifeCycleState() {
          throw new ErrorWhoseReportFails();
        }
      };
      final HealthMonitor monitor = new HealthMonitor(target, 0L);
      monitor.setClock(() -> 4242L);

      monitor.tickSafely(); // before the fix: the OutOfMemoryError of the report escaped here

      assertThat(monitor.getLastTickEndedMs()).isEqualTo(4242L);
    }
  }

  /** A loop that is alive but stuck inside one tick recovers nothing either: readiness must say so. */
  @Nested
  @Timeout(value = 2, unit = TimeUnit.MINUTES) // hang detector only
  class StalledTickLoop {

    // Regression for #12552
    @Test
    void aTickStuckPastTheBoundIsReportedAndClearsOnceItEnds() throws Exception {
      final CountDownLatch entered = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final HealthMonitorTest.FakeHealthTarget target = new HealthMonitorTest.FakeHealthTarget() {
        @Override
        public void restartRatisIfNeeded() {
          entered.countDown();
          try {
            release.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          super.restartRatisIfNeeded();
        }
      };
      target.state.set(LifeCycle.State.CLOSED);
      final AtomicLong now = new AtomicLong(1_000_000L);
      final HealthMonitor monitor = new HealthMonitor(target, 10L);
      monitor.setClock(now::get);
      monitor.start();
      try {
        assertThat(entered.await(30, TimeUnit.SECONDS)).as("a tick is inside the restart").isTrue();
        assertThat(monitor.describeTickLoopFailure(5_000L)).as("within the bound").isNull();

        now.addAndGet(60_000L);
        assertThat(monitor.describeTickLoopFailure(5_000L)).as("past the bound").contains("has completed no tick");

        release.countDown();
        await().atMost(30, TimeUnit.SECONDS).until(() -> monitor.getLastTickEndedMs() == now.get());
        assertThat(monitor.describeTickLoopFailure(5_000L)).as("the tick ended").isNull();
      } finally {
        release.countDown();
        monitor.stop();
      }
    }

    // Regression for #12552
    @Test
    void aStoppedOrDisabledMonitorReportsNothing() {
      final HealthMonitor disabled = new HealthMonitor(new HealthMonitorTest.FakeHealthTarget(), 0L);
      disabled.start();
      assertThat(disabled.describeTickLoopFailure(0L)).isNull();

      final HealthMonitor stopped = new HealthMonitor(new HealthMonitorTest.FakeHealthTarget(), 10_000L);
      stopped.start();
      stopped.stop();
      assertThat(stopped.describeTickLoopFailure(0L)).isNull();
    }
  }

  /**
   * The old Ratis server never finished closing. Every restart fails at once beside it, and those failures must spend
   * {@code arcadedb.ha.ratisRestartMaxRetries}, whose end stops the server, instead of feeding the crash-loop ladder,
   * whose give-up parks the node half-alive for good.
   */
  @Nested
  class StuckCloseEscalation {

    // Regression for #12552
    @Test
    void aStuckCloseKeepsAttemptingInsteadOfGivingUpOnTheCrashLoop() {
      final HealthMonitorTest.FakeHealthTarget target = new HealthMonitorTest.FakeHealthTarget() {
        @Override
        public boolean isRatisCloseStuck() {
          return true;
        }
      };
      target.state.set(LifeCycle.State.CLOSED);
      final int threshold = 3;
      final HealthMonitor monitor = new HealthMonitor(target, 0L, 0L, 0L, false, 0, threshold);

      for (int i = 0; i < 10; i++)
        monitor.tick();

      assertThat(target.recoveryCalls.get()).as("every tick attempts the restart, which counts toward stopping the server")
          .isEqualTo(10);
      assertThat(monitor.isCrashLoopEscalated()).as("the crash-loop give-up must not park the node").isFalse();
    }

    // Regression for #12552: the crash-loop ladder is unchanged for a restart that runs but does not stick
    @Test
    void withoutAStuckCloseTheCrashLoopLadderStillGivesUp() {
      final HealthMonitorTest.FakeHealthTarget target = new HealthMonitorTest.FakeHealthTarget();
      target.state.set(LifeCycle.State.CLOSED);
      final HealthMonitor monitor = new HealthMonitor(target, 0L, 0L, 0L, false, 0, 3);

      for (int i = 0; i < 10; i++)
        monitor.tick();

      assertThat(target.recoveryCalls.get()).isEqualTo(3);
      assertThat(monitor.isCrashLoopEscalated()).isTrue();
    }
  }

  /** What the readiness probe reads from the Raft layer. */
  @Nested
  @Timeout(value = 2, unit = TimeUnit.MINUTES) // hang detector only
  class ReplicationFailureReason {

    // Regression for #12552: a close of the old Ratis server that never ended
    @Test
    void aStuckRatisCloseIsReported() throws Exception {
      final RaftHAServer raft = detachedServer();
      final CountDownLatch release = new CountDownLatch(1);
      final Thread closer = new Thread(() -> {
        try {
          release.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }, "issue12552-stuck-close");
      closer.setDaemon(true);
      closer.start();
      try {
        assertThat(raft.getReplicationFailureReason()).isNull();
        assertThat(raft.isRatisCloseStuck()).isFalse();

        setField(raft, "stuckRatisClose", closer);

        assertThat(raft.isRatisCloseStuck()).isTrue();
        assertThat(raft.getReplicationFailureReason()).contains("issue12552-stuck-close");
        assertThat(raft.isReadyForTraffic(Long.MAX_VALUE)).isFalse();
      } finally {
        release.countDown();
        closer.join(TimeUnit.SECONDS.toMillis(30));
      }
      assertThat(raft.isRatisCloseStuck()).as("the close ended").isFalse();
      assertThat(raft.getReplicationFailureReason()).isNull();
    }

    // Regression for #12552: a dead StateMachineUpdater, under a division that can still report RUNNING
    @Test
    void aDeadStateMachineUpdaterIsReported() throws Exception {
      final RaftHAServer raft = detachedServer();
      final ArcadeStateMachine stateMachine = new ArcadeStateMachine();
      final Thread updater = new Thread(() -> {
      }, "issue12552-StateMachineUpdater");
      updater.start();
      updater.join(TimeUnit.SECONDS.toMillis(30));
      final Field applyThread = ArcadeStateMachine.class.getDeclaredField("applyThread");
      applyThread.setAccessible(true);
      applyThread.set(stateMachine, updater);
      setField(raft, "stateMachine", stateMachine);

      assertThat(raft.getReplicationFailureReason()).contains("issue12552-StateMachineUpdater").contains("terminated");

      setField(raft, "shutdownRequested", true);
      assertThat(raft.getReplicationFailureReason()).as("a shutdown ends the updater on purpose").isNull();
    }

    // Regression for #12552
    @Test
    void theStallBoundCoversTwoCloseTimeoutsAndTenIntervals() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481");
      config.setValue(GlobalConfiguration.HA_RATIS_CLOSE_TIMEOUT_MS, 30_000L);
      config.setValue(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL, 3_000L);
      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
      assertThat(new RaftHAServer(arcadeServer, config).healthMonitorStallBoundMs()).isEqualTo(120_000L);

      config.setValue(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL, 60_000L);
      assertThat(new RaftHAServer(arcadeServer, config).healthMonitorStallBoundMs()).isEqualTo(600_000L);
    }

    private static RaftHAServer detachedServer() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");
      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
      return new RaftHAServer(arcadeServer, config);
    }

    private static void setField(final Object target, final String name, final Object value) throws Exception {
      final Field field = RaftHAServer.class.getDeclaredField(name);
      field.setAccessible(true);
      field.set(target, value);
    }
  }
}
