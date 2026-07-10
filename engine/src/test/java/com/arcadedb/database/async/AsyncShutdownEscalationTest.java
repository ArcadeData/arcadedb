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
package com.arcadedb.database.async;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the #5062 review, round 4 (point 1): {@code shutdownThreadsLocked} sent
 * {@code interrupt()} only when the {@code offer(FORCE_EXIT, 1s)} FAILED. But the marker can be
 * consumed inside {@code offerHelping} - which sets {@code forceShutdown} and keeps looping to hand
 * off the current task's follow-up - so a worker parked on a wedged peer's full queue re-checks the
 * flag only when the hand-off resolves (up to the stall backstop, ~60s with defaults). The offer
 * SUCCEEDED, so no interrupt was ever sent: {@code join} timed out, a WARNING was logged, and
 * {@code close()} returned with the worker still alive. The fixed shutdown escalates: a worker
 * still alive after the grace period is interrupted and re-joined.
 */
class AsyncShutdownEscalationTest extends TestHelper {

  @Test
  @Timeout(60)
  void closeInterruptsWorkerParkedInHelpingLoopAfterConsumingForceExit() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE, 4); // 2 PER WORKER
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(2);
    // Force thread re-creation so the queue size above is picked up regardless of the previous level.
    async.setTransactionUseWAL(true);
    // Shrink the shutdown grace period so the escalation (not the wedge ceiling) decides the timing.
    async.shutdownJoinTimeoutMs = 500;

    final CountDownLatch wedgeStarted = new CountDownLatch(1);
    final CountDownLatch releaseWedge = new CountDownLatch(1);
    final CountDownLatch outerStarted = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    try {
      // Worker 1: wedged in user code that SWALLOWS interrupts (so the close() interrupt for its
      // full queue does not free it), keeping the hand-off target full for the whole shutdown.
      async.scheduleTask(1, interruptSwallowingWedge(wedgeStarted, releaseWedge), true, 0);
      assertThat(wedgeStarted.await(5, TimeUnit.SECONDS)).isTrue();
      for (int i = 0; i < 2; i++)
        assertThat(async.scheduleTask(1, noOpTask(), false, 0)).isTrue();

      // Worker 0: parks in offerHelping handing a follow-up to worker 1's full queue. Its own queue
      // is empty, so close()'s FORCE_EXIT offer SUCCEEDS and the helping loop consumes the marker.
      async.scheduleTask(0, new DatabaseAsyncTask() {
        @Override
        public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
          outerStarted.countDown();
          try {
            if (!proceed.await(20, TimeUnit.SECONDS))
              return;
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
          async.scheduleTask(1, noOpTask(), true, 0);
        }

        @Override
        public boolean requiresActiveTx() {
          return false;
        }
      }, true, 0);
      assertThat(outerStarted.await(5, TimeUnit.SECONDS)).isTrue();
      proceed.countDown();
      // Let worker 0 enter the helping loop before shutting down.
      Thread.sleep(300);

      final long start = System.currentTimeMillis();
      async.close();
      assertThat(System.currentTimeMillis() - start).as("close() must stay bounded").isLessThan(15_000);

      // The escalation must not leave worker 0 behind: without it, worker 0 kept looping in
      // offerHelping (forceShutdown set but unchecked until the hand-off resolves, and the wedged
      // peer never resolves it). Worker 1 itself legitimately survives - its user code swallows
      // interrupts - which is exactly why worker 0's park never frees on its own.
      final long deadline = System.currentTimeMillis() + 5_000;
      while (helpingWorkerAlive(db) && System.currentTimeMillis() < deadline)
        Thread.sleep(50);
      assertThat(helpingWorkerAlive(db))
          .as("close() must not return leaving the help-waiting worker alive")
          .isFalse();
    } finally {
      releaseWedge.countDown();
      proceed.countDown();
    }
  }

  private static boolean helpingWorkerAlive(final DatabaseInternal db) {
    final String name = "AsyncExecutor-" + db.getName() + "-0";
    return Thread.getAllStackTraces().keySet().stream().anyMatch(t -> t.getName().equals(name) && t.isAlive());
  }

  private static DatabaseAsyncTask interruptSwallowingWedge(final CountDownLatch started, final CountDownLatch release) {
    return new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        started.countDown();
        final long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline)
          try {
            if (release.await(Math.max(1, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS))
              return;
          } catch (final InterruptedException e) {
            // SIMULATES USER CODE THAT SWALLOWS INTERRUPTS: KEEP WAITING
          }
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    };
  }

  private static DatabaseAsyncTask noOpTask() {
    return new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        // NO ACTIONS
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    };
  }
}
