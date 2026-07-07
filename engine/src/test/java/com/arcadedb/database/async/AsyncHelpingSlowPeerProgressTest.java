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

import static com.arcadedb.database.async.AsyncTestTasks.awaitTask;
import static com.arcadedb.database.async.AsyncTestTasks.countingTask;
import static com.arcadedb.database.async.AsyncTestTasks.noOpTask;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Positive-case guard for the helping-loop backstop (#5072 review round 1, point 2): a peer that
 * keeps COMPLETING tasks while its queue stays full must never trip the backstop, however long the
 * hand-off takes - the no-progress window counter must reset on every progress tick, or any
 * saturated-but-progressing pipeline would be falsely aborted once the bound elapses. The peer's
 * progress is simulated white-box (ticking its package-private {@code completedTaskCount}) because
 * a genuinely progressing peer frees queue slots and ends the park long before enough windows
 * elapse; the tick keeps the queue full while showing steady progress, which is exactly the
 * signature of a saturated pipeline refilled by faster producers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AsyncHelpingSlowPeerProgressTest extends TestHelper {

  @Test
  @Timeout(60)
  void progressingPeerNeverTripsHelpingBackstop() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE, 2); // CAPACITY 1 PER WORKER
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(2);
    // Rebuild the pool so the queue size above is picked up (dedicated hook, #5072 review point 4).
    async.recreateThreadsForTests();
    async.setCheckForStalledQueuesMaxDelay(50); // BACKSTOP = 12 x 50ms = 600ms

    final CountDownLatch stallReported = new CountDownLatch(1);
    async.onError(e -> {
      if (String.valueOf(e.getMessage()).contains("stalled"))
        stallReported.countDown();
    });

    final CountDownLatch wedgeStarted = new CountDownLatch(1);
    final CountDownLatch releaseWedge = new CountDownLatch(1);
    final CountDownLatch outerStarted = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    final CountDownLatch markerRan = new CountDownLatch(1);
    try {
      // Worker 1: parked in user code behind a full queue, so the hand-off below stays blocked; its
      // "progress" is injected below without freeing queue slots.
      async.scheduleTask(1, awaitTask(wedgeStarted, releaseWedge), true, 0);
      assertThat(wedgeStarted.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(async.scheduleTask(1, noOpTask(), false, 0)).isTrue();

      // Worker 0: hands off to worker 1's full queue with an empty own queue - the exact backstop
      // scenario of AsyncHelpingEmptyQueueBackstopTest, except the peer now shows progress.
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
          async.scheduleTask(1, countingTask(markerRan), true, 0);
        }

        @Override
        public boolean requiresActiveTx() {
          return false;
        }
      }, true, 0);
      assertThat(outerStarted.await(5, TimeUnit.SECONDS)).isTrue();
      proceed.countDown();

      // Tick the peer's completed-task counter every 100ms (a progress event every ~2 windows) for
      // 2s = ~40 elapsed windows, far beyond the 12-window bound. The counter is only written by
      // the (currently parked) peer, so the test thread is the single writer during this phase.
      final DatabaseAsyncExecutorImpl.AsyncThread peer = findWorkerThread(db, 1);
      final long tickUntil = System.currentTimeMillis() + 2_000;
      while (System.currentTimeMillis() < tickUntil) {
        peer.completedTaskCount++;
        Thread.sleep(100);
      }

      assertThat(stallReported.getCount())
          .as("a progressing peer must keep resetting the backstop, not trip it")
          .isEqualTo(1L);
      assertThat(markerRan.getCount()).as("the hand-off must still be parked (queue never freed)").isEqualTo(1L);

      // With the progress stopped, the backstop must resume counting from the last reset and fire:
      // proves the counter was resetting (not merely never reaching the bound) during the ticks.
      assertThat(stallReported.await(10, TimeUnit.SECONDS))
          .as("once progress stops, the wedged peer must surface at the backstop")
          .isTrue();
    } finally {
      releaseWedge.countDown();
      proceed.countDown();
    }

    assertThat(async.waitCompletion(10_000)).isTrue();
  }

  private static DatabaseAsyncExecutorImpl.AsyncThread findWorkerThread(final DatabaseInternal db, final int slot) {
    final String name = "AsyncExecutor-" + db.getName() + "-" + slot;
    return (DatabaseAsyncExecutorImpl.AsyncThread) Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.getName().equals(name)).findFirst()
        .orElseThrow(() -> new IllegalStateException("Worker thread " + name + " not found"));
  }
}
