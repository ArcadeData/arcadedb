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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.arcadedb.database.async.AsyncTestTasks.awaitTask;
import static com.arcadedb.database.async.AsyncTestTasks.countingTask;
import static com.arcadedb.database.async.AsyncTestTasks.noOpTask;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the #5062 review, round 6 (point 1): a worker help-waiting on a wedged-alive
 * peer with an EMPTY own queue had no backstop at all - the loop spun forever (offer fails, peer
 * alive, deferral budget never grows because there is nothing to poll), losing both workers during
 * normal operation with no error surfaced. The producer path already failed loudly after
 * {@code STALLED_NO_PROGRESS_WINDOWS}; the helping loop now applies the same progress-gated bound,
 * aborting the hand-off (onError + batch rollback, worker survives) instead of hanging forever.
 */
class AsyncHelpingEmptyQueueBackstopTest extends TestHelper {

  @Test
  @Timeout(60)
  void helpWaitingWorkerWithEmptyQueueSurfacesWedgedPeerInsteadOfSpinningForever() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE, 2); // CAPACITY 1 PER WORKER
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(2);
    // Rebuild the pool so the queue size above is picked up (dedicated hook, #5072 review point 4).
    async.recreateThreadsForTests();
    async.setCheckForStalledQueuesMaxDelay(50); // BACKSTOP = 12 x 50ms = 600ms

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final CountDownLatch stallReported = new CountDownLatch(1);
    async.onError(e -> {
      errors.add(e);
      if (String.valueOf(e.getMessage()).contains("stalled"))
        stallReported.countDown();
    });

    final CountDownLatch wedgeStarted = new CountDownLatch(1);
    final CountDownLatch releaseWedge = new CountDownLatch(1);
    final CountDownLatch outerStarted = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    try {
      // Worker 1: wedged in user code (alive, completing nothing) behind a full queue.
      async.scheduleTask(1, awaitTask(wedgeStarted, releaseWedge), true, 0);
      assertThat(wedgeStarted.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(async.scheduleTask(1, noOpTask(), false, 0)).isTrue();

      // Worker 0: hands a follow-up to worker 1's full queue with its OWN queue empty, so the
      // deferral budget can never grow and only the backstop can end the wait.
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

      // The buggy loop spun forever with no error: the backstop must surface the stall.
      assertThat(stallReported.await(10, TimeUnit.SECONDS))
          .as("the help-waiting worker must report the wedged peer instead of spinning forever")
          .isTrue();

      // The worker must survive the aborted hand-off and keep serving its queue.
      final CountDownLatch survived = new CountDownLatch(1);
      async.scheduleTask(0, countingTask(survived), true, 0);
      assertThat(survived.await(5, TimeUnit.SECONDS))
          .as("the worker must stay serviceable after the aborted hand-off")
          .isTrue();
    } finally {
      releaseWedge.countDown();
      proceed.countDown();
    }

    assertThat(async.waitCompletion(10_000)).isTrue();
  }
}
