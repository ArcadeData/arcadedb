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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #4954: shutdown/interrupt paths dropped queued tasks without ever invoking
 * their {@code completed()} callback, so any thread blocked in {@code scanType()} /
 * {@code waitCompletion()} on those tasks hung forever. Additionally {@code shutdownThreadsLocked}
 * blocked on an untimed {@code queue.put(FORCE_EXIT)} while holding the lifecycle lock: a busy
 * worker with a full queue wedged {@code database.close()} for as long as its current task ran.
 */
class AsyncShutdownDrainTest extends TestHelper {

  @Test
  void killNotifiesCompletionOfDroppedQueuedTasks() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE, 4);
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(1);
    // Force thread re-creation so the queue size above is picked up regardless of the previous level.
    async.setTransactionUseWAL(true);

    final CountDownLatch blockerStarted = new CountDownLatch(1);
    async.scheduleTask(0, blockerTask(blockerStarted, 10_000), true, 0);
    assertThat(blockerStarted.await(5, TimeUnit.SECONDS)).isTrue();

    // Queue up tasks behind the blocker; their completed() must run even if they are dropped.
    final CountDownLatch probesCompleted = new CountDownLatch(3);
    for (int i = 0; i < 3; i++)
      assertThat(async.scheduleTask(0, probeTask(probesCompleted), false, 0)).isTrue();

    async.kill();

    assertThat(probesCompleted.await(5, TimeUnit.SECONDS))
        .as("queued tasks dropped by kill() must still be completed() so waiters do not hang")
        .isTrue();
  }

  @Test
  void closeDoesNotBlockOnFullQueueOfBusyWorker() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE, 2);
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(1);
    // Force thread re-creation so the queue size above is picked up regardless of the previous level.
    async.setTransactionUseWAL(true);

    final CountDownLatch blockerStarted = new CountDownLatch(1);
    async.scheduleTask(0, blockerTask(blockerStarted, 15_000), true, 0);
    assertThat(blockerStarted.await(5, TimeUnit.SECONDS)).isTrue();

    // Fill the queue (capacity 2) so the FORCE_EXIT marker cannot be enqueued.
    final CountDownLatch probesCompleted = new CountDownLatch(2);
    for (int i = 0; i < 2; i++)
      assertThat(async.scheduleTask(0, probeTask(probesCompleted), false, 0)).isTrue();

    // The old code blocked in queue.put(FORCE_EXIT) under the lifecycle lock until the blocker
    // finished (~15s). The fixed code times out the offer, interrupts the worker and returns.
    final CountDownLatch closed = new CountDownLatch(1);
    final Thread closer = new Thread(() -> {
      async.close();
      closed.countDown();
    }, getClass().getSimpleName() + "-closer");
    closer.start();

    assertThat(closed.await(8, TimeUnit.SECONDS))
        .as("close() must not hang on a busy worker with a full queue")
        .isTrue();
    assertThat(probesCompleted.await(5, TimeUnit.SECONDS))
        .as("queued tasks must be executed or completed() during shutdown")
        .isTrue();
    closer.join(5_000);
  }

  private static DatabaseAsyncTask blockerTask(final CountDownLatch started, final long sleepMs) {
    return new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        started.countDown();
        try {
          Thread.sleep(sleepMs);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    };
  }

  private static DatabaseAsyncTask probeTask(final CountDownLatch completedLatch) {
    return new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        // NO ACTIONS
      }

      @Override
      public void completed() {
        completedLatch.countDown();
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    };
  }
}
