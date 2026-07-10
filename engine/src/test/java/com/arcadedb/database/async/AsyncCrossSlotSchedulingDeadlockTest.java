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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #4953 (deadlock branch): under bidirectional edge load, worker A schedules a
 * follow-up task into worker B's slot (and vice versa) from inside its own task. With both queues
 * full this is a real wait cycle; the old code "resolved" it by throwing
 * {@code DatabaseOperationException("...is stalled")} inside the worker, which rolled back the whole
 * in-flight commit batch and silently dropped the cross-slot follow-up (ghost edges).
 * <p>
 * The fixed executor lets a worker blocked on another worker's full queue drain its OWN queue while
 * it waits, so the cycle resolves with every task executed and no error callback fired.
 */
class AsyncCrossSlotSchedulingDeadlockTest extends TestHelper {

  @Test
  void crossSlotSchedulingFromWorkersDoesNotStallOrDropTasks() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE, 2); // 1 PER WORKER
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(2);
    // Force thread re-creation so the queue size above is picked up regardless of the previous level.
    async.setTransactionUseWAL(true);
    async.setCheckForStalledQueuesMaxDelay(100);

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    async.onError(errors::add);

    final CountDownLatch bothCrossTasksRunning = new CountDownLatch(2);
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch markersExecuted = new CountDownLatch(2);
    final CountDownLatch fillersExecuted = new CountDownLatch(2);

    // Each cross task, once released, schedules a follow-up marker into the OTHER worker's slot.
    async.scheduleTask(0, crossTask(async, 1, bothCrossTasksRunning, release, markersExecuted, errors), true, 0);
    async.scheduleTask(1, crossTask(async, 0, bothCrossTasksRunning, release, markersExecuted, errors), true, 0);

    assertThat(bothCrossTasksRunning.await(5, TimeUnit.SECONDS)).isTrue();

    // Both workers are busy and their queues (capacity 1) are empty: fill them.
    assertThat(async.scheduleTask(0, countingTask(fillersExecuted), false, 0)).isTrue();
    assertThat(async.scheduleTask(1, countingTask(fillersExecuted), false, 0)).isTrue();

    // Both workers now offer into each other's full queue at the same time: the textbook cycle.
    release.countDown();

    assertThat(markersExecuted.await(10, TimeUnit.SECONDS)).as("cross-slot follow-up tasks must execute").isTrue();
    assertThat(fillersExecuted.await(10, TimeUnit.SECONDS)).as("queued filler tasks must execute").isTrue();
    assertThat(errors).as("no worker may fail with a stall exception").isEmpty();
  }

  private static DatabaseAsyncTask crossTask(final DatabaseAsyncExecutorImpl async, final int targetSlot,
                                             final CountDownLatch running, final CountDownLatch release,
                                             final CountDownLatch markers, final List<Throwable> errors) {
    return new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        running.countDown();
        try {
          if (!release.await(10, TimeUnit.SECONDS)) {
            errors.add(new IllegalStateException("release latch never fired"));
            return;
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          errors.add(e);
          return;
        }
        async.scheduleTask(targetSlot, countingTask(markers), true, 0);
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    };
  }

  private static DatabaseAsyncTask countingTask(final CountDownLatch latch) {
    return new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        latch.countDown();
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    };
  }
}
