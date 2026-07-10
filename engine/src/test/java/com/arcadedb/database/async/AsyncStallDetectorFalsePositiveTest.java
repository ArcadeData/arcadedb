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
 * Regression test for #4953 (false-positive branch): the stall detector in
 * {@code DatabaseAsyncExecutorImpl.scheduleTask} compared the identity of the queue head across two
 * offer windows. A worker busy on a single task longer than 2x {@code checkForStalledQueuesMaxDelay}
 * left the (untouched) head identical, so a healthy producer blocked on the full queue was thrown a
 * spurious {@code DatabaseOperationException("Asynchronous queue ... is stalled")}.
 */
class AsyncStallDetectorFalsePositiveTest extends TestHelper {

  @Test
  void slowHeadTaskDoesNotTripStallDetector() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE, 2);
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(1);
    // Force thread re-creation so the queue size above is picked up regardless of the previous level.
    async.setTransactionUseWAL(true);
    // 300ms window: the 1.2s head task is 4x the old 2-window false-positive bound, while staying
    // well below the wedged-worker backstop (STALLED_NO_PROGRESS_WINDOWS x 300ms = 3.6s), which
    // must not fire for a single legitimately slow task.
    async.setCheckForStalledQueuesMaxDelay(300);

    final CountDownLatch slowTaskStarted = new CountDownLatch(1);
    final CountDownLatch executed = new CountDownLatch(4);

    // Head task busy for ~1.2s: much longer than 2x the stall-check delay.
    async.scheduleTask(0, new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        slowTaskStarted.countDown();
        try {
          Thread.sleep(1_200);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        executed.countDown();
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    }, true, 0);

    assertThat(slowTaskStarted.await(5, TimeUnit.SECONDS)).isTrue();

    // Fill the queue (capacity 2) while the worker is busy.
    for (int i = 0; i < 2; i++)
      assertThat(async.scheduleTask(0, countingTask(executed), true, 0)).isTrue();

    // This producer must wait for the (healthy but slow) worker instead of throwing
    // "Asynchronous queue 0 is stalled" after 2x checkForStalledQueuesMaxDelay.
    assertThat(async.scheduleTask(0, countingTask(executed), true, 0)).isTrue();

    assertThat(executed.await(10, TimeUnit.SECONDS)).as("all scheduled tasks must execute").isTrue();
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
