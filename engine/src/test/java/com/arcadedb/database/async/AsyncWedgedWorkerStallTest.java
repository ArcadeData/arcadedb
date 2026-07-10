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
import com.arcadedb.exception.DatabaseOperationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for the #5062 review, round 2 (point 1): the progress-based stall detector of
 * #4953 fired only when the target worker was itself parked handing a task cross-slot
 * ({@code waitingCrossSlotOffer}). A worker genuinely wedged inside a user {@code execute()} (an
 * infinite loop or a blocking call) never sets that flag, so a producer blocked on its full queue
 * waited forever with no exit: the (false-positive-prone) old head-identity detector at least
 * converted that hang into an exception. The backstop now throws after a much longer, progress-gated
 * bound: {@code STALLED_NO_PROGRESS_WINDOWS} consecutive stall windows with zero completed tasks.
 */
class AsyncWedgedWorkerStallTest extends TestHelper {

  @Test
  @Timeout(30)
  void producerBlockedOnWedgedWorkerEventuallyThrows() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE, 2);
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(1);
    // Force thread re-creation so the queue size above is picked up regardless of the previous level.
    async.setTransactionUseWAL(true);
    async.setCheckForStalledQueuesMaxDelay(50);

    final CountDownLatch wedgeStarted = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    try {
      // Simulates a worker wedged in user code: alive, not parked cross-slot, completing nothing.
      async.scheduleTask(0, new DatabaseAsyncTask() {
        @Override
        public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
          wedgeStarted.countDown();
          try {
            release.await(15, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }

        @Override
        public boolean requiresActiveTx() {
          return false;
        }
      }, true, 0);
      assertThat(wedgeStarted.await(5, TimeUnit.SECONDS)).isTrue();

      // Fill the queue (capacity 2) so the next producer has to wait.
      for (int i = 0; i < 2; i++)
        assertThat(async.scheduleTask(0, noOpTask(), false, 0)).isTrue();

      final long start = System.currentTimeMillis();
      assertThatThrownBy(() -> async.scheduleTask(0, noOpTask(), true, 0))
          .as("a producer must not hang forever on a worker wedged in user code")
          .isInstanceOf(DatabaseOperationException.class)
          .hasMessageContaining("stalled");
      assertThat(System.currentTimeMillis() - start)
          .as("the backstop must fire after the configured no-progress bound, not the wedge duration")
          .isLessThan(10_000);
    } finally {
      release.countDown();
    }

    assertThat(async.waitCompletion(10_000)).isTrue();
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
