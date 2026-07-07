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
 * Regression test for the #5062 review (point 3): {@code waitCompletion(timeout)} bounded the await
 * phase but not the enqueue phase, which retried a 500ms offer for as long as the worker stayed
 * alive. A caller passing a 1s timeout could block for the whole duration of a slow task on a
 * worker with a persistently full queue. The timeout is a single budget spanning both phases.
 */
class AsyncWaitCompletionTimeoutTest extends TestHelper {

  @Test
  @Timeout(30)
  void waitCompletionHonorsTimeoutWhileQueueIsFull() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE, 2);
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(1);
    // Force thread re-creation so the queue size above is picked up regardless of the previous level.
    async.setTransactionUseWAL(true);

    final CountDownLatch blockerStarted = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    async.scheduleTask(0, blockerTask(blockerStarted, release), true, 0);
    assertThat(blockerStarted.await(5, TimeUnit.SECONDS)).isTrue();

    // Fill the queue (capacity 2) so the completion marker cannot be enqueued.
    for (int i = 0; i < 2; i++)
      assertThat(async.scheduleTask(0, noOpTask(), false, 0)).isTrue();

    final long start = System.currentTimeMillis();
    final boolean flushed = async.waitCompletion(700);
    final long elapsed = System.currentTimeMillis() - start;

    assertThat(flushed).as("waitCompletion must report the timeout instead of a flush").isFalse();
    assertThat(elapsed).as("the timeout budget must also bound the enqueue phase").isLessThan(5_000);

    release.countDown();
    assertThat(async.waitCompletion(10_000)).isTrue();
  }

  private static DatabaseAsyncTask blockerTask(final CountDownLatch started, final CountDownLatch release) {
    return new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        started.countDown();
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
