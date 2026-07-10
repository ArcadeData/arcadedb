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
import static com.arcadedb.database.async.AsyncTestTasks.noOpTask;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the #5062 review, round 6 (point 2): after consuming {@code FORCE_EXIT} the
 * helping loop set {@code forceShutdown} but kept looping to hand off the follow-up. On a wedged
 * peer the worker ignored its own shutdown request until the interrupt escalation fired after the
 * full grace period, so {@code close()} paid the whole {@code shutdownJoinTimeoutMs} for a worker
 * that already knew it had to exit. The loop now bails out promptly once the flag is set (the
 * follow-up is dropped loudly via the shutdown exception - workers are dying, same as the
 * target-dead branch).
 */
class AsyncShutdownPromptHelpingBailTest extends TestHelper {

  @Test
  @Timeout(60)
  void closeReturnsPromptlyWhenHelpingWorkerConsumesForceExit() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE, 2); // CAPACITY 1 PER WORKER
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(2);
    // Rebuild the pool so the queue size above is picked up (dedicated hook, #5072 review point 4).
    async.recreateThreadsForTests();
    // Large grace period: without the prompt bail, close() pays it in full on the helping worker
    // before the interrupt escalation kicks in.
    async.shutdownJoinTimeoutMs = 8_000;

    final CountDownLatch wedgeStarted = new CountDownLatch(1);
    final CountDownLatch releaseWedge = new CountDownLatch(1);
    final CountDownLatch outerStarted = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    try {
      // Worker 1: wedged behind a full queue, so the hand-off below cannot land before close().
      async.scheduleTask(1, awaitTask(wedgeStarted, releaseWedge), true, 0);
      assertThat(wedgeStarted.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(async.scheduleTask(1, noOpTask(), false, 0)).isTrue();

      // Worker 0: parks in offerHelping with an empty own queue; close()'s FORCE_EXIT offer lands
      // there and the helping loop consumes it.
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

      // The wedge stays in place for the whole shutdown: worker 0 is handled first by close() and
      // must exit on the consumed FORCE_EXIT alone (worker 1 is only interrupted afterwards, via
      // the offer-failed path, and honors it).
      final long start = System.currentTimeMillis();
      async.close();
      final long elapsed = System.currentTimeMillis() - start;

      // Without the bail: worker 0 sat in the helping loop for the full 8s grace period before the
      // escalation interrupt. With it: worker 0 exits within one offer window of consuming the
      // marker.
      assertThat(elapsed)
          .as("close() must not pay the full grace period for a worker that already consumed FORCE_EXIT")
          .isLessThan(5_000);
    } finally {
      releaseWedge.countDown();
      proceed.countDown();
    }
  }
}
