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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the #5062 review (point 1): the help-while-waiting path of #4953 must not run
 * queued tasks re-entrantly while the worker's current task is suspended mid-execute() inside
 * scheduleTask. A helped task hitting the commit-every-N boundary (or managing the transaction
 * itself, like {@code DatabaseAsyncTransaction}) would commit the suspended task's PARTIAL writes,
 * breaking per-task atomicity: a crash in that window persists half a task. Tasks polled while
 * help-waiting must instead be parked and executed only after the current task fully unwinds.
 */
class AsyncHelpingAtomicityTest extends TestHelper {

  @Test
  void helpedTaskMustNotCommitSuspendedTaskPartialWrites() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE, 2); // 1 PER WORKER
    database.getSchema().createDocumentType("HelpAtomic");

    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(2);
    // Force thread re-creation so the queue size above is picked up regardless of the previous level.
    async.setTransactionUseWAL(true);
    async.setCommitEvery(1); // EVERY TASK BOUNDARY COMMITS: A NESTED EXECUTION WOULD COMMIT IMMEDIATELY

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    async.onError(errors::add);

    final CountDownLatch blockerStarted = new CountDownLatch(1);
    final CountDownLatch finish = new CountDownLatch(1);
    final CountDownLatch outerWrote = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    final CountDownLatch outerDone = new CountDownLatch(1);
    final CountDownLatch helpedRan = new CountDownLatch(1);
    final CountDownLatch markerRan = new CountDownLatch(1);
    final AtomicBoolean helpedRanBeforeOuterFinished = new AtomicBoolean(false);

    // Worker 1: busy until released, with a full queue behind it, so worker 0 has to help-wait.
    async.scheduleTask(1, awaitTask(blockerStarted, finish, errors), true, 0);
    assertThat(blockerStarted.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(async.scheduleTask(1, countingTask(new CountDownLatch(1)), false, 0)).isTrue();

    // Worker 0: writes a document (uncommitted, commitEvery boundary not reached yet), then
    // schedules a follow-up into worker 1's full queue, parking mid-execute in the helping loop.
    async.scheduleTask(0, new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        database.newDocument("HelpAtomic").set("k", 1).save();
        outerWrote.countDown();
        try {
          if (!proceed.await(20, TimeUnit.SECONDS)) {
            errors.add(new IllegalStateException("proceed latch never fired"));
            return;
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          errors.add(e);
          return;
        }
        async.scheduleTask(1, countingTask(markerRan), true, 0);
      }

      @Override
      public void completed() {
        // FIRES IN executeTask'S finally, AFTER THE commitEvery BOUNDARY OF THIS TASK
        outerDone.countDown();
      }
    }, true, 0);

    assertThat(outerWrote.await(5, TimeUnit.SECONDS)).isTrue();

    // A task sits in worker 0's own queue: the helping loop will poll it while parked. The task
    // records deterministically whether it ran before the suspended task finished: any such
    // execution is nested inside the suspended task and, with commitEvery=1, committed its partial
    // write on the buggy code (#5062 review r2, point 4: ordering probe instead of a timed sleep).
    assertThat(async.scheduleTask(0, new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        helpedRanBeforeOuterFinished.set(outerDone.getCount() > 0);
        helpedRan.countDown();
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    }, false, 0)).isTrue();

    proceed.countDown();

    // Wait until the parked worker's helping loop polls the probe out of its own queue: the
    // aggregate queue size drops from 2 (probe + filler) to 1 (filler). This pins the interleaving
    // without a timed sleep: from here on the helping episode has provably engaged.
    final long spinDeadline = System.currentTimeMillis() + 10_000;
    while (async.getStats().queueSize > 1 && System.currentTimeMillis() < spinDeadline)
      Thread.sleep(10);
    assertThat(async.getStats().queueSize).as("the helping loop must drain the worker's own queue").isEqualTo(1L);

    // Deterministic under the fix: the suspended task cannot complete (hence cannot commit) while
    // worker 1's queue is still full, so no write of it may be visible here.
    database.transaction(
        () -> assertThat(database.countType("HelpAtomic", true))
            .as("the suspended task's partial write must not be committed by a helped task")
            .isEqualTo(0L));

    // Release everything: the cycle resolves, all tasks (including the parked one) must execute.
    finish.countDown();

    assertThat(async.waitCompletion(10_000)).isTrue();
    assertThat(helpedRan.await(5, TimeUnit.SECONDS)).as("the task polled while helping must still execute").isTrue();
    assertThat(helpedRanBeforeOuterFinished.get())
        .as("the task polled while helping must not run before the suspended task fully unwound")
        .isFalse();
    assertThat(markerRan.await(5, TimeUnit.SECONDS)).as("the cross-slot follow-up must execute").isTrue();
    assertThat(errors).isEmpty();
    database.transaction(
        () -> assertThat(database.countType("HelpAtomic", true)).as("the write must be committed once the task completed")
            .isEqualTo(1L));
  }

  private static DatabaseAsyncTask awaitTask(final CountDownLatch started, final CountDownLatch release,
                                             final List<Throwable> errors) {
    return new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        started.countDown();
        try {
          if (!release.await(20, TimeUnit.SECONDS))
            errors.add(new IllegalStateException("release latch never fired"));
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
