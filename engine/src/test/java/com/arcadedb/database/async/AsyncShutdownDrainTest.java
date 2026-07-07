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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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

  @Test
  @Timeout(30)
  void closeDoesNotHangOnAWedgedAsyncWorker() throws Exception {
    // #5080: database.close()/drop() drained the async executor via an UNBOUNDED waitCompletion(), so a
    // worker wedged inside a user task (here a 60s sleep, standing in for an infinite loop or stuck I/O)
    // made close() hang. With ASYNC_CLOSE_TIMEOUT the graceful drain gives up and force-shuts the workers.
    final String dbPath = "target/databases/AsyncCloseTimeoutTest";
    DatabaseFactory factory = new DatabaseFactory(dbPath);
    if (factory.exists())
      factory.open().drop();

    final DatabaseInternal db = (DatabaseInternal) factory.create();
    try {
      db.getConfiguration().setValue(GlobalConfiguration.ASYNC_CLOSE_TIMEOUT, 1_000L);
      final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
      async.setParallelLevel(1);
      // #5105 review: pin the join timeout small so the <15s bound is self-contained (not coupled to the
      // 10s shutdownJoinTimeoutMs default) - the force-shutdown here is offer -> join -> interrupt -> join.
      async.shutdownJoinTimeoutMs = 2_000;
      // Force thread re-creation so the parallel level above is picked up.
      async.setTransactionUseWAL(true);

      final CountDownLatch blockerStarted = new CountDownLatch(1);
      // Wedged far longer than both the 1s close timeout and the 30s test timeout: pre-fix, the unbounded
      // waitCompletion() would block close() until it finished (~60s), tripping @Timeout.
      async.scheduleTask(0, blockerTask(blockerStarted, 60_000), true, 0);
      assertThat(blockerStarted.await(5, TimeUnit.SECONDS)).isTrue();

      final long begin = System.currentTimeMillis();
      db.close();
      final long elapsed = System.currentTimeMillis() - begin;

      assertThat(elapsed)
          .as("close() must give up the async drain after ASYNC_CLOSE_TIMEOUT instead of hanging on a wedged worker")
          .isLessThan(15_000L);
    } finally {
      // #5105 review: drop even if the assertion failed, so a failure does not leak the wedged worker's
      // 60s thread or the DB directory. The bounded close config above keeps this final close bounded too.
      if (db.isOpen())
        db.close();
      final DatabaseFactory cleanup = new DatabaseFactory(dbPath);
      if (cleanup.exists())
        cleanup.open().drop();
    }
  }

  @Test
  @Timeout(30)
  void forceShutdownStillInterruptsWorkerWhenCallerThreadIsInterrupted() throws Exception {
    // #5105 review (finding 1): when close() drains from an already-interrupted thread, waitCompletion
    // returns false and re-sets the interrupt flag. async.close()'s shutdown uses interruptible steps, so
    // leaving the flag set makes them throw immediately and the wedged worker is never interrupted - it
    // keeps running behind a returned close(). The fix clears the flag across the force-shutdown.
    final String dbPath = "target/databases/AsyncCloseInterruptTest";
    DatabaseFactory factory = new DatabaseFactory(dbPath);
    if (factory.exists())
      factory.open().drop();

    final DatabaseInternal db = (DatabaseInternal) factory.create();
    try {
      db.getConfiguration().setValue(GlobalConfiguration.ASYNC_CLOSE_TIMEOUT, 1_000L);
      final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
      async.setParallelLevel(1);
      // #5105 review: pin the join timeout small so the assertion is not coupled to the 10s default.
      async.shutdownJoinTimeoutMs = 2_000;
      async.setTransactionUseWAL(true);

      final CountDownLatch blockerStarted = new CountDownLatch(1);
      // Signals iff the worker actually receives an interrupt: on the pre-fix code the caller's set flag
      // neuters the force-shutdown, this latch never fires, and the test times out.
      final CountDownLatch workerInterrupted = new CountDownLatch(1);
      async.scheduleTask(0, interruptSignallingBlockerTask(blockerStarted, workerInterrupted), true, 0);
      assertThat(blockerStarted.await(5, TimeUnit.SECONDS)).isTrue();

      // Close from an INTERRUPTED thread - the scenario finding 1 covers.
      Thread.currentThread().interrupt();
      db.close();
      // close() restores the caller's interrupt flag; consume it so it does not leak into the assertions
      // (and JUnit's own waits) below.
      assertThat(Thread.interrupted()).as("close() must restore the caller's interrupt flag").isTrue();

      assertThat(workerInterrupted.await(10, TimeUnit.SECONDS))
          .as("the wedged worker must be interrupted by the force-shutdown even when close()'s caller was interrupted")
          .isTrue();
    } finally {
      Thread.interrupted();
      if (db.isOpen())
        db.close();
      final DatabaseFactory cleanup = new DatabaseFactory(dbPath);
      if (cleanup.exists())
        cleanup.open().drop();
    }
  }

  private static DatabaseAsyncTask interruptSignallingBlockerTask(final CountDownLatch started,
      final CountDownLatch interrupted) {
    return new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        started.countDown();
        try {
          Thread.sleep(60_000);
        } catch (final InterruptedException e) {
          interrupted.countDown();
          Thread.currentThread().interrupt();
        }
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    };
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
