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
 * Regression test for #5066: with {@code arcadedb.asyncOperationsQueueImpl=fast} (also enabled by
 * the {@code high-performance} profile) the worker queue was Conversant's
 * {@code PushPullBlockingQueue}, whose {@code remove(Object)} throws
 * {@code UnsupportedOperationException}. That degraded {@code scheduleTask}'s post-shutdown undo
 * (remove the task offered after the dead worker's final drain, then throw) to a silent no-op: the
 * orphaned task's {@code completed()} never fired and waiters hung. Worse, that queue is an
 * explicit single-producer/single-consumer design ("Transfers from a single thread writer to a
 * single thread reader") with no CAS on the tail sequence, while every worker queue here has many
 * producers (any application thread, cross-scheduling workers, completion markers, the closing
 * thread), so concurrent offers could silently lose tasks in normal operation. The 'fast' setting
 * now maps to {@code DisruptorBlockingQueue}, the multi-producer variant from the same library,
 * which both honors the multi-producer contract and implements {@code remove(Object)}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AsyncFastQueueShutdownUndoTest extends TestHelper {

  @Test
  @Timeout(60)
  void fastQueueMustSupportRemoveForThePostShutdownUndo() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_IMPL, "fast");
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE, 4);
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(1);
    // Rebuild the pool so the settings above are picked up.
    async.recreateThreadsForTests();

    final CountDownLatch blockerStarted = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    try {
      // Park the worker so the probe below stays in the queue while we exercise remove().
      async.scheduleTask(0, awaitTask(blockerStarted, release), true, 0);
      assertThat(blockerStarted.await(5, TimeUnit.SECONDS)).isTrue();

      final DatabaseAsyncTask probe = noOpTask();
      assertThat(async.scheduleTask(0, probe, false, 0)).isTrue();

      // The exact operation the post-shutdown undo depends on: on the old PushPullBlockingQueue it
      // threw UnsupportedOperationException, leaving the orphaned task enqueued forever.
      final DatabaseAsyncExecutorImpl.AsyncThread worker = findWorkerThread(db, 0);
      assertThat(worker.queue.remove(probe))
          .as("the 'fast' queue impl must support remove(Object): scheduleTask's post-shutdown undo relies on it")
          .isTrue();
      assertThat(worker.queue.remove(probe)).as("the probe must be gone after removal").isFalse();
    } finally {
      release.countDown();
    }

    assertThat(async.waitCompletion(10_000)).isTrue();
  }

  @Test
  @Timeout(60)
  void fastQueueMustNotLoseTasksUnderConcurrentProducers() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_IMPL, "fast");
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE, 64);
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(2);
    // Rebuild the pool so the settings above are picked up.
    async.recreateThreadsForTests();

    // Integrity guard for the multi-producer contract: the SPSC impl the 'fast' setting used to
    // select has no CAS on its tail sequence, so racing producers could overwrite each other's
    // slot and silently lose tasks. Deterministically green on a compliant MPMC impl.
    final int producers = 8;
    final int tasksPerProducer = 250;
    final CountDownLatch executed = new CountDownLatch(producers * tasksPerProducer);
    final Thread[] threads = new Thread[producers];
    for (int p = 0; p < producers; p++) {
      final int slotSeed = p;
      threads[p] = new Thread(() -> {
        for (int i = 0; i < tasksPerProducer; i++)
          async.scheduleTask((slotSeed + i) % 2, countingTask(executed), true, 0);
      }, getClass().getSimpleName() + "-producer-" + p);
      threads[p].start();
    }
    for (final Thread t : threads)
      t.join(30_000);

    assertThat(executed.await(30, TimeUnit.SECONDS))
        .as("every task offered by concurrent producers must execute exactly once")
        .isTrue();
    assertThat(async.waitCompletion(10_000)).isTrue();
  }

  private static DatabaseAsyncExecutorImpl.AsyncThread findWorkerThread(final DatabaseInternal db, final int slot) {
    final String name = "AsyncExecutor-" + db.getName() + "-" + slot;
    return (DatabaseAsyncExecutorImpl.AsyncThread) Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.getName().equals(name)).findFirst()
        .orElseThrow(() -> new IllegalStateException("Worker thread " + name + " not found"));
  }
  @Test
  @Timeout(60)
  void fastQueueSupportsEffectiveCapacityOne() throws Exception {
    // #5081 review: queueSize = ASYNC_OPERATIONS_QUEUE_SIZE / parallelLevel floors to 1 on large pools, so
    // capacity 1 is production-reachable - and the Conversant ring's minimum-size rounding is
    // library-version-specific. Pin the guarantee: offer/remove/reuse must behave on a 1-slot Disruptor
    // queue so a future library bump cannot silently regress it.
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_IMPL, "fast");
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE, 1);
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(1);
    async.recreateThreadsForTests();

    final CountDownLatch blockerStarted = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    try {
      // Park the worker so the queue keeps exactly its 1 slot for the probes below.
      async.scheduleTask(0, awaitTask(blockerStarted, release), true, 0);
      assertThat(blockerStarted.await(5, TimeUnit.SECONDS)).isTrue();

      final DatabaseAsyncExecutorImpl.AsyncThread worker = findWorkerThread(db, 0);
      final DatabaseAsyncTask first = noOpTask();
      final DatabaseAsyncTask second = noOpTask();
      assertThat(worker.queue.offer(first)).as("a 1-slot queue accepts the first task").isTrue();
      assertThat(worker.queue.offer(second)).as("a full 1-slot queue rejects (not blocks/throws) the second").isFalse();
      assertThat(worker.queue.remove(first))
          .as("remove(Object) works at capacity 1 - the post-shutdown undo primitive").isTrue();
      assertThat(worker.queue.offer(second)).as("the slot freed by remove is reusable").isTrue();
    } finally {
      release.countDown();
    }

    assertThat(async.waitCompletion(10_000)).isTrue();
  }

}
