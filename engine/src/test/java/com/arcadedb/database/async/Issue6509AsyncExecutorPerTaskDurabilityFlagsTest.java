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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.WALFile;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6509: {@code DatabaseAsyncExecutorImpl.setTransactionUseWAL()}/
 * {@code setTransactionSync()} used to apply the new value by tearing down and respawning the
 * ENTIRE worker pool ({@code createThreads()}), because {@code AsyncThread.run()} only ever applied
 * the durability flags once, at thread start. Issue #5665 diagnosed this and shipped a cheap interim
 * mitigation (GraphBatch toggling the flags once per batch instead of once per flush), but explicitly
 * deferred the real fix, and a production incident (#6505) showed the interim mitigation is not
 * sufficient under real multi-writer concurrency: any concurrent user of {@code database.async()} still
 * has its in-flight/queued tasks force-exited whenever the pool churns.
 * <p>
 * The fix moves {@code transactionUseWAL}/{@code transactionSync} off "applied once at thread start"
 * and onto "applied per task" ({@code AsyncThread.executeTask()}), so the setters become plain volatile
 * writes and {@code createThreads()} drops out of the path entirely - exactly as #5665 originally
 * proposed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6509AsyncExecutorPerTaskDurabilityFlagsTest extends TestHelper {

  @Test
  void directDurabilityFlagFlipsDoNotRecreateAsyncThreads() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(3);
    async.recreateThreadsForTests();

    final Set<Long> threadIdsBefore = asyncWorkerThreadIds();
    assertThat(threadIdsBefore).as("async worker pool must exist before the flips").hasSize(3);
    final int threadCountBefore = async.getThreadCount();

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    async.onError(errors::add);

    // Repeatedly flip both flags directly on the executor - the exact pattern GraphBatch used to
    // trigger on every open/close (and, pre-#5665, on every flush) - interleaving a real scheduled and
    // awaited task between flips so any respawn would have to race live work rather than an idle pool.
    for (int i = 0; i < 25; i++) {
      async.setTransactionUseWAL(i % 2 == 0);
      async.setTransactionSync(i % 2 == 0 ? WALFile.FlushType.NO : WALFile.FlushType.YES_NOMETADATA);

      final CountDownLatch done = new CountDownLatch(1);
      assertThat(async.scheduleTask(i % 3, countingTask(done), true, 0)).isTrue();
      assertThat(done.await(5, TimeUnit.SECONDS)).as("task %d must complete", i).isTrue();
    }

    assertThat(errors).as("no unrelated task may fail while the flags are flipped").isEmpty();
    assertThat(async.getThreadCount())
        .as("thread count must be unchanged by flag flips")
        .isEqualTo(threadCountBefore);
    assertThat(asyncWorkerThreadIds())
        .as("the same worker threads must survive every flag flip: no createThreads() teardown")
        .isEqualTo(threadIdsBefore);
  }

  @Test
  void durabilityFlagChangeTakesEffectOnNextTaskWithoutThreadRespawn() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(1);
    async.recreateThreadsForTests();
    async.setCommitEvery(1); // COMMIT AFTER EVERY TASK

    final Set<Long> threadIdsBefore = asyncWorkerThreadIds();

    async.setTransactionUseWAL(false);
    async.setTransactionSync(WALFile.FlushType.NO);
    assertThat(observeUseWALOnWorker(async)).as("task must observe the flag in effect when it runs").isFalse();

    // Flip WITHOUT recreating the pool: the next task, on the SAME already-running worker, must
    // observe the new value.
    async.setTransactionUseWAL(true);
    async.setTransactionSync(WALFile.FlushType.YES_NOMETADATA);
    assertThat(observeUseWALOnWorker(async)).as("the flip must be visible to the very next task").isTrue();

    assertThat(asyncWorkerThreadIds())
        .as("the durability change must take effect without the worker thread being respawned")
        .isEqualTo(threadIdsBefore);
  }

  /** Schedules a task on worker 0 that captures the transaction's current useWAL flag, and waits for it. */
  private static boolean observeUseWALOnWorker(final DatabaseAsyncExecutorImpl async) throws Exception {
    final AtomicReference<Boolean> observedUseWAL = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(1);
    async.scheduleTask(0, new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        observedUseWAL.set(database.getTransaction().isUseWAL());
      }

      @Override
      public void completed() {
        done.countDown();
      }
    }, true, 0);
    assertThat(done.await(5, TimeUnit.SECONDS)).as("observation task must complete").isTrue();
    return observedUseWAL.get();
  }

  private static DatabaseAsyncTask countingTask(final CountDownLatch latch) {
    return new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        // NO ACTIONS
      }

      @Override
      public void completed() {
        latch.countDown();
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    };
  }

  private Set<Long> asyncWorkerThreadIds() {
    final String prefix = "AsyncExecutor-" + database.getName() + "-";
    final Set<Long> ids = new HashSet<>();
    for (final Thread t : Thread.getAllStackTraces().keySet())
      if (t.getName().startsWith(prefix))
        ids.add(t.threadId());
    return ids;
  }
}
