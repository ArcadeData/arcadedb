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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11212: since #11155 {@link DatabaseAsyncExecutor#waitCompletion()} returns when the last
 * marker counts its latch down, and a worker counted the latch down BEFORE it dropped its "executing a task" flag, so a
 * caller asking {@link DatabaseAsyncExecutor#isProcessing()} right after the return got {@code true} in up to half of the
 * calls with nothing left to run. A dispatched command had the same gap one step earlier: its sequence left the set
 * {@code waitCompletion()} awaits before the in-flight count {@code isProcessing()} reads went down.
 * <p>
 * The post-condition restored here: with no other thread submitting, the executor is idle the moment {@code waitCompletion()}
 * (or any other wait released by a task's completion, such as {@code scanType()}) returns. The first test pins the ordering
 * that guarantees it deterministically, the others run the reporter's loop. Each {@code @Timeout} is a hang detector.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11212IdleAfterWaitCompletionTest extends TestHelper {

  private static final String TYPE   = "Msg";
  private static final int    ROUNDS = 300;

  @Override
  protected void beginTest() {
    database.transaction(() -> database.getSchema().createDocumentType(TYPE));
  }

  /**
   * Whoever a task's completion releases must find the worker idle, so the worker has to stop counting the task as running
   * before it signals that it finished. Asked from inside {@code completed()} itself, the last instant before the release,
   * so the answer does not depend on how the threads happen to be scheduled.
   */
  @Test
  @Timeout(60)
  void aTaskCompletionIsSignalledOnlyOnceTheWorkerCountsAsIdle() throws Exception {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) database.async();
    async.setParallelLevel(1);
    async.waitCompletion();

    final AtomicInteger processingAtCompletion = new AtomicInteger(-1);
    final CountDownLatch released = new CountDownLatch(1);
    async.scheduleTask(0, new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread worker, final DatabaseInternal database) {
        // NOTHING TO DO: ONLY THE COMPLETION MATTERS
      }

      @Override
      public void completed() {
        processingAtCompletion.set(async.isProcessing() ? 1 : 0);
        released.countDown();
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    }, true, 0);

    assertThat(released.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(processingAtCompletion.get()).as("isProcessing() asked while the completion of the only task is being signalled")
        .isEqualTo(0);
  }

  @Test
  @Timeout(120)
  void idleRightAfterWaitCompletionWithOneWorker() {
    assertIdleRightAfterEveryWaitCompletion(1);
  }

  @Test
  @Timeout(120)
  void idleRightAfterWaitCompletionWithThreeWorkers() {
    assertIdleRightAfterEveryWaitCompletion(3);
  }

  /**
   * The command half: DDL dispatched through {@code async().command()} runs on the command pool, and {@code waitCompletion()}
   * waits for its sequence to leave the in-flight set while {@code isProcessing()} reads the in-flight count.
   */
  @Test
  @Timeout(120)
  void idleRightAfterWaitCompletionOfDispatchedCommands() {
    final DatabaseAsyncExecutor async = database.async();
    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    int busy = 0;
    for (int r = 0; r < 100; r++) {
      async.command("sql", "CREATE DOCUMENT TYPE Cmd" + r, new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet resultSet) {
          // NOTHING TO CHECK: THE TYPE IS THERE OR onError IS CALLED
        }

        @Override
        public void onError(final Exception exception) {
          errors.add(exception);
        }
      });
      async.waitCompletion();
      if (async.isProcessing())
        ++busy;
    }
    assertThat(errors).isEmpty();
    assertThat(busy).as("rounds where isProcessing() answered true right after waitCompletion() returned").isZero();
  }

  @Test
  @Timeout(120)
  void idleRightAfterScanType() {
    final DatabaseAsyncExecutor async = database.async();
    async.setParallelLevel(3);
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument(TYPE).set("n", i).save();
    });

    int busy = 0;
    for (int r = 0; r < ROUNDS; r++) {
      final AtomicInteger seen = new AtomicInteger();
      async.scanType(TYPE, true, record -> {
        seen.incrementAndGet();
        return true;
      });
      assertThat(seen.get()).isEqualTo(100);
      if (async.isProcessing())
        ++busy;
    }
    assertThat(busy).as("rounds where isProcessing() answered true right after scanType() returned").isZero();
  }

  private void assertIdleRightAfterEveryWaitCompletion(final int parallelLevel) {
    final DatabaseAsyncExecutor async = database.async();
    async.setParallelLevel(parallelLevel);
    async.setCommitEvery(100);

    final AtomicInteger applied = new AtomicInteger();
    int busy = 0;
    for (int r = 0; r < ROUNDS; r++) {
      for (int i = 0; i < 50; i++) {
        final MutableDocument doc = database.newDocument(TYPE);
        doc.set("n", i);
        async.createRecord(doc, record -> applied.incrementAndGet());
      }
      async.waitCompletion();
      if (async.isProcessing())
        ++busy;
    }

    assertThat(applied.get()).isEqualTo(ROUNDS * 50);
    assertThat(busy).as("rounds where isProcessing() answered true right after waitCompletion() returned").isZero();
  }
}
