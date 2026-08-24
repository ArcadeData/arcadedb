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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #6462: a bidirectional cross-slot edge schedules its incoming-edge cascade
 * task onto the DESTINATION worker's slot from INSIDE the source task's own callback -
 * {@code DatabaseAsyncExecutorImpl.newEdge()} schedules {@code CreateIncomingEdgeAsyncTask} onto
 * {@code destinationSlot} from inside {@code CreateEdgeAsyncTask}'s completion callback - i.e.
 * AFTER the destination worker may already have confirmed a completion marker to a concurrent
 * {@link DatabaseAsyncExecutorImpl#waitCompletion(long)} caller. The public {@code waitCompletion()}
 * promises to wait "for the completion of all pending operations" ({@link DatabaseAsyncExecutor})
 * but placed exactly one marker per worker in a single pass, with no way to notice a task that
 * landed on a worker AFTER that worker's own marker had already fired.
 * <p>
 * This reproduces the shape without touching the graph API at all: a plain task on worker 0 that,
 * once released, schedules a follow-up task onto worker 1 from inside its own execution - mirroring
 * {@code CreateEdgeAsyncTask}'s callback scheduling {@code CreateIncomingEdgeAsyncTask}. The
 * follow-up is held open with a latch so the buggy and fixed behaviour can be told apart
 * deterministically (by what has and has not happened when {@code waitCompletion()} returns) rather
 * than by timing.
 */
class Issue6462CrossSlotWaitCompletionMissesCascadeTest extends TestHelper {

  @Test
  @Timeout(30)
  void waitCompletionMustNotReturnBeforeACrossSlotCascadeTaskCompletes() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(2);
    // Force thread re-creation so the fresh pair of workers is used regardless of any prior level.
    async.recreateThreadsForTests();

    final CountDownLatch sourceTaskRunning = new CountDownLatch(1);
    final CountDownLatch releaseSourceTask = new CountDownLatch(1);
    final CountDownLatch cascadeStarted    = new CountDownLatch(1);
    final CountDownLatch releaseCascade    = new CountDownLatch(1);
    final AtomicBoolean  cascadeCompleted  = new AtomicBoolean(false);

    // SOURCE TASK on slot 0 - mirrors CreateEdgeAsyncTask: once released, it schedules a follow-up
    // task onto slot 1 FROM INSIDE its own execution, exactly as newEdge()'s cross-slot callback
    // schedules CreateIncomingEdgeAsyncTask onto the destination slot.
    async.scheduleTask(0, new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        sourceTaskRunning.countDown();
        awaitLatch(releaseSourceTask);

        async.scheduleTask(1, new DatabaseAsyncTask() {
          @Override
          public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
            cascadeStarted.countDown();
            awaitLatch(releaseCascade);
            cascadeCompleted.set(true);
          }

          @Override
          public boolean requiresActiveTx() {
            return false;
          }
        }, true, 0);
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    }, true, 0);

    assertThat(sourceTaskRunning.await(5, TimeUnit.SECONDS)).isTrue();

    // AT THIS POINT: worker 0's queue holds the still-blocked source task; worker 1 is completely
    // idle. This is exactly the state waitCompletion() sees when it places one marker per worker in
    // a single pass: worker 1's marker has nothing ahead of it and is free to fire long before the
    // source task's callback ever reaches the line that schedules the cascade onto worker 1.
    final CountDownLatch waitCompletionReturned = new CountDownLatch(1);
    final AtomicBoolean  waitCompletionResult   = new AtomicBoolean();
    final Thread waiter = new Thread(() -> {
      waitCompletionResult.set(async.waitCompletion(20_000));
      waitCompletionReturned.countDown();
    }, "waitCompletion-caller");
    waiter.setDaemon(true);
    waiter.start();

    // Worker 1's marker must have fired before the source task is released - proven with the same
    // observable worker 1's completed-task counter already used elsewhere in this suite
    // (AsyncHelpingSlowPeerProgressTest), never a fixed sleep: nothing but waitCompletion()'s own
    // marker can complete on worker 1 before this point.
    final DatabaseAsyncExecutorImpl.AsyncThread worker1 = findWorkerThread(db, 1);
    final long deadline = System.currentTimeMillis() + 10_000;
    while (worker1.completedTaskCount == 0) {
      if (System.currentTimeMillis() > deadline)
        throw new AssertionError("Worker 1's completion marker never fired");
      Thread.sleep(5);
    }

    // waitCompletion() must still be waiting on worker 0 at this point (its own trailing marker sits
    // behind the still-blocked source task) - true regardless of the fix.
    assertThat(waitCompletionReturned.getCount()).as("waitCompletion() must still be waiting on worker 0").isEqualTo(1L);

    // RELEASE THE SOURCE TASK: it now schedules the cascade onto worker 1, whose own marker has
    // ALREADY fired - the exact race #6462 describes - and then finishes, letting worker 0's own
    // trailing marker run.
    releaseSourceTask.countDown();

    assertThat(cascadeStarted.await(10, TimeUnit.SECONDS)).as("the cascade task must be scheduled and started").isTrue();

    // THE ASSERTION: with the cascade deliberately held open, a waitCompletion() that returns now
    // provably returned before "all pending operations" completed - the bug (#6462). The fix must
    // block here until the cascade is released below.
    assertThat(waitCompletionReturned.await(500, TimeUnit.MILLISECONDS))
        .as("waitCompletion() must not return while a cross-slot cascade task scheduled from another task's own "
            + "callback is still running")
        .isFalse();

    releaseCascade.countDown();

    assertThat(waitCompletionReturned.await(10, TimeUnit.SECONDS)).as("waitCompletion() must eventually return").isTrue();
    assertThat(waitCompletionResult.get()).as("waitCompletion() must report success, not a timeout").isTrue();
    assertThat(cascadeCompleted.get()).as("the cascade must have completed by the time waitCompletion() returned").isTrue();

    waiter.join(5000);
  }

  private static void awaitLatch(final CountDownLatch latch) {
    try {
      if (!latch.await(20, TimeUnit.SECONDS))
        throw new AssertionError("Latch never released within 20s");
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static DatabaseAsyncExecutorImpl.AsyncThread findWorkerThread(final DatabaseInternal db, final int slot) {
    final String name = "AsyncExecutor-" + db.getName() + "-" + slot;
    return (DatabaseAsyncExecutorImpl.AsyncThread) Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.getName().equals(name)).findFirst()
        .orElseThrow(() -> new IllegalStateException("Worker thread " + name + " not found"));
  }
}
