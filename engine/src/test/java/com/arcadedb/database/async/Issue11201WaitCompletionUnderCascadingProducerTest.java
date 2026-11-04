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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11201: {@link DatabaseAsyncExecutor#waitCompletion()} went around its marker pass again while a
 * counter of the tasks scheduled BY WORKERS kept moving. That counter was one per executor, so it also counted the cascades
 * of tasks other threads submitted after the call began: a second thread whose async tasks schedule further async work kept
 * it moving for ever, and the barrier (the one every index build waits on, {@code LocalDatabase.waitForAsyncCompletion()})
 * never returned although the caller's own work was done after the first pass.
 * <p>
 * The interleaving is forced, as in {@link Issue11155WaitCompletionUnderSteadyProducerTest}: one worker, and a producer on
 * its own (non-worker) thread that queues the next task while the running one is held. The only difference with that test is
 * that every producer task schedules one more task from inside the worker before it ends, which is the shape the engine
 * itself uses for the incoming half of a cross-slot bidirectional edge. Each {@code @Timeout} is a hang detector; the budgets
 * passed to {@code waitCompletion} are tripwires between a barrier that returns after a bounded number of passes and one that
 * waits out whatever budget it is given.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11201WaitCompletionUnderCascadingProducerTest extends TestHelper {

  private static final long BUDGET_MS = 30_000;

  @Override
  protected void beginTest() {
    database.async().setParallelLevel(1);
  }

  @Test
  @Timeout(120)
  void waitCompletionReturnsWhileAnotherThreadKeepsSchedulingCascades() throws Exception {
    final DatabaseAsyncExecutor async = database.async();

    try (final CascadingProducer producer = new CascadingProducer(async)) {
      producer.awaitRunning();

      final AtomicBoolean ownTaskDone = new AtomicBoolean();
      async.transaction(() -> ownTaskDone.set(true), 0, null, null, 0);

      final int cascadesBefore = producer.cascadesRun();
      assertThat(async.waitCompletion(BUDGET_MS))
          .as("waitCompletion() must return once the caller's own work is done, whatever cascades another thread keeps scheduling")
          .isTrue();

      assertThat(ownTaskDone.get()).as("a task submitted before waitCompletion() must be done when it returns").isTrue();
      assertThat(producer.cascadesRun()).as("the producer's tasks must have cascaded while the barrier waited, or this test "
          + "proves nothing").isGreaterThan(cascadesBefore);
      assertThat(async.isProcessing()).as("the producer must still be keeping the executor busy, or this test proves nothing")
          .isTrue();
      producer.assertNoErrors();
    }
  }

  @Test
  @Timeout(120)
  void theIndexBuildBarrierReturnsWhileAnotherThreadKeepsSchedulingCascades() throws Exception {
    final DatabaseAsyncExecutor async = database.async();

    try (final CascadingProducer producer = new CascadingProducer(async)) {
      producer.awaitRunning();

      final AtomicBoolean ownTaskDone = new AtomicBoolean();
      async.transaction(() -> ownTaskDone.set(true), 0, null, null, 0);

      // No budget here: this is the call an index build makes, and before the fix it never returned
      ((DatabaseInternal) database).waitForAsyncCompletion();

      assertThat(ownTaskDone.get()).as("a task submitted before the barrier must be done when it returns").isTrue();
      assertThat(async.isProcessing()).as("the producer must still be keeping the executor busy, or this test proves nothing")
          .isTrue();
      producer.assertNoErrors();
    }
  }

  /**
   * The other direction, which the fix must not give up: the cascades of the CALLER's own tasks are still waited for, to any
   * depth, while another thread keeps the executor busy with cascades of its own.
   */
  @Test
  @Timeout(120)
  void theCallersOwnCascadesAreStillAwaitedWhileAnotherThreadCascades() throws Exception {
    final DatabaseAsyncExecutor async = database.async();
    final int depth = 6;

    try (final CascadingProducer producer = new CascadingProducer(async)) {
      producer.awaitRunning();

      final AtomicInteger levelsRun = new AtomicInteger();
      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      async.transaction(() -> scheduleChain(async, levelsRun, errors, depth), 0, null, errors::add, 0);

      assertThat(async.waitCompletion(BUDGET_MS)).as("the caller's own cascades are a finite set, so the wait must end")
          .isTrue();

      assertThat(levelsRun.get()).as("every level of a cascade the caller's own task started must be done when the wait returns")
          .isEqualTo(depth + 1);
      assertThat(errors).isEmpty();
      producer.assertNoErrors();
    }
  }

  /**
   * A task's {@code completed()} runs on the worker too, after the task itself, and what it schedules descends from that task:
   * another thread's tasks cascading from there must not keep the wait going.
   */
  @Test
  @Timeout(120)
  void waitCompletionReturnsWhileAnotherThreadKeepsCascadingFromCompleted() throws Exception {
    final DatabaseAsyncExecutor async = database.async();

    try (final CascadingProducer producer = new CascadingProducer(async, true)) {
      producer.awaitRunning();

      final AtomicBoolean ownTaskDone = new AtomicBoolean();
      async.transaction(() -> ownTaskDone.set(true), 0, null, null, 0);

      final int cascadesBefore = producer.cascadesRun();
      assertThat(async.waitCompletion(BUDGET_MS))
          .as("a cascade another thread's task schedules from completed() is not this wait's to follow").isTrue();

      assertThat(ownTaskDone.get()).as("a task submitted before waitCompletion() must be done when it returns").isTrue();
      assertThat(producer.cascadesRun()).as("the producer's tasks must have cascaded while the barrier waited, or this test "
          + "proves nothing").isGreaterThan(cascadesBefore);
      producer.assertNoErrors();
    }
  }

  /**
   * The other direction: a cascade the caller's OWN task schedules from completed() is that task's, so the wait follows it. The
   * task is held until the wait's marker is queued behind it, so the cascade lands behind that marker: the one order in which
   * only the wait's count, and not the marker, can answer for it.
   */
  @Test
  @Timeout(120)
  void aCascadeTheCallersOwnTaskSchedulesFromCompletedIsAwaited() {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) database.async();
    final AtomicBoolean markerSeen = new AtomicBoolean();
    final AtomicBoolean cascadeDone = new AtomicBoolean();
    final List<Throwable> errors = new CopyOnWriteArrayList<>();

    async.scheduleTask(0, new DatabaseAsyncAbstractTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread worker, final DatabaseInternal database) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(BUDGET_MS);
        while (!markerSeen.get() && System.nanoTime() < deadline)
          markerSeen.set(worker.queue.stream().anyMatch(DatabaseAsyncCompletion.class::isInstance));
      }

      @Override
      public void completed() {
        async.transaction(() -> {
          outlastAWaitThatMissedThis();
          cascadeDone.set(true);
        }, 0, null, errors::add, 0);
      }
    }, true, 0);

    assertThat(async.waitCompletion(BUDGET_MS)).as("the caller's own cascades are a finite set, so the wait must end").isTrue();

    assertThat(markerSeen.get()).as("the marker must have been queued behind the task, or this test proves nothing").isTrue();
    assertThat(cascadeDone.get()).as("what the caller's own task scheduled from completed() must be done when the wait returns")
        .isTrue();
    assertThat(errors).isEmpty();
  }

  /**
   * A DDL command runs on the command pool, whose threads are not workers, and one it dispatches from there is past the sequence
   * the running pass snapshotted, so only the wait's count can answer for it. The first command is held until the caller is
   * inside the wait: submitted any earlier, the second one would be in that snapshot.
   */
  @Test
  @Timeout(120)
  void aCommandAnotherCommandDispatchesFromThePoolIsAwaited() {
    final DatabaseAsyncExecutor async = database.async();
    final AtomicBoolean nestedDone = new AtomicBoolean();
    final List<Throwable> errors = new CopyOnWriteArrayList<>();

    async.command("sql", "CREATE DOCUMENT TYPE Issue11201Outer", new AsyncResultsetCallback() {
      @Override
      public void onComplete(final ResultSet rs) {
        outlastAWaitThatMissedThis();
        async.command("sql", "CREATE DOCUMENT TYPE Issue11201Nested", new AsyncResultsetCallback() {
          @Override
          public void onComplete(final ResultSet rs) {
            outlastAWaitThatMissedThis();
            nestedDone.set(true);
          }

          @Override
          public void onError(final Exception exception) {
            errors.add(exception);
          }
        });
      }

      @Override
      public void onError(final Exception exception) {
        errors.add(exception);
      }
    });

    assertThat(async.waitCompletion(BUDGET_MS)).as("the caller's own cascades are a finite set, so the wait must end").isTrue();

    assertThat(nestedDone.get()).as("a command the caller's own command dispatched must be done when the wait returns").isTrue();
    assertThat(errors).isEmpty();
  }

  /**
   * The executor-wide callback belongs to no submission, and it fires from every marker of every pass, so a callback that
   * schedules work kept the wait going all by itself: each pass produced the cascade that asked for the next pass.
   */
  @Test
  @Timeout(120)
  void anExecutorWideOkCallbackThatSchedulesWorkDoesNotKeepTheWaitGoing() {
    final DatabaseAsyncExecutor async = database.async();
    final AtomicBoolean callbackSchedules = new AtomicBoolean(true);
    final AtomicInteger callbackTasksRun = new AtomicInteger();
    final List<Throwable> errors = new CopyOnWriteArrayList<>();

    async.onOk(() -> {
      if (callbackSchedules.get())
        async.transaction(callbackTasksRun::incrementAndGet, 0, null, errors::add, 0);
    });
    try {
      final AtomicBoolean ownTaskDone = new AtomicBoolean();
      async.transaction(() -> ownTaskDone.set(true), 0, null, errors::add, 0);

      assertThat(async.waitCompletion(BUDGET_MS)).as("a task the executor-wide callback schedules is nobody's cascade").isTrue();

      assertThat(ownTaskDone.get()).as("a task submitted before waitCompletion() must be done when it returns").isTrue();
      assertThat(errors).isEmpty();
    } finally {
      callbackSchedules.set(false);
      async.onOk((OkCallback) null);
    }
    assertThat(async.waitCompletion(BUDGET_MS)).isTrue();
    assertThat(callbackTasksRun.get()).as("the callback must have scheduled work, or this test proves nothing").isGreaterThan(0);
  }

  /** The same for the executor-wide error callback, whose task fails and so asks for the next one by itself. */
  @Test
  @Timeout(120)
  void anExecutorWideErrorCallbackThatSchedulesWorkDoesNotKeepTheWaitGoing() {
    final DatabaseAsyncExecutor async = database.async();
    final AtomicBoolean callbackSchedules = new AtomicBoolean(true);
    final AtomicInteger failuresSeen = new AtomicInteger();
    final Runnable failing = () -> async.transaction(() -> {
      throw new IllegalStateException("expected by the test");
    }, 0, null, null, 0);

    async.onError(e -> {
      failuresSeen.incrementAndGet();
      if (callbackSchedules.get())
        failing.run();
    });
    try {
      failing.run();

      assertThat(async.waitCompletion(BUDGET_MS)).as("a task the executor-wide callback schedules is nobody's cascade").isTrue();
    } finally {
      callbackSchedules.set(false);
    }
    assertThat(async.waitCompletion(BUDGET_MS)).isTrue();
    async.onError((ErrorCallback) null);
    assertThat(failuresSeen.get()).as("the callback must have run, or this test proves nothing").isGreaterThan(0);
  }

  /** Longer than a wait that did not count a cascade takes to wake up. A wait that did is not affected by how long this takes. */
  private static void outlastAWaitThatMissedThis() {
    try {
      Thread.sleep(500);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Runs one level of the chain on a worker and, below the bottom, schedules the next one from that worker. */
  private static void scheduleChain(final DatabaseAsyncExecutor async, final AtomicInteger levelsRun, final List<Throwable> errors,
      final int remaining) {
    levelsRun.incrementAndGet();
    if (remaining > 0)
      async.transaction(() -> scheduleChain(async, levelsRun, errors, remaining - 1), 0, null, errors::add, 0);
  }

  /**
   * Keeps a task queued on the only worker behind the one running there, from its own (non-worker) thread, and every one of
   * those tasks schedules one more task from inside the worker before it ends: a cascade, which is what moved the counter the
   * barrier used to loop on.
   */
  private static final class CascadingProducer implements AutoCloseable {
    private final DatabaseAsyncExecutor async;
    private final Semaphore             started     = new Semaphore(0);
    private final Semaphore             mayFinish   = new Semaphore(0);
    private final AtomicBoolean         stop        = new AtomicBoolean();
    private final AtomicInteger         completed   = new AtomicInteger();
    private final AtomicInteger         cascadesRun = new AtomicInteger();
    private final List<Throwable>       errors      = new CopyOnWriteArrayList<>();
    private final Thread                thread;
    private final boolean               cascadeFromCompleted;

    CascadingProducer(final DatabaseAsyncExecutor async) {
      this(async, false);
    }

    CascadingProducer(final DatabaseAsyncExecutor async, final boolean cascadeFromCompleted) {
      this.async = async;
      this.cascadeFromCompleted = cascadeFromCompleted;
      this.thread = new Thread(() -> {
        try {
          submit();
          submit();
          while (!stop.get()) {
            if (!started.tryAcquire(100, TimeUnit.MILLISECONDS))
              continue;
            // THE NEXT ONE IS QUEUED BEFORE THE RUNNING ONE MAY FINISH
            submit();
            mayFinish.release();
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (final Throwable t) {
          errors.add(t);
        }
      }, "issue11201-cascading-producer");
      this.thread.setDaemon(true);
      this.thread.start();
    }

    private void submit() {
      if (cascadeFromCompleted) {
        ((DatabaseAsyncExecutorImpl) async).scheduleTask(0, new DatabaseAsyncAbstractTask() {
          @Override
          public void execute(final DatabaseAsyncExecutorImpl.AsyncThread worker, final DatabaseInternal database) {
            started.release();
            mayFinish.acquireUninterruptibly();
          }

          @Override
          public void completed() {
            completed.incrementAndGet();
            // A CASCADE: SCHEDULED BY THE WORKER, AFTER THE TASK ITSELF
            async.transaction(cascadesRun::incrementAndGet, 0, null, errors::add, 0);
          }
        }, true, 0);
        return;
      }

      async.transaction(() -> {
        started.release();
        mayFinish.acquireUninterruptibly();
        // A CASCADE: SCHEDULED BY THE WORKER, FROM INSIDE THE TASK
        async.transaction(cascadesRun::incrementAndGet, 0, null, errors::add, 0);
      }, 0, completed::incrementAndGet, errors::add, 0);
    }

    int cascadesRun() {
      return cascadesRun.get();
    }

    void awaitRunning() throws InterruptedException {
      while (completed.get() < 3 || cascadesRun.get() < 2) {
        assertThat(errors).isEmpty();
        Thread.sleep(1);
      }
    }

    void assertNoErrors() {
      assertThat(errors).isEmpty();
    }

    @Override
    public void close() throws InterruptedException {
      stop.set(true);
      // LET EVERY TASK STILL QUEUED RUN THROUGH. BEFORE THE JOIN: A TEST THAT HIT ITS @Timeout GETS HERE INTERRUPTED, AND
      // THE JOIN THROWS AT ONCE, WHICH WOULD LEAVE THE WORKER HELD FOR EVER
      mayFinish.release(1_000_000);
      thread.join(10_000);
      // BOUNDED: A REGRESSION OF THE BARRIER MUST FAIL THE TEST THAT CAUGHT IT, NOT HANG THE CLEANUP PAST ITS @Timeout
      async.waitCompletion(BUDGET_MS);
    }
  }
}
