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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.query.sql.executor.ResultSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for AsyncWaitCompletion. Each nested class keeps the fixture of the test it was folded from.
 */
class AsyncWaitCompletionTest {

  /**
   * Regression test for issue #11155: {@link DatabaseAsyncExecutor#waitCompletion()} went around its marker pass again for
   * as long as {@link DatabaseAsyncExecutor#isProcessing()} answered true, which also counts the tasks OTHER threads keep
   * submitting after the call began. A steady producer that keeps the queues non-empty - one thread is enough against 3
   * workers, a 4-CPU CI runner - made every pass end with "still processing", for ever:
   * {@code GraphBatchRegressionTest.GraphBatchAsyncPoolChurn.unrelatedAsyncCallerSurvivesConcurrentParallelFlushes} hung 20+ minutes
   * in a GraphBatch parallel flush, which waits there for its own connect tasks. {@code LocalDatabase.waitForAsyncCompletion()}
   * (the barrier of every index build) wrapped the same loop around it.
   * <p>
   * The interleaving is forced, not hoped for: one worker, and a producer that queues the next task while the running one is
   * held, so at every instant after the first task a task is queued behind the one running. The executor is therefore never
   * idle, which is what the old loop waited for. Each {@code @Timeout} is a hang detector.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WaitCompletionUnderSteadyProducer extends TestHelper {

    private static final String VERTEX_TYPE = "SteadyVertex";
    private static final String EDGE_TYPE   = "STEADY_EDGE";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType(VERTEX_TYPE);
        database.getSchema().createEdgeType(EDGE_TYPE);
      });
      database.async().setParallelLevel(1);
    }

    @Test
    @Timeout(60)
    void waitCompletionReturnsWhileAnotherThreadKeepsTheExecutorBusy() throws Exception {
      final DatabaseAsyncExecutor async = database.async();

      try (final SteadyProducer producer = new SteadyProducer(async)) {
        producer.awaitRunning();

        // SUBMITTED BEFORE THE WAIT: the one task the wait owes the caller, queued behind the producer's
        final AtomicBoolean ownTaskDone = new AtomicBoolean();
        async.transaction(() -> ownTaskDone.set(true), 0, null, null, 0);

        async.waitCompletion();

        assertThat(ownTaskDone.get()).as("a task submitted before waitCompletion() must be done when it returns").isTrue();
        assertThat(async.isProcessing()).as("the producer must still be keeping the executor busy, or this test proves nothing")
            .isTrue();
        producer.assertNoErrors();
      }
    }

    @Test
    @Timeout(60)
    void theIndexBuildBarrierReturnsWhileAnotherThreadKeepsTheExecutorBusy() throws Exception {
      final DatabaseAsyncExecutor async = database.async();

      try (final SteadyProducer producer = new SteadyProducer(async)) {
        producer.awaitRunning();

        final AtomicBoolean ownTaskDone = new AtomicBoolean();
        async.transaction(() -> ownTaskDone.set(true), 0, null, null, 0);

        ((DatabaseInternal) database).waitForAsyncCompletion();

        assertThat(ownTaskDone.get()).as("a task submitted before the barrier must be done when it returns").isTrue();
        assertThat(async.isProcessing()).as("the producer must still be keeping the executor busy, or this test proves nothing")
            .isTrue();
        producer.assertNoErrors();
      }
    }

    /**
     * The same starvation one barrier step earlier: DDL dispatched through {@code async().command()} runs on the command pool,
     * not on a worker, and the barrier waited for no command to be in flight, which a steady producer of commands never lets
     * happen. It waits for the commands submitted before it now.
     */
    @Test
    @Timeout(60)
    void waitCompletionReturnsWhileAnotherThreadKeepsSubmittingCommands() throws Exception {
      final DatabaseAsyncExecutor async = database.async();

      try (final SteadyProducer producer = new SteadyProducer(async, true)) {
        producer.awaitRunning();

        final AtomicBoolean ownCommandDone = new AtomicBoolean();
        async.command("sql", "CREATE DOCUMENT TYPE OwnCommand", (AsyncResultsetCallback) resultSet -> ownCommandDone.set(true));

        async.waitCompletion();

        assertThat(ownCommandDone.get()).as("a command submitted before waitCompletion() must be done when it returns").isTrue();
        assertThat(async.isProcessing()).as("the producer must still have a command in flight, or this test proves nothing")
            .isTrue();
        producer.assertNoErrors();
      }
    }

    @Test
    @Timeout(60)
    void aParallelGraphBatchFlushEndsWhileAnotherThreadKeepsTheExecutorBusy() throws Exception {
      final int vertices = 20;
      final int edges = 40;

      try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(edges).withParallelFlush(true).build()) {
        final RID[] vertexRIDs = batch.createVertices(VERTEX_TYPE, vertices);

        try (final SteadyProducer producer = new SteadyProducer(database.async())) {
          producer.awaitRunning();

          for (int i = 0; i < edges; i++)
            batch.newEdge(vertexRIDs[i % vertices], EDGE_TYPE, vertexRIDs[(i + 1) % vertices]);
          // The batch size is reached on the last edge, so the flush has run by now; this one has nothing left to do
          batch.flush();

          assertThat(database.async().isProcessing())
              .as("the producer must still be keeping the executor busy, or this test proves nothing").isTrue();
          producer.assertNoErrors();
        }
      }

      assertThat(database.countType(EDGE_TYPE, false)).isEqualTo(edges);
    }

    /**
     * Keeps a task queued on slot 0 behind the one running there, from its own (non-worker) thread: each task is held until
     * the producer has queued the one after next, so the queue is never empty, not even in the instant the worker polls.
     * With {@code commands}, the same with DDL commands, which run on the command pool: each is held in its callback until
     * the next is submitted, so a command is in flight at every instant.
     */
    private static final class SteadyProducer implements AutoCloseable {
      private final DatabaseAsyncExecutor async;
      private final boolean               commands;
      private final Semaphore             started   = new Semaphore(0);
      private final Semaphore             mayFinish = new Semaphore(0);
      private final AtomicBoolean         stop      = new AtomicBoolean();
      private final AtomicInteger         completed = new AtomicInteger();
      private final AtomicInteger         submitted = new AtomicInteger();
      private final List<Throwable>       errors    = new CopyOnWriteArrayList<>();
      private final Thread                thread;

      SteadyProducer(final DatabaseAsyncExecutor async) {
        this(async, false);
      }

      SteadyProducer(final DatabaseAsyncExecutor async, final boolean commands) {
        this.async = async;
        this.commands = commands;
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
        }, "issue11155-steady-producer");
        this.thread.setDaemon(true);
        this.thread.start();
      }

      private void submit() {
        if (commands)
          async.command("sql", "CREATE DOCUMENT TYPE SteadyCommand" + submitted.incrementAndGet(), new AsyncResultsetCallback() {
            @Override
            public void onComplete(final ResultSet resultSet) {
              started.release();
              mayFinish.acquireUninterruptibly();
              completed.incrementAndGet();
            }

            @Override
            public void onError(final Exception exception) {
              errors.add(exception);
            }
          });
        else
          async.transaction(() -> {
            started.release();
            mayFinish.acquireUninterruptibly();
          }, 0, completed::incrementAndGet, errors::add, 0);
      }

      void awaitRunning() throws InterruptedException {
        while (completed.get() < 3) {
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
        async.waitCompletion();
      }
    }
  }

  /**
   * Regression test for issue #11201: {@link DatabaseAsyncExecutor#waitCompletion()} went around its marker pass again while a
   * counter of the tasks scheduled BY WORKERS kept moving. That counter was one per executor, so it also counted the cascades
   * of tasks other threads submitted after the call began: a second thread whose async tasks schedule further async work kept
   * it moving for ever, and the barrier (the one every index build waits on, {@code LocalDatabase.waitForAsyncCompletion()})
   * never returned although the caller's own work was done after the first pass.
   * <p>
   * The interleaving is forced, as in {@link WaitCompletionUnderSteadyProducer}: one worker, and a producer on
   * its own (non-worker) thread that queues the next task while the running one is held. The only difference with that test is
   * that every producer task schedules one more task from inside the worker before it ends, which is the shape the engine
   * itself uses for the incoming half of a cross-slot bidirectional edge. Each {@code @Timeout} is a hang detector; the budgets
   * passed to {@code waitCompletion} are tripwires between a barrier that returns after a bounded number of passes and one that
   * waits out whatever budget it is given.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WaitCompletionUnderCascadingProducer extends TestHelper {

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
  @Nested
  class IdleAfterWaitCompletion extends TestHelper {

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
  @Nested
  class CrossSlotWaitCompletionMissesCascade extends TestHelper {

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

  /**
   * Issue #10537: the async barrier ({@code waitForAsyncCompletion}) returns at once for the thread that holds a quiescence, because the
   * quiescence committed every worker's batch and holds the workers parked, so the marker the barrier would enqueue behind them is never
   * dequeued. The exemption is the holder's alone: the lock it is read from is reentrant and owned by one thread, so another thread, which
   * the workers are parked against as well, is not exempted and keeps its barrier.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AsyncBarrierUnderQuiescence extends TestHelper {

    /** A database that never touched the async API has no executor: the quiescence and the barrier are free, and create none. */
    @Test
    void withoutAnExecutorTheQuiescenceAndTheBarrierAreFreeAndCreateNone() {
      assertThat(((LocalDatabase) database).getAsyncIfExists()).isNull();

      try (final AsyncQuiesce asyncPaused = ((DatabaseInternal) database).quiesceAsync()) {
        ((DatabaseInternal) database).waitForAsyncCompletion();
      }

      assertThat(((LocalDatabase) database).getAsyncIfExists()).as("no worker pool was started to be parked").isNull();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void onlyTheThreadThatHoldsTheQuiescenceIsExemptedFromTheBarrier() throws Exception {
      database.getSchema().createVertexType("Batch");
      for (int i = 0; i < 100; i++)
        database.async().createRecord(database.newVertex("Batch").set("i", i), null);
      final DatabaseAsyncExecutorImpl executor = ((LocalDatabase) database).getAsyncIfExists();
      assertThat(executor).isNotNull();
      assertThat(executor.isQuiescedByCurrentThread()).as("nothing is quiesced yet").isFalse();

      final ExecutorService other = Executors.newSingleThreadExecutor();
      try {
        try (final AsyncQuiesce asyncPaused = ((DatabaseInternal) database).quiesceAsync()) {
          assertThat(executor.isQuiescedByCurrentThread()).as("the holder").isTrue();
          assertThat(other.submit(executor::isQuiescedByCurrentThread).get()).as("another thread").isFalse();

          // The holder does not wait on workers it has parked, and a nested quiescence rides on its own
          ((DatabaseInternal) database).waitForAsyncCompletion();
          try (final AsyncQuiesce nested = ((DatabaseInternal) database).quiesceAsync()) {
            assertThat(executor.isQuiescedByCurrentThread()).isTrue();
          }
          assertThat(executor.isQuiescedByCurrentThread()).as("the outer quiescence is still held").isTrue();
        }

        assertThat(executor.isQuiescedByCurrentThread()).as("released").isFalse();
      } finally {
        other.shutdownNow();
      }

      // Released, the barrier is the real one again and sees what the async side wrote
      ((DatabaseInternal) database).waitForAsyncCompletion();
      assertThat(database.countType("Batch", false)).isEqualTo(100L);
    }
  }

  /**
   * Regression test for issue #6534: {@code quiesceWorkers()} used to release {@code resizeLock} right after
   * scheduling its park tasks, BEFORE waiting for them to be confirmed. A concurrent {@code setParallelLevel()} GROW
   * landing in that window published brand-new workers that this quiescence's park latch was never sized to include -
   * so the quiescence could confirm "everything is parked" and hand back a clean barrier while a worker that came into
   * existence a moment earlier was free to write, entirely unparked. An index build reading right after would then be
   * scanning under a torn view, precisely the failure {@code quiesceWorkers()} exists to prevent (#6281).
   * <p>
   * The fix holds {@code resizeLock} for the WHOLE quiescence (scheduling AND the await), not just the scheduling
   * loop, so a concurrent {@code setParallelLevel()} - grow or shrink - cannot publish anything until every worker this
   * quiescence knows about has confirmed parked. This test pins that: a grow attempted while the quiescence is
   * genuinely stuck waiting on a busy worker must not be able to publish its new pool size until the quiescence
   * itself completes.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class QuiesceResizeRace extends TestHelper {

    // The 3s polling window below (code review, PR #6661) puts this on the slower end - comparable to the
    // WORKER_HOLD_MS-scale tests in AsyncExecutorConfigurationTest.AsyncExecutorFollowUps, which carry the same tag.
    @Test
    @Tag("slow")
    @Timeout(60)
    void aGrowCannotPublishWhileAQuiescenceIsStillWaitingOnABusyWorker() throws Exception {
      final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) database).async();
      async.setParallelLevel(1);

      // Occupies the only worker so the park task the quiescence schedules behind it cannot run until this is
      // released - which is what keeps the quiescence stuck in parked.await() for as long as this test needs.
      final CountDownLatch taskStarted = new CountDownLatch(1);
      final CountDownLatch releaseTask = new CountDownLatch(1);
      assertThat(async.scheduleTask(0, new DatabaseAsyncTask() {
        @Override
        public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal db) {
          taskStarted.countDown();
          try {
            assertThat(releaseTask.await(30, TimeUnit.SECONDS)).isTrue();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }

        @Override
        public void completed() {
        }
      }, true, 0)).isTrue();
      assertThat(taskStarted.await(10, TimeUnit.SECONDS)).isTrue();

      final AtomicReference<Throwable> quiesceFailure = new AtomicReference<>();
      final CountDownLatch quiesceReturned = new CountDownLatch(1);
      // quiesceLock is a plain ReentrantLock, so the handle it hands out must be closed by the SAME thread that
      // acquired it - the quiescer below both opens and closes it, never the main test thread.
      final CountDownLatch closeQuiesce = new CountDownLatch(1);
      final Thread quiescer = new Thread(() -> {
        try (final AsyncQuiesce quiesce = async.quiesceWorkers()) {
          quiesceReturned.countDown();
          assertThat(closeQuiesce.await(30, TimeUnit.SECONDS)).isTrue();
        } catch (final Throwable e) {
          quiesceFailure.set(e);
          quiesceReturned.countDown();
        }
      }, "issue6534-quiescer");
      quiescer.start();

      final Thread grower = new Thread(() -> async.setParallelLevel(3), "issue6534-grower");
      grower.start();

      // Polled rather than a single check after a fixed sleep (code review, PR #6661): a one-shot check could flake
      // on a slow/contended runner that had not yet let the quiescer reach resizeLock.lock() and schedule its park
      // task within the sleep window, reporting a false pass rather than exercising the race. Sampling continuously
      // over a generous window means the assertion only ever fails when the grow ACTUALLY published early - which is
      // the regression this test exists to catch - not when the scheduler was merely slow to run the quiescer.
      final long deadline = System.currentTimeMillis() + 3_000;
      while (System.currentTimeMillis() < deadline) {
        assertThat(async.getThreadCount())
            .as("a grow must not publish a new pool size while a quiescence is still waiting on a busy worker to park")
            .isEqualTo(1);
        Thread.sleep(20);
      }
      assertThat(quiesceReturned.getCount()).as("the quiescence itself must still be waiting too").isEqualTo(1L);

      // Let the busy task finish: its park task can now run, count down the latch and let the quiescence complete.
      releaseTask.countDown();

      assertThat(quiesceReturned.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(quiesceFailure.get()).isNull();

      closeQuiesce.countDown();
      quiescer.join(30_000);
      assertThat(quiescer.isAlive()).isFalse();
      assertThat(quiesceFailure.get()).isNull();

      grower.join(30_000);
      assertThat(grower.isAlive()).as("the grow must not hang once the quiescence releases resizeLock").isFalse();
      assertThat(async.getThreadCount()).as("the grow must have published once it was finally allowed to").isEqualTo(3);
    }
  }
}
