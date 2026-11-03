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
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;
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
 * Regression test for issue #11155: {@link DatabaseAsyncExecutor#waitCompletion()} went around its marker pass again for
 * as long as {@link DatabaseAsyncExecutor#isProcessing()} answered true, which also counts the tasks OTHER threads keep
 * submitting after the call began. A steady producer that keeps the queues non-empty - one thread is enough against 3
 * workers, a 4-CPU CI runner - made every pass end with "still processing", for ever:
 * {@code Issue5665GraphBatchAsyncPoolChurnTest.unrelatedAsyncCallerSurvivesConcurrentParallelFlushes} hung 20+ minutes
 * in a GraphBatch parallel flush, which waits there for its own connect tasks. {@code LocalDatabase.waitForAsyncCompletion()}
 * (the barrier of every index build) wrapped the same loop around it.
 * <p>
 * The interleaving is forced, not hoped for: one worker, and a producer that queues the next task while the running one is
 * held, so at every instant after the first task a task is queued behind the one running. The executor is therefore never
 * idle, which is what the old loop waited for. Each {@code @Timeout} is a hang detector.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11155WaitCompletionUnderSteadyProducerTest extends TestHelper {

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
