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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the #5062 review, round 3 (point 1): a producer offering onto the full queue
 * of a worker that exhausted its help-deferral budget and fell into the bounded wait (so
 * {@code waitingCrossSlotOffer} is true and its completed-task count is flat) threw the
 * "Asynchronous queue is stalled" exception after a SINGLE no-progress window, even when the
 * worker's cross-slot peer was merely busy on one slow task and about to accept the hand-off - a
 * narrower recurrence of the #4953 false positive. The cross-slot-park branch now requires
 * {@code STALLED_CROSS_SLOT_NO_PROGRESS_WINDOWS} consecutive flat windows before throwing.
 */
class AsyncSlowPeerNoFalseStallTest extends TestHelper {

  @Test
  @Timeout(60)
  void producerMustNotThrowWhileHelpWaitingWorkerPeerIsMerelySlow() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getConfiguration().setValue(GlobalConfiguration.ASYNC_OPERATIONS_QUEUE_SIZE, 2); // CAPACITY 1 PER WORKER
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    async.setParallelLevel(2);
    // Force thread re-creation so the queue size above is picked up regardless of the previous level.
    async.setTransactionUseWAL(true);
    // 500ms window: the old code threw after ONE flat window (~500ms), the fixed code needs three
    // (1.5s); the slow peer is released after ~700ms, comfortably between the two bounds.
    async.setCheckForStalledQueuesMaxDelay(500);

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    async.onError(errors::add);

    final CountDownLatch slowPeerStarted = new CountDownLatch(1);
    final CountDownLatch releaseSlowPeer = new CountDownLatch(1);
    final CountDownLatch outerStarted = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    final CountDownLatch executed = new CountDownLatch(4); // PARKED + MARKER + 2 PRODUCER TASKS

    // Worker 1 (the slow-but-live peer): busy until released, with a full queue behind it.
    async.scheduleTask(1, awaitTask(slowPeerStarted, releaseSlowPeer, errors), true, 0);
    assertThat(slowPeerStarted.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(async.scheduleTask(1, countingTask(executed), false, 0)).isTrue();

    // Worker 0: once released, hands a follow-up to worker 1's full queue. With queue capacity 1
    // its help-deferral budget is a single task, so it parks the one queued task below and falls
    // into the bounded wait with waitingCrossSlotOffer=true and a flat completed count.
    async.scheduleTask(0, new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        outerStarted.countDown();
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
        async.scheduleTask(1, countingTask(executed), true, 0);
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    }, true, 0);
    assertThat(outerStarted.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(async.scheduleTask(0, countingTask(executed), false, 0)).isTrue(); // THE TASK IT WILL PARK

    proceed.countDown();

    // Wait until worker 0 parked the queued task (aggregate queue size drops to the peer's filler
    // only): from here on it is budget-exhausted inside the bounded wait.
    final long spinDeadline = System.currentTimeMillis() + 10_000;
    while (async.getStats().queueSize > 1 && System.currentTimeMillis() < spinDeadline)
      Thread.sleep(10);
    assertThat(async.getStats().queueSize).as("worker 0 must park its queued task and fall back").isEqualTo(1L);

    // The producer: first task fills worker 0's queue, second blocks on it while worker 0 shows a
    // flat completed count and waitingCrossSlotOffer=true. It must NOT be thrown a stall exception
    // while the peer is merely slow.
    final AtomicReference<Throwable> producerFailure = new AtomicReference<>();
    final Thread producer = new Thread(() -> {
      try {
        async.scheduleTask(0, countingTask(executed), true, 0);
        async.scheduleTask(0, countingTask(executed), true, 0);
      } catch (final Throwable t) {
        producerFailure.set(t);
      }
    }, getClass().getSimpleName() + "-producer");
    producer.start();

    // Let the producer sit through more than one flat window (the old throw bound), then release
    // the slow peer well before the three-window cross-slot bound.
    Thread.sleep(700);
    releaseSlowPeer.countDown();

    producer.join(20_000);
    assertThat(producer.isAlive()).as("producer must unblock once the peer drains").isFalse();
    assertThat(producerFailure.get())
        .as("a slow-but-live cross-slot peer must not surface as a stall on the producer")
        .isNull();
    assertThat(async.waitCompletion(10_000)).isTrue();
    assertThat(executed.await(5, TimeUnit.SECONDS)).as("every scheduled task must execute").isTrue();
    assertThat(errors).isEmpty();
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
