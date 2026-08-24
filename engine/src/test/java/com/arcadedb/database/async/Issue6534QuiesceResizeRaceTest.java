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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue6534QuiesceResizeRaceTest extends TestHelper {

  // The 3s polling window below (code review, PR #6661) puts this on the slower end - comparable to the
  // WORKER_HOLD_MS-scale tests in Issue6526AsyncExecutorFollowUpsTest, which carry the same tag.
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
