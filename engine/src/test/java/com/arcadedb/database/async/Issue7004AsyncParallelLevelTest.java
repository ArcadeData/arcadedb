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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #7004, two defects around {@code setParallelLevel()}:
 * <ol>
 *   <li>{@code backPressurePercentage} is executor-wide but was (re)read from the configuration inside every
 *   {@code AsyncThread} constructor, so each resize silently reverted whatever {@code setBackPressure()} asked for.</li>
 *   <li>{@code DatabaseAsyncScanBucket} (and {@code DatabaseAsyncBrowseIterator}, the same shape) bailed on the
 *   worker's {@code shutdown} flag, which a shrinking {@code setParallelLevel()} sets on the workers it retires while
 *   they drain their queues - so a scan task queued on a retired worker stopped after its first record while
 *   {@code scanType()} still reported success.</li>
 * </ol>
 * The success-path waits are stall-discounted slices ({@link StallAwareStopwatch#effectiveMs()}), so a JVM-wide pause
 * late in a long run cannot expire them (#6260); {@code @Timeout} stays as the plain hang detector.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7004AsyncParallelLevelTest extends TestHelper {

  private static final String TYPE      = "Issue7004Doc";
  private static final int    RECORDS   = 500;
  /** Stall-discounted budget of every success-path wait: generous, since a wider bound cannot turn a passing run red. */
  private static final long   BUDGET_MS = 30_000;
  private static final long   POLL_MS   = 10;

  @Test
  void setParallelLevelKeepsTheBackPressureAskedFor() {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) database).async();
    final int configured = database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_BACK_PRESSURE);
    final int requested = configured == 80 ? 70 : 80;

    async.setBackPressure(requested);
    assertThat(async.getBackPressure()).isEqualTo(requested);

    async.setParallelLevel(async.getThreadCount() + 2);
    assertThat(async.getBackPressure()).as("a grow must not revert setBackPressure()").isEqualTo(requested);

    async.setParallelLevel(1);
    assertThat(async.getBackPressure()).as("a shrink must not revert setBackPressure()").isEqualTo(requested);
  }

  @Test
  @Timeout(60)
  void aScanQueuedOnARetiredWorkerRunsToCompletion() throws Exception {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) database).async();
    async.setParallelLevel(2);
    createRecords();

    // PARK BOTH WORKERS ON A GATE, SO THE SCAN TASKS SCHEDULED NEXT ARE PROVABLY STILL QUEUED - NOT RUN - WHEN THE
    // SHRINK RETIRES ONE OF THE TWO. WHICH BUCKET LANDS ON WHICH SLOT DOES NOT MATTER: BOTH SLOTS ARE GATED.
    final CountDownLatch gatesEntered = new CountDownLatch(2);
    final CountDownLatch release = new CountDownLatch(1);
    for (int slot = 0; slot < 2; slot++)
      assertThat(async.scheduleTask(slot, AsyncTestTasks.awaitTask(gatesEntered, release), true, 0)).isTrue();
    assertThat(awaited(gatesEntered)).as("both workers must be parked on their gate").isTrue();

    // THE SCAN, FROM ITS OWN THREAD: scanType() BLOCKS UNTIL EVERY BUCKET TASK HAS COMPLETED
    final AtomicInteger scanned = new AtomicInteger();
    final AtomicReference<Throwable> scanFailure = new AtomicReference<>();
    final Thread scanner = new Thread(() -> {
      try {
        database.async().scanType(TYPE, true, record -> {
          scanned.incrementAndGet();
          return true;
        });
      } catch (final Throwable t) {
        scanFailure.set(t);
      }
    }, "issue7004-scanner");
    scanner.start();

    // WAIT UNTIL BOTH BUCKET TASKS SIT IN THE QUEUES BEHIND THE GATES
    assertThat(awaited(() -> async.getStats().queueSize >= 2)).as("the two bucket scans must be queued behind the gates")
        .isTrue();

    final Thread resizer = shrinkToOneWorkerWhileGated(async);

    release.countDown();
    assertThat(awaited(resizer)).as("the resize must complete once the gates open").isTrue();
    assertThat(awaited(scanner)).as("scanType() must have returned").isTrue();

    assertThat(scanFailure.get()).isNull();
    assertThat(scanned.get())
        .as("the bucket scan drained by the retired worker must deliver every record, not stop after the first one")
        .isEqualTo(RECORDS);
  }

  /**
   * Same shape for {@link DatabaseAsyncBrowseIterator}, which gated on the same flag: the iterator task is queued
   * behind a gate on the worker the shrink retires, and every record it holds must reach the callback.
   */
  @Test
  @Timeout(60)
  void aBrowseIteratorQueuedOnARetiredWorkerDeliversEveryRecord() throws Exception {
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) ((DatabaseInternal) database).async();
    async.setParallelLevel(2);
    final List<RID> rids = createRecords();

    // WORKER 1 IS THE ONE THE SHRINK RETIRES. PARK IT ON A GATE AND QUEUE THE ITERATOR TASK BEHIND THE GATE.
    final CountDownLatch gateEntered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    assertThat(async.scheduleTask(1, AsyncTestTasks.awaitTask(gateEntered, release), true, 0)).isTrue();
    assertThat(awaited(gateEntered)).as("worker 1 must be parked on its gate").isTrue();

    final AtomicInteger browsed = new AtomicInteger();
    final CountDownLatch completed = new CountDownLatch(1);
    final List<Identifiable> toBrowse = new ArrayList<>(rids);
    assertThat(async.scheduleTask(1, new DatabaseAsyncBrowseIterator(completed, record -> {
      browsed.incrementAndGet();
      return true;
    }, toBrowse.iterator()), true, 0)).isTrue();

    final Thread resizer = shrinkToOneWorkerWhileGated(async);

    release.countDown();
    assertThat(awaited(resizer)).as("the resize must complete once the gate opens").isTrue();
    assertThat(awaited(completed)).as("the iterator task must complete").isTrue();

    assertThat(browsed.get())
        .as("the iterator drained by the retired worker must deliver every record, not stop after the first one")
        .isEqualTo(RECORDS);
  }

  /**
   * Shrinks the pool to one worker from its own thread, because the resize WAITS for the retired worker to drain -
   * which it cannot do until the caller releases the gate. Returns once the survivors are published, i.e. once the
   * retired worker carries the {@code shutdown} flag with the gated task still in its queue: the shape the bug needs.
   */
  private Thread shrinkToOneWorkerWhileGated(final DatabaseAsyncExecutorImpl async) throws InterruptedException {
    final AtomicReference<Throwable> resizeFailure = new AtomicReference<>();
    final Thread resizer = new Thread(() -> {
      try {
        async.setParallelLevel(1);
      } catch (final Throwable t) {
        resizeFailure.set(t);
      }
    }, "issue7004-resizer");
    resizer.start();
    assertThat(awaited(() -> async.getThreadCount() == 1)).as("the survivors must be published before the gates open")
        .isTrue();
    assertThat(resizeFailure.get()).isNull();
    return resizer;
  }

  private List<RID> createRecords() {
    database.getSchema().createDocumentType(TYPE, 2);
    final List<RID> rids = new ArrayList<>(RECORDS);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        rids.add(database.newDocument(TYPE).set("id", i).save().getIdentity());
    });
    return rids;
  }

  /** A latch await in stall-discounted slices: a JVM-wide pause inside the wait does not count against the budget. */
  private static boolean awaited(final CountDownLatch latch) throws InterruptedException {
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    do {
      if (latch.await(POLL_MS, TimeUnit.MILLISECONDS))
        return true;
    } while (watch.effectiveMs() < BUDGET_MS);
    return false;
  }

  /** {@link #awaited(CountDownLatch)} for a thread join. */
  private static boolean awaited(final Thread thread) throws InterruptedException {
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    do {
      thread.join(POLL_MS);
      if (!thread.isAlive())
        return true;
    } while (watch.effectiveMs() < BUDGET_MS);
    return false;
  }

  /** {@link #awaited(CountDownLatch)} for a polled condition. */
  private static boolean awaited(final BooleanSupplier condition) throws InterruptedException {
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    do {
      if (condition.getAsBoolean())
        return true;
      Thread.sleep(POLL_MS);
    } while (watch.effectiveMs() < BUDGET_MS);
    return false;
  }
}
