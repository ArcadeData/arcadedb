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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #7004, two defects around {@code setParallelLevel()}:
 * <ol>
 *   <li>{@code backPressurePercentage} is executor-wide but was (re)read from the configuration inside every
 *   {@code AsyncThread} constructor, so each resize silently reverted whatever {@code setBackPressure()} asked for.</li>
 *   <li>{@code DatabaseAsyncScanBucket} bailed on the worker's {@code shutdown} flag, which a shrinking
 *   {@code setParallelLevel()} sets on the workers it retires while they drain their queues - so a scan task queued on
 *   a retired worker stopped after its first record while {@code scanType()} still reported success.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7004AsyncParallelLevelTest extends TestHelper {

  private static final String TYPE    = "Issue7004Doc";
  private static final int    RECORDS = 500;

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

    database.getSchema().createDocumentType(TYPE, 2);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set("id", i).save();
    });

    // PARK BOTH WORKERS ON A GATE, SO THE SCAN TASKS SCHEDULED NEXT ARE PROVABLY STILL QUEUED - NOT RUN - WHEN THE
    // SHRINK RETIRES ONE OF THE TWO. WHICH BUCKET LANDS ON WHICH SLOT DOES NOT MATTER: BOTH SLOTS ARE GATED.
    final CountDownLatch gatesEntered = new CountDownLatch(2);
    final CountDownLatch release = new CountDownLatch(1);
    for (int slot = 0; slot < 2; slot++)
      assertThat(async.scheduleTask(slot, AsyncTestTasks.awaitTask(gatesEntered, release), true, 0)).isTrue();
    assertThat(gatesEntered.await(30, TimeUnit.SECONDS)).as("both workers must be parked on their gate").isTrue();

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
    final long deadline = System.currentTimeMillis() + 30_000;
    while (async.getStats().queueSize < 2 && System.currentTimeMillis() < deadline)
      Thread.sleep(10);
    assertThat(async.getStats().queueSize).as("the two bucket scans must be queued behind the gates").isEqualTo(2);

    // SHRINK TO ONE WORKER, FROM ITS OWN THREAD BECAUSE THE RESIZE WAITS FOR THE RETIRED WORKER TO DRAIN - WHICH IT
    // CANNOT DO UNTIL THE GATE IS RELEASED. THE RETIRED WORKER NOW CARRIES THE shutdown FLAG WITH THE SCAN OF ONE
    // BUCKET STILL IN ITS QUEUE: THE SHAPE THE BUG NEEDS.
    final AtomicReference<Throwable> resizeFailure = new AtomicReference<>();
    final Thread resizer = new Thread(() -> {
      try {
        async.setParallelLevel(1);
      } catch (final Throwable t) {
        resizeFailure.set(t);
      }
    }, "issue7004-resizer");
    resizer.start();
    while (async.getThreadCount() != 1 && System.currentTimeMillis() < deadline)
      Thread.sleep(10);
    assertThat(async.getThreadCount()).as("the survivors must be published before the gates open").isEqualTo(1);

    release.countDown();
    resizer.join(30_000);
    scanner.join(30_000);

    assertThat(resizeFailure.get()).isNull();
    assertThat(scanFailure.get()).isNull();
    assertThat(scanner.isAlive()).as("scanType() must have returned").isFalse();
    assertThat(scanned.get())
        .as("the bucket scan drained by the retired worker must deliver every record, not stop after the first one")
        .isEqualTo(RECORDS);
  }
}
