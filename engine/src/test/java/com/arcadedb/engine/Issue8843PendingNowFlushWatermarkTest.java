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
package com.arcadedb.engine;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Issue #8843: {@link PageManagerFlushThread#waitPagesPendingNowOfDatabaseAreFlushedUntil} waits for the backlog FOUND
 * at the call, not for the pipeline to empty. Under sustained writes the pipeline may never empty, so the wait must also
 * end once as many pages of the database have been written since the call as were pending at its start.
 * <p>
 * Deterministic by construction: the pages of one commit are held in the pipeline with
 * {@code PageManager.suspendFlushAndExecute}, so the pipeline provably does NOT empty while the waiter is parked, and
 * the flushed-pages counter is then advanced by hand - the signal the flush thread gives when it writes pages that
 * arrived after the call. Only the watermark exit can release the waiter.
 */
class Issue8843PendingNowFlushWatermarkTest {

  private static final String DB_PATH = "./target/databases/test-8843-pending-now-watermark";

  private LocalDatabase   database;
  private ExecutorService executor;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = (LocalDatabase) new DatabaseFactory(DB_PATH).create();
    database.getSchema().createDocumentType("Seed");
    assertThat(database.getPageManager().waitAllPagesOfDatabaseAreFlushed(database)).isTrue();
    executor = Executors.newFixedThreadPool(2);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    executor.shutdownNow();
    executor.awaitTermination(30, TimeUnit.SECONDS);
    if (database != null && database.isOpen())
      database.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void theWaitEndsOnceTheBacklogFoundAtTheCallWasWrittenEvenIfThePipelineNeverEmpties() throws Exception {
    final CountDownLatch committed = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Future<?> hold = executor.submit(() -> {
      database.getPageManager().suspendFlushAndExecute(database, () -> {
        database.transaction(() -> database.newDocument("Seed").set("k", 1).save());
        committed.countDown();
        release.await(60, TimeUnit.SECONDS);
      });
      return null;
    });

    try {
      assertThat(committed.await(60, TimeUnit.SECONDS)).isTrue();
      final PageManagerFlushThread flushThread = database.getPageManager().getFlushThread();
      final int pendingAtCall = flushThread.pageIndex.pendingOf(database);
      assertThat(pendingAtCall).as("the held commit's pages are in the pipeline").isGreaterThan(0);

      final AtomicReference<Thread> waiterThread = new AtomicReference<>();
      final Future<Boolean> waited = executor.submit(() -> {
        waiterThread.set(Thread.currentThread());
        return flushThread.waitPagesPendingNowOfDatabaseAreFlushedUntil(database, System.currentTimeMillis() + 60_000L);
      });
      awaitParked(waiterThread, waited);
      assertThat(waited.isDone()).as("nothing has been written yet: the waiter must still be waiting").isFalse();

      // THE FLUSH THREAD WROTE AS MANY PAGES OF THIS DATABASE AS WERE PENDING AT THE CALL (IN PRODUCTION: THE BACKLOG,
      // FOLLOWED BY PAGES OF LATER COMMITS) WHILE THE PIPELINE STAYS NON-EMPTY
      final AtomicLong flushedCounter = flushThread.flushedPagesPerDatabase.get(database);
      assertThat(flushedCounter).isNotNull();
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      flushedCounter.addAndGet(pendingAtCall);

      assertThat(waited.get(60, TimeUnit.SECONDS)).isTrue();
      stopwatch.assertGaveUpWithin(30_000L, "the watermark exit from a wait for an empty pipeline, held for 60 s");
      assertThat(flushThread.pageIndex.pendingOf(database))
          .as("the pipeline never emptied: only the watermark can have ended the wait")
          .isGreaterThan(0);
    } finally {
      release.countDown();
      hold.get(60, TimeUnit.SECONDS);
    }
  }

  @Test
  void theWaitGivesUpAtItsDeadlineWhenNothingIsWritten() throws Exception {
    final CountDownLatch committed = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Future<?> hold = executor.submit(() -> {
      database.getPageManager().suspendFlushAndExecute(database, () -> {
        database.transaction(() -> database.newDocument("Seed").set("k", 1).save());
        committed.countDown();
        release.await(60, TimeUnit.SECONDS);
      });
      return null;
    });

    try {
      assertThat(committed.await(60, TimeUnit.SECONDS)).isTrue();
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThat(database.getPageManager().waitPagesPendingNowOfDatabaseAreFlushed(database, 200L)).isFalse();
      stopwatch.assertGaveUpWithin(30_000L, "a 200 ms bound from a flush held until the test releases it");
    } finally {
      release.countDown();
      hold.get(60, TimeUnit.SECONDS);
    }
    assertThat(database.getPageManager().waitPagesPendingNowOfDatabaseAreFlushed(database, 60_000L)).isTrue();
  }

  private static void awaitParked(final AtomicReference<Thread> thread, final Future<?> future) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 60_000L;
    while (!future.isDone() && System.currentTimeMillis() < deadline) {
      final Thread t = thread.get();
      // Any waiting state counts, not only the drain: a brief wait elsewhere (class loading, logging) can only release
      // the hold early, which weakens the test towards passing on an unfixed reader, never towards failing a fixed one.
      if (t != null && (t.getState() == Thread.State.TIMED_WAITING || t.getState() == Thread.State.WAITING))
        return;
      Thread.sleep(5);
    }
    if (!future.isDone())
      fail("the waiter neither parked nor answered within 60 s");
  }
}
