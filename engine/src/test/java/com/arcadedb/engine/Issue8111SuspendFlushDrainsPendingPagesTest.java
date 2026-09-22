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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression test for issue #8111: a full backup taken with {@code PAGE_SNAPSHOT_ENABLED} disabled reads the
 * "frozen" files through {@link PageManager#suspendFlushAndExecute}, whose only wait -
 * {@code PageManagerFlushThread#waitForCurrentFlushToComplete} - blocks for a batch ALREADY in flight but is a
 * no-op for a page that has been committed and indexed as pending but that the (asynchronous, periodic) flush
 * thread has not yet picked up into a batch at all. Suspending right after that no-op leaves such a page sitting
 * in RAM, never written to disk, for the whole window the callback runs in - a backup taken right after a commit
 * can therefore archive a bucket file at whatever size it had BEFORE that commit, sometimes a bare newly-created
 * file with no data pages in it, which is exactly what
 * {@code Issue7586BackupArchiveEntriesSitAtArchiveRootIT}'s intermittent "restored database has 0 records"
 * failure turned out to be (confirmed by a 300-iteration repro harness outside this suite, not committed here:
 * one bucket entry in the archive at 0 bytes while every other entry, including its own index, was complete).
 * <p>
 * The fix makes {@code suspendFlushAndExecute} drain the WHOLE pending-page index -
 * {@link PageManagerFlushThread#waitAllPagesOfDatabaseAreFlushed} - before it suspends, the same primitive and the
 * same ordering the point-in-time snapshot path ({@code PageManager#openSnapshot}) already uses for the identical
 * reason.
 * <p>
 * Driven the same deterministic way {@link PageManagerFlushSuspendRefCountTest}'s
 * {@code interruptDuringWaitForFlushStillReleasesSuspension} drives the REAL flush thread bound to
 * {@code PageManager.INSTANCE}: a page is indexed directly into {@link PageManagerFlushThread#pageIndex} with a
 * non-existent file id, so it is "pending" from {@code waitAllPagesOfDatabaseAreFlushed}'s point of view without
 * ever being offered to the queue or {@code nextPagesToFlush} - precisely the state a commit leaves behind before
 * the periodic flush loop gets to it - and without risking any real I/O.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8111SuspendFlushDrainsPendingPagesTest extends TestHelper {

  @Test
  void suspendFlushAndExecuteWaitsForAPendingPageNeverOfferedToTheQueue() throws Exception {
    final Database db = (Database) database;
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final PageManagerFlushThread flush = pageManager.getFlushThread();

    // A page this database committed and indexed as pending, but that never reached the queue or an in-flight
    // batch - the state waitForCurrentFlushToComplete alone cannot see. Non-existent file id, so nothing here
    // can ever attempt real I/O on it.
    final PageId pageId = new PageId(database, 999_886, 0);
    final MutablePage page = new MutablePage(pageId, 1024, new byte[1024], 0, 0);
    flush.pageIndex.put(page);

    final CountDownLatch callbackRan = new CountDownLatch(1);
    final AtomicBoolean pendingWhenCallbackRan = new AtomicBoolean(true);

    final Thread suspender = new Thread(() -> {
      try {
        pageManager.suspendFlushAndExecute(db, () -> {
          pendingWhenCallbackRan.set(flush.pageIndex.hasPendingOf(database));
          callbackRan.countDown();
        });
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }, "test-suspender");

    assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
      suspender.start();

      // The callback must NOT run while the page is still pending: pre-fix it ran immediately regardless,
      // because waitForCurrentFlushToComplete had nothing in flight to wait for.
      assertThat(callbackRan.await(500, TimeUnit.MILLISECONDS))
          .as("the callback must wait for the pending page instead of observing a stale file")
          .isFalse();

      // Simulate the flush the periodic thread would eventually have done, and let the drain notice it.
      flush.pageIndex.remove(pageId);

      assertThat(callbackRan.await(10, TimeUnit.SECONDS))
          .as("the callback must run once the pending page has actually been flushed")
          .isTrue();
      suspender.join();
    });

    assertThat(pendingWhenCallbackRan.get())
        .as("by the time the callback ran, the page must no longer be pending - the files it reads must be "
            + "current, not frozen mid-write")
        .isFalse();
    assertThat(pageManager.isPageFlushingSuspended(db)).isFalse();
  }
}
