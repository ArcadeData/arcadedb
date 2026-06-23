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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.engine.PageManagerFlushThread.PagesToFlush;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression test for the suspend/unsuspend defer race in {@link PageManagerFlushThread}.
 * <p>
 * While a database is suspended (e.g. during a backup) the flush thread defers polled batches into an
 * internal map instead of writing them to disk. {@code setSuspended(false)} clears the suspended flag and
 * drains that map. Without serialization, the flush thread could read "suspended == true", then
 * {@code setSuspended(false)} could clear the flag and drain the map, and only THEN the flush thread would
 * offer its batch - stranding it in the deferred map (and {@code pageIndex}) forever, so
 * {@code waitAllPagesOfDatabaseAreFlushed} would loop indefinitely and a "completed" backup would not have
 * flushed all pages.
 * <p>
 * The fix holds a per-database lock across the suspended-check + defer and across the unsuspend's
 * flag-clear + deferred-detach, so a batch deferred during the unsuspend window is always handed back to
 * the main queue. The blocking re-enqueue runs outside the lock to avoid deadlocking the flush thread.
 */
class PageManagerFlushSuspendDeferRaceTest extends TestHelper {

  @Test
  void deferredBatchDuringUnsuspendIsHandedBackToQueue() {
    final CountDownLatch flushReachedCheck = new CountDownLatch(1);
    final CountDownLatch unsuspendReturned = new CountDownLatch(1);
    final AtomicBoolean coordinated = new AtomicBoolean(false);

    // Constructing the flush thread directly does NOT start the background thread; the methods are driven
    // explicitly from the test threads below. isSuspended() is overridden to pause the flush thread exactly
    // at the suspended-check, opening the race window for a concurrent unsuspend.
    final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, new ContextConfiguration()) {
      @Override
      public boolean isSuspended(final Database db) {
        final boolean suspended = super.isSuspended(db);
        if (suspended && coordinated.compareAndSet(false, true)) {
          flushReachedCheck.countDown();
          try {
            // Wait for the concurrent unsuspend to finish (pre-fix) or time out because the per-database
            // lock serializes us against it (post-fix). The timeout guarantees no deadlock either way.
            unsuspendReturned.await(1, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        return suspended;
      }
    };

    final Database db = (Database) database;
    flush.setSuspended(db, true);

    final PageId pageId = new PageId(database, 9, 1);
    final MutablePage page = new MutablePage(pageId, 1024, new byte[1024], 0, 0);
    final PagesToFlush batch = new PagesToFlush(List.of(page));
    flush.pageIndex.put(pageId, page);
    flush.queue.offer(batch);

    final Thread flusher = new Thread(() -> {
      try {
        flush.flushPagesFromQueueToDisk(null, 1_000L);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }, "test-flusher");

    final Thread unsuspender = new Thread(() -> {
      try {
        flushReachedCheck.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      flush.setSuspended(db, false);
      unsuspendReturned.countDown();
    }, "test-unsuspender");

    assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
      flusher.start();
      unsuspender.start();
      flusher.join();
      unsuspender.join();
    });

    // The batch deferred during the unsuspend window must have been re-enqueued for flushing, not stranded
    // in the internal deferred map. queue.contains uses identity (same batch instance re-enqueued).
    assertThat(flush.queue).contains(batch);
  }
}
