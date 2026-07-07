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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageManagerFlushThread.PagesToFlush;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression test for issue #5068: {@code suspendFlushAndExecute} ownership was first-caller-wins.
 * <p>
 * {@code PageManagerFlushThread.setSuspended(db, true)} was {@code putIfAbsent}-based: only the FIRST
 * caller owned the suspension, but {@code suspendFlushAndExecute} ran every caller's callback regardless
 * (#4958). When the owner exited it resumed flushing (and synchronously flushed the deferred batches)
 * while a concurrent non-owning caller (SQL BACKUP DATABASE, database verify, HA snapshot serving) was
 * still reading the database files - torn reads.
 * <p>
 * The fix makes the suspension refcounted: flushing stays suspended while the refcount is greater than
 * zero and is resumed (deferred batches flushed) only when the LAST suspender exits, so every caller of
 * {@code suspendFlushAndExecute} legitimately owns its whole window.
 */
class PageManagerFlushSuspendRefCountTest extends TestHelper {

  /**
   * Two overlapping {@code suspendFlushAndExecute} windows on the same database: flushing must remain
   * suspended for the whole union of the two windows and resume only after BOTH exited. On the pre-fix
   * code the first caller's exit resumed flushing while the second caller was still inside its callback.
   */
  @Test
  void overlappingSuspendersBothOwnTheirWindow() {
    final Database db = (Database) database;
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();

    final CountDownLatch firstInside = new CountDownLatch(1);
    final CountDownLatch secondInside = new CountDownLatch(1);
    final CountDownLatch releaseFirst = new CountDownLatch(1);
    final CountDownLatch releaseSecond = new CountDownLatch(1);
    final AtomicBoolean secondSawSuspendedAfterFirstExit = new AtomicBoolean(false);

    final Thread first = new Thread(() -> {
      try {
        pageManager.suspendFlushAndExecute(db, () -> {
          firstInside.countDown();
          releaseFirst.await(10, TimeUnit.SECONDS);
        });
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }, "test-suspender-1");

    final Thread second = new Thread(() -> {
      try {
        pageManager.suspendFlushAndExecute(db, () -> {
          secondInside.countDown();
          releaseSecond.await(10, TimeUnit.SECONDS);
          // By now the first suspender has exited: this window must STILL be suspended.
          secondSawSuspendedAfterFirstExit.set(pageManager.isPageFlushingSuspended(db));
        });
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }, "test-suspender-2");

    assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
      first.start();
      firstInside.await();

      second.start();
      secondInside.await();
      assertThat(pageManager.isPageFlushingSuspended(db)).isTrue();

      // The first suspender exits while the second is still inside its window.
      releaseFirst.countDown();
      first.join();

      // Pre-fix: the first caller owned the flag, so its exit resumed flushing here.
      assertThat(pageManager.isPageFlushingSuspended(db)).isTrue();

      releaseSecond.countDown();
      second.join();
      assertThat(secondSawSuspendedAfterFirstExit.get()).isTrue();

      // Only after the LAST suspender exits the flushing is resumed.
      assertThat(pageManager.isPageFlushingSuspended(db)).isFalse();
    });
  }

  /**
   * White-box refcount contract on {@link PageManagerFlushThread#setSuspended}: only the transition
   * 0 to 1 reports "acquired" and only the transition 1 to 0 reports "resumed"; a non-last release keeps
   * the database suspended. On the pre-fix code the first release always cleared the flag.
   */
  @Test
  void setSuspendedIsRefCounted() {
    // Constructing the flush thread directly does NOT start the background thread.
    final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, new ContextConfiguration());
    final Database db = (Database) database;

    assertThat(flush.setSuspended(db, true)).isTrue(); // FIRST SUSPENDER: 0 -> 1
    assertThat(flush.setSuspended(db, true)).isFalse(); // OVERLAPPING SUSPENDER: 1 -> 2
    assertThat(flush.isSuspended(db)).isTrue();

    assertThat(flush.setSuspended(db, false)).isFalse(); // NON-LAST RELEASE: 2 -> 1
    assertThat(flush.isSuspended(db)).isTrue(); // PRE-FIX: THIS WAS false - THE FIRST RELEASE RESUMED FLUSHING

    assertThat(flush.setSuspended(db, false)).isTrue(); // LAST RELEASE: 1 -> 0
    assertThat(flush.isSuspended(db)).isFalse();

    // A release without a matching suspend is a no-op instead of corrupting the refcount.
    assertThat(flush.setSuspended(db, false)).isFalse();
    assertThat(flush.isSuspended(db)).isFalse();
  }

  /**
   * Contract test for the resume gate (not reproducible pre-fix, where an acquire during the resume
   * simply became a non-owner): a NEW suspender arriving while the last release is synchronously flushing
   * the deferred batches must WAIT until the resume completes, so its window can never overlap the
   * deferred-batch writes. The deferred flush is stalled via the package-private
   * {@code removeFromFlushIndex} hook invoked once per deferred page.
   */
  @Test
  void suspendDuringResumeWaitsUntilDeferredFlushCompletes() {
    final CountDownLatch resumeReachedDeferredFlush = new CountDownLatch(1);
    final CountDownLatch allowResumeToComplete = new CountDownLatch(1);
    final AtomicBoolean stalled = new AtomicBoolean(false);

    final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, new ContextConfiguration()) {
      @Override
      void removeFromFlushIndex(final MutablePage page) {
        // Stall the resume exactly once, in the middle of the deferred-batch flush (Phase 1).
        if (stalled.compareAndSet(false, true)) {
          resumeReachedDeferredFlush.countDown();
          try {
            allowResumeToComplete.await(10, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        super.removeFromFlushIndex(page);
      }
    };

    final Database db = (Database) database;

    assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
      flush.setSuspended(db, true);

      // Defer one batch while suspended. The fake file id does not exist, so the resume-time flush of
      // this page is a silent skip - no real I/O is performed on the test database.
      final PageId pageId = new PageId(database, 999_888, 0);
      final MutablePage page = new MutablePage(pageId, 1024, new byte[1024], 0, 0);
      final PagesToFlush batch = new PagesToFlush(List.of(page));
      flush.pageIndex.put(pageId, page);
      flush.queue.offer(batch);
      flush.flushPagesFromQueueToDisk(null, 1_000L);
      assertThat(flush.queue).isEmpty(); // THE BATCH WAS DEFERRED, NOT LEFT IN THE QUEUE

      final Thread resumer = new Thread(() -> flush.setSuspended(db, false), "test-resumer");
      resumer.start();
      resumeReachedDeferredFlush.await();

      // While the resume is mid-flight through the deferred writes, a new suspender must block.
      final CountDownLatch acquired = new CountDownLatch(1);
      final Thread newSuspender = new Thread(() -> {
        flush.setSuspended(db, true);
        acquired.countDown();
      }, "test-new-suspender");
      newSuspender.start();

      assertThat(acquired.await(500, TimeUnit.MILLISECONDS)).isFalse(); // STILL BLOCKED ON THE RESUME

      allowResumeToComplete.countDown();
      resumer.join();

      assertThat(acquired.await(10, TimeUnit.SECONDS)).isTrue(); // ACQUIRED AFTER THE RESUME COMPLETED
      newSuspender.join();
      assertThat(flush.isSuspended(db)).isTrue();

      flush.setSuspended(db, false);
      assertThat(flush.isSuspended(db)).isFalse();
    });
  }

  /**
   * Regression test for the second half of the #5068 fix: an interrupt delivered while
   * {@code suspendFlushAndExecute} waits for the in-flight batch ({@code waitForCurrentFlushToComplete})
   * must still release this caller's suspension reference. Pre-fix the wait ran BEFORE the try block, so
   * the {@code InterruptedException} skipped {@code setSuspended(false)} and the database stayed suspended
   * forever. The interleaving is fully deterministic: the in-flight batch is fabricated through the
   * package-private {@code nextPagesToFlush} marker and the suspender interrupts ITSELF before entering,
   * so the wait's {@code Thread.sleep} throws immediately - no timing window.
   */
  @Test
  void interruptDuringWaitForFlushStillReleasesSuspension() {
    final Database db = (Database) database;
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final PageManagerFlushThread flush = pageManager.getFlushThread();

    // Fake in-flight batch for this database: makes waitForCurrentFlushToComplete actually wait. It is
    // installed only in the marker (never in the queue), so no I/O can ever be attempted on it.
    final PageId pageId = new PageId(database, 999_887, 0);
    final MutablePage page = new MutablePage(pageId, 1024, new byte[1024], 0, 0);
    final PagesToFlush inFlight = new PagesToFlush(List.of(page));

    final AtomicBoolean callbackRan = new AtomicBoolean(false);
    final AtomicReference<Throwable> thrown = new AtomicReference<>();

    assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
      // Install the marker only when the flush thread is idle (empty queue, no in-flight batch), so the
      // real thread cannot clear it while the suspender is parked in the wait.
      while (!(flush.queue.isEmpty() && flush.nextPagesToFlush.compareAndSet(null, inFlight)))
        Thread.sleep(10);

      try {
        final Thread suspender = new Thread(() -> {
          // Self-interrupt BEFORE entering: the wait's Thread.sleep(1) then throws on its first call,
          // which is exactly an interrupt landing during waitForCurrentFlushToComplete.
          Thread.currentThread().interrupt();
          try {
            pageManager.suspendFlushAndExecute(db, () -> callbackRan.set(true));
          } catch (final Throwable t) {
            thrown.set(t);
          }
        }, "test-interrupted-suspender");
        suspender.start();
        suspender.join();
      } finally {
        flush.nextPagesToFlush.compareAndSet(inFlight, null);
      }
    });

    // The interrupt aborted the suspension before the callback could run...
    assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
    assertThat(callbackRan.get()).isFalse();
    // ...but this caller's reference was still released. Pre-fix this stayed true forever (the leak).
    assertThat(pageManager.isPageFlushingSuspended(db)).isFalse();
  }
}
