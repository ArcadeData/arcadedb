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
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6259.
 * <p>
 * {@code PageManager.publishPages} holds the JVM-wide page-manager lock across BOTH halves of publication - the page
 * write and the flush enqueue - and it has to: the snapshot t0 barrier (#6075/#6125) takes that same lock precisely so
 * that from there on no committer can put a page on disk OR into the flush pipeline. The enqueue blocked, though:
 * {@code while (running) queue.offer(batch, 1, SECONDS)}. So whenever the bounded flush queue filled - a write burst, a
 * slow or contended volume, an fsync spike, no suspension or backup involved - the committing thread parked in that
 * offer <b>holding a lock every committer in the process needs</b>, and one database's burst serialized the commits of
 * every unrelated database in the JVM, including ones on idle volumes whose pages could have been written immediately.
 * <p>
 * The queue is meant to backpressure the writers it belongs to; what was wrong is only that the wait was charged to
 * everybody. So the wait moved ahead of the lock, exactly as #6200 moved the deferred-RAM cap: a committer now reserves
 * its queue slot before {@code lock()} and reaches the enqueue with the slot already its own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6259FlushQueueAdmissionTest extends TestHelper {
  /** A file that does not exist, so a fabricated page can be driven through the pipeline without touching a real one. */
  private static final int  FILE_ID               = 4_000;
  private static final int  PAGE_SIZE             = 64;
  /**
   * Bounds only the waits expected to SUCCEED - the short windows asserting "not released yet" are written inline - so
   * a generous value cannot turn a passing run red, while a tight one can under the stop-the-world pauses the shared
   * 12000-test JVM produces late in a run (#6260).
   */
  private static final long ASSERTION_TIMEOUT_SEC = 60;

  /**
   * The heart of the issue, and the invariant rather than the symptom: while a committer waits for room in the flush
   * queue, another thread must still be able to take the page-manager lock.
   * <p>
   * Before the fix that committer did its waiting inside the lock, so this test's second thread - which wants the lock
   * for something that needs no queue slot at all - waited for a disk it has nothing to do with. Every commit in the
   * JVM was behind it.
   * <p>
   * The flush thread is wedged rather than slowed: it is held on the batch monitor it takes to write a batch, which is
   * a real code path (the same monitor the dropped-file purge uses) and stops the pipeline exactly where a stalled
   * disk would. It is always released, on a deadline of its own, because a flush thread left wedged would hang every
   * other test sharing this JVM.
   */
  @Test
  void aCommitterWaitingForFlushQueueRoomDoesNotHoldThePublicationLock() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();
    final PageManagerFlushThread flush = pageManager.getFlushThread();

    // The batch the flush thread will be held on: the test keeps its monitor, which is what the write loop takes.
    final List<MutablePage> wedge = new ArrayList<>(List.of(page(db, 0)));
    final CountDownLatch releaseWedge = new CountDownLatch(1);
    final CountDownLatch wedgeHeld = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final Thread holder = new Thread(() -> {
      synchronized (wedge) {
        wedgeHeld.countDown();
        try {
          // A deadline of its own: whatever happens to the assertions below, the flush thread comes back.
          releaseWedge.await(ASSERTION_TIMEOUT_SEC * 2, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }, "issue6259-wedge-holder");
    holder.setDaemon(true);
    holder.start();
    assertThat(wedgeHeld.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).isTrue();

    final Thread committer;
    final CountDownLatch published = new CountDownLatch(1);
    try {
      // The flush thread polls this batch, then blocks on its monitor: from here nothing leaves the queue.
      flush.scheduleFlushOfPages(wedge);
      final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC);
      while (!isWriting(flush, wedge)) {
        assertThat(System.currentTimeMillis()).as("the flush thread never reached the batch it must be held on")
            .isLessThan(deadline);
        Thread.sleep(1);
      }

      // Fill every remaining slot. Committers are admitted one per free slot, so this is the state a burst against a
      // slow disk reaches on its own - reached here deterministically instead.
      for (int i = flush.queue.remainingCapacity(); i > 0; i--)
        flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(db, i))));
      assertThat(flush.queue.remainingCapacity()).as("the flush queue must be full for this test to test anything")
          .isZero();

      committer = new Thread(() -> {
        try {
          pageManager.publishPages(List.of(page(db, 999_999)), null, true);
          published.countDown();
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      }, "issue6259-committer");
      committer.start();

      assertThat(published.await(500, TimeUnit.MILLISECONDS)).as(
          "a committer must be held while the flush queue is full - that backpressure is the queue's job").isFalse();

      // THE ASSERTION THE WHOLE ISSUE IS ABOUT. Before the fix the committer was parked in queue.offer holding this
      // lock, and this call waited for the wedged flush thread - as would every commit of every other database.
      final CountDownLatch lockTaken = new CountDownLatch(1);
      final Thread locker = new Thread(() -> {
        pageManager.executeInLock(() -> {
          lockTaken.countDown();
          return null;
        });
      }, "issue6259-lock-taker");
      locker.start();

      assertThat(lockTaken.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).as(
          "the page-manager lock must stay available while a committer waits for flush-queue room: waiting inside it stalls every committer of every database in the JVM")
          .isTrue();
      locker.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));

      assertThat(flush.queueSlotWaits.get()).as("and the committer must be waiting for a SLOT, not for the lock")
          .isPositive();
    } finally {
      releaseWedge.countDown();
      holder.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
    }

    // Released, the pipeline drains and the committer goes through.
    assertThat(published.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).as(
        "the committer must be admitted once the flush thread drains the queue").isTrue();
    committer.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
    assertThat(failure.get()).isNull();

    final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC);
    while (!flush.queue.isEmpty() && System.currentTimeMillis() < deadline)
      Thread.sleep(5);
    assertThat(flush.queueReservations.get()).as("and no publication may leave a reservation behind").isZero();
  }

  /**
   * The admission control itself: exactly {@code arcadedb.pageFlushQueue} callers get in, the next one waits, and what
   * lets it in is the flush thread polling a batch.
   * <p>
   * The wait is proven to end on the POLL'S SIGNAL rather than on the fallback interval by stretching that interval to
   * a minute: a wait that ends inside seconds cannot have been ended by it. Without the signal a committer would sit
   * out its interval against a queue that had room all along - under a lock, before the fix, and for everybody.
   */
  @Test
  void aFullQueueHoldsTheNextCommitterUntilTheFlushThreadPollsABatch() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManagerFlushThread flush = smallQueueFlushThread(2);
    // Far longer than the assertions below tolerate, so only the signal can release the wait.
    flush.queueSlotWaitPollMillis = TimeUnit.MINUTES.toMillis(1);

    flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(db, 0))));
    flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(db, 1))));
    assertThat(flush.queue.remainingCapacity()).isZero();
    assertThat(flush.queueReservations.get()).as("an enqueued batch holds a slot, not a reservation").isZero();

    final CountDownLatch admitted = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread committer = new Thread(() -> {
      try {
        if (flush.reserveQueueSlot()) {
          flush.releaseQueueReservation(false);
          admitted.countDown();
        }
      } catch (final Throwable e) {
        failure.compareAndSet(null, e);
      }
    }, "issue6259-waiting-committer");
    committer.start();

    assertThat(admitted.await(500, TimeUnit.MILLISECONDS)).as("no slot, no admission").isFalse();
    assertThat(flush.queueSlotWaits.get()).isEqualTo(1);

    // One batch leaves the queue: one committer gets in.
    flush.flushPagesFromQueueToDisk(null, 20L);

    assertThat(admitted.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).as(
        "the poll must SIGNAL the free slot: with a one-minute fallback interval, only the signal can end this wait")
        .isTrue();
    committer.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
    assertThat(failure.get()).isNull();
    assertThat(flush.queueReservations.get()).isZero();
  }

  /**
   * A reservation is a promise of room, so the enqueue that follows it - the one inside the page-manager lock - must
   * never have to wait. Here the queue is one short of full and the reservation is the last slot: a second batch
   * arriving in between must be refused admission rather than take it.
   */
  @Test
  void aReservedSlotCannotBeTakenByAnotherCommitter() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManagerFlushThread flush = smallQueueFlushThread(2);

    flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(db, 0))));
    assertThat(flush.reserveQueueSlot()).as("one slot left, so one more caller gets in").isTrue();

    final CountDownLatch admitted = new CountDownLatch(1);
    final Thread other = new Thread(() -> {
      try {
        if (flush.reserveQueueSlot())
          admitted.countDown();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }, "issue6259-late-committer");
    other.setDaemon(true);
    other.start();
    assertThat(admitted.await(500, TimeUnit.MILLISECONDS)).as(
        "the last slot is spoken for: admitting a second caller would put the reserver's enqueue back inside the lock")
        .isFalse();

    // The reservation is honoured: the enqueue finds the room it was promised, without waiting.
    flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(db, 1))), true);
    assertThat(flush.queue.remainingCapacity()).isZero();
    assertThat(flush.queueReservations.get()).isZero();

    other.interrupt();
    other.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
  }

  /**
   * Every path out of a publication gives its reservation back - including the ones that never enqueue anything.
   * <p>
   * A leaked reservation is silent and permanent: the pipeline is one slot smaller for the life of the process, and
   * enough of them stop it accepting anything at all. Nothing in a running system would ever point at it.
   */
  @Test
  void noPathLeavesAReservationBehind() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManagerFlushThread flush = smallQueueFlushThread(4);

    // An empty batch is never enqueued (the empty list is the shutdown sentinel), with or without a reservation.
    flush.scheduleFlushOfPages(new ArrayList<>());
    assertThat(flush.queueReservations.get()).isZero();
    assertThat(flush.reserveQueueSlot()).isTrue();
    flush.scheduleFlushOfPages(new ArrayList<>(), true);
    assertThat(flush.queueReservations.get()).as("a reservation handed to an empty batch must come straight back")
        .isZero();

    // A real publication, through the whole PageManager path.
    final PageManager pageManager = db.getPageManager();
    pageManager.publishPages(List.of(page(db, 7)), null, true);
    assertThat(pageManager.getFlushThread().queueReservations.get()).isZero();

    // And a batch dropped because the thread is shutting down.
    flush.closeAndJoin();
    flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(db, 8))));
    assertThat(flush.queueReservations.get()).as("a batch dropped at shutdown must not strand its slot").isZero();
  }

  /** True once the flush thread has polled this batch and is inside the write loop that takes its monitor. */
  private static boolean isWriting(final PageManagerFlushThread flush, final List<MutablePage> pages) {
    final PageManagerFlushThread.PagesToFlush current = flush.nextPagesToFlush.get();
    return current != null && current.pages == pages;
  }

  /**
   * Constructing the flush thread directly does NOT start the background thread, so the pipeline only moves when the
   * test moves it.
   */
  private static PageManagerFlushThread smallQueueFlushThread(final int capacity) {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.PAGE_FLUSH_QUEUE, capacity);
    return new PageManagerFlushThread(PageManager.INSTANCE, cfg);
  }

  private static MutablePage page(final DatabaseInternal database, final int pageNumber) {
    return new MutablePage(new PageId(database, FILE_ID, pageNumber), PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
  }
}
