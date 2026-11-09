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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.PageManagerFlushThread.PagesToFlush;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Flush queue admission, per-database flush bookkeeping, pending-page accounting and drain behavior of the page manager.
 * <p>
 * Each nested class is the regression suite of one issue (the number is in the nested class name).
 */
class PageManagerFlushQueueAdmissionTest {

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
  @Nested
  class FlushQueueAdmission6259 extends TestHelper {
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

        // Fill every remaining slot OF THIS DATABASE's budget (per-database since #6281). Committers are admitted one
        // per free slot, so this is the state a burst against a slow disk reaches on its own - reached here
        // deterministically instead.
        for (int i = flush.slotsUsedBy(db) + 1; i <= flush.getQueueCapacity(); i++)
          flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(db, i))));
        assertThat(flush.slotsUsedBy(db)).as("the database's flush budget must be exhausted for this test to test anything")
            .isEqualTo(flush.getQueueCapacity());

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
      assertThat(flush.slotsUsedBy(db)).as("and no publication may leave a reservation behind").isZero();
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
      assertThat(flush.slotsUsedBy(db)).as("an enqueued batch KEEPS the slot it was reserved on until it is polled")
          .isEqualTo(2);

      final CountDownLatch admitted = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread committer = new Thread(() -> {
        try {
          if (flush.reserveQueueSlot(db)) {
            flush.releaseQueueReservation(db);
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
      // One batch left the queue and the admitted committer gave its slot straight back: the other batch still holds
      // the one it was enqueued on.
      assertThat(flush.slotsUsedBy(db)).isEqualTo(1);
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
      assertThat(flush.reserveQueueSlot(db)).as("one slot left, so one more caller gets in").isTrue();

      final CountDownLatch admitted = new CountDownLatch(1);
      final Thread other = new Thread(() -> {
        try {
          if (flush.reserveQueueSlot(db))
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
      flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(db, 1))), db);
      assertThat(flush.slotsUsedBy(db)).as("both slots are now held by enqueued batches").isEqualTo(2);

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
      assertThat(flush.slotsUsedBy(db)).isZero();
      assertThat(flush.reserveQueueSlot(db)).isTrue();
      flush.scheduleFlushOfPages(new ArrayList<>(), db);
      assertThat(flush.slotsUsedBy(db)).as("a reservation handed to an empty batch must come straight back")
          .isZero();

      // A real publication, through the whole PageManager path. The slot stays with the batch until the (real, running)
      // flush thread polls it, so the assertion is about what is left AFTER the pipeline drains.
      final PageManager pageManager = db.getPageManager();
      pageManager.publishPages(List.of(page(db, 7)), null, true);
      final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC);
      while (pageManager.getFlushThread().slotsUsedBy(db) > 0 && System.currentTimeMillis() < deadline)
        Thread.sleep(5);
      assertThat(pageManager.getFlushThread().slotsUsedBy(db)).isZero();

      // And a batch dropped because the thread is shutting down. closeAndJoin returns at once here: this flush thread
      // was constructed but never start()ed, and Thread.join() on a thread in NEW state returns immediately. What is
      // under test is the shutdown flag, not the join.
      flush.closeAndJoin();
      flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(db, 8))));
      assertThat(flush.slotsUsedBy(db)).as("a batch dropped at shutdown must not strand its slot").isZero();
    }

    /**
     * The admission bound under real contention, which the deterministic tests above cannot reach: many threads racing
     * the CAS loop of {@code tryReserveQueueSlot} against a queue of 4.
     * <p>
     * The bound is what the enqueue inside the page-manager lock rests on - a reservation is only worth holding if it
     * guarantees a free slot - and a time-of-check/time-of-use regression in that loop would break it while leaving
     * every single-threaded test green. Phase 1 races reservations alone, so the assertion is exact: the number held at
     * once can never exceed the capacity. Phase 2 then runs the whole path (reserve, enqueue, drain) concurrently to
     * prove the accounting comes back to zero rather than drifting a slot per round.
     */
    @Test
    void theAdmissionBoundHoldsUnderContention() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int capacity = 4;
      final int threads = 12;
      final PageManagerFlushThread flush = smallQueueFlushThread(capacity);

      final AtomicInteger held = new AtomicInteger();
      final AtomicInteger maxHeld = new AtomicInteger();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final CountDownLatch start = new CountDownLatch(1);

      // PHASE 1: reservations only. Nothing is ever enqueued, so the queue stays empty and the bound is exactly the
      // number of reservations handed out at once - anything above the capacity is an over-admission.
      final List<Thread> racers = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final Thread racer = new Thread(() -> {
          try {
            start.await();
            for (int i = 0; i < 2_000; i++) {
              if (!flush.reserveQueueSlot(db))
                return;
              maxHeld.accumulateAndGet(held.incrementAndGet(), Math::max);
              held.decrementAndGet();
              flush.releaseQueueReservation(db);
            }
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          }
        }, "issue6259-racer-" + t);
        racer.setDaemon(true);
        racers.add(racer);
        racer.start();
      }
      start.countDown();
      for (final Thread racer : racers)
        racer.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));

      assertThat(failure.get()).isNull();
      assertThat(maxHeld.get()).as(
          "the CAS loop must never hand out more reservations than the queue has slots: each one is a promise that the enqueue inside the page-manager lock will not block")
          .isLessThanOrEqualTo(capacity);
      assertThat(maxHeld.get()).as("and the race must actually have contended, or this test proves nothing").isGreaterThan(1);
      assertThat(flush.slotsUsedBy(db)).isZero();

      // PHASE 2: the whole path at once - producers reserving and enqueueing, a drainer polling - so the two halves of
      // the sum (queue size and reservations) move against each other under contention rather than one at a time.
      final int batchesPerProducer = 200;
      final int producerCount = 6;
      final CountDownLatch producersDone = new CountDownLatch(producerCount);
      final List<Thread> producers = new ArrayList<>();
      for (int t = 0; t < producerCount; t++) {
        final int id = t;
        final Thread producer = new Thread(() -> {
          try {
            for (int i = 0; i < batchesPerProducer; i++) {
              final boolean reserved = flush.reserveQueueSlot(db);
              assertThat(reserved).isTrue();
              flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(db, id * batchesPerProducer + i))), db);
            }
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          } finally {
            producersDone.countDown();
          }
        }, "issue6259-producer-" + t);
        producer.setDaemon(true);
        producers.add(producer);
        producer.start();
      }

      final Thread drainer = new Thread(() -> {
        try {
          while (producersDone.getCount() > 0 || !flush.queue.isEmpty())
            flush.flushPagesFromQueueToDisk(null, 5L);
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      }, "issue6259-drainer");
      drainer.setDaemon(true);
      drainer.start();

      assertThat(producersDone.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).as(
          "every producer must get through: a reservation that is never granted, or an enqueue that blocks behind one, hangs here")
          .isTrue();
      drainer.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
      for (final Thread producer : producers)
        producer.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));

      assertThat(failure.get()).isNull();
      assertThat(flush.queue).isEmpty();
      assertThat(flush.slotsUsedBy(db)).as(
          "the accounting must come back to zero: a slot leaked per round shrinks the pipeline until it stops admitting anything")
          .isZero();
      assertThat(flush.pageIndex.pendingOf(db)).as("and every page must have left the pipeline").isZero();
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

  /**
   * Regression test for issue #6281, item 3.
   * <p>
   * {@code arcadedb.pageFlushQueue} was one JVM-wide bound: the flush queue was an {@code ArrayBlockingQueue} of that
   * size shared by every database in the process. #6259 took the wait for room out of the page-manager lock, so a full
   * queue stopped freezing the whole JVM - but the coupling itself survived. One database's write burst against a slow
   * volume still consumed the admission budget of every other database, so a committer of an idle database on an idle
   * volume waited for a disk it has nothing to do with. It waited outside the lock rather than inside it, which is
   * strictly better and still not right.
   * <p>
   * The budget is now per database, and the shared queue carries no capacity of its own: it is an ordering structure,
   * and admission is the bound. A per-database QUEUE would have bounded the same thing while forcing the single flush
   * thread to choose between queues on every poll - a fairness policy, and a starvation risk, that a per-database
   * BUDGET over one FIFO queue simply does not have.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PerDatabaseFlushQueue6281 extends TestHelper {
    /** A file that does not exist, so a fabricated page can be driven through the pipeline without touching a real one. */
    private static final int  FILE_ID               = 4_100;
    private static final int  PAGE_SIZE             = 64;
    /**
     * Bounds only the waits expected to SUCCEED - the short windows asserting "not released yet" are written inline - so
     * a generous value cannot turn a passing run red, while a tight one can under the stop-the-world pauses the shared
     * 12000-test JVM produces late in a run (#6260).
     */
    private static final long ASSERTION_TIMEOUT_SEC = 60;

    /**
     * The issue itself: one database sitting at its bound must not cost another database's committer anything at all.
     * <p>
     * Before this, both committers were competing for the same {@code arcadedb.pageFlushQueue} slots, so B's admission
     * was gated on A's disk. The flush thread here is detached and never started, which is what a wedged disk looks
     * like from admission's point of view: nothing is ever polled, so nothing is ever given back.
     */
    @Test
    void aDatabaseAtItsBoundDoesNotHoldTheCommittersOfAnother() throws Exception {
      final DatabaseInternal busy = (DatabaseInternal) database;
      final Database idle = TestHelper.createDatabase("target/databases/" + getClass().getSimpleName() + "-idle");
      try {
        final PageManagerFlushThread flush = smallQueueFlushThread(2);

        // Database A exhausts its budget: two batches, nothing polling.
        flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(busy, 0))));
        flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(busy, 1))));
        assertThat(flush.slotsUsedBy(busy)).isEqualTo(2);

        // ...so A's next committer waits, which is what the budget is FOR.
        final CountDownLatch busyAdmitted = new CountDownLatch(1);
        final Thread busyCommitter = new Thread(() -> {
          try {
            if (flush.reserveQueueSlot(busy))
              busyAdmitted.countDown();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }, "issue6281-busy-committer");
        busyCommitter.setDaemon(true);
        busyCommitter.start();
        assertThat(busyAdmitted.await(500, TimeUnit.MILLISECONDS)).as(
            "the database at its bound must be held: that backpressure is the budget's job").isFalse();

        // THE ASSERTION THE WHOLE ITEM IS ABOUT: database B has published nothing, so it owes nothing and waits for
        // nothing - however full A's share of the pipeline is, and whatever A's disk is doing.
        final CountDownLatch idleAdmitted = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread idleCommitter = new Thread(() -> {
          try {
            if (flush.reserveQueueSlot((DatabaseInternal) idle)) {
              idleAdmitted.countDown();
              flush.releaseQueueReservation((DatabaseInternal) idle);
            }
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          }
        }, "issue6281-idle-committer");
        idleCommitter.setDaemon(true);
        idleCommitter.start();

        assertThat(idleAdmitted.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).as(
            "a committer of an idle database must not queue behind another database's backlog: one database's slow disk is not everybody's")
            .isTrue();
        idleCommitter.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
        assertThat(failure.get()).isNull();
        assertThat(flush.slotsUsedBy(idle)).isZero();

        // And A is still exactly where it was: admitting B took nothing from it.
        assertThat(busyAdmitted.getCount()).isEqualTo(1);
        assertThat(flush.slotsUsedBy(busy)).isEqualTo(2);

        busyCommitter.interrupt();
        busyCommitter.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
      } finally {
        idle.drop();
      }
    }

    /**
     * The queue is no longer what bounds the pipeline, so N databases at their bound hold N times the budget between
     * them - which is the whole point, and the property a shared {@code ArrayBlockingQueue} could not express.
     */
    @Test
    void theBoundIsPerDatabaseAndTheSharedQueueHasNoneOfItsOwn() throws Exception {
      final DatabaseInternal first = (DatabaseInternal) database;
      final Database second = TestHelper.createDatabase("target/databases/" + getClass().getSimpleName() + "-second");
      try {
        final int capacity = 3;
        final PageManagerFlushThread flush = smallQueueFlushThread(capacity);

        for (int i = 0; i < capacity; i++) {
          flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(first, i))));
          flush.scheduleFlushOfPages(new ArrayList<>(List.of(page((DatabaseInternal) second, i))));
        }

        assertThat(flush.slotsUsedBy(first)).isEqualTo(capacity);
        assertThat(flush.slotsUsedBy(second)).isEqualTo(capacity);
        assertThat(flush.queue).as("the shared queue holds the SUM of the per-database budgets").hasSize(2 * capacity);

        // Neither database can take one more, and neither is holding the other back.
        assertThat(flush.tryReserveQueueSlot(first)).isFalse();
        assertThat(flush.tryReserveQueueSlot(second)).isFalse();

        // The gauge an operator can still compare against arcadedb.pageFlushQueue. pageFlushQueueLength is the SUM
        // across databases now - 2 x capacity right here - which is why it needed a companion and not just a doc note.
        assertThat(flush.maxSlotsUsedByAnyDatabase()).as(
            "the busiest database's share is what the setting bounds, whatever the shared queue holds").isEqualTo(capacity);

        // A poll frees a slot of exactly ONE database: the one whose batch it was.
        flush.flushPagesFromQueueToDisk(null, 20L);
        assertThat(flush.slotsUsedBy(first)).as("the polled batch was the first database's").isEqualTo(capacity - 1);
        assertThat(flush.slotsUsedBy(second)).as("and its poll gave nothing back to the other").isEqualTo(capacity);
        assertThat(flush.maxSlotsUsedByAnyDatabase()).as("the busiest is now the database that lost nothing")
            .isEqualTo(capacity);
      } finally {
        second.drop();
      }
    }

    /**
     * A database that is closed or dropped takes its budget with it: the entry keying the dead instance must not be left
     * behind, and the batches of that database still in the queue must not drive its count negative when they are polled.
     */
    @Test
    void aDroppedDatabaseLeavesNoBudgetBehind() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManagerFlushThread flush = smallQueueFlushThread(4);

      flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(db, 0))));
      flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(db, 1))));
      assertThat(flush.slotsUsedBy(db)).isEqualTo(2);

      flush.removeAllPagesOfDatabase(db);
      assertThat(flush.slotsInUse).doesNotContainKey(db);

      // The emptied batches are still in the queue and are polled normally: with the budget gone there is nothing to
      // give back, and nothing that could go negative (the assertion inside would fail this test under -ea).
      flush.flushPagesFromQueueToDisk(null, 20L);
      flush.flushPagesFromQueueToDisk(null, 20L);
      assertThat(flush.queue).isEmpty();
      assertThat(flush.slotsUsedBy(db)).isZero();

      // And the database can be used again from scratch, its budget rebuilt on demand.
      flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(db, 2))));
      assertThat(flush.slotsUsedBy(db)).isEqualTo(1);
    }

    /**
     * A batch left over from a database that has since been closed must not be able to spend a DIFFERENT database's
     * budget - specifically not the budget of a database reopened at the same path.
     * <p>
     * {@code LocalDatabase} defines equality by database PATH, not by instance, so a stale batch's {@code database}
     * field resolves to whatever entry is keyed at that path now. Releasing through a map lookup would therefore
     * decrement the new instance's count for a slot it never took: an assertion failure here, and silent admission
     * drift with assertions off.
     * <p>
     * Two things close that, and this test pins the first: the emptied wrapper is taken OUT of the queue when its
     * database goes, so there is nothing left to poll. The second - the batch carrying the counter it was charged on
     * rather than looking one up - is defence in depth, and measured as such: with the queue removal in place this
     * test passes even with that capture reverted, because it can no longer reach the release. It is kept because the
     * removal has to be remembered at every future path that could leave a batch queued across a close, and the
     * capture does not.
     */
    @Test
    void aBatchOfAClosedDatabaseCannotSpendTheBudgetOfOneReopenedAtTheSamePath() throws Exception {
      final String path = "target/databases/" + getClass().getSimpleName() + "-recycled";
      final PageManagerFlushThread flush = smallQueueFlushThread(4);

      final Database first = TestHelper.createDatabase(path);
      try {
        flush.scheduleFlushOfPages(new ArrayList<>(List.of(page((DatabaseInternal) first, 0))));
        flush.scheduleFlushOfPages(new ArrayList<>(List.of(page((DatabaseInternal) first, 1))));
        assertThat(flush.slotsUsedBy(first)).isEqualTo(2);

        // The database goes away with its batches still in the pipeline.
        flush.removeAllPagesOfDatabase(first);
        assertThat(flush.queue).as("an emptied batch of a closed database has no business staying queued").isEmpty();
      } finally {
        first.drop();
      }

      // A new database at the SAME path: a different instance, an equal map key.
      final Database second = TestHelper.createDatabase(path);
      try {
        assertThat(second).isEqualTo(first);
        assertThat(second).isNotSameAs(first);

        flush.scheduleFlushOfPages(new ArrayList<>(List.of(page((DatabaseInternal) second, 0))));
        assertThat(flush.slotsUsedBy(second)).isEqualTo(1);

        // Drain everything the pipeline still holds. The new database must be charged for its own batch and nothing
        // else - a stale release landing here would take its count to -1 and trip the assert inside.
        while (!flush.queue.isEmpty())
          flush.flushPagesFromQueueToDisk(null, 20L);

        assertThat(flush.slotsUsedBy(second)).as("the reopened database pays for its own batches and no one else's")
            .isZero();
      } finally {
        second.drop();
      }
    }

    /**
     * A non-positive {@code arcadedb.pageFlushQueue} is raised to 1 rather than rejected.
     * <p>
     * It used to be rejected, by {@code ArrayBlockingQueue}'s constructor, and that was fine while the queue carried
     * the capacity. Now that admission is the only bound, a budget of 0 would refuse every publication for ever - an
     * unrecoverable hang in place of a startup failure - and there is no constructor left to reject it on the way in.
     */
    @Test
    void aNonPositiveBudgetIsRaisedToOneRatherThanRefusingEveryPublication() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      for (final int configured : new int[] { 0, -1 }) {
        final PageManagerFlushThread flush = smallQueueFlushThread(configured);
        assertThat(flush.getQueueCapacity()).as("a budget of %d must not stay one that admits nothing", configured)
            .isEqualTo(1);

        // And it genuinely admits: one batch in, the next caller held until that one is polled.
        flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(db, 0))));
        assertThat(flush.slotsUsedBy(db)).isEqualTo(1);
        assertThat(flush.tryReserveQueueSlot(db)).as("...and exactly one, so the second is refused").isFalse();

        flush.flushPagesFromQueueToDisk(null, 20L);
        assertThat(flush.slotsUsedBy(db)).isZero();
      }
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

  /**
   * Regression test for issue #6133.
   * <p>
   * "Does this database still have pages in the flush pipeline?" used to be answered by walking the JVM-WIDE flush index
   * and comparing every key's database, so the cost of the question scaled with the backlog of every OTHER open database
   * - and the question is polled every 10 ms for the whole duration of a drain by close, rename, index compaction,
   * backup suspension and the snapshot t0 barrier, the last of which polls it holding the JVM-wide page-manager lock.
   * The answer is now an O(1) per-database counter maintained inside {@link FlushPageIndex}.
   * <p>
   * A counter is only as good as its exactness, which is what this test is about: one that drifts HIGH hangs
   * {@code close()} forever on a pipeline that is in fact empty, and one that drifts LOW lets a backup stamp its t0 over
   * a half-written batch. Every route a page can take OUT of the pipeline is exercised below - flushed (with and without
   * a newer instance superseding it), explicitly removed by the replay detach, purged with its file, purged with its
   * database - and after each one the counter is compared against the brute-force scan it replaced.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PendingPagesPerDatabase6133 extends TestHelper {
    private static final int PAGE_SIZE = 1024;

    /**
     * Every mutation of the index keeps the per-database count in agreement with the map, and the two databases account
     * for their pages independently.
     */
    @Test
    void everyRouteOutOfThePipelineKeepsTheCountExact() {
      final DatabaseInternal db1 = (DatabaseInternal) database;
      final Database db2 = TestHelper.createDatabase("target/databases/" + getClass().getSimpleName() + "-sibling");
      try {
        final FlushPageIndex index = new FlushPageIndex();

        // db1: 5 pages on file 7, 3 on file 8. db2: 4 pages on file 7 - a same-numbered file of another database must
        // not be confused with db1's, which is the whole point of keying the count by database.
        final List<MutablePage> file7 = pages(db1, 7, 5);
        final List<MutablePage> file8 = pages(db1, 8, 3);
        final List<MutablePage> sibling = pages((DatabaseInternal) db2, 7, 4);
        index.putAll(file7);
        index.putAll(file8);
        index.putAll(sibling);
        assertCountsAgree(index, db1, 8);
        assertCountsAgree(index, db2, 4);

        // A later transaction supersedes a page still queued (the two-instance case of #4544): the index holds ONE
        // entry for that pageId, so the count must not grow.
        final MutablePage superseded = file7.getFirst();
        final MutablePage newer = new MutablePage(superseded.getPageId(), PAGE_SIZE, new byte[PAGE_SIZE], 1, 0);
        index.put(newer);
        assertCountsAgree(index, db1, 8);

        // The superseded copy reaching the disk must NOT release the entry - it still belongs to the newer instance,
        // which is still pending - nor its count.
        assertThat(index.removeIfSame(superseded)).as("a superseded copy must not evict the newer entry").isFalse();
        assertCountsAgree(index, db1, 8);

        // The indexed instance reaching the disk releases both.
        assertThat(index.removeIfSame(newer)).isTrue();
        assertCountsAgree(index, db1, 7);

        // Flushing a page that is not indexed at all (already superseded AND already dropped) releases nothing.
        assertThat(index.removeIfSame(newer)).isFalse();
        assertCountsAgree(index, db1, 7);

        // The replay detach takes a copy out by pageId.
        assertThat(index.remove(file8.getFirst().getPageId())).isSameAs(file8.getFirst());
        assertCountsAgree(index, db1, 6);
        assertThat(index.remove(file8.getFirst().getPageId())).as("a second detach of the same page finds nothing").isNull();
        assertCountsAgree(index, db1, 6);

        // A dropped file purges its pages, and only its own: db2's file 7 is a different file.
        index.removeAllOfFile(db1, 7);
        assertCountsAgree(index, db1, 2);
        assertCountsAgree(index, db2, 4);

        // A dropped/closed database purges the rest, and forgets its counter with them.
        index.removeAllOfDatabase(db1);
        assertCountsAgree(index, db1, 0);
        assertCountsAgree(index, db2, 4);
        assertThat(index.isEmpty()).isFalse();

        index.removeAllOfDatabase(db2);
        assertCountsAgree(index, db2, 0);
        assertThat(index.isEmpty()).as("the index is empty once both databases are gone").isTrue();
      } finally {
        db2.drop();
      }
    }

    /**
     * The count a drain polls is this database's own: a sibling database with a large backlog in the same JVM-wide
     * pipeline must neither be counted into it nor make the drain wait for it.
     */
    @Test
    void aSiblingBacklogIsNeitherCountedNorWaitedFor() throws Exception {
      final DatabaseInternal db1 = (DatabaseInternal) database;
      final Database db2 = TestHelper.createDatabase("target/databases/" + getClass().getSimpleName() + "-backlog");
      try {
        // Constructing the flush thread directly does NOT start the background thread (that happens in
        // PageManager.startup()), so the pipeline can be loaded up deterministically and never drains.
        final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, db1.getConfiguration());

        final int backlog = 50_000;
        flush.pageIndex.putAll(pages((DatabaseInternal) db2, 3, backlog));

        assertThat(flush.pageIndex.pendingOf(db2)).isEqualTo(backlog);
        assertThat(flush.hasPendingPagesOfDatabase(db2)).isTrue();
        assertThat(flush.pageIndex.pendingOf(db1)).as("a sibling's backlog is not this database's").isZero();
        assertThat(flush.hasPendingPagesOfDatabase(db1)).isFalse();

        // The drain of db1 returns immediately even though the pipeline is far from empty.
        assertThat(flush.waitAllPagesOfDatabaseAreFlushed(db1)).isTrue();
        assertThat(flush.waitPendingPagesOfDatabaseUntil(db1, System.currentTimeMillis() + 1_000)).isTrue();

        // One page of db1 in flight is enough for the same drain to report the pipeline busy.
        final MutablePage own = pages(db1, 3, 1).getFirst();
        flush.pageIndex.put(own);
        assertThat(flush.hasPendingPagesOfDatabase(db1)).isTrue();
        assertThat(flush.waitPendingPagesOfDatabaseUntil(db1, System.currentTimeMillis() + 50)).isFalse();

        flush.removeFromFlushIndex(own);
        assertThat(flush.hasPendingPagesOfDatabase(db1)).isFalse();

        flush.pageIndex.removeAllOfDatabase(db2);
        assertThat(flush.pageIndex.isEmpty()).isTrue();
      } finally {
        db2.drop();
      }
    }

    /**
     * The accounting survives real contention. Each thread owns a disjoint page range and drives it through the whole
     * life cycle - queued, superseded by a later transaction, flushed, detached - so any lost update in the counter
     * shows up as a disagreement with the map once the threads are joined and the index is quiescent again. It is
     * asserted quiescent on purpose: a comparison sampled WHILE the mutations run proves nothing either way, because
     * the two reads straddle other threads' mutations.
     */
    @Test
    void theCountSurvivesConcurrentMutation() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final FlushPageIndex index = new FlushPageIndex();

      final int threads = 8;
      final int perThread = 2_000;
      final CountDownLatch start = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread[] workers = new Thread[threads];

      for (int t = 0; t < threads; t++) {
        final int id = t;
        workers[t] = new Thread(() -> {
          try {
            start.await();
            for (int i = 0; i < perThread; i++) {
              final PageId pageId = new PageId(db, 100 + id, i);
              final MutablePage queued = new MutablePage(pageId, PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
              index.put(queued);
              // A later transaction supersedes it while it is still queued: one entry, still one page pending.
              final MutablePage newer = new MutablePage(pageId, PAGE_SIZE, new byte[PAGE_SIZE], 1, 0);
              index.put(newer);
              // The superseded copy reaches the disk first and must release nothing...
              index.removeIfSame(queued);
              // ...then the indexed one leaves, by the flush on even pages and by the replay detach on odd ones.
              if (i % 2 == 0)
                index.removeIfSame(newer);
              else
                index.remove(pageId);
            }
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          }
        }, "issue6133-mutator-" + t);
        workers[t].start();
      }

      start.countDown();
      for (final Thread worker : workers)
        worker.join(TimeUnit.MINUTES.toMillis(2));

      assertThat(failure.get()).isNull();
      assertCountsAgree(index, db, 0);
      assertThat(index.isEmpty()).isTrue();
    }

    /**
     * The end-to-end invariant on the real pipeline: after a burst of concurrent commits the count settles at exactly
     * zero, in agreement with the map. A count left ABOVE zero here is a close() that would never return; one left
     * BELOW is a backup free to stamp its t0 over a page still on its way to the disk.
     */
    @Test
    void theCountSettlesAtZeroAfterConcurrentCommits() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      db.getSchema().createDocumentType("Doc");

      final FlushPageIndex index = PageManager.INSTANCE.getFlushThread().pageIndex;

      final Thread[] writers = new Thread[4];
      for (int t = 0; t < writers.length; t++) {
        final int id = t;
        writers[t] = new Thread(() -> {
          for (int i = 0; i < 250; i++) {
            final int value = i;
            db.transaction(() -> db.newDocument("Doc").set("writer", id).set("v", value).save());
          }
        }, "issue6133-writer-" + t);
        writers[t].start();
      }
      for (final Thread writer : writers)
        writer.join(TimeUnit.MINUTES.toMillis(2));

      assertThat(PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();
      assertThat(index.pendingOf(db)).as("everything reached the disk, so nothing is pending").isZero();
      assertThat(index.scanPendingOf(db)).isZero();
    }

    /**
     * A plain, successful close must make the JVM-wide flush thread forget the database, not just drain it.
     * <p>
     * The purge that clears the per-database bookkeeping - the pending-page counter added here, and with it the
     * pre-existing suspend locks, deferred batches and flush-progress counter - used to run only when the close was a
     * DROP or when the flush wait gave up. On the common path nothing cleared them, so every closed database stayed
     * pinned as a map key for the life of {@code PageManager.INSTANCE}: one dead {@code LocalDatabase}, and everything
     * it references, per close, without bound.
     */
    @Test
    void aCleanCloseForgetsTheDatabase() {
      final String path = "target/databases/" + getClass().getSimpleName() + "-close";
      final PageManagerFlushThread flush = PageManager.INSTANCE.getFlushThread();

      final DatabaseInternal closed = (DatabaseInternal) TestHelper.createDatabase(path);
      try {
        closed.getSchema().createDocumentType("Doc");
        closed.transaction(() -> closed.newDocument("Doc").set("v", 1).save());
        // Both entries exist now: the counter from the commit, the progress counter from the wait below.
        assertThat(PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(closed)).isTrue();
        assertThat(flush.pageIndex.isTracked(closed)).isTrue();
        assertThat(flush.flushedPagesPerDatabase).containsKey(closed);

        closed.close();

        assertThat(flush.pageIndex.isTracked(closed))
            .as("a clean close must release the pending-page counter, not pin the closed database as its key").isFalse();
        assertThat(flush.flushedPagesPerDatabase)
            .as("and the flush-progress counter with it").doesNotContainKey(closed);
        assertThat(flush.pageIndex.scanPendingOf(closed)).isZero();
      } finally {
        if (closed.isOpen())
          closed.close();
        new DatabaseFactory(path).open().drop();
      }
    }

    private static List<MutablePage> pages(final DatabaseInternal database, final int fileId, final int total) {
      final List<MutablePage> pages = new ArrayList<>(total);
      for (int i = 0; i < total; i++) {
        final PageId pageId = new PageId(database, fileId, 1_000_000 + i);
        pages.add(new MutablePage(pageId, PAGE_SIZE, new byte[PAGE_SIZE], 0, 0));
      }
      return pages;
    }

    private static void assertCountsAgree(final FlushPageIndex index, final Database database, final int expected) {
      assertThat(index.pendingOf(database)).as("pending count of '%s'", database.getName()).isEqualTo(expected);
      assertThat(index.scanPendingOf(database)).as("scan of the index for '%s'", database.getName()).isEqualTo(expected);
    }

  }

  /**
   * Issue #6142: the creates of a recording session are carried forward as a consumable queue, not re-derived from the
   * cumulative log on every read.
   * <p>
   * The HA schema-instalment producer asks "what was created since my last instalment?" once per instalment. It used to
   * answer by walking the whole of {@code getRecordedChanges()} and filtering out what it had already shipped, which is
   * O(instalments x file changes): harmless for an index rebuild, which records one or two file changes however many
   * instalments its WAL volume produces, and quadratic for a DDL that creates many files through the same buffered
   * path. An index into the cumulative list is NOT the alternative - {@code dropFile} removes the cancelled create from
   * the middle of it - so the split is carried forward instead.
   * <p>
   * What these tests pin is that the queue tells the same story as the cumulative log about every file id, because the
   * consumer of one is the compensation for the other: a create the queue forgets is a file the followers are told to
   * create and nobody ever retires.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RecordedCreatesDrain6142 {
    private static final Set<String> FILE_EXT = Set.of(LocalBucket.BUCKET_EXT);

    @Test
    void aDrainHandsOverOnlyWhatWasCreatedSinceThePreviousOne(@TempDir final Path dir) throws Exception {
      final FileManager fileManager = newFileManager(dir);
      assertThat(fileManager.startRecordingChanges()).isTrue();
      try {
        createFile(fileManager, dir, 0, "first");
        createFile(fileManager, dir, 1, "second");

        assertThat(fileManager.drainRecordedCreates())
            .as("the first instalment must announce both files, in creation order")
            .containsExactly(Map.entry(0, "first.0.65536.v1.bucket"), Map.entry(1, "second.1.65536.v1.bucket"));

        assertThat(fileManager.drainRecordedCreates())
            .as("nothing was created since, so the next instalment announces nothing - the whole point of draining "
                + "rather than re-reading the cumulative list").isEmpty();

        createFile(fileManager, dir, 2, "third");

        assertThat(fileManager.drainRecordedCreates())
            .as("only what appeared since the previous drain: re-announcing an already shipped file would make the "
                + "follower create over a file that is being written into")
            .containsExactly(Map.entry(2, "third.2.65536.v1.bucket"));

        assertThat(fileManager.getRecordedChanges())
            .as("draining must not consume the CUMULATIVE log: the session's final entry is built from it").hasSize(3);
      } finally {
        fileManager.stopRecordingChanges();
        fileManager.close();
      }
    }

    /**
     * The drop-cancels-create rule, which the cumulative log has always had and the queue has to mirror. A session that
     * creates a file and drops it again says nothing about it in its final entry, so a queue that still offered the
     * create would have an instalment announce a file that this node no longer has and no later entry mentions.
     */
    @Test
    void aCreateCancelledByADropIsNeverHandedOver(@TempDir final Path dir) throws Exception {
      final FileManager fileManager = newFileManager(dir);
      assertThat(fileManager.startRecordingChanges()).isTrue();
      try {
        createFile(fileManager, dir, 0, "kept");
        createFile(fileManager, dir, 1, "dropped");
        fileManager.dropFile(1);

        assertThat(fileManager.drainRecordedCreates())
            .as("the drop cancelled the create, exactly as it does in the cumulative log")
            .containsExactly(Map.entry(0, "kept.0.65536.v1.bucket"));
        assertThat(fileManager.getRecordedChanges()).hasSize(1);
      } finally {
        fileManager.stopRecordingChanges();
        fileManager.close();
      }
    }

    /**
     * A file dropped AFTER its create was drained is not the queue's business, and must not resurface as a create: the
     * instalment already announced it, so retiring it is the job of the shipper's own bookkeeping (which is what
     * {@code RaftReplicatedDatabase.reconcileInstalmentFiles} does with it).
     */
    @Test
    void aDropAfterTheDrainDoesNotReviveTheCreate(@TempDir final Path dir) throws Exception {
      final FileManager fileManager = newFileManager(dir);
      assertThat(fileManager.startRecordingChanges()).isTrue();
      try {
        createFile(fileManager, dir, 0, "shipped");
        assertThat(fileManager.drainRecordedCreates()).containsExactly(Map.entry(0, "shipped.0.65536.v1.bucket"));

        fileManager.dropFile(0);

        assertThat(fileManager.drainRecordedCreates()).as("a drop is not a create and never enters the queue").isEmpty();
      } finally {
        fileManager.stopRecordingChanges();
        fileManager.close();
      }
    }

    /**
     * Issue #4083 on the incremental path: {@code PaginatedComponent.removeTempSuffix} renames a file AFTER its create
     * was recorded, and an instalment announcing the pre-rename name would have the follower create the file under a
     * name the schema JSON does not use.
     */
    @Test
    void aRenameReachesTheQueueToo(@TempDir final Path dir) throws Exception {
      final FileManager fileManager = newFileManager(dir);
      assertThat(fileManager.startRecordingChanges()).isTrue();
      try {
        final ComponentFile file = createFile(fileManager, dir, 0, "temp_renamed");
        ((PaginatedComponentFile) file).rename("renamed.0.65536.v1." + LocalBucket.BUCKET_EXT);
        fileManager.refreshRecordedFileName(file);

        assertThat(fileManager.drainRecordedCreates())
            .as("the queue must carry the current name, like the cumulative log does")
            .containsExactly(Map.entry(0, "renamed.0.65536.v1.bucket"));
      } finally {
        fileManager.stopRecordingChanges();
        fileManager.close();
      }
    }

    /** Outside a session there is nothing to collect into, and the drain must say so rather than fail. */
    @Test
    void drainingWithNoSessionOpenIsEmpty(@TempDir final Path dir) {
      final FileManager fileManager = newFileManager(dir);
      try {
        assertThat(fileManager.drainRecordedCreates()).isEmpty();
      } finally {
        fileManager.close();
      }
    }

    // ---------------------------------------------------------------------------------------------------------------

    private static FileManager newFileManager(final Path dir) {
      return new FileManager(dir.toFile().getAbsolutePath(), ComponentFile.MODE.READ_WRITE, FILE_EXT);
    }

    private static ComponentFile createFile(final FileManager fileManager, final Path dir, final int fileId,
        final String name) throws Exception {
      return fileManager.getOrCreateFile(fileId,
          dir.resolve(name + "." + fileId + ".65536.v1." + LocalBucket.BUCKET_EXT).toFile().getAbsolutePath());
    }
  }

  /**
   * Regression test for issue #6199.
   * <p>
   * Both drains of the flush pipeline used to discover that it had emptied by SLEEPING and asking again, so the latency
   * they paid was set by the poll interval rather than by when the pages actually landed: 10 ms for the bulk drain that
   * every close, rename, index compaction and backup suspension goes through, and 1 ms for the snapshot t0 barrier's
   * residual drain - which polls with BOTH the database's apply write lock and the JVM-wide page-manager lock held, so
   * each of those sleeps was a millisecond in which no committer of any database in the process could publish a page.
   * <p>
   * Since #6133 there is exactly one place a database's pending count goes down ({@code FlushPageIndex.release}), which
   * is what makes a "this database just drained" signal possible at all, and the waits now park on it.
   * <p>
   * <b>How these tests can tell a notification from a poll</b>, given that both end the wait: the fallback interval is
   * stretched to ten minutes. A wait that returns in well under that was released by the signal; one released by the
   * interval would blow the assertion's own timeout. The fallback is deliberately kept (a bounded {@code wait}, not an
   * unbounded park) so the callers' timeout machinery still gets re-evaluated and a hypothetical lost notification
   * degrades to the polling this used to be - which is why it has to be stretched here to be excluded.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DrainWakeup6199 extends TestHelper {
    private static final int  PAGE_SIZE             = 1024;
    /** Far longer than any assertion below waits: a wait that ends inside the timeouts was NOT ended by this. */
    private static final long LONG_FALLBACK_MILLIS  = TimeUnit.MINUTES.toMillis(10);
    /**
     * Generous on purpose, and it costs nothing: it bounds only the waits that are expected to SUCCEED, so a longer
     * bound cannot turn a passing run red - while a short one can, as this class already learned when a 15 s bound
     * met a 24 s stop-the-world pause in the shared 12000-test JVM. What the tests actually rest on is the gap
     * between this and the fallback above, which stays an order of magnitude wide.
     */
    private static final long ASSERTION_TIMEOUT_SEC = 60;

    /**
     * The last page leaving the pipeline releases a waiter immediately, instead of at the next poll boundary.
     */
    @Test
    void theLastPageLeavingThePipelineReleasesTheWaiter() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final FlushPageIndex index = new FlushPageIndex();

      final MutablePage first = page(db, 3, 0);
      final MutablePage last = page(db, 3, 1);
      index.put(first);
      index.put(last);

      final CountDownLatch started = new CountDownLatch(1);
      final CountDownLatch returned = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread waiter = new Thread(() -> {
        try {
          started.countDown();
          while (index.hasPendingOf(db))
            index.awaitDrain(db, LONG_FALLBACK_MILLIS);
          returned.countDown();
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      }, "issue6199-waiter");
      waiter.start();

      assertThat(started.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).isTrue();
      // Long enough that the waiter is parked rather than still on its way there; the racing case is covered below.
      Thread.sleep(100);

      // The first page is not the last one: the pipeline is not empty yet, so no wake-up may end the wait.
      assertThat(index.removeIfSame(first)).isTrue();
      assertThat(returned.await(200, TimeUnit.MILLISECONDS)).as("the wait must not end while a page is still pending")
          .isFalse();

      assertThat(index.removeIfSame(last)).isTrue();
      assertThat(returned.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).as(
          "the drain must be released by the last page landing, not by the fallback interval").isTrue();

      waiter.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
      assertThat(failure.get()).isNull();
    }

    /**
     * The notification cannot be missed between the count reaching zero and the waiter parking: the guard is re-read
     * with the monitor already held, so a release that lands in that window is observed instead of slept through.
     * Run repeatedly with the release racing the park, since the losing interleaving is the one that hangs.
     */
    @Test
    void aReleaseRacingTheParkIsNeverMissed() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final FlushPageIndex index = new FlushPageIndex();

      for (int i = 0; i < 300; i++) {
        final MutablePage page = page(db, 4, i);
        index.put(page);

        final CountDownLatch returned = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread waiter = new Thread(() -> {
          try {
            while (index.hasPendingOf(db))
              index.awaitDrain(db, LONG_FALLBACK_MILLIS);
            returned.countDown();
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          }
        }, "issue6199-racer-" + i);
        waiter.start();

        // NO handshake on purpose: the release lands wherever it lands - before the guard read, between it and the
        // park, or after - and every one of those must end the wait.
        assertThat(index.removeIfSame(page)).isTrue();

        assertThat(returned.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).as(
            "iteration %d: a release racing the park must not be slept through", i).isTrue();
        waiter.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
        assertThat(failure.get()).isNull();
      }

      assertThat(index.isEmpty()).isTrue();
    }

    /**
     * A database purged from the index (close or drop) releases its waiters too: their counter is gone, so nothing
     * else would ever signal them and they would sleep out the whole fallback interval for a database that no longer
     * has pages at all.
     */
    @Test
    void purgingTheDatabaseReleasesItsWaiters() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final FlushPageIndex index = new FlushPageIndex();
      index.put(page(db, 5, 0));

      final CountDownLatch started = new CountDownLatch(1);
      final CountDownLatch returned = new CountDownLatch(1);
      final Thread waiter = new Thread(() -> {
        try {
          started.countDown();
          while (index.hasPendingOf(db))
            index.awaitDrain(db, LONG_FALLBACK_MILLIS);
          returned.countDown();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }, "issue6199-purge-waiter");
      waiter.start();

      assertThat(started.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).isTrue();
      Thread.sleep(100);

      index.removeAllOfDatabase(db);

      assertThat(returned.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).as(
          "a purged database must release the waiters parked on its counter").isTrue();
      waiter.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
    }

    /**
     * The same purge, racing the park instead of following it.
     * <p>
     * The purge drops the map entry and then signals ONCE, so a waiter that resolved the counter just before the
     * removal and enters the monitor just after the signal has to find the drained state on the counter OBJECT: the
     * map entry it would otherwise consult is already gone. If the purge only notified without zeroing it, that
     * waiter would re-read a stale positive count and park through a signal it can never be sent again.
     * <p>
     * <b>A probabilistic guard, and worth being exact about how probabilistic.</b> The window is a handful of
     * instructions, so it is reached by alignment and repetition rather than forced: the waiters and the purge are
     * released from one gate. Measured against a purge that notifies WITHOUT zeroing the counter, it reproduced the
     * miss in 1 run out of 12 on a loaded machine and 2 out of 6 on an idle one - samples that small do not
     * distinguish the two, so rely on the lower figure. A single UNALIGNED waiter per round reproduced NOTHING in
     * three runs, which is why the gate is here at all.
     * <p>
     * The round is deliberately no larger (12 waiters, 80 rounds) although a larger one reproduces more often: this
     * runs in the shared engine-suite JVM where thread churn is a cost every other test pays, and the guarantee rests
     * on the argument written into {@code FlushPageIndex.removeAllOfDatabase} rather than on this test catching a
     * regression of it every time.
     */
    @Test
    void aPurgeRacingTheParkIsNeverMissed() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int waitersPerRound = 12;

      for (int round = 0; round < 80; round++) {
        final FlushPageIndex index = new FlushPageIndex();
        index.put(page(db, 8, round));

        // One gate for the waiters AND the purge, so they are released within microseconds of each other: the window
        // is a handful of instructions wide, and nothing but alignment plus repetition can land inside it.
        final CountDownLatch gate = new CountDownLatch(1);
        final CountDownLatch returned = new CountDownLatch(waitersPerRound);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread[] waiters = new Thread[waitersPerRound];
        for (int w = 0; w < waitersPerRound; w++) {
          waiters[w] = new Thread(() -> {
            try {
              gate.await();
              while (index.hasPendingOf(db))
                index.awaitDrain(db, LONG_FALLBACK_MILLIS);
              returned.countDown();
            } catch (final Throwable e) {
              failure.compareAndSet(null, e);
            }
          }, "issue6199-purge-racer-" + round + "-" + w);
          waiters[w].start();
        }

        gate.countDown();
        index.removeAllOfDatabase(db);

        assertThat(returned.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).as(
            "round %d: a purge racing the park must not leave a waiter asleep on a signal that already fired", round)
            .isTrue();
        for (final Thread waiter : waiters)
          waiter.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
        assertThat(failure.get()).isNull();
      }
    }

    /**
     * The bulk drain - the one every close, rename, index compaction and backup suspension goes through - is released
     * by the page landing and not by its own interval, which is stretched to a minute here to make the two
     * distinguishable.
     */
    @Test
    void theBulkDrainIsReleasedByTheFlushAndNotByItsInterval() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      // Constructing the flush thread directly does NOT start the background thread, so the pipeline only moves when
      // this test moves it.
      final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, db.getConfiguration());
      flush.flushWaitPollMillis = LONG_FALLBACK_MILLIS;

      final MutablePage page = page(db, 6, 0);
      flush.pageIndex.put(page);

      final CountDownLatch started = new CountDownLatch(1);
      final AtomicBoolean drained = new AtomicBoolean();
      final CountDownLatch returned = new CountDownLatch(1);
      final Thread waiter = new Thread(() -> {
        started.countDown();
        drained.set(flush.waitAllPagesOfDatabaseAreFlushed(db));
        returned.countDown();
      }, "issue6199-bulk-drain");
      waiter.start();

      assertThat(started.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).isTrue();
      Thread.sleep(100);
      assertThat(returned.getCount()).as("the drain must still be waiting while its page is pending").isEqualTo(1);

      flush.removeFromFlushIndex(page);

      assertThat(returned.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).as(
          "the bulk drain must wake on the flush, not %d ms later", LONG_FALLBACK_MILLIS).isTrue();
      assertThat(drained).isTrue();
      waiter.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
    }

    /**
     * The residual drain of the snapshot t0 barrier - the one that runs under the JVM-wide page-manager lock - keeps
     * its hard deadline: a page that never lands must not hold that lock past it, and an already-expired deadline must
     * not park at all (a {@code wait(0)} would park forever, the shape this rewrite has to avoid).
     */
    @Test
    void theResidualDrainKeepsItsHardDeadline() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, db.getConfiguration());

      final MutablePage page = page(db, 7, 0);
      flush.pageIndex.put(page);

      final long begin = System.currentTimeMillis();
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThat(flush.waitPendingPagesOfDatabaseUntil(db, begin + 200)).as("a page that never lands expires the deadline")
          .isFalse();
      // A TRIPWIRE, NOT A LATENCY ASSERTION, and its bound says so. What it can catch is a wait that came back only
      // because something else eventually ended it; what it must NOT do is measure how promptly a 200 ms deadline
      // expires, because this runs in the shared engine-suite JVM after ~12000 other tests, where a stop-the-world
      // pause of tens of seconds is not exotic - a 15 s bound here failed on exactly that, at 24 s, with the code
      // behaving correctly. The deadline being honoured is asserted above, by the return value.
      //
      // #6260: the stopwatch discounts that pause, so the tripwire no longer has to be widened out to a minute to
      // survive it. 5 s is still far above the honest ~200 ms and far below the forever an unbounded wait takes.
      stopwatch.assertGaveUpWithin(5_000L, "a wait bounded by its 200ms deadline from one that never ends on its own");

      assertThat(flush.waitPendingPagesOfDatabaseUntil(db, System.currentTimeMillis())).as(
          "an expired deadline must return at once instead of parking").isFalse();

      // And it returns as soon as the page does land.
      final CountDownLatch returned = new CountDownLatch(1);
      final AtomicBoolean settled = new AtomicBoolean();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread waiter = new Thread(() -> {
        try {
          settled.set(flush.waitPendingPagesOfDatabaseUntil(db, System.currentTimeMillis() + LONG_FALLBACK_MILLIS));
          returned.countDown();
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      }, "issue6199-residual-drain");
      waiter.start();

      Thread.sleep(100);
      flush.removeFromFlushIndex(page);

      assertThat(returned.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).isTrue();
      assertThat(settled).isTrue();
      waiter.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
      assertThat(failure.get()).isNull();
    }

    private static MutablePage page(final DatabaseInternal database, final int fileId, final int pageNumber) {
      return new MutablePage(new PageId(database, fileId, pageNumber), PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
    }
  }

  /**
   * Regression test for issue #6200.
   * <p>
   * The deferred-flush backpressure of #4728 bounds a JVM-wide resource (heap) and was therefore right to use a JVM-wide
   * ceiling - but its RESPONSE to crossing that ceiling was JVM-wide too: the flush thread stopped draining its bounded
   * queue altogether, so the queue filled and the committing threads of EVERY open database were throttled, not just
   * those of the suspended database whose backlog it was. A leader shipping a multi-GB snapshot of database A stalled
   * the committers of unrelated databases B and C, whose pages could have gone straight to disk - which would have
   * RELIEVED the heap the cap exists to protect, since only the batches of suspended databases are ever deferred.
   * <p>
   * The cap is now served on the committer side, before the JVM-wide page-manager lock is taken, and only for the
   * databases that are actually suspended. The flush thread always drains.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PerDatabaseDeferredBacklog6200 extends TestHelper {
    private static final int  PAGE_SIZE             = 256 * 1024;   // 256 KB per page
    private static final int  CAP_MB                = 1;            // 1 MB deferred cap -> 4 pages fit exactly
    private static final long CAP_BYTES             = (long) CAP_MB * 1024 * 1024;
    private static final int  FILE_ID               = 9;
    /**
     * Bounds only the waits expected to SUCCEED - the short windows asserting "not released yet" are written
     * inline - so a generous value cannot turn a passing run red, while a tight one can under the stop-the-world
     * pauses the shared 12000-test JVM produces late in a run.
     */
    private static final long ASSERTION_TIMEOUT_SEC = 60;

    /**
     * The heart of the issue: while one suspended database sits over the cap, the batches of the OTHER databases must
     * still be polled and written. Before the fix the flush thread returned without polling at all, so a batch queued
     * behind the over-cap database's backlog was never reached, whatever database it belonged to.
     */
    @Test
    void aDatabaseOverTheCapDoesNotStopTheDrainForTheOthers() throws Exception {
      final DatabaseInternal suspendedDb = (DatabaseInternal) database;
      final Database otherDb = TestHelper.createDatabase("target/databases/" + getClass().getSimpleName() + "-other");
      try {
        final PageManagerFlushThread flush = cappedFlushThread();
        flush.setSuspended(suspendedDb, true);
        try {
          // Six batches of the suspended database - twice what fits under the cap - and one batch of a database that
          // is NOT suspended, queued behind all of them.
          for (int i = 0; i < 6; i++)
            enqueue(flush, suspendedDb, i);
          final MutablePage otherPage = enqueue(flush, (DatabaseInternal) otherDb, 0);

          for (int i = 0; i < 7; i++)
            flush.flushPagesFromQueueToDisk(null, 20L);

          assertThat(flush.queue).as("the flush thread must keep draining while a suspended database is over the cap")
              .isEmpty();
          assertThat(flush.pageIndex.pendingOf(otherDb)).as(
              "the batch of the database that is not suspended must have been flushed, not left behind the backlog")
              .isZero();
          assertThat(flush.pageIndex.get(otherPage.getPageId())).isNull();

          // The suspended database's own batches are all deferred, and all charged to IT.
          assertThat(flush.pageIndex.pendingOf(suspendedDb)).isEqualTo(6);
          assertThat(flush.getDeferredRAMBytesOf(suspendedDb)).isEqualTo(6L * PAGE_SIZE);
          assertThat(flush.getDeferredRAMBytesOf(otherDb)).as("a database that is not suspended defers nothing").isZero();
          assertThat(flush.deferredRAMBytes.get()).isEqualTo(6L * PAGE_SIZE);
        } finally {
          flush.setSuspended(suspendedDb, false);
        }
      } finally {
        otherDb.drop();
      }
    }

    /**
     * The cap still throttles the committers of the database it applies to - that is #4728's whole point, and the
     * bound on the backlog now rests on it entirely - and releases them when the backlog is written out.
     */
    @Test
    void theSuspendedDatabaseOwnCommittersAreHeldUntilTheBacklogDrains() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManagerFlushThread flush = cappedFlushThread();
      flush.setSuspended(db, true);

      // Defer exactly the cap: 4 pages of 256 KB.
      for (int i = 0; i < 4; i++)
        enqueue(flush, db, i);
      for (int i = 0; i < 4; i++)
        flush.flushPagesFromQueueToDisk(null, 20L);
      assertThat(flush.deferredRAMBytes.get()).isEqualTo(CAP_BYTES);

      final CountDownLatch released = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread committer = new Thread(() -> {
        try {
          flush.awaitDeferredBacklogUnderCap(db);
          released.countDown();
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      }, "issue6200-committer");
      committer.start();

      assertThat(released.await(500, TimeUnit.MILLISECONDS)).as(
          "a committer of the suspended database must be held while its backlog is at the cap").isFalse();

      // The suspension ends: the deferred pages are written out and the committer is released.
      flush.setSuspended(db, false);

      assertThat(released.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).as(
          "the committer must be released once the backlog is written out").isTrue();
      committer.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
      assertThat(failure.get()).isNull();
      assertThat(flush.deferredRAMBytes.get()).isZero();
      assertThat(flush.getDeferredRAMBytesOf(db)).isZero();
    }

    /**
     * The other half of the same rule: a database that is NOT suspended is never held by the cap, however far over it
     * another database's backlog is. Its pages go straight to the disk, so committing them can only relieve the heap.
     */
    @Test
    void aDatabaseThatIsNotSuspendedIsNeverHeldByAnotherDatabaseBacklog() throws Exception {
      final DatabaseInternal suspendedDb = (DatabaseInternal) database;
      final Database otherDb = TestHelper.createDatabase("target/databases/" + getClass().getSimpleName() + "-free");
      try {
        final PageManagerFlushThread flush = cappedFlushThread();
        flush.setSuspended(suspendedDb, true);
        try {
          for (int i = 0; i < 8; i++)
            enqueue(flush, suspendedDb, i);
          for (int i = 0; i < 8; i++)
            flush.flushPagesFromQueueToDisk(null, 20L);
          assertThat(flush.deferredRAMBytes.get()).as("the backlog is far over the cap").isGreaterThan(CAP_BYTES);

          final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
          flush.awaitDeferredBacklogUnderCap(otherDb);
          stopwatch.assertStayedUnder(1_000,
              "a free database returning at once, not waiting on another database's over-cap backlog");
        } finally {
          flush.setSuspended(suspendedDb, false);
        }
      } finally {
        otherDb.drop();
      }
    }

    /**
     * The backlog is accounted per database and not only as one JVM-wide total, which is what makes it possible to say
     * WHICH suspension is holding the heap - and what the per-database {@code deferred_ram_bytes} gauge of item 2 of
     * #6087 needs. The total stays the exact sum of the parts.
     */
    @Test
    void theBacklogIsAccountedPerDatabaseAndSumsToTheTotal() throws Exception {
      final DatabaseInternal db1 = (DatabaseInternal) database;
      final Database db2 = TestHelper.createDatabase("target/databases/" + getClass().getSimpleName() + "-split");
      try {
        final PageManagerFlushThread flush = cappedFlushThread();
        flush.setSuspended(db1, true);
        flush.setSuspended(db2, true);
        try {
          for (int i = 0; i < 3; i++)
            enqueue(flush, db1, i);
          for (int i = 0; i < 2; i++)
            enqueue(flush, (DatabaseInternal) db2, i);
          for (int i = 0; i < 5; i++)
            flush.flushPagesFromQueueToDisk(null, 20L);

          assertThat(flush.getDeferredRAMBytesOf(db1)).isEqualTo(3L * PAGE_SIZE);
          assertThat(flush.getDeferredRAMBytesOf(db2)).isEqualTo(2L * PAGE_SIZE);
          assertThat(flush.deferredRAMBytes.get()).as("the total is the sum of the per-database parts")
              .isEqualTo(flush.getDeferredRAMBytesOf(db1) + flush.getDeferredRAMBytesOf(db2));
        } finally {
          flush.setSuspended(db1, false);
          flush.setSuspended(db2, false);
        }

        // Both suspensions released and both backlogs written: the accounting returns to zero on every axis.
        assertThat(flush.deferredRAMBytes.get()).isZero();
        assertThat(flush.getDeferredRAMBytesOf(db1)).isZero();
        assertThat(flush.getDeferredRAMBytesOf(db2)).isZero();
      } finally {
        db2.drop();
      }
    }

    /**
     * One database's backlog ending must not touch another's. The reset this replaced zeroed the total AND the whole
     * per-database split whenever it observed {@code deferredByDatabase} empty - a check made on the resuming thread
     * while the flush thread could be deferring the FIRST batch of an unrelated database, whose fresh charge was then
     * wiped, defeating the cap for it until its next mutation (review of #6200).
     */
    @Test
    void oneDatabaseResumingDoesNotWipeAnotherDatabaseBacklog() throws Exception {
      final DatabaseInternal resumingDb = (DatabaseInternal) database;
      final Database otherDb = TestHelper.createDatabase("target/databases/" + getClass().getSimpleName() + "-survivor");
      try {
        final PageManagerFlushThread flush = cappedFlushThread();
        flush.setSuspended(resumingDb, true);
        flush.setSuspended(otherDb, true);
        try {
          // Both databases defer a batch, then only the first one resumes.
          enqueue(flush, resumingDb, 0);
          enqueue(flush, (DatabaseInternal) otherDb, 0);
          flush.flushPagesFromQueueToDisk(null, 20L);
          flush.flushPagesFromQueueToDisk(null, 20L);
          assertThat(flush.deferredRAMBytes.get()).isEqualTo(2L * PAGE_SIZE);

          flush.setSuspended(resumingDb, false);

          assertThat(flush.getDeferredRAMBytesOf(resumingDb)).as("the resumed database wrote its backlog out").isZero();
          assertThat(flush.getDeferredRAMBytesOf(otherDb)).as(
              "a sibling's resume must not wipe the backlog of a database that is still suspended").isEqualTo(PAGE_SIZE);
          assertThat(flush.deferredRAMBytes.get()).as("and the total must still hold the sibling's share")
              .isEqualTo((long) PAGE_SIZE);
        } finally {
          flush.setSuspended(otherDb, false);
        }
        assertThat(flush.deferredRAMBytes.get()).isZero();
      } finally {
        otherDb.drop();
      }
    }

    /**
     * Shutdown releases a committer that is still parked on the cap. Its database may well be left suspended - the
     * backlog is only relieved by a resume that {@code closeAndJoin} is not going to wait for - so without the signal
     * there the committer would sit out its fallback interval against a condition that can never become true.
     */
    @Test
    void shutdownReleasesACommitterParkedOnAStillSuspendedDatabase() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManagerFlushThread flush = cappedFlushThread();
      flush.setSuspended(db, true);
      try {
        for (int i = 0; i < 4; i++)
          enqueue(flush, db, i);
        for (int i = 0; i < 4; i++)
          flush.flushPagesFromQueueToDisk(null, 20L);
        assertThat(flush.deferredRAMBytes.get()).isGreaterThanOrEqualTo(CAP_BYTES);

        final CountDownLatch released = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread committer = new Thread(() -> {
          try {
            flush.awaitDeferredBacklogUnderCap(db);
            released.countDown();
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          }
        }, "issue6200-shutdown-committer");
        committer.start();

        assertThat(released.await(300, TimeUnit.MILLISECONDS)).as("held while the backlog is at the cap").isFalse();

        // The thread was never started, so closeAndJoin's join() returns at once: what is under test is the signal.
        flush.closeAndJoin();

        assertThat(released.await(ASSERTION_TIMEOUT_SEC, TimeUnit.SECONDS)).as(
            "shutdown must release a committer parked on a database that is still suspended").isTrue();
        committer.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
        assertThat(failure.get()).isNull();
      } finally {
        flush.setSuspended(db, false);
      }
    }

    /**
     * Purging a database that is STILL suspended takes its whole backlog with it - the deferred queue, its share of
     * the total, and the suspension flag - and leaves the databases that are still suspended untouched.
     * <p>
     * Nothing ever resumes a database that is gone, so anything left behind here is permanent: a queue pinning the
     * closed instance as a map key with its pages in RAM, and a charge on the JVM-wide total that would throttle the
     * committers of every LATER suspension in the process.
     */
    @Test
    void purgingASuspendedDatabaseTakesItsWholeBacklogWithIt() throws Exception {
      final DatabaseInternal purgedDb = (DatabaseInternal) database;
      final Database survivingDb = TestHelper.createDatabase("target/databases/" + getClass().getSimpleName() + "-purge");
      try {
        final PageManagerFlushThread flush = cappedFlushThread();
        flush.setSuspended(purgedDb, true);
        flush.setSuspended(survivingDb, true);
        try {
          enqueue(flush, purgedDb, 0);
          enqueue(flush, (DatabaseInternal) survivingDb, 0);
          flush.flushPagesFromQueueToDisk(null, 20L);
          flush.flushPagesFromQueueToDisk(null, 20L);
          assertThat(flush.deferredRAMBytes.get()).isEqualTo(2L * PAGE_SIZE);

          flush.removeAllPagesOfDatabase(purgedDb);

          assertThat(flush.hasDeferredBatches(purgedDb)).as("no deferred queue may outlive the database").isFalse();
          assertThat(flush.getDeferredRAMBytesOf(purgedDb)).isZero();
          assertThat(flush.isSuspended(purgedDb)).as("a database that is gone cannot be suspended").isFalse();

          assertThat(flush.getDeferredRAMBytesOf(survivingDb)).as("and the sibling keeps its own backlog")
              .isEqualTo(PAGE_SIZE);
          assertThat(flush.deferredRAMBytes.get()).isEqualTo((long) PAGE_SIZE);
        } finally {
          flush.setSuspended(survivingDb, false);
        }
      } finally {
        survivingDb.drop();
      }
    }

    /**
     * The same purge, racing the flush thread's poll of one of that database's batches.
     * <p>
     * The defer is decided under the database's suspend monitor, so the purge has to drop the suspension flag and
     * detach the deferred queue under that SAME monitor: otherwise the flush thread can read "suspended" just before
     * the purge and offer its batch just after it, re-creating a queue for a database the purge already forgot.
     * <p>
     * The interleaving is a few instructions wide, so this stresses it rather than forcing it: measured against the
     * unguarded version it fails about one run in three, around iteration 100. What it pins deterministically is the
     * invariant either order must leave - no queue, no charge - which is also what a future refactor would break.
     */
    @Test
    void aBatchPolledWhileTheDatabaseIsPurgedIsNeverLeftDeferredForIt() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManagerFlushThread flush = cappedFlushThread();

      for (int i = 0; i < 200; i++) {
        flush.setSuspended(db, true);
        enqueue(flush, db, i);

        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread poller = new Thread(() -> {
          try {
            flush.flushPagesFromQueueToDisk(null, 100L);
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          }
        }, "issue6200-poller-" + i);
        final Thread purger = new Thread(() -> flush.removeAllPagesOfDatabase(db), "issue6200-purger-" + i);

        poller.start();
        purger.start();
        poller.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
        purger.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));

        assertThat(failure.get()).isNull();
        assertThat(flush.hasDeferredBatches(db)).as("iteration %d: a purged database must be left with no deferred queue", i)
            .isFalse();
        assertThat(flush.getDeferredRAMBytesOf(db)).as("iteration %d", i).isZero();
        assertThat(flush.deferredRAMBytes.get()).as("iteration %d: nothing may stay charged to a purged database", i)
            .isZero();
      }
    }

    /**
     * A purge landing in the middle of its OWN database's resume must not release the same bytes twice.
     * <p>
     * The resume walks the deferred batches holding only {@code replayDrainLock}, releasing them page by page; the
     * purge takes neither that lock nor anything else the resume holds, and its {@code releaseResidualDeferredRAM}
     * takes the database's whole remaining charge out of the JVM-wide total in one subtraction. The straggling
     * per-page releases that follow must then subtract NOTHING - they were already covered by that bulk one.
     * <p>
     * Getting it wrong drives the total negative, and the total is what the cap is read from: {@code total >= cap}
     * answers false for every suspended database in the process until enough new deferrals climb back over it, so the
     * #4728 OOM protection would be off for databases that have nothing to do with the one that was purged. That is
     * why this asserts the total is exactly zero and never negative, on every iteration.
     */
    @Test
    void aPurgeRacingItsOwnResumeReleasesTheSameBytesOnlyOnce() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManagerFlushThread flush = cappedFlushThread();

      for (int i = 0; i < 100; i++) {
        flush.setSuspended(db, true);
        // ONE long batch rather than many short ones: the window is inside Phase 1's per-page loop, so the loop has
        // to be long enough for the purge to land in it.
        enqueueBatch(flush, db, i, 400);
        flush.flushPagesFromQueueToDisk(null, 20L);
        assertThat(flush.deferredRAMBytes.get()).isPositive();

        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread resumer = new Thread(() -> {
          try {
            flush.setSuspended(db, false);
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          }
        }, "issue6200-resumer-" + i);
        final Thread purger = new Thread(() -> flush.removeAllPagesOfDatabase(db), "issue6200-purge-resume-" + i);

        resumer.start();
        purger.start();
        resumer.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
        purger.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));

        assertThat(failure.get()).isNull();
        // The drift counter, NOT just the total: the negative-total repair would otherwise hide exactly this bug, by
        // clamping a double release back to the zero a correct run also ends on.
        assertThat(flush.deferredRAMDriftRepairs.get()).as(
            "iteration %d: the JVM-wide total must never be released twice for the same bytes - a negative total silently disables the cap for every other suspended database",
            i).isZero();
        assertThat(flush.deferredRAMBytes.get()).as("iteration %d", i).isZero();
        assertThat(flush.getDeferredRAMBytesOf(db)).as("iteration %d", i).isZero();
      }
    }

    /**
     * An UNEXPECTED exception out of the resume's page-writing phase must not strand the rest of it.
     * <p>
     * Phase 1 contains every failure it expects - a dropped file, an interrupt, an I/O error - per page. Anything else
     * escaping it used to abort Phases 2 to 4 outright: the batches deferred during Phase 1 stayed undetached, their
     * pages pinned in the flush index forever (so the next close of that database waits for a drain that can never
     * happen), and its charge stayed on the cap that throttles every suspended database. The caller's own finally
     * heals the suspension COUNT, which is what makes the rest easy to miss.
     * <p>
     * A {@code null} slipped into a deferred batch stands in for "something the per-page handlers do not catch"; what
     * the assertions are about is the state the resume leaves behind, not the exception itself.
     * <p>
     * The batch that has to survive is one deferred WHILE Phase 1 runs - Phase 1 detaches the pre-existing backlog on
     * entry, so only a batch that arrives afterwards depends on Phase 2 - which is why this runs the resume on its own
     * thread and feeds the flush thread's defer path underneath it. The first attempt at this test did it all on one
     * thread and passed against the unfixed code, proving nothing.
     */
    @Test
    void anUnexpectedFailureWhileWritingTheBacklogStillFinishesTheResume() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      for (int round = 0; round < 20; round++) {
        final PageManagerFlushThread flush = cappedFlushThread();
        flush.setSuspended(db, true);

        // Long enough that the defer below lands inside Phase 1's write loop, with the trap at the very end so the
        // exception comes after that window rather than before it.
        final int pageSize = 1024;
        final List<MutablePage> pages = new ArrayList<>();
        for (int i = 0; i < 600; i++)
          pages.add(new MutablePage(new PageId(db, FILE_ID, i), pageSize, new byte[pageSize], 0, 0));
        final PagesToFlush trapped = new PagesToFlush(pages);
        flush.pageIndex.putAll(pages);
        flush.offerBatch(trapped, false);
        flush.flushPagesFromQueueToDisk(null, 20L);
        assertThat(flush.getDeferredRAMBytesOf(db)).isEqualTo((long) 600 * pageSize);

        // Booby-trapped AFTER the deferral accounted for it, so the failure lands in Phase 1's write loop rather than
        // in the defer that charged it.
        trapped.pages.add(null);

        final AtomicReference<Throwable> resumeFailure = new AtomicReference<>();
        final Thread resumer = new Thread(() -> {
          try {
            flush.setSuspended(db, false);
          } catch (final Throwable e) {
            resumeFailure.set(e);
          }
        }, "issue6200-failing-resume-" + round);
        resumer.start();

        // A batch deferred while Phase 1 is still grinding: this is the one only Phase 2 can detach.
        enqueue(flush, db, 5_000 + round);
        flush.flushPagesFromQueueToDisk(null, 20L);

        resumer.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
        assertThat(resumeFailure.get()).as("round %d: the failure must still reach the caller", round).isNotNull();

        assertThat(flush.isSuspended(db)).as("round %d: the suspension must not survive a failed resume", round).isFalse();
        assertThat(flush.hasDeferredBatches(db)).as(
            "round %d: Phase 2 must still detach what was deferred during Phase 1, or those pages stay pinned in the flush index and the next close waits for a drain that can never happen",
            round).isFalse();
        assertThat(flush.getDeferredRAMBytesOf(db)).as(
            "round %d: Phase 4 must still release the charge, or it throttles every later suspension in the process",
            round).isZero();
        assertThat(flush.deferredRAMDriftRepairs.get()).as(
            "round %d: and it must get there by accounting, not by the drift repair", round).isZero();
      }
    }

    /**
     * The invariant the whole per-database accounting rests on, made loud instead of silent.
     * <p>
     * Everything downstream reads the owning database off a batch's FIRST page: the suspension check that decides
     * whether to defer, the key of the deferred map, the per-database RAM charge, and the committer-side cap. A mixed
     * batch would throw nowhere; it would charge one database for another's pages and hold the wrong committers - the
     * quiet accounting drift #6200 spent its review rounds removing. This test exists so the assert that guards it is
     * known to FIRE rather than assumed to: an assertion nobody has seen trip is worth what a test nobody has seen
     * fail is worth.
     */
    @Test
    void aBatchMixingTwoDatabasesIsRejectedLoudly() {
      boolean assertionsEnabled = false;
      // A deliberate side effect inside an assert: the only portable way to ask whether -ea is on.
      assert assertionsEnabled = true;
      assumeTrue(assertionsEnabled, "assertions are disabled (-da), so the guard under test is not active");

      final DatabaseInternal db1 = (DatabaseInternal) database;
      final Database db2 = TestHelper.createDatabase("target/databases/" + getClass().getSimpleName() + "-mixed");
      try {
        final MutablePage ownPage = new MutablePage(new PageId(db1, FILE_ID, 0), PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
        final List<MutablePage> mixed = List.of(ownPage,
            new MutablePage(new PageId((DatabaseInternal) db2, FILE_ID, 0), PAGE_SIZE, new byte[PAGE_SIZE], 0, 0));

        assertThatThrownBy(() -> new PagesToFlush(mixed)).isInstanceOf(AssertionError.class)
            .hasMessageContaining("ONE database");

        // ...and the single-database batch it is contrasted with is built without complaint.
        assertThat(new PagesToFlush(List.of(ownPage)).database).isEqualTo(db1);
      } finally {
        db2.drop();
      }
    }

    /**
     * Constructing the flush thread directly does NOT start the background thread, so the pipeline only moves when the
     * test moves it.
     */
    private PageManagerFlushThread cappedFlushThread() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.FLUSH_SUSPEND_MAX_DEFERRED_RAM, (long) CAP_MB);
      return new PageManagerFlushThread(PageManager.INSTANCE, cfg);
    }

    private static MutablePage enqueue(final PageManagerFlushThread flush, final DatabaseInternal database,
        final int pageNumber) {
      final MutablePage page = new MutablePage(new PageId(database, FILE_ID, pageNumber), PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
      flush.pageIndex.put(page);
      flush.offerBatch(new PagesToFlush(List.of(page)), false);
      return page;
    }

    /** One batch of {@code pages} small pages, for the races whose window is inside a per-page loop. */
    private static void enqueueBatch(final PageManagerFlushThread flush, final DatabaseInternal database, final int round,
        final int pages) {
      final int pageSize = 1024;
      final List<MutablePage> batch = new ArrayList<>(pages);
      for (int i = 0; i < pages; i++)
        batch.add(new MutablePage(new PageId(database, FILE_ID, round * pages + i), pageSize, new byte[pageSize], 0, 0));
      flush.pageIndex.putAll(batch);
      flush.offerBatch(new PagesToFlush(batch), false);
    }
  }

  /**
   * Regression test for issue #6303, item 1: the per-database bookkeeping of {@link PageManagerFlushThread} must not be
   * reachable through a batch whose database has been closed.
   * <p>
   * {@code LocalDatabase} compares by database PATH, not by instance, so every map in that class keyed by a
   * {@code Database} answers about whatever database is open at that path NOW. Almost every caller holds a live
   * instance it owns and cannot be caught by that, with ONE exception: the flush thread reads a batch's own
   * {@code database} field after polling it, and a batch can outlive the database that queued it (it is polled, then
   * the database closes, then the suspension check runs). #6281 defended {@code slotsInUse}; the suspension flag, the
   * deferred-batch map, the deferred-RAM charge and the flush-progress counter were left aliasing.
   * <p>
   * These tests drive that exact interleaving white-box, because it is a race the public API cannot be made to produce
   * on demand: a batch of a closed database is put in the pipeline by hand and the pipeline is stepped one poll at a
   * time.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class StaleBatchBookkeeping6303 extends TestHelper {
    /** A file that does not exist, so a fabricated page can be driven through the pipeline without touching a real one. */
    private static final int FILE_ID   = 4_101;
    private static final int PAGE_SIZE = 64;

    /**
     * THE ISSUE. A batch left behind by a closed database must not be deferred into the backlog of one reopened at the
     * same path, nor charged to its deferred RAM.
     * <p>
     * Before the fix the flush thread asked {@code isSuspended(staleInstance)}, which resolved the NEW database's flag:
     * a batch of a database that no longer exists was deferred into a backlog it never contributed to and charged bytes
     * to a database that had queued nothing. The batch is not written either way - there is nothing left to write it to
     * - so the only observable effect was accounting drift in an unrelated database, which is precisely the shape that
     * takes the longest to explain.
     */
    @Test
    void aBatchOfAClosedDatabaseIsNotDeferredIntoTheBacklogOfOneReopenedAtTheSamePath() throws Exception {
      final String path = "target/databases/" + getClass().getSimpleName() + "-recycled";
      final PageManagerFlushThread flush = detachedFlushThread();

      final Database closed = TestHelper.createDatabase(path);
      final PageManagerFlushThread.PagesToFlush straggler = new PageManagerFlushThread.PagesToFlush(
          new ArrayList<>(List.of(page((DatabaseInternal) closed, 0))));
      closed.drop();
      assertThat(closed.isOpen()).isFalse();

      final Database reopened = TestHelper.createDatabase(path);
      try {
        assertThat(reopened).isEqualTo(closed);
        assertThat(reopened).isNotSameAs(closed);

        // The reopened database is suspended, exactly as a backup or an HA snapshot ship would leave it. This is the
        // flag the stale batch used to consult.
        assertThat(flush.setSuspended(reopened, true)).isTrue();
        try {
          // The straggler enters the pipeline as one polled just before its database closed would: it never went
          // through reserveQueueSlot, so it is charged one here.
          flush.offerBatch(straggler, false);
          flush.flushPagesFromQueueToDisk(null, 20L);

          assertThat(flush.hasDeferredBatches(reopened)).as(
                  "a batch of a database that no longer exists must not join the backlog of one merely sharing its path")
              .isFalse();
          assertThat(flush.getDeferredRAMBytesOf(reopened)).as(
              "...nor be charged to its deferred RAM: it queued nothing").isZero();
          assertThat(flush.deferredRAMBytes.get()).as("...nor to the JVM-wide total the #4728 cap is read from")
              .isZero();
        } finally {
          flush.setSuspended(reopened, false);
        }

        assertThat(flush.deferredRAMDriftRepairs.get()).as(
            "the accounting must balance by construction, not by the repair net of #6200").isZero();
      } finally {
        reopened.drop();
      }
    }

    /**
     * The progress signal of #4928 is now reported on the counter the BATCH carries rather than on one looked up per
     * page, so this pins that the refactor still counts what the bounded waits read - one bump per page that leaves the
     * pipeline, on the database the batch belongs to and on no other.
     * <p>
     * The lookup it replaces was keyed by the page's own database, which compares by PATH, so it was the same aliasing
     * as above one indirection further down. The check hoisted into the suspend monitor makes it unreachable through
     * the flush loop - a batch of a closed database no longer gets that far - so this is defence in depth rather than
     * the fix, and it is strictly less work than what it replaces: one map lookup per batch instead of one per page, on
     * the flush thread's hot loop.
     */
    @Test
    void progressIsCountedPerPageOnTheDatabaseTheBatchBelongsTo() throws Exception {
      final PageManagerFlushThread flush = detachedFlushThread();
      final DatabaseInternal live = (DatabaseInternal) database;
      final Database other = TestHelper.createDatabase("target/databases/" + getClass().getSimpleName() + "-other");
      try {
        flush.scheduleFlushOfPages(new ArrayList<>(List.of(page(live, 0), page(live, 1), page(live, 2))));
        flush.flushPagesFromQueueToDisk(null, 20L);

        assertThat(flush.flushedPagesPerDatabase.get(live)).isNotNull();
        assertThat(flush.flushedPagesPerDatabase.get(live).get()).as("one bump per page that left the pipeline")
            .isEqualTo(3);
        final AtomicLong othersProgress = flush.flushedPagesPerDatabase.get(other);
        assertThat(othersProgress == null ? 0L : othersProgress.get()).as(
            "and nothing at all for a database that queued nothing").isZero();
      } finally {
        other.drop();
      }
    }

    /**
     * Constructing the flush thread directly does NOT start the background thread, so the pipeline only moves when the
     * test moves it.
     */
    private static PageManagerFlushThread detachedFlushThread() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.PAGE_FLUSH_QUEUE, 8);
      return new PageManagerFlushThread(PageManager.INSTANCE, cfg);
    }

    private static MutablePage page(final DatabaseInternal database, final int pageNumber) {
      return new MutablePage(new PageId(database, FILE_ID, pageNumber), PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
    }
  }

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
  @Nested
  class PendingNowFlushWatermark8843 {

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
}
