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
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue6281PerDatabaseFlushQueueTest extends TestHelper {
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
