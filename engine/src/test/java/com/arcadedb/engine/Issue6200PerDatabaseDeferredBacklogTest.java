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
import com.arcadedb.engine.PageManagerFlushThread.PagesToFlush;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue6200PerDatabaseDeferredBacklogTest extends TestHelper {
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
      flush.queue.offer(trapped);
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
    flush.queue.offer(new PagesToFlush(List.of(page)));
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
    flush.queue.offer(new PagesToFlush(batch));
  }
}
