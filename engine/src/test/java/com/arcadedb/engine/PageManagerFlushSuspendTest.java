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
import com.arcadedb.utility.CallableNoReturn;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Suspending and resuming the page-manager flush (backups, snapshots): drains, overlapping suspenders, resume interruption and WAL acks on close.
 * <p>
 * Each nested class is the regression suite of one issue (the number is in the nested class name).
 */
class PageManagerFlushSuspendTest {

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
  @Nested
  class SuspendFlushDrainsPendingPages8111 extends TestHelper {

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

  /**
   * Regression test for a review finding on the #8111 fix (PR #8128): {@link PageManager#suspendFlushAndExecute}
   * used to run its two drains ({@code waitAllPagesOfDatabaseAreFlushed}, {@code waitPendingPagesOfDatabaseUntil})
   * unconditionally, even when the database was ALREADY suspended by another caller - which #5068's own refcounted
   * design explicitly supports ("every caller... owns its whole window even when the windows overlap").
   * <p>
   * The problem: while a database is suspended, a commit's page is DEFERRED rather than written
   * ({@code PageManagerFlushThread#flushPagesFromQueueToDisk}'s {@code isSuspended(db)} branch) and stays counted
   * as pending in {@code pageIndex} - it is never removed from it - until the LAST suspender releases and flushes
   * the backlog. An overlapping suspender's own drains would therefore see {@code pending() > 0} with NO possible
   * progress until the first suspender finishes, turning a previously-instant, previously-safe overlapping acquire
   * into a stall bounded only by {@code arcadedb.flushAllPagesTimeout} (60 s default) followed by a hard failure.
   * <p>
   * The fix adds a fast path: when the database is already suspended, skip the drains entirely and just bump the
   * refcount, the same instant acquisition the unconditional {@code setSuspended(database, true)} call always did
   * before the #8111 fix existed.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class OverlappingSuspendersWithConcurrentWrites8128 extends TestHelper {

    @Test
    void overlappingSuspenderAcquiresInstantlyDespiteADeferredPendingPage() throws Exception {
      final Database db = (Database) database;
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final PageManagerFlushThread flush = pageManager.getFlushThread();

      database.getSchema().createDocumentType("Overlap");

      final CountDownLatch firstInside = new CountDownLatch(1);
      final CountDownLatch releaseFirst = new CountDownLatch(1);

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

      final AtomicBoolean secondRan = new AtomicBoolean(false);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        first.start();
        firstInside.await();

        // A real commit while suspender #1 holds the freeze: its page is deferred, not written, and stays
        // counted as pending until the last suspender resumes.
        database.transaction(() -> database.newDocument("Overlap").set("id", 1).save());

        // Driven by the real state rather than a fixed sleep: wait until the deferral is actually visible.
        final long deadline = System.currentTimeMillis() + 5_000;
        while (!flush.pageIndex.hasPendingOf(database) && System.currentTimeMillis() < deadline)
          Thread.sleep(5);
        assertThat(flush.pageIndex.hasPendingOf(database))
            .as("the commit during the freeze must have left a page deferred and pending")
            .isTrue();

        // The overlapping suspender must acquire INSTANTLY - the pre-#8111-fix behavior - not stall behind
        // drains that can never make progress while suspender #1 still owns the freeze. StallAwareStopwatch,
        // not a raw System.currentTimeMillis() delta (review on PR #8128): this is exactly the tripwire case it
        // exists for - the instant fast-path join vs. the drain-then-acquire path bounded by
        // SNAPSHOT_BARRIER_MAX_MILLIS - and a GC pause or scheduling hiccup in a shared-JVM full-suite run must
        // not fail this test for reasons unrelated to the regression it guards against.
        final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
        pageManager.suspendFlushAndExecute(db, () -> secondRan.set(true));
        stopwatch.assertGaveUpWithin(1_000,
            "an overlapping suspender's instant join from a fresh drain-then-acquire that would have to wait out "
                + "SNAPSHOT_BARRIER_MAX_MILLIS while the freeze it can make no progress against is still held");

        releaseFirst.countDown();
        first.join();
      });

      assertThat(secondRan.get()).isTrue();
      // Once the last suspender resumes, the deferred write is handed back to the flush thread's normal
      // pipeline rather than written synchronously inside the resume itself - poll rather than assert
      // immediately, the same way the deferral itself was awaited above.
      final long flushDeadline = System.currentTimeMillis() + 5_000;
      while (flush.pageIndex.hasPendingOf(database) && System.currentTimeMillis() < flushDeadline)
        Thread.sleep(5);
      assertThat(flush.pageIndex.hasPendingOf(database))
          .as("the deferred write must have been flushed once the last suspender resumed")
          .isFalse();
      assertThat(database.query("sql", "select count(*) as c from Overlap").next().<Long>getProperty("c")).isEqualTo(1L);
    }

    /**
     * The scenario a follow-up review round found the fast-path probe alone did not close: two callers can
     * BOTH observe "nothing suspended yet" (the probe is a plain, unsynchronized-relative-to-each-other read
     * in that sense) and both commit to the first-suspender sequence, each running its own step 1 bulk drain
     * concurrently. Whichever reaches {@code trySuspendUntil} first, inside {@code PageManager}'s own
     * {@code lock()}, genuinely becomes first; the fix is that the OTHER one re-checks {@code isSuspended}
     * inside that SAME lock before attempting its own drain-dependent acquisition, so it discovers the
     * nesting instead of running a residual drain that can never converge while the winner holds the freeze.
     * <p>
     * Not reproduced deterministically (that would need instrumenting the exact interleaving inside
     * {@code PageManagerFlushThread}) but driven hard enough, with many threads racing to start at the same
     * instant and each doing real, overlapping writes inside its own window, that the race window this fix
     * closes is exercised many times over the run. Before the round-2 fix this reliably surfaced as an
     * {@code IOException} ("the flush pipeline did not settle...") within a handful of iterations locally.
     */
    @Test
    @Tag("slow")
    void manyRacingSuspendersWithConcurrentWritesNeverStallOrFail() throws Exception {
      final Database db = (Database) database;
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();

      database.getSchema().createDocumentType("Race");

      final int threads = 8;
      final int iterations = 20;
      final AtomicInteger nextId = new AtomicInteger();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final ExecutorService pool = Executors.newFixedThreadPool(threads);

      try {
        assertTimeoutPreemptively(Duration.ofSeconds(90), () -> {
          for (int iteration = 0; iteration < iterations && failure.get() == null; iteration++) {
            final CountDownLatch ready = new CountDownLatch(threads);
            final CountDownLatch go = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(threads);

            for (int t = 0; t < threads; t++)
              pool.submit(() -> {
                try {
                  ready.countDown();
                  // Every thread is released at the same instant, maximizing how often two of them both
                  // start their own suspendFlushAndExecute call before either has established the freeze.
                  go.await();
                  pageManager.suspendFlushAndExecute(db, () -> {
                    try {
                      for (int w = 0; w < 5; w++) {
                        database.transaction(() -> database.newDocument("Race").set("id", nextId.incrementAndGet()).save());
                        Thread.sleep(5);
                      }
                    } catch (final Exception e) {
                      failure.compareAndSet(null, e);
                    }
                  });
                } catch (final Throwable t2) {
                  failure.compareAndSet(null, t2);
                } finally {
                  done.countDown();
                }
              });

            ready.await();
            go.countDown();

            assertThat(done.await(10, TimeUnit.SECONDS))
                .as("iteration %d: every overlapping suspender must complete well within budget, not stall behind "
                    + "a drain that can never converge", iteration)
                .isTrue();
          }
        });
      } finally {
        // shutdownNow(), not shutdown(): if the preemptive timeout above fired, worker threads can still be
        // blocked inside suspendFlushAndExecute against `database` - shutdown() would leave them running while
        // TestHelper's own @AfterEach tears that database down underneath them, corrupting whichever test runs
        // next rather than just this one (review on PR #8128).
        pool.shutdownNow();
        pool.awaitTermination(10, TimeUnit.SECONDS);
      }

      assertThat(failure.get())
          .as("no overlapping suspender may fail with the residual race this test targets")
          .isNull();
    }

    /**
     * Regression test for a review finding on the fast path and the STEP 2 re-check above (PR #8128, round 5):
     * both used to decide "joined, skip the #8111 drain" from an {@code isSuspended(database)} PEEK taken
     * BEFORE the acquire, rather than from the acquire's own outcome. If the suspension the peek observed
     * finishes releasing - Phase 2 clears the refcount, Phase 3 re-enqueues whatever it deferred into the
     * ASYNC flush queue rather than writing it synchronously - before the peek-triggered {@code trySuspendUntil}
     * wakes up, that call's own acquisition is a genuine fresh 0-to-1: this caller has become the first
     * suspender of a brand-new window, not a joiner of the one the peek saw. The stale "joined" verdict then
     * skipped the drain over exactly that caller, so its callback could run against files still missing
     * whatever the departed suspender's release had queued but not yet written - #8111 again, reintroduced
     * through this interleaving.
     * <p>
     * {@link PageManagerFlushThread#testHookAfterDeferredBacklogSnapshot} makes this deterministic rather than
     * a matter of thread-scheduling luck: it pauses a real resume right after it has snapshotted the backlog
     * it is about to flush, so a commit made while paused becomes a genuine, not-yet-flushed deferred entry of
     * its own rather than part of what the resume is already about to write - and only then lets a second
     * suspender's acquisition attempt land, exactly in the window this fix closes.
     */
    @Test
    void suspenderWhoseAcquireStraddlesACompletingReleaseDrainsRatherThanSkips() throws Exception {
      final Database db = (Database) database;
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final PageManagerFlushThread flush = pageManager.getFlushThread();

      database.getSchema().createDocumentType("Race5");

      final CountDownLatch backlogSnapshotted = new CountDownLatch(1);
      final CountDownLatch releaseHook = new CountDownLatch(1);
      flush.testHookAfterDeferredBacklogSnapshot = () -> {
        backlogSnapshotted.countDown();
        try {
          releaseHook.await(10, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      };

      // Declared outside the try, not final: a failure before releaseHook.countDown() below must still be able
      // to release and interrupt both workers in the finally, rather than leaving one of them blocked in the
      // hook (or, for B, in its own acquisition attempt) while TestHelper's own teardown proceeds against the
      // same database underneath it (review on PR #8128).
      Thread x = null;
      Thread b = null;
      try {
        x = new Thread(() -> {
          try {
            pageManager.suspendFlushAndExecute(db,
                () -> database.transaction(() -> database.newDocument("Race5").set("id", 1).save()));
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
        }, "test-suspender-x");
        x.start();

        assertThat(backlogSnapshotted.await(10, TimeUnit.SECONDS))
            .as("X's release must have reached the hook - snapshotted its own backlog - before this test proceeds")
            .isTrue();

        // A commit made now, while X's release is paused right after snapshotting its OWN backlog, cannot be
        // part of what X is about to flush: it becomes a fresh, genuinely unflushed deferred entry of its own.
        database.transaction(() -> database.newDocument("Race5").set("id", 2).save());

        final AtomicBoolean sawPendingAtCallbackEntry = new AtomicBoolean();
        final AtomicReference<Throwable> bFailure = new AtomicReference<>();
        b = new Thread(() -> {
          try {
            pageManager.suspendFlushAndExecute(db, () -> sawPendingAtCallbackEntry.set(flush.pageIndex.hasPendingOf(database)));
          } catch (final Throwable t) {
            bFailure.set(t);
          }
        }, "test-suspender-b");
        b.start();

        // Generous, non-tight padding: gives B's thread a real chance to reach its own acquisition attempt
        // WHILE X is still paused at the hook - the interleaving this fix targets - whether B ends up waiting
        // (pre-fix trySuspendUntil) or declining outright (post-fix tryJoinActiveSuspension). The hook alone
        // guarantees the interleaving is POSSIBLE; this only makes it reliably OBSERVED rather than missed by
        // scheduling luck - it does not stand in for the correctness this test asserts.
        Thread.sleep(200);

        releaseHook.countDown();

        x.join(10_000);
        b.join(10_000);

        assertThat(x.isAlive()).as("suspender X must have finished releasing").isFalse();
        assertThat(b.isAlive()).as("suspender B must have finished its acquisition and callback").isFalse();
        assertThat(bFailure.get()).as("suspender B must not have failed").isNull();

        assertThat(sawPendingAtCallbackEntry.get())
            .as("suspender B's callback ran while a page committed just before it was still pending - it "
                + "treated a fresh acquisition as a join of X's already-completed window and skipped the "
                + "#8111 drain")
            .isFalse();
      } finally {
        // Unconditionally release the hook FIRST: an assertion above can fail before releaseHook.countDown()
        // runs on the happy path, and without this X would stay parked in the hook for up to its own 10s
        // timeout while teardown proceeds. Interrupting both threads then bounds how long a failure elsewhere
        // (e.g. B stuck in its own drain) can keep them alive past this test.
        releaseHook.countDown();
        if (x != null)
          x.interrupt();
        if (b != null)
          b.interrupt();
        try {
          if (x != null)
            x.join(10_000);
          if (b != null)
            b.join(10_000);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          flush.testHookAfterDeferredBacklogSnapshot = null;
        }
      }

      final long deadline = System.currentTimeMillis() + 5_000;
      while (flush.pageIndex.hasPendingOf(database) && System.currentTimeMillis() < deadline)
        Thread.sleep(5);
      assertThat(database.query("sql", "select count(*) as c from Race5").next().<Long>getProperty("c")).isEqualTo(2L);
    }
  }

  /**
   * Regression test for issue #9832.
   * <p>
   * Phase 3b of {@code PageManagerFlushThread.resumeFlushing} puts back into the main queue the batches deferred while
   * Phase 1 was draining the backlog. It re-enters admission control one batch at a time, and with a backlog larger than
   * the database's own budget the loop fills that budget with its own batches and then WAITS for a slot. An interrupt of
   * the resuming thread at that point used to {@code break} out of the loop and drop every batch it had not reached:
   * no longer in the deferred map, never in the queue, still in {@code pageIndex} and still holding their WAL ack. The
   * database's pending count then stayed above zero for good, so every later backup, verify, HA snapshot and file freeze
   * timed out on its first drain.
   * <p>
   * The flush thread here is constructed and never started, so the pipeline only moves when the test moves it. The budget
   * is 2 and the resume carries 3 batches: the loop admits two and waits for the third, which is the production shape
   * the issue singles out (no pre-filled budget needed, no competing writer).
   */
  @Nested
  class ResumeReEnqueueInterrupt9832 extends TestHelper {
    /** A file that does not exist, so a fabricated page can be driven through the pipeline without touching a real one. */
    private static final int  FILE_ID               = 9_832;
    private static final int  PAGE_SIZE             = 64;
    private static final int  BUDGET                = 2;
    private static final int  DEFERRED_BATCHES      = 3;
    /** Bounds only waits expected to SUCCEED, so a generous value cannot turn a passing run red (#6260). */
    private static final long ASSERTION_TIMEOUT_SEC = 60;

    private final List<Thread>                threads       = new ArrayList<>();
    private final AtomicReference<Throwable> resumeFailure = new AtomicReference<>();

    @AfterEach
    void stopThreads() throws InterruptedException {
      for (final Thread t : threads) {
        t.interrupt();
        t.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
      }
    }

    /**
     * The resuming thread arrives at the re-enqueue with its interrupt flag already set - here raised inside Phase 1, as a
     * cancelled backup would. Phase 1 wrote no page, so it never consumed the flag on its own, and the first reservation
     * that has to wait threw {@link InterruptedException} straight away.
     */
    @Test
    void anInterruptPendingWhenTheReEnqueueStartsAbandonsNothing() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManagerFlushThread flush = smallBudgetFlushThread();

      flush.setSuspended(db, true);
      flush.testHookAfterDeferredBacklogSnapshot = () -> {
        deferBatchesDuringPhase1(flush, db);
        Thread.currentThread().interrupt();
      };

      final AtomicBoolean interruptRestored = new AtomicBoolean();
      final Thread resumer = resume(flush, db, interruptRestored);
      awaitParkedOrDone(resumer);

      startDrainer(flush);
      resumer.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
      assertThat(resumer.isAlive()).as("the resume must complete once the pipeline drains").isFalse();

      assertDrained(flush, db);
      assertThat(interruptRestored.get()).as("the caller must still observe its own cancellation").isTrue();
      assertThat(flush.isSuspended(db)).isFalse();
    }

    /**
     * The interrupt arrives while the re-enqueue is parked in {@code reserveQueueSlot}, waiting for the database's
     * budget, which the loop's own first batches filled.
     */
    @Test
    void anInterruptWhileTheReEnqueueWaitsForASlotAbandonsNothing() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManagerFlushThread flush = smallBudgetFlushThread();

      flush.setSuspended(db, true);
      flush.testHookAfterDeferredBacklogSnapshot = () -> deferBatchesDuringPhase1(flush, db);

      final AtomicBoolean interruptRestored = new AtomicBoolean();
      final Thread resumer = resume(flush, db, interruptRestored);
      awaitParkedOrDone(resumer);
      assertThat(resumer.isAlive()).as("with no drainer the third reservation must wait for a slot").isTrue();
      assertThat(flush.slotsUsedBy(db)).as("the loop filled the budget with its own batches").isEqualTo(BUDGET);

      resumer.interrupt();
      // Give an interrupt that WOULD abandon the remaining batch the time to do so before anything frees a slot.
      resumer.join(200);

      startDrainer(flush);
      resumer.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
      assertThat(resumer.isAlive()).as("the resume must complete once the pipeline drains").isFalse();

      assertDrained(flush, db);
      assertThat(interruptRestored.get()).as("the caller must still observe its own cancellation").isTrue();
    }

    /**
     * The one case where giving up on a batch is right: the flush thread shuts down while the re-enqueue waits. What the
     * loop never enqueues must leave {@code pageIndex} - not be left pinning the database's pending count - while the
     * database is still open, so its WAL entry stays un-acked and is replayed on the next open.
     */
    @Test
    void aShutdownDuringTheReEnqueueReleasesTheBatchesItGivesUp() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final WALFile walFile = new WALFile(new File(db.getDatabasePath(), "issue9832-open.wal").getAbsolutePath());
      try {
        final PageManagerFlushThread flush = shutDownWhileReEnqueueWaits(db, walFile, false);

        // The run loop drains what was queued before it exits; do the same here. The queued pages' file does not
        // exist, so the drain acks them as abandoned.
        while (!flush.queue.isEmpty())
          flush.flushPagesFromQueueToDisk(null, 20L);
        assertThat(flush.pageIndex.pendingOf(db)).isZero();
        assertThat(walFile.getPendingPagesToFlush()).as(
                "the page given up on while its database is open was never written: its WAL entry must stay un-acked so the next open replays it")
            .isEqualTo(DEFERRED_BATCHES - BUDGET);
      } finally {
        walFile.close();
      }
    }

    /**
     * Same shutdown, but the database closed while the re-enqueue was waiting: nothing will ever replay the page given
     * up on, so - like every other site that drops a batch of a closed database (issue #6440) - its WAL ack is released.
     */
    @Test
    void aShutdownDuringTheReEnqueueOfAClosedDatabaseAlsoReleasesTheWalAcks() throws Exception {
      final String path = "target/databases/" + getClass().getSimpleName() + "-closed";
      final DatabaseInternal db = (DatabaseInternal) TestHelper.createDatabase(path);
      final WALFile walFile = new WALFile(new File(path, "issue9832-closed.wal").getAbsolutePath());
      try {
        final PageManagerFlushThread flush = shutDownWhileReEnqueueWaits(db, walFile, true);

        assertThat(walFile.getPendingPagesToFlush()).as(
            "the page given up on belongs to a closed database: its WAL ack must be released").isEqualTo(BUDGET);

        while (!flush.queue.isEmpty())
          flush.flushPagesFromQueueToDisk(null, 20L);
        assertThat(flush.pageIndex.pendingOf(db)).isZero();
        assertThat(walFile.getPendingPagesToFlush()).isZero();
      } finally {
        walFile.close();
        if (db.isOpen())
          db.close();
        TestHelper.dropDatabase(path);
      }
    }

    /**
     * Parks a resume in Phase 3b's slot wait with {@value #BUDGET} of its {@value #DEFERRED_BATCHES} batches queued, then
     * shuts the flush thread down - after closing the database first when asked.
     */
    private PageManagerFlushThread shutDownWhileReEnqueueWaits(final DatabaseInternal db, final WALFile walFile,
        final boolean closeDatabaseFirst) throws Exception {
      final PageManagerFlushThread flush = smallBudgetFlushThread();

      flush.setSuspended(db, true);
      flush.testHookAfterDeferredBacklogSnapshot = () -> deferBatchesDuringPhase1(flush, db, walFile);

      final Thread resumer = resume(flush, db, new AtomicBoolean());
      awaitParkedOrDone(resumer);
      assertThat(resumer.isAlive()).isTrue();
      assertThat(walFile.getPendingPagesToFlush()).isEqualTo(DEFERRED_BATCHES);

      if (closeDatabaseFirst)
        db.close();

      // Never started, so the join inside returns at once; what matters is `running` going false and the waiters waking.
      flush.closeAndJoin();
      resumer.join(TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC));
      assertThat(resumer.isAlive()).as("a shutdown must release the resume").isFalse();
      assertThat(resumeFailure.get()).isNull();

      assertThat(flush.pageIndex.pendingOf(db)).as(
          "only the pages that made it into the queue may still be pending: the batch the loop gave up on must leave the index")
          .isEqualTo(BUDGET);
      return flush;
    }

    /**
     * Runs inside Phase 1, after it has taken its snapshot of the backlog: every batch published and polled here is
     * deferred, so it is exactly what Phase 3a detaches and Phase 3b must re-enqueue.
     */
    private static void deferBatchesDuringPhase1(final PageManagerFlushThread flush, final DatabaseInternal db) {
      deferBatchesDuringPhase1(flush, db, null);
    }

    /** @param walFile when not null, every page is charged to it, as {@code WALFile.writeTransactionToFile} does. */
    private static void deferBatchesDuringPhase1(final PageManagerFlushThread flush, final DatabaseInternal db, final WALFile walFile) {
      try {
        for (int i = 0; i < DEFERRED_BATCHES; i++) {
          final MutablePage page = page(db, i);
          if (walFile != null) {
            page.setWALFile(walFile);
            bumpPendingPagesToFlush(walFile);
          }
          flush.scheduleFlushOfPages(new ArrayList<>(List.of(page)));
          flush.flushPagesFromQueueToDisk(null, 50L);
        }
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      assertThat(flush.queue).as("every batch was deferred, none left queued").isEmpty();
      assertThat(flush.pageIndex.pendingOf(db)).isEqualTo(DEFERRED_BATCHES);
    }

    private Thread resume(final PageManagerFlushThread flush, final DatabaseInternal db, final AtomicBoolean interruptRestored) {
      final Thread resumer = new Thread(() -> {
        try {
          flush.setSuspended(db, false);
          interruptRestored.set(Thread.currentThread().isInterrupted());
        } catch (final Throwable e) {
          resumeFailure.set(e);
        }
      }, "issue9832-resumer");
      resumer.setDaemon(true);
      threads.add(resumer);
      resumer.start();
      return resumer;
    }

    private void startDrainer(final PageManagerFlushThread flush) {
      final Thread drainer = new Thread(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            flush.flushPagesFromQueueToDisk(null, 20L);
          } catch (final InterruptedException e) {
            return;
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      }, "issue9832-drainer");
      drainer.setDaemon(true);
      threads.add(drainer);
      drainer.start();
    }

    /** Waits until the resumer either finished or parked in the slot reservation (a timed monitor wait). */
    private static void awaitParkedOrDone(final Thread resumer) throws InterruptedException {
      final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC);
      while (resumer.isAlive() && resumer.getState() != Thread.State.TIMED_WAITING && System.currentTimeMillis() < deadline)
        Thread.sleep(5);
    }

    private void assertDrained(final PageManagerFlushThread flush, final DatabaseInternal db) throws InterruptedException {
      final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC);
      while (flush.pageIndex.pendingOf(db) > 0 && System.currentTimeMillis() < deadline)
        Thread.sleep(5);
      assertThat(flush.pageIndex.pendingOf(db)).as(
          "every deferred page must leave the pipeline: a batch the re-enqueue dropped stays pending forever").isZero();
      assertThat(resumeFailure.get()).isNull();
      assertThat(flush.getDeferredRAMBytesOf(db)).isZero();
    }

    private static PageManagerFlushThread smallBudgetFlushThread() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.PAGE_FLUSH_QUEUE, BUDGET);
      return new PageManagerFlushThread(PageManager.INSTANCE, cfg);
    }

    /** Mirrors what {@code WALFile.writeTransactionToFile} does for each page it writes, without touching disk I/O. */
    private static void bumpPendingPagesToFlush(final WALFile walFile) throws ReflectiveOperationException {
      final Field f = WALFile.class.getDeclaredField("pagesToFlush");
      f.setAccessible(true);
      ((AtomicInteger) f.get(walFile)).incrementAndGet();
    }

    private static MutablePage page(final DatabaseInternal database, final int pageNumber) {
      return new MutablePage(new PageId(database, FILE_ID, pageNumber), PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
    }
  }

  /**
   * Regression test for issue #9881: a database closed or dropped while its flushing was suspended (a backup, a verify, an
   * HA snapshot) has batches sitting in the flush thread's deferred backlog rather than in its queue. The purge used to
   * detach them without ever releasing their WAL acks, so {@code WALFile.pagesToFlush} stayed above zero for each page.
   * <ul>
   * <li>On a {@code drop()} that is wrong: the files are deleted, nothing will ever replay those pages, and the stranded
   * count makes {@code TransactionManager.close()} spin its retry loop on WAL files it cannot drop.</li>
   * <li>On a {@code close()} it is right: the deferred pages never reached the disk, and the un-acked WAL entry is the
   * only durable copy of them - the close is crash-equivalent and the next open replays them.</li>
   * </ul>
   * Both cases are driven end to end through the real flush thread and a real suspension. The {@link TestHelper} database
   * stays open throughout only to hold a page-manager lifecycle reference: the database under test is closed inside the
   * suspension, and were it the last one open, the page manager would shut down under the suspender.
   */
  @Nested
  class DeferredBatchWalAckOnClose9881 extends TestHelper {
    private static final String DB_PATH               = "target/databases/Issue9881DeferredBatchWalAckOnCloseTest-suspended";
    /** Bounds only waits expected to SUCCEED, so a generous value cannot turn a passing run red (#6260). */
    private static final long   ASSERTION_TIMEOUT_SEC = 60;
    private static final int    RECORDS               = 10;

    @BeforeEach
    void cleanSuspendedDatabase() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @AfterEach
    void removeSuspendedDatabase() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void droppingASuspendedDatabaseReleasesTheWalAcksOfItsDeferredBatches() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) new DatabaseFactory(DB_PATH).create();
      db.getSchema().createDocumentType("Doc");

      final List<WALFile> walFiles = new ArrayList<>();
      final long[] pendingBeforeDrop = { 0 };
      runSuspended(db, () -> {
        commitDeferredRecords(db);
        walFiles.addAll(activeWALFiles(db));
        pendingBeforeDrop[0] = pendingAcks(walFiles);
        db.drop();
      });

      assertThat(pendingBeforeDrop[0]).as("the deferred pages were written to the WAL and not yet acked").isPositive();

      assertThat(db.isOpen()).isFalse();
      assertThat(new File(DB_PATH)).doesNotExist();
      assertThat(pendingAcks(walFiles)).as(
          "a dropped database's deferred pages will never be written nor replayed: their WAL acks must be released").isZero();
    }

    @Test
    void closingASuspendedDatabaseKeepsTheWalOfItsDeferredBatchesForRecovery() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) new DatabaseFactory(DB_PATH).create();
      db.getSchema().createDocumentType("Doc");
      // The deferred pages cannot make progress while the database is suspended: give up the close's flush wait fast.
      db.getConfiguration().setValue(GlobalConfiguration.FLUSH_ALL_PAGES_TIMEOUT, 200L);

      final List<WALFile> walFiles = new ArrayList<>();
      runSuspended(db, () -> {
        commitDeferredRecords(db);
        walFiles.addAll(activeWALFiles(db));
        db.close();
      });

      assertThat(db.isOpen()).isFalse();
      assertThat(pendingAcks(walFiles)).as(
          "a closed database's deferred pages never reached the disk: the WAL entries must stay un-acked for recovery").isPositive();

      try (final Database reopened = new DatabaseFactory(DB_PATH).open()) {
        final long[] count = { 0 };
        reopened.iterateType("Doc", false).forEachRemaining(r -> count[0]++);
        assertThat(count[0]).as("the next open must replay the deferred pages from the preserved WAL").isEqualTo(RECORDS);
      }
    }

    /**
     * Runs {@code body} inside a real flush suspension. {@code suspendFlushAndExecute} logs and swallows whatever its
     * callback throws, so the failure is carried out and rethrown here instead of passing the test silently.
     */
    private static void runSuspended(final DatabaseInternal db, final CallableNoReturn body) throws Exception {
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicBoolean ran = new AtomicBoolean();
      db.getPageManager().suspendFlushAndExecute(db, () -> {
        try {
          body.call();
          ran.set(true);
        } catch (final Throwable t) {
          failure.set(t);
        }
      });
      if (failure.get() instanceof Error error)
        throw error;
      if (failure.get() instanceof Exception exception)
        throw exception;
      assertThat(ran.get()).as("the suspended body must have run to completion").isTrue();
    }

    /** Commits inside the suspension and waits for the flush thread to defer the resulting batch. */
    private static void commitDeferredRecords(final DatabaseInternal db) {
      db.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          db.newDocument("Doc").set("id", i).save();
      });

      final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ASSERTION_TIMEOUT_SEC);
      while (db.getPageManager().getDeferredRAMBytesOf(db) == 0 && System.currentTimeMillis() < deadline)
        try {
          Thread.sleep(5);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      assertThat(db.getPageManager().getDeferredRAMBytesOf(db)).as("the commit's pages must be deferred by the suspension")
          .isPositive();
    }

    private static List<WALFile> activeWALFiles(final DatabaseInternal db) {
      try {
        final Field f = TransactionManager.class.getDeclaredField("activeWALFilePool");
        f.setAccessible(true);
        final List<WALFile> files = new ArrayList<>();
        for (final WALFile file : (WALFile[]) f.get(db.getTransactionManager()))
          if (file != null)
            files.add(file);
        return files;
      } catch (final ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }

    private static long pendingAcks(final List<WALFile> walFiles) {
      long total = 0;
      for (final WALFile file : walFiles)
        total += file.getPendingPagesToFlush();
      return total;
    }
  }

  /**
   * Regression test for issue #9927: {@code PageManager.suspendFlushAndExecute} re-read the {@code flushThread} field in
   * its {@code finally}. When the callback closed or dropped the LAST open database of the JVM, the page-manager
   * lifecycle refcount went 1 -> 0, {@code shutdown()} nulled {@code flushThread}, and releasing the suspension threw
   * {@code NullPointerException: Cannot invoke "PageManagerFlushThread.setSuspended(...)" because "this.flushThread" is
   * null}. Both release sites are driven: the first suspender's own {@code finally}, and {@code runAlreadySuspended} for
   * a nested suspender that joined the active window.
   * <p>
   * Deliberately NOT a {@code TestHelper}: its database would hold a lifecycle reference and keep the page manager
   * alive, which is exactly what #9881's test does to steer clear of this bug. Here the database under test must be the
   * only reference, so each test checks the JVM-wide refcount before starting and skips (rather than passing for the
   * wrong reason) if a database leaked by an earlier test class is still holding one.
   */
  @Nested
  class SuspendFlushOnLastDatabaseClose9927 {
    private static final String DB_NAME    = "Issue9927SuspendFlushOnLastDatabaseCloseTest";
    private static final String DB_PATH    = "target/databases/" + DB_NAME;
    private static final String AFTER_PATH = DB_PATH + "-after";

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      FileUtils.deleteRecursively(new File(AFTER_PATH));
      assumeThat(PageManager.INSTANCE.getLifecycleRefCount())
          .as("another database is open in this JVM, so closing the one under test cannot shut the page manager down")
          .isZero();
    }

    @AfterEach
    void tearDown() {
      // A failed assertion can leave the database under test (or the AFTER_PATH one) open. Its lifecycle reference
      // would then make every later test of this class skip on the refcount assumption, so close it here.
      for (final Database db : new ArrayList<>(DatabaseFactory.getActiveDatabaseInstances()))
        if (db.getDatabasePath().contains(DB_NAME))
          db.close();
      FileUtils.deleteRecursively(new File(DB_PATH));
      FileUtils.deleteRecursively(new File(AFTER_PATH));
    }

    @Test
    void droppingTheLastOpenDatabaseInsideTheSuspensionDoesNotThrow() throws Exception {
      final DatabaseInternal db = createDatabase();
      final PageManagerFlushThread suspendedOn = PageManager.INSTANCE.getFlushThread();

      runSuspended(db, () -> {
        db.drop();
        assertPageManagerShutDown();
      });

      assertThat(db.isOpen()).isFalse();
      assertThat(new File(DB_PATH)).doesNotExist();
      // The fix releases on the thread that shut down, which is safe only because the close made that thread forget the
      // database's suspension count: pin that coupling, so a refactor of setSuspended cannot quietly break it.
      assertThat(suspendedOn.isSuspended(db)).isFalse();
      assertThat(suspendedOn.setSuspended(db, false)).as("an extra release on the dead thread must be a no-op").isFalse();
      assertPageManagerUsableAgain();
    }

    @Test
    void closingTheLastOpenDatabaseInsideTheSuspensionDoesNotThrow() throws Exception {
      final DatabaseInternal db = createDatabase();

      runSuspended(db, () -> {
        db.close();
        assertPageManagerShutDown();
      });

      assertThat(db.isOpen()).isFalse();
      assertPageManagerUsableAgain();
    }

    @Test
    void droppingTheLastOpenDatabaseInsideANestedSuspensionDoesNotThrow() throws Exception {
      final DatabaseInternal db = createDatabase();
      final AtomicBoolean nestedRan = new AtomicBoolean();

      // The inner call joins the outer window (tryJoinActiveSuspension), so it releases through runAlreadySuspended;
      // once it returns, the outer call releases through its own finally - both after the page manager shut down.
      runSuspended(db, () -> runSuspended(db, () -> {
        db.drop();
        assertPageManagerShutDown();
        nestedRan.set(true);
      }));

      assertThat(nestedRan.get()).isTrue();
      assertThat(db.isOpen()).isFalse();
      assertPageManagerUsableAgain();
    }

    private static DatabaseInternal createDatabase() {
      final DatabaseInternal db = (DatabaseInternal) new DatabaseFactory(DB_PATH).create();
      db.getSchema().createDocumentType("Doc");
      db.transaction(() -> db.newDocument("Doc").set("id", 0).save());
      assertThat(PageManager.INSTANCE.getLifecycleRefCount()).as("the database under test holds the only reference")
          .isEqualTo(1);
      return db;
    }

    /** Proves the scenario reached the 1 -> 0 transition the bug needs, so a pass is not a pass for the wrong reason. */
    private static void assertPageManagerShutDown() {
      assertThat(PageManager.INSTANCE.getLifecycleRefCount()).isZero();
      assertThat(PageManager.INSTANCE.getFlushThread()).as("the last release must have shut the flush thread down")
          .isNull();
    }

    /** The suspension released against a dead flush thread must leave nothing behind that breaks the next database. */
    private static void assertPageManagerUsableAgain() throws Exception {
      try (final Database db = new DatabaseFactory(AFTER_PATH).create()) {
        db.getSchema().createDocumentType("Doc");
        db.transaction(() -> db.newDocument("Doc").set("id", 1).save());
        assertThat(db.countType("Doc", false)).isEqualTo(1);
        final PageManager pageManager = ((DatabaseInternal) db).getPageManager();
        pageManager.suspendFlushAndExecute(db, () -> {
        });
        assertThat(pageManager.isPageFlushingSuspended(db)).isFalse();
      }
      new DatabaseFactory(AFTER_PATH).open().drop();
      assertThat(PageManager.INSTANCE.getLifecycleRefCount()).isZero();
    }

    /**
     * Runs {@code body} inside a real flush suspension. {@code suspendFlushAndExecute} logs and swallows whatever its
     * callback throws, so a failed assertion is carried out and rethrown here instead of passing the test silently. What
     * the release of the suspension throws AFTER the callback - the #9927 NPE - propagates straight out.
     */
    private static void runSuspended(final DatabaseInternal db, final CallableNoReturn body) throws Exception {
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicBoolean ran = new AtomicBoolean();
      db.getPageManager().suspendFlushAndExecute(db, () -> {
        try {
          body.call();
          ran.set(true);
        } catch (final Throwable t) {
          failure.set(t);
        }
      });
      if (failure.get() instanceof Error error)
        throw error;
      if (failure.get() instanceof Exception exception)
        throw exception;
      assertThat(ran.get()).as("the suspended body must have run to completion").isTrue();
    }
  }
}
