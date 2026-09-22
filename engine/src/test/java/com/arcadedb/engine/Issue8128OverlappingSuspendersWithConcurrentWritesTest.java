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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

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
class Issue8128OverlappingSuspendersWithConcurrentWritesTest extends TestHelper {

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
      // drains that can never make progress while suspender #1 still owns the freeze.
      final long start = System.currentTimeMillis();
      pageManager.suspendFlushAndExecute(db, () -> secondRan.set(true));
      final long elapsed = System.currentTimeMillis() - start;

      assertThat(elapsed)
          .as("an overlapping suspender must acquire instantly, not pay for the #8111 drains a second time "
              + "while they can make no progress")
          .isLessThan(1_000);

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
