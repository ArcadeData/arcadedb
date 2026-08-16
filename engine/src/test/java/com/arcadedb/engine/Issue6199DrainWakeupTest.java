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
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue6199DrainWakeupTest extends TestHelper {
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
    assertThat(flush.waitPendingPagesOfDatabaseUntil(db, begin + 200)).as("a page that never lands expires the deadline")
        .isFalse();
    // A TRIPWIRE, NOT A LATENCY ASSERTION, and its bound says so. What it can catch is a wait that came back only
    // because something else eventually ended it; what it must NOT do is measure how promptly a 200 ms deadline
    // expires, because this runs in the shared engine-suite JVM after ~12000 other tests, where a stop-the-world
    // pause of tens of seconds is not exotic - a 15 s bound here failed on exactly that, at 24 s, with the code
    // behaving correctly. The deadline being honoured is asserted above, by the return value.
    assertThat(System.currentTimeMillis() - begin).as("the wait must be bounded by the deadline rather than unbounded")
        .isLessThan(TimeUnit.MINUTES.toMillis(1));

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
