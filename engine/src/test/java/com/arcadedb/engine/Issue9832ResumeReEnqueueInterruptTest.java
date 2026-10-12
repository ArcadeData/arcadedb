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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue9832ResumeReEnqueueInterruptTest extends TestHelper {
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
