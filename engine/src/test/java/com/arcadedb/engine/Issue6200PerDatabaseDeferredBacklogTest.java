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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

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
  private static final long ASSERTION_TIMEOUT_SEC = 15;

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

        final long begin = System.currentTimeMillis();
        flush.awaitDeferredBacklogUnderCap(otherDb);
        assertThat(System.currentTimeMillis() - begin).as(
            "a database that is not suspended must not wait on another database's backlog").isLessThan(1_000);
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
}
