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
import com.arcadedb.engine.PageManagerFlushThread.PagesToFlush;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4728: a busy leader shipping a multi-GB HA snapshot OOM'd because dirty pages
 * accumulated without bound in {@link PageManagerFlushThread}'s deferred map while page flushing was suspended.
 * <p>
 * The fix caps the deferred backlog ({@code arcadedb.flushSuspendMaxDeferredRAM}). Once the cap is reached the
 * committing threads of the suspended database are throttled, instead of the deferred map growing until the heap
 * is exhausted.
 * <p>
 * <b>Where that throttling happens moved in #6200</b>, and this test moved with it. It used to be the flush thread
 * that stopped draining its bounded queue, which then filled and backpressured the committers - all of them, of
 * every open database, and inside the JVM-wide page-manager lock that publication holds. The cap is now served on
 * the committer side of the suspended database only ({@code awaitDeferredBacklogUnderCap}), before that lock is
 * taken, and the flush thread always drains. The bound this test exists for is unchanged and is asserted below in
 * its new form: past the cap, no further page of the suspended database can be published.
 */
class PageManagerFlushSuspendBackpressureTest extends TestHelper {

  private static final int  PAGE_SIZE = 256 * 1024;          // 256 KB per page
  private static final int  CAP_MB    = 1;                   // 1 MB deferred cap -> 4 pages fit exactly
  private static final long CAP_BYTES = (long) CAP_MB * 1024 * 1024;

  @Test
  void deferredBacklogStaysBoundedWhileSuspended() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.FLUSH_SUSPEND_MAX_DEFERRED_RAM, (long) CAP_MB);

    // Constructing the flush thread directly does NOT start the background thread; flushPagesFromQueueToDisk is
    // driven explicitly below so the test is deterministic.
    final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, cfg);

    final Database db = (Database) database;
    flush.setSuspended(db, true);
    try {
      // Enqueue 4 single-page batches: exactly what fits under the 1 MB cap.
      final int batches = 4;
      for (int i = 0; i < batches; i++) {
        final PageId pageId = new PageId(database, 9, i);
        final MutablePage page = new MutablePage(pageId, PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
        flush.pageIndex.put(page);
        flush.queue.offer(new PagesToFlush(List.of(page)));
      }

      // Drive the flush thread by hand: with the database suspended every batch is deferred, never written.
      for (int i = 0; i < batches; i++)
        flush.flushPagesFromQueueToDisk(null, 20L);

      assertThat(flush.deferredRAMBytes.get()).isEqualTo(4L * PAGE_SIZE);
      assertThat(flush.deferredRAMBytes.get()).isGreaterThanOrEqualTo(CAP_BYTES);

      // The backlog is at the cap, so the next committer of THIS database is held before it can publish anything
      // more into it - which is what keeps the deferred map from growing until the heap is exhausted.
      final CountDownLatch published = new CountDownLatch(1);
      final Thread committer = new Thread(() -> {
        try {
          flush.awaitDeferredBacklogUnderCap(db);
          published.countDown();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }, "issue4728-committer");
      committer.start();
      try {
        assertThat(published.await(500, TimeUnit.MILLISECONDS)).as(
            "past the cap no further page of the suspended database may be published").isFalse();
      } finally {
        committer.interrupt();
        committer.join(TimeUnit.SECONDS.toMillis(10));
      }
    } finally {
      flush.setSuspended(db, false);
    }
  }

  @Test
  void disabledCapKeepsDrainingTheQueue() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.FLUSH_SUSPEND_MAX_DEFERRED_RAM, 0L); // 0 = unbounded (pre-#4728 behavior)

    final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, cfg);

    final Database db = (Database) database;
    flush.setSuspended(db, true);

    final int batches = 6;
    for (int i = 0; i < batches; i++) {
      final PageId pageId = new PageId(database, 9, i);
      final MutablePage page = new MutablePage(pageId, PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
      flush.pageIndex.put(page);
      flush.queue.offer(new PagesToFlush(List.of(page)));
    }

    for (int i = 0; i < batches; i++)
      flush.flushPagesFromQueueToDisk(null, 20L);

    // With the cap disabled the whole queue drains into the deferred map regardless of size.
    assertThat(flush.queue).isEmpty();
    assertThat(flush.deferredRAMBytes.get()).isEqualTo((long) batches * PAGE_SIZE);
  }
}
