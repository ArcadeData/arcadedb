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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4728: a busy leader shipping a multi-GB HA snapshot OOM'd because dirty pages
 * accumulated without bound in {@link PageManagerFlushThread}'s deferred map while page flushing was suspended.
 * <p>
 * The fix caps the deferred backlog ({@code arcadedb.flushSuspendMaxDeferredRAM}). Once the cap is reached the
 * flush thread stops draining its bounded queue, which then fills and backpressures the committing threads
 * instead of letting the deferred map grow until the heap is exhausted.
 */
class PageManagerFlushSuspendBackpressureTest extends TestHelper {

  private static final int PAGE_SIZE = 256 * 1024;          // 256 KB per page
  private static final int CAP_MB    = 1;                   // 1 MB deferred cap -> 4 pages fit exactly

  @Test
  void deferredBacklogStaysBoundedWhileSuspended() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.FLUSH_SUSPEND_MAX_DEFERRED_RAM, (long) CAP_MB);

    // Constructing the flush thread directly does NOT start the background thread; flushPagesFromQueueToDisk is
    // driven explicitly below so the test is deterministic.
    final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, cfg);

    final Database db = (Database) database;
    flush.setSuspended(db, true);

    // Enqueue 6 single-page batches: 4 fit under the 1 MB cap, the remaining 2 must stay in the queue.
    final int batches = 6;
    for (int i = 0; i < batches; i++) {
      final PageId pageId = new PageId(database, 9, i);
      final MutablePage page = new MutablePage(pageId, PAGE_SIZE, new byte[PAGE_SIZE], 0, 0);
      flush.pageIndex.put(pageId, page);
      flush.queue.offer(new PagesToFlush(List.of(page)));
    }

    // Drive the flush thread by hand. Each call either defers one batch (under cap) or, once the cap is hit,
    // returns without polling. A short timeout keeps the over-cap sleeps fast.
    for (int i = 0; i < batches + 4; i++)
      flush.flushPagesFromQueueToDisk(null, 20L);

    final long capBytes = (long) CAP_MB * 1024 * 1024;
    // The deferred backlog never exceeds the cap...
    assertThat(flush.deferredRAMBytes.get()).isLessThanOrEqualTo(capBytes);
    // ...and exactly the 4 pages that fit were deferred.
    assertThat(flush.deferredRAMBytes.get()).isEqualTo(4L * PAGE_SIZE);
    // ...leaving the remaining 2 batches stuck in the queue (this is the writer backpressure signal).
    assertThat(flush.queue).hasSize(2);
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
      flush.pageIndex.put(pageId, page);
      flush.queue.offer(new PagesToFlush(List.of(page)));
    }

    for (int i = 0; i < batches; i++)
      flush.flushPagesFromQueueToDisk(null, 20L);

    // With the cap disabled the whole queue drains into the deferred map regardless of size.
    assertThat(flush.queue).isEmpty();
    assertThat(flush.deferredRAMBytes.get()).isEqualTo((long) batches * PAGE_SIZE);
  }
}
