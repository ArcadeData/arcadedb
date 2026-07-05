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
import com.arcadedb.engine.PageManagerFlushThread.PagesToFlush;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Follow-up to the #4924/#4938 "interrupt must throw, never silently skip I/O" fixes: the durability gap of the
 * flush thread's OWN interruption. Once {@code concurrentPageAccess} fails loud on interrupt, an interrupted
 * {@link PageManagerFlushThread} would have dropped the page it was flushing (batch aborted, page removed from
 * {@code pageIndex} without reaching disk) and left the rest of the batch leaked in {@code pageIndex}. The policy
 * under test: an interrupt of the flush thread is a shutdown request, never permission to drop a dirty page - the
 * page whose WAL entry was already acked to the committer is still written, the queue is drained, then the thread
 * exits. Same policy for the deferred-batch flush in {@code setSuspended(false)}, which runs on the caller's
 * (backup/HA) thread: retry once with the flag cleared, then restore the flag for the caller.
 */
class PageManagerFlushThreadInterruptTest extends TestHelper {

  @Test
  void flushThreadInterruptStillWritesPageAndShutsDownCleanly() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.getSchema().createDocumentType("FlushDoc");
    db.transaction(() -> db.newDocument("FlushDoc").set("v", 1).save());
    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

    final PaginatedComponent bucket = (PaginatedComponent) db.getSchema().getType("FlushDoc").getBuckets(false).getFirst();
    final int fileId = bucket.getFileId();
    final int pageSize = bucket.getPageSize();
    final int pageNum = bucket.getTotalPages();

    // A dedicated (unstarted until below) flush thread with its own queue, so the database's real flush thread
    // is not involved and the test is deterministic.
    final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, db.getConfiguration());

    final PageId pageId = new PageId(db, fileId, pageNum);
    final MutablePage page = new MutablePage(pageId, pageSize, new byte[pageSize], 0, 0);
    flush.pageIndex.put(pageId, page);
    flush.queue.offer(new PagesToFlush(List.of(page)));

    // Occupy the page's I/O slot so the flush thread spins inside concurrentPageAccess (where the interrupt
    // check lives) until the slot is released, making the interrupt timing deterministic. NOTE: pendingFlushPages
    // lives on the shared PageManager.INSTANCE singleton; isolation relies on the page number being beyond any
    // page the database's real flush thread could touch.
    final Field pendingField = PageManager.class.getDeclaredField("pendingFlushPages");
    pendingField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final ConcurrentMap<PageId, Boolean> pending = (ConcurrentMap<PageId, Boolean>) pendingField.get(PageManager.INSTANCE);
    pending.put(pageId, true);
    try {
      flush.start();

      // Wait until the batch has been polled, then interrupt the flush thread. Wherever the interrupt lands
      // (before flushPage or inside the slot spin), concurrentPageAccess detects it and the policy consumes it.
      while (!flush.queue.isEmpty())
        Thread.sleep(1);
      flush.interrupt();
      // Wait on observable state instead of a fixed sleep: the policy CLEARS the flag when it handles the fault.
      awaitInterruptConsumed(flush);
    } finally {
      // Release the I/O slot: the retried write can now reach the disk.
      pending.remove(pageId);
    }

    flush.join(10_000);
    assertThat(flush.isAlive()).as("interrupted flush thread must shut down after draining").isFalse();

    // The dirty page must have reached the disk despite the interrupt: its WAL entry was already acked, so
    // dropping it would silently lose it for readers once the cache evicts it.
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    assertThat(file.getSize()).as("interrupted flush must still write the page before shutting down")
        .isGreaterThanOrEqualTo((long) (pageNum + 1) * pageSize);
    assertThat(flush.pageIndex).as("no page may leak in the flush index").isEmpty();
  }

  @Test
  void doubleInterruptDoesNotAbortTheBatch() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.getSchema().createDocumentType("FlushDoc");
    db.transaction(() -> db.newDocument("FlushDoc").set("v", 1).save());
    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

    final PaginatedComponent bucket = (PaginatedComponent) db.getSchema().getType("FlushDoc").getBuckets(false).getFirst();
    final int fileId = bucket.getFileId();
    final int pageSize = bucket.getPageSize();
    final int pageNum = bucket.getTotalPages();

    final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, db.getConfiguration());

    // Batch of two pages. The FIRST page's I/O slot is held busy so its flush (and the interrupt-policy retry)
    // spins; a SECOND interrupt during the retry breaks the spin and double-faults the page. The batch must NOT
    // be aborted: the second page must still reach the disk and no page may leak in pageIndex (a leak hangs the
    // database close in waitAllPagesOfDatabaseAreFlushed).
    final PageId blockedPageId = new PageId(db, fileId, pageNum + 1);
    final MutablePage blockedPage = new MutablePage(blockedPageId, pageSize, new byte[pageSize], 0, 0);
    final PageId freePageId = new PageId(db, fileId, pageNum);
    final MutablePage freePage = new MutablePage(freePageId, pageSize, new byte[pageSize], 0, 0);
    flush.pageIndex.put(blockedPageId, blockedPage);
    flush.pageIndex.put(freePageId, freePage);
    flush.queue.offer(new PagesToFlush(List.of(blockedPage, freePage)));

    final Field pendingField = PageManager.class.getDeclaredField("pendingFlushPages");
    pendingField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final ConcurrentMap<PageId, Boolean> pending = (ConcurrentMap<PageId, Boolean>) pendingField.get(PageManager.INSTANCE);
    pending.put(blockedPageId, true);
    try {
      flush.start();

      while (!flush.queue.isEmpty())
        Thread.sleep(1);
      flush.interrupt(); // 1st: InterruptedIOException -> policy clears the flag and retries (spins on the busy slot)
      // Wait on observable state instead of fixed sleeps: the policy CLEARS the flag when it handles each fault,
      // so a consumed flag proves the previous interrupt was processed and the next one cannot merge with it.
      awaitInterruptConsumed(flush);
      flush.interrupt(); // 2nd: re-detected by the retry's slot spin -> second InterruptedIOException, contained per-page
      awaitInterruptConsumed(flush);
    } finally {
      pending.remove(blockedPageId);
    }

    flush.join(10_000);
    assertThat(flush.isAlive()).as("flush thread must shut down after draining").isFalse();

    // The second page of the batch must have been flushed regardless of the first page's double fault.
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    assertThat(file.getSize()).as("the rest of the batch must still be flushed after a double fault")
        .isGreaterThanOrEqualTo((long) (pageNum + 1) * pageSize);
    assertThat(flush.pageIndex).as("no page may leak in the flush index (a leak hangs the database close)").isEmpty();
  }

  /**
   * Waits (bounded) until the thread's interrupt flag has been consumed by the interrupt policy, the
   * observable proof that the fault was handled. On timeout the test proceeds and its assertions fail with
   * a meaningful message instead of hanging here.
   */
  private static void awaitInterruptConsumed(final Thread thread) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 5_000;
    while (thread.isInterrupted() && System.currentTimeMillis() < deadline)
      Thread.sleep(1);
  }

  @Test
  void unsuspendFlushOfDeferredPagesSurvivesCallerInterrupt() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.getSchema().createDocumentType("FlushDoc");
    db.transaction(() -> db.newDocument("FlushDoc").set("v", 1).save());
    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

    final PaginatedComponent bucket = (PaginatedComponent) db.getSchema().getType("FlushDoc").getBuckets(false).getFirst();
    final int fileId = bucket.getFileId();
    final int pageSize = bucket.getPageSize();
    final int pageNum = bucket.getTotalPages();

    final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, db.getConfiguration());
    flush.setSuspended(db, true);

    final PageId pageId = new PageId(db, fileId, pageNum);
    final MutablePage page = new MutablePage(pageId, pageSize, new byte[pageSize], 0, 0);
    flush.pageIndex.put(pageId, page);
    flush.queue.offer(new PagesToFlush(List.of(page)));

    // Drive the (unstarted) flush thread by hand: with the database suspended, the batch is deferred.
    flush.flushPagesFromQueueToDisk(null, 100L);
    assertThat(flush.deferredRAMBytes.get()).isEqualTo(pageSize);

    // Unsuspend with the caller's interrupt flag set: the synchronous phase-1 flush of the deferred page must
    // not drop it, and the flag must be preserved for the caller's own cancellation logic.
    Thread.currentThread().interrupt();
    try {
      flush.setSuspended(db, false);
      assertThat(Thread.currentThread().isInterrupted()).as("caller interrupt must be preserved").isTrue();
    } finally {
      Thread.interrupted();
    }

    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    assertThat(file.getSize()).as("deferred page must still be written despite the caller's interrupt")
        .isGreaterThanOrEqualTo((long) (pageNum + 1) * pageSize);
    assertThat(flush.pageIndex).as("no page may leak in the flush index").isEmpty();
  }
}
