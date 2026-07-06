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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageManagerFlushThread.PagesToFlush;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the flush-robustness fixes:
 * <ul>
 * <li>#4928: a plain {@code IOException} from {@code flushPage} used to escape the per-page loop, aborting
 * the batch - the remaining pages were never flushed yet their {@code pageIndex} entries survived, so
 * {@code waitAllPagesOfDatabaseAreFlushed} spun forever and {@code close()}/rename/backup-suspend hung. The
 * failure is now contained per page (the page is WAL-recoverable) and the wait is bounded with a SEVERE
 * escalation.</li>
 * <li>#4930: after a {@code ClosedChannelException}, the reopen path used to blindly re-open with
 * {@code RandomAccessFile("rw")}, RE-CREATING a file that DDL had deleted (a one-page ghost re-registered by
 * the next open). It now refuses when the file was closed on purpose or no longer exists on disk.</li>
 * </ul>
 */
class FlushRobustnessTest extends TestHelper {

  @Test
  void ioExceptionOnOnePageDoesNotAbortTheBatchNorResurrectDroppedFiles() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    // Two sacrificial types: one whose file gets broken (channel closed underneath + OS file deleted,
    // simulating an interrupt-closed channel racing a DDL drop), one that stays healthy.
    db.getSchema().createDocumentType("BrokenDoc");
    db.getSchema().createDocumentType("HealthyDoc");
    db.transaction(() -> {
      db.newDocument("BrokenDoc").set("v", 1).save();
      db.newDocument("HealthyDoc").set("v", 1).save();
    });
    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

    final PaginatedComponent brokenBucket = (PaginatedComponent) db.getSchema().getType("BrokenDoc").getBuckets(false).getFirst();
    final PaginatedComponent healthyBucket = (PaginatedComponent) db.getSchema().getType("HealthyDoc").getBuckets(false)
        .getFirst();
    final PaginatedComponentFile brokenFile = (PaginatedComponentFile) db.getFileManager().getFile(brokenBucket.getFileId());
    final PaginatedComponentFile healthyFile = (PaginatedComponentFile) db.getFileManager().getFile(healthyBucket.getFileId());

    // Break the file the way an interrupt + concurrent drop would: the channel dies while the component
    // still believes it is open, and the OS file disappears.
    final Field channelField = PaginatedComponentFile.class.getDeclaredField("channel");
    channelField.setAccessible(true);
    ((FileChannel) channelField.get(brokenFile)).close();
    final File brokenOsFile = new File(brokenFile.getFilePath());
    assertThat(brokenOsFile.delete()).isTrue();

    // Standalone flush thread (not started): drive it by hand with a batch [brokenPage, healthyPage].
    final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, db.getConfiguration());
    final int pageSize = brokenBucket.getPageSize();
    final PageId brokenPageId = new PageId(db, brokenBucket.getFileId(), brokenBucket.getTotalPages());
    final MutablePage brokenPage = new MutablePage(brokenPageId, pageSize, new byte[pageSize], 0, 0);
    final int healthyPageNum = healthyBucket.getTotalPages();
    final PageId healthyPageId = new PageId(db, healthyBucket.getFileId(), healthyPageNum);
    final MutablePage healthyPage = new MutablePage(healthyPageId, pageSize, new byte[pageSize], 0, 0);
    flush.pageIndex.put(brokenPageId, brokenPage);
    flush.pageIndex.put(healthyPageId, healthyPage);
    flush.queue.offer(new PagesToFlush(List.of(brokenPage, healthyPage)));

    // #4928: the broken page's IOException must be contained: the batch continues, the healthy page reaches
    // the disk, and no entry leaks in pageIndex (a leak would hang the database close).
    flush.flushPagesFromQueueToDisk(null, 1_000L);

    final PaginatedComponentFile healthyOnDisk = (PaginatedComponentFile) db.getFileManager().getFile(healthyBucket.getFileId());
    // NOTE: capture the page number BEFORE the flush - flushPage bumps the component's page count, so
    // re-reading getTotalPages() here would inflate the expectation past what was written.
    assertThat(healthyOnDisk.getSize()).as("the healthy page of the batch must still be flushed (#4928)")
        .isGreaterThanOrEqualTo((long) (healthyPageNum + 1) * pageSize);
    assertThat(flush.pageIndex).as("no page may leak in the flush index (#4928)").isEmpty();

    // #4930: the reopen path must NOT have re-created the dropped file on disk.
    assertThat(brokenOsFile).as("a dropped file must not be resurrected by the channel-reopen path (#4930)")
        .doesNotExist();
  }

  @Test
  void waitAllPagesFlushedIsBoundedInsteadOfHangingForever() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    final PageManagerFlushThread flush = new PageManagerFlushThread(PageManager.INSTANCE, db.getConfiguration());
    // A pending entry that nothing will ever flush (the thread is never started): zero progress by design.
    final PageId stuckPageId = new PageId(db, 0, 3_000_000);
    flush.pageIndex.put(stuckPageId, new MutablePage(stuckPageId, 1024, new byte[1024], 0, 0));

    // The no-progress window is read per call from the database's own configuration.
    db.getConfiguration().setValue(GlobalConfiguration.FLUSH_ALL_PAGES_TIMEOUT, 300L);
    try {
      final long start = System.currentTimeMillis();
      final boolean flushed = flush.waitAllPagesOfDatabaseAreFlushed(db);
      final long elapsed = System.currentTimeMillis() - start;
      assertThat(flushed).as("the wait must report that it gave up (#4928)").isFalse();
      assertThat(elapsed)
          .as("the wait must give up loudly after the no-progress window instead of spinning forever (#4928)")
          .isBetween(250L, 10_000L);
    } finally {
      db.getConfiguration().setValue(GlobalConfiguration.FLUSH_ALL_PAGES_TIMEOUT, 60_000L);
      flush.pageIndex.remove(stuckPageId);
    }
  }

  @Test
  void closeWithUnflushablePagesPreservesWalAndRecoversOnNextOpen() throws Exception {
    // Dedicated throwaway database: this test wedges ITS close, which must not disturb the shared test db.
    final String dbPath = database.getDatabasePath() + "_walpreserve";
    final com.arcadedb.database.DatabaseFactory factory = new com.arcadedb.database.DatabaseFactory(dbPath);
    if (factory.exists())
      factory.open().drop();

    PageId stuckPageId = null;
    try {
      final DatabaseInternal db = (DatabaseInternal) factory.create();
      db.getSchema().createDocumentType("Doc");
      db.transaction(() -> db.newDocument("Doc").set("v", 42).save());

      // Wedge the close: a pending page nothing will ever flush, and a short no-progress window.
      db.getConfiguration().setValue(GlobalConfiguration.FLUSH_ALL_PAGES_TIMEOUT, 300L);
      stuckPageId = new PageId(db, 0, 3_000_000);
      PageManager.INSTANCE.getFlushThread().pageIndex
          .put(stuckPageId, new MutablePage(stuckPageId, 1024, new byte[1024], 0, 0));

      // #4928: the close must COMPLETE (bounded wait), and because pages could not be flushed it must be
      // crash-equivalent: WAL files and lock file preserved so the next open recovers.
      db.close();

      final File dbDir = new File(dbPath);
      assertThat(dbDir.listFiles((d, n) -> n.endsWith(".wal")))
          .as("the WAL protecting the unflushed pages must NOT be deleted by a give-up close (#4928)")
          .isNotEmpty();
      assertThat(new File(dbDir, "database.lck"))
          .as("the lock file must be preserved as the unclean-shutdown marker (#4928)").exists();

      // The give-up close must also have purged the stuck entries from the shared flush thread's index:
      // they reference a now-closed database and could never be flushed (their content is in the preserved WAL).
      assertThat(PageManager.INSTANCE.getFlushThread().pageIndex.containsKey(stuckPageId))
          .as("a give-up close must purge its stuck pageIndex entries (no JVM-wide leak)").isFalse();
      stuckPageId = null;

      // The next open must run recovery and the data must be there; its clean close then drops the WAL.
      final DatabaseInternal reopened = (DatabaseInternal) factory.open();
      assertThat(reopened.query("sql", "SELECT v FROM Doc").next().<Integer>getProperty("v")).isEqualTo(42);
      reopened.close();
      assertThat(new File(dbPath).listFiles((d, n) -> n.endsWith(".wal")))
          .as("a healthy close must delete the WAL as before").isNullOrEmpty();
    } finally {
      if (stuckPageId != null)
        PageManager.INSTANCE.getFlushThread().pageIndex.remove(stuckPageId);
      if (factory.exists())
        factory.open().drop();
      factory.close();
    }
  }

  @Test
  void cleanCloseAfterContainedFlushFailurePreservesWalViaAckGate() throws Exception {
    // The reviewer's scenario: a page's flush fails and is contained (#4928) - it leaves the flush pipeline
    // WITHOUT its WAL ack, so pageIndex is empty and a later CLEAN close believes everything was flushed.
    // Deleting the WAL then would silently lose the committed change. The close-time ack gate must catch it.
    final String dbPath = database.getDatabasePath() + "_ackgate";
    final com.arcadedb.database.DatabaseFactory factory = new com.arcadedb.database.DatabaseFactory(dbPath);
    if (factory.exists())
      factory.open().drop();

    try {
      final DatabaseInternal db = (DatabaseInternal) factory.create();
      db.getSchema().createDocumentType("Doc");
      db.transaction(() -> db.newDocument("Doc").set("v", 7).save());
      db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

      // Simulate the contained failure's ack state: one WAL page acknowledged to the committer but never
      // flushed (notifyPageFlushed never called). Reflection on the live WAL pool mirrors the counter state
      // the real IOException containment leaves behind.
      final java.lang.reflect.Field poolField = TransactionManager.class.getDeclaredField("activeWALFilePool");
      poolField.setAccessible(true);
      final WALFile[] pool = (WALFile[]) poolField.get(db.getTransactionManager());
      final java.lang.reflect.Field pendingField = WALFile.class.getDeclaredField("pagesToFlush");
      pendingField.setAccessible(true);
      final java.util.concurrent.atomic.AtomicInteger pending =
          (java.util.concurrent.atomic.AtomicInteger) pendingField.get(pool[0]);
      pending.incrementAndGet();

      // The pipeline is empty, so the flush wait succeeds - but the ack gate must still preserve the WAL.
      db.close();

      final File dbDir = new File(dbPath);
      assertThat(dbDir.listFiles((d, n) -> n.endsWith(".wal")))
          .as("a clean close must NOT delete a WAL that still holds unacked pages (#4928 ack gate)")
          .isNotEmpty();
      assertThat(new File(dbDir, "database.lck"))
          .as("the lock file must be preserved so the next open recovers").exists();

      // Recovery replays (idempotent here), and the following genuinely-clean close deletes the WAL.
      final DatabaseInternal reopened = (DatabaseInternal) factory.open();
      assertThat(reopened.query("sql", "SELECT v FROM Doc").next().<Integer>getProperty("v")).isEqualTo(7);
      reopened.close();
      assertThat(new File(dbPath).listFiles((d, n) -> n.endsWith(".wal")))
          .as("a genuinely clean close still deletes the WAL").isNullOrEmpty();
    } finally {
      if (factory.exists())
        factory.open().drop();
      factory.close();
    }
  }

  @Test
  void walAckIsReleasedExactlyOncePerPage() throws Exception {
    // #4928 review: the flush success path, the file-dropped flush branch and the dropped-file batch purge
    // can race on the SAME page (the flush loop does not remove pages from the batch list). A double
    // notifyPageFlushed would steal another page's pending ack, letting the close-time ack gate delete a
    // WAL that still protects unflushed committed data. takeWALFile() must make the release exactly-once.
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getSchema().createDocumentType("AckDoc");
    db.transaction(() -> db.newDocument("AckDoc").set("v", 1).save());

    final Field poolField = TransactionManager.class.getDeclaredField("activeWALFilePool");
    poolField.setAccessible(true);
    final WALFile wal = ((WALFile[]) poolField.get(db.getTransactionManager()))[0];

    final PageId pageId = new PageId(db, 0, 4_000_000);
    final MutablePage page = new MutablePage(pageId, 1024, new byte[1024], 0, 0);
    page.setWALFile(wal);

    final int before = wal.getPendingPagesToFlush();
    db.getTransactionManager().notifyPageFlushed(page); // first ack: releases
    db.getTransactionManager().notifyPageFlushed(page); // racing second ack: must be a no-op
    assertThat(page.takeWALFile()).as("the WAL reference must have been taken by the first ack").isNull();
    assertThat(wal.getPendingPagesToFlush())
        .as("the pending count must decrease exactly once no matter how many paths ack the page")
        .isEqualTo(before - 1);

    // Restore the counter so this synthetic ack does not skew the shared database's close-time gate.
    final Field pendingField = WALFile.class.getDeclaredField("pagesToFlush");
    pendingField.setAccessible(true);
    ((java.util.concurrent.atomic.AtomicInteger) pendingField.get(wal)).incrementAndGet();
  }
}
