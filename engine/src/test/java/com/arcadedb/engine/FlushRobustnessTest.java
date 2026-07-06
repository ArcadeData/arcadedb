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
    // A pending entry that nothing will ever flush (the thread is never started).
    final PageId stuckPageId = new PageId(db, 0, 3_000_000);
    flush.pageIndex.put(stuckPageId, new MutablePage(stuckPageId, 1024, new byte[1024], 0, 0));

    final long originalTimeout = PageManagerFlushThread.WAIT_FLUSH_TIMEOUT_MS;
    PageManagerFlushThread.WAIT_FLUSH_TIMEOUT_MS = 500;
    try {
      final long start = System.currentTimeMillis();
      flush.waitAllPagesOfDatabaseAreFlushed(db);
      final long elapsed = System.currentTimeMillis() - start;
      assertThat(elapsed)
          .as("the wait must give up loudly after the deadline instead of spinning forever (#4928)")
          .isBetween(400L, 10_000L);
    } finally {
      PageManagerFlushThread.WAIT_FLUSH_TIMEOUT_MS = originalTimeout;
      flush.pageIndex.remove(stuckPageId);
    }
  }
}
