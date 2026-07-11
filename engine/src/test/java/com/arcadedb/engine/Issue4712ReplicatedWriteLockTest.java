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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4712: {@link TransactionManager#applyChanges} wrote a replicated/recovery page directly via
 * {@code file.write(modifiedPage)}, bypassing the per-page I/O interlock ({@code concurrentPageAccess}) that
 * {@link PageManager#flushPage} and the read path ({@code loadPage}) use to avoid partial reads/writes. A reader loading the same
 * page concurrently with a {@code forceApply} replay could therefore observe partially-written bytes.
 * <p>
 * The fix routes the write through {@link PageManager#writePageWithLock}, which acquires the same interlock. This test verifies the
 * write now blocks while the interlock is held by a (simulated) in-flight reader and completes only once it is released. With the
 * old direct {@code file.write} the write would complete immediately regardless of the interlock.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4712ReplicatedWriteLockTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("TestType");
  }

  @Test
  @SuppressWarnings("unchecked")
  void applyChangesRespectsPerPageIoLock() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("TestType").set("name", "record-" + i).save();
    });
    // Drain the async flush of the setup transaction: its in-flight I/O uses the same per-page interlock
    // (concurrentPageAccess) this test then manipulates, and the winner's finally-remove would otherwise
    // race the fake entry planted below / leave the stale setup page in the flush-queue index.
    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

    final int fileId = db.getSchema().getType("TestType").getBuckets(false).get(0).getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final int pageSize = file.getPageSize();
    final PageId pageId = new PageId(db, fileId, 0);
    final PageManager pageManager = db.getPageManager();

    // Pre-load the page into the read cache so applyChanges' internal version read is served from cache and does
    // not itself touch the interlock; the only contention point is then the replicated page write.
    final ImmutablePage page = pageManager.getImmutablePage(pageId, pageSize, false, true);
    final int baseVersion = (int) page.getVersion();

    // Full-content WAL page bumped by one version: this is the replicated/recovery write that applyChanges performs.
    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = fileId;
    walPage.pageNumber = 0;
    walPage.currentPageVersion = baseVersion + 1;
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
    walPage.currentPageSize = page.getContentSize();
    final byte[] content = new byte[11];
    System.arraycopy(page.getContent().array(), walPage.changesFrom, content, 0, content.length);
    walPage.currentContent = new Binary(content);

    final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
    walTx.txId = 47120;
    walTx.timestamp = System.currentTimeMillis();
    walTx.pages = new WALFile.WALPage[] { walPage };

    // Simulate a reader/flusher holding the per-page interlock: concurrentPageAccess registers the pageId in
    // pendingFlushPages for the whole I/O operation. While it is present, the replicated write inside applyChanges
    // must wait. With the old direct file.write it would proceed immediately (the bug), exposing partial bytes.
    final Field field = PageManager.class.getDeclaredField("pendingFlushPages");
    field.setAccessible(true);
    final ConcurrentMap<PageId, Boolean> pendingFlushPages = (ConcurrentMap<PageId, Boolean>) field.get(pageManager);
    pendingFlushPages.put(pageId, Boolean.FALSE); // FALSE = read access in flight

    final CountDownLatch applyDone = new CountDownLatch(1);
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final Thread applier = new Thread(() -> {
      try {
        db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false);
      } catch (final Throwable t) {
        error.set(t);
      } finally {
        applyDone.countDown();
      }
    }, "issue4712-applier");
    applier.start();

    try {
      // While the interlock is occupied applyChanges must NOT complete (the page write spins waiting for the lock).
      assertThat(applyDone.await(500, TimeUnit.MILLISECONDS)).isFalse();
    } finally {
      // Release the interlock: the write can now proceed.
      pendingFlushPages.remove(pageId);
    }

    assertThat(applyDone.await(5, TimeUnit.SECONDS)).isTrue();
    applier.join(5_000);
    assertThat(error.get()).isNull();

    // The new version was durably written and reloads correctly from disk.
    pageManager.removePageFromCache(pageId);
    final ImmutablePage reloaded = pageManager.getImmutablePage(pageId, pageSize, false, true);
    assertThat(reloaded.getVersion()).isEqualTo(baseVersion + 1);
  }

  @Test
  void applyChangesStillReplaysPageThroughLockedWrite() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("TestType").set("name", "record-" + i).save();
    });
    // Drain the async flush of the setup transaction: its in-flight I/O uses the same per-page interlock
    // (concurrentPageAccess) this test then manipulates, and the winner's finally-remove would otherwise
    // race the fake entry planted below / leave the stale setup page in the flush-queue index.
    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

    final int fileId = db.getSchema().getType("TestType").getBuckets(false).get(0).getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final int pageSize = file.getPageSize();
    final PageId pageId = new PageId(db, fileId, 0);

    final ImmutablePage page = db.getPageManager().getImmutablePage(pageId, pageSize, false, true);
    final int baseVersion = (int) page.getVersion();

    // Full-content WAL page so the apply is accepted, carrying a recognizable marker byte at the start of the content area.
    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = fileId;
    walPage.pageNumber = 0;
    walPage.currentPageVersion = baseVersion + 1;
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
    walPage.currentPageSize = page.getContentSize();
    final byte[] content = new byte[11];
    System.arraycopy(page.getContent().array(), walPage.changesFrom, content, 0, content.length);
    walPage.currentContent = new Binary(content);

    final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
    walTx.txId = 4712;
    walTx.timestamp = System.currentTimeMillis();
    walTx.pages = new WALFile.WALPage[] { walPage };

    final boolean changed = db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false);
    assertThat(changed).isTrue();

    db.getPageManager().removePageFromCache(pageId);
    final ImmutablePage reloaded = db.getPageManager().getImmutablePage(pageId, pageSize, false, true);
    assertThat(reloaded.getVersion()).isEqualTo(baseVersion + 1);
  }
}
