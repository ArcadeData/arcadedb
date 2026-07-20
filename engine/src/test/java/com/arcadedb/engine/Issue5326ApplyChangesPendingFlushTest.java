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

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * White-box regression test for the interaction between {@link TransactionManager#applyChanges} and the asynchronous
 * flush pipeline.
 * <p>
 * A committed page is published to the read cache and to the flush thread's {@code pageIndex} before it reaches the
 * disk. {@code applyChanges} (replicated/recovery replay) writes the page straight to the file, bypassing the queue.
 * While the older copy stayed in the pipeline two things went wrong:
 * <ul>
 *   <li>{@code PageManager.loadPage} resolves a page from the flush queue BEFORE reading the file, so every read after
 *   the replicated write kept returning the superseded version.</li>
 *   <li>when the flush thread eventually wrote the queued copy, it overwrote the replicated page on disk, rolling the
 *   page version backwards and re-opening the version-gap cascade the replay is meant to close.</li>
 * </ul>
 * Both outcomes depend on whether the flush thread happens to drain the queue before the read, which is what made the
 * {@code applyChanges} regression tests fail intermittently on loaded CI machines (issue #5326).
 * <p>
 * {@code applyChanges} now pushes any pending copy of the page to disk and detaches it from the pipeline before
 * applying the WAL delta, so the outcome no longer depends on flush timing.
 */
class Issue5326ApplyChangesPendingFlushTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("TestType");
  }

  @Test
  void applyChangesOverridesAPageStillPendingInTheFlushPipeline() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("TestType").set("name", "record-" + i).save();
    });

    final int fileId = db.getSchema().getType("TestType").getBuckets(false).getFirst().getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final int pageSize = file.getPageSize();
    final PageId pageId = new PageId(db, fileId, 0);
    final PageManager pageManager = db.getPageManager();

    // Start from a quiet pipeline so the only pending entry is the one this test publishes below.
    assertThat(pageManager.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();

    final ImmutablePage page = pageManager.getImmutablePage(pageId, pageSize, false, true);
    final int baseVersion = (int) page.getVersion();

    // Reproduce the state a page is in between commit and flush: published in the flush thread's index (so reads
    // resolve it from there) while the disk still holds the same version.
    final MutablePage pending = page.modify();
    pageManager.getFlushThread().pageIndex.put(pageId, pending);

    // Replicated/recovery WAL entry bumping the page by one version.
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
    walTx.txId = 5326;
    walTx.timestamp = System.currentTimeMillis();
    walTx.pages = new WALFile.WALPage[] { walPage };

    assertThat(db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false)).isTrue();

    // No stale copy is left behind that a later flush would write over the replicated page.
    assertThat(pageManager.getFlushThread().pageIndex.get(pageId)).isNull();

    // The replicated version is what every subsequent read observes, cache-hit or reload from disk.
    assertThat(pageManager.getImmutablePage(pageId, pageSize, false, true).getVersion()).isEqualTo(baseVersion + 1);

    pageManager.removePageFromCache(pageId);
    final ImmutablePage reloaded = pageManager.getImmutablePage(pageId, pageSize, false, true);
    assertThat(reloaded.getVersion()).isEqualTo(baseVersion + 1);
  }

  /**
   * Same defect, but driven through the real flush pipeline instead of a fabricated index entry: the page is scheduled
   * by an ordinary commit while flushing is suspended, so it sits in a genuine deferred batch. This covers the batch
   * removal and the deferred-RAM accounting (#4728) that the white-box case above cannot reach, and it verifies the
   * durability half of the fix: once flushing resumes, the deferred batch must no longer carry a copy able to write
   * the superseded version over the replicated one.
   */
  @Test
  void applyChangesTakesTheDeferredBatchCopyOutOfThePipeline() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("TestType").set("name", "record-" + i).save();
    });

    final int fileId = db.getSchema().getType("TestType").getBuckets(false).getFirst().getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final int pageSize = file.getPageSize();
    final PageId pageId = new PageId(db, fileId, 0);
    final PageManager pageManager = db.getPageManager();
    final PageManagerFlushThread flushThread = pageManager.getFlushThread();

    assertThat(pageManager.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();

    final int replicatedVersion;

    // Suspend flushing (what a backup or an HA snapshot ship does) so the next commit's pages are parked in a
    // deferred batch instead of reaching the disk.
    flushThread.setSuspended(db, true);
    try {
      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          db.newDocument("TestType").set("name", "deferred-" + i).save();
      });

      // Wait for the flush thread to move the committed batch into the deferred backlog. The deadline only bounds a
      // hang: the flush thread polls its queue on a 1s timeout, so 60s is orders of magnitude above the worst-case
      // deferral latency even on the loaded runners that made #5326 visible in the first place.
      final long deadline = System.currentTimeMillis() + 60_000;
      while (flushThread.deferredRAMBytes.get() == 0 && System.currentTimeMillis() < deadline)
        Thread.sleep(10);

      final MutablePage pending = flushThread.pageIndex.get(pageId);
      assertThat(pending).as("the committed page must still be pending in the flush pipeline").isNotNull();
      assertThat(flushThread.deferredRAMBytes.get()).isPositive();

      final long deferredBefore = flushThread.deferredRAMBytes.get();
      final long pendingSize = pending.getPhysicalSize();
      final int baseVersion = (int) pending.getVersion();
      replicatedVersion = baseVersion + 1;

      final WALFile.WALPage walPage = new WALFile.WALPage();
      walPage.fileId = fileId;
      walPage.pageNumber = 0;
      walPage.currentPageVersion = baseVersion + 1;
      walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
      walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
      walPage.currentPageSize = pending.getContentSize();
      final byte[] content = new byte[11];
      System.arraycopy(pending.getContent().array(), walPage.changesFrom, content, 0, content.length);
      walPage.currentContent = new Binary(content);

      final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
      walTx.txId = 53260;
      walTx.timestamp = System.currentTimeMillis();
      walTx.pages = new WALFile.WALPage[] { walPage };

      assertThat(db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false)).isTrue();

      assertThat(flushThread.pageIndex.get(pageId)).isNull();
      assertThat(flushThread.deferredRAMBytes.get()).isEqualTo(deferredBefore - pendingSize);
    } finally {
      // Resuming writes every batch still deferred: none of them may carry the superseded copy of this page.
      flushThread.setSuspended(db, false);
    }

    assertThat(pageManager.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();
    pageManager.removePageFromCache(pageId);
    final ImmutablePage reloaded = pageManager.getImmutablePage(pageId, pageSize, false, true);
    assertThat(reloaded.getVersion()).isEqualTo(replicatedVersion);
  }

  /**
   * Two commits of the same page while flushing is suspended leave TWO pending copies: the newest in
   * {@code pageIndex} and the older one still in the first batch (the two-instance case
   * {@link PageManagerFlushThread#removeFromFlushIndex} exists for, issue #4544). Detaching only the indexed copy
   * would leave the older one free to write the superseded version over the replicated one once flushing resumes,
   * so every copy has to leave the pipeline.
   */
  @Test
  void applyChangesTakesEveryPendingCopyOfThePageOutOfThePipeline() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.transaction(() -> db.newDocument("TestType").set("name", "seed").save());

    final int fileId = db.getSchema().getType("TestType").getBuckets(false).getFirst().getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final int pageSize = file.getPageSize();
    final PageId pageId = new PageId(db, fileId, 0);
    final PageManager pageManager = db.getPageManager();
    final PageManagerFlushThread flushThread = pageManager.getFlushThread();

    assertThat(pageManager.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();

    final int replicatedVersion;

    flushThread.setSuspended(db, true);
    try {
      // Two separate commits: two batches, each carrying its own MutablePage for page 0.
      for (int tx = 0; tx < 2; tx++) {
        final int round = tx;
        db.transaction(() -> {
          for (int i = 0; i < 5; i++)
            db.newDocument("TestType").set("name", "round-" + round + "-" + i).save();
        });
      }

      final long deadline = System.currentTimeMillis() + 60_000;
      while (flushThread.deferredRAMBytes.get() == 0 && System.currentTimeMillis() < deadline)
        Thread.sleep(10);

      final MutablePage newest = flushThread.pageIndex.get(pageId);
      assertThat(newest).as("the twice-committed page must still be pending in the flush pipeline").isNotNull();

      final int baseVersion = (int) newest.getVersion();
      assertThat(baseVersion).as("both commits must have touched page 0, leaving two pending copies").isGreaterThan(1);
      replicatedVersion = baseVersion + 1;

      final WALFile.WALPage walPage = new WALFile.WALPage();
      walPage.fileId = fileId;
      walPage.pageNumber = 0;
      walPage.currentPageVersion = replicatedVersion;
      walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
      walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
      walPage.currentPageSize = newest.getContentSize();
      final byte[] content = new byte[11];
      System.arraycopy(newest.getContent().array(), walPage.changesFrom, content, 0, content.length);
      walPage.currentContent = new Binary(content);

      final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
      walTx.txId = 53261;
      walTx.timestamp = System.currentTimeMillis();
      walTx.pages = new WALFile.WALPage[] { walPage };

      final long deferredBefore = flushThread.deferredRAMBytes.get();

      assertThat(db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false)).isTrue();
      assertThat(flushThread.pageIndex.get(pageId)).isNull();

      // Both copies left the deferred backlog, not just the indexed one.
      assertThat(deferredBefore - flushThread.deferredRAMBytes.get())
          .as("every pending copy of the page must be detached, not only the one in pageIndex")
          .isEqualTo(2L * newest.getPhysicalSize());
    } finally {
      flushThread.setSuspended(db, false);
    }

    // Resuming writes every batch still deferred. If the older copy had survived the detach it would land here and
    // roll the page back below the replicated version.
    assertThat(pageManager.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();
    pageManager.removePageFromCache(pageId);
    final ImmutablePage reloaded = pageManager.getImmutablePage(pageId, pageSize, false, true);
    assertThat(reloaded.getVersion()).isEqualTo(replicatedVersion);
  }
}
