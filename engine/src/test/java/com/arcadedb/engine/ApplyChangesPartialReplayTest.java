/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.exception.WALVersionGapException;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for partial WAL replay in {@link TransactionManager#applyChanges}.
 * <p>
 * Verifies that when a multi-page WAL transaction is replayed and one page is already
 * at the target version (e.g., from a previous partial apply interrupted by a crash),
 * the remaining pages in the same transaction are still applied. Previously, the first
 * already-applied page would throw ConcurrentModificationException and abort the entire
 * loop, leaving un-applied pages behind. This caused cascading WALVersionGapException
 * errors on followers under heavy load (version gap spreading).
 */
class ApplyChangesPartialReplayTest extends TestHelper {
  /**
   * These tests hand-write WAL pages over the bucket, so the records the cached counter was taken from stop existing
   * without anything booking a delta for them. The blanket end-of-test check would report that as the #8040 finding
   * it technically is - about the fixture, not about the replay under test.
   */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("TestType");
  }

  @Test
  void partialReplayAppliesRemainingPages() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    // Insert records to ensure we have pages with known versions
    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("TestType").set("name", "record-" + i).save();
    });

    // Get the bucket's file ID
    final int fileId = db.getSchema().getType("TestType").getBuckets(false).get(0).getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final int pageSize = file.getPageSize();

    // Read page 0's current state
    final PageId pageId0 = new PageId(db, fileId, 0);
    final ImmutablePage page0 = db.getPageManager().getImmutablePage(pageId0, pageSize, false, true);
    final int page0Version = (int) page0.getVersion();

    // Build a WAL transaction with 1 page: page 0 at version+1
    final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
    walTx.txId = 99999;
    walTx.timestamp = System.currentTimeMillis();

    final WALFile.WALPage walPage0 = new WALFile.WALPage();
    walPage0.fileId = fileId;
    walPage0.pageNumber = 0;
    walPage0.currentPageVersion = page0Version + 1;
    walPage0.changesFrom = BasePage.PAGE_HEADER_SIZE; // start of content area
    walPage0.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
    walPage0.currentPageSize = page0.getContentSize();
    // Create content bytes for the modified range
    final byte[] content0 = new byte[11]; // changesTo - changesFrom + 1
    System.arraycopy(page0.getContent().array(), walPage0.changesFrom, content0, 0, content0.length);
    walPage0.currentContent = new Binary(content0);

    walTx.pages = new WALFile.WALPage[] { walPage0 };

    // Apply the WAL transaction - this should succeed (version matches)
    final boolean changed = db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false);
    assertThat(changed).isTrue();

    // Verify page 0 is now at the new version
    db.getPageManager().removePageFromCache(pageId0);
    final ImmutablePage page0After = db.getPageManager().getImmutablePage(pageId0, pageSize, false, true);
    assertThat(page0After.getVersion()).isEqualTo(page0Version + 1);

    // Now build a 2-page WAL transaction where page 0 is ALREADY APPLIED (version+1)
    // but a hypothetical page 1 needs applying. We simulate this by re-submitting
    // page 0 at the same target version (already applied) alongside a fresh page.

    // First, ensure page 0 has data at version+1 (already done above)
    // Re-read page 0 to get its current state
    final ImmutablePage page0Current = db.getPageManager().getImmutablePage(pageId0, pageSize, false, true);
    final int page0CurrentVersion = (int) page0Current.getVersion();

    // Build page 0 entry with currentPageVersion = page0CurrentVersion (already at this version)
    final WALFile.WALPage replayPage0 = new WALFile.WALPage();
    replayPage0.fileId = fileId;
    replayPage0.pageNumber = 0;
    replayPage0.currentPageVersion = page0CurrentVersion; // <= current version, triggers skip
    replayPage0.changesFrom = BasePage.PAGE_HEADER_SIZE;
    replayPage0.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
    replayPage0.currentPageSize = page0Current.getContentSize();
    final byte[] replayContent0 = new byte[11];
    System.arraycopy(page0Current.getContent().array(), replayPage0.changesFrom, replayContent0, 0, replayContent0.length);
    replayPage0.currentContent = new Binary(replayContent0);

    // Build page 0 entry with currentPageVersion = page0CurrentVersion + 1 (needs applying)
    final WALFile.WALPage freshPage0 = new WALFile.WALPage();
    freshPage0.fileId = fileId;
    freshPage0.pageNumber = 0;
    freshPage0.currentPageVersion = page0CurrentVersion + 1; // version matches, should apply
    freshPage0.changesFrom = BasePage.PAGE_HEADER_SIZE;
    freshPage0.changesTo = BasePage.PAGE_HEADER_SIZE + 5;
    freshPage0.currentPageSize = page0Current.getContentSize();
    final byte[] freshContent = new byte[6];
    // Write a marker byte to verify the page was updated
    freshContent[0] = (byte) 0xAB;
    freshPage0.currentContent = new Binary(freshContent);

    // Build WAL tx with: first page already-applied (skip), second page needs applying
    final WALFile.WALTransaction replayTx = new WALFile.WALTransaction();
    replayTx.txId = 100000;
    replayTx.timestamp = System.currentTimeMillis();
    replayTx.pages = new WALFile.WALPage[] { replayPage0, freshPage0 };

    // With the fix: replayPage0 is skipped (already applied), freshPage0 is applied
    // Without the fix: replayPage0 throws CME, freshPage0 is never reached
    db.getPageManager().removePageFromCache(pageId0);
    final boolean changed2 = db.getTransactionManager().applyChanges(replayTx, Collections.emptyMap(), false);
    assertThat(changed2).isTrue();

    // Verify the fresh page update was applied (version advanced)
    db.getPageManager().removePageFromCache(pageId0);
    final ImmutablePage page0Final = db.getPageManager().getImmutablePage(pageId0, pageSize, false, true);
    assertThat(page0Final.getVersion()).isEqualTo(page0CurrentVersion + 1);
  }

  // #4510: forceApply (negative txId) must not write a partial delta on top of a stale baseline during HA replication
  // replay. Over a version gap only a full-page payload (what the compaction replicator ships) is honoured; a partial
  // delta is rejected so the follower recovers via a full snapshot instead of corrupting the page.
  @Test
  void forceApplyPartialDeltaOverVersionGapIsRejected() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("TestType").set("name", "record-" + i).save();
    });

    final int fileId = db.getSchema().getType("TestType").getBuckets(false).get(0).getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final int pageSize = file.getPageSize();

    final PageId pageId0 = new PageId(db, fileId, 0);
    db.getPageManager().removePageFromCache(pageId0);
    final ImmutablePage page0 = db.getPageManager().getImmutablePage(pageId0, pageSize, false, true);
    final int baseVersion = (int) page0.getVersion();

    // Snapshot a byte that lives OUTSIDE the partial delta range so we can prove it is not corrupted.
    final int probeOffset = BasePage.PAGE_HEADER_SIZE + 40; // far past the 6-byte delta below
    final byte probeBefore = page0.getContent().array()[probeOffset];

    // Build a forceApply WAL transaction with a PARTIAL delta and a version gap (K=3 > 1).
    final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
    walTx.txId = -1; // negative txId == forceApply (compaction page replication)
    walTx.forceApply = true;
    walTx.timestamp = System.currentTimeMillis();

    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = fileId;
    walPage.pageNumber = 0;
    walPage.currentPageVersion = baseVersion + 3; // gap > 1
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 5; // only 6 bytes -> partial delta
    walPage.currentPageSize = page0.getContentSize();
    final byte[] partialContent = new byte[6];
    for (int i = 0; i < partialContent.length; i++)
      partialContent[i] = (byte) 0xEE; // bogus bytes the leader never had at this baseline
    walPage.currentContent = new Binary(partialContent);

    walTx.pages = new WALFile.WALPage[] { walPage };

    // The partial delta over a stale baseline must be refused (would otherwise silently corrupt).
    assertThatThrownBy(() -> db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false))
        .isInstanceOf(WALVersionGapException.class);

    // The page must be untouched: original version and the probe byte outside the delta range.
    db.getPageManager().removePageFromCache(pageId0);
    final ImmutablePage page0After = db.getPageManager().getImmutablePage(pageId0, pageSize, false, true);
    assertThat(page0After.getVersion()).isEqualTo(baseVersion);
    assertThat(page0After.getContent().array()[probeOffset]).isEqualTo(probeBefore);

    // With ignoreErrors=true the partial forceApply is skipped (no change), again leaving the page intact.
    db.getPageManager().removePageFromCache(pageId0);
    final boolean changed = db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), true);
    assertThat(changed).isFalse();
    db.getPageManager().removePageFromCache(pageId0);
    assertThat(db.getPageManager().getImmutablePage(pageId0, pageSize, false, true).getVersion()).isEqualTo(baseVersion);
  }

  @Test
  void forceApplyFullPageOverVersionGapIsApplied() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("TestType").set("name", "record-" + i).save();
    });

    final int fileId = db.getSchema().getType("TestType").getBuckets(false).get(0).getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final int pageSize = file.getPageSize();

    final PageId pageId0 = new PageId(db, fileId, 0);
    db.getPageManager().removePageFromCache(pageId0);
    final ImmutablePage page0 = db.getPageManager().getImmutablePage(pageId0, pageSize, false, true);
    final int baseVersion = (int) page0.getVersion();

    // Build a forceApply WAL transaction that ships the FULL content region (what compaction sends).
    final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
    walTx.txId = -1;
    walTx.forceApply = true;
    walTx.timestamp = System.currentTimeMillis();

    final int deltaSize = pageSize - BasePage.PAGE_HEADER_SIZE;
    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = fileId;
    walPage.pageNumber = 0;
    walPage.currentPageVersion = baseVersion + 3; // gap > 1, allowed because it is a full page
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = pageSize - 1; // whole content region -> full page
    walPage.currentPageSize = page0.getContentSize();
    final byte[] fullContent = new byte[deltaSize];
    fullContent[10] = (byte) 0xAB; // marker we can verify after the apply
    walPage.currentContent = new Binary(fullContent);

    walTx.pages = new WALFile.WALPage[] { walPage };

    final boolean changed = db.getTransactionManager().applyChanges(walTx, Collections.emptyMap(), false);
    assertThat(changed).isTrue();

    db.getPageManager().removePageFromCache(pageId0);
    final ImmutablePage page0After = db.getPageManager().getImmutablePage(pageId0, pageSize, false, true);
    assertThat(page0After.getVersion()).isEqualTo(baseVersion + 3);
    assertThat(page0After.getContent().array()[BasePage.PAGE_HEADER_SIZE + 10]).isEqualTo((byte) 0xAB);
  }
}
