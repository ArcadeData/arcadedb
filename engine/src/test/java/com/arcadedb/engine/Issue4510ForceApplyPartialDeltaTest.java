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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #4510: {@code forceApply} must not write a partial delta on top of a
 * stale baseline during HA replication replay.
 * <p>
 * {@code forceApply=true} (signalled by a negative txId) bypasses the version-gap check used for
 * compaction page replication. When the follower's page is at version N and the WAL entry carries
 * version N+K with K&gt;1 (the follower missed the intermediate transactions), applying only the
 * delta range {@code [changesFrom, changesTo]} would leave the bytes outside that range stale while
 * forcing the version forward to N+K. Every subsequent MVCC check would then pass over a silently
 * corrupted page, and the corruption would propagate across the cluster.
 * <p>
 * The fix only honours {@code forceApply} over a version gap when the WAL entry rewrites the whole
 * content region (a full-page payload, which is exactly what the compaction replicator ships).
 * A partial delta over a gap is rejected so the follower recovers via a full snapshot instead of
 * corrupting the page.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4510ForceApplyPartialDeltaTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("TestType");
  }

  @Test
  void forceApplyPartialDeltaOverVersionGapIsRejected() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("TestType").set("name", "record-" + i).save();
    });

    final int fileId = db.getSchema().getType("TestType").getBuckets(false).getFirst().getFileId();
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
    assertThatThrownBy(() -> db.getTransactionManager().applyChanges(walTx, Map.of(), false))
        .isInstanceOf(WALVersionGapException.class);

    // The page must be untouched: original version and the probe byte outside the delta range.
    db.getPageManager().removePageFromCache(pageId0);
    final ImmutablePage page0After = db.getPageManager().getImmutablePage(pageId0, pageSize, false, true);
    assertThat(page0After.getVersion()).isEqualTo(baseVersion);
    assertThat(page0After.getContent().array()[probeOffset]).isEqualTo(probeBefore);

    // With ignoreErrors=true the partial forceApply is skipped (no change), again leaving the page intact.
    db.getPageManager().removePageFromCache(pageId0);
    final boolean changed = db.getTransactionManager().applyChanges(walTx, Map.of(), true);
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

    final int fileId = db.getSchema().getType("TestType").getBuckets(false).getFirst().getFileId();
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

    final boolean changed = db.getTransactionManager().applyChanges(walTx, Map.of(), false);
    assertThat(changed).isTrue();

    db.getPageManager().removePageFromCache(pageId0);
    final ImmutablePage page0After = db.getPageManager().getImmutablePage(pageId0, pageSize, false, true);
    assertThat(page0After.getVersion()).isEqualTo(baseVersion + 3);
    assertThat(page0After.getContent().array()[BasePage.PAGE_HEADER_SIZE + 10]).isEqualTo((byte) 0xAB);
  }
}
