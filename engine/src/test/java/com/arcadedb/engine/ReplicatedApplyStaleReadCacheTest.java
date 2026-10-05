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
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A replicated apply must leave the applied page in the read cache, so a reader that loaded the page from disk just
 * BEFORE the apply wrote it cannot cache its older image afterwards.
 * <p>
 * {@code PageManager.loadPage} reads under the per-page I/O lock and caches after releasing it. The apply used to
 * write the page under that lock and then EVICT it from the cache: with the page absent, the version-monotonic put of
 * #4925 had nothing newer to keep, and the reader's late put of version N won over the N+1 just written. The next
 * entry on the page, carrying N+2, then failed its version check against the cached N with a
 * {@link com.arcadedb.exception.WALVersionGapException}: on an HA follower that is a false divergence, a snapshot
 * resync, and every open handle on the database closed under its users ({@code RaftChunkChainConcurrentResizeIT}).
 * Only a node that applies entries while serving local reads of the same pages could hit it, which is why a workload
 * issued on the leader alone never did.
 */
class ReplicatedApplyStaleReadCacheTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("TestType");
  }

  @Test
  void aReaderCachingThePreApplyImageAfterTheApplyDoesNotRollThePageBack() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("TestType").set("name", "record-" + i).save();
    });

    final int fileId = db.getSchema().getType("TestType").getBuckets(false).getFirst().getFileId();
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
    final PageId pageId = new PageId(db, fileId, 0);
    final PageManager pageManager = db.getPageManager();

    final ImmutablePage page = pageManager.getImmutablePage(pageId, pageSize, false, true);
    final int baseVersion = (int) page.getVersion();

    // The image a concurrent reader read from disk before the apply below writes the page: it caches it only later.
    final CachedPage staleReaderImage = new CachedPage(page.modify(), true);
    // The reader got here on a cache miss, so the page is not cached when the apply runs.
    pageManager.removePageFromCache(pageId);

    assertThat(db.getTransactionManager().applyChanges(walTx(page, fileId, baseVersion + 1, 1), Collections.emptyMap(), false))
        .isTrue();

    // The reader resumes and caches what it read.
    pageManager.putPageInReadCache(staleReaderImage);

    assertThat(pageManager.getImmutablePage(pageId, pageSize, false, true).getVersion())
        .as("the page just applied must not be rolled back by a reader that read it before the apply")
        .isEqualTo(baseVersion + 1);

    // The next entry on the page follows the one just applied, so it must apply cleanly rather than see a version gap
    assertThat(db.getTransactionManager().applyChanges(walTx(page, fileId, baseVersion + 2, 2), Collections.emptyMap(), false))
        .isTrue();
    pageManager.removePageFromCache(pageId);
    assertThat(pageManager.getImmutablePage(pageId, pageSize, false, true).getVersion()).isEqualTo(baseVersion + 2);
  }

  /** A one-page entry rewriting the first bytes of the content area with their current value, at {@code version}. */
  private static WALFile.WALTransaction walTx(final ImmutablePage page, final int fileId, final int version, final long txId) {
    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = fileId;
    walPage.pageNumber = 0;
    walPage.currentPageVersion = version;
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
    walPage.currentPageSize = page.getContentSize();
    final byte[] content = new byte[11];
    System.arraycopy(page.getContent().array(), walPage.changesFrom, content, 0, content.length);
    walPage.currentContent = new Binary(content);

    final WALFile.WALTransaction tx = new WALFile.WALTransaction();
    tx.txId = txId;
    tx.timestamp = System.currentTimeMillis();
    tx.pages = new WALFile.WALPage[] { walPage };
    return tx;
  }
}
