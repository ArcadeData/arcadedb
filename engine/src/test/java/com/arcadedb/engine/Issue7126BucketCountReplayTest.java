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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7126: the bucket record-count delta was folded as a relative increment guarded
 * only by the "something changed" flag of {@link TransactionManager#applyChanges}. Since #4926 a page whose
 * WAL version EQUALS the on-disk one is deliberately re-applied (torn-write repair), so a Raft entry replayed
 * after a clean restart set that flag on a transaction that had already been applied, and the delta was added
 * a second time. On a cleanly-stopped node the cached counter is restored from {@code statistics.json} and
 * never invalidated, so {@code SELECT count(*)} over-reported permanently and disagreed with a full scan.
 * <p>
 * The fold now runs only when at least one page was written at a version STRICTLY GREATER than the one on
 * disk, which is exactly the case in which the entry carried content this node did not already have.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7126BucketCountReplayTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Counted");
  }

  @Test
  void replayAtEqualVersionDoesNotDoubleFoldTheRecordCountDelta() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("Counted").set("name", "record-" + i).save();
    });

    final LocalBucket bucket = (LocalBucket) db.getSchema().getType("Counted").getBuckets(false).getFirst();
    // Materialize the cached counter: the fold is a no-op while it is still -1, which is what makes an
    // UNCLEAN restart safe and a CLEAN one not (issue #7126).
    final long baseline = bucket.count();
    assertThat(bucket.getCachedRecordCount()).isEqualTo(baseline);

    final int fileId = bucket.getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final PageId pageId = new PageId(db, fileId, 0);

    // A Raft replay of an entry whose pages are already at their final version: same bytes, same version.
    final long versionBeforeReplay = db.getPageManager().getImmutablePage(pageId, file.getPageSize(), false, true)
        .getVersion();
    final boolean replayChanged = db.getTransactionManager()
        .applyChanges(buildWalTransaction(db, fileId, (int) versionBeforeReplay, 7001), Map.of(fileId, 5), false);

    // The page IS re-written (torn-write repair, #4926) ...
    assertThat(replayChanged).isTrue();
    // ... but the delta must NOT be folded again: this entry brought no content the node did not have.
    assertThat(bucket.getCachedRecordCount()).isEqualTo(baseline);

    // A genuinely new entry (version advances) still folds its delta exactly once.
    db.getPageManager().removePageFromCache(pageId);
    final long versionAfterReplay = db.getPageManager().getImmutablePage(pageId, file.getPageSize(), false, true)
        .getVersion();
    final boolean freshChanged = db.getTransactionManager()
        .applyChanges(buildWalTransaction(db, fileId, (int) versionAfterReplay + 1, 7002), Map.of(fileId, 5), false);

    assertThat(freshChanged).isTrue();
    assertThat(bucket.getCachedRecordCount()).isEqualTo(baseline + 5);

    // And replaying THAT entry once more leaves the counter where it is.
    db.getPageManager().removePageFromCache(pageId);
    final long versionAfterFresh = db.getPageManager().getImmutablePage(pageId, file.getPageSize(), false, true)
        .getVersion();
    db.getTransactionManager()
        .applyChanges(buildWalTransaction(db, fileId, (int) versionAfterFresh, 7003), Map.of(fileId, 5), false);
    assertThat(bucket.getCachedRecordCount()).isEqualTo(baseline + 5);
  }

  /**
   * Builds a single-page WAL transaction that rewrites the first bytes of page 0 with the content they
   * already hold, at {@code targetVersion}. The bytes are irrelevant here: what the test drives is the
   * version comparison that decides whether the record-count delta is folded.
   */
  private static WALFile.WALTransaction buildWalTransaction(final DatabaseInternal db, final int fileId,
      final int targetVersion, final long txId) throws Exception {
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final PageId pageId = new PageId(db, fileId, 0);
    final ImmutablePage page = db.getPageManager().getImmutablePage(pageId, file.getPageSize(), false, true);

    final WALFile.WALPage walPage = new WALFile.WALPage();
    walPage.fileId = fileId;
    walPage.pageNumber = 0;
    walPage.currentPageVersion = targetVersion;
    walPage.changesFrom = BasePage.PAGE_HEADER_SIZE;
    walPage.changesTo = BasePage.PAGE_HEADER_SIZE + 10;
    walPage.currentPageSize = page.getContentSize();

    final byte[] content = new byte[walPage.changesTo - walPage.changesFrom + 1];
    System.arraycopy(page.getContent().array(), walPage.changesFrom, content, 0, content.length);
    walPage.currentContent = new Binary(content);

    final WALFile.WALTransaction walTx = new WALFile.WALTransaction();
    walTx.txId = txId;
    walTx.timestamp = System.currentTimeMillis();
    walTx.pages = new WALFile.WALPage[] { walPage };
    return walTx;
  }
}
