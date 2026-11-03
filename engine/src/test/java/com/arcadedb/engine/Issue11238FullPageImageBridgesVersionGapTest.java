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
import com.arcadedb.exception.WALVersionGapException;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A WAL entry that ships a page as a FULL image bridges a version gap in an ordinary replay, not only in a
 * {@code forceApply} one (issue #11238): it is what a writer that committed a page without the WAL writes when the
 * page could not reach the disk in time, so recovery must apply it over whatever older version the file holds. A
 * partial delta over a gap is still refused.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11238FullPageImageBridgesVersionGapTest extends TestHelper {
  /** The tests hand-write WAL pages over the bucket, which the end-of-test integrity check would report. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("GapType");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("GapType").set("name", "record-" + i).save();
    });
  }

  @Test
  void aFullPageImageOverAVersionGapIsApplied() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = db.getSchema().getType("GapType").getBuckets(false).getFirst().getFileId();
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
    final PageId pageId = new PageId(db, fileId, 0);
    final ImmutablePage before = loadFromDisk(db, pageId, pageSize);
    final int baseVersion = (int) before.getVersion();

    final byte[] image = new byte[pageSize - BasePage.PAGE_HEADER_SIZE];
    image[10] = (byte) 0xAB;
    final WALFile.WALTransaction tx = transactionOf(fileId, baseVersion + 3, BasePage.PAGE_HEADER_SIZE, pageSize - 1, image,
        before.getContentSize());

    assertThat(db.getTransactionManager().applyChanges(tx, Collections.emptyMap(), false)).isTrue();

    final ImmutablePage after = loadFromDisk(db, pageId, pageSize);
    assertThat(after.getVersion()).isEqualTo(baseVersion + 3);
    assertThat(after.getContent().array()[BasePage.PAGE_HEADER_SIZE + 10]).isEqualTo((byte) 0xAB);
  }

  /** Only a gap is bridged: a full image OLDER than the page is superseded and must not roll the page back. */
  @Test
  void aStaleFullPageImageDoesNotOverwriteANewerPage() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = db.getSchema().getType("GapType").getBuckets(false).getFirst().getFileId();
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
    final PageId pageId = new PageId(db, fileId, 0);
    final ImmutablePage before = loadFromDisk(db, pageId, pageSize);
    final int baseVersion = (int) before.getVersion();
    assertThat(baseVersion).as("the page needs an older version to be stale against").isGreaterThan(0);
    final byte markerBefore = before.getContent().array()[BasePage.PAGE_HEADER_SIZE + 10];

    final byte[] image = new byte[pageSize - BasePage.PAGE_HEADER_SIZE];
    image[10] = (byte) (markerBefore + 1);
    final WALFile.WALTransaction tx = transactionOf(fileId, baseVersion - 1, BasePage.PAGE_HEADER_SIZE, pageSize - 1, image,
        before.getContentSize());

    assertThat(db.getTransactionManager().applyChanges(tx, Collections.emptyMap(), false)).isFalse();

    final ImmutablePage after = loadFromDisk(db, pageId, pageSize);
    assertThat(after.getVersion()).isEqualTo(baseVersion);
    assertThat(after.getContent().array()[BasePage.PAGE_HEADER_SIZE + 10]).isEqualTo(markerBefore);
  }

  @Test
  void aPartialDeltaOverAVersionGapIsStillRefused() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = db.getSchema().getType("GapType").getBuckets(false).getFirst().getFileId();
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
    final PageId pageId = new PageId(db, fileId, 0);
    final ImmutablePage before = loadFromDisk(db, pageId, pageSize);
    final int baseVersion = (int) before.getVersion();

    // the whole page but one byte: still a delta, and the byte it leaves out would keep whatever the file holds
    final byte[] almostAll = new byte[pageSize - BasePage.PAGE_HEADER_SIZE - 1];
    final WALFile.WALTransaction tx = transactionOf(fileId, baseVersion + 3, BasePage.PAGE_HEADER_SIZE, pageSize - 2, almostAll,
        before.getContentSize());

    assertThatThrownBy(() -> db.getTransactionManager().applyChanges(tx, Collections.emptyMap(), false))
        .isInstanceOf(WALVersionGapException.class);
    assertThat(loadFromDisk(db, pageId, pageSize).getVersion()).isEqualTo(baseVersion);
  }

  @Test
  void aPageMarkedFullyModifiedShipsOneFullImage() {
    // an existing page (a new one is modified in full from the start)
    final MutablePage page = new MutablePage(new PageId(database, 1, 0), 65_536, new byte[65_536], 3, 1_000);
    page.updateModifiedRange(100, 103);
    page.updateModifiedRange(9_000, 9_003);
    assertThat(page.getModifiedRangeCount()).isEqualTo(2);

    page.markFullyModified();

    assertThat(page.getModifiedRangeCount()).isEqualTo(1);
    assertThat(page.getModifiedRanges()[0]).isEqualTo(BasePage.PAGE_HEADER_SIZE);
    assertThat(page.getModifiedRanges()[1]).isEqualTo(page.getPhysicalSize() - 1);
  }

  private static ImmutablePage loadFromDisk(final DatabaseInternal db, final PageId pageId, final int pageSize)
      throws Exception {
    db.getPageManager().removePageFromCache(pageId);
    return db.getPageManager().getImmutablePage(pageId, pageSize, false, true);
  }

  private static WALFile.WALTransaction transactionOf(final int fileId, final int version, final int changesFrom,
      final int changesTo, final byte[] content, final int contentSize) {
    final WALFile.WALTransaction tx = new WALFile.WALTransaction();
    tx.txId = 1_000;
    tx.timestamp = System.currentTimeMillis();

    final WALFile.WALPage page = new WALFile.WALPage();
    page.fileId = fileId;
    page.pageNumber = 0;
    page.currentPageVersion = version;
    page.changesFrom = changesFrom;
    page.changesTo = changesTo;
    page.currentPageSize = contentSize;
    page.currentContent = new Binary(content);
    tx.pages = new WALFile.WALPage[] { page };
    return tx;
  }
}
