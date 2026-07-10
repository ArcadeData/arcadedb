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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for WAL version-gap handling during crash recovery (issue #4331).
 *
 * <p>Verifies that a version gap detected during recovery:
 * <ul>
 *   <li>Throws WALVersionGapException immediately (no sibling-page application)</li>
 *   <li>Aborts recovery without dropping WAL files (WAL preserved for inspection)</li>
 * </ul>
 */
class WALVersionGapRecoveryTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("GapTestType");
  }

  @Test
  void applyChangesThrowsOnVersionGap() throws Exception {
    database.transaction(() -> database.newDocument("GapTestType").set("k", 1).save());

    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = db.getSchema().getType("GapTestType").getBuckets(false).getFirst().getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final int pageSize = file.getPageSize();

    final PageId pageId = new PageId(db, fileId, 0);
    final ImmutablePage page = db.getPageManager().getImmutablePage(pageId, pageSize, false, true);
    final int currentVersion = (int) page.getVersion();

    final WALFile.WALPage gappedPage = buildWALPage(fileId, 0, currentVersion + 2, page.getContentSize());

    final WALFile.WALTransaction tx = new WALFile.WALTransaction();
    tx.txId = 99999L;
    tx.timestamp = System.currentTimeMillis();
    tx.pages = new WALFile.WALPage[] { gappedPage };

    assertThatThrownBy(() -> db.getTransactionManager().applyChanges(tx, Map.of(), false))
        .isInstanceOf(WALVersionGapException.class);
  }

  @Test
  void versionGapPageNotApplied() throws Exception {
    database.transaction(() -> database.newDocument("GapTestType").set("k", 2).save());

    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = db.getSchema().getType("GapTestType").getBuckets(false).getFirst().getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    final int pageSize = file.getPageSize();

    final PageId pageId = new PageId(db, fileId, 0);
    final ImmutablePage page = db.getPageManager().getImmutablePage(pageId, pageSize, false, true);
    final int versionBefore = (int) page.getVersion();

    final WALFile.WALPage gappedPage = buildWALPage(fileId, 0, versionBefore + 2, page.getContentSize());

    final WALFile.WALTransaction tx = new WALFile.WALTransaction();
    tx.txId = 88888L;
    tx.timestamp = System.currentTimeMillis();
    tx.pages = new WALFile.WALPage[] { gappedPage };

    assertThatThrownBy(() -> db.getTransactionManager().applyChanges(tx, Map.of(), false))
        .isInstanceOf(WALVersionGapException.class);

    // Page must be unchanged - the exception must have prevented the write
    db.getPageManager().removePageFromCache(pageId);
    final ImmutablePage pageAfter = db.getPageManager().getImmutablePage(pageId, pageSize, false, true);
    assertThat(pageAfter.getVersion())
        .as("Version-gap page must not be applied to disk")
        .isEqualTo(versionBefore);
  }

  @Test
  void checkIntegrityPreservesWALOnVersionGap() throws Exception {
    for (int i = 0; i < 3; i++) {
      final int n = i;
      database.transaction(() -> database.newDocument("GapTestType").set("n", n).save());
    }

    final DatabaseInternal db = (DatabaseInternal) database;
    final String dbPath = database.getDatabasePath();
    final int fileId = db.getSchema().getType("GapTestType").getBuckets(false).getFirst().getFileId();

    // Simulate a crash: kill without flush, WAL files remain
    db.kill();

    final File dbDir = new File(dbPath);
    final File[] walFilesAfterKill = dbDir.listFiles((d, n) -> n.endsWith(".wal"));
    assertThat(walFilesAfterKill)
        .as("WAL files must be present after kill()")
        .isNotEmpty();

    // Find the max page version and max txId across all WAL transactions
    int maxPageVersion = 0;
    long maxTxId = 0;
    for (final File walFile : walFilesAfterKill) {
      final WALFile wal = new WALFile(walFile.getAbsolutePath());
      WALFile.WALTransaction tx = wal.getFirstTransaction();
      while (tx != null) {
        maxTxId = Math.max(maxTxId, tx.txId);
        for (final WALFile.WALPage p : tx.pages) {
          if (p.fileId == fileId)
            maxPageVersion = Math.max(maxPageVersion, p.currentPageVersion);
        }
        tx = wal.getTransaction(tx.endPositionInLog);
      }
      wal.close();
    }

    // Write a new WAL file containing a transaction with a version gap:
    // maxPageVersion+2 skips maxPageVersion+1, triggering the gap check after the
    // existing WAL transactions are replayed and the page is at maxPageVersion.
    final long gapTxId = maxTxId + 1;
    final int gapVersion = maxPageVersion + 2;
    writeGapWALFile(dbPath, gapTxId, fileId, 0, gapVersion);

    // Reopen: checkIntegrity runs, detects the gap, must preserve WAL files
    database.close();
    final AtomicBoolean recoveryRan = new AtomicBoolean(false);
    factory.registerCallback(DatabaseInternal.CALLBACK_EVENT.DB_NOT_CLOSED, () -> {
      recoveryRan.set(true);
      return null;
    });
    database = factory.open();

    assertThat(recoveryRan.get())
        .as("DB_NOT_CLOSED callback must fire - recovery must have run")
        .isTrue();

    // #4958: the preserved files carry the .corrupt suffix so the fresh active pool cannot reuse them
    // and their stale records are never replayed again.
    final File[] preservedAfterReopen = dbDir.listFiles((d, n) -> n.endsWith(".corrupt"));
    assertThat(preservedAfterReopen)
        .as("WAL files must be preserved (renamed .corrupt) when a version gap is detected during recovery")
        .isNotEmpty();
  }

  // Builds a WALPage for applyChanges tests (1 byte of content at PAGE_HEADER_SIZE offset)
  private WALFile.WALPage buildWALPage(final int fileId, final int pageNumber, final int version, final int contentSize) {
    final WALFile.WALPage p = new WALFile.WALPage();
    p.fileId = fileId;
    p.pageNumber = pageNumber;
    p.changesFrom = BasePage.PAGE_HEADER_SIZE;
    p.changesTo = BasePage.PAGE_HEADER_SIZE;
    p.currentPageVersion = version;
    p.currentPageSize = contentSize;
    p.currentContent = new Binary(new byte[1]);
    return p;
  }

  /**
   * Writes a minimal valid WAL file containing one transaction with the given page version.
   * WAL binary format (all big-endian):
   *   TX header (24 bytes): txId(8) timestamp(8) pageCount(4) segmentSize(4)
   *   Per page  (24 bytes + delta): fileId(4) pageNumber(4) changesFrom(4) changesTo(4)
   *             currentPageVersion(4) currentPageSize(4) delta(1)
   *   TX footer (12 bytes): segmentSize(4) MAGIC_NUMBER(8)
   */
  private void writeGapWALFile(final String dbPath, final long txId, final int fileId,
      final int pageNumber, final int pageVersion) throws IOException {
    // 1 byte delta: changesFrom == changesTo
    final int deltaSize = 1;
    // PAGE_HEADER = 6 ints = 24 bytes
    final int pageSegmentSize = 24 + deltaSize;
    // TX_HEADER = 2 longs + 2 ints = 24 bytes; TX_FOOTER = 1 int + 1 long = 12 bytes
    final int totalBytes = 24 + pageSegmentSize + 12;

    final ByteBuffer buf = ByteBuffer.allocate(totalBytes); // big-endian by default
    // TX header
    buf.putLong(txId);
    buf.putLong(System.currentTimeMillis());
    buf.putInt(1);               // 1 page
    buf.putInt(pageSegmentSize);
    // Page header
    buf.putInt(fileId);
    buf.putInt(pageNumber);
    buf.putInt(BasePage.PAGE_HEADER_SIZE);     // changesFrom
    buf.putInt(BasePage.PAGE_HEADER_SIZE);     // changesTo (single byte)
    buf.putInt(pageVersion);
    buf.putInt(512);             // currentPageSize - small safe value
    buf.put((byte) 0);           // 1 byte of delta content
    // TX footer
    buf.putInt(pageSegmentSize);
    buf.putLong(WALFile.MAGIC_NUMBER);

    final String walPath = dbPath + File.separator + "txlog_gap_" + txId + ".wal";
    try (final RandomAccessFile raf = new RandomAccessFile(walPath, "rw");
        final FileChannel ch = raf.getChannel()) {
      buf.rewind();
      ch.write(buf);
    }
  }
}
