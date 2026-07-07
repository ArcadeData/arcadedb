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
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4508: a torn write in the middle of one WAL transaction record makes
 * {@link WALFile#getTransaction(long)} return {@code null}, which the recovery loop used to interpret as
 * "end of WAL", silently abandoning later valid transactions in the same file.
 *
 * <p>The fix distinguishes a benign torn record at the physical end of the file (the last write interrupted
 * by a crash) from a corrupt record in the middle of the WAL that is followed by intact transactions. The
 * latter is reported loudly (SEVERE) and the WAL files are preserved for manual inspection instead of being
 * silently dropped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4508TornWALRecoveryTest extends TestHelper {

  // Layout constants mirroring the private statics in WALFile.
  private static final int TX_HEADER_SIZE   = 24; // txId(8) + timestamp(8) + pages(4) + segmentSize(4)
  private static final int PAGE_HEADER_SIZE = 24; // fileId(4) + pageNumber(4) + from(4) + to(4) + version(4) + size(4)
  private static final int TX_FOOTER_SIZE   = 12; // segmentSize(4) + MAGIC_NUMBER(8)

  // A file id that does not exist in the database, so applyChanges() treats the crafted transactions as
  // no-ops (it logs "skipping missing file" and continues). This lets the recovery loop reach the torn
  // record without depending on real page-version chains.
  private static final int  MISSING_FILE_ID = 990_000;
  private static final long BASE_TX_ID      = 9_000_000L;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("TornWALType");
  }

  // ---------------------------------------------------------------------------------------------------------
  // Unit-level tests of WALFile.findNextValidTransactionPosition(...) on a raw, hand-crafted WAL file.
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void findsValidTransactionAfterTornRecord() throws Exception {
    final File walFile = File.createTempFile("wal-torn-", ".wal");
    walFile.deleteOnExit();
    try {
      final int tx1Size = transactionSize();
      final int gapSize = 40; // zero-filled garbage between the two valid transactions

      try (final RandomAccessFile raf = new RandomAccessFile(walFile, "rw")) {
        final FileChannel ch = raf.getChannel();
        ch.write(buildTransaction(BASE_TX_ID, MISSING_FILE_ID), 0);
        ch.write(ByteBuffer.allocate(gapSize), tx1Size);                       // torn/garbage region (no magic)
        ch.write(buildTransaction(BASE_TX_ID + 1, MISSING_FILE_ID), tx1Size + gapSize);
      }

      final WALFile wf = new WALFile(walFile.getAbsolutePath());
      try {
        final WALFile.WALTransaction tx1 = wf.getTransaction(0);
        assertThat(tx1).isNotNull();
        assertThat(tx1.txId).isEqualTo(BASE_TX_ID);
        assertThat(tx1.endPositionInLog).isEqualTo(tx1Size);

        // The garbage region must not parse as a transaction.
        assertThat(wf.getTransaction(tx1.endPositionInLog)).isNull();

        // ...but the scanner must locate the intact transaction that follows it.
        final long nextPos = wf.findNextValidTransactionPosition(tx1.endPositionInLog);
        assertThat(nextPos).isEqualTo((long) tx1Size + gapSize);

        final WALFile.WALTransaction tx2 = wf.getTransaction(nextPos);
        assertThat(tx2).isNotNull();
        assertThat(tx2.txId).isEqualTo(BASE_TX_ID + 1);
      } finally {
        wf.close();
      }
    } finally {
      walFile.delete();
    }
  }

  @Test
  void benignTruncatedTailReturnsNoNextTransaction() throws Exception {
    final File walFile = File.createTempFile("wal-trunc-", ".wal");
    walFile.deleteOnExit();
    try {
      final int tx1Size = transactionSize();

      // tx1 followed by the first half of a second transaction that was never fully flushed (no footer magic).
      final ByteBuffer tx2 = buildTransaction(BASE_TX_ID + 1, MISSING_FILE_ID);
      final byte[] partial = new byte[tx1Size - 4]; // truncate well before the footer magic
      tx2.get(partial);

      try (final RandomAccessFile raf = new RandomAccessFile(walFile, "rw")) {
        final FileChannel ch = raf.getChannel();
        ch.write(buildTransaction(BASE_TX_ID, MISSING_FILE_ID), 0);
        ch.write(ByteBuffer.wrap(partial), tx1Size);
      }

      final WALFile wf = new WALFile(walFile.getAbsolutePath());
      try {
        final WALFile.WALTransaction tx1 = wf.getTransaction(0);
        assertThat(tx1).isNotNull();
        assertThat(wf.getTransaction(tx1.endPositionInLog)).isNull();
        // No complete transaction follows: this is a benign torn last record, not mid-file corruption.
        assertThat(wf.findNextValidTransactionPosition(tx1.endPositionInLog)).isEqualTo(-1L);
      } finally {
        wf.close();
      }
    } finally {
      walFile.delete();
    }
  }

  // ---------------------------------------------------------------------------------------------------------
  // Integration tests: the recovery loop in TransactionManager.checkIntegrity().
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void recoveryPreservesWALOnMidFileCorruption() throws Exception {
    final String dbPath = database.getDatabasePath();

    // Produce some real WAL content, then crash without a clean close so recovery runs on reopen.
    database.transaction(() -> database.newDocument("TornWALType").set("k", 1).save());
    ((DatabaseInternal) database).kill();

    // Add a crafted WAL file: [valid tx][torn garbage][valid tx]. High txIds guarantee it is replayed last.
    final String tornWalName = "txlog_torn_" + BASE_TX_ID + ".wal";
    writeTornWALFile(new File(dbPath, tornWalName), 40);

    reopenExpectingRecovery();

    // #4958: preserved WAL files are renamed to .corrupt so a later open can neither adopt them as
    // active WALs (appending after the corrupt content) nor re-scan and re-abort on them forever.
    assertThat(new File(dbPath, tornWalName + ".corrupt"))
        .as("WAL file with mid-file corruption must be preserved (as .corrupt), not dropped")
        .exists();
    assertThat(new File(dbPath, tornWalName))
        .as("the corrupt WAL must not survive under its active .wal name")
        .doesNotExist();
  }

  @Test
  void recoveryDropsWALOnBenignTruncatedTail() throws Exception {
    final String dbPath = database.getDatabasePath();

    database.transaction(() -> database.newDocument("TornWALType").set("k", 2).save());
    ((DatabaseInternal) database).kill();

    // Crafted WAL file with a benign torn last record (no valid transaction follows).
    final String tornWalName = "txlog_trunc_" + BASE_TX_ID + ".wal";
    writeBenignTruncatedWALFile(new File(dbPath, tornWalName));

    reopenExpectingRecovery();

    assertThat(new File(dbPath, tornWalName))
        .as("WAL file with only a benign torn last record must be dropped after a successful recovery")
        .doesNotExist();
  }

  // --- helpers ---

  private void reopenExpectingRecovery() {
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
  }

  private void writeTornWALFile(final File file, final int gapSize) throws IOException {
    final int tx1Size = transactionSize();
    try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      final FileChannel ch = raf.getChannel();
      ch.write(buildTransaction(BASE_TX_ID, MISSING_FILE_ID), 0);
      ch.write(ByteBuffer.allocate(gapSize), tx1Size);
      ch.write(buildTransaction(BASE_TX_ID + 1, MISSING_FILE_ID), tx1Size + gapSize);
    }
  }

  private void writeBenignTruncatedWALFile(final File file) throws IOException {
    final int tx1Size = transactionSize();
    final ByteBuffer tx2 = buildTransaction(BASE_TX_ID + 1, MISSING_FILE_ID);
    final byte[] partial = new byte[tx1Size - 4];
    tx2.get(partial);
    try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      final FileChannel ch = raf.getChannel();
      ch.write(buildTransaction(BASE_TX_ID, MISSING_FILE_ID), 0);
      ch.write(ByteBuffer.wrap(partial), tx1Size);
    }
  }

  // Total on-disk size of a transaction built by buildTransaction(): a single 1-byte-delta page.
  private static int transactionSize() {
    return TX_HEADER_SIZE + (PAGE_HEADER_SIZE + 1) + TX_FOOTER_SIZE;
  }

  /**
   * Builds a complete, valid single-page WAL transaction (big-endian) with a 1-byte delta. The page references
   * {@code fileId}; when that file does not exist in the database the transaction is a no-op during replay.
   */
  private static ByteBuffer buildTransaction(final long txId, final int fileId) {
    final int deltaSize = 1;
    final int segmentSize = PAGE_HEADER_SIZE + deltaSize;
    final ByteBuffer buf = ByteBuffer.allocate(TX_HEADER_SIZE + segmentSize + TX_FOOTER_SIZE);
    // TX header
    buf.putLong(txId);
    buf.putLong(1L);          // timestamp (fixed value: keeps the test deterministic)
    buf.putInt(1);            // page count
    buf.putInt(segmentSize);
    // Page header
    buf.putInt(fileId);
    buf.putInt(0);            // pageNumber
    buf.putInt(BasePage.PAGE_HEADER_SIZE); // changesFrom
    buf.putInt(BasePage.PAGE_HEADER_SIZE); // changesTo (single byte)
    buf.putInt(1);            // currentPageVersion
    buf.putInt(512);          // currentPageSize
    buf.put((byte) 0);        // 1 byte delta
    // TX footer
    buf.putInt(segmentSize);
    buf.putLong(WALFile.MAGIC_NUMBER);
    buf.rewind();
    return buf;
  }
}
