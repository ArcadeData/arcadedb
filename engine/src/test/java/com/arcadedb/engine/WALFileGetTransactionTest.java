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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link WALFile#getTransaction(long)}.
 *
 * Covers:
 * 1. Valid transactions are read back with all fields intact.
 * 2. Truncated WAL files return {@code null} at every truncation point.
 * 3. An I/O error on the channel throws {@link WALException} instead of returning {@code null}.
 */
class WALFileGetTransactionTest {

  // Layout constants mirroring the private statics in WALFile
  // TX_HEADER: txId(8) + timestamp(8) + pages(4) + segmentSize(4) = 24
  private static final int TX_HEADER_SIZE = 24;
  // PAGE_HEADER: fileId(4) + pageNumber(4) + changesFrom(4) + changesTo(4) + pageVersion(4) + pageSize(4) = 24
  private static final int PAGE_HEADER_SIZE = 24;
  // TX_FOOTER: segmentSize(4) + MAGIC_NUMBER(8) = 12
  private static final int TX_FOOTER_SIZE = 12;

  private static final byte[] DELTA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

  private static final long   TX_ID      = 42L;
  private static final long   TIMESTAMP  = 987654321L;
  private static final int    FILE_ID    = 7;
  private static final int    PAGE_NUM   = 3;
  private static final int    FROM       = 0;
  private static final int    TO         = DELTA.length - 1;   // 9
  private static final int    PAGE_VER   = 5;
  private static final int    PAGE_SIZE  = 4096;
  private static final int    DELTA_SIZE = TO - FROM + 1;       // 10
  private static final int    SEG_SIZE   = PAGE_HEADER_SIZE + DELTA_SIZE; // 34

  private File    tempFile;
  private WALFile walFile;

  @BeforeEach
  void setUp() throws Exception {
    tempFile = File.createTempFile("wal-test-", ".wal");
    tempFile.deleteOnExit();
    writeValidTransaction(tempFile);
    walFile = new WALFile(tempFile.getAbsolutePath());
  }

  @AfterEach
  void tearDown() throws Exception {
    if (walFile != null)
      walFile.close();
    if (tempFile != null)
      tempFile.delete();
  }

  @Test
  void validTransactionIsReadCorrectly() {
    final WALFile.WALTransaction tx = walFile.getTransaction(0);

    assertThat(tx).isNotNull();
    assertThat(tx.txId).isEqualTo(TX_ID);
    assertThat(tx.timestamp).isEqualTo(TIMESTAMP);
    assertThat(tx.pages).hasSize(1);

    final WALFile.WALPage page = tx.pages[0];
    assertThat(page.fileId).isEqualTo(FILE_ID);
    assertThat(page.pageNumber).isEqualTo(PAGE_NUM);
    assertThat(page.changesFrom).isEqualTo(FROM);
    assertThat(page.changesTo).isEqualTo(TO);
    assertThat(page.currentPageVersion).isEqualTo(PAGE_VER);
    assertThat(page.currentPageSize).isEqualTo(PAGE_SIZE);
    assertThat(page.currentContent.getContent()).startsWith(DELTA);

    // endPositionInLog must point past the footer
    assertThat(tx.endPositionInLog).isEqualTo(TX_HEADER_SIZE + SEG_SIZE + TX_FOOTER_SIZE);
  }

  @Test
  void truncatedWalReturnsNull() throws Exception {
    // Truncate one byte before the magic-number footer completes
    final int fullSize = TX_HEADER_SIZE + SEG_SIZE + TX_FOOTER_SIZE;
    for (int truncPoint = 0; truncPoint < fullSize; truncPoint++) {
      final File truncFile = File.createTempFile("wal-trunc-", ".wal");
      truncFile.deleteOnExit();
      WALFile wf = null;
      try {
        writeTruncatedTransaction(truncFile, truncPoint);
        wf = new WALFile(truncFile.getAbsolutePath());
        assertThat(wf.getTransaction(0))
            .as("truncated at byte %d should return null", truncPoint)
            .isNull();
      } finally {
        if (wf != null)
          wf.close();
        truncFile.delete();
      }
    }
  }

  @Test
  void ioErrorOnChannelThrowsWALException() throws Exception {
    // Close the underlying FileChannel via reflection to induce IOException on the next read.
    // Prior to the fix this returned null (swallowed IOException); the fix must throw WALException.
    final FileChannel channel = extractChannel(walFile);
    channel.close();

    assertThatThrownBy(() -> walFile.getTransaction(0))
        .isInstanceOf(WALException.class);
  }

  // --- helpers ---

  private static void writeValidTransaction(final File file) throws Exception {
    final ByteBuffer buf = buildValidTransactionBuffer();
    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      buf.rewind();
      raf.getChannel().write(buf, 0);
    }
  }

  private static void writeTruncatedTransaction(final File file, final int bytes) throws Exception {
    if (bytes == 0)
      return; // leave file empty
    final ByteBuffer full = buildValidTransactionBuffer();
    full.rewind();
    final byte[] truncated = new byte[bytes];
    full.get(truncated);
    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      raf.getChannel().write(ByteBuffer.wrap(truncated), 0);
    }
  }

  /** Builds a complete, valid WAL transaction ByteBuffer in BIG_ENDIAN order. */
  private static ByteBuffer buildValidTransactionBuffer() {
    final ByteBuffer buf = ByteBuffer.allocate(TX_HEADER_SIZE + SEG_SIZE + TX_FOOTER_SIZE);
    // TX header
    buf.putLong(TX_ID);
    buf.putLong(TIMESTAMP);
    buf.putInt(1);            // page count
    buf.putInt(SEG_SIZE);
    // Page header
    buf.putInt(FILE_ID);
    buf.putInt(PAGE_NUM);
    buf.putInt(FROM);
    buf.putInt(TO);
    buf.putInt(PAGE_VER);
    buf.putInt(PAGE_SIZE);
    // Delta bytes
    buf.put(DELTA);
    // TX footer
    buf.putInt(SEG_SIZE);
    buf.putLong(WALFile.MAGIC_NUMBER);
    buf.rewind();
    return buf;
  }

  private static FileChannel extractChannel(final WALFile wf) throws Exception {
    final Field f = WALFile.class.getDeclaredField("channel");
    f.setAccessible(true);
    return (FileChannel) f.get(wf);
  }
}
