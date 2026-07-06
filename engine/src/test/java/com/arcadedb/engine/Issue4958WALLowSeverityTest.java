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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the low-severity WALFile findings grouped in issue #4958.
 * <ul>
 *   <li>A corrupt page header whose {@code changesFrom}/{@code changesTo} imply a huge delta must be
 *   rejected against the remaining file size BEFORE any buffer allocation. Before the fix,
 *   {@code getTransaction} attempted {@code ByteBuffer.allocate(Integer.MAX_VALUE)} - an
 *   {@link OutOfMemoryError} ("Requested array size exceeds VM limit") that escaped the
 *   {@code catch (Exception)} recovery guard.</li>
 *   <li>{@code readLong}/{@code readInt} must not rely on a single unlooped {@code channel.read} into a
 *   shared instance buffer: covered indirectly by the round-trip test (the new implementation loops
 *   until the buffer is full and uses per-call buffers).</li>
 * </ul>
 */
class Issue4958WALLowSeverityTest {

  // Layout constants mirroring the private statics in WALFile.
  private static final int TX_HEADER_SIZE   = 24; // txId(8) + timestamp(8) + pages(4) + segmentSize(4)
  private static final int PAGE_HEADER_SIZE = 24; // fileId(4) + pageNumber(4) + from(4) + to(4) + version(4) + size(4)
  private static final int TX_FOOTER_SIZE   = 12; // segmentSize(4) + MAGIC_NUMBER(8)

  @Test
  void corruptDeltaSizeIsRejectedWithoutHugeAllocation() throws Exception {
    final File tempFile = File.createTempFile("wal-corrupt-delta-", ".wal");
    tempFile.deleteOnExit();
    WALFile walFile = null;
    try {
      // A record whose declared segment size is small (passes the outer bound check) but whose page
      // header declares changesFrom=0, changesTo=Integer.MAX_VALUE-1 -> deltaSize == Integer.MAX_VALUE.
      final int deltaBytesOnDisk = 8;
      final int segmentSize = PAGE_HEADER_SIZE + deltaBytesOnDisk;
      final ByteBuffer buf = ByteBuffer.allocate(TX_HEADER_SIZE + segmentSize + TX_FOOTER_SIZE);
      buf.putLong(1L);                       // txId
      buf.putLong(1L);                       // timestamp
      buf.putInt(1);                         // page count
      buf.putInt(segmentSize);
      buf.putInt(9);                         // fileId
      buf.putInt(0);                         // pageNumber
      buf.putInt(0);                         // changesFrom
      buf.putInt(Integer.MAX_VALUE - 1);     // changesTo -> deltaSize = Integer.MAX_VALUE
      buf.putInt(1);                         // currentPageVersion
      buf.putInt(512);                       // currentPageSize
      buf.put(new byte[deltaBytesOnDisk]);   // the bytes actually on disk
      buf.putInt(segmentSize);
      buf.putLong(WALFile.MAGIC_NUMBER);
      buf.rewind();

      try (final RandomAccessFile raf = new RandomAccessFile(tempFile, "rw")) {
        final FileChannel ch = raf.getChannel();
        ch.write(buf, 0);
      }

      walFile = new WALFile(tempFile.getAbsolutePath());

      // Before the fix: OutOfMemoryError("Requested array size exceeds VM limit") from
      // ByteBuffer.allocate(Integer.MAX_VALUE). After the fix: the delta is clamped against the
      // remaining file size and the record is rejected as corrupt.
      assertThat(walFile.getTransaction(0))
          .as("a page delta larger than the remaining file must be rejected as corrupt")
          .isNull();
    } finally {
      if (walFile != null)
        walFile.close();
      tempFile.delete();
    }
  }

  @Test
  void appendedTransactionRoundTripsThroughReadHelpers() throws Exception {
    final File tempFile = File.createTempFile("wal-roundtrip-", ".wal");
    tempFile.deleteOnExit();
    WALFile walFile = null;
    try {
      final byte[] delta = { 10, 20, 30, 40, 50 };
      final int segmentSize = PAGE_HEADER_SIZE + delta.length;
      final ByteBuffer buf = ByteBuffer.allocate(TX_HEADER_SIZE + segmentSize + TX_FOOTER_SIZE);
      buf.putLong(77L);
      buf.putLong(123456L);
      buf.putInt(1);
      buf.putInt(segmentSize);
      buf.putInt(3);                          // fileId
      buf.putInt(2);                          // pageNumber
      buf.putInt(0);                          // changesFrom
      buf.putInt(delta.length - 1);           // changesTo
      buf.putInt(4);                          // currentPageVersion
      buf.putInt(1024);                       // currentPageSize
      buf.put(delta);
      buf.putInt(segmentSize);
      buf.putLong(WALFile.MAGIC_NUMBER);

      walFile = new WALFile(tempFile.getAbsolutePath());
      walFile.append(buf);

      final WALFile.WALTransaction tx = walFile.getTransaction(0);
      assertThat(tx).isNotNull();
      assertThat(tx.txId).isEqualTo(77L);
      assertThat(tx.timestamp).isEqualTo(123456L);
      assertThat(tx.pages).hasSize(1);
      assertThat(tx.pages[0].fileId).isEqualTo(3);
      assertThat(tx.pages[0].pageNumber).isEqualTo(2);
      assertThat(tx.pages[0].currentContent.getContent()).startsWith(delta);
    } finally {
      if (walFile != null)
        walFile.close();
      tempFile.delete();
    }
  }
}
