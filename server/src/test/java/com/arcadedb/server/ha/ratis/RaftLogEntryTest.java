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
package com.arcadedb.server.ha.ratis;

import com.arcadedb.database.Binary;
import com.arcadedb.engine.WALFile;
import com.arcadedb.server.ha.ReplicationException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests serialization and deserialization of Raft log entries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftLogEntryTest {

  @Test
  void testTransactionSerializationRoundTrip() {
    // Create a minimal WAL buffer with the expected format
    final Binary walBuffer = createTestWalBuffer(42L, 1234567890L, 0);

    final Map<Integer, Integer> bucketDelta = new HashMap<>();
    bucketDelta.put(1, 5);
    bucketDelta.put(2, -3);

    // Serialize
    final byte[] serialized = RaftLogEntry.serializeTransaction("testDb", bucketDelta, walBuffer, null, null, null, "node-1");

    // Verify type marker
    assertThat(RaftLogEntry.readType(ByteBuffer.wrap(serialized))).isEqualTo(RaftLogEntry.EntryType.TRANSACTION);

    // Deserialize
    final RaftLogEntry.TransactionEntry entry = RaftLogEntry.deserializeTransaction(serialized);

    assertThat(entry.originPeerId()).isEqualTo("node-1");
    assertThat(entry.databaseName()).isEqualTo("testDb");
    assertThat(entry.bucketRecordDelta()).hasSize(2);
    assertThat(entry.bucketRecordDelta().get(1)).isEqualTo(5);
    assertThat(entry.bucketRecordDelta().get(2)).isEqualTo(-3);
    assertThat(entry.schemaJson()).isNull();
    assertThat(entry.filesToAdd()).isNull();
    assertThat(entry.filesToRemove()).isNull();

    // Verify the WAL buffer can be parsed
    final WALFile.WALTransaction walTx = ArcadeDBStateMachine.parseWalTransaction(entry.walBuffer());
    assertThat(walTx.txId).isEqualTo(42L);
    assertThat(walTx.timestamp).isEqualTo(1234567890L);
    assertThat(walTx.pages.length).isEqualTo(0);
  }

  @Test
  void testTransactionWithSchemaChange() {
    final Binary walBuffer = createTestWalBuffer(99L, 9999L, 0);

    final Map<Integer, Integer> bucketDelta = new HashMap<>();
    bucketDelta.put(1, 1);

    final Map<Integer, String> filesToAdd = new HashMap<>();
    filesToAdd.put(10, "bucket_V1_0.pcf");
    filesToAdd.put(11, null);

    final Map<Integer, String> filesToRemove = new HashMap<>();
    filesToRemove.put(5, "old_index.idx");

    final String schemaJson = "{\"types\":[{\"name\":\"V1\",\"type\":\"vertex\"}]}";

    // Serialize
    final byte[] serialized = RaftLogEntry.serializeTransaction("myDb", bucketDelta, walBuffer, schemaJson, filesToAdd,
        filesToRemove, "leader-0");

    // Deserialize
    final RaftLogEntry.TransactionEntry entry = RaftLogEntry.deserializeTransaction(serialized);

    assertThat(entry.originPeerId()).isEqualTo("leader-0");
    assertThat(entry.databaseName()).isEqualTo("myDb");
    assertThat(entry.schemaJson()).isEqualTo(schemaJson);
    assertThat(entry.filesToAdd()).hasSize(2);
    assertThat(entry.filesToAdd().get(10)).isEqualTo("bucket_V1_0.pcf");
    assertThat(entry.filesToAdd().get(11)).isNull();
    assertThat(entry.filesToRemove()).hasSize(1);
    assertThat(entry.filesToRemove().get(5)).isEqualTo("old_index.idx");
  }

  @Test
  void testCreateDatabaseSerializationRoundTrip() {
    final byte[] serialized = RaftLogEntry.serializeCreateDatabase("newDb", "leader-node");

    assertThat(RaftLogEntry.readType(ByteBuffer.wrap(serialized))).isEqualTo(RaftLogEntry.EntryType.CREATE_DATABASE);

    final RaftLogEntry.CreateDatabaseEntry entry = RaftLogEntry.deserializeCreateDatabase(serialized);

    assertThat(entry.originPeerId()).isEqualTo("leader-node");
    assertThat(entry.databaseName()).isEqualTo("newDb");
  }

  @Test
  void testFromCodeReturnsNullForUnknownType() {
    // Forward-compatibility: unknown type codes return null instead of throwing,
    // so an older node can skip entries from a newer node during rolling upgrades.
    assertThat(RaftLogEntry.EntryType.fromCode((byte) 0)).isNull();
    assertThat(RaftLogEntry.EntryType.fromCode((byte) 2)).isNull();
    assertThat(RaftLogEntry.EntryType.fromCode((byte) 99)).isNull();
    assertThat(RaftLogEntry.EntryType.fromCode((byte) -1)).isNull();
  }

  @Test
  void testReadTypeReturnsNullForUnknownType() {
    final ByteBuffer buffer = ByteBuffer.allocate(1);
    buffer.put((byte) 42);
    buffer.flip();
    assertThat(RaftLogEntry.readType(buffer)).isNull();
  }

  @Test
  void testDeserializeTransactionRejectsCorruptedStringLength() {
    // Craft a binary buffer that looks like a TRANSACTION entry but has a corrupted string
    // length field that declares a huge string (e.g. 1GB), which would cause OOM if uncapped.
    final Binary stream = new Binary(64);
    stream.putByte(RaftLogEntry.EntryType.TRANSACTION.code()); // type marker

    // Write originPeerId with a legitimate-looking varint length prefix that's way too large.
    // Binary.putString() uses putUnsignedNumber() for length. We'll manually write a large varint.
    // Varint encoding for 1_000_000_000 (0x3B9ACA00):
    //   0x80 | (0x00) = 0x80
    //   0x80 | (0x14) = 0x94
    //   0x80 | (0x69) = 0xE9
    //   0x80 | (0x5C) = 0xDC
    //   0x03
    // But this is bigger than our buffer, so deserialization should reject it.
    stream.putByte((byte) 0x80);
    stream.putByte((byte) 0x94);
    stream.putByte((byte) 0xE9);
    stream.putByte((byte) 0xDC);
    stream.putByte((byte) 0x03);
    // No actual string data follows

    stream.flip();
    final byte[] data = new byte[stream.size()];
    stream.getByteBuffer().get(data);

    assertThatThrownBy(() -> RaftLogEntry.deserializeTransaction(data))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds");
  }

  @Test
  void testParseWalTransactionRejectsTruncatedHeader() {
    // A buffer with only 10 bytes is too short for the 24-byte header
    // (txId:8 + timestamp:8 + pageCount:4 + segmentSize:4)
    final Binary truncated = new Binary(new byte[10]);
    assertThatThrownBy(() -> ArcadeDBStateMachine.parseWalTransaction(truncated))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("truncated");
  }

  @Test
  void testParseWalTransactionRejectsEmptyBuffer() {
    final Binary empty = new Binary(new byte[0]);
    assertThatThrownBy(() -> ArcadeDBStateMachine.parseWalTransaction(empty))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("truncated");
  }

  @Test
  void testParseWalTransactionRejectsTruncatedPageHeader() {
    // Build a valid WAL buffer with 1 page, then truncate it mid-page-header so only
    // partial fixed fields are present. The bounds check must catch this before reading.
    final Binary full = createTestWalBufferWithOnePage(1L, 100L, 0, 10, 19);
    final int headerEnd = Binary.LONG_SERIALIZED_SIZE  // txId
        + Binary.LONG_SERIALIZED_SIZE                  // timestamp
        + Binary.INT_SERIALIZED_SIZE                   // pageCount
        + Binary.INT_SERIALIZED_SIZE;                  // segmentSize

    // Truncate after only 2 of the 4 page-header ints (fileId + pageNumber = 8 bytes)
    final Binary truncated = sliceBuffer(full, headerEnd + 8);

    assertThatThrownBy(() -> ArcadeDBStateMachine.parseWalTransaction(truncated))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("corrupted");
  }

  @Test
  void testParseWalTransactionRejectsTruncatedPageDelta() {
    // Build a valid WAL buffer with 1 page (delta = 10 bytes), then truncate it after
    // the 6 fixed ints but before all delta bytes are present.
    final Binary full = createTestWalBufferWithOnePage(1L, 100L, 0, 10, 19);
    final int headerEnd = Binary.LONG_SERIALIZED_SIZE  // txId
        + Binary.LONG_SERIALIZED_SIZE                  // timestamp
        + Binary.INT_SERIALIZED_SIZE                   // pageCount
        + Binary.INT_SERIALIZED_SIZE;                  // segmentSize

    // Include all 6 ints (24 bytes) but only 3 of the 10 delta bytes
    final Binary truncated = sliceBuffer(full, headerEnd + 6 * Binary.INT_SERIALIZED_SIZE + 3);

    assertThatThrownBy(() -> ArcadeDBStateMachine.parseWalTransaction(truncated))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("corrupted");
  }

  @Test
  void testParseWalTransactionWithValidPage() {
    // Verify a well-formed single-page buffer parses correctly
    final Binary buffer = createTestWalBufferWithOnePage(42L, 999L, 5, 100, 109);

    final WALFile.WALTransaction tx = ArcadeDBStateMachine.parseWalTransaction(buffer);
    assertThat(tx.txId).isEqualTo(42L);
    assertThat(tx.timestamp).isEqualTo(999L);
    assertThat(tx.pages.length).isEqualTo(1);
    assertThat(tx.pages[0].fileId).isEqualTo(5);
    assertThat(tx.pages[0].changesFrom).isEqualTo(100);
    assertThat(tx.pages[0].changesTo).isEqualTo(109);
  }

  /**
   * Creates a minimal valid WAL transaction buffer with the expected format:
   * [txId:8][timestamp:8][pageCount:4][segmentSize:4][...pages...][segmentSize:4][magicNumber:8]
   */
  private Binary createTestWalBuffer(final long txId, final long timestamp, final int pageCount) {
    // Calculate segment size (no pages in this test)
    final int segmentSize = 0;
    final int totalSize = Binary.LONG_SERIALIZED_SIZE    // txId
        + Binary.LONG_SERIALIZED_SIZE                    // timestamp
        + Binary.INT_SERIALIZED_SIZE                     // page count
        + Binary.INT_SERIALIZED_SIZE                     // segment size
        + segmentSize                                    // pages data
        + Binary.INT_SERIALIZED_SIZE                     // trailing segment size
        + Binary.LONG_SERIALIZED_SIZE;                   // magic number

    final Binary buffer = new Binary(totalSize);
    buffer.putLong(txId);
    buffer.putLong(timestamp);
    buffer.putInt(pageCount);
    buffer.putInt(segmentSize);
    // No page data for pageCount=0
    buffer.putInt(segmentSize);
    buffer.putLong(WALFile.MAGIC_NUMBER);

    buffer.flip();
    return buffer;
  }

  /**
   * Creates a valid WAL buffer containing exactly one page.
   * Per-page format: [fileId:4][pageNumber:4][changesFrom:4][changesTo:4][pageVersion:4][pageSize:4][delta bytes]
   */
  private Binary createTestWalBufferWithOnePage(final long txId, final long timestamp, final int fileId, final int changesFrom,
      final int changesTo) {
    final int deltaSize = changesTo - changesFrom + 1;
    final int pageDataSize = 6 * Binary.INT_SERIALIZED_SIZE + deltaSize;
    final int segmentSize = pageDataSize;

    final int totalSize = Binary.LONG_SERIALIZED_SIZE    // txId
        + Binary.LONG_SERIALIZED_SIZE                    // timestamp
        + Binary.INT_SERIALIZED_SIZE                     // page count
        + Binary.INT_SERIALIZED_SIZE                     // segment size
        + segmentSize                                    // pages data
        + Binary.INT_SERIALIZED_SIZE                     // trailing segment size
        + Binary.LONG_SERIALIZED_SIZE;                   // magic number

    final Binary buffer = new Binary(totalSize);
    buffer.putLong(txId);
    buffer.putLong(timestamp);
    buffer.putInt(1);            // pageCount
    buffer.putInt(segmentSize);

    // Page data
    buffer.putInt(fileId);
    buffer.putInt(0);            // pageNumber
    buffer.putInt(changesFrom);
    buffer.putInt(changesTo);
    buffer.putInt(1);            // currentPageVersion
    buffer.putInt(1024);         // currentPageSize
    buffer.putByteArray(new byte[deltaSize]);

    buffer.putInt(segmentSize);
    buffer.putLong(WALFile.MAGIC_NUMBER);

    buffer.flip();
    return buffer;
  }

  /** Returns a new Binary containing only the first {@code length} bytes of {@code source}. */
  private Binary sliceBuffer(final Binary source, final int length) {
    final byte[] data = new byte[length];
    source.getByteBuffer().position(0);
    source.getByteBuffer().get(data, 0, length);
    return new Binary(data);
  }
}
