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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Binary;
import com.arcadedb.engine.WALFile;
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
class RaftLogEntryCodecTest {

  @Test
  void testTransactionSerializationRoundTrip() {
    final Binary walBuffer = createTestWalBuffer(42L, 1234567890L, 0);

    final Map<Integer, Integer> bucketDelta = new HashMap<>();
    bucketDelta.put(1, 5);
    bucketDelta.put(2, -3);

    final byte[] serialized = RaftLogEntryCodec.serializeTransaction("testDb", bucketDelta, walBuffer, null, null, null, "node-1");

    assertThat(RaftLogEntryCodec.readType(ByteBuffer.wrap(serialized))).isEqualTo(RaftLogEntryType.TRANSACTION);

    final RaftLogEntryCodec.TransactionEntry entry = RaftLogEntryCodec.deserializeTransaction(serialized);

    assertThat(entry.originPeerId()).isEqualTo("node-1");
    assertThat(entry.databaseName()).isEqualTo("testDb");
    assertThat(entry.bucketRecordDelta()).hasSize(2);
    assertThat(entry.bucketRecordDelta().get(1)).isEqualTo(5);
    assertThat(entry.bucketRecordDelta().get(2)).isEqualTo(-3);
    assertThat(entry.schemaJson()).isNull();
    assertThat(entry.filesToAdd()).isNull();
    assertThat(entry.filesToRemove()).isNull();

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

    final byte[] serialized = RaftLogEntryCodec.serializeTransaction("myDb", bucketDelta, walBuffer, schemaJson, filesToAdd,
        filesToRemove, "leader-0");

    final RaftLogEntryCodec.TransactionEntry entry = RaftLogEntryCodec.deserializeTransaction(serialized);

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
  void testSerializeTransactionDoesNotMutateWalBufferPosition() {
    final Binary walBuffer = createTestWalBuffer(1L, 2L, 0);
    final int positionBefore = walBuffer.getByteBuffer().position();

    RaftLogEntryCodec.serializeTransaction("db", Map.of(), walBuffer, null, null, null, "peer-1");

    assertThat(walBuffer.getByteBuffer().position()).isEqualTo(positionBefore);
  }

  @Test
  void testCreateDatabaseSerializationRoundTrip() {
    final byte[] serialized = RaftLogEntryCodec.serializeCreateDatabase("newDb", "leader-node");

    assertThat(RaftLogEntryCodec.readType(ByteBuffer.wrap(serialized))).isEqualTo(RaftLogEntryType.CREATE_DATABASE);

    final RaftLogEntryCodec.CreateDatabaseEntry entry = RaftLogEntryCodec.deserializeCreateDatabase(serialized);

    assertThat(entry.originPeerId()).isEqualTo("leader-node");
    assertThat(entry.databaseName()).isEqualTo("newDb");
  }

  @Test
  void testFromCodeReturnsNullForUnknownType() {
    assertThat(RaftLogEntryType.fromCode((byte) 0)).isNull();
    assertThat(RaftLogEntryType.fromCode((byte) 2)).isEqualTo(RaftLogEntryType.DROP_DATABASE);
    assertThat(RaftLogEntryType.fromCode((byte) 99)).isNull();
    assertThat(RaftLogEntryType.fromCode((byte) -1)).isNull();
  }

  @Test
  void testReadTypeReturnsNullForUnknownType() {
    final ByteBuffer buffer = ByteBuffer.allocate(1);
    buffer.put((byte) 42);
    buffer.flip();
    assertThat(RaftLogEntryCodec.readType(buffer)).isNull();
  }

  @Test
  void testDeserializeTransactionRejectsCorruptedStringLength() {
    final Binary stream = new Binary(64);
    stream.putByte(RaftLogEntryType.TRANSACTION.code()); // type marker

    // Write originPeerId with a varint length that's too large to prevent OOM.
    stream.putByte((byte) 0x80);
    stream.putByte((byte) 0x94);
    stream.putByte((byte) 0xE9);
    stream.putByte((byte) 0xDC);
    stream.putByte((byte) 0x03);

    stream.flip();
    final byte[] data = new byte[stream.size()];
    stream.getByteBuffer().get(data);

    assertThatThrownBy(() -> RaftLogEntryCodec.deserializeTransaction(data))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds");
  }

  @Test
  void testParseWalTransactionRejectsTruncatedHeader() {
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
    final Binary full = createTestWalBufferWithOnePage(1L, 100L, 0, 10, 19);
    final int headerEnd = Binary.LONG_SERIALIZED_SIZE
        + Binary.LONG_SERIALIZED_SIZE
        + Binary.INT_SERIALIZED_SIZE
        + Binary.INT_SERIALIZED_SIZE;

    final Binary truncated = sliceBuffer(full, headerEnd + 8);

    assertThatThrownBy(() -> ArcadeDBStateMachine.parseWalTransaction(truncated))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("corrupted");
  }

  @Test
  void testParseWalTransactionRejectsTruncatedPageDelta() {
    final Binary full = createTestWalBufferWithOnePage(1L, 100L, 0, 10, 19);
    final int headerEnd = Binary.LONG_SERIALIZED_SIZE
        + Binary.LONG_SERIALIZED_SIZE
        + Binary.INT_SERIALIZED_SIZE
        + Binary.INT_SERIALIZED_SIZE;

    final Binary truncated = sliceBuffer(full, headerEnd + 6 * Binary.INT_SERIALIZED_SIZE + 3);

    assertThatThrownBy(() -> ArcadeDBStateMachine.parseWalTransaction(truncated))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("corrupted");
  }

  @Test
  void testParseWalTransactionWithValidPage() {
    final Binary buffer = createTestWalBufferWithOnePage(42L, 999L, 5, 100, 109);

    final WALFile.WALTransaction tx = ArcadeDBStateMachine.parseWalTransaction(buffer);
    assertThat(tx.txId).isEqualTo(42L);
    assertThat(tx.timestamp).isEqualTo(999L);
    assertThat(tx.pages.length).isEqualTo(1);
    assertThat(tx.pages[0].fileId).isEqualTo(5);
    assertThat(tx.pages[0].changesFrom).isEqualTo(100);
    assertThat(tx.pages[0].changesTo).isEqualTo(109);
  }

  private Binary createTestWalBuffer(final long txId, final long timestamp, final int pageCount) {
    final int segmentSize = 0;
    final int totalSize = Binary.LONG_SERIALIZED_SIZE
        + Binary.LONG_SERIALIZED_SIZE
        + Binary.INT_SERIALIZED_SIZE
        + Binary.INT_SERIALIZED_SIZE
        + segmentSize
        + Binary.INT_SERIALIZED_SIZE
        + Binary.LONG_SERIALIZED_SIZE;

    final Binary buffer = new Binary(totalSize);
    buffer.putLong(txId);
    buffer.putLong(timestamp);
    buffer.putInt(pageCount);
    buffer.putInt(segmentSize);
    buffer.putInt(segmentSize);
    buffer.putLong(WALFile.MAGIC_NUMBER);

    buffer.flip();
    return buffer;
  }

  private Binary createTestWalBufferWithOnePage(final long txId, final long timestamp, final int fileId,
      final int changesFrom, final int changesTo) {
    final int deltaSize = changesTo - changesFrom + 1;
    final int pageDataSize = 6 * Binary.INT_SERIALIZED_SIZE + deltaSize;
    final int segmentSize = pageDataSize;

    final int totalSize = Binary.LONG_SERIALIZED_SIZE
        + Binary.LONG_SERIALIZED_SIZE
        + Binary.INT_SERIALIZED_SIZE
        + Binary.INT_SERIALIZED_SIZE
        + segmentSize
        + Binary.INT_SERIALIZED_SIZE
        + Binary.LONG_SERIALIZED_SIZE;

    final Binary buffer = new Binary(totalSize);
    buffer.putLong(txId);
    buffer.putLong(timestamp);
    buffer.putInt(1);
    buffer.putInt(segmentSize);

    buffer.putInt(fileId);
    buffer.putInt(0);
    buffer.putInt(changesFrom);
    buffer.putInt(changesTo);
    buffer.putInt(1);
    buffer.putInt(1024);
    buffer.putByteArray(new byte[deltaSize]);

    buffer.putInt(segmentSize);
    buffer.putLong(WALFile.MAGIC_NUMBER);

    buffer.flip();
    return buffer;
  }

  private Binary sliceBuffer(final Binary source, final int length) {
    final byte[] data = new byte[length];
    source.getByteBuffer().position(0);
    source.getByteBuffer().get(data, 0, length);
    return new Binary(data);
  }
}
