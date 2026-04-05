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
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
    final byte[] serialized = RaftLogEntry.serializeTransaction("testDb", bucketDelta, walBuffer, null, null, null);

    // Verify type marker
    assertThat(RaftLogEntry.readType(ByteBuffer.wrap(serialized))).isEqualTo(RaftLogEntry.EntryType.TRANSACTION);

    // Deserialize
    final RaftLogEntry.TransactionEntry entry = RaftLogEntry.deserializeTransaction(serialized);

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
        filesToRemove);

    // Deserialize
    final RaftLogEntry.TransactionEntry entry = RaftLogEntry.deserializeTransaction(serialized);

    assertThat(entry.databaseName()).isEqualTo("myDb");
    assertThat(entry.schemaJson()).isEqualTo(schemaJson);
    assertThat(entry.filesToAdd()).hasSize(2);
    assertThat(entry.filesToAdd().get(10)).isEqualTo("bucket_V1_0.pcf");
    assertThat(entry.filesToAdd().get(11)).isNull();
    assertThat(entry.filesToRemove()).hasSize(1);
    assertThat(entry.filesToRemove().get(5)).isEqualTo("old_index.idx");
  }

  @Test
  void testTransactionForwardSerializationRoundTrip() {
    final Binary walBuffer = createTestWalBuffer(77L, 5555L, 0);

    final Map<Integer, Integer> bucketDelta = new HashMap<>();
    bucketDelta.put(3, 10);

    final byte[] indexChanges = new byte[] { 1, 2, 3, 4, 5 };

    // Serialize
    final byte[] serialized = RaftLogEntry.serializeTransactionForward("forwardDb", bucketDelta, walBuffer, indexChanges);

    // Verify type marker
    assertThat(RaftLogEntry.readType(ByteBuffer.wrap(serialized))).isEqualTo(RaftLogEntry.EntryType.TRANSACTION_FORWARD);

    // Deserialize
    final RaftLogEntry.TransactionForwardEntry entry = RaftLogEntry.deserializeTransactionForward(serialized);

    assertThat(entry.databaseName()).isEqualTo("forwardDb");
    assertThat(entry.bucketRecordDelta()).hasSize(1);
    assertThat(entry.bucketRecordDelta().get(3)).isEqualTo(10);
    assertThat(entry.indexChanges()).isEqualTo(indexChanges);
  }

  @Test
  void testTransactionForwardWithoutIndexChanges() {
    final Binary walBuffer = createTestWalBuffer(88L, 6666L, 0);

    final Map<Integer, Integer> bucketDelta = new HashMap<>();

    // Serialize without index changes
    final byte[] serialized = RaftLogEntry.serializeTransactionForward("db", bucketDelta, walBuffer, null);

    // Deserialize
    final RaftLogEntry.TransactionForwardEntry entry = RaftLogEntry.deserializeTransactionForward(serialized);

    assertThat(entry.databaseName()).isEqualTo("db");
    assertThat(entry.indexChanges()).isNull();
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
}
