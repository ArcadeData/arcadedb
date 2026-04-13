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

import com.arcadedb.compression.CompressionFactory;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializes and deserializes Raft log entries. Each entry represents a replicated operation
 * (transaction, schema change, or command) that must be applied to all nodes in the same order.
 *
 * <p>Wire format:
 * <pre>
 *   [1 byte: type] [variable: type-specific payload]
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RaftLogEntryCodec {

  private RaftLogEntryCodec() {
  }

  // -- Result records --

  /**
   * Parsed transaction entry ready for application.
   */
  public record TransactionEntry(String originPeerId, String databaseName, int uncompressedLength, Binary walBuffer,
                                 Map<Integer, Integer> bucketRecordDelta, String schemaJson,
                                 Map<Integer, String> filesToAdd, Map<Integer, String> filesToRemove) {
  }

  /**
   * Parsed CREATE_DATABASE entry.
   */
  public record CreateDatabaseEntry(String originPeerId, String databaseName) {
  }

  /**
   * Parsed DROP_DATABASE entry.
   */
  public record DropDatabaseEntry(String originPeerId, String databaseName) {
  }

  // -- Serialization --

  /**
   * Serializes a transaction into a byte buffer suitable for the Ratis log.
   *
   * @param databaseName      target database
   * @param bucketRecordDelta per-bucket record count changes
   * @param walBuffer         the WAL changes buffer (from commit1stPhase)
   * @param schemaJson        optional schema JSON (null if no schema change)
   * @param filesToAdd        optional files to add (null if no structural change)
   * @param filesToRemove     optional files to remove (null if no structural change)
   * @param originPeerId      the peer ID of the node that originated this transaction
   * @return serialized bytes
   *
   * <p><b>Note:</b> {@code walBuffer} is rewound and fully consumed by this method.
   * The caller must not read from it after this call returns.
   */
  public static byte[] serializeTransaction(final String databaseName, final Map<Integer, Integer> bucketRecordDelta,
      final Binary walBuffer, final String schemaJson, final Map<Integer, String> filesToAdd,
      final Map<Integer, String> filesToRemove, final String originPeerId) {

    final Binary stream = new Binary(walBuffer.size() + 256);
    stream.putByte(RaftLogEntryType.TRANSACTION.code());
    stream.putString(originPeerId);
    writeCommonTransactionFields(stream, databaseName, bucketRecordDelta, walBuffer);

    // Schema change (optional)
    final boolean hasSchemaChange = schemaJson != null;
    stream.putByte((byte) (hasSchemaChange ? 1 : 0));
    if (hasSchemaChange) {
      stream.putString(schemaJson);
      writeFileMap(stream, filesToAdd);
      writeFileMap(stream, filesToRemove);
    }

    return toByteArray(stream);
  }

  public static byte[] serializeCreateDatabase(final String databaseName, final String originPeerId) {
    final Binary stream = new Binary(64);
    stream.putByte(RaftLogEntryType.CREATE_DATABASE.code());
    stream.putString(originPeerId);
    stream.putString(databaseName);
    stream.flip();
    return stream.toByteArray();
  }

  public static byte[] serializeDropDatabase(final String databaseName, final String originPeerId) {
    final Binary stream = new Binary(64);
    stream.putByte(RaftLogEntryType.DROP_DATABASE.code());
    stream.putString(originPeerId);
    stream.putString(databaseName);
    stream.flip();
    return stream.toByteArray();
  }

  // -- Deserialization --

  public static RaftLogEntryType readType(final ByteBuffer buffer) {
    return RaftLogEntryType.fromCode(buffer.get(0));
  }

  public static TransactionEntry deserializeTransaction(final byte[] data) {
    final Binary stream = new Binary(data);
    stream.getByte(); // skip type marker

    final String originPeerId = readBoundedString(stream);
    final String databaseName = readBoundedString(stream);

    final int uncompressedLength = stream.getInt();
    if (uncompressedLength < 0 || uncompressedLength > MAX_UNCOMPRESSED_SIZE)
      throw new IllegalArgumentException("Invalid WAL uncompressed length: " + uncompressedLength);
    final Binary walBuffer = CompressionFactory.getDefault().decompress(new Binary(stream.getBytes()),
        uncompressedLength);

    final int deltaSize = stream.getInt();
    if (deltaSize < 0 || deltaSize > MAX_DELTA_SIZE)
      throw new IllegalArgumentException("Invalid bucket delta size: " + deltaSize);
    final Map<Integer, Integer> bucketRecordDelta = new HashMap<>(deltaSize);
    for (int i = 0; i < deltaSize; i++)
      bucketRecordDelta.put(stream.getInt(), stream.getInt());

    String schemaJson = null;
    Map<Integer, String> filesToAdd = null;
    Map<Integer, String> filesToRemove = null;

    if (stream.getByte() == 1) {
      schemaJson = readBoundedString(stream);
      filesToAdd = readFileMap(stream);
      filesToRemove = readFileMap(stream);
    }

    return new TransactionEntry(originPeerId, databaseName, uncompressedLength, walBuffer, bucketRecordDelta,
        schemaJson, filesToAdd, filesToRemove);
  }

  public static CreateDatabaseEntry deserializeCreateDatabase(final byte[] data) {
    final Binary stream = new Binary(data);
    stream.getByte(); // skip type byte
    final String originPeerId = readBoundedString(stream);
    final String databaseName = readBoundedString(stream);
    return new CreateDatabaseEntry(originPeerId, databaseName);
  }

  public static DropDatabaseEntry deserializeDropDatabase(final byte[] data) {
    final Binary stream = new Binary(data);
    stream.getByte(); // skip type byte
    final String originPeerId = readBoundedString(stream);
    final String databaseName = readBoundedString(stream);
    return new DropDatabaseEntry(originPeerId, databaseName);
  }

  // -- Internal helpers --

  // Max allowed sizes for deserialized buffers to prevent OOM from corrupted entries
  private static final int MAX_UNCOMPRESSED_SIZE = 256 * 1024 * 1024; // 256 MB
  private static final int MAX_DELTA_SIZE        = 1_000_000;
  private static final int MAX_STRING_LENGTH     = 64 * 1024 * 1024;  // 64 MB (covers large schema JSON)

  private static void writeCommonTransactionFields(final Binary stream, final String databaseName,
      final Map<Integer, Integer> bucketRecordDelta, final Binary walBuffer) {
    stream.putString(databaseName);

    // WAL changes (compressed). Rewind mutates the caller's buffer position (intentional, see Javadoc).
    walBuffer.rewind();
    final int uncompressedLength = walBuffer.size();
    final Binary compressed = CompressionFactory.getDefault().compress(walBuffer);
    stream.putInt(uncompressedLength);
    stream.putBytes(compressed.getContent(), compressed.size());

    // Bucket record delta
    stream.putInt(bucketRecordDelta.size());
    for (final Map.Entry<Integer, Integer> entry : bucketRecordDelta.entrySet()) {
      stream.putInt(entry.getKey());
      stream.putInt(entry.getValue());
    }
  }

  private static byte[] toByteArray(final Binary stream) {
    stream.flip();
    final byte[] result = new byte[stream.size()];
    stream.getByteBuffer().get(result);
    return result;
  }

  private static void writeFileMap(final Binary stream, final Map<Integer, String> files) {
    if (files == null) {
      stream.putInt(0);
      return;
    }
    stream.putInt(files.size());
    for (final Map.Entry<Integer, String> entry : files.entrySet()) {
      stream.putInt(entry.getKey());
      stream.putByte((byte) (entry.getValue() != null ? 1 : 0));
      if (entry.getValue() != null)
        stream.putString(entry.getValue());
    }
  }

  /**
   * Reads a length-prefixed string from the stream, validating that the declared length
   * does not exceed the cap or the remaining buffer bytes. Prevents OOM from corrupted entries
   * that declare a huge string length.
   */
  private static String readBoundedString(final Binary stream) {
    final int pos = stream.position();
    final long declaredLength = stream.getUnsignedNumber();
    if (declaredLength < 0 || declaredLength > MAX_STRING_LENGTH)
      throw new IllegalArgumentException(
          "String length " + declaredLength + " exceeds maximum " + MAX_STRING_LENGTH + " at position " + pos);
    final int remaining = stream.size() - stream.position();
    if (declaredLength > remaining)
      throw new IllegalArgumentException(
          "String length " + declaredLength + " exceeds remaining buffer bytes " + remaining + " at position " + pos);
    final byte[] bytes = new byte[(int) declaredLength];
    if (bytes.length > 0)
      stream.getByteArray(bytes);
    return new String(bytes, DatabaseFactory.getDefaultCharset());
  }

  private static Map<Integer, String> readFileMap(final Binary stream) {
    final int count = stream.getInt();
    if (count < 0 || count > MAX_DELTA_SIZE)
      throw new IllegalArgumentException("Invalid file map count: " + count);
    final Map<Integer, String> result = new HashMap<>(count);
    for (int i = 0; i < count; i++) {
      final int fileId = stream.getInt();
      final boolean notNull = stream.getByte() == 1;
      result.put(fileId, notNull ? readBoundedString(stream) : null);
    }
    return result;
  }
}
