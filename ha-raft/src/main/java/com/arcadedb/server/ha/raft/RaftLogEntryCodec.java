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
import com.arcadedb.engine.WALFile;

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

  /**
   * Parsed CREATE_USER or UPDATE_USER entry.
   */
  public record UserEntry(String originPeerId, String userJson) {
  }

  /**
   * Parsed DROP_USER entry.
   */
  public record DropUserEntry(String originPeerId, String userName) {
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
   * <p>This method does not modify the position of {@code walBuffer}.
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
    return toByteArray(stream);
  }

  public static byte[] serializeDropDatabase(final String databaseName, final String originPeerId) {
    final Binary stream = new Binary(64);
    stream.putByte(RaftLogEntryType.DROP_DATABASE.code());
    stream.putString(originPeerId);
    stream.putString(databaseName);
    return toByteArray(stream);
  }

  public static byte[] serializeCreateUser(final String userJson, final String originPeerId) {
    final Binary stream = new Binary(256);
    stream.putByte(RaftLogEntryType.CREATE_USER.code());
    stream.putString(originPeerId);
    stream.putString(userJson);
    return toByteArray(stream);
  }

  public static byte[] serializeUpdateUser(final String userJson, final String originPeerId) {
    final Binary stream = new Binary(256);
    stream.putByte(RaftLogEntryType.UPDATE_USER.code());
    stream.putString(originPeerId);
    stream.putString(userJson);
    return toByteArray(stream);
  }

  public static byte[] serializeDropUser(final String userName, final String originPeerId) {
    final Binary stream = new Binary(64);
    stream.putByte(RaftLogEntryType.DROP_USER.code());
    stream.putString(originPeerId);
    stream.putString(userName);
    return toByteArray(stream);
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
    if (deltaSize < 0 || deltaSize > MAX_BUCKET_DELTA_ENTRIES)
      throw new IllegalArgumentException(
          "Invalid bucket delta entry count: " + deltaSize + " (max " + MAX_BUCKET_DELTA_ENTRIES + ")");
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

  public static UserEntry deserializeUserEntry(final byte[] data) {
    final Binary stream = new Binary(data);
    stream.getByte(); // skip type byte
    final String originPeerId = readBoundedString(stream);
    final String userJson = readBoundedString(stream);
    return new UserEntry(originPeerId, userJson);
  }

  public static DropUserEntry deserializeDropUser(final byte[] data) {
    final Binary stream = new Binary(data);
    stream.getByte(); // skip type byte
    final String originPeerId = readBoundedString(stream);
    final String userName = readBoundedString(stream);
    return new DropUserEntry(originPeerId, userName);
  }

  // -- Internal helpers --

  // Max allowed sizes for deserialized buffers to prevent OOM from corrupted entries.
  // These bound MAP CARDINALITIES and BYTE LENGTHS - the per-WAL-page byte cap is governed by
  // MAX_UNCOMPRESSED_SIZE, which covers the entire compressed WAL change set (all touched pages
  // across all files combined), not per-page.
  private static final int MAX_UNCOMPRESSED_SIZE     = 256 * 1024 * 1024; // 256 MB - total uncompressed WAL change set
  /** Maximum number of (bucketId, recordCountDelta) entries in the bucketRecordDelta map. Each
   *  entry represents ONE bucket touched by the transaction, not bytes of page data. 1,000,000
   *  is orders of magnitude above any realistic schema (ArcadeDB databases have thousands of
   *  buckets at most); the bound exists solely to cap allocation from a corrupted/adversarial
   *  length prefix. */
  private static final int MAX_BUCKET_DELTA_ENTRIES  = 1_000_000;
  private static final int MAX_FILES_PER_TX          = 65_536;            // max files added/removed in one transaction
  private static final int MAX_STRING_LENGTH         = 64 * 1024 * 1024;  // 64 MB (covers large schema JSON)

  private static void writeCommonTransactionFields(final Binary stream, final String databaseName,
      final Map<Integer, Integer> bucketRecordDelta, final Binary walBuffer) {
    stream.putString(databaseName);

    // WAL changes (compressed). Use a lightweight copy to avoid mutating the caller's buffer position.
    final Binary walSnapshot = walBuffer.copy();
    walSnapshot.rewind();
    final int uncompressedLength = walSnapshot.size();
    final Binary compressed = CompressionFactory.getDefault().compress(walSnapshot);
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
    // Binary#getUnsignedNumber decodes a variable-length integer into the full 64-bit long
    // (it does NOT zigzag-decode or constrain the sign; values like Long.MIN_VALUE round-trip).
    // So a corrupted stream can legitimately produce a negative long here, and the < 0 check
    // is not dead code - it rejects those values before the subsequent cast to int could yield
    // a nonsensical positive allocation size.
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

  // -- WAL Transaction Parsing --

  /**
   * Parses a decompressed WAL buffer into a {@link WALFile.WALTransaction} for replay on followers.
   * Validates header, page entries, footer, and magic number to detect corrupted entries.
   */
  public static WALFile.WALTransaction parseWalTransaction(final Binary buffer) {
    final WALFile.WALTransaction tx = new WALFile.WALTransaction();

    // Minimum header: txId(8) + timestamp(8) + pages(4) + segmentSize(4) = 24 bytes
    final int headerSize = 2 * Binary.LONG_SERIALIZED_SIZE + 2 * Binary.INT_SERIALIZED_SIZE;
    if (buffer.size() < headerSize)
      throw new ReplicationException(
          "Replicated transaction buffer is truncated: expected at least " + headerSize + " header bytes, got " + buffer.size());

    int pos = 0;
    tx.txId = buffer.getLong(pos);
    pos += Binary.LONG_SERIALIZED_SIZE;

    tx.timestamp = buffer.getLong(pos);
    pos += Binary.LONG_SERIALIZED_SIZE;

    final int pages = buffer.getInt(pos);
    pos += Binary.INT_SERIALIZED_SIZE;

    final int segmentSize = buffer.getInt(pos);
    pos += Binary.INT_SERIALIZED_SIZE;

    if (segmentSize < 0 || pos + segmentSize + Binary.LONG_SERIALIZED_SIZE > buffer.size())
      throw new ReplicationException("Replicated transaction buffer is corrupted (segmentSize=" + segmentSize + ")");

    tx.pages = new WALFile.WALPage[pages];

    for (int i = 0; i < pages; ++i) {
      // Validate that the 4 fixed-size header fields (fileId, pageNumber, changesFrom, changesTo) fit
      if (pos + 4 * Binary.INT_SERIALIZED_SIZE > buffer.size())
        throw new ReplicationException("Replicated transaction buffer is corrupted");

      tx.pages[i] = new WALFile.WALPage();
      tx.pages[i].fileId = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;
      tx.pages[i].pageNumber = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;
      tx.pages[i].changesFrom = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;
      tx.pages[i].changesTo = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      final int deltaSize = tx.pages[i].changesTo - tx.pages[i].changesFrom + 1;
      if (deltaSize <= 0)
        throw new ReplicationException(
            "Invalid delta range in replicated transaction: changesFrom=" + tx.pages[i].changesFrom + " changesTo=" + tx.pages[i].changesTo);

      // Validate that the remaining 2 fixed fields + delta bytes fit before reading them
      if (pos + 2 * Binary.INT_SERIALIZED_SIZE + deltaSize > buffer.size())
        throw new ReplicationException("Replicated transaction buffer is corrupted");

      tx.pages[i].currentPageVersion = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;
      tx.pages[i].currentPageSize = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      final byte[] pageData = new byte[deltaSize];
      tx.pages[i].currentContent = new Binary(pageData);
      buffer.getByteArray(pos, pageData, 0, deltaSize);
      pos += deltaSize;
    }

    // Trailing footer: segmentSize(4) + magicNumber(8)
    final int footerSize = Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;
    if (pos + footerSize > buffer.size())
      throw new ReplicationException(
          "Replicated transaction buffer is truncated: expected " + footerSize + " footer bytes at position " + pos + ", buffer size " + buffer.size());

    final int trailingSegmentSize = buffer.getInt(pos);
    pos += Binary.INT_SERIALIZED_SIZE;
    if (trailingSegmentSize != segmentSize)
      throw new ReplicationException(
          "Replicated transaction buffer is corrupted (trailing segment size " + trailingSegmentSize + " != leading " + segmentSize + ")");

    final long magicNumber = buffer.getLong(pos);
    if (magicNumber != WALFile.MAGIC_NUMBER)
      throw new ReplicationException("Replicated transaction buffer is corrupted (bad magic number)");
    pos += Binary.LONG_SERIALIZED_SIZE;

    // The header + pages + footer must have consumed the entire buffer. Any trailing bytes mean
    // the serializer produced more than the parser recognizes (framing mismatch, forward-incompatible
    // writer, or corruption that happened to leave a valid magic at the right offset) and would be
    // silently dropped otherwise.
    if (pos != buffer.size())
      throw new ReplicationException(
          "Replicated transaction buffer has " + (buffer.size() - pos) + " unexpected trailing bytes after footer");

    return tx;
  }

  // -- Internal helpers --

  private static Map<Integer, String> readFileMap(final Binary stream) {
    final int count = stream.getInt();
    if (count < 0 || count > MAX_FILES_PER_TX)
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
