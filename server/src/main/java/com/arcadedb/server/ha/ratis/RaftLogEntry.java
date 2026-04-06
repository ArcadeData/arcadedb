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

import com.arcadedb.compression.CompressionFactory;
import com.arcadedb.database.Binary;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Defines the format of entries stored in the Ratis log. Each entry represents a replicated operation
 * (transaction, schema change, or command) that must be applied to all nodes in the same order.
 *
 * <p>Wire format:
 * <pre>
 *   [1 byte: type] [variable: type-specific payload]
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RaftLogEntry {

  public enum EntryType {
    /** Replicate a committed transaction (WAL page diffs + optional schema changes). */
    TRANSACTION((byte) 1),
    /** Forward a write from a non-leader node (includes index key changes for constraint validation). */
    TRANSACTION_FORWARD((byte) 2),
    /** Forward a command (SQL/Cypher) to the leader via the query() path (not logged in Raft). */
    COMMAND_FORWARD((byte) 'C');

    private final byte code;

    EntryType(final byte code) {
      this.code = code;
    }

    public byte code() {
      return code;
    }

    public static EntryType fromCode(final byte code) {
      return switch (code) {
        case 1 -> TRANSACTION;
        case 2 -> TRANSACTION_FORWARD;
        case 'C' -> COMMAND_FORWARD;
        default -> throw new IllegalArgumentException("Unknown RaftLogEntry type code: " + code);
      };
    }
  }

  // -- Serialization helpers for TRANSACTION entries --

  /**
   * Serializes a transaction into a byte buffer suitable for the Ratis log.
   *
   * @param databaseName      target database
   * @param bucketRecordDelta per-bucket record count changes
   * @param walBuffer         the WAL changes buffer (from commit1stPhase)
   * @param schemaJson        optional schema JSON (null if no schema change)
   * @param filesToAdd        optional files to add (null if no structural change)
   * @param filesToRemove     optional files to remove (null if no structural change)
   * @return serialized bytes
   */
  public static byte[] serializeTransaction(final String databaseName, final Map<Integer, Integer> bucketRecordDelta,
      final Binary walBuffer, final String schemaJson, final Map<Integer, String> filesToAdd,
      final Map<Integer, String> filesToRemove) {

    final Binary stream = new Binary(walBuffer.size() + 256);
    stream.putByte(EntryType.TRANSACTION.code());
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

  /**
   * Serializes a transaction forward request (from a non-leader node) into a byte buffer.
   *
   * @param databaseName      target database
   * @param bucketRecordDelta per-bucket record count changes
   * @param walBuffer         the WAL changes buffer
   * @param indexChanges      serialized index key changes for constraint validation (already compressed), or null
   * @return serialized bytes
   */
  public static byte[] serializeTransactionForward(final String databaseName, final Map<Integer, Integer> bucketRecordDelta,
      final Binary walBuffer, final byte[] indexChanges) {

    final Binary stream = new Binary(walBuffer.size() + 256);
    stream.putByte(EntryType.TRANSACTION_FORWARD.code());
    writeCommonTransactionFields(stream, databaseName, bucketRecordDelta, walBuffer);

    // Index changes (optional, for unique constraint validation on leader)
    if (indexChanges != null) {
      stream.putByte((byte) 1);
      stream.putBytes(indexChanges, indexChanges.length);
    } else
      stream.putByte((byte) 0);

    return toByteArray(stream);
  }

  private static void writeCommonTransactionFields(final Binary stream, final String databaseName,
      final Map<Integer, Integer> bucketRecordDelta, final Binary walBuffer) {
    // Database name
    stream.putString(databaseName);

    // WAL changes (compressed)
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

  // -- Deserialization --

  /** Parsed transaction entry ready for application. */
  public record TransactionEntry(String databaseName, int uncompressedLength, Binary walBuffer,
      Map<Integer, Integer> bucketRecordDelta, String schemaJson, Map<Integer, String> filesToAdd,
      Map<Integer, String> filesToRemove) {
  }

  /** Parsed transaction forward entry. */
  public record TransactionForwardEntry(String databaseName, int uncompressedLength, Binary walBuffer,
      Map<Integer, Integer> bucketRecordDelta, byte[] indexChanges) {
  }

  public static EntryType readType(final ByteBuffer buffer) {
    return EntryType.fromCode(buffer.get(0));
  }

  public static TransactionEntry deserializeTransaction(final byte[] data) {
    final Binary stream = new Binary(data);
    stream.getByte(); // skip type marker

    final String databaseName = stream.getString();

    final int uncompressedLength = stream.getInt();
    final Binary walBuffer = CompressionFactory.getDefault().decompress(new Binary(stream.getBytes()), uncompressedLength);

    final int deltaSize = stream.getInt();
    final Map<Integer, Integer> bucketRecordDelta = new HashMap<>(deltaSize);
    for (int i = 0; i < deltaSize; i++)
      bucketRecordDelta.put(stream.getInt(), stream.getInt());

    String schemaJson = null;
    Map<Integer, String> filesToAdd = null;
    Map<Integer, String> filesToRemove = null;

    if (stream.getByte() == 1) {
      schemaJson = stream.getString();
      filesToAdd = readFileMap(stream);
      filesToRemove = readFileMap(stream);
    }

    return new TransactionEntry(databaseName, uncompressedLength, walBuffer, bucketRecordDelta, schemaJson, filesToAdd,
        filesToRemove);
  }

  public static TransactionForwardEntry deserializeTransactionForward(final byte[] data) {
    final Binary stream = new Binary(data);
    stream.getByte(); // skip type marker

    final String databaseName = stream.getString();

    final int uncompressedLength = stream.getInt();
    final Binary walBuffer = CompressionFactory.getDefault().decompress(new Binary(stream.getBytes()), uncompressedLength);

    final int deltaSize = stream.getInt();
    final Map<Integer, Integer> bucketRecordDelta = new HashMap<>(deltaSize);
    for (int i = 0; i < deltaSize; i++)
      bucketRecordDelta.put(stream.getInt(), stream.getInt());

    byte[] indexChanges = null;
    if (stream.getByte() == 1)
      indexChanges = stream.getBytes();

    return new TransactionForwardEntry(databaseName, uncompressedLength, walBuffer, bucketRecordDelta, indexChanges);
  }

  // -- Command forwarding (via Ratis query(), not logged) --

  /** Serializes a command forward request for execution on the leader via the state machine query() path. */
  public static byte[] serializeCommandForward(final String databaseName, final String language, final String command,
      final Map<String, Object> namedParams, final Object[] positionalParams) {
    final Binary stream = new Binary(256);
    stream.putByte(EntryType.COMMAND_FORWARD.code()); // Command forward marker
    stream.putString(databaseName);
    stream.putString(language);
    stream.putString(command);

    // Named params as binary key-value pairs
    if (namedParams != null && !namedParams.isEmpty()) {
      stream.putInt(namedParams.size());
      for (final Map.Entry<String, Object> entry : namedParams.entrySet()) {
        stream.putString(entry.getKey());
        writeValue(stream, entry.getValue());
      }
    } else
      stream.putInt(0);

    // Positional params as binary values
    if (positionalParams != null && positionalParams.length > 0) {
      stream.putInt(positionalParams.length);
      for (final Object p : positionalParams)
        writeValue(stream, p);
    } else
      stream.putInt(0);

    stream.flip();
    final byte[] result = new byte[stream.size()];
    stream.getByteBuffer().get(result);
    return result;
  }

  /** Parsed command forward request. */
  public record CommandForwardEntry(String databaseName, String language, String command,
      Map<String, Object> namedParams, Object[] positionalParams) {
  }

  public static CommandForwardEntry deserializeCommandForward(final byte[] data) {
    final Binary stream = new Binary(data);
    stream.getByte(); // skip marker

    final String databaseName = stream.getString();
    final String language = stream.getString();
    final String command = stream.getString();

    final int namedCount = stream.getInt();
    Map<String, Object> namedParams = null;
    if (namedCount > 0) {
      namedParams = new LinkedHashMap<>(namedCount);
      for (int i = 0; i < namedCount; i++)
        namedParams.put(stream.getString(), readValue(stream));
    }

    final int positionalCount = stream.getInt();
    Object[] positionalParams = null;
    if (positionalCount > 0) {
      positionalParams = new Object[positionalCount];
      for (int i = 0; i < positionalCount; i++)
        positionalParams[i] = readValue(stream);
    }

    return new CommandForwardEntry(databaseName, language, command, namedParams, positionalParams);
  }

  /** Serializes a command result (ResultSet) into a binary format. */
  public static byte[] serializeCommandResult(final com.arcadedb.query.sql.executor.ResultSet rs) {
    final Binary stream = new Binary(1024);
    stream.putByte((byte) 'R'); // Result marker
    // Collect all results first (ResultSet is consumed once)
    final List<Map<String, Object>> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next().toMap());
    stream.putInt(rows.size());
    for (final Map<String, Object> row : rows) {
      stream.putInt(row.size());
      for (final Map.Entry<String, Object> entry : row.entrySet()) {
        stream.putString(entry.getKey());
        writeValue(stream, entry.getValue());
      }
    }
    stream.flip();
    final byte[] result = new byte[stream.size()];
    stream.getByteBuffer().get(result);
    return result;
  }

  /** Deserializes a command result from binary format into a list of property maps. */
  public static List<Map<String, Object>> deserializeCommandResult(final byte[] data) {
    final Binary stream = new Binary(data);
    stream.getByte(); // skip marker
    final int rowCount = stream.getInt();
    final List<Map<String, Object>> rows = new ArrayList<>(rowCount);
    for (int r = 0; r < rowCount; r++) {
      final int propCount = stream.getInt();
      final Map<String, Object> row = new LinkedHashMap<>(propCount);
      for (int p = 0; p < propCount; p++) {
        final String key = stream.getString();
        final Object value = readValue(stream);
        row.put(key, value);
      }
      rows.add(row);
    }
    return rows;
  }

  private static void writeValue(final Binary stream, final Object value) {
    if (value == null) {
      stream.putByte((byte) 0);
    } else if (value instanceof String s) {
      stream.putByte((byte) 1);
      stream.putString(s);
    } else if (value instanceof Integer i) {
      stream.putByte((byte) 2);
      stream.putInt(i);
    } else if (value instanceof Long l) {
      stream.putByte((byte) 3);
      stream.putLong(l);
    } else if (value instanceof Double d) {
      stream.putByte((byte) 4);
      stream.putLong(Double.doubleToLongBits(d));
    } else if (value instanceof Float f) {
      stream.putByte((byte) 5);
      stream.putInt(Float.floatToIntBits(f));
    } else if (value instanceof Boolean b) {
      stream.putByte((byte) 6);
      stream.putByte((byte) (b ? 1 : 0));
    } else {
      // Fallback: serialize as string
      stream.putByte((byte) 1);
      stream.putString(value.toString());
    }
  }

  private static Object readValue(final Binary stream) {
    final byte type = stream.getByte();
    return switch (type) {
      case 0 -> null;
      case 1 -> stream.getString();
      case 2 -> stream.getInt();
      case 3 -> stream.getLong();
      case 4 -> Double.longBitsToDouble(stream.getLong());
      case 5 -> Float.intBitsToFloat(stream.getInt());
      case 6 -> stream.getByte() == 1;
      default -> null;
    };
  }

  // -- Internal helpers --

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

  private static Map<Integer, String> readFileMap(final Binary stream) {
    final int count = stream.getInt();
    final Map<Integer, String> result = new HashMap<>(count);
    for (int i = 0; i < count; i++) {
      final int fileId = stream.getInt();
      final boolean notNull = stream.getByte() == 1;
      result.put(fileId, notNull ? stream.getString() : null);
    }
    return result;
  }
}
