/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Codec for encoding and decoding Raft log entries. Converts WAL transaction data
 * and schema commands into Ratis ByteString representations and back.
 */
public final class RaftLogEntryCodec {

  /** Maximum allowed size for a single byte array allocation during decoding (64 MB). */
  static final int MAX_ENTRY_BYTES = 64 * 1024 * 1024;

  /** Maximum allowed element count for collections during decoding. */
  static final int MAX_COLLECTION_SIZE = 1_000_000;

  private RaftLogEntryCodec() {
    // utility class
  }

  private static void checkByteLength(final int length, final String context) {
    if (length < 0 || length > MAX_ENTRY_BYTES)
      throw new IllegalStateException(
          "Invalid byte length " + length + " in " + context + " (max " + MAX_ENTRY_BYTES + ")");
  }

  private static void checkCollectionSize(final int size, final String context) {
    if (size < 0 || size > MAX_COLLECTION_SIZE)
      throw new IllegalStateException(
          "Invalid collection size " + size + " in " + context + " (max " + MAX_COLLECTION_SIZE + ")");
  }

  public record DecodedEntry(
      RaftLogEntryType type,
      String databaseName,
      byte[] walData,
      Map<Integer, Integer> bucketRecordDelta,
      String schemaJson,
      Map<Integer, String> filesToAdd,
      Map<Integer, String> filesToRemove,
      List<byte[]> walEntries,
      List<Map<Integer, Integer>> bucketDeltas,
      String usersJson,
      boolean forceSnapshot
  ) {
  }

  /**
   * Encodes a transaction entry into a ByteString.
   * <p>
   * Binary format: type byte, databaseName (UTF), walData length (int), walData bytes,
   * bucketDelta count (int), followed by pairs of bucketId (int) and delta (int).
   */
  public static ByteString encodeTxEntry(final String databaseName, final byte[] walData,
      final Map<Integer, Integer> bucketRecordDelta) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);

      dos.writeByte(RaftLogEntryType.TX_ENTRY.getId());
      dos.writeUTF(databaseName);

      final byte[] compressed = CompressionFactory.getDefault().compress(walData);
      dos.writeInt(walData.length);       // uncompressed length (for decompression)
      dos.writeInt(compressed.length);    // compressed length
      dos.write(compressed);

      dos.writeInt(bucketRecordDelta.size());
      for (final Map.Entry<Integer, Integer> entry : bucketRecordDelta.entrySet()) {
        dos.writeInt(entry.getKey());
        dos.writeInt(entry.getValue());
      }

      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to encode TX entry", e);
    }
  }

  /**
   * Encodes a schema entry into a ByteString.
   * <p>
   * Binary format: type byte, databaseName (UTF), schemaJson (UTF),
   * filesToAdd map, filesToRemove map,
   * walEntries count (int), then for each WAL entry: length (int) + bytes,
   * then for each bucket delta: entry count (int) + pairs of fileId (int) and delta (int).
   */
  public static ByteString encodeSchemaEntry(final String databaseName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove,
      final List<byte[]> walEntries, final List<Map<Integer, Integer>> bucketDeltas) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);

      dos.writeByte(RaftLogEntryType.SCHEMA_ENTRY.getId());
      dos.writeUTF(databaseName);
      dos.writeUTF(schemaJson != null ? schemaJson : "");

      writeFileMap(dos, filesToAdd);
      writeFileMap(dos, filesToRemove);

      final int walCount = walEntries != null ? walEntries.size() : 0;
      dos.writeInt(walCount);
      for (int i = 0; i < walCount; i++) {
        final byte[] walData = walEntries.get(i);
        final byte[] compressedWal = CompressionFactory.getDefault().compress(walData);
        dos.writeInt(walData.length);         // uncompressed length
        dos.writeInt(compressedWal.length);   // compressed length
        dos.write(compressedWal);

        final Map<Integer, Integer> delta = (bucketDeltas != null && i < bucketDeltas.size())
            ? bucketDeltas.get(i)
            : Collections.emptyMap();
        dos.writeInt(delta.size());
        for (final Map.Entry<Integer, Integer> e : delta.entrySet()) {
          dos.writeInt(e.getKey());
          dos.writeInt(e.getValue());
        }
      }

      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to encode SCHEMA entry", e);
    }
  }

  /**
   * Convenience overload with no embedded WAL entries (for schema-only changes).
   */
  public static ByteString encodeSchemaEntry(final String databaseName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove) {
    return encodeSchemaEntry(databaseName, schemaJson, filesToAdd, filesToRemove, Collections.emptyList(), Collections.emptyList());
  }

  /**
   * Encodes an install-database entry into a ByteString.
   * <p>
   * Binary format: type byte, databaseName (UTF), forceSnapshot (boolean).
   */
  public static ByteString encodeInstallDatabaseEntry(final String databaseName) {
    return encodeInstallDatabaseEntry(databaseName, false);
  }

  /**
   * Encodes an install-database entry with an explicit forceSnapshot flag.
   * When {@code forceSnapshot} is true, replicas pull a fresh snapshot from the
   * leader even if the database already exists locally (used for restore).
   */
  public static ByteString encodeInstallDatabaseEntry(final String databaseName, final boolean forceSnapshot) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);

      dos.writeByte(RaftLogEntryType.INSTALL_DATABASE_ENTRY.getId());
      dos.writeUTF(databaseName);
      dos.writeBoolean(forceSnapshot);

      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to encode INSTALL_DATABASE entry", e);
    }
  }

  /**
   * Encodes a security-users entry into a ByteString.
   * <p>
   * Binary format: type byte, empty databaseName (UTF), jsonLength (int), UTF-8 bytes.
   * The empty databaseName slot keeps the decoder symmetric with other entry types.
   */
  public static ByteString encodeSecurityUsersEntry(final String usersJson) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);

      dos.writeByte(RaftLogEntryType.SECURITY_USERS_ENTRY.getId());
      dos.writeUTF("");
      final byte[] bytes = usersJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      dos.writeInt(bytes.length);
      dos.write(bytes);

      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to encode SECURITY_USERS entry", e);
    }
  }

  /**
   * Encodes a drop-database entry into a ByteString.
   * <p>
   * Binary format: type byte, databaseName (UTF).
   */
  public static ByteString encodeDropDatabaseEntry(final String databaseName) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);

      dos.writeByte(RaftLogEntryType.DROP_DATABASE_ENTRY.getId());
      dos.writeUTF(databaseName);

      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to encode DROP_DATABASE entry", e);
    }
  }

  /**
   * Decodes a ByteString back into a DecodedEntry.
   */
  public static DecodedEntry decode(final ByteString data) {
    try (final InputStream input = data.newInput();
        final DataInputStream dis = new DataInputStream(input)) {

      final byte typeByte = dis.readByte();
      final RaftLogEntryType type = RaftLogEntryType.fromId(typeByte);
      if (type == null)
        return new DecodedEntry(null, null, null, null, null, null, null, null, null, null, false);
      final String databaseName = dis.readUTF();

      final DecodedEntry result = switch (type) {
        case TX_ENTRY -> decodeTxEntry(dis, databaseName);
        case SCHEMA_ENTRY -> decodeSchemaEntry(dis, databaseName);
        case INSTALL_DATABASE_ENTRY -> decodeInstallDatabaseEntry(dis, databaseName);
        case DROP_DATABASE_ENTRY -> new DecodedEntry(RaftLogEntryType.DROP_DATABASE_ENTRY, databaseName,
            null, null, null, null, null, null, null, null, false);
        case SECURITY_USERS_ENTRY -> decodeSecurityUsersEntry(dis);
      };

      // Trailing-byte validation: detect truncated or corrupted entries.
      // SCHEMA_ENTRY is excluded because older entries may lack the embedded WAL section
      // and the decoder already handles that gracefully via IOException catch.
      if (type != RaftLogEntryType.SCHEMA_ENTRY && dis.available() > 0)
        throw new IllegalStateException(
            "Corrupted Raft log entry: " + dis.available() + " trailing bytes after " + type + " decode");

      return result;
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to decode Raft log entry", e);
    }
  }

  private static DecodedEntry decodeTxEntry(final DataInputStream dis, final String databaseName) throws IOException {
    final int uncompressedLength = dis.readInt();
    checkByteLength(uncompressedLength, "TX_ENTRY uncompressed WAL");
    final int compressedLength = dis.readInt();
    checkByteLength(compressedLength, "TX_ENTRY compressed WAL");
    final byte[] compressed = new byte[compressedLength];
    dis.readFully(compressed);
    final byte[] walData = CompressionFactory.getDefault().decompress(compressed, uncompressedLength);

    final int deltaCount = dis.readInt();
    checkCollectionSize(deltaCount, "TX_ENTRY bucket deltas");
    final Map<Integer, Integer> bucketRecordDelta = HashMap.newHashMap(deltaCount);
    for (int i = 0; i < deltaCount; i++) {
      final int bucketId = dis.readInt();
      final int delta = dis.readInt();
      bucketRecordDelta.put(bucketId, delta);
    }

    return new DecodedEntry(RaftLogEntryType.TX_ENTRY, databaseName, walData, bucketRecordDelta,
        null, null, null, null, null, null, false);
  }

  private static DecodedEntry decodeSchemaEntry(final DataInputStream dis, final String databaseName) throws IOException {
    final String schemaJson = dis.readUTF();
    final Map<Integer, String> filesToAdd = readFileMap(dis);
    final Map<Integer, String> filesToRemove = readFileMap(dis);

    // Read embedded WAL entries; older entries without this section are handled gracefully
    List<byte[]> walEntries = Collections.emptyList();
    List<Map<Integer, Integer>> bucketDeltas = Collections.emptyList();
    try {
      final int walCount = dis.readInt();
      checkCollectionSize(walCount, "SCHEMA_ENTRY WAL entries");
      if (walCount > 0) {
        walEntries = new ArrayList<>(walCount);
        bucketDeltas = new ArrayList<>(walCount);
        for (int i = 0; i < walCount; i++) {
          final int walUncompressedLen = dis.readInt();
          checkByteLength(walUncompressedLen, "SCHEMA_ENTRY WAL uncompressed");
          final int walCompressedLen = dis.readInt();
          checkByteLength(walCompressedLen, "SCHEMA_ENTRY WAL compressed");
          final byte[] walCompressed = new byte[walCompressedLen];
          dis.readFully(walCompressed);
          final byte[] walData = CompressionFactory.getDefault().decompress(walCompressed, walUncompressedLen);
          walEntries.add(walData);

          final int deltaCount = dis.readInt();
          checkCollectionSize(deltaCount, "SCHEMA_ENTRY bucket deltas");
          final Map<Integer, Integer> delta = HashMap.newHashMap(deltaCount);
          for (int j = 0; j < deltaCount; j++)
            delta.put(dis.readInt(), dis.readInt());
          bucketDeltas.add(delta);
        }
      }
    } catch (final IOException ignored) {
      // Older log entries without embedded WAL section - treat as empty
    }

    return new DecodedEntry(RaftLogEntryType.SCHEMA_ENTRY, databaseName, null, null,
        schemaJson, filesToAdd, filesToRemove, walEntries, bucketDeltas, null, false);
  }

  private static DecodedEntry decodeInstallDatabaseEntry(final DataInputStream dis, final String databaseName) throws IOException {
    // Length-based detection of the trailing forceSnapshot flag.
    // Legacy entries (pre-forceSnapshot codec) have no trailing byte; they decode as forceSnapshot=false.
    boolean forceSnapshot = false;
    if (dis.available() > 0) {
      forceSnapshot = dis.readBoolean();
    }
    return new DecodedEntry(RaftLogEntryType.INSTALL_DATABASE_ENTRY, databaseName,
        null, null, null, null, null, null, null, null, forceSnapshot);
  }

  private static DecodedEntry decodeSecurityUsersEntry(final DataInputStream dis) throws IOException {
    final int length = dis.readInt();
    checkByteLength(length, "SECURITY_USERS_ENTRY");
    final byte[] bytes = new byte[length];
    dis.readFully(bytes);
    final String usersJson = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    return new DecodedEntry(RaftLogEntryType.SECURITY_USERS_ENTRY, "",
        null, null, null, null, null, null, null, usersJson, false);
  }

  private static void writeFileMap(final DataOutputStream dos, final Map<Integer, String> fileMap) throws IOException {
    if (fileMap == null) {
      dos.writeInt(0);
      return;
    }
    dos.writeInt(fileMap.size());
    for (final Map.Entry<Integer, String> entry : fileMap.entrySet()) {
      dos.writeInt(entry.getKey());
      final String value = entry.getValue();
      final boolean hasValue = value != null;
      dos.writeBoolean(hasValue);
      if (hasValue)
        dos.writeUTF(value);
    }
  }

  private static Map<Integer, String> readFileMap(final DataInputStream dis) throws IOException {
    final int count = dis.readInt();
    checkCollectionSize(count, "file map");
    final Map<Integer, String> map = HashMap.newHashMap(count);
    for (int i = 0; i < count; i++) {
      final int fileId = dis.readInt();
      final boolean hasValue = dis.readBoolean();
      final String fileName = hasValue ? dis.readUTF() : null;
      map.put(fileId, fileName);
    }
    return map;
  }
}
