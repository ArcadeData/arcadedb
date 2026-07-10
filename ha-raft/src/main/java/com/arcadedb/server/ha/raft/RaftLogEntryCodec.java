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
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.CRC32;

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
      boolean forceSnapshot,
      // BOOTSTRAP_FINGERPRINT_ENTRY fields (issue #4147). Hex-encoded SHA-256 of the bootstrap
      // source's database files and the corresponding lastTxId. Null/-1 for non-bootstrap entries.
      String bootstrapFingerprint,
      long bootstrapLastTxId,
      // TimeSeries sealed-store blobs embedded in a SCHEMA_ENTRY (issue #4382). Empty for all other
      // entry types and for SCHEMA_ENTRYs produced by nodes that predate this section.
      List<TsSealedBlob> sealedFileBlobs
  ) {
  }

  /**
   * A TimeSeries sealed-store file shipped to followers as part of a compaction/maintenance
   * SCHEMA_ENTRY. The whole file is carried (the smallest safe unit: sealed blocks use cumulative
   * offsets and a rewritten header, so partial appends are unsafe across nodes whose pre-image may
   * differ). The bytes are already decompressed and CRC-verified by the decoder.
   *
   * @param typeName   the TimeSeries type owning the shard
   * @param shardIndex the shard index whose sealed store changed
   * @param fileName   the sealed-store file name relative to the database directory
   * @param bytes      the full sealed-store file content
   */
  public record TsSealedBlob(String typeName, int shardIndex, String fileName, byte[] bytes) {
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
   * Binary format: type byte, databaseName (UTF), schemaJson length (int) and UTF-8 bytes,
   * filesToAdd map, filesToRemove map,
   * walEntries count (int), then for each WAL entry: length (int) + bytes,
   * then for each bucket delta: entry count (int) + pairs of fileId (int) and delta (int).
   * <p>
   * The schemaJson is length-prefixed rather than written via {@code writeUTF} because the
   * modified-UTF-8 format used by {@code DataOutputStream.writeUTF} is capped at 65535 bytes,
   * which realistic schemas (many types) can exceed.
   */
  public static ByteString encodeSchemaEntry(final String databaseName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove,
      final List<byte[]> walEntries, final List<Map<Integer, Integer>> bucketDeltas) {
    return encodeSchemaEntry(databaseName, schemaJson, filesToAdd, filesToRemove, walEntries, bucketDeltas,
        List.of());
  }

  /**
   * Encodes a schema entry, optionally embedding TimeSeries sealed-store blobs (issue #4382).
   * <p>
   * The sealed-blob section is appended AFTER the WAL section as a self-describing trailing section,
   * so older nodes (whose decoder stops after the WAL section) ignore it and never produce it. Each
   * blob carries its type name, shard index, file name, a CRC32 of the uncompressed bytes, and the
   * compressed bytes.
   */
  public static ByteString encodeSchemaEntry(final String databaseName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove,
      final List<byte[]> walEntries, final List<Map<Integer, Integer>> bucketDeltas,
      final List<TsSealedBlob> sealedFileBlobs) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);

      dos.writeByte(RaftLogEntryType.SCHEMA_ENTRY.getId());
      dos.writeUTF(databaseName);
      final byte[] schemaBytes = (schemaJson != null ? schemaJson : "").getBytes(StandardCharsets.UTF_8);
      dos.writeInt(schemaBytes.length);
      dos.write(schemaBytes);

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

        final Map<Integer, Integer> delta = bucketDeltas != null && i < bucketDeltas.size()
            ? bucketDeltas.get(i)
            : Map.of();
        dos.writeInt(delta.size());
        for (final Map.Entry<Integer, Integer> e : delta.entrySet()) {
          dos.writeInt(e.getKey());
          dos.writeInt(e.getValue());
        }
      }

      // TimeSeries sealed-store blob section (trailing, backward/forward compatible).
      final int blobCount = sealedFileBlobs != null ? sealedFileBlobs.size() : 0;
      dos.writeInt(blobCount);
      for (int i = 0; i < blobCount; i++) {
        final TsSealedBlob blob = sealedFileBlobs.get(i);
        final byte[] raw = blob.bytes() != null ? blob.bytes() : new byte[0];
        checkByteLength(raw.length, "SCHEMA_ENTRY sealed blob uncompressed");
        final CRC32 crc = new CRC32();
        crc.update(raw);
        final byte[] compressed = CompressionFactory.getDefault().compress(raw);
        dos.writeUTF(blob.typeName());
        dos.writeInt(blob.shardIndex());
        dos.writeUTF(blob.fileName());
        dos.writeLong(crc.getValue());
        dos.writeInt(raw.length);          // uncompressed length
        dos.writeInt(compressed.length);   // compressed length
        dos.write(compressed);
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
    return encodeSchemaEntry(databaseName, schemaJson, filesToAdd, filesToRemove, List.of(), List.of());
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
      final byte[] bytes = usersJson.getBytes(StandardCharsets.UTF_8);
      dos.writeInt(bytes.length);
      dos.write(bytes);

      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to encode SECURITY_USERS entry", e);
    }
  }

  /**
   * Encodes a bootstrap-fingerprint entry into a ByteString. Issue #4147.
   * <p>
   * Binary format: type byte, databaseName (UTF), fingerprint (UTF-8 hex), lastTxId (long).
   * Committed once per database during first cluster formation when
   * {@code arcadedb.ha.bootstrapFromLocalDatabase} is enabled, naming the peer chosen as the
   * bootstrap source. Followers verify their local fingerprint against this entry; match means
   * "bootstrap locally", mismatch means "fall back to leader-shipped snapshot".
   */
  public static ByteString encodeBootstrapFingerprintEntry(final String databaseName, final String fingerprint,
      final long lastTxId) {
    if (databaseName == null)
      throw new IllegalArgumentException("databaseName is required");
    if (fingerprint == null)
      throw new IllegalArgumentException("fingerprint is required");
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);

      dos.writeByte(RaftLogEntryType.BOOTSTRAP_FINGERPRINT_ENTRY.getId());
      dos.writeUTF(databaseName);
      final byte[] fpBytes = fingerprint.getBytes(StandardCharsets.UTF_8);
      dos.writeInt(fpBytes.length);
      dos.write(fpBytes);
      dos.writeLong(lastTxId);

      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to encode BOOTSTRAP_FINGERPRINT entry", e);
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
        return new DecodedEntry(null, null, null, null, null, null, null, null, null, null, false, null, -1L,
            List.of());
      final String databaseName = dis.readUTF();

      final DecodedEntry result = switch (type) {
        case TX_ENTRY -> decodeTxEntry(dis, databaseName);
        case SCHEMA_ENTRY -> decodeSchemaEntry(dis, databaseName);
        case INSTALL_DATABASE_ENTRY -> decodeInstallDatabaseEntry(dis, databaseName);
        case DROP_DATABASE_ENTRY -> new DecodedEntry(RaftLogEntryType.DROP_DATABASE_ENTRY, databaseName,
            null, null, null, null, null, null, null, null, false, null, -1L, List.of());
        case SECURITY_USERS_ENTRY -> decodeSecurityUsersEntry(dis);
        case BOOTSTRAP_FINGERPRINT_ENTRY -> decodeBootstrapFingerprintEntry(dis, databaseName);
      };

      // Trailing-byte validation: detect truncated or corrupted entries.
      // SCHEMA_ENTRY is excluded because it carries optional, self-describing trailing sections
      // (the embedded WAL section and the TimeSeries sealed-blob section) that newer nodes may
      // append and older decoders stop before; a clean EOF at a section boundary is normal here.
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
        null, null, null, null, null, null, false, null, -1L, List.of());
  }

  private static DecodedEntry decodeSchemaEntry(final DataInputStream dis, final String databaseName) throws IOException {
    final int schemaLen = dis.readInt();
    checkByteLength(schemaLen, "SCHEMA_ENTRY schemaJson");
    final byte[] schemaBytes = new byte[schemaLen];
    dis.readFully(schemaBytes);
    final String schemaJson = new String(schemaBytes, StandardCharsets.UTF_8);
    final Map<Integer, String> filesToAdd = readFileMap(dis);
    final Map<Integer, String> filesToRemove = readFileMap(dis);

    // Read embedded WAL entries. The section is optional: log entries produced by nodes that predate
    // it end the stream cleanly right after the file maps. A clean section boundary leaves no bytes
    // (available()==0) and is decoded as an absent (empty) section, mirroring decodeInstallDatabaseEntry.
    // Once any bytes remain the section IS present, so a truncated/misaligned section makes the reads
    // below hit EOF and propagate as corruption rather than silently yielding empty/partial WAL pages
    // (which would apply a schema change with missing index/WAL pages on followers).
    List<byte[]> walEntries = List.of();
    List<Map<Integer, Integer>> bucketDeltas = List.of();
    if (dis.available() > 0) {
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
    }

    // TimeSeries sealed-store blob section (issue #4382). Trailing, self-describing section with the
    // same presence rule as the WAL section above: no remaining bytes (available()==0) means the
    // section is absent (older entry) and is decoded as empty. Once any bytes remain the section IS
    // present, so a truncated/misaligned blob makes the reads below hit EOF and propagate rather than
    // being silently dropped; a CRC mismatch on a fully-read blob is likewise a hard failure.
    List<TsSealedBlob> sealedFileBlobs = List.of();
    if (dis.available() > 0) {
      final int blobCount = dis.readInt();
      checkCollectionSize(blobCount, "SCHEMA_ENTRY sealed blobs");
      if (blobCount > 0) {
        sealedFileBlobs = new ArrayList<>(blobCount);
        for (int i = 0; i < blobCount; i++) {
          final String typeName = dis.readUTF();
          final int shardIndex = dis.readInt();
          final String fileName = dis.readUTF();
          final long expectedCrc = dis.readLong();
          final int uncompressedLen = dis.readInt();
          checkByteLength(uncompressedLen, "SCHEMA_ENTRY sealed blob uncompressed");
          final int compressedLen = dis.readInt();
          checkByteLength(compressedLen, "SCHEMA_ENTRY sealed blob compressed");
          final byte[] compressed = new byte[compressedLen];
          dis.readFully(compressed);
          final byte[] raw = CompressionFactory.getDefault().decompress(compressed, uncompressedLen);
          final CRC32 crc = new CRC32();
          crc.update(raw);
          if (crc.getValue() != expectedCrc)
            throw new IllegalStateException(
                "CRC mismatch decoding SCHEMA_ENTRY sealed blob for '" + fileName + "' (corrupted replication payload)");
          sealedFileBlobs.add(new TsSealedBlob(typeName, shardIndex, fileName, raw));
        }
      }
    }

    return new DecodedEntry(RaftLogEntryType.SCHEMA_ENTRY, databaseName, null, null,
        schemaJson, filesToAdd, filesToRemove, walEntries, bucketDeltas, null, false, null, -1L, sealedFileBlobs);
  }

  private static DecodedEntry decodeInstallDatabaseEntry(final DataInputStream dis, final String databaseName) throws IOException {
    // Length-based detection of the trailing forceSnapshot flag.
    // Legacy entries (pre-forceSnapshot codec) have no trailing byte; they decode as forceSnapshot=false.
    boolean forceSnapshot = false;
    if (dis.available() > 0) {
      forceSnapshot = dis.readBoolean();
    }
    return new DecodedEntry(RaftLogEntryType.INSTALL_DATABASE_ENTRY, databaseName,
        null, null, null, null, null, null, null, null, forceSnapshot, null, -1L, List.of());
  }

  private static DecodedEntry decodeBootstrapFingerprintEntry(final DataInputStream dis, final String databaseName)
      throws IOException {
    final int fpLen = dis.readInt();
    checkByteLength(fpLen, "BOOTSTRAP_FINGERPRINT fingerprint");
    final byte[] fpBytes = new byte[fpLen];
    dis.readFully(fpBytes);
    final String fingerprint = new String(fpBytes, StandardCharsets.UTF_8);
    final long lastTxId = dis.readLong();
    return new DecodedEntry(RaftLogEntryType.BOOTSTRAP_FINGERPRINT_ENTRY, databaseName,
        null, null, null, null, null, null, null, null, false, fingerprint, lastTxId, List.of());
  }

  private static DecodedEntry decodeSecurityUsersEntry(final DataInputStream dis) throws IOException {
    final int length = dis.readInt();
    checkByteLength(length, "SECURITY_USERS_ENTRY");
    final byte[] bytes = new byte[length];
    dis.readFully(bytes);
    final String usersJson = new String(bytes, StandardCharsets.UTF_8);
    return new DecodedEntry(RaftLogEntryType.SECURITY_USERS_ENTRY, "",
        null, null, null, null, null, null, null, usersJson, false, null, -1L, List.of());
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
