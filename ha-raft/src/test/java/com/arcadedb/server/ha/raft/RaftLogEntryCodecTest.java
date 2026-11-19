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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.network.binary.ReplicatedEntryTooLargeException;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RaftLogEntryCodecTest {

  @Test
  void roundTripTxEntry() {
    final byte[] walData = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
    final Map<Integer, Integer> bucketDeltas = Map.of(0, 5, 1, -2);

    final ByteString encoded = RaftLogEntryCodec.encodeTxEntry("testdb", walData, bucketDeltas);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.TX_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo("testdb");
    assertThat(decoded.walData()).isEqualTo(walData);
    assertThat(decoded.bucketRecordDelta()).isEqualTo(bucketDeltas);
    assertThat(decoded.schemaJson()).isNull();
    assertThat(decoded.filesToAdd()).isNull();
    assertThat(decoded.filesToRemove()).isNull();
  }

  @Test
  void roundTripSchemaEntry() {
    final String schemaJson = "{\"types\":{}}";
    final Map<Integer, String> filesToAdd = Map.of(5, "testdb/schema.json");
    final Map<Integer, String> filesToRemove = Map.of(3, "testdb/old.idx");

    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb", schemaJson, filesToAdd, filesToRemove);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SCHEMA_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo("testdb");
    assertThat(decoded.schemaJson()).isEqualTo(schemaJson);
    assertThat(decoded.filesToAdd()).isEqualTo(filesToAdd);
    assertThat(decoded.filesToRemove()).isEqualTo(filesToRemove);
    assertThat(decoded.walData()).isNull();
  }

  @Test
  void roundTripTxEntryWithEmptyBucketDeltas() {
    final byte[] walData = new byte[] { 10, 20, 30 };

    final ByteString encoded = RaftLogEntryCodec.encodeTxEntry("db2", walData, Map.of());
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.TX_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo("db2");
    assertThat(decoded.walData()).isEqualTo(walData);
    assertThat(decoded.bucketRecordDelta()).isEmpty();
  }

  @Test
  void roundTripSchemaEntryWithNullFileNames() {
    final Map<Integer, String> filesToAdd = new HashMap<>();
    filesToAdd.put(7, null);
    filesToAdd.put(8, "testdb/newfile.dat");

    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb", "{}", filesToAdd, Map.of());
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.filesToAdd()).containsEntry(7, null);
    assertThat(decoded.filesToAdd()).containsEntry(8, "testdb/newfile.dat");
  }

  @Test
  void roundTripInstallDatabaseEntry() {
    final ByteString encoded = RaftLogEntryCodec.encodeInstallDatabaseEntry("mydb");
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.INSTALL_DATABASE_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo("mydb");
    assertThat(decoded.walData()).isNull();
    assertThat(decoded.schemaJson()).isNull();
    assertThat(decoded.filesToAdd()).isNull();
    assertThat(decoded.filesToRemove()).isNull();
    assertThat(decoded.bucketRecordDelta()).isNull();
  }

  @Test
  void roundTripLargeWalData() {
    final byte[] walData = new byte[1024 * 1024]; // 1MB
    for (int i = 0; i < walData.length; i++)
      walData[i] = (byte) (i % 256);

    final ByteString encoded = RaftLogEntryCodec.encodeTxEntry("bigdb", walData, Map.of());
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.walData()).isEqualTo(walData);
  }

  @Test
  void schemaEntryWithEmbeddedWalRoundtrip() {
    final byte[] fakeWal = new byte[] { 1, 2, 3, 4, 5 };
    final Map<Integer, Integer> fakeDelta = Map.of(1, 5);

    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb",
        "{\"schemaVersion\": 1}",
        Map.of(1, "User_0"),
        Map.of(),
        List.of(fakeWal),
        List.of(fakeDelta));

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SCHEMA_ENTRY);
    assertThat(decoded.walEntries()).hasSize(1);
    assertThat(decoded.walEntries().get(0)).isEqualTo(fakeWal);
    assertThat(decoded.bucketDeltas().get(0)).isEqualTo(fakeDelta);
  }

  @Test
  void schemaEntryWithNoWalHasEmptyLists() {
    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb", "{}", Map.of(), Map.of());
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.walEntries()).isEmpty();
    assertThat(decoded.bucketDeltas()).isEmpty();
  }

  @Test
  void roundTripSchemaEntryWithLargeSchemaJsonOver64Kb() {
    // Regression: DataOutputStream.writeUTF has a 64KB limit on the UTF-8 encoded length.
    // Real-world schemas with many types can exceed it (e.g., OrientDB-to-ArcadeDB
    // migrations that create hundreds of vertex types). Reproduces the reported
    // "encoded string ... too long: 65660 bytes" IllegalStateException from cluster mode.
    final StringBuilder builder = new StringBuilder("{\"types\":{");
    for (int i = 0; builder.length() < 70_000; i++) {
      if (i > 0)
        builder.append(',');
      builder.append("\"Type").append(i)
          .append("\":{\"parentType\":\"AbstractType").append(i)
          .append("\",\"fields\":[\"id\",\"name\",\"timestamp\",\"payload\"]}");
    }
    builder.append("}}");
    final String largeSchemaJson = builder.toString();
    assertThat(largeSchemaJson.getBytes(StandardCharsets.UTF_8).length).isGreaterThan(65535);

    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb", largeSchemaJson, Map.of(), Map.of());
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SCHEMA_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo("testdb");
    assertThat(decoded.schemaJson()).isEqualTo(largeSchemaJson);
  }

  @Test
  void roundTripSchemaEntryWithMultiByteUtf8Schema() {
    // Ensures the schema-JSON codec handles multi-byte UTF-8 correctly
    // (non-ASCII identifiers like accented type names).
    final String schemaJson = "{\"types\":{\"Usuário\":{\"parentType\":\"Persona\"},\"Ünicode\":{\"fields\":[\"naïve\",\"café\"]}}}";
    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("dbñame", schemaJson, Map.of(), Map.of());
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.schemaJson()).isEqualTo(schemaJson);
    assertThat(decoded.databaseName()).isEqualTo("dbñame");
  }

  @Test
  void roundTripTxEntryCompressesWalData() {
    // Create a WAL-like payload with repetitive data that compresses well
    final byte[] walData = new byte[4096];
    for (int i = 0; i < walData.length; i++)
      walData[i] = (byte) (i % 10);

    final Map<Integer, Integer> bucketDeltas = Map.of(0, 5);

    final ByteString encoded = RaftLogEntryCodec.encodeTxEntry("testdb", walData, bucketDeltas);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.TX_ENTRY);
    assertThat(decoded.walData()).isEqualTo(walData);
    assertThat(decoded.bucketRecordDelta()).isEqualTo(bucketDeltas);

    // Verify compression actually reduced size (repetitive data compresses well)
    assertThat(encoded.size()).isLessThan(walData.length);
  }

  @Test
  void roundTripSchemaEntryWithEmbeddedWalCompresses() {
    final byte[] fakeWal = new byte[2048];
    Arrays.fill(fakeWal, (byte) 42);
    final Map<Integer, Integer> fakeDelta = Map.of(1, 5);

    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb",
        "{\"schemaVersion\": 1}",
        Map.of(1, "User_0"),
        Map.of(),
        List.of(fakeWal),
        List.of(fakeDelta));

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.walEntries()).hasSize(1);
    assertThat(decoded.walEntries().get(0)).isEqualTo(fakeWal);
  }

  @Test
  void roundTripDropDatabaseEntry() {
    final ByteString encoded = RaftLogEntryCodec.encodeDropDatabaseEntry("testdb");
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.DROP_DATABASE_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo("testdb");
    assertThat(decoded.walData()).isNull();
    assertThat(decoded.bucketRecordDelta()).isNull();
    assertThat(decoded.schemaJson()).isNull();
    assertThat(decoded.filesToAdd()).isNull();
    assertThat(decoded.filesToRemove()).isNull();
    assertThat(decoded.usersJson()).isNull();
    assertThat(decoded.forceSnapshot()).isFalse();
  }

  @Test
  void roundTripInstallDatabaseEntryDefaults() {
    final ByteString encoded = RaftLogEntryCodec.encodeInstallDatabaseEntry("testdb");
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.INSTALL_DATABASE_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo("testdb");
    assertThat(decoded.forceSnapshot()).isFalse();
  }

  @Test
  void roundTripInstallDatabaseEntryWithForceSnapshot() {
    final ByteString encoded = RaftLogEntryCodec.encodeInstallDatabaseEntry("testdb", true);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.INSTALL_DATABASE_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo("testdb");
    assertThat(decoded.forceSnapshot()).isTrue();
  }

  @Test
  void decodeLegacyInstallDatabaseEntryWithoutFlag() throws Exception {
    // Hand-crafted byte layout matching the pre-forceSnapshot codec:
    // type byte + UTF(databaseName), no trailing boolean.
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final DataOutputStream dos = new DataOutputStream(baos)) {
      dos.writeByte(RaftLogEntryType.INSTALL_DATABASE_ENTRY.getId());
      dos.writeUTF("legacydb");
    }
    final ByteString legacy = ByteString.copyFrom(baos.toByteArray());

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(legacy);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.INSTALL_DATABASE_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo("legacydb");
    assertThat(decoded.forceSnapshot()).isFalse();
  }

  @Test
  void roundTripSecurityUsersEntryEmpty() {
    final String payload = "[]";
    final ByteString encoded = RaftLogEntryCodec.encodeSecurityUsersEntry(payload);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_USERS_ENTRY);
    assertThat(decoded.databaseName()).isEmpty();
    assertThat(decoded.usersJson()).isEqualTo(payload);
  }

  @Test
  void roundTripSecurityUsersEntrySingleUser() {
    final String payload = "[{\"name\":\"alice\",\"password\":\"$2a$hash\",\"databases\":{\"*\":[\"admin\"]}}]";
    final ByteString encoded = RaftLogEntryCodec.encodeSecurityUsersEntry(payload);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_USERS_ENTRY);
    assertThat(decoded.usersJson()).isEqualTo(payload);
  }

  @Test
  void roundTripSecurityUsersEntryUnicodeAndNewlines() {
    final String payload = "[{\"name\":\"björn\",\"note\":\"line1\\nline2\"}]";
    final ByteString encoded = RaftLogEntryCodec.encodeSecurityUsersEntry(payload);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.usersJson()).isEqualTo(payload);
  }

  @Test
  void unknownEntryTypeReturnsNull() {
    assertThat(RaftLogEntryType.fromId((byte) 99)).isNull();
  }

  @Test
  void decodeUnknownTypeReturnsNullTypeEntry() throws Exception {
    // Build an entry with an unknown type byte (99)
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final DataOutputStream dos = new DataOutputStream(baos)) {
      dos.writeByte(99);
    }
    final ByteString unknown = ByteString.copyFrom(baos.toByteArray());
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(unknown);

    assertThat(decoded.type()).isNull();
  }

  @Test
  void decodeTxEntryWithTrailingBytesThrows() {
    final byte[] walData = new byte[] { 1, 2, 3 };
    final ByteString valid = RaftLogEntryCodec.encodeTxEntry("testdb", walData, Map.of());

    final byte[] corrupted = new byte[valid.size() + 5];
    valid.copyTo(corrupted, 0);
    corrupted[valid.size()] = 99;

    final ByteString corruptedBS = ByteString.copyFrom(corrupted);

    Assertions.assertThatThrownBy(() -> RaftLogEntryCodec.decode(corruptedBS))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("trailing");
  }

  @Test
  void decodeDropDatabaseEntryWithTrailingBytesThrows() {
    final ByteString valid = RaftLogEntryCodec.encodeDropDatabaseEntry("testdb");

    final byte[] corrupted = new byte[valid.size() + 3];
    valid.copyTo(corrupted, 0);
    corrupted[valid.size()] = 1;

    final ByteString corruptedBS = ByteString.copyFrom(corrupted);

    Assertions.assertThatThrownBy(() -> RaftLogEntryCodec.decode(corruptedBS))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("trailing");
  }

  @Test
  void roundTripBootstrapFingerprintEntry() {
    // Issue #4147: the bootstrap protocol commits this entry once per database at first cluster
    // formation, naming the peer chosen as the source.
    final String fp = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    final long lastTxId = 42_000_000L;

    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry("testdb", fp, lastTxId);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.BOOTSTRAP_FINGERPRINT_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo("testdb");
    assertThat(decoded.bootstrapFingerprint()).isEqualTo(fp);
    assertThat(decoded.bootstrapLastTxId()).isEqualTo(lastTxId);
    // Other fields stay null/-1 to keep the record discriminated-union shaped.
    assertThat(decoded.walData()).isNull();
    assertThat(decoded.schemaJson()).isNull();
    assertThat(decoded.usersJson()).isNull();
    assertThat(decoded.forceSnapshot()).isFalse();
  }

  @Test
  void bootstrapFingerprintEntryRejectsNullArguments() {
    Assertions.assertThatThrownBy(
            () -> RaftLogEntryCodec.encodeBootstrapFingerprintEntry(null, "fp", 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("databaseName");

    Assertions.assertThatThrownBy(
            () -> RaftLogEntryCodec.encodeBootstrapFingerprintEntry("db", null, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("fingerprint");
  }

  @Test
  void decodeBootstrapFingerprintEntryWithTrailingBytesThrows() {
    final ByteString valid = RaftLogEntryCodec.encodeBootstrapFingerprintEntry("testdb",
        "0".repeat(64), 7L);

    final byte[] corrupted = new byte[valid.size() + 3];
    valid.copyTo(corrupted, 0);
    corrupted[valid.size()] = 99;

    final ByteString corruptedBS = ByteString.copyFrom(corrupted);

    Assertions.assertThatThrownBy(() -> RaftLogEntryCodec.decode(corruptedBS))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("trailing");
  }

  // --- TimeSeries sealed-store blob section (issue #4382) ---

  @Test
  void roundTripSchemaEntryWithSealedBlobs() {
    final byte[] sealed0 = new byte[1024];
    for (int i = 0; i < sealed0.length; i++)
      sealed0[i] = (byte) (i % 7);
    final byte[] sealed1 = "TSIX-fake-sealed-store-content".getBytes(StandardCharsets.UTF_8);

    final List<RaftLogEntryCodec.TsSealedBlob> blobs = List.of(
        new RaftLogEntryCodec.TsSealedBlob("weather", 0, "weather_shard_0.ts.sealed", sealed0),
        new RaftLogEntryCodec.TsSealedBlob("weather", 1, "weather_shard_1.ts.sealed", sealed1));

    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb", "{\"types\":{}}",
        Map.of(), Map.of(), List.of(), List.of(), blobs);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SCHEMA_ENTRY);
    assertThat(decoded.sealedFileBlobs()).hasSize(2);
    assertThat(decoded.sealedFileBlobs().get(0).typeName()).isEqualTo("weather");
    assertThat(decoded.sealedFileBlobs().get(0).shardIndex()).isEqualTo(0);
    assertThat(decoded.sealedFileBlobs().get(0).fileName()).isEqualTo("weather_shard_0.ts.sealed");
    assertThat(decoded.sealedFileBlobs().get(0).bytes()).isEqualTo(sealed0);
    assertThat(decoded.sealedFileBlobs().get(1).shardIndex()).isEqualTo(1);
    assertThat(decoded.sealedFileBlobs().get(1).bytes()).isEqualTo(sealed1);
  }

  @Test
  void moreChunksFollowSurvivesTheRoundTrip() {
    // The flag decides whether the applier reloads the schema for a chunk, so a chunk that says "more
    // follow" must still say it on the other side (issue #5443).
    final ByteString notLast = RaftLogEntryCodec.encodeSchemaEntry("testdb", null, Map.of(1, "f.pages"), Map.of(),
        List.of(new byte[] { 1, 2, 3 }), List.of(Map.of()), List.of(), true);
    final ByteString last = RaftLogEntryCodec.encodeSchemaEntry("testdb", "{\"types\":{}}", Map.of(), Map.of(),
        List.of(), List.of(), List.of(), false);

    assertThat(RaftLogEntryCodec.decode(notLast).moreChunksFollow()).isTrue();
    assertThat(RaftLogEntryCodec.decode(last).moreChunksFollow()).isFalse();
  }

  @Test
  void anEntryWithoutTheTrailingFlagDecodesAsTheLastChunk() {
    // What a node running the pre-#5443 codec emits: the byte is simply not there. Reading it as false
    // is what keeps the format compatible in both directions, so the decoder must not require it.
    final ByteString withFlag = RaftLogEntryCodec.encodeSchemaEntry("testdb", "{\"types\":{}}", Map.of(), Map.of(),
        List.of(), List.of(), List.of(), false);
    final byte[] raw = withFlag.toByteArray();
    final ByteString withoutFlag = ByteString.copyFrom(raw, 0, raw.length - 1);

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(withoutFlag);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SCHEMA_ENTRY);
    assertThat(decoded.moreChunksFollow()).isFalse();
    assertThat(decoded.schemaJson()).isEqualTo("{\"types\":{}}");
  }

  @Test
  void schemaEntryWithoutSealedBlobsHasEmptyList() {
    // Entries produced by the pre-#4382 codec (no blob section) must decode with an empty list.
    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb", "{}", Map.of(), Map.of());
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.sealedFileBlobs()).isEmpty();
  }

  @Test
  void schemaEntryWithEmbeddedWalAndSealedBlobsRoundtrip() {
    final byte[] clearWal = new byte[] { 9, 8, 7, 6 };
    final Map<Integer, Integer> delta = Map.of(2, -1);
    final byte[] sealed = new byte[512];
    Arrays.fill(sealed, (byte) 3);

    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb", "{\"schemaVersion\":2}",
        Map.of(), Map.of(), List.of(clearWal), List.of(delta),
        List.of(new RaftLogEntryCodec.TsSealedBlob("metrics", 0, "metrics_shard_0.ts.sealed", sealed)));
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.walEntries()).hasSize(1);
    assertThat(decoded.walEntries().get(0)).isEqualTo(clearWal);
    assertThat(decoded.bucketDeltas().get(0)).isEqualTo(delta);
    assertThat(decoded.sealedFileBlobs()).hasSize(1);
    assertThat(decoded.sealedFileBlobs().get(0).bytes()).isEqualTo(sealed);
  }

  @Test
  void corruptedSealedBlobFailsCrc() {
    final byte[] sealed = new byte[256];
    for (int i = 0; i < sealed.length; i++)
      sealed[i] = (byte) i;

    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb", "{}",
        Map.of(), Map.of(), List.of(), List.of(),
        List.of(new RaftLogEntryCodec.TsSealedBlob("weather", 0, "weather_shard_0.ts.sealed", sealed)));

    // Flip a byte inside the trailing compressed blob payload to corrupt it. NOT the very last byte:
    // since #5443 that one is the split-continuation flag, and flipping it would leave the blob intact.
    final byte[] corrupted = new byte[encoded.size()];
    encoded.copyTo(corrupted, 0);
    corrupted[corrupted.length - 2] ^= 0xFF;

    Assertions.assertThatThrownBy(
            () -> RaftLogEntryCodec.decode(ByteString.copyFrom(corrupted)))
        .isInstanceOf(IllegalStateException.class);
  }

  // --- Embedded WAL section truncation/backward-compatibility (issue #4825) ---

  // Wire-format offsets of an encoded SCHEMA_ENTRY with dbName "testdb", schema "{}", empty file maps,
  // and a single embedded WAL entry. Computed explicitly (rather than via a fraction of the entry size)
  // so the truncation points stay anchored to the format if the encoder or compression ratio changes.
  // Layout: type(1) + writeUTF("testdb")=2+6 + schemaLen(4)+"{}"(2) + filesToAdd count(4)
  //         + filesToRemove count(4) + walCount(4) + walUncompressedLen(4) + walCompressedLen(4) ...
  private static final int SCHEMA_WAL_COUNT_END     = 1 + (2 + 6) + (4 + 2) + 4 + 4 + 4; // = 27
  private static final int SCHEMA_WAL_PAYLOAD_START = SCHEMA_WAL_COUNT_END + 4 + 4;       // = 35

  @Test
  void decodeSchemaEntryWithTruncatedWalPayloadThrows() {
    // A SCHEMA_ENTRY whose embedded WAL section is present but truncated mid-payload must surface as a
    // decode failure, not silently decode with empty/partial WAL entries. A silently emptied WAL
    // section makes a follower apply the schema change with missing index/WAL pages (the
    // "Cannot find indexes ..." class of failure) with no error surfaced.
    final byte[] walData = new byte[4096];
    for (int i = 0; i < walData.length; i++)
      walData[i] = (byte) ((i * 31 + 7) ^ (i >>> 3));
    final Map<Integer, Integer> delta = Map.of(1, 5);

    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb", "{}",
        Map.of(), Map.of(), List.of(walData), List.of(delta));

    // Cut a few bytes into the compressed WAL payload: the WAL count and both length prefixes are read
    // in full, then readFully hits EOF inside the payload.
    final int cut = SCHEMA_WAL_PAYLOAD_START + 4;
    assertThat(cut).isLessThan(encoded.size());
    final ByteString truncated = encoded.substring(0, cut);

    Assertions.assertThatThrownBy(() -> RaftLogEntryCodec.decode(truncated))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void decodeSchemaEntryTruncatedInWalLengthPrefixThrows() {
    // Boundary semantics: once the WAL count has been read, a truncation a couple of bytes into the
    // first entry's length prefix (before any payload) must still propagate rather than being treated
    // as an absent section.
    final byte[] walData = new byte[64];
    Arrays.fill(walData, (byte) 7);

    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb", "{}",
        Map.of(), Map.of(), List.of(walData), List.of(Map.of(1, 5)));

    final int cut = SCHEMA_WAL_COUNT_END + 2; // 2 of the 4 bytes of walUncompressedLen present
    assertThat(cut).isLessThan(encoded.size());
    final ByteString truncated = encoded.substring(0, cut);

    Assertions.assertThatThrownBy(() -> RaftLogEntryCodec.decode(truncated))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void decodeLegacySchemaEntryWithoutWalSectionDecodesEmpty() throws Exception {
    // Hand-crafted byte layout matching a pre-embedded-WAL SCHEMA_ENTRY: the stream ends right after
    // the filesToRemove map, so the WAL-count read hits a clean EOF at the section boundary. This is
    // the one legitimate "absent section" case and must still decode as an empty WAL section.
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final DataOutputStream dos = new DataOutputStream(baos)) {
      dos.writeByte(RaftLogEntryType.SCHEMA_ENTRY.getId());
      dos.writeUTF("legacydb");
      final byte[] schemaBytes = "{}".getBytes(StandardCharsets.UTF_8);
      dos.writeInt(schemaBytes.length);
      dos.write(schemaBytes);
      dos.writeInt(0); // filesToAdd: empty
      dos.writeInt(0); // filesToRemove: empty
      // no WAL section, no sealed-blob section
    }
    final ByteString legacy = ByteString.copyFrom(baos.toByteArray());

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(legacy);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SCHEMA_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo("legacydb");
    assertThat(decoded.walEntries()).isEmpty();
    assertThat(decoded.bucketDeltas()).isEmpty();
    assertThat(decoded.sealedFileBlobs()).isEmpty();
  }

  /**
   * Regression test for issue #7138: Raft log entries carry no version field - the type byte is the whole
   * envelope - and only {@code SCHEMA_ENTRY} tolerated trailing bytes. For the other five types an unknown
   * trailing field was fatal, and fatal in the worst arm: {@code RaftLogEntryCodec.decode} runs OUTSIDE
   * {@code applyWithRetry}, so the throw was neither a {@code NeedRetryException} nor a
   * {@code ReplicationException} and could not be quarantined - it reached {@code catch (Throwable)} and halted
   * the node, with the applied index deliberately not advanced so the halt repeated on every restart.
   * <p>
   * A newer leader adding one field to {@code DROP_DATABASE_ENTRY} would therefore have permanently halted every
   * not-yet-upgraded peer: two nodes down during a rolling upgrade of a three-node cluster.
   * <p>
   * The fix gives every type the extension mechanism {@code SCHEMA_ENTRY} demonstrated the need for, framed so
   * the corruption signal the old check existed for is kept rather than traded away: a deliberate extension
   * carries {@code RaftLogEntryCodec.EXTENSION_MAGIC}, garbage does not.

   */
  @Nested
  class EntryExtensionSection {

    private static final String DB = "mydb";

    /** What a future release adds: one more field on an entry type that has no extension mechanism today. */
    private static ByteString dropDatabaseEntryFromANewerNode() {
      return RaftLogEntryCodec.appendExtensionSection(RaftLogEntryCodec.encodeDropDatabaseEntry(DB),
          "a field this version has never heard of".getBytes(StandardCharsets.UTF_8));
    }

    // Regression for #7138: EntryExtensionSection
    @Test
    void aFieldAppendedByANewerNodeIsSkippedInsteadOfHaltingTheDecoder() {
      final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(dropDatabaseEntryFromANewerNode());

      assertThat(decoded.type()).isEqualTo(RaftLogEntryType.DROP_DATABASE_ENTRY);
      assertThat(decoded.databaseName()).isEqualTo(DB);
    }

    /**
     * All five non-schema types, not just the one: the point of #7138 is that the constraint stops being per-type
     * folklore, so a regression that reintroduced it for a single type has to fail here.
     */
    // Regression for #7138: EntryExtensionSection
    @Test
    void everyEntryTypeToleratesATrailingExtensionSection() {
      final byte[] future = new byte[] { 1, 2, 3, 4 };

      assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.appendExtensionSection(
          RaftLogEntryCodec.encodeTxEntry(DB, new byte[0], Map.of()), future)).type())
          .isEqualTo(RaftLogEntryType.TX_ENTRY);

      assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.appendExtensionSection(
          RaftLogEntryCodec.encodeDropDatabaseEntry(DB), future)).type())
          .isEqualTo(RaftLogEntryType.DROP_DATABASE_ENTRY);

      assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.appendExtensionSection(
          RaftLogEntryCodec.encodeInstallDatabaseEntry(DB, true), future)).type())
          .isEqualTo(RaftLogEntryType.INSTALL_DATABASE_ENTRY);

      assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.appendExtensionSection(
          RaftLogEntryCodec.encodeSecurityUsersEntry("[]"), future)).type())
          .isEqualTo(RaftLogEntryType.SECURITY_USERS_ENTRY);

      assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.appendExtensionSection(
          RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB, "fp", 42L), future)).type())
          .isEqualTo(RaftLogEntryType.BOOTSTRAP_FINGERPRINT_ENTRY);
    }

    /** Several fields added over several releases stack up; a decoder skips all of them. */
    // Regression for #7138: EntryExtensionSection
    @Test
    void repeatedExtensionSectionsAreAllSkipped() {
      ByteString entry = RaftLogEntryCodec.encodeDropDatabaseEntry(DB);
      entry = RaftLogEntryCodec.appendExtensionSection(entry, new byte[] { 9 });
      entry = RaftLogEntryCodec.appendExtensionSection(entry, new byte[0]);
      entry = RaftLogEntryCodec.appendExtensionSection(entry, new byte[] { 7, 7, 7 });

      assertThat(RaftLogEntryCodec.decode(entry).databaseName()).isEqualTo(DB);
    }

    /**
     * The half that must NOT be traded away: tolerating anything trailing (what {@code SCHEMA_ENTRY} does) would
     * make every type extensible but would stop reporting a truncated or corrupt entry at all.
     */
    // Regression for #7138: EntryExtensionSection
    @Test
    void unframedTrailingBytesAreStillReportedAsCorruption() throws Exception {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(RaftLogEntryCodec.encodeDropDatabaseEntry(DB).toByteArray());
      baos.write(new byte[] { 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42 });

      assertThatThrownBy(() -> RaftLogEntryCodec.decode(ByteString.copyFrom(baos.toByteArray())))
          .isInstanceOf(RaftLogEntryDecodeException.class)
          .hasMessageContaining("DROP_DATABASE_ENTRY")
          .rootCause()
          .hasMessageContaining("not an extension section");
    }

    /** A tail too short to even be a frame header is corruption, not an empty extension. */
    // Regression for #7138: EntryExtensionSection
    @Test
    void aTailTooShortForAFrameHeaderIsCorruption() throws Exception {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(RaftLogEntryCodec.encodeDropDatabaseEntry(DB).toByteArray());
      baos.write(new byte[] { 0x41, 0x44 });

      assertThatThrownBy(() -> RaftLogEntryCodec.decode(ByteString.copyFrom(baos.toByteArray())))
          .isInstanceOf(RaftLogEntryDecodeException.class)
          .rootCause()
          .hasMessageContaining("too few for an extension section header");
    }

    /** A frame whose length runs past the end of the entry is a truncation, and must not be skipped over. */
    // Regression for #7138: EntryExtensionSection
    @Test
    void anExtensionSectionTruncatedInFlightIsCorruption() throws Exception {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);
      RaftLogEntryCodec.encodeDropDatabaseEntry(DB).writeTo(dos);
      dos.writeInt(RaftLogEntryCodec.EXTENSION_MAGIC);
      dos.writeInt(64);            // claims 64 bytes...
      dos.write(new byte[] { 1 }); // ...and carries 1
      dos.flush();

      assertThatThrownBy(() -> RaftLogEntryCodec.decode(ByteString.copyFrom(baos.toByteArray())))
          .isInstanceOf(RaftLogEntryDecodeException.class)
          .rootCause()
          .hasMessageContaining("extension section");
    }

    /**
     * SCHEMA_ENTRY is EXCLUDED from the framing, not merely exempt from the check, and the difference is worth a
     * test: its optional sections are unframed and positional, so a decoder that has consumed the file maps reads
     * whatever comes next as the #4382 WAL section's count. A frame appended to one is therefore not skipped - the
     * magic is read as that count and refused. Anyone extending SCHEMA_ENTRY has to add a section to its own
     * mechanism instead, and this is what says so if they reach for the helper.
     */
    // Regression for #7138: EntryExtensionSection
    @Test
    void aFrameAppendedToASchemaEntryIsNotAnExtensionSection() {
      final ByteString schema = RaftLogEntryCodec.encodeSchemaEntry(DB, "{}", Map.of(), Map.of(),
          Collections.emptyList(), Collections.emptyList());

      assertThatThrownBy(() -> RaftLogEntryCodec.decode(
          RaftLogEntryCodec.appendExtensionSection(schema, new byte[] { 1, 2, 3, 4 })))
          .as("the frame header lands where the WAL section's count is read")
          .isInstanceOf(RaftLogEntryDecodeException.class);
    }

    /** Control for the exclusion: a SCHEMA_ENTRY carrying only its own sections still decodes untouched. */
    // Regression for #7138: EntryExtensionSection
    @Test
    void aSchemaEntryWithItsOwnSectionsIsUnaffected() {
      final ByteString schema = RaftLogEntryCodec.encodeSchemaEntry(DB, "{\"v\":1}", Map.of(), Map.of(),
          Collections.emptyList(), Collections.emptyList());

      final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(schema);
      assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SCHEMA_ENTRY);
      assertThat(decoded.databaseName()).isEqualTo(DB);
    }

    /**
     * The second half of #7138: a decode failure on ONE database must not be a whole-node halt. It is isolable
     * exactly like an apply error on that database, so the node quarantines it, resyncs it from the leader and
     * keeps serving its other databases - and the entry is still never silently skipped.
     */
    // Regression for #7138: EntryExtensionSection
    @Test
    void aCorruptEntryQuarantinesItsDatabaseInsteadOfHaltingTheNode() throws Exception {
      final ArcadeStateMachine sm = new ArcadeStateMachine();

      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(RaftLogEntryCodec.encodeDropDatabaseEntry("db-A").toByteArray());
      baos.write(new byte[] { 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42 });

      final CompletableFuture<Message> future = sm.applyTransaction(entryAt(sm, ByteString.copyFrom(baos.toByteArray()), 5L));

      assertThat(future.isCompletedExceptionally()).isTrue();
      assertThat(sm.isHaltedAfterCriticalError())
          .as("one unreadable entry must not stop every co-located database (issue #7138)")
          .isFalse();
      assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
      assertThat(sm.isDatabaseDiverged("db-B")).isFalse();
    }

    /**
     * An entry whose ENVELOPE HEADER did not survive - it ends before the database name - is a decode failure like
     * any other, so it is reported as one rather than as a generic unexpected error. It still halts, because it
     * names no database to quarantine and so nothing a targeted resync could repair.
     */
    // Regression for #7138: EntryExtensionSection
    @Test
    void anEntryTruncatedInsideItsHeaderIsADecodeFailureAndStillHalts() {
      // A valid type byte and nothing after it: the database name's UTF read hits EOF.
      final ByteString truncated = ByteString.copyFrom(new byte[] { RaftLogEntryType.TX_ENTRY.getId() });

      assertThatThrownBy(() -> RaftLogEntryCodec.decode(truncated))
          .isInstanceOf(RaftLogEntryDecodeException.class)
          .hasMessageContaining("envelope header");

      final ArcadeStateMachine sm = new ArcadeStateMachine();
      final CompletableFuture<Message> future = sm.applyTransaction(entryAt(sm, truncated, 9L));

      assertThat(future.isCompletedExceptionally()).isTrue();
      assertThat(sm.isHaltedAfterCriticalError())
          .as("no database name means nothing to quarantine, so the node-wide halt is the honest answer")
          .isTrue();
    }

    /**
     * Control for the arm above: an unknown TYPE is a different failure and still halts. #4798 is right that
     * skipping a committed mutation nobody can read is a silent divergence, and nothing here loosens that.
     */
    // Regression for #7138: EntryExtensionSection
    @Test
    void anUnknownEntryTypeStillHaltsTheNode() {
      final ArcadeStateMachine sm = new ArcadeStateMachine();

      final CompletableFuture<Message> future = sm.applyTransaction(
          entryAt(sm, ByteString.copyFrom(new byte[] { (byte) 99, 0, 0 }), 7L));

      assertThat(future.isCompletedExceptionally()).isTrue();
      assertThat(sm.isHaltedAfterCriticalError()).isTrue();
    }

    private static TransactionContext entryAt(final ArcadeStateMachine sm, final ByteString payload, final long index) {
      final LogEntryProto logEntry = LogEntryProto.newBuilder()
          .setTerm(1L)
          .setIndex(index)
          .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
          .build();
      return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
    }
  }

  /**
   * Regression tests for issue #4743 round 4: a bulk load into a 3-node Raft cluster failed after ~500k
   * vertices with the leader stepping down every ~130 s, every node answering {@code NotLeaderException}
   * for the same request id, and phase-2 applies unconfirmed for 20 minutes - all with no CPU, disk or
   * network pressure on any node.
   * <p>
   * The cause was an oversized Raft entry, produced two ways:
   * <ul>
   *   <li>a single transaction whose compressed WAL exceeded {@code arcadedb.ha.appendBufferSize} - 4MB at
   *       the time, and the reporter's records were 4-6.5MB each, which for incompressible content maps
   *       roughly 1:1 onto the entry (a 6MB random-content record measures 6.03MB encoded). The default has
   *       since been raised to 32MB precisely because it sat below a single legitimate record;</li>
   *   <li>index-compaction replication, which shipped the WHOLE newly compacted index as ONE synthetic
   *       WAL entry inside ONE {@code SCHEMA_ENTRY} - 21.5MB for a 517k-key index, growing without bound
   *       with the index size.</li>
   * </ul>
   * Ratis rejects a log entry above the appender byte limit with a {@code StateMachineException} whose
   * {@code leaderShouldStepDown()} is {@code true}, so the leader steps down; the caller then retried the
   * identical entry against the newly elected leader and toppled that one too, forever.
   * <p>
   * The pre-existing size check only looked at {@code arcadedb.ha.grpcMessageSizeMax} (128MB), two orders
   * of magnitude above the limit that actually binds, so both entries sailed through it.

   */
  @Nested
  class OversizedRaftEntry {

    // Regression for #4743: OversizedRaftEntry
    @Test
    void effectiveEntrySizeIsTheSmallerOfTheTwoRatisLimits() {
      final ContextConfiguration cfg = new ContextConfiguration();

      // Stock defaults: the appender byte limit binds, NOT grpcMessageSizeMax (128MB).
      assertThat(GlobalConfiguration.maxReplicatedRaftEntrySize(cfg)).isEqualTo(32L * 1024 * 1024);

      // Raising only the appender limit above the gRPC cap makes the gRPC cap bind again.
      cfg.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "256MB");
      assertThat(GlobalConfiguration.maxReplicatedRaftEntrySize(cfg))
          .isEqualTo(cfg.getValueAsLong(GlobalConfiguration.HA_GRPC_MESSAGE_SIZE_MAX));

      // Lowering it below the gRPC cap makes THAT the effective ceiling again.
      cfg.setValue(GlobalConfiguration.HA_APPEND_BUFFER_SIZE, "8MB");
      assertThat(GlobalConfiguration.maxReplicatedRaftEntrySize(cfg)).isEqualTo(8L * 1024 * 1024);

      // RaftPropertiesBuilder must agree: one definition, no drift between engine and ha-raft.
      assertThat(RaftPropertiesBuilder.maxReplicatedEntrySize(cfg))
          .isEqualTo(GlobalConfiguration.maxReplicatedRaftEntrySize(cfg));
    }

    /**
     * The default ceiling must sit above a single legitimate record, and the two coupled buffers must stay
     * consistent. Ratis requires {@code writeBufferSize >= appendBufferSize + 8} and allocates the write
     * buffer as a DIRECT ByteBuffer at startup, so the two defaults can only move together - a mismatch
     * fails every HA server on boot with a ConfigurationException.
     */
    // Regression for #4743: OversizedRaftEntry
    @Test
    void defaultBuffersAllowARealisticRecordAndStayConsistent() {
      final ContextConfiguration cfg = new ContextConfiguration();
      final long appendBuffer = FileUtils.getSizeAsNumber(cfg.getValueAsString(GlobalConfiguration.HA_APPEND_BUFFER_SIZE));
      final long writeBuffer = FileUtils.getSizeAsNumber(cfg.getValueAsString(GlobalConfiguration.HA_WRITE_BUFFER_SIZE));

      // A 6.5MB incompressible record encodes to ~6.6MB; the default must clear that with real headroom, so
      // a single record can never again be unreplicable out of the box (issue #4743).
      assertThat(appendBuffer).as("the default entry ceiling must be well above one large record")
          .isGreaterThanOrEqualTo(32L * 1024 * 1024);
      assertThat(writeBuffer).as("Ratis rejects writeBufferSize < appendBufferSize + 8 at startup")
          .isGreaterThanOrEqualTo(appendBuffer + 8);
      // An oversized write buffer is pure off-heap waste: it is preallocated and never grows past need.
      assertThat(writeBuffer).as("the write buffer should track the append buffer, not dwarf it")
          .isLessThanOrEqualTo(appendBuffer * 2);
      // The log segment must be able to hold a maximum-size entry.
      assertThat(FileUtils.getSizeAsNumber(cfg.getValueAsString(GlobalConfiguration.HA_LOG_SEGMENT_SIZE)))
          .as("a maximum-size entry must fit a Raft log segment").isGreaterThanOrEqualTo(appendBuffer);
      // And the gRPC frame cap must not be the binding limit by accident.
      assertThat(cfg.getValueAsLong(GlobalConfiguration.HA_GRPC_MESSAGE_SIZE_MAX))
          .as("the gRPC cap must stay above the appender limit").isGreaterThanOrEqualTo(appendBuffer);

      // The defaults must actually build a valid Ratis configuration.
      assertThatNoException().isThrownBy(() -> RaftPropertiesBuilder.build(cfg));
    }

    // Regression for #4743: OversizedRaftEntry
    @Test
    void oversizedEntryIsRejectedBeforeDispatchAndIsNotRetryable() {
      final long cap = 1024L;
      final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 500,
          500, 10_000, 100, cap, null);
      try {
        final Throwable thrown = org.assertj.core.api.Assertions
            .catchThrowable(() -> committer.submitAndWait(new byte[(int) cap + 1]));

        assertThat(thrown).isInstanceOf(ReplicatedEntryTooLargeException.class);
        // The crux of #4743: retrying an oversized entry topples one leader per attempt, so this must
        // NOT be a retryable error. GraphBatch and the HTTP layer both branch on NeedRetryException.
        assertThat(thrown).isNotInstanceOf(NeedRetryException.class);
        // The message must name the knob that actually binds, and its coupled companion.
        assertThat(thrown).hasMessageContaining("arcadedb.ha.appendBufferSize")
            .hasMessageContaining("arcadedb.ha.writeBufferSize")
            .hasMessageContaining("maximum replicated Raft entry size");
        assertThat(committer.maxEntrySize()).isEqualTo(cap);
      } finally {
        committer.stop();
      }
    }

    // Regression for #4743: OversizedRaftEntry
    @Test
    void entryExactlyAtTheCapIsAccepted() {
      // Boundary: the check is "greater than", so an entry of exactly cap bytes must get past it (and
      // then fail on the null RaftClient like any other dispatched entry).
      final long cap = 1024L;
      final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 500,
          500, 10_000, 100, cap, null);
      try {
        assertThatThrownBy(() -> committer.submitAndWait(new byte[(int) cap]))
            .isNotInstanceOf(ReplicatedEntryTooLargeException.class);
      } finally {
        committer.stop();
      }
    }

    // Regression for #4743: OversizedRaftEntry
    @Test
    void compactionSchemaEntryIsSplitIntoOrderedChunksThatEachFit() {
      // Shape of the reporter's failure: one compacted index file serialized as many WAL chunks, plus the
      // schema JSON that publishes it and the retired file to remove.
      final long cap = 64 * 1024L;
      final String schemaJson = "{\"types\":{\"Address\":{}}}";
      final Map<Integer, String> filesToAdd = new LinkedHashMap<>();
      filesToAdd.put(456, "Address_0_4157906202920600_456.v1.ptree");
      final Map<Integer, String> filesToRemove = new LinkedHashMap<>();
      filesToRemove.put(445, "Address_0_4157857925972376_445.v1.ptree");

      final List<byte[]> wal = new ArrayList<>();
      final List<Map<Integer, Integer>> deltas = new ArrayList<>();
      for (int i = 0; i < 40; i++) {
        // Incompressible payload so the encoded size tracks the raw size (a compressible pattern would
        // make every chunk fit and prove nothing).
        final byte[] chunk = new byte[8 * 1024];
        for (int b = 0; b < chunk.length; b++)
          chunk[b] = (byte) ((i * 31 + b * 17 + (b >> 3)) ^ (b * 7));
        wal.add(chunk);
        deltas.add(Collections.emptyMap());
      }

      final ByteString single = RaftLogEntryCodec.encodeSchemaEntry("graph", schemaJson, filesToAdd,
          filesToRemove, wal, deltas, Collections.emptyList());
      assertThat(single.size()).as("the unsplit entry must be over the cap for this test to mean anything")
          .isGreaterThan((int) cap);

      final List<ByteString> chunks = RaftTransactionBroker.splitSchemaEntry("graph", schemaJson, filesToAdd,
          filesToRemove, wal, deltas, Collections.emptyList(), cap, single.size());

      assertThat(chunks).hasSizeGreaterThan(1);
      for (final ByteString chunk : chunks)
        assertThat(chunk.size()).as("every chunk must be replicable").isLessThanOrEqualTo((int) cap);

      // Ordering contract: files first (so pages have somewhere to land), publication last.
      final List<RaftLogEntryCodec.DecodedEntry> decoded = new ArrayList<>();
      for (final ByteString chunk : chunks)
        decoded.add(RaftLogEntryCodec.decode(chunk));

      assertThat(decoded.getFirst().filesToAdd()).isEqualTo(filesToAdd);
      assertThat(decoded.getFirst().schemaJson()).isEmpty();
      assertThat(decoded.getLast().schemaJson()).isEqualTo(schemaJson);
      assertThat(decoded.getLast().filesToRemove()).isEqualTo(filesToRemove);
      for (int i = 1; i < decoded.size(); i++)
        assertThat(decoded.get(i).filesToAdd()).as("only the first chunk creates files").isEmpty();
      for (int i = 0; i < decoded.size() - 1; i++) {
        assertThat(decoded.get(i).schemaJson()).as("only the last chunk publishes the schema").isEmpty();
        assertThat(decoded.get(i).filesToRemove()).as("only the last chunk removes files").isEmpty();
      }

      // No WAL entry may be lost or reordered: the pages must arrive exactly once, in order.
      final List<byte[]> reassembled = new ArrayList<>();
      for (final RaftLogEntryCodec.DecodedEntry e : decoded)
        reassembled.addAll(e.walEntries());
      assertThat(reassembled).hasSameSizeAs(wal);
      for (int i = 0; i < wal.size(); i++)
        assertThat(reassembled.get(i)).isEqualTo(wal.get(i));
    }

    // Regression for #4743: OversizedRaftEntry
    @Test
    void unsplittableSchemaEntryFailsWithActionableErrorInsteadOfTopplingTheLeader() {
      // A schema JSON that alone blows the cap cannot be chunked. Failing the DDL once, naming the knob,
      // beats dispatching an entry that makes every elected leader step down in turn.
      final long cap = 4096L;
      final String hugeSchema = "x".repeat((int) cap * 2);
      final ByteString single = RaftLogEntryCodec.encodeSchemaEntry("graph", hugeSchema,
          Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
          Collections.emptyList());

      assertThatThrownBy(() -> RaftTransactionBroker.splitSchemaEntry("graph", hugeSchema,
          Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
          Collections.emptyList(), cap, single.size()))
          .isInstanceOf(ReplicatedEntryTooLargeException.class)
          .isNotInstanceOf(NeedRetryException.class)
          .hasMessageContaining("cannot be split")
          .hasMessageContaining("arcadedb.ha.appendBufferSize");
    }

    // Regression for #4743: OversizedRaftEntry
    @Test
    void schemaEntrySplitKeepsBucketDeltasAlignedWithTheirWalEntry() {
      // The codec pairs walEntries[i] with bucketDeltas[i]; regrouping must not shift that pairing, or a
      // follower would apply one transaction's page deltas to another's record counts.
      final long cap = 8 * 1024L;
      final List<byte[]> wal = new ArrayList<>();
      final List<Map<Integer, Integer>> deltas = new ArrayList<>();
      for (int i = 0; i < 12; i++) {
        final byte[] payload = new byte[2 * 1024];
        for (int b = 0; b < payload.length; b++)
          payload[b] = (byte) ((i * 131 + b * 37) ^ (b << 1));
        wal.add(payload);
        deltas.add(Map.of(100 + i, i));
      }

      final ByteString single = RaftLogEntryCodec.encodeSchemaEntry("graph", "{}", Collections.emptyMap(),
          Collections.emptyMap(), wal, deltas, Collections.emptyList());
      final List<ByteString> chunks = RaftTransactionBroker.splitSchemaEntry("graph", "{}",
          Collections.emptyMap(), Collections.emptyMap(), wal, deltas, Collections.emptyList(), cap,
          single.size());
      assertThat(chunks).hasSizeGreaterThan(1);

      final List<Map<Integer, Integer>> reassembled = new ArrayList<>();
      for (final ByteString chunk : chunks)
        reassembled.addAll(RaftLogEntryCodec.decode(chunk).bucketDeltas());
      assertThat(reassembled).isEqualTo(deltas);
    }

    // Regression for #4743: OversizedRaftEntry
    @Test
    void oversizedEntryMessageIsNotMisleadingAboutTheGrpcCap() {
      // The old message told operators to raise arcadedb.ha.grpcMessageSizeMax, which for the default
      // configuration changes nothing at all: the appender limit is 32x smaller. Keep that advice from
      // creeping back in as the primary remedy.
      final long cap = 512L;
      final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 500,
          500, 10_000, 100, cap, null);
      try {
        final Throwable thrown = org.assertj.core.api.Assertions
            .catchThrowable(() -> committer.submitAndWait("x".repeat((int) cap + 1).getBytes(StandardCharsets.UTF_8)));
        final String message = thrown.getMessage();
        assertThat(message.indexOf("arcadedb.ha.appendBufferSize"))
            .as("appendBufferSize must be named before grpcMessageSizeMax as the remedy")
            .isLessThan(message.lastIndexOf("arcadedb.ha.grpcMessageSizeMax"));
      } finally {
        committer.stop();
      }
    }
  }
}
