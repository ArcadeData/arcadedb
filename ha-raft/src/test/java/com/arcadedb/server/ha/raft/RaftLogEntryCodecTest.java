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

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
    assertThat(decoded.walEntries().getFirst()).isEqualTo(fakeWal);
    assertThat(decoded.bucketDeltas().getFirst()).isEqualTo(fakeDelta);
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
    assertThat(decoded.walEntries().getFirst()).isEqualTo(fakeWal);
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
    assertThat(decoded.sealedFileBlobs().getFirst().typeName()).isEqualTo("weather");
    assertThat(decoded.sealedFileBlobs().getFirst().shardIndex()).isEqualTo(0);
    assertThat(decoded.sealedFileBlobs().getFirst().fileName()).isEqualTo("weather_shard_0.ts.sealed");
    assertThat(decoded.sealedFileBlobs().getFirst().bytes()).isEqualTo(sealed0);
    assertThat(decoded.sealedFileBlobs().get(1).shardIndex()).isEqualTo(1);
    assertThat(decoded.sealedFileBlobs().get(1).bytes()).isEqualTo(sealed1);
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
    assertThat(decoded.walEntries().getFirst()).isEqualTo(clearWal);
    assertThat(decoded.bucketDeltas().getFirst()).isEqualTo(delta);
    assertThat(decoded.sealedFileBlobs()).hasSize(1);
    assertThat(decoded.sealedFileBlobs().getFirst().bytes()).isEqualTo(sealed);
  }

  @Test
  void corruptedSealedBlobFailsCrc() {
    final byte[] sealed = new byte[256];
    for (int i = 0; i < sealed.length; i++)
      sealed[i] = (byte) i;

    final ByteString encoded = RaftLogEntryCodec.encodeSchemaEntry("testdb", "{}",
        Map.of(), Map.of(), List.of(), List.of(),
        List.of(new RaftLogEntryCodec.TsSealedBlob("weather", 0, "weather_shard_0.ts.sealed", sealed)));

    // Flip a byte inside the trailing compressed blob payload to corrupt it.
    final byte[] corrupted = new byte[encoded.size()];
    encoded.copyTo(corrupted, 0);
    corrupted[corrupted.length - 1] ^= 0xFF;

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
}
