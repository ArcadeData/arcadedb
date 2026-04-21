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
import org.junit.jupiter.api.Test;

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
    java.util.Arrays.fill(fakeWal, (byte) 42);
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
    final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    try (final java.io.DataOutputStream dos = new java.io.DataOutputStream(baos)) {
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
    final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    try (final java.io.DataOutputStream dos = new java.io.DataOutputStream(baos)) {
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

    org.assertj.core.api.Assertions.assertThatThrownBy(() -> RaftLogEntryCodec.decode(corruptedBS))
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

    org.assertj.core.api.Assertions.assertThatThrownBy(() -> RaftLogEntryCodec.decode(corruptedBS))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("trailing");
  }
}
