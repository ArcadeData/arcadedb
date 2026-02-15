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
  void roundTripLargeWalData() {
    final byte[] walData = new byte[1024 * 1024]; // 1MB
    for (int i = 0; i < walData.length; i++)
      walData[i] = (byte) (i % 256);

    final ByteString encoded = RaftLogEntryCodec.encodeTxEntry("bigdb", walData, Map.of());
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(encoded);

    assertThat(decoded.walData()).isEqualTo(walData);
  }
}
