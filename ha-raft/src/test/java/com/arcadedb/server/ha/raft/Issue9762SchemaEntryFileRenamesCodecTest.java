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

import com.arcadedb.serializer.json.JSONObject;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9762: the {@code SCHEMA_ENTRY} wire format carries an optional trailing file-rename section, so a type rename can
 * tell the followers which files to move before they load a schema naming them. These tests pin its placement after the
 * schema-delta slot, the empty slot it forces out when the entry carries no delta, and that an entry without renames is
 * byte-identical to what the previous codec produced.
 */
class Issue9762SchemaEntryFileRenamesCodecTest {

  private static final String DB = "mydb";

  private static final Map<Integer, String> ADD     = Map.of(7, "Foo_7.bucket");
  private static final Map<Integer, String> REMOVE  = Map.of(3, "Old_3.bucket");
  private static final Map<Integer, String> RENAMES = renames();

  private static Map<Integer, String> renames() {
    final Map<Integer, String> renames = new LinkedHashMap<>();
    renames.put(1, "R2_0");
    renames.put(2, "R2_1");
    renames.put(5, "R2_0_123456789");
    return renames;
  }

  private static final String SCHEMA_JSON = new JSONObject().put("schemaVersion", 3).toString();

  @Test
  void renamesWithoutADeltaSurviveTheRoundTrip() {
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, SCHEMA_JSON, ADD, REMOVE, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), false, Collections.emptyList(), null, RENAMES);

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SCHEMA_ENTRY);
    assertThat(decoded.filesToRename()).isEqualTo(RENAMES);
    assertThat(decoded.schemaDelta()).as("the empty slot is not a delta").isNull();
    assertThat(decoded.schemaJson()).isEqualTo(SCHEMA_JSON);
    assertThat(decoded.filesToAdd()).isEqualTo(ADD);
    assertThat(decoded.filesToRemove()).isEqualTo(REMOVE);
    assertThat(decoded.sealedFileChunks()).isEmpty();
  }

  @Test
  void renamesRideAfterADelta() {
    final SchemaDelta.Payload delta = new SchemaDelta.Payload(41L, new JSONObject().put(SchemaDelta.FIELD_BASE, 41L).toString());
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, "", ADD, REMOVE, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), false, Collections.emptyList(), delta, RENAMES);

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry);

    assertThat(decoded.schemaDelta()).isEqualTo(delta);
    assertThat(decoded.filesToRename()).isEqualTo(RENAMES);
  }

  @Test
  void anEntryWithoutRenamesIsByteIdenticalToThePreviousCodec() {
    final ByteString withoutRenames = RaftLogEntryCodec.encodeSchemaEntry(DB, SCHEMA_JSON, ADD, REMOVE,
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false, Collections.emptyList(), null);
    final ByteString emptyRenames = RaftLogEntryCodec.encodeSchemaEntry(DB, SCHEMA_JSON, ADD, REMOVE,
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false, Collections.emptyList(), null,
        Collections.emptyMap());

    assertThat(emptyRenames).isEqualTo(withoutRenames);
    assertThat(RaftLogEntryCodec.decode(withoutRenames).filesToRename()).isEmpty();
  }

  @Test
  void aRenameToNoNameIsRefusedAsCorrupt() {
    final Map<Integer, String> broken = new LinkedHashMap<>();
    broken.put(1, null);
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, SCHEMA_JSON, ADD, REMOVE, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), false, Collections.emptyList(), null, broken);

    assertThatThrownBy(() -> RaftLogEntryCodec.decode(entry)).hasStackTraceContaining("renames file id 1 to no name");
  }

  /**
   * What a build predating this section reads: everything up to the delta slot, then bytes it does not know. Its #7219
   * backstop refuses them rather than applying a schema naming files it never moved - which is why the leader asks first.
   * Simulated by appending a section this build does not know after the rename section.
   */
  @Test
  void bytesAfterTheRenameSectionAreStillRefused() throws IOException {
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, SCHEMA_JSON, ADD, REMOVE, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), false, Collections.emptyList(), null, RENAMES);
    final ByteArrayOutputStream extended = new ByteArrayOutputStream();
    extended.write(entry.toByteArray());
    new DataOutputStream(extended).writeInt(42);

    assertThatThrownBy(() -> RaftLogEntryCodec.decode(ByteString.copyFrom(extended.toByteArray())))
        .hasStackTraceContaining("trailing bytes");
  }

  @Test
  void onASplitTheRenamesRideTheLastChunkOnly() {
    final List<byte[]> walEntries = new ArrayList<>();
    final List<Map<Integer, Integer>> bucketDeltas = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      final byte[] wal = new byte[4096];
      for (int b = 0; b < wal.length; b++)
        wal[b] = (byte) ((b * 31 + i * 7) & 0xFF);
      walEntries.add(wal);
      bucketDeltas.add(Collections.emptyMap());
    }

    final int single = RaftLogEntryCodec.encodeSchemaEntry(DB, SCHEMA_JSON, ADD, REMOVE, walEntries, bucketDeltas,
        Collections.emptyList(), false, Collections.emptyList(), null, RENAMES).size();

    final List<ByteString> chunks = RaftTransactionBroker.splitSchemaEntry(DB, SCHEMA_JSON, ADD, REMOVE, walEntries,
        bucketDeltas, Collections.emptyList(), Collections.emptyList(), single / 2, single, false, null, RENAMES);

    assertThat(chunks).hasSizeGreaterThan(1);
    for (int i = 0; i < chunks.size(); i++) {
      final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(chunks.get(i));
      if (i < chunks.size() - 1)
        assertThat(decoded.filesToRename()).as("chunk %d only delivers", i).isEmpty();
      else
        assertThat(decoded.filesToRename()).as("the publishing chunk").isEqualTo(RENAMES);
    }
  }

  @Test
  void thisBuildAdvertisesTheRenameSection() {
    assertThat(PeerCapabilities.LOCAL).contains(PeerCapabilities.SCHEMA_FILE_RENAMES);
  }
}
