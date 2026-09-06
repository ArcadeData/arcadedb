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

import com.arcadedb.serializer.json.JSONObject;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6989: the {@code SCHEMA_ENTRY} wire format carries an OPTIONAL trailing schema-delta section, so a DDL
 * can ship what it changed instead of the whole schema document.
 * <p>
 * These tests pin the section's placement and its presence rule. The rule is the same one #4382, #5443 and
 * #4416 use - no remaining bytes means the section is absent - which is what keeps an entry produced without a
 * delta byte-identical to what the previous codec emitted.
 */
class Issue6989SchemaDeltaCodecTest {

  private static final String DB = "mydb";

  private static final Map<Integer, String> ADD    = Map.of(7, "Foo_7.bucket");
  private static final Map<Integer, String> REMOVE = Map.of(3, "Old_3.bucket");

  private static JSONObject bigSchema(final int typeCount) {
    final JSONObject root = new JSONObject();
    root.put("schemaVersion", 42);
    final JSONObject types = new JSONObject();
    for (int i = 0; i < typeCount; i++) {
      final JSONObject type = new JSONObject();
      type.put("name", "Type_" + i);
      type.put("properties", new JSONObject().put("id", new JSONObject().put("type", "STRING")));
      types.put("Type_" + i, type);
    }
    root.put("types", types);
    return root;
  }

  /**
   * A delta the way the leader produces one. Built rather than hand-written so the size comparison below is
   * anchored to what {@link SchemaDelta} actually emits.
   */
  private static String deltaForOneChangedType(final JSONObject schema) {
    final JSONObject updated = new JSONObject(schema.toString());
    updated.put("schemaVersion", 43);
    updated.getJSONObject("types").getJSONObject("Type_7").put("properties",
        new JSONObject().put("id", new JSONObject().put("type", "STRING"))
            .put("extra", new JSONObject().put("type", "INTEGER")));
    return SchemaDelta.compute(schema, updated).toString();
  }

  /**
   * The codec treats a delta as an OPAQUE string - it length-prefixes, compresses and hands it back untouched -
   * so the placement tests carry a marker rather than a realistic document. The round trip asserting the exact
   * string back out is what pins that opacity; a realistic delta is used where the size is the point.
   */
  private static final String DELTA_MARKER = new JSONObject().put(SchemaDelta.FIELD_BASE, 41L).toString();

  @Test
  void anEntryWithoutADeltaDecodesAsBeforeAndCarriesNoDeltaSection() {
    final String schemaJson = new JSONObject().put("schemaVersion", 1).toString();
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, schemaJson, ADD, REMOVE);
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SCHEMA_ENTRY);
    assertThat(decoded.schemaDelta()).isNull();
    assertThat(decoded.schemaJson()).isEqualTo(schemaJson);
    assertThat(decoded.filesToAdd()).isEqualTo(ADD);
    assertThat(decoded.filesToRemove()).isEqualTo(REMOVE);
  }

  @Test
  void aDeltaSurvivesTheRoundTrip() {
    final JSONObject base = bigSchema(3);
    final JSONObject updated = new JSONObject(base.toString());
    updated.put("schemaVersion", 43);
    final String deltaJson = SchemaDelta.compute(base, updated).toString();

    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, "", ADD, REMOVE,
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false, Collections.emptyList(),
        new SchemaDelta.Payload(41L, deltaJson));

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry);

    assertThat(decoded.schemaDelta()).isNotNull();
    assertThat(decoded.schemaDelta().baseVersion()).isEqualTo(41L);
    assertThat(decoded.schemaDelta().deltaJson()).isEqualTo(deltaJson);
    assertThat(decoded.schemaJson()).isEmpty();
    assertThat(decoded.filesToAdd()).isEqualTo(ADD);
    assertThat(decoded.filesToRemove()).isEqualTo(REMOVE);
  }

  @Test
  void aDeltaRidesAlongsideEveryOtherOptionalSection() {
    final byte[] wal = "a WAL page".getBytes(StandardCharsets.UTF_8);
    final byte[] sealed = "a sealed store".getBytes(StandardCharsets.UTF_8);
    final Map<Integer, Integer> bucketDelta = new HashMap<>();
    bucketDelta.put(7, 3);

    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, "", ADD, REMOVE,
        List.of(wal), List.of(bucketDelta),
        List.of(new RaftLogEntryCodec.TsSealedBlob("Metric", 0, "Metric_0.sealed", sealed)),
        true, Collections.emptyList(), new SchemaDelta.Payload(41L, DELTA_MARKER));

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry);

    assertThat(decoded.walEntries()).hasSize(1);
    assertThat(decoded.walEntries().getFirst()).isEqualTo(wal);
    assertThat(decoded.bucketDeltas()).containsExactly(bucketDelta);
    assertThat(decoded.sealedFileBlobs()).hasSize(1);
    assertThat(decoded.sealedFileBlobs().getFirst().bytes()).isEqualTo(sealed);
    assertThat(decoded.moreChunksFollow()).isTrue();
    assertThat(decoded.sealedFileChunks()).isEmpty();
    assertThat(decoded.schemaDelta().deltaJson()).isEqualTo(DELTA_MARKER);
  }

  @Test
  void aDeltaRidesAfterSealedSlices() {
    final byte[] slice = "sliced sealed store".getBytes(StandardCharsets.UTF_8);
    final RaftLogEntryCodec.TsSealedChunk chunk = new RaftLogEntryCodec.TsSealedChunk("Metric", 0,
        "Metric_0.sealed", slice.length, 1234L, 0L, slice, true);

    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, "", Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false,
        List.of(chunk), new SchemaDelta.Payload(41L, DELTA_MARKER));

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry);

    assertThat(decoded.sealedFileChunks()).hasSize(1);
    assertThat(decoded.sealedFileChunks().getFirst().bytes()).isEqualTo(slice);
    assertThat(decoded.schemaDelta().deltaJson()).isEqualTo(DELTA_MARKER);
  }

  @Test
  void aDeltaEntryIsAFractionOfTheDocumentEntryForTheSameChange() {
    final JSONObject fullSchema = bigSchema(500);
    final String deltaJson = deltaForOneChangedType(fullSchema);

    final int documentEntry = RaftLogEntryCodec.encodeSchemaEntry(DB, fullSchema.toString(), ADD, REMOVE).size();
    final int deltaEntry = RaftLogEntryCodec.encodeSchemaEntry(DB, "", ADD, REMOVE,
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false, Collections.emptyList(),
        new SchemaDelta.Payload(41L, deltaJson)).size();

    assertThat(deltaEntry)
        .as("a one-type change must not cost an entry sized by the schema (%d bytes) - it costs %d",
            documentEntry, deltaEntry)
        .isLessThan(documentEntry / 10);
  }

  @Test
  void onASplitTheDeltaRidesTheLastChunkOnly() {
    // Four WAL entries that cannot share one entry, so the change is split; only the publishing chunk may
    // carry the delta, exactly like the schema JSON it replaces.
    final List<byte[]> walEntries = new ArrayList<>();
    final List<Map<Integer, Integer>> bucketDeltas = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      final byte[] wal = new byte[4096];
      // Non-repeating content so compression cannot collapse the groups back into one entry.
      for (int b = 0; b < wal.length; b++)
        wal[b] = (byte) ((b * 31 + i * 7) & 0xFF);
      walEntries.add(wal);
      bucketDeltas.add(Collections.emptyMap());
    }
    final SchemaDelta.Payload delta = new SchemaDelta.Payload(41L, DELTA_MARKER);

    final int single = RaftLogEntryCodec.encodeSchemaEntry(DB, "", ADD, REMOVE, walEntries, bucketDeltas,
        Collections.emptyList(), false, Collections.emptyList(), delta).size();

    final List<ByteString> chunks = RaftTransactionBroker.splitSchemaEntry(DB, "", ADD, REMOVE, walEntries,
        bucketDeltas, Collections.emptyList(), Collections.emptyList(), single / 2, single, false, delta);

    assertThat(chunks).hasSizeGreaterThan(1);
    for (int i = 0; i < chunks.size(); i++) {
      final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(chunks.get(i));
      if (i < chunks.size() - 1) {
        assertThat(decoded.schemaDelta()).as("chunk %d only delivers", i).isNull();
        assertThat(decoded.moreChunksFollow()).isTrue();
      } else {
        assertThat(decoded.schemaDelta()).as("the last chunk publishes").isNotNull();
        assertThat(decoded.schemaDelta().deltaJson()).isEqualTo(DELTA_MARKER);
        assertThat(decoded.moreChunksFollow()).isFalse();
      }
    }
  }
}
