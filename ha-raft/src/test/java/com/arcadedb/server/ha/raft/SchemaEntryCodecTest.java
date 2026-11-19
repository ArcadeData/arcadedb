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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Wire format of the schema-related {@link RaftLogEntryCodec} entries: {@code SCHEMA_ENTRY} (delta, trailing sections, file renames), the database configuration entry and the transaction prepared-at index.
 */
class SchemaEntryCodecTest {

  // ---- #6989 ----
  // the {@code SCHEMA_ENTRY} wire format carries an OPTIONAL trailing schema-delta section, so a DDL
  // can ship what it changed instead of the whole schema document.
  // <p>
  // These tests pin the section's placement and its presence rule. The rule is the same one #4382, #5443 and
  // #4416 use - no remaining bytes means the section is absent - which is what keeps an entry produced without a
  // delta byte-identical to what the previous codec emitted.

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

  // Regression for #6989: the SCHEMA_ENTRY wire format carries an OPTIONAL trailing schema-delta section, so a DDL can ship what it changed instead of the whole schema docum...
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

  // Regression for #6989: the SCHEMA_ENTRY wire format carries an OPTIONAL trailing schema-delta section, so a DDL can ship what it changed instead of the whole schema docum...
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

  // Regression for #6989: the SCHEMA_ENTRY wire format carries an OPTIONAL trailing schema-delta section, so a DDL can ship what it changed instead of the whole schema docum...
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

  // Regression for #6989: the SCHEMA_ENTRY wire format carries an OPTIONAL trailing schema-delta section, so a DDL can ship what it changed instead of the whole schema docum...
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

  // Regression for #6989: the SCHEMA_ENTRY wire format carries an OPTIONAL trailing schema-delta section, so a DDL can ship what it changed instead of the whole schema docum...
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

  // Regression for #6989: the SCHEMA_ENTRY wire format carries an OPTIONAL trailing schema-delta section, so a DDL can ship what it changed instead of the whole schema docum...
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

  // ---- #7219 ----
  // the receiver-side backstop: a {@code SCHEMA_ENTRY} carrying a section this version does not know
  // about is refused rather than half-applied.
  // <p>
  // Peer-capability negotiation is the guarantee - a leader does not write an optional section until every peer has
  // advertised that it decodes it. This is what catches the release that adds a section and forgets to declare a
  // capability for it. It cannot help a node that predates the section (nothing can, retroactively), which is
  // exactly why the leader-side gate is the primary mechanism and this is only the second line.
  // <p>
  // The refusal is a decode failure naming the database, so it quarantines and resyncs that one database (#7138)
  // instead of halting the node - loud and recoverable, where accepting the entry would be silent and permanent.

  // Regression for #7219: the receiver-side backstop: a SCHEMA_ENTRY carrying a section this version does not know about is refused rather than half-applied.
  @Test
  void anEntryWithNoTrailingSectionStillDecodes() {
    // The control: everything this version writes must keep decoding, or the check below is just breakage.
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, "{\"schemaVersion\":7}",
        Map.of(3, "one.bucket"), Collections.emptyMap());

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SCHEMA_ENTRY);
    assertThat(decoded.schemaJson()).isEqualTo("{\"schemaVersion\":7}");
    assertThat(decoded.schemaDelta()).isNull();
  }

  // Regression for #7219: the receiver-side backstop: a SCHEMA_ENTRY carrying a section this version does not know about is refused rather than half-applied.
  @Test
  void aDeltaBearingEntryStillDecodes() {
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, "", Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false,
        Collections.emptyList(), new SchemaDelta.Payload(41L, "{\"types\":{}}"));

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry);

    assertThat(decoded.schemaDelta()).isNotNull();
    assertThat(decoded.schemaDelta().baseVersion()).isEqualTo(41L);
  }

  // Regression for #7219: the receiver-side backstop: a SCHEMA_ENTRY carrying a section this version does not know about is refused rather than half-applied.
  @Test
  void aSectionAppendedAfterTheDeltaIsRefusedRatherThanIgnored() {
    // What a newer node adding a section would produce. Before #7219 these bytes were dropped on the floor and
    // the entry applied as if the section were not there - the silent half-apply this issue is about. Appended after
    // the LAST section this codec knows, which since #9762 is the file-rename section after the delta.
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, "", Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false,
        Collections.emptyList(), new SchemaDelta.Payload(41L, "{\"types\":{}}"), Map.of(3, "Renamed_0"));

    final ByteString withFutureSection = entry.concat(ByteString.copyFrom(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }));

    assertThatThrownBy(() -> RaftLogEntryCodec.decode(withFutureSection))
        .isInstanceOf(RaftLogEntryDecodeException.class)
        .hasMessageContaining(DB)
        .hasMessageContaining("trailing bytes")
        .hasMessageContaining("7219");
  }

  // Regression for #7219: the receiver-side backstop: a SCHEMA_ENTRY carrying a section this version does not know about is refused rather than half-applied.
  @Test
  void theRefusalNamesTheDatabaseSoItQuarantinesRatherThanHaltingTheNode() {
    // The database name is what routes a decode failure to a per-database quarantine instead of a node-wide halt
    // (#7138). A refusal that lost it would take the whole node down for one unreadable entry.
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, "", Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false,
        Collections.emptyList(), new SchemaDelta.Payload(41L, "{\"types\":{}}"))
        .concat(ByteString.copyFrom(new byte[] { 9, 9, 9, 9 }));

    assertThatThrownBy(() -> RaftLogEntryCodec.decode(entry))
        .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.throwable(RaftLogEntryDecodeException.class))
        .extracting(RaftLogEntryDecodeException::getDatabaseName)
        .isEqualTo(DB);
  }

  // ---- #9762 ----
  // the {@code SCHEMA_ENTRY} wire format carries an optional trailing file-rename section, so a type rename can
  // tell the followers which files to move before they load a schema naming them. These tests pin its placement after the
  // schema-delta slot, the empty slot it forces out when the entry carries no delta, and that an entry without renames is
  // byte-identical to what the previous codec produced.

  private static final Map<Integer, String> RENAMES = renames();

  private static Map<Integer, String> renames() {
    final Map<Integer, String> renames = new LinkedHashMap<>();
    renames.put(1, "R2_0");
    renames.put(2, "R2_1");
    renames.put(5, "R2_0_123456789");
    return renames;
  }

  private static final String SCHEMA_JSON = new JSONObject().put("schemaVersion", 3).toString();

  // Regression for #9762: the SCHEMA_ENTRY wire format carries an optional trailing file-rename section, so a type rename can tell the followers which files to move before t...
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

  // Regression for #9762: the SCHEMA_ENTRY wire format carries an optional trailing file-rename section, so a type rename can tell the followers which files to move before t...
  @Test
  void renamesRideAfterADelta() {
    final SchemaDelta.Payload delta = new SchemaDelta.Payload(41L, new JSONObject().put(SchemaDelta.FIELD_BASE, 41L).toString());
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, "", ADD, REMOVE, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), false, Collections.emptyList(), delta, RENAMES);

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry);

    assertThat(decoded.schemaDelta()).isEqualTo(delta);
    assertThat(decoded.filesToRename()).isEqualTo(RENAMES);
  }

  // Regression for #9762: the SCHEMA_ENTRY wire format carries an optional trailing file-rename section, so a type rename can tell the followers which files to move before t...
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

  // Regression for #9762: the SCHEMA_ENTRY wire format carries an optional trailing file-rename section, so a type rename can tell the followers which files to move before t...
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
  // Regression for #9762: the SCHEMA_ENTRY wire format carries an optional trailing file-rename section, so a type rename can tell the followers which files to move before t...
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

  // Regression for #9762: the SCHEMA_ENTRY wire format carries an optional trailing file-rename section, so a type rename can tell the followers which files to move before t...
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

  // Regression for #9762: the SCHEMA_ENTRY wire format carries an optional trailing file-rename section, so a type rename can tell the followers which files to move before t...
  @Test
  void thisBuildAdvertisesTheRenameSection() {
    assertThat(PeerCapabilities.LOCAL).contains(PeerCapabilities.SCHEMA_FILE_RENAMES);
  }

  // ---- #9763 ----
  // Wire coverage for the entry type issue #9763 adds: the {@code configuration.json} of one database, which used to be
  // written on the node that ran {@code ALTER DATABASE} only.

  private static final String CONFIGURATION =
      "{\"configuration\":{\"dateImplementation\":\"java.time.LocalDate\",\"typeDefaultBuckets\":5}}";

  // Regression for #9763: Wire coverage for the entry type issue #9763 adds: the configuration.json of one database, which used to be written on the node that ran ALTER DATA...
  @Test
  void aConfigurationEntryRoundTripsWithItsDatabase() {
    final RaftLogEntryCodec.DecodedEntry decoded =
        RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDatabaseConfigurationEntry("db1", CONFIGURATION));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.DATABASE_CONFIGURATION_ENTRY);
    assertThat(decoded.databaseName())
        .as("database-scoped: the applier resolves the database, and the per-database applied position advances")
        .isEqualTo("db1");
    assertThat(decoded.usersJson()).isEqualTo(CONFIGURATION);
  }

  /** writeUTF stops at 64KB; a length-prefixed document does not, and non-ASCII survives the UTF-8 round trip. */
  // Regression for #9763: Wire coverage for the entry type issue #9763 adds: the configuration.json of one database, which used to be written on the node that ran ALTER DATA...
  @Test
  void aLargeNonAsciiDocumentRoundTrips() {
    final String big = "{\"configuration\":{\"externalPropertyBucketPath\":\"" + "ü".repeat(70_000) + "\"}}";

    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDatabaseConfigurationEntry("db1", big)).usersJson())
        .isEqualTo(big);
  }

  /** A new type must take an id no existing type uses, or a peer would hand the document to the wrong applier. */
  // Regression for #9763: Wire coverage for the entry type issue #9763 adds: the configuration.json of one database, which used to be written on the node that ran ALTER DATA...
  @Test
  void theTypeIdIsUniqueAndDecodesBackToItself() {
    final Set<Byte> ids = new HashSet<>();
    for (final RaftLogEntryType type : RaftLogEntryType.values())
      assertThat(ids.add(type.getId())).as("duplicate id %d on %s", type.getId(), type).isTrue();

    assertThat(RaftLogEntryType.fromId((byte) 9)).isEqualTo(RaftLogEntryType.DATABASE_CONFIGURATION_ENTRY);
  }

  /** The framed extension mechanism of issue #7138 applies to this type: a field added later is skipped, not fatal. */
  // Regression for #9763: Wire coverage for the entry type issue #9763 adds: the configuration.json of one database, which used to be written on the node that ran ALTER DATA...
  @Test
  void aTrailingExtensionSectionIsTolerated() {
    final ByteString entry = RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeDatabaseConfigurationEntry("db1", CONFIGURATION), new byte[] { 0, 1, 'x' });

    assertThat(RaftLogEntryCodec.decode(entry).usersJson()).isEqualTo(CONFIGURATION);
  }

  /** The token is permanent: renaming it makes every older peer read as incapable and refuses every ALTER DATABASE. */
  // Regression for #9763: Wire coverage for the entry type issue #9763 adds: the configuration.json of one database, which used to be written on the node that ran ALTER DATA...
  @Test
  void theCapabilityIsAdvertisedAndGatesTheType() {
    assertThat(PeerCapabilities.DATABASE_CONFIGURATION_ENTRY).isEqualTo("database-configuration-entry");
    assertThat(PeerCapabilities.LOCAL).contains(PeerCapabilities.DATABASE_CONFIGURATION_ENTRY);
    assertThat(SecurityEntryCapabilityGate.capabilityFor(RaftLogEntryType.DATABASE_CONFIGURATION_ENTRY))
        .isEqualTo(PeerCapabilities.DATABASE_CONFIGURATION_ENTRY);
  }

  // ---- #8686 ----
  // a TX_ENTRY carries the Raft log index its originator had applied when it prepared the transaction, as
  // a framed extension section, so the leader can tell a transaction prepared before a schema change from one prepared
  // after it. The section is optional: an entry without it (a peer that predates it, or a node that could not state the
  // index) reads as "unknown" and is never refused for it.
  //

  private static final byte[] WAL = { 1, 2, 3, 4 };

  // Regression for #8686: a TX_ENTRY carries the Raft log index its originator had applied when it prepared the transaction, as a framed extension section, so the leader can...
  @Test
  void thePreparedAtIndexRoundTrips() {
    final ByteString entry = RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap(), 4711L);

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry);

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.TX_ENTRY);
    assertThat(decoded.txPreparedAtIndex()).isEqualTo(4711L);
    assertThat(decoded.walData()).isEqualTo(WAL);
  }

  // Regression for #8686: a TX_ENTRY carries the Raft log index its originator had applied when it prepared the transaction, as a framed extension section, so the leader can...
  @Test
  void anEntryWithoutTheSectionReadsAsUnknown() {
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap()));

    assertThat(decoded.txPreparedAtIndex()).isEqualTo(-1L);
  }

  // Regression for #8686: a TX_ENTRY carries the Raft log index its originator had applied when it prepared the transaction, as a framed extension section, so the leader can...
  @Test
  void anUnknownIndexWritesNoSectionAtAll() {
    // Byte for byte the entry an older build writes, so an unknown index costs nothing on the wire.
    assertThat(RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap(), -1L))
        .isEqualTo(RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap()));
  }

  /** The section is framed, so a decoder that predates it skips it: what makes emitting it safe on a mixed cluster. */
  // Regression for #8686: a TX_ENTRY carries the Raft log index its originator had applied when it prepared the transaction, as a framed extension section, so the leader can...
  @Test
  void theSectionIsAFramedExtensionThatAnOlderDecoderSkips() {
    final ByteString withSection = RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap(), 9L);
    final ByteString plain = RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap());

    assertThat(withSection.size()).isGreaterThan(plain.size());
    assertThat(withSection.substring(0, plain.size())).as("the fields an older build reads are untouched").isEqualTo(plain);
    assertThat(withSection.substring(plain.size(), plain.size() + 4).toByteArray())
        .as("and the tail opens with the extension magic").containsExactly(0x41, 0x44, 0x42, 0x58);
  }

  // Regression for #8686: a TX_ENTRY carries the Raft log index its originator had applied when it prepared the transaction, as a framed extension section, so the leader can...
  @Test
  void aSectionOfAnotherNameIsSkippedAndTheIndexStaysUnknown() {
    final ByteString entry = RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap()), new byte[] { 0, 3, 'x', 'y', 'z' });

    assertThat(RaftLogEntryCodec.decode(entry).txPreparedAtIndex()).isEqualTo(-1L);
  }

  // Regression for #8686: a TX_ENTRY carries the Raft log index its originator had applied when it prepared the transaction, as a framed extension section, so the leader can...
  @Test
  void aTruncatedSectionIsRefusedAsCorruptionRatherThanReadAsUnknown() {
    // Named like ours but carrying no long: reading it as "unknown" would silently switch the check off.
    final ByteString entry = RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeTxEntry("db", WAL, Collections.emptyMap()),
        new byte[] { 0, 20, 't', 'x', '-', 'p', 'r', 'e', 'p', 'a', 'r', 'e', 'd', '-', 'a', 't', '-', 'i', 'n', 'd', 'e', 'x', 1, 2 });

    assertThatThrownBy(() -> RaftLogEntryCodec.decode(entry))
        .isInstanceOf(RaftLogEntryDecodeException.class);
  }
}
