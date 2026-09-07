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

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7219, the receiver-side backstop: a {@code SCHEMA_ENTRY} carrying a section this version does not know
 * about is refused rather than half-applied.
 * <p>
 * Peer-capability negotiation is the guarantee - a leader does not write an optional section until every peer has
 * advertised that it decodes it. This is what catches the release that adds a section and forgets to declare a
 * capability for it. It cannot help a node that predates the section (nothing can, retroactively), which is
 * exactly why the leader-side gate is the primary mechanism and this is only the second line.
 * <p>
 * The refusal is a decode failure naming the database, so it quarantines and resyncs that one database (#7138)
 * instead of halting the node - loud and recoverable, where accepting the entry would be silent and permanent.
 */
class Issue7219SchemaEntryTrailingSectionTest {

  private static final String DB = "mydb";

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

  @Test
  void aDeltaBearingEntryStillDecodes() {
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, "", Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false,
        Collections.emptyList(), new SchemaDelta.Payload(41L, "{\"types\":{}}"));

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry);

    assertThat(decoded.schemaDelta()).isNotNull();
    assertThat(decoded.schemaDelta().baseVersion()).isEqualTo(41L);
  }

  @Test
  void aSectionAppendedAfterTheDeltaIsRefusedRatherThanIgnored() {
    // What a newer node adding a section would produce. Before #7219 these bytes were dropped on the floor and
    // the entry applied as if the section were not there - the silent half-apply this issue is about.
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, "", Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false,
        Collections.emptyList(), new SchemaDelta.Payload(41L, "{\"types\":{}}"));

    final ByteString withFutureSection = entry.concat(ByteString.copyFrom(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }));

    assertThatThrownBy(() -> RaftLogEntryCodec.decode(withFutureSection))
        .isInstanceOf(RaftLogEntryDecodeException.class)
        .hasMessageContaining(DB)
        .hasMessageContaining("trailing bytes")
        .hasMessageContaining("7219");
  }

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
}
