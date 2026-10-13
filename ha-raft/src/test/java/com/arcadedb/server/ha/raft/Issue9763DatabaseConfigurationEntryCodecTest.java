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

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wire coverage for the entry type issue #9763 adds: the {@code configuration.json} of one database, which used to be
 * written on the node that ran {@code ALTER DATABASE} only.
 */
class Issue9763DatabaseConfigurationEntryCodecTest {

  private static final String CONFIGURATION =
      "{\"configuration\":{\"dateImplementation\":\"java.time.LocalDate\",\"typeDefaultBuckets\":5}}";

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
  @Test
  void aLargeNonAsciiDocumentRoundTrips() {
    final String big = "{\"configuration\":{\"externalPropertyBucketPath\":\"" + "ü".repeat(70_000) + "\"}}";

    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDatabaseConfigurationEntry("db1", big)).usersJson())
        .isEqualTo(big);
  }

  /** A new type must take an id no existing type uses, or a peer would hand the document to the wrong applier. */
  @Test
  void theTypeIdIsUniqueAndDecodesBackToItself() {
    final Set<Byte> ids = new HashSet<>();
    for (final RaftLogEntryType type : RaftLogEntryType.values())
      assertThat(ids.add(type.getId())).as("duplicate id %d on %s", type.getId(), type).isTrue();

    assertThat(RaftLogEntryType.fromId((byte) 9)).isEqualTo(RaftLogEntryType.DATABASE_CONFIGURATION_ENTRY);
  }

  /** The framed extension mechanism of issue #7138 applies to this type: a field added later is skipped, not fatal. */
  @Test
  void aTrailingExtensionSectionIsTolerated() {
    final ByteString entry = RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeDatabaseConfigurationEntry("db1", CONFIGURATION), new byte[] { 0, 1, 'x' });

    assertThat(RaftLogEntryCodec.decode(entry).usersJson()).isEqualTo(CONFIGURATION);
  }

  /** The token is permanent: renaming it makes every older peer read as incapable and refuses every ALTER DATABASE. */
  @Test
  void theCapabilityIsAdvertisedAndGatesTheType() {
    assertThat(PeerCapabilities.DATABASE_CONFIGURATION_ENTRY).isEqualTo("database-configuration-entry");
    assertThat(PeerCapabilities.LOCAL).contains(PeerCapabilities.DATABASE_CONFIGURATION_ENTRY);
    assertThat(SecurityEntryCapabilityGate.capabilityFor(RaftLogEntryType.DATABASE_CONFIGURATION_ENTRY))
        .isEqualTo(PeerCapabilities.DATABASE_CONFIGURATION_ENTRY);
  }
}
