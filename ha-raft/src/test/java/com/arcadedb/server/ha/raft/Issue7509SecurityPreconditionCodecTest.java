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

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wire coverage for the compare-and-set precondition issue #7509 adds to the three node-scoped security
 * entries.
 * <p>
 * The precondition rides in an extension section rather than in a new field, because of what has to stay true
 * during a rolling upgrade: a peer that predates the section must still be able to DECODE the entry - it skips
 * what it does not recognise (issue #7138) and installs the document unconditionally, which is exactly the
 * behaviour it had before. That is the property {@link #anEntryWrittenWithoutAPreconditionDecodesToNull} and
 * {@link #anUnrelatedExtensionSectionDoesNotHideThePrecondition} pin: the section is optional in both
 * directions, and one section never swallows another.
 */
class Issue7509SecurityPreconditionCodecTest {

  private static final String USERS       = "[{\"name\":\"root\"}]";
  private static final String GROUPS      = "{\"databases\":{\"*\":{\"groups\":{}}},\"version\":2}";
  private static final String TOKENS      = "{\"version\":1,\"tokens\":[]}";
  private static final String FINGERPRINT = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

  @Test
  void aUsersEntryCarriesItsPreconditionAndItsPayloadUnchanged() {
    final RaftLogEntryCodec.DecodedEntry decoded =
        RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityUsersEntry(USERS, FINGERPRINT));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_USERS_ENTRY);
    assertThat(decoded.usersJson()).as("the section must not disturb the document").isEqualTo(USERS);
    assertThat(decoded.securityPrecondition()).isEqualTo(FINGERPRINT);
  }

  @Test
  void aGroupsEntryCarriesItsPrecondition() {
    final RaftLogEntryCodec.DecodedEntry decoded =
        RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityGroupsEntry(GROUPS, FINGERPRINT));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_GROUPS_ENTRY);
    assertThat(decoded.usersJson()).isEqualTo(GROUPS);
    assertThat(decoded.securityPrecondition()).isEqualTo(FINGERPRINT);
  }

  @Test
  void anApiTokensEntryCarriesItsPrecondition() {
    final RaftLogEntryCodec.DecodedEntry decoded =
        RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityApiTokensEntry(TOKENS, FINGERPRINT));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_API_TOKENS_ENTRY);
    assertThat(decoded.usersJson()).isEqualTo(TOKENS);
    assertThat(decoded.securityPrecondition()).isEqualTo(FINGERPRINT);
  }

  /** A seed, and every entry a node that predates issue #7509 writes: no section, so no precondition. */
  @Test
  void anEntryWrittenWithoutAPreconditionDecodesToNull() {
    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityUsersEntry(USERS)).securityPrecondition())
        .isNull();
    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityUsersEntry(USERS, null)).securityPrecondition())
        .isNull();
    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityGroupsEntry(GROUPS)).securityPrecondition())
        .isNull();
    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityApiTokensEntry(TOKENS)).securityPrecondition())
        .isNull();
  }

  /**
   * A future section appended after this one - the case the framing exists for - is skipped, and the
   * precondition in front of it is still read.
   */
  @Test
  void anUnrelatedExtensionSectionDoesNotHideThePrecondition() {
    final ByteString entry = RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeSecurityUsersEntry(USERS, FINGERPRINT),
        "some-future-section".getBytes(StandardCharsets.UTF_8));

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry);

    assertThat(decoded.usersJson()).isEqualTo(USERS);
    assertThat(decoded.securityPrecondition()).isEqualTo(FINGERPRINT);
  }

  /** And an entry that carries ONLY a section this version does not know still decodes, with no precondition. */
  @Test
  void anEntryCarryingOnlyAnUnknownSectionStillDecodes() {
    final ByteString entry = RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeSecurityUsersEntry(USERS),
        "some-future-section".getBytes(StandardCharsets.UTF_8));

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(entry);

    assertThat(decoded.usersJson()).isEqualTo(USERS);
    assertThat(decoded.securityPrecondition()).isNull();
  }

  /** No other entry type grows a precondition by accident. */
  @Test
  void aNonSecurityEntryHasNoPrecondition() {
    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry("graph")).securityPrecondition())
        .isNull();
  }
}
