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

import com.arcadedb.server.security.SecurityDocumentVersions;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The wire half of issue #7509: a node-scoped security entry now carries the version of the document its
 * submitter read and the version it establishes, so the apply can refuse an entry built from a document that
 * has since changed on another node.
 * <p>
 * The two versions ride in a {@link RaftLogEntryCodec#EXTENSION_MAGIC} extension section (the #7138 frame)
 * rather than in the entry body, and the tests below pin both directions of that choice: a new entry round
 * trips its versions, and an entry produced by a node that predates the fix - which carries no section at all -
 * still decodes, with no versions, which the applier reads as "apply unconditionally".
 */
class Issue7509SecurityEntryCasCodecTest {

  @Test
  void aSecurityUsersEntryRoundTripsItsCompareAndSetVersions() {
    final String payload = "[{\"name\":\"root\"}]";

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeSecurityUsersEntry(payload, 7L, 8L));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_USERS_ENTRY);
    assertThat(decoded.usersJson()).isEqualTo(payload);
    assertThat(decoded.securityCas()).isNotNull();
    assertThat(decoded.securityCas().expectedVersion()).isEqualTo(7L);
    assertThat(decoded.securityCas().newVersion()).isEqualTo(8L);
  }

  @Test
  void aSecurityGroupsEntryRoundTripsItsCompareAndSetVersions() {
    final String payload = "{\"version\":2,\"databases\":{}}";

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeSecurityGroupsEntry(payload, 0L, 1L));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_GROUPS_ENTRY);
    assertThat(decoded.usersJson()).isEqualTo(payload);
    assertThat(decoded.securityCas().expectedVersion()).isZero();
    assertThat(decoded.securityCas().newVersion()).isEqualTo(1L);
  }

  @Test
  void aSecurityApiTokensEntryRoundTripsItsCompareAndSetVersions() {
    final String payload = "{\"version\":1,\"tokens\":[]}";

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeSecurityApiTokensEntry(payload, 3L, 4L));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_API_TOKENS_ENTRY);
    assertThat(decoded.usersJson()).isEqualTo(payload);
    assertThat(decoded.securityCas().expectedVersion()).isEqualTo(3L);
    assertThat(decoded.securityCas().newVersion()).isEqualTo(4L);
  }

  /** A peer seed: applied whatever the receiving node holds, but still stamping the version it establishes. */
  @Test
  void anUnconditionalSeedCarriesTheSentinelAndTheVersionItEstablishes() {
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeSecurityUsersEntry("[]", SecurityDocumentVersions.UNCONDITIONAL, 12L));

    assertThat(decoded.securityCas().expectedVersion()).isEqualTo(SecurityDocumentVersions.UNCONDITIONAL);
    assertThat(decoded.securityCas().newVersion()).isEqualTo(12L);
  }

  /**
   * The rolling-upgrade half: an entry a node that predates #7509 wrote carries no section at all. It must still
   * decode - halting on it would take down every not-yet-upgraded peer - and it must decode with NO versions, so
   * the applier installs it unconditionally exactly as it does today.
   */
  @Test
  void anEntryFromANodeThatPredatesTheFixDecodesWithNoVersions() throws IOException {
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(
        legacySecurityEntry(RaftLogEntryType.SECURITY_USERS_ENTRY, "[{\"name\":\"root\"}]"));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_USERS_ENTRY);
    assertThat(decoded.usersJson()).isEqualTo("[{\"name\":\"root\"}]");
    assertThat(decoded.securityCas())
        .as("no section means no compare-and-set, which the applier reads as 'apply regardless'")
        .isNull();
  }

  /** Every other entry type is unaffected: it carries no security section and decodes with none. */
  @Test
  void anEntryOfAnotherTypeCarriesNoCompareAndSetVersions() {
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(
        RaftLogEntryCodec.encodeDropDatabaseEntry("graph"));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.DROP_DATABASE_ENTRY);
    assertThat(decoded.securityCas()).isNull();
  }

  /**
   * A section this version does not know is still skipped rather than mistaken for the compare-and-set one -
   * the forward compatibility #7138 exists for has to keep working now that something actually reads a section.
   */
  @Test
  void anUnknownExtensionSectionIsSkippedAndDoesNotBecomeTheVersions() {
    final ByteString withForeignSection = RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeSecurityUsersEntry("[]", 5L, 6L), new byte[] { 1, 2, 3 });

    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(withForeignSection);

    assertThat(decoded.usersJson()).isEqualTo("[]");
    assertThat(decoded.securityCas().expectedVersion()).isEqualTo(5L);
    assertThat(decoded.securityCas().newVersion()).isEqualTo(6L);
  }

  /** The entry shape a node that predates #7509 produces: no trailing extension section. */
  private static ByteString legacySecurityEntry(final RaftLogEntryType type, final String json) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final DataOutputStream dos = new DataOutputStream(baos);
    dos.writeByte(type.getId());
    dos.writeUTF("");
    final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
    dos.writeInt(bytes.length);
    dos.write(bytes);
    dos.flush();
    return ByteString.copyFrom(baos.toByteArray());
  }
}
