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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Wire-format tests for the replicated security entries (config entries and the security precondition) in {@link RaftLogEntryCodec}.
 *
 * <p>Regression coverage for #7373:
 * Wire coverage for the two Raft entry types issue #7373 adds: the group document and the API-token document,
 * which used to be written to a file on whichever node served the request while the user list was replicated.
 * <p>
 * The three security entries share a wire shape and a payload slot, so the thing that has to hold is that the
 * type byte still tells them apart end to end. A decoder that collapsed the three - or a type id reused from an
 * entry that already exists - would deliver a group document to the user applier, which on a peer means
 * replacing every account on the node with nothing.
 *
 * <p>Regression coverage for #7509:
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
class SecurityEntryCodecTest {

  private static final String GROUPS = "{\"databases\":{\"*\":{\"groups\":{\"reader\":{\"access\":[]}}}},\"version\":2}";

  private static final String TOKENS = "{\"version\":1,\"tokens\":[{\"tokenHash\":\"ab12\",\"name\":\"ci\"}]}";

  // Regression for #7373: a groups entry round trips as a groups entry
  @Test
  void aGroupsEntryRoundTripsAsAGroupsEntry() {
    final RaftLogEntryCodec.DecodedEntry decoded =
        RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityGroupsEntry(GROUPS));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_GROUPS_ENTRY);
    assertThat(decoded.usersJson()).isEqualTo(GROUPS);
    assertThat(decoded.databaseName())
        .as("node-scoped like the users entry: no database, so it advances the global applied position only")
        .isEmpty();
  }

  // Regression for #7373: an api tokens entry round trips as an api tokens entry
  @Test
  void anApiTokensEntryRoundTripsAsAnApiTokensEntry() {
    final RaftLogEntryCodec.DecodedEntry decoded =
        RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityApiTokensEntry(TOKENS));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_API_TOKENS_ENTRY);
    assertThat(decoded.usersJson()).isEqualTo(TOKENS);
    assertThat(decoded.databaseName()).isEmpty();
  }

  // Regression for #7373: the three security entries are not interchangeable
  /**
   * The crux: identical wire shape, three different types. Encoding the same bytes under each type must decode
   * back to the type it was encoded as, never to a sibling.
   */
  @Test
  void theThreeSecurityEntriesAreNotInterchangeable() {
    final String payload = "[]";

    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityUsersEntry(payload)).type())
        .isEqualTo(RaftLogEntryType.SECURITY_USERS_ENTRY);
    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityGroupsEntry(payload)).type())
        .isEqualTo(RaftLogEntryType.SECURITY_GROUPS_ENTRY);
    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityApiTokensEntry(payload)).type())
        .isEqualTo(RaftLogEntryType.SECURITY_API_TOKENS_ENTRY);
  }

  // Regression for #7373: every entry type id is distinct and the new ones are seven and eight
  /**
   * A reused type id is silent at compile time and catastrophic at apply time - the applier would dispatch a
   * document to the wrong installer - so the ids are pinned here as well as being distinct.
   */
  @Test
  void everyEntryTypeIdIsDistinctAndTheNewOnesAreSevenAndEight() {
    final Set<Byte> ids = new HashSet<>();
    for (final RaftLogEntryType type : RaftLogEntryType.values())
      assertThat(ids.add(type.getId())).as("duplicate id for %s", type).isTrue();

    assertThat(RaftLogEntryType.SECURITY_GROUPS_ENTRY.getId()).isEqualTo((byte) 7);
    assertThat(RaftLogEntryType.SECURITY_API_TOKENS_ENTRY.getId()).isEqualTo((byte) 8);
    assertThat(RaftLogEntryType.fromId((byte) 7)).isEqualTo(RaftLogEntryType.SECURITY_GROUPS_ENTRY);
    assertThat(RaftLogEntryType.fromId((byte) 8)).isEqualTo(RaftLogEntryType.SECURITY_API_TOKENS_ENTRY);
  }

  // Regression for #7373: non ascii payloads survive the round trip
  /**
   * Unicode and newlines survive: the group document is written by an operator and the token document holds
   * names they chose, so the payload is not ASCII by construction.
   */
  @Test
  void nonAsciiPayloadsSurviveTheRoundTrip() {
    final String groups = "{\"databases\":{\"björn\":{\"groups\":{}}},\"version\":2}";
    final String tokens = "{\"version\":1,\"tokens\":[{\"tokenHash\":\"ab\",\"name\":\"ci\\nrunner\"}]}";

    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityGroupsEntry(groups)).usersJson())
        .isEqualTo(groups);
    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityApiTokensEntry(tokens)).usersJson())
        .isEqualTo(tokens);
  }

  // Regression for #7373: an empty token document round trips
  /** An empty document is a legitimate state - every token revoked - and must not be mistaken for a truncation. */
  @Test
  void anEmptyTokenDocumentRoundTrips() {
    final String empty = "{\"version\":1,\"tokens\":[]}";
    final ByteString encoded = RaftLogEntryCodec.encodeSecurityApiTokensEntry(empty);

    assertThat(RaftLogEntryCodec.decode(encoded).usersJson()).isEqualTo(empty);
  }

  private static final String USERS       = "[{\"name\":\"root\"}]";

  private static final String GROUPS_PRECONDITION      = "{\"databases\":{\"*\":{\"groups\":{}}},\"version\":2}";

  private static final String TOKENS_PRECONDITION      = "{\"version\":1,\"tokens\":[]}";

  private static final String FINGERPRINT = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

  // Regression for #7509: a users entry carries its precondition and its payload unchanged
  @Test
  void aUsersEntryCarriesItsPreconditionAndItsPayloadUnchanged() {
    final RaftLogEntryCodec.DecodedEntry decoded =
        RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityUsersEntry(USERS, FINGERPRINT));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_USERS_ENTRY);
    assertThat(decoded.usersJson()).as("the section must not disturb the document").isEqualTo(USERS);
    assertThat(decoded.securityPrecondition()).isEqualTo(FINGERPRINT);
  }

  // Regression for #7509: a groups entry carries its precondition
  @Test
  void aGroupsEntryCarriesItsPrecondition() {
    final RaftLogEntryCodec.DecodedEntry decoded =
        RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityGroupsEntry(GROUPS_PRECONDITION, FINGERPRINT));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_GROUPS_ENTRY);
    assertThat(decoded.usersJson()).isEqualTo(GROUPS_PRECONDITION);
    assertThat(decoded.securityPrecondition()).isEqualTo(FINGERPRINT);
  }

  // Regression for #7509: an api tokens entry carries its precondition
  @Test
  void anApiTokensEntryCarriesItsPrecondition() {
    final RaftLogEntryCodec.DecodedEntry decoded =
        RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityApiTokensEntry(TOKENS_PRECONDITION, FINGERPRINT));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_API_TOKENS_ENTRY);
    assertThat(decoded.usersJson()).isEqualTo(TOKENS_PRECONDITION);
    assertThat(decoded.securityPrecondition()).isEqualTo(FINGERPRINT);
  }

  // Regression for #7509: an entry written without a precondition decodes to null
  /** A seed, and every entry a node that predates issue #7509 writes: no section, so no precondition. */
  @Test
  void anEntryWrittenWithoutAPreconditionDecodesToNull() {
    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityUsersEntry(USERS)).securityPrecondition())
        .isNull();
    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityUsersEntry(USERS, null)).securityPrecondition())
        .isNull();
    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityGroupsEntry(GROUPS_PRECONDITION)).securityPrecondition())
        .isNull();
    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityApiTokensEntry(TOKENS_PRECONDITION)).securityPrecondition())
        .isNull();
  }

  // Regression for #7509: an unrelated extension section does not hide the precondition
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

  // Regression for #7509: an entry carrying only an unknown section still decodes
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

  // Regression for #7509: a corrupted precondition section fails the entry instead of applying it unconditionally
  /**
   * A section that IDENTIFIES ITSELF as the precondition but cannot be read past its name is corruption, and must
   * not degrade into "no precondition, install unconditionally".
   * <p>
   * That default is safe for an unrecognised section - it is what the #7138 framing is for - and is exactly wrong
   * for this one: the precondition is the guard that stops a stale security document from reverting a committed
   * change, so silently dropping it turns a corrupt entry into the divergence the guard exists to prevent. It
   * has to fail the entry like any other malformed payload in the codec.
   */
  @Test
  void aCorruptedPreconditionSectionFailsTheEntryInsteadOfApplyingItUnconditionally() {
    final ByteString truncated = RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeSecurityUsersEntry(USERS), utf(RaftLogEntryCodec.SECURITY_PRECONDITION_SECTION));

    assertThatThrownBy(() -> RaftLogEntryCodec.decode(truncated))
        .isInstanceOf(RaftLogEntryDecodeException.class)
        .hasMessageContaining("SECURITY_USERS_ENTRY");

    // Same for a fingerprint whose own UTF frame is cut short rather than absent entirely.
    final byte[] name = utf(RaftLogEntryCodec.SECURITY_PRECONDITION_SECTION);
    final byte[] halfAFingerprint = new byte[name.length + 3];
    System.arraycopy(name, 0, halfAFingerprint, 0, name.length);
    halfAFingerprint[name.length] = 0;
    halfAFingerprint[name.length + 1] = 32;   // declares 32 bytes of fingerprint...
    halfAFingerprint[name.length + 2] = 'a';  // ...and carries one

    assertThatThrownBy(() -> RaftLogEntryCodec.decode(
        RaftLogEntryCodec.appendExtensionSection(RaftLogEntryCodec.encodeSecurityUsersEntry(USERS), halfAFingerprint)))
        .isInstanceOf(RaftLogEntryDecodeException.class);
  }

  // Regression for #7509: a section whose name cannot be read is still skipped
  /** A section whose NAME is unreadable is not ours, so it is skipped - the #7138 contract, unchanged. */
  @Test
  void aSectionWhoseNameCannotBeReadIsStillSkipped() {
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(
        RaftLogEntryCodec.appendExtensionSection(RaftLogEntryCodec.encodeSecurityUsersEntry(USERS),
            new byte[] { 0, 40, 'x' }));   // declares a 40-byte name, carries one

    assertThat(decoded.usersJson()).isEqualTo(USERS);
    assertThat(decoded.securityPrecondition()).isNull();
  }

  private static byte[] utf(final String value) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);
      dos.writeUTF(value);
      dos.flush();
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }

  // Regression for #7509: a non security entry has no precondition
  /** No other entry type grows a precondition by accident. */
  @Test
  void aNonSecurityEntryHasNoPrecondition() {
    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeDropDatabaseEntry("graph")).securityPrecondition())
        .isNull();
  }
}
