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
 * Wire coverage for the two Raft entry types issue #7373 adds: the group document and the API-token document,
 * which used to be written to a file on whichever node served the request while the user list was replicated.
 * <p>
 * The three security entries share a wire shape and a payload slot, so the thing that has to hold is that the
 * type byte still tells them apart end to end. A decoder that collapsed the three - or a type id reused from an
 * entry that already exists - would deliver a group document to the user applier, which on a peer means
 * replacing every account on the node with nothing.
 */
class Issue7373SecurityConfigEntryCodecTest {

  private static final String GROUPS = "{\"databases\":{\"*\":{\"groups\":{\"reader\":{\"access\":[]}}}},\"version\":2}";
  private static final String TOKENS = "{\"version\":1,\"tokens\":[{\"tokenHash\":\"ab12\",\"name\":\"ci\"}]}";

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

  @Test
  void anApiTokensEntryRoundTripsAsAnApiTokensEntry() {
    final RaftLogEntryCodec.DecodedEntry decoded =
        RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSecurityApiTokensEntry(TOKENS));

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.SECURITY_API_TOKENS_ENTRY);
    assertThat(decoded.usersJson()).isEqualTo(TOKENS);
    assertThat(decoded.databaseName()).isEmpty();
  }

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

  /** An empty document is a legitimate state - every token revoked - and must not be mistaken for a truncation. */
  @Test
  void anEmptyTokenDocumentRoundTrips() {
    final String empty = "{\"version\":1,\"tokens\":[]}";
    final ByteString encoded = RaftLogEntryCodec.encodeSecurityApiTokensEntry(empty);

    assertThat(RaftLogEntryCodec.decode(encoded).usersJson()).isEqualTo(empty);
  }
}
