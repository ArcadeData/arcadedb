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

import com.arcadedb.server.ServerException;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10761: {@code parsePeerList} never applied {@code validatePeerAddress}, the legal-address rule of its own class,
 * and split the positional form on every colon. A bracketed IPv6 entry parsed into a Raft peer at {@code [:} while the
 * gRPC allowlist resolved {@code ::1} from the same line, and a non-numeric or out-of-range port became a peer address.
 * <p>
 * The census below is over every entry shape the javadoc of {@code parsePeerList} documents, each with a DNS name, an
 * IPv4 and a bracketed IPv6 host: the host the allowlist derives equals the host of the Raft address, and every address
 * built passes {@code validatePeerAddress}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10761PeerListAddressValidationTest {

  /** Every documented shape, with {@code %s} for the host. */
  private static final List<String> SHAPES = List.of(
      "%s",
      "%s:2434",
      "%s:2434:2480",
      "%s:2434:2480:5",
      "%s:2434:2480:5:2490",
      "%s:{raft:2434}",
      "%s:{raft:2434,http:2480,https:2490,bolt:7687,grpc:50051,priority:10}",
      "%s:{}",
      "frankfurt@%s:2434:2480",
      "frankfurt@%s:{raft:2434,http:2480}");

  private static final List<String> HOSTS = List.of("arcadedb-0", "10.0.0.1", "[2001:db8::1]", "[::1]");

  @Test
  void theAllowlistAndTheRaftPeerDeriveOneHostFromEveryDocumentedShape() {
    final List<String> expectedEntries = List.of(
        "arcadedb-0",
        "arcadedb-0:2434",
        "arcadedb-0:2434:2480",
        "arcadedb-0:2434:2480:5",
        "arcadedb-0:2434:2480:5:2490",
        "arcadedb-0:{raft:2434}",
        "arcadedb-0:{raft:2434,http:2480,https:2490,bolt:7687,grpc:50051,priority:10}",
        "arcadedb-0:{}",
        "frankfurt@arcadedb-0:2434:2480",
        "frankfurt@arcadedb-0:{raft:2434,http:2480}",
        "10.0.0.1",
        "10.0.0.1:2434",
        "10.0.0.1:2434:2480",
        "10.0.0.1:2434:2480:5",
        "10.0.0.1:2434:2480:5:2490",
        "10.0.0.1:{raft:2434}",
        "10.0.0.1:{raft:2434,http:2480,https:2490,bolt:7687,grpc:50051,priority:10}",
        "10.0.0.1:{}",
        "frankfurt@10.0.0.1:2434:2480",
        "frankfurt@10.0.0.1:{raft:2434,http:2480}",
        "[2001:db8::1]",
        "[2001:db8::1]:2434",
        "[2001:db8::1]:2434:2480",
        "[2001:db8::1]:2434:2480:5",
        "[2001:db8::1]:2434:2480:5:2490",
        "[2001:db8::1]:{raft:2434}",
        "[2001:db8::1]:{raft:2434,http:2480,https:2490,bolt:7687,grpc:50051,priority:10}",
        "[2001:db8::1]:{}",
        "frankfurt@[2001:db8::1]:2434:2480",
        "frankfurt@[2001:db8::1]:{raft:2434,http:2480}",
        "[::1]",
        "[::1]:2434",
        "[::1]:2434:2480",
        "[::1]:2434:2480:5",
        "[::1]:2434:2480:5:2490",
        "[::1]:{raft:2434}",
        "[::1]:{raft:2434,http:2480,https:2490,bolt:7687,grpc:50051,priority:10}",
        "[::1]:{}",
        "frankfurt@[::1]:2434:2480",
        "frankfurt@[::1]:{raft:2434,http:2480}");
    final List<String> checked = new ArrayList<>();
    for (final String host : HOSTS)
      for (final String shape : SHAPES) {
        final String entry = shape.formatted(host);
        final RaftPeerAddressResolver.ParsedPeerList parsed = RaftPeerAddressResolver.parsePeerList(entry, 2424);
        assertThat(parsed.peers()).as(entry).hasSize(1);
        final RaftPeer peer = parsed.peers().getFirst();

        final String raftHost = RaftHAServer.extractHost(peer.getAddress());
        assertThat(raftHost).as("the host of the Raft address of '%s'", entry).isEqualTo(host);
        assertThat(PeerAddressAllowlistFilter.extractPeerHosts(entry)).as("the allowlist host of '%s'", entry)
            .containsExactly(RaftPeerAddressResolver.unbracketed(raftHost));

        assertLegal(entry, peer.getAddress());
        assertLegal(entry, parsed.httpAddresses().get(peer.getId()));
        assertLegal(entry, parsed.httpsAddresses().get(peer.getId()));
        assertLegal(entry, parsed.boltAddresses().get(peer.getId()));
        assertLegal(entry, parsed.grpcAddresses().get(peer.getId()));
        checked.add(entry);
      }
    assertThat(checked).containsExactlyInAnyOrderElementsOf(expectedEntries);
  }

  /** The two entries of the report, verbatim: the positional IPv6 form keeps its host whole in every field. */
  @Test
  void aPositionalIpv6EntryKeepsItsHostWhole() {
    final RaftPeerAddressResolver.ParsedPeerList parsed = RaftPeerAddressResolver.parsePeerList("[::1]:2434:2480", 2424);
    final RaftPeer peer = parsed.peers().getFirst();

    assertThat(peer.getAddress()).isEqualTo("[::1]:2434");
    assertThat(peer.getPriority()).isZero();
    assertThat(parsed.httpAddresses().get(peer.getId())).isEqualTo("[::1]:2480");
    assertThat(parsed.httpsAddresses()).isEmpty();
    // The id still follows the one rule every node applies to the Raft address
    assertThat(peer.getId()).isEqualTo(RaftPeerId.valueOf(RaftPeerAddressResolver.peerIdForAddress("[::1]:2434")));

    final RaftPeer second = RaftPeerAddressResolver.parsePeerList("[2001:db8::1]:2434", 2424).peers().getFirst();
    assertThat(second.getAddress()).as("no longer 'Invalid priority value 1]'").isEqualTo("[2001:db8::1]:2434");
  }

  @Test
  void aValidPositionalPortIsKeptAsWrittenSoTheDerivedPeerIdDoesNotChange() {
    final RaftPeer peer = RaftPeerAddressResolver.parsePeerList("host:02434:2480", 2424).peers().getFirst();

    assertThat(peer.getAddress()).isEqualTo("host:02434");
    assertThat(peer.getId().toString()).isEqualTo("host_02434");
  }

  @ParameterizedTest
  @ValueSource(strings = { "host:abc", "host:-1:2480", "host:0:2480", "host:2434:99999", "host:2434:notaport",
      "host:2434:2480:0:0", "host:2434:", "host:{raft:0}", "host:{http:70000}", "host:{bolt:x}", "fe80::1:2434" })
  void anIllegalPortIsRefusedNamingTheField(final String entry) {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList(entry, 2424))
        .isInstanceOf(ServerException.class)
        .hasMessageContaining(entry)
        .hasMessageContaining(" port");
  }

  @ParameterizedTest
  @ValueSource(strings = { "[::1", "[]:2434", "[::1]2434", "fe80::1:{raft:2434}", ":2434", "[::1]:2434:{http:2480}",
      "host:2434:{http:2480}" })
  void aMalformedHostIsRefused(final String entry) {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList(entry, 2424)).isInstanceOf(ServerException.class);
    assertThat(PeerAddressAllowlistFilter.extractPeerHosts(entry)).as("an entry that names no peer resolves nothing")
        .isEmpty();
  }

  @Test
  void anUnbracketedIpv6EntryIsRefusedWithTheBracketHint() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("fe80::1:2434", 2424))
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("written in brackets");
  }

  @Test
  void aDnsSuffixIsNotAppliedToABracketedHost() {
    final RaftPeer peer = RaftPeerAddressResolver.parsePeerList("[fd00::5]:2434:2480", 2424, ".svc.cluster.local").peers()
        .getFirst();

    assertThat(peer.getAddress()).isEqualTo("[fd00::5]:2434");
  }

  private static void assertLegal(final String entry, final String address) {
    if (address != null)
      assertThatCode(() -> RaftPeerAddressResolver.validatePeerAddress(address))
          .as("'%s' built from '%s'", address, entry).doesNotThrowAnyException();
  }
}
