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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Edge cases for {@link RaftPeerAddressResolver#parsePeerList} not covered by {@link RaftHAServerTest}.
 * Ported from apache-ratis branch.
 */
class RaftHAServerAddressParsingTest {

  @Test
  void emptyStringThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void blankEntryThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("  ", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void tooManyColonsThrows() {
    // 6 colon-separated parts exceed the supported host:raftPort:httpPort:priority:httpsPort format
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("host:1:2:3:4:5", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void fivePartsParsesHttpsPort() {
    // host:raftPort:httpPort:priority:httpsPort
    final var parsed = RaftPeerAddressResolver.parsePeerList("myhost:2434:2480:7:2490", 2434);
    final var peerId = parsed.peers().get(0).getId();
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("myhost:2434");
    assertThat(parsed.peers().get(0).getPriority()).isEqualTo(7);
    assertThat(parsed.httpAddresses()).containsEntry(peerId, "myhost:2480");
    assertThat(parsed.httpsAddresses()).containsEntry(peerId, "myhost:2490");
  }

  @Test
  void namedFivePartsParsesHttpsPort() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("alpha@myhost:2434:2480:0:2490", 2434);
    final var peerId = parsed.peers().get(0).getId();
    assertThat(parsed.httpsAddresses()).containsEntry(peerId, "myhost:2490");
    assertThat(parsed.peerNames()).containsEntry(peerId, "alpha");
  }

  @Test
  void fourPartsLeavesHttpsAddressesEmpty() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("myhost:2434:2480:7", 2434);
    assertThat(parsed.httpsAddresses()).isEmpty();
  }

  @Test
  void objectFormFullParsesAllFields() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("myhost:{raft:2434,http:2480,https:2490,priority:10}", 2434);
    final var peer = parsed.peers().get(0);
    assertThat(peer.getAddress()).isEqualTo("myhost:2434");
    assertThat(peer.getPriority()).isEqualTo(10);
    assertThat(parsed.httpAddresses()).containsEntry(peer.getId(), "myhost:2480");
    assertThat(parsed.httpsAddresses()).containsEntry(peer.getId(), "myhost:2490");
  }

  @Test
  void objectFormFieldsAreUnorderedAndOptional() {
    // https omitted, fields out of order, whitespace tolerated
    final var parsed = RaftPeerAddressResolver.parsePeerList("myhost:{ priority: 5 , http: 2480 , raft: 2434 }", 2434);
    final var peer = parsed.peers().get(0);
    assertThat(peer.getAddress()).isEqualTo("myhost:2434");
    assertThat(peer.getPriority()).isEqualTo(5);
    assertThat(parsed.httpAddresses()).containsEntry(peer.getId(), "myhost:2480");
    assertThat(parsed.httpsAddresses()).isEmpty();
  }

  @Test
  void objectFormRaftDefaultsToDefaultPort() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("myhost:{http:2480}", 9999);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("myhost:9999");
    assertThat(parsed.httpAddresses()).containsEntry(parsed.peers().get(0).getId(), "myhost:2480");
  }

  @Test
  void objectFormWithNamePrefix() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("frankfurt@db1:{raft:2434,http:2480,https:2490}", 2434);
    final var peer = parsed.peers().get(0);
    assertThat(peer.getAddress()).isEqualTo("db1:2434");
    assertThat(parsed.peerNames()).containsEntry(peer.getId(), "frankfurt");
    assertThat(parsed.httpsAddresses()).containsEntry(peer.getId(), "db1:2490");
  }

  @Test
  void mixedObjectAndPositionalForms() {
    final var parsed = RaftPeerAddressResolver.parsePeerList(
        "host1:{raft:2434,http:2480,https:2490},host2:2434:2480", 2434);
    assertThat(parsed.peers()).hasSize(2);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("host1:2434");
    assertThat(parsed.peers().get(1).getAddress()).isEqualTo("host2:2434");
    assertThat(parsed.httpsAddresses()).containsEntry(parsed.peers().get(0).getId(), "host1:2490");
    assertThat(parsed.httpsAddresses()).doesNotContainKey(parsed.peers().get(1).getId());
  }

  @Test
  void objectFormUnknownKeyThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("host:{raft:2434,foo:1}", 2434))
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("Unknown key 'foo'");
  }

  @Test
  void objectFormDuplicateKeyThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("host:{raft:2434,raft:2435}", 2434))
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("Duplicate key 'raft'");
  }

  @Test
  void objectFormNonNumericPortThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("host:{raft:abc}", 2434))
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("Invalid raft value");
  }

  @Test
  void objectFormUnbalancedBraceThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("host:{raft:2434", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void objectFormEmptyBracesUsesDefaults() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("myhost:{}", 2434);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("myhost:2434");
    assertThat(parsed.httpAddresses()).isEmpty();
    assertThat(parsed.httpsAddresses()).isEmpty();
  }

  @Test
  void blankHostnameThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList(":2434", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void nonNumericPriorityThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("host:2434:2480:abc", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void singleNodeCluster() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("myhost:2434:2480", 2434);
    assertThat(parsed.peers()).hasSize(1);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("myhost:2434");
    assertThat(parsed.httpAddresses()).hasSize(1);
  }

  @Test
  void trailingCommaIgnored() {
    // parsePeerList splits on comma; trailing comma creates empty entry that should be handled
    // This test documents the current behavior - adjust if it should throw instead
    final var parsed = RaftPeerAddressResolver.parsePeerList("host1:2434,host2:2435", 2434);
    assertThat(parsed.peers()).hasSize(2);
  }

  @Test
  void leadingWhitespaceInEntryTrimmed() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("  host1:2434 , host2:2435 ", 2434);
    assertThat(parsed.peers()).hasSize(2);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("host1:2434");
    assertThat(parsed.peers().get(1).getAddress()).isEqualTo("host2:2435");
  }

  @Test
  void noNamesYieldsEmptyPeerNamesMap() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("host1:2434,host2:2435", 2434);
    assertThat(parsed.peerNames()).isEmpty();
  }

  @Test
  void namedSinglePart() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("alpha@myhost", 2434);
    assertThat(parsed.peers()).hasSize(1);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("myhost:2434");
    assertThat(parsed.peerNames()).containsEntry(parsed.peers().get(0).getId(), "alpha");
  }

  @Test
  void namedTwoParts() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("alpha@myhost:2434", 2434);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("myhost:2434");
    assertThat(parsed.peerNames()).containsEntry(parsed.peers().get(0).getId(), "alpha");
  }

  @Test
  void namedThreeParts() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("alpha@myhost:2434:2480", 2434);
    final var peerId = parsed.peers().get(0).getId();
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("myhost:2434");
    assertThat(parsed.httpAddresses()).containsEntry(peerId, "myhost:2480");
    assertThat(parsed.peerNames()).containsEntry(peerId, "alpha");
  }

  @Test
  void namedFourParts() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("alpha@myhost:2434:2480:5", 2434);
    final var peerId = parsed.peers().get(0).getId();
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("myhost:2434");
    assertThat(parsed.peers().get(0).getPriority()).isEqualTo(5);
    assertThat(parsed.httpAddresses()).containsEntry(peerId, "myhost:2480");
    assertThat(parsed.peerNames()).containsEntry(peerId, "alpha");
  }

  @Test
  void mixedNamedAndUnnamedPeers() {
    final var parsed = RaftPeerAddressResolver.parsePeerList(
        "alpha@host1:2434,host2:2435,gamma@host3:2436", 2434);
    final var peers = parsed.peers();
    assertThat(peers).hasSize(3);
    assertThat(parsed.peerNames()).containsEntry(peers.get(0).getId(), "alpha");
    assertThat(parsed.peerNames()).doesNotContainKey(peers.get(1).getId());
    assertThat(parsed.peerNames()).containsEntry(peers.get(2).getId(), "gamma");
  }

  @Test
  void duplicatePeerNamesThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList(
        "alpha@host1:2434,alpha@host2:2435", 2434))
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("alpha");
  }

  @Test
  void blankPeerNameThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("@host:2434", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void whitespaceAroundPeerNameTrimmed() {
    final var parsed = RaftPeerAddressResolver.parsePeerList(" alpha @host:2434", 2434);
    assertThat(parsed.peerNames()).containsEntry(parsed.peers().get(0).getId(), "alpha");
  }

  @Test
  void multipleAtSignsThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("alpha@beta@host:2434", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void namedPeerIdStillUsesAddressFormat() {
    // Peer ID is derived from the address, NOT the name - this preserves Ratis identity stability
    final var parsed = RaftPeerAddressResolver.parsePeerList("alpha@myhost:9999", 2434);
    assertThat(parsed.peers().get(0).getId().toString()).isEqualTo("myhost_9999");
  }

  // --- Issue #4838: HA_K8S_DNS_SUFFIX must be applied to peer addresses, not only to the self host ---

  @Test
  void k8sSuffixAppendedToShortPeerHosts() {
    // Short pod names with a K8s DNS suffix must be expanded to the cluster-internal FQDN so peers resolve.
    final var parsed = RaftPeerAddressResolver.parsePeerList(
        "arcadedb-0:2434:2480,arcadedb-1:2434:2480", 2434, ".arcadedb.default.svc.cluster.local");
    final var peer0 = parsed.peers().get(0);
    final var peer1 = parsed.peers().get(1);
    assertThat(peer0.getAddress()).isEqualTo("arcadedb-0.arcadedb.default.svc.cluster.local:2434");
    assertThat(peer1.getAddress()).isEqualTo("arcadedb-1.arcadedb.default.svc.cluster.local:2434");
    assertThat(parsed.httpAddresses()).containsEntry(peer0.getId(), "arcadedb-0.arcadedb.default.svc.cluster.local:2480");
    assertThat(parsed.httpAddresses()).containsEntry(peer1.getId(), "arcadedb-1.arcadedb.default.svc.cluster.local:2480");
  }

  @Test
  void k8sSuffixReflectedInPeerId() {
    // The peer ID is derived from the (now-qualified) Raft address.
    final var parsed = RaftPeerAddressResolver.parsePeerList(
        "arcadedb-0:2434:2480", 2434, ".arcadedb.default.svc.cluster.local");
    assertThat(parsed.peers().get(0).getId().toString())
        .isEqualTo("arcadedb-0.arcadedb.default.svc.cluster.local_2434");
  }

  @Test
  void k8sSuffixAppendedInObjectFormAndToHttps() {
    final var parsed = RaftPeerAddressResolver.parsePeerList(
        "arcadedb-0:{raft:2434,http:2480,https:2490}", 2434, ".svc.local");
    final var peer = parsed.peers().get(0);
    assertThat(peer.getAddress()).isEqualTo("arcadedb-0.svc.local:2434");
    assertThat(parsed.httpAddresses()).containsEntry(peer.getId(), "arcadedb-0.svc.local:2480");
    assertThat(parsed.httpsAddresses()).containsEntry(peer.getId(), "arcadedb-0.svc.local:2490");
  }

  @Test
  void k8sSuffixIsIdempotentWhenHostAlreadyQualified() {
    // An operator who already wrote FQDNs must not get the suffix appended twice.
    final var parsed = RaftPeerAddressResolver.parsePeerList(
        "arcadedb-0.svc.local:2434:2480", 2434, ".svc.local");
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("arcadedb-0.svc.local:2434");
  }

  @Test
  void k8sSuffixSkippedForIpLiteral() {
    final var parsed = RaftPeerAddressResolver.parsePeerList(
        "10.0.0.5:2434:2480", 2434, ".svc.local");
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("10.0.0.5:2434");
  }

  @Test
  void k8sSuffixSkippedForLocalhost() {
    final var parsed = RaftPeerAddressResolver.parsePeerList(
        "localhost:2434:2480", 2434, ".svc.local");
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("localhost:2434");
  }

  @Test
  void emptyK8sSuffixLeavesHostsUnchanged() {
    final var parsed = RaftPeerAddressResolver.parsePeerList(
        "arcadedb-0:2434:2480", 2434, "");
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("arcadedb-0:2434");
  }

  @Test
  void legacyTwoArgParsePeerListLeavesHostsUnchanged() {
    // The 2-arg overload must behave exactly as before (no suffix).
    final var parsed = RaftPeerAddressResolver.parsePeerList("arcadedb-0:2434:2480", 2434);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("arcadedb-0:2434");
  }

  @Test
  void objectFormParsesBoltPort() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("myhost:{raft:2434,http:2480,bolt:7687}", 2434);
    final var peerId = parsed.peers().get(0).getId();
    assertThat(parsed.boltAddresses()).containsEntry(peerId, "myhost:7687");
    assertThat(parsed.httpAddresses()).containsEntry(peerId, "myhost:2480");
  }

  @Test
  void objectFormWithoutBoltLeavesBoltAddressesEmpty() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("myhost:{raft:2434,http:2480}", 2434);
    assertThat(parsed.boltAddresses()).isEmpty();
  }

  @Test
  void positionalFormNeverPopulatesBoltAddresses() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("myhost:2434:2480:7:2490", 2434);
    assertThat(parsed.boltAddresses()).isEmpty();
  }

  @Test
  void namedObjectFormParsesBoltPort() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("alpha@myhost:{raft:2434,bolt:7690}", 2434);
    final var peerId = parsed.peers().get(0).getId();
    assertThat(parsed.boltAddresses()).containsEntry(peerId, "myhost:7690");
    assertThat(parsed.peerNames()).containsEntry(peerId, "alpha");
  }

  @Test
  void unknownObjectKeyStillThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("myhost:{raft:2434,ftp:21}", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void deriveBoltAddressCombinesRaftHostWithBoltPort() {
    assertThat(RaftHAServer.deriveBoltAddress("db1:2434", 7687)).isEqualTo("db1:7687");
  }

  @Test
  void deriveBoltAddressReturnsNullForNonPositivePort() {
    assertThat(RaftHAServer.deriveBoltAddress("db1:2434", 0)).isNull();
    assertThat(RaftHAServer.deriveBoltAddress("db1:2434", -1)).isNull();
  }

  @Test
  void deriveBoltAddressHandlesIpv6Literal() {
    assertThat(RaftHAServer.deriveBoltAddress("[::1]:2434", 7687)).isEqualTo("[::1]:7687");
  }
}
