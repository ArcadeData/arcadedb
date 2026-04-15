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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RaftHAServerTest {

  @Test
  void parsePeerListSingleServer() {
    final List<RaftPeer> peers = RaftPeerAddressResolver.parsePeerList("localhost:2434", 2434).peers();
    assertThat(peers).hasSize(1);
    assertThat(peers.get(0).getAddress()).isEqualTo("localhost:2434");
  }

  @Test
  void parsePeerListMultipleServers() {
    final List<RaftPeer> peers = RaftPeerAddressResolver.parsePeerList("host1:2434,host2:2435,host3:2436", 2434).peers();
    assertThat(peers).hasSize(3);
    assertThat(peers.get(0).getAddress()).isEqualTo("host1:2434");
    assertThat(peers.get(1).getAddress()).isEqualTo("host2:2435");
    assertThat(peers.get(2).getAddress()).isEqualTo("host3:2436");
  }

  @Test
  void parsePeerListAssignsUniqueIds() {
    final List<RaftPeer> peers = RaftPeerAddressResolver.parsePeerList("a:2434,b:2435", 2434).peers();
    assertThat(peers.get(0).getId()).isNotEqualTo(peers.get(1).getId());
  }

  @Test
  void parsePeerListUsesHostPortAsId() {
    final List<RaftPeer> peers = RaftPeerAddressResolver.parsePeerList("myhost:9999,other:8888", 2434).peers();
    assertThat(peers.get(0).getId().toString()).isEqualTo("myhost_9999");
    assertThat(peers.get(1).getId().toString()).isEqualTo("other_8888");
  }

  @Test
  void parsePeerListPreservesExactPort() {
    final List<RaftPeer> peers = RaftPeerAddressResolver.parsePeerList("myhost:9999", 2434).peers();
    assertThat(peers.get(0).getAddress()).isEqualTo("myhost:9999");
  }

  @Test
  void parsePeerListHostnameOnlyUsesDefaultPort() {
    final List<RaftPeer> peers = RaftPeerAddressResolver.parsePeerList("node1,node2,node3", 2434).peers();
    assertThat(peers).hasSize(3);
    assertThat(peers.get(0).getAddress()).isEqualTo("node1:2434");
    assertThat(peers.get(1).getAddress()).isEqualTo("node2:2434");
    assertThat(peers.get(2).getAddress()).isEqualTo("node3:2434");
  }

  @Test
  void parsePeerListMixedEntriesAppliesDefaultPortOnlyWhereNeeded() {
    final List<RaftPeer> peers = RaftPeerAddressResolver.parsePeerList("node1,node2:9000,node3", 2434).peers();
    assertThat(peers).hasSize(3);
    assertThat(peers.get(0).getAddress()).isEqualTo("node1:2434");
    assertThat(peers.get(1).getAddress()).isEqualTo("node2:9000");
    assertThat(peers.get(2).getAddress()).isEqualTo("node3:2434");
  }

  @Test
  void parsePeerListCustomDefaultPort() {
    final List<RaftPeer> peers = RaftPeerAddressResolver.parsePeerList("myhost", 9999).peers();
    assertThat(peers.get(0).getAddress()).isEqualTo("myhost:9999");
  }

  @Test
  void parsePeerListThreePartExtractsRaftAndHttpAddresses() {
    final RaftPeerAddressResolver.ParsedPeerList parsed = RaftPeerAddressResolver.parsePeerList("node1:2434:2480", 2434);
    assertThat(parsed.peers()).hasSize(1);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("node1:2434");
    assertThat(parsed.httpAddresses()).containsEntry(parsed.peers().get(0).getId(), "node1:2480");
  }

  @Test
  void parsePeerListThreePartMultipleNodes() {
    final RaftPeerAddressResolver.ParsedPeerList parsed = RaftPeerAddressResolver.parsePeerList(
        "host1:2434:2480,host2:2435:2481,host3:2436:2482", 2434);
    final List<RaftPeer> peers = parsed.peers();
    assertThat(peers).hasSize(3);
    assertThat(peers.get(0).getAddress()).isEqualTo("host1:2434");
    assertThat(peers.get(1).getAddress()).isEqualTo("host2:2435");
    assertThat(peers.get(2).getAddress()).isEqualTo("host3:2436");
    assertThat(parsed.httpAddresses()).containsEntry(peers.get(0).getId(), "host1:2480");
    assertThat(parsed.httpAddresses()).containsEntry(peers.get(1).getId(), "host2:2481");
    assertThat(parsed.httpAddresses()).containsEntry(peers.get(2).getId(), "host3:2482");
  }

  @Test
  void parsePeerListTwoPartHasNoHttpAddress() {
    final RaftPeerAddressResolver.ParsedPeerList parsed = RaftPeerAddressResolver.parsePeerList("myhost:2434", 2434);
    assertThat(parsed.httpAddresses()).isEmpty();
  }

  @Test
  void parsePeerListOnePartHasNoHttpAddress() {
    final RaftPeerAddressResolver.ParsedPeerList parsed = RaftPeerAddressResolver.parsePeerList("myhost", 2434);
    assertThat(parsed.httpAddresses()).isEmpty();
  }

  @Test
  void parsePeerListMixedThreePartAndTwoPart() {
    final RaftPeerAddressResolver.ParsedPeerList parsed = RaftPeerAddressResolver.parsePeerList("node1:2434:2480,node2:2435", 2434);
    final List<RaftPeer> peers = parsed.peers();
    assertThat(peers).hasSize(2);
    assertThat(peers.get(0).getAddress()).isEqualTo("node1:2434");
    assertThat(peers.get(1).getAddress()).isEqualTo("node2:2435");
    assertThat(parsed.httpAddresses()).containsEntry(peers.get(0).getId(), "node1:2480");
    assertThat(parsed.httpAddresses()).doesNotContainKey(peers.get(1).getId());
  }

  @Test
  void parsePeerListFourPartSetsPriority() {
    final RaftPeerAddressResolver.ParsedPeerList parsed = RaftPeerAddressResolver.parsePeerList(
        "node1:2434:2480:10,node2:2435:2481:0", 2434);
    final List<RaftPeer> peers = parsed.peers();
    assertThat(peers).hasSize(2);
    assertThat(peers.get(0).getAddress()).isEqualTo("node1:2434");
    assertThat(peers.get(1).getAddress()).isEqualTo("node2:2435");
    assertThat(parsed.httpAddresses()).containsEntry(peers.get(0).getId(), "node1:2480");
    assertThat(parsed.httpAddresses()).containsEntry(peers.get(1).getId(), "node2:2481");
    assertThat(peers.get(0).getPriority()).isEqualTo(10);
    assertThat(peers.get(1).getPriority()).isEqualTo(0);
  }

  @Test
  void parsePeerListThreePartDefaultsPriorityToZero() {
    final List<RaftPeer> peers = RaftPeerAddressResolver.parsePeerList("node1:2434:2480", 2434).peers();
    assertThat(peers.get(0).getPriority()).isEqualTo(0);
  }

  @Test
  void parsePeerListTwoPartDefaultsPriorityToZero() {
    final List<RaftPeer> peers = RaftPeerAddressResolver.parsePeerList("node1:2434", 2434).peers();
    assertThat(peers.get(0).getPriority()).isEqualTo(0);
  }

  @Test
  void initClusterTokenDerivesTokenFromClusterNameAndPassword() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    // HA_CLUSTER_TOKEN starts blank by default

    ClusterTokenProvider.initClusterTokenForTest(config);

    final String token = config.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
    assertThat(token).isNotBlank();
  }

  @Test
  void initClusterTokenIsDeterministicForSameClusterNameAndPassword() {
    final ContextConfiguration config1 = new ContextConfiguration();
    final ContextConfiguration config2 = new ContextConfiguration();
    config1.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");
    config2.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "test-password");

    ClusterTokenProvider.initClusterTokenForTest(config1);
    ClusterTokenProvider.initClusterTokenForTest(config2);

    // Both nodes with the same cluster name and root password must derive the same token
    assertThat(config1.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN))
        .isEqualTo(config2.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN));
  }

  @Test
  void initClusterTokenKeepsExplicitConfigValue() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "explicit-token");

    ClusterTokenProvider.initClusterTokenForTest(config);

    assertThat(config.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN)).isEqualTo("explicit-token");
  }

  @Test
  void peerDisplayNamesWithHttpAddresses() {
    final RaftPeerAddressResolver.ParsedPeerList parsed = RaftPeerAddressResolver.parsePeerList(
        "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482", 2434);
    final List<RaftPeer> peers = parsed.peers();

    // Simulate the display name construction logic from the constructor
    final String prefix = "ArcadeDB";
    final Map<RaftPeerId, String> displayNames = new HashMap<>(peers.size());
    for (int i = 0; i < peers.size(); i++) {
      final RaftPeerId peerId = peers.get(i).getId();
      final String nodeName = prefix + "_" + i;
      final String httpAddr = parsed.httpAddresses().get(peerId);
      displayNames.put(peerId, httpAddr != null ? nodeName + " (" + httpAddr + ")" : nodeName);
    }

    assertThat(displayNames.get(peers.get(0).getId())).isEqualTo("ArcadeDB_0 (localhost:2480)");
    assertThat(displayNames.get(peers.get(1).getId())).isEqualTo("ArcadeDB_1 (localhost:2481)");
    assertThat(displayNames.get(peers.get(2).getId())).isEqualTo("ArcadeDB_2 (localhost:2482)");
  }

  @Test
  void peerDisplayNamesWithoutHttpAddresses() {
    final RaftPeerAddressResolver.ParsedPeerList parsed = RaftPeerAddressResolver.parsePeerList("localhost:2434,localhost:2435",
        2434);
    final List<RaftPeer> peers = parsed.peers();

    final String prefix = "MyDB";
    final Map<RaftPeerId, String> displayNames = new HashMap<>(peers.size());
    for (int i = 0; i < peers.size(); i++) {
      final RaftPeerId peerId = peers.get(i).getId();
      final String nodeName = prefix + "_" + i;
      final String httpAddr = parsed.httpAddresses().get(peerId);
      displayNames.put(peerId, httpAddr != null ? nodeName + " (" + httpAddr + ")" : nodeName);
    }

    assertThat(displayNames.get(peers.get(0).getId())).isEqualTo("MyDB_0");
    assertThat(displayNames.get(peers.get(1).getId())).isEqualTo("MyDB_1");
  }

  @Test
  void findLastSeparatorIndexWithUnderscore() {
    assertThat(RaftPeerAddressResolver.findLastSeparatorIndex("ArcadeDB_0")).isEqualTo(8);
    assertThat(RaftPeerAddressResolver.findLastSeparatorIndex("ArcadeDB_12")).isEqualTo(8);
  }

  @Test
  void findLastSeparatorIndexWithHyphen() {
    assertThat(RaftPeerAddressResolver.findLastSeparatorIndex("arcadedb-0")).isEqualTo(8);
    assertThat(RaftPeerAddressResolver.findLastSeparatorIndex("arcadedb-12")).isEqualTo(8);
  }

  @Test
  void findLastSeparatorIndexPrefersLastSeparator() {
    // When both separators are present, the last one wins
    assertThat(RaftPeerAddressResolver.findLastSeparatorIndex("my-db_0")).isEqualTo(5);
    assertThat(RaftPeerAddressResolver.findLastSeparatorIndex("my_db-0")).isEqualTo(5);
  }

  @Test
  void findLastSeparatorIndexThrowsWithoutSeparator() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.findLastSeparatorIndex("arcadedb0"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void findLastSeparatorIndexThrowsWhenSeparatorIsLast() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.findLastSeparatorIndex("arcadedb-"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
