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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RaftHAServerTest {

  @Test
  void parsePeerListSingleServer() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("localhost:2434", 2434).peers();
    assertThat(peers).hasSize(1);
    assertThat(peers.get(0).getAddress()).isEqualTo("localhost:2434");
  }

  @Test
  void parsePeerListMultipleServers() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("host1:2434,host2:2435,host3:2436", 2434).peers();
    assertThat(peers).hasSize(3);
    assertThat(peers.get(0).getAddress()).isEqualTo("host1:2434");
    assertThat(peers.get(1).getAddress()).isEqualTo("host2:2435");
    assertThat(peers.get(2).getAddress()).isEqualTo("host3:2436");
  }

  @Test
  void parsePeerListAssignsUniqueIds() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("a:2434,b:2435", 2434).peers();
    assertThat(peers.get(0).getId()).isNotEqualTo(peers.get(1).getId());
  }

  @Test
  void parsePeerListPreservesExactPort() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("myhost:9999", 2434).peers();
    assertThat(peers.get(0).getAddress()).isEqualTo("myhost:9999");
  }

  @Test
  void parsePeerListHostnameOnlyUsesDefaultPort() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("node1,node2,node3", 2434).peers();
    assertThat(peers).hasSize(3);
    assertThat(peers.get(0).getAddress()).isEqualTo("node1:2434");
    assertThat(peers.get(1).getAddress()).isEqualTo("node2:2434");
    assertThat(peers.get(2).getAddress()).isEqualTo("node3:2434");
  }

  @Test
  void parsePeerListMixedEntriesAppliesDefaultPortOnlyWhereNeeded() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("node1,node2:9000,node3", 2434).peers();
    assertThat(peers).hasSize(3);
    assertThat(peers.get(0).getAddress()).isEqualTo("node1:2434");
    assertThat(peers.get(1).getAddress()).isEqualTo("node2:9000");
    assertThat(peers.get(2).getAddress()).isEqualTo("node3:2434");
  }

  @Test
  void parsePeerListCustomDefaultPort() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("myhost", 9999).peers();
    assertThat(peers.get(0).getAddress()).isEqualTo("myhost:9999");
  }

  @Test
  void parsePeerListThreePartExtractsRaftAndHttpAddresses() {
    final RaftHAServer.ParsedPeerList parsed = RaftHAServer.parsePeerList("node1:2434:2480", 2434);
    assertThat(parsed.peers()).hasSize(1);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("node1:2434");
    assertThat(parsed.httpAddresses()).containsEntry(parsed.peers().get(0).getId(), "node1:2480");
  }

  @Test
  void parsePeerListThreePartMultipleNodes() {
    final RaftHAServer.ParsedPeerList parsed = RaftHAServer.parsePeerList("host1:2434:2480,host2:2435:2481,host3:2436:2482", 2434);
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
    final RaftHAServer.ParsedPeerList parsed = RaftHAServer.parsePeerList("myhost:2434", 2434);
    assertThat(parsed.httpAddresses()).isEmpty();
  }

  @Test
  void parsePeerListOnePartHasNoHttpAddress() {
    final RaftHAServer.ParsedPeerList parsed = RaftHAServer.parsePeerList("myhost", 2434);
    assertThat(parsed.httpAddresses()).isEmpty();
  }

  @Test
  void parsePeerListMixedThreePartAndTwoPart() {
    final RaftHAServer.ParsedPeerList parsed = RaftHAServer.parsePeerList("node1:2434:2480,node2:2435", 2434);
    final List<RaftPeer> peers = parsed.peers();
    assertThat(peers).hasSize(2);
    assertThat(peers.get(0).getAddress()).isEqualTo("node1:2434");
    assertThat(peers.get(1).getAddress()).isEqualTo("node2:2435");
    assertThat(parsed.httpAddresses()).containsEntry(peers.get(0).getId(), "node1:2480");
    assertThat(parsed.httpAddresses()).doesNotContainKey(peers.get(1).getId());
  }

  @Test
  void parsePeerListFourPartSetsPriority() {
    final RaftHAServer.ParsedPeerList parsed = RaftHAServer.parsePeerList("node1:2434:2480:10,node2:2435:2481:0", 2434);
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
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("node1:2434:2480", 2434).peers();
    assertThat(peers.get(0).getPriority()).isEqualTo(0);
  }

  @Test
  void parsePeerListTwoPartDefaultsPriorityToZero() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("node1:2434", 2434).peers();
    assertThat(peers.get(0).getPriority()).isEqualTo(0);
  }

  @Test
  void initClusterTokenGeneratesAndPersistsTokenWhenBlank(@TempDir final File tempDir) throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    // HA_CLUSTER_TOKEN starts blank by default

    RaftHAServer.initClusterToken(config, tempDir);

    final String token = config.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
    assertThat(token).isNotBlank();

    // token must also be in the file
    final File tokenFile = new File(tempDir, "cluster-token.txt");
    assertThat(tokenFile).exists();
    assertThat(Files.readString(tokenFile.toPath()).trim()).isEqualTo(token);
  }

  @Test
  void initClusterTokenReadsExistingFileWhenConfigBlank(@TempDir final File tempDir) throws Exception {
    final String existingToken = "my-existing-token";
    Files.writeString(new File(tempDir, "cluster-token.txt").toPath(), existingToken);

    final ContextConfiguration config = new ContextConfiguration();
    RaftHAServer.initClusterToken(config, tempDir);

    assertThat(config.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN)).isEqualTo(existingToken);
  }

  @Test
  void initClusterTokenKeepsExplicitConfigValueWithoutTouchingFile(@TempDir final File tempDir) throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "explicit-token");

    RaftHAServer.initClusterToken(config, tempDir);

    assertThat(config.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN)).isEqualTo("explicit-token");
    assertThat(new File(tempDir, "cluster-token.txt")).doesNotExist();
  }

  @Test
  void peerDisplayNamesWithHttpAddresses() {
    final RaftHAServer.ParsedPeerList parsed = RaftHAServer.parsePeerList(
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
    final RaftHAServer.ParsedPeerList parsed = RaftHAServer.parsePeerList("localhost:2434,localhost:2435", 2434);
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
}
