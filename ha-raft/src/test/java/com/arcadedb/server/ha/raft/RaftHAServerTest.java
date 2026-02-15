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

import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RaftHAServerTest {

  @Test
  void parsePeerListSingleServer() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("localhost:2424", 2434);
    assertThat(peers).hasSize(1);
    assertThat(peers.get(0).getAddress()).isEqualTo("localhost:2434");
  }

  @Test
  void parsePeerListMultipleServers() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("host1:2424,host2:2424,host3:2424", 2434);
    assertThat(peers).hasSize(3);
    assertThat(peers.get(0).getAddress()).isEqualTo("host1:2434");
    assertThat(peers.get(1).getAddress()).isEqualTo("host2:2434");
    assertThat(peers.get(2).getAddress()).isEqualTo("host3:2434");
  }

  @Test
  void parsePeerListAssignsUniqueIds() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("a:2424,b:2424", 2434);
    assertThat(peers.get(0).getId()).isNotEqualTo(peers.get(1).getId());
  }

  @Test
  void parsePeerListWithCustomRaftPort() {
    final List<RaftPeer> peers = RaftHAServer.parsePeerList("myhost:2424", 9999);
    assertThat(peers.get(0).getAddress()).isEqualTo("myhost:9999");
  }
}
