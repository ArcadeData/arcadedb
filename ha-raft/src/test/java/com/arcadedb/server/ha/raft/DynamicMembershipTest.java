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

import com.arcadedb.server.BaseGraphServerTest;

import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

class DynamicMembershipTest extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void getLivePeersReturnsAllConfiguredPeers() {
    final int leaderIndex = getLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raftServer = (RaftHAServer) getServer(leaderIndex).getHA();
    final Collection<RaftPeer> livePeers = raftServer.getLivePeers();
    assertThat(livePeers).hasSize(3);
  }

  @Test
  void removePeerDecreasesClusterSize() {
    final int leaderIndex = getLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    // Pick a non-leader peer to remove, since Ratis requires the leader to process the change
    final int targetIndex = leaderIndex == 0 ? 2 : 0;
    final String targetPeerId = ((RaftHAServer) getServer(targetIndex).getHA()).getLocalPeerId().toString();

    final RaftHAServer raftServer = (RaftHAServer) getServer(leaderIndex).getHA();
    assertThat(raftServer.getLivePeers()).hasSize(3);

    raftServer.removePeer(targetPeerId);
    assertThat(raftServer.getLivePeers()).hasSize(2);
  }

  @Test
  void removePeerThrowsForUnknownPeer() {
    final int leaderIndex = getLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final RaftHAServer raftServer = (RaftHAServer) getServer(leaderIndex).getHA();
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> raftServer.removePeer("nonexistent"))
        .isInstanceOf(com.arcadedb.exception.ConfigurationException.class);
  }
}
