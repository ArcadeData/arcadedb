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

import com.arcadedb.GlobalConfiguration;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RaftHAServer#selectChannelEscalationTarget} (issue #5346).
 * <p>
 * When the bounded replication-channel reset budget is exhausted for a wedged follower, the leader
 * escalates by transferring leadership to a healthy peer, which rebuilds the appender. The target
 * choice must never be the wedged follower itself (promoting it would leave the cluster leaderless
 * behind a dead channel) and never this node.
 */
class RaftHAServerChannelEscalationTest {

  private static RaftPeer peer(final String id, final int priority) {
    return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress(id + ":2434").setPriority(priority).build();
  }

  @Test
  void picksAHealthyPeerOtherThanTheWedgedFollower() {
    final List<RaftPeer> peers = List.of(peer("n0", 0), peer("n1", 0), peer("n2", 0));
    final RaftPeer target = RaftHAServer.selectChannelEscalationTarget(peers, RaftPeerId.valueOf("n0"), "n2", null);
    assertThat(target).isNotNull();
    assertThat(target.getId().toString()).isEqualTo("n1");
  }

  @Test
  void neverPicksTheWedgedFollowerEvenWhenItIsTheOnlyOtherPeer() {
    // Two-node cluster whose single follower is the wedged one: there is nowhere safe to go.
    final List<RaftPeer> peers = List.of(peer("n0", 0), peer("n2", 0));
    final RaftPeer target = RaftHAServer.selectChannelEscalationTarget(peers, RaftPeerId.valueOf("n0"), "n2", null);
    assertThat(target).isNull();
  }

  @Test
  void neverPicksThisNode() {
    final List<RaftPeer> peers = List.of(peer("n0", 0));
    final RaftPeer target = RaftHAServer.selectChannelEscalationTarget(peers, RaftPeerId.valueOf("n0"), "n2", null);
    assertThat(target).isNull();
  }

  @Test
  void prefersTheHighestPriorityHealthyPeer() {
    final List<RaftPeer> peers = List.of(peer("n0", 5), peer("n1", 1), peer("n2", 0), peer("n3", 7));
    final RaftPeer target = RaftHAServer.selectChannelEscalationTarget(peers, RaftPeerId.valueOf("n0"), "n2", null);
    assertThat(target).isNotNull();
    assertThat(target.getId().toString()).isEqualTo("n3");
  }

  /**
   * A lagging peer is skipped by {@link RaftHAServer#selectStepDownTargets}, so a cluster where every
   * remaining follower is behind yields no escalation target and the leader keeps the previous
   * operator-intervention behaviour instead of flapping leadership onto a peer that cannot serve.
   */
  @Test
  void skipsLaggingPeers() {
    final ClusterMonitor monitor = new ClusterMonitor(10L);
    monitor.updateLeaderCommitIndex(10_000L);
    monitor.updateReplicaMatchIndex("n1", 0L, 0L); // lag 10000 > threshold 10

    final List<RaftPeer> peers = List.of(peer("n0", 0), peer("n1", 0), peer("n2", 0));
    final RaftPeer target = RaftHAServer.selectChannelEscalationTarget(peers, RaftPeerId.valueOf("n0"), "n2", monitor);
    assertThat(target).isNull();
  }

  @Test
  void escalationIsEnabledByDefault() {
    assertThat(GlobalConfiguration.HA_PEER_CHANNEL_RESET_ESCALATION.getKey())
        .isEqualTo("arcadedb.ha.peerChannelResetEscalation");
    assertThat(GlobalConfiguration.HA_PEER_CHANNEL_RESET_ESCALATION.getDefValue()).isEqualTo(Boolean.TRUE);
  }
}
