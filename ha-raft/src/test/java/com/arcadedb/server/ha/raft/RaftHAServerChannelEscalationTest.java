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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  /**
   * The cooldown is what bounds cross-node leadership flapping when a follower is unreachable for a reason
   * a fresh appender cannot fix. ClusterMonitor's per-streak latch cannot do it, because
   * {@code clusterMonitor.reset()} wipes channel state on every leadership acquisition, so each new leader
   * would arrive with a fresh budget and pass the problem on forever.
   */
  @Test
  void escalationForTheSamePeerIsAdmittedOnlyOncePerCooldown() {
    final Map<String, Long> escalations = new HashMap<>();
    final long t0 = 1_000_000L;

    assertThat(RaftHAServer.admitChannelEscalation(escalations, "n2", t0)).isTrue();
    // Same peer, well inside the window: refused, so leadership is not bounced again.
    assertThat(RaftHAServer.admitChannelEscalation(escalations, "n2", t0 + 60_000L)).isFalse();
    assertThat(RaftHAServer.admitChannelEscalation(escalations, "n2", t0 + 29 * 60_000L)).isFalse();
  }

  @Test
  void escalationIsAdmittedAgainAfterTheCooldownElapses() {
    final Map<String, Long> escalations = new HashMap<>();
    final long t0 = 1_000_000L;

    assertThat(RaftHAServer.admitChannelEscalation(escalations, "n2", t0)).isTrue();
    assertThat(RaftHAServer.admitChannelEscalation(escalations, "n2", t0 + 31 * 60_000L)).isTrue();
    // ...and the window restarts from the admitted escalation.
    assertThat(RaftHAServer.admitChannelEscalation(escalations, "n2", t0 + 32 * 60_000L)).isFalse();
  }

  /**
   * The cooldown must be consumed only when a transfer is actually attempted. `escalateWedgedPeerChannel`
   * therefore selects the target BEFORE admitting: a no-eligible-target give-up must leave the window
   * intact, or a genuinely recoverable escalation would be suppressed for 30 minutes once a healthy peer
   * rejoins. This pins the ordering contract that makes that possible - a refused admit is the only thing
   * that burns the window.
   */
  @Test
  void aRefusedTargetSelectionLeavesTheCooldownWindowIntact() {
    final Map<String, Long> escalations = new HashMap<>();
    final long t0 = 1_000_000L;

    // No eligible target: escalateWedgedPeerChannel returns before admitting, so nothing is recorded.
    final List<RaftPeer> onlyWedgedPeerLeft = List.of(peer("n0", 0), peer("n2", 0));
    assertThat(RaftHAServer.selectChannelEscalationTarget(onlyWedgedPeerLeft, RaftPeerId.valueOf("n0"), "n2", null))
        .isNull();
    assertThat(escalations).isEmpty();

    // A healthy peer rejoins moments later: the escalation is still admitted, not suppressed.
    assertThat(RaftHAServer.admitChannelEscalation(escalations, "n2", t0 + 1_000L)).isTrue();
  }

  @Test
  void cooldownIsTrackedPerFollower() {
    final Map<String, Long> escalations = new HashMap<>();
    final long t0 = 1_000_000L;

    assertThat(RaftHAServer.admitChannelEscalation(escalations, "n2", t0)).isTrue();
    // A different follower wedging at the same time is a distinct incident and must not be suppressed.
    assertThat(RaftHAServer.admitChannelEscalation(escalations, "n3", t0)).isTrue();
  }

  @Test
  void escalationIsEnabledByDefault() {
    assertThat(GlobalConfiguration.HA_PEER_CHANNEL_RESET_ESCALATION.getKey())
        .isEqualTo("arcadedb.ha.peerChannelResetEscalation");
    assertThat(GlobalConfiguration.HA_PEER_CHANNEL_RESET_ESCALATION.getDefValue()).isEqualTo(Boolean.TRUE);
  }
}
