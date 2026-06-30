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
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RaftHAServer#selectStepDownTargets} (issue #4808). The old {@code stepDown}
 * picked the first non-self peer in iteration order, ignoring Raft priority, follower lag, and whether
 * the peer was a priority-0 witness/replica that Ratis is told never to elect. These tests pin the new
 * selection so a {@code POST /cluster/stepdown} never hands leadership to an unsuitable node.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftHAServerStepDownTargetTest {

  private static RaftPeer peer(final String id, final int priority) {
    return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress(id + ":2434").setPriority(priority).build();
  }

  private static List<String> ids(final List<RaftPeer> peers) {
    return peers.stream().map(p -> p.getId().toString()).toList();
  }

  @Test
  void excludesSelf() {
    final List<RaftPeer> peers = List.of(peer("n0", 0), peer("n1", 0), peer("n2", 0));
    final List<RaftPeer> targets = RaftHAServer.selectStepDownTargets(peers, RaftPeerId.valueOf("n0"), null);
    assertThat(ids(targets)).containsExactly("n1", "n2");
  }

  @Test
  void homogeneousDefaultPrioritiesKeepEveryFollowerEligible() {
    // The common deployment: no explicit priorities, so every peer is priority-0 and equally electable.
    final List<RaftPeer> peers = List.of(peer("n0", 0), peer("n1", 0), peer("n2", 0));
    final List<RaftPeer> targets = RaftHAServer.selectStepDownTargets(peers, RaftPeerId.valueOf("n0"), null);
    assertThat(ids(targets)).containsExactlyInAnyOrder("n1", "n2");
  }

  @Test
  void prefersHighestPriorityPeer() {
    final List<RaftPeer> peers = List.of(peer("n0", 5), peer("n1", 1), peer("n2", 9), peer("n3", 3));
    final List<RaftPeer> targets = RaftHAServer.selectStepDownTargets(peers, RaftPeerId.valueOf("n0"), null);
    // Ordered by descending priority: n2(9) > n3(3) > n1(1).
    assertThat(ids(targets)).containsExactly("n2", "n3", "n1");
  }

  @Test
  void skipsPriorityZeroWitnessWhenVotersExist() {
    // n2 is a priority-0 witness/replica; with positive-priority voters present it must never be a target.
    final List<RaftPeer> peers = List.of(peer("n0", 5), peer("n1", 3), peer("n2", 0));
    final List<RaftPeer> targets = RaftHAServer.selectStepDownTargets(peers, RaftPeerId.valueOf("n0"), null);
    assertThat(ids(targets)).containsExactly("n1");
  }

  @Test
  void skipsLaggingFollower() {
    final List<RaftPeer> peers = List.of(peer("n0", 0), peer("n1", 0), peer("n2", 0));

    final ClusterMonitor monitor = new ClusterMonitor(10L);
    monitor.updateLeaderCommitIndex(1000L);
    monitor.updateReplicaMatchIndex("n1", 5L, 1L);    // lag 995 > threshold 10 -> lagging
    monitor.updateReplicaMatchIndex("n2", 1000L, 1L); // lag 0 -> healthy

    final List<RaftPeer> targets = RaftHAServer.selectStepDownTargets(peers, RaftPeerId.valueOf("n0"), monitor);
    assertThat(ids(targets)).containsExactly("n2");
  }

  @Test
  void combinesPriorityAndLag() {
    // n2 has the highest priority but is lagging; n1 (healthy, lower priority) must win.
    final List<RaftPeer> peers = List.of(peer("n0", 1), peer("n1", 3), peer("n2", 9));

    final ClusterMonitor monitor = new ClusterMonitor(10L);
    monitor.updateLeaderCommitIndex(1000L);
    monitor.updateReplicaMatchIndex("n1", 1000L, 1L); // healthy
    monitor.updateReplicaMatchIndex("n2", 5L, 1L);    // lagging

    final List<RaftPeer> targets = RaftHAServer.selectStepDownTargets(peers, RaftPeerId.valueOf("n0"), monitor);
    assertThat(ids(targets)).containsExactly("n1");
  }

  @Test
  void noEligibleTargetReturnsEmptyList() {
    // Only peers are a priority-0 witness and a lagging voter: nothing eligible, caller must defer to Ratis.
    final List<RaftPeer> peers = List.of(peer("n0", 5), peer("n1", 3), peer("n2", 0));

    final ClusterMonitor monitor = new ClusterMonitor(10L);
    monitor.updateLeaderCommitIndex(1000L);
    monitor.updateReplicaMatchIndex("n1", 5L, 1L); // the only voter is lagging

    final List<RaftPeer> targets = RaftHAServer.selectStepDownTargets(peers, RaftPeerId.valueOf("n0"), monitor);
    assertThat(targets).isEmpty();
  }

  @Test
  void singleNodeClusterReturnsEmptyList() {
    final List<RaftPeer> peers = List.of(peer("n0", 0));
    final List<RaftPeer> targets = RaftHAServer.selectStepDownTargets(peers, RaftPeerId.valueOf("n0"), null);
    assertThat(targets).isEmpty();
  }
}
