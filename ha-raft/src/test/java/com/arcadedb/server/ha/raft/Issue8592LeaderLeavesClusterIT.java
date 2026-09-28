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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8592 on a live cluster: the leader leaving goes through the shared hand-off (the ranked candidates of
 * {@link RaftHAServer#selectStepDownTargets}, no bare step-down) and the removal, and the two nodes left keep a leader
 * and a configuration without it.
 */
@Tag("slow")
class Issue8592LeaderLeavesClusterIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void theLeaderLeavingHandsOffAndIsRemoved() throws Exception {
    final int leaderIndex = waitForLeaderIndex(30_000);
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final String leavingPeerId = peerIdForIndex(leaderIndex);

    getRaftPlugin(leaderIndex).getRaftHAServer().leaveCluster(false);

    int newLeaderIndex = -1;
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      newLeaderIndex = leaderAmong(leaderIndex);
      if (newLeaderIndex >= 0 && !peerIds(newLeaderIndex).contains(leavingPeerId))
        break;
      Thread.sleep(200);
    }

    assertThat(newLeaderIndex).as("One of the two remaining nodes leads").isGreaterThanOrEqualTo(0).isNotEqualTo(leaderIndex);
    assertThat(peerIds(newLeaderIndex)).as("The leader left the configuration").doesNotContain(leavingPeerId).hasSize(2);
  }

  /** The leader among the nodes other than {@code excluded}, or -1. */
  private int leaderAmong(final int excluded) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != excluded && getRaftPlugin(i) != null && getRaftPlugin(i).getRaftHAServer().isLeader())
        return i;
    return -1;
  }

  private List<String> peerIds(final int serverIndex) {
    final List<String> ids = new ArrayList<>();
    for (final RaftPeer peer : getRaftPlugin(serverIndex).getRaftHAServer().getLivePeers())
      ids.add(peer.getId().toString());
    return ids;
  }

  private int waitForLeaderIndex(final long timeoutMs) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      final int leaderIndex = findLeaderIndex();
      if (leaderIndex >= 0)
        return leaderIndex;
      Thread.sleep(200);
    }
    return findLeaderIndex();
  }
}
