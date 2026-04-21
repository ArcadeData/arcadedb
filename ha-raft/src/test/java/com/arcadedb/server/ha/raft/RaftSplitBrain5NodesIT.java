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

import com.arcadedb.log.LogManager;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 5-node cluster via MiniRaftClusterWithGrpc.
 * Tests partition scenario: 2 minority nodes are killed, 3-node majority continues
 * and accepts writes. Both minority nodes restart and converge to the majority state.
 * Verifies that all 5 state machines applied all 100 entries after convergence.
 */
class RaftSplitBrain5NodesIT extends BaseMiniRaftTest {

  private static final String DB_NAME = "mini-raft-test";

  @Override
  protected int getPeerCount() {
    return 5;
  }

  @Test
  void majorityOfThreeContinuesAfterTwoNodesKilled() throws Exception {
    // Phase 1: submit 50 entries with all 5 nodes up
    for (int i = 0; i < 50; i++) {
      final RaftClientReply reply = submitSchemaEntry(DB_NAME, null);
      assertThat(reply.isSuccess()).as("Initial entry %d should succeed with 5 nodes up", i).isTrue();
    }

    assertAllPeersConverged(50);

    // Phase 2: kill 2 minority nodes (non-leaders to keep the 3-node majority intact)
    final int leaderPeerIndex = findLeaderPeerIndex();
    assertThat(leaderPeerIndex).as("A leader must exist").isGreaterThanOrEqualTo(0);

    final List<Integer> killedIndices = new ArrayList<>();
    for (int i = 0; i < getPeerCount() && killedIndices.size() < 2; i++) {
      if (i != leaderPeerIndex) {
        final RaftPeerId peerId = getPeers().get(i).getId();
        LogManager.instance().log(this, Level.INFO, "TEST: Killing minority peer %s (index %d)", peerId, i);
        killPeer(i);
        killedIndices.add(i);
      }
    }
    assertThat(killedIndices).hasSize(2);

    // Phase 3: submit 50 more entries on the 3-node majority (quorum = 2 out of 3)
    for (int i = 0; i < 50; i++) {
      final RaftClientReply reply = submitSchemaEntry(DB_NAME, null);
      assertThat(reply.isSuccess()).as("Majority write %d should succeed with 3/5 nodes up", i).isTrue();
    }

    // Phase 4: restart both killed nodes — they converge to the majority log
    for (final int idx : killedIndices) {
      final RaftPeerId peerId = getPeers().get(idx).getId();
      LogManager.instance().log(this, Level.INFO, "TEST: Restarting minority peer %s (index %d)", peerId, idx);
      restartPeer(idx);
    }

    // All 5 nodes must have applied all 100 entries
    assertAllPeersConverged(100);
  }
}
