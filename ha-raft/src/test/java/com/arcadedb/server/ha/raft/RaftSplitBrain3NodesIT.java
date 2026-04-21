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
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 3-node cluster via MiniRaftClusterWithGrpc.
 * Tests leader loss and recovery: the 2-node majority elects a new leader after
 * the leader is killed, accepts further writes, then the old leader restarts and converges.
 * Verifies that all 3 state machines applied all 100 entries after convergence.
 * <p>
 * Note: true split-brain (where both partitions continue accepting writes simultaneously)
 * requires gRPC-level message interception. This test covers the key correctness property
 * achievable in-process: majority continues after leader loss and the recovered node
 * converges to the majority state.
 */
class RaftSplitBrain3NodesIT extends BaseMiniRaftTest {

  private static final String DB_NAME = "mini-raft-test";

  @Override
  protected int getPeerCount() {
    return 3;
  }

  @Test
  void majorityElectsNewLeaderAfterLeaderLoss() throws Exception {
    // Phase 1: submit 50 entries with all 3 nodes up
    for (int i = 0; i < 50; i++) {
      final RaftClientReply reply = submitSchemaEntry(DB_NAME, null);
      assertThat(reply.isSuccess()).as("Entry %d should succeed with 3 nodes up", i).isTrue();
    }

    assertAllPeersConverged(50);

    // Phase 2: kill the leader
    final int leaderPeerIndex = findLeaderPeerIndex();
    assertThat(leaderPeerIndex).as("A leader must exist").isGreaterThanOrEqualTo(0);
    final RaftPeerId leaderPeerId = getPeers().get(leaderPeerIndex).getId();

    LogManager.instance().log(this, Level.INFO, "TEST: Killing leader peer %s", leaderPeerId);
    killPeer(leaderPeerIndex);

    // Phase 3: wait for new leader among surviving 2 nodes
    final long electionDeadline = System.currentTimeMillis() + 30_000;
    int newLeaderIndex = -1;
    while (System.currentTimeMillis() < electionDeadline) {
      newLeaderIndex = findLeaderPeerIndex();
      if (newLeaderIndex >= 0 && newLeaderIndex != leaderPeerIndex)
        break;
      Thread.sleep(500);
    }
    assertThat(newLeaderIndex).as("A new leader must be elected").isGreaterThanOrEqualTo(0);
    assertThat(newLeaderIndex).as("New leader must differ from old leader").isNotEqualTo(leaderPeerIndex);
    LogManager.instance().log(this, Level.INFO, "TEST: New leader elected: peer index %d", newLeaderIndex);

    // Phase 4: submit 50 more entries on the new 2-node majority
    for (int i = 0; i < 50; i++) {
      final RaftClientReply reply = submitSchemaEntry(DB_NAME, null);
      assertThat(reply.isSuccess()).as("Entry %d after failover should succeed", i).isTrue();
    }

    // Phase 5: restart the old leader — it rejoins and converges to the majority log
    LogManager.instance().log(this, Level.INFO, "TEST: Restarting old leader peer index %d", leaderPeerIndex);
    restartPeer(leaderPeerIndex);

    // All 3 nodes must have applied all 100 entries
    assertAllPeersConverged(100);

    // Verify old leader is no longer the leader
    final RaftServer.Division restarted = getCluster().getDivision(leaderPeerId);
    assertThat(restarted).isNotNull();
    assertThat(restarted.getInfo().isLeader()).as("Old leader should be a follower after restart").isFalse();
  }
}
