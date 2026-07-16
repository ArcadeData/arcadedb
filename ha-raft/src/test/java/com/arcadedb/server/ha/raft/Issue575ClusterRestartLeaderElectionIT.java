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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduction attempt for issue #575 (Locstat): after a config change and a restart of ALL cluster
 * nodes, the 3-node cluster stayed leaderless with every node reporting {@code VOTING_FOR_ME} and
 * {@code leader == null}. The client also saw the Ratis backward-term assertion
 * {@code Failed updateLastAppliedTermIndex: newTI = (t:10, i:39707284) < oldTI = (t:11, i:39707283)}
 * during the shutdown that preceded the restart.
 * <p>
 * This test drives the closest scenario the in-process {@link org.apache.ratis.grpc.MiniRaftClusterWithGrpc}
 * harness can express: build up a committed log, force a leader change (term bump) as happened during the
 * client's shutdown, then restart every peer and assert that a leader is re-elected (i.e. the cluster does
 * NOT get stuck leaderless).
 * <p>
 * NOTE: this harness does NOT wire {@code setRaftHAServer(...)}, so ArcadeDB's snapshot-install path
 * ({@link ArcadeStateMachine#notifyInstallSnapshotFromLeader}) is NOT exercised here. The backward-term
 * assertion the client hit originates in that path and is therefore NOT expected to reproduce in this test.
 */
@Tag("slow")
class Issue575ClusterRestartLeaderElectionIT extends BaseMiniRaftTest {

  private static final String DB_NAME = "mini-raft-test";

  @Override
  protected int getPeerCount() {
    return 3;
  }

  @Test
  void clusterReelectsLeaderAfterFullRestartFollowingLeaderChange() throws Exception {
    // Phase 1: build up a committed log on all 3 nodes.
    for (int i = 0; i < 50; i++) {
      final RaftClientReply reply = submitSchemaEntry(DB_NAME, null);
      assertThat(reply.isSuccess()).as("Entry %d should succeed with 3 nodes up", i).isTrue();
    }
    assertAllPeersConverged(50);

    // Phase 2: force a leader change (term bump) - this mirrors the leadership change the client
    // observed during shutdown (old leader term 10 -> new leader term 11).
    final int firstLeader = findLeaderPeerIndex();
    assertThat(firstLeader).as("A leader must exist before the leader change").isGreaterThanOrEqualTo(0);
    LogManager.instance().log(this, Level.INFO, "TEST #575: killing leader %d to force a term bump", firstLeader);
    killPeer(firstLeader);

    final long electionDeadline = System.currentTimeMillis() + 30_000;
    int newLeader = -1;
    while (System.currentTimeMillis() < electionDeadline) {
      newLeader = findLeaderPeerIndex();
      if (newLeader >= 0 && newLeader != firstLeader)
        break;
      Thread.sleep(300);
    }
    assertThat(newLeader).as("A new leader must be elected after the term bump").isGreaterThanOrEqualTo(0);

    // Submit more entries at the higher term so the log carries a term boundary.
    for (int i = 0; i < 20; i++)
      submitSchemaEntry(DB_NAME, null);

    // Bring the killed node back so all 3 are up again before the full restart.
    restartPeer(firstLeader);
    Thread.sleep(2_000);

    // Phase 3: restart EVERY node (the client's actual operation - "restarting all cluster nodes").
    LogManager.instance().log(this, Level.INFO, "TEST #575: restarting ALL peers simultaneously");
    for (int i = 0; i < getPeerCount(); i++)
      killPeer(i);
    for (int i = 0; i < getPeerCount(); i++)
      restartPeer(i);

    // Phase 4: the client's symptom was a PERMANENT leaderless state. Assert a leader re-emerges.
    final long reelectionDeadline = System.currentTimeMillis() + 60_000;
    int leaderAfterRestart = -1;
    while (System.currentTimeMillis() < reelectionDeadline) {
      leaderAfterRestart = findLeaderPeerIndex();
      if (leaderAfterRestart >= 0)
        break;
      Thread.sleep(500);
    }
    assertThat(leaderAfterRestart)
        .as("After restarting all nodes the cluster must re-elect a leader (client saw permanent VOTING_FOR_ME)")
        .isGreaterThanOrEqualTo(0);
    LogManager.instance().log(this, Level.INFO, "TEST #575: leader after full restart = peer %d", leaderAfterRestart);
  }
}
