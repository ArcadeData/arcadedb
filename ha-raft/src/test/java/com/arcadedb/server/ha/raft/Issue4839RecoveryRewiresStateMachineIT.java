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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression IT for issue #4839: the HealthMonitor-driven in-place Ratis restart
 * ({@link RaftHAServer#restartRatisIfNeeded()}) builds a brand-new {@link ArcadeStateMachine}. Before
 * the fix the recovery path called only {@code setServer} and forgot {@code setRaftHAServer(this)}, so
 * the new machine's {@code raftHAServer} field stayed null forever. A node recovered through that path
 * could then no longer install snapshots from the leader ({@code notifyInstallSnapshotFromLeader}
 * dereferences {@code raftHAServer} -> NPE), never marked its locally-originated transactions (the
 * origin-skip never fired -> a future leader re-applies its own committed txns -> divergence), and
 * silently skipped leader-change handling.
 * <p>
 * This test starts a real 3-node cluster, triggers the same restart the HealthMonitor would, and
 * asserts that the freshly built state machine is fully wired ({@code getRaftHAServer() != null}) and
 * that the recovered follower still replicates writes from the leader.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue4839RecoveryRewiresStateMachineIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Test
  void restartRewiresStateMachineRaftHAServer() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    int replicaIndex = -1;
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex) {
        replicaIndex = i;
        break;
      }
    assertThat(replicaIndex).as("a replica must exist").isGreaterThanOrEqualTo(0);

    final RaftHAServer follower = getRaftPlugin(replicaIndex).getRaftHAServer();

    // Sanity: at startup the state machine is fully wired.
    final ArcadeStateMachine before = follower.getStateMachine();
    assertThat(before.getRaftHAServer())
        .as("state machine built at startup must be wired to the owning RaftHAServer")
        .isSameAs(follower);

    // Trigger the in-place HealthMonitor recovery path (CLOSED/EXCEPTION restart). It replaces the
    // state machine with a fresh instance.
    LogManager.instance().log(this, Level.INFO, "TEST: triggering Ratis restart on replica %d", replicaIndex);
    follower.restartRatisIfNeeded();

    final ArcadeStateMachine after = follower.getStateMachine();
    assertThat(after).as("recovery must build a new state machine instance").isNotSameAs(before);

    // The regression: before the fix this field was null on the recovery-built machine.
    assertThat(after.getRaftHAServer())
        .as("recovery-built state machine must be wired to its RaftHAServer (issue #4839)")
        .isSameAs(follower);

    // The recovered follower must remain a working cluster member: a write on the leader must still
    // replicate to it. (With a null raftHAServer the snapshot-install path would NPE.)
    waitForReplicationIsCompleted(replicaIndex);

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("PostRecovery"))
        leaderDb.getSchema().createVertexType("PostRecovery");
    });
    leaderDb.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        final MutableVertex v = leaderDb.newVertex("PostRecovery");
        v.set("name", "post-" + i);
        v.save();
      }
    });

    assertClusterConsistency();

    final var followerDb = getServerDatabase(replicaIndex, getDatabaseName());
    assertThat(followerDb.countType("PostRecovery", true))
        .as("recovered follower must receive writes replicated after the restart")
        .isEqualTo(20);
  }
}
