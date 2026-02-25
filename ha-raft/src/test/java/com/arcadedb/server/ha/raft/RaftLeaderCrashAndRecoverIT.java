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
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 3-node cluster with majority quorum.
 * Verifies that after a leader crash, a new leader is elected, writes continue,
 * the old leader rejoins as a follower and catches up, with full DatabaseComparator
 * verification after recovery.
 */
class RaftLeaderCrashAndRecoverIT extends BaseRaftHATest {

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void oldLeaderRejoinsAsFollowerAfterRestart() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Phase 1: write initial data with all nodes up
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("RaftLeaderRecover"))
        leaderDb.getSchema().createVertexType("RaftLeaderRecover");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 200; i++) {
        final MutableVertex v = leaderDb.newVertex("RaftLeaderRecover");
        v.set("name", "phase1-" + i);
        v.set("phase", 1);
        v.save();
      }
    });

    assertClusterConsistency();

    // Phase 2: crash the leader, wait for new leader
    LogManager.instance().log(this, Level.INFO, "TEST: Crashing leader %d", leaderIndex);
    getServer(leaderIndex).stop();

    final int newLeaderIndex = waitForNewLeader(leaderIndex);
    assertThat(newLeaderIndex).as("A new leader must be elected").isGreaterThanOrEqualTo(0);
    assertThat(newLeaderIndex).as("New leader must differ from old leader").isNotEqualTo(leaderIndex);
    LogManager.instance().log(this, Level.INFO, "TEST: New leader elected: server %d", newLeaderIndex);

    // Phase 3: write more data on the new leader
    final var newLeaderDb = getServerDatabase(newLeaderIndex, getDatabaseName());
    newLeaderDb.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableVertex v = newLeaderDb.newVertex("RaftLeaderRecover");
        v.set("name", "phase2-" + i);
        v.set("phase", 2);
        v.save();
      }
    });

    // Verify surviving nodes have 300 records
    for (int i = 0; i < getServerCount(); i++) {
      if (i == leaderIndex)
        continue;
      waitForReplicationIsCompleted(i);
      assertThat(getServerDatabase(i, getDatabaseName()).countType("RaftLeaderRecover", true))
          .as("Surviving server " + i + " should have 300 records").isEqualTo(300);
    }

    // Phase 4: restart the old leader as a follower
    LogManager.instance().log(this, Level.INFO, "TEST: Restarting old leader %d as follower", leaderIndex);
    restartServer(leaderIndex);

    // Verify old leader rejoined as follower (not leader) and has all 300 records
    final RaftHAPlugin oldLeaderPlugin = getRaftPlugin(leaderIndex);
    assertThat(oldLeaderPlugin).isNotNull();
    assertThat(oldLeaderPlugin.isLeader())
        .as("Old leader should be a follower after restart").isFalse();

    assertThat(getServerDatabase(leaderIndex, getDatabaseName()).countType("RaftLeaderRecover", true))
        .as("Recovered node should have all 300 records").isEqualTo(300);

    // Full DatabaseComparator verification across all 3 nodes
    assertClusterConsistency();
  }

  private int findLeaderIndex() {
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin != null && plugin.isLeader())
        return i;
    }
    return -1;
  }

  private int waitForNewLeader(final int excludeIndex) {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      for (int i = 0; i < getServerCount(); i++) {
        if (i == excludeIndex)
          continue;
        final RaftHAPlugin plugin = getRaftPlugin(i);
        if (plugin != null && plugin.isLeader())
          return i;
      }
      try {
        Thread.sleep(500);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return -1;
      }
    }
    return -1;
  }
}
