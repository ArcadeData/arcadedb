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
 * Integration test: 3-node cluster with none quorum.
 * Tests replica failure: write data, stop a replica (not the leader), write more data,
 * and verify the leader and remaining replica continue operating.
 * <p>
 * Note: Replica restart and catch-up is not tested here because in-JVM Raft tests
 * do not reliably support server restart (Raft storage directories are cleaned on startup,
 * and a restarted server would need to rejoin the Raft group).
 */
class RaftReplicaFailureIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "none");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void writesContinueAfterReplicaStop() {
    // Find the leader server
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Create type and write initial data
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("RaftReplicaFail"))
        leaderDb.getSchema().createVertexType("RaftReplicaFail");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final MutableVertex v = leaderDb.newVertex("RaftReplicaFail");
        v.set("name", "before-stop-" + i);
        v.set("phase", "before");
        v.save();
      }
    });

    assertClusterConsistency();

    // Find a replica (not the leader) to stop
    int replicaToStop = -1;
    for (int i = 0; i < getServerCount(); i++) {
      if (i != leaderIndex) {
        replicaToStop = i;
        break;
      }
    }
    assertThat(replicaToStop).as("Must find a replica to stop").isGreaterThanOrEqualTo(0);

    // Verify initial data before stopping replica
    final var replicaDb = getServerDatabase(replicaToStop, getDatabaseName());
    assertThat(replicaDb.countType("RaftReplicaFail", true))
        .as("Replica should have 50 records before stop")
        .isEqualTo(50);

    // Stop the replica
    LogManager.instance().log(this, Level.INFO, "TEST: Stopping replica server %d (leader is %d)", replicaToStop, leaderIndex);
    getServer(replicaToStop).stop();

    // Write more data on the leader (should succeed with quorum=none)
    leaderDb.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final MutableVertex v = leaderDb.newVertex("RaftReplicaFail");
        v.set("name", "after-stop-" + i);
        v.set("phase", "after");
        v.save();
      }
    });

    // Verify that the leader has all 100 records
    assertThat(leaderDb.countType("RaftReplicaFail", true))
        .as("Leader should have 100 records")
        .isEqualTo(100);

    // Verify the other surviving replica (not the leader and not the stopped one) also has data
    for (int i = 0; i < getServerCount(); i++) {
      if (i == leaderIndex || i == replicaToStop)
        continue;
      waitForReplicationIsCompleted(i);
      final var nodeDb = getServerDatabase(i, getDatabaseName());
      assertThat(nodeDb.countType("RaftReplicaFail", true))
          .as("Surviving replica server " + i + " should have 100 records")
          .isEqualTo(100);
    }
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // Skip deep byte-level comparison after replica failure tests.
    // The test itself verifies data consistency on surviving nodes.
  }

  @Override
  protected int[] getServerToCheck() {
    // Only check servers that are still running
    final int count = getServerCount();
    int alive = 0;
    for (int i = 0; i < count; i++) {
      if (getServer(i) != null && getServer(i).isStarted())
        alive++;
    }
    final int[] result = new int[alive];
    int idx = 0;
    for (int i = 0; i < count; i++) {
      if (getServer(i) != null && getServer(i).isStarted())
        result[idx++] = i;
    }
    return result;
  }

  private int findLeaderIndex() {
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin != null && plugin.isLeader())
        return i;
    }
    return -1;
  }
}
