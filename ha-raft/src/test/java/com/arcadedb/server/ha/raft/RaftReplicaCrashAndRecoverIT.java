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
 * Integration test: 3-node cluster with majority quorum replication.
 * Verifies that a crashed replica restarts and catches up to the leader via Raft log replay
 * (hot resync), with full DatabaseComparator verification after recovery.
 * <p>
 * A 3-node cluster is used so that when one follower is stopped, the remaining
 * leader + 1 follower still form a majority (2/3), allowing writes to continue.
 * This is the minimal viable cluster topology for crash-and-recover testing.
 */
class RaftReplicaCrashAndRecoverIT extends BaseRaftHATest {

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
  void replicaCatchesUpAfterRestart() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    // Pick any follower as the replica to crash
    int replicaIndex = -1;
    for (int i = 0; i < getServerCount(); i++) {
      if (i != leaderIndex) {
        replicaIndex = i;
        break;
      }
    }
    assertThat(replicaIndex).as("Must find a replica to crash").isGreaterThanOrEqualTo(0);

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Phase 1: write initial data with all nodes up
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("RaftCrashRecover"))
        leaderDb.getSchema().createVertexType("RaftCrashRecover");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 200; i++) {
        final MutableVertex v = leaderDb.newVertex("RaftCrashRecover");
        v.set("name", "phase1-" + i);
        v.set("phase", 1);
        v.save();
      }
    });

    assertClusterConsistency();

    // Verify replica has all phase-1 data before crash
    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("RaftCrashRecover", true))
        .as("Replica should have 200 records before crash").isEqualTo(200);

    // Phase 2: crash one follower, write more data on the leader
    // The remaining 2 nodes (leader + 1 follower) still form majority (2/3)
    LogManager.instance().log(this, Level.INFO, "TEST: Crashing replica %d", replicaIndex);
    getServer(replicaIndex).stop();

    leaderDb.transaction(() -> {
      for (int i = 0; i < 200; i++) {
        final MutableVertex v = leaderDb.newVertex("RaftCrashRecover");
        v.set("name", "phase2-" + i);
        v.set("phase", 2);
        v.save();
      }
    });

    assertThat(leaderDb.countType("RaftCrashRecover", true))
        .as("Leader should have 400 records while replica is down").isEqualTo(400);

    // Phase 3: restart the crashed replica and let it catch up via log replay
    LogManager.instance().log(this, Level.INFO, "TEST: Restarting replica %d", replicaIndex);
    restartServer(replicaIndex);

    // Verify catch-up: crashed replica must have all 400 records
    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("RaftCrashRecover", true))
        .as("Replica should have all 400 records after recovery").isEqualTo(400);

    // Full DatabaseComparator verification (not overridden — byte-level check)
    assertClusterConsistency();
  }
}
