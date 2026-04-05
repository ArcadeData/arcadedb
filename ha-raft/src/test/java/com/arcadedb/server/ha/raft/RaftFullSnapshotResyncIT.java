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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 2-node cluster, low snapshot threshold.
 * Verifies that a replica which has fallen too far behind the leader's compacted log
 * receives a full snapshot install (rather than log replay) when it restarts, and
 * passes DatabaseComparator after recovery.
 * <p>
 * <strong>Status: disabled pending {@code ArcadeStateMachine.installSnapshot()} implementation.</strong>
 * <p>
 * {@link ArcadeStateMachine#takeSnapshot()} is implemented and returns the last-applied
 * index so Ratis can compact the log (reducing disk usage). However, snapshot-based
 * resync requires {@code installSnapshot()} to transfer actual ArcadeDB database files
 * from the leader to the lagging replica. Until that is implemented, a replica whose
 * Raft log entries have been purged post-snapshot cannot catch up and will fail to
 * rejoin. Implement {@code ArcadeStateMachine.installSnapshot()} to:
 * <ol>
 *   <li>Receive the snapshot file chunks from the leader peer</li>
 *   <li>Replace the local database directory with the snapshot data</li>
 *   <li>Reopen the database at the snapshot term/index</li>
 * </ol>
 * Once implemented, re-enable this test and verify it passes.
 */
@Disabled("Requires ArcadeStateMachine.installSnapshot() for snapshot-based replica resync")
class RaftFullSnapshotResyncIT extends BaseRaftHATest {

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "none");
    // Trigger a snapshot every 10 log entries so the leader compacts before the replica restarts
    config.setValue(GlobalConfiguration.HA_RAFT_SNAPSHOT_THRESHOLD, 10L);
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void replicaReceivesFullSnapshotAfterRestart() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Phase 1: write enough records to trigger at least one snapshot on the leader
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("RaftSnapshotResync"))
        leaderDb.getSchema().createVertexType("RaftSnapshotResync");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final MutableVertex v = leaderDb.newVertex("RaftSnapshotResync");
        v.set("name", "phase1-" + i);
        v.set("phase", 1);
        v.save();
      }
    });

    assertClusterConsistency();

    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("RaftSnapshotResync", true))
        .as("Replica should have 50 records before crash").isEqualTo(50);

    // Phase 2: stop replica, write many more records (> snapshot threshold) to force log compaction
    LogManager.instance().log(this, Level.INFO, "TEST: Stopping replica %d to simulate long absence", replicaIndex);
    getServer(replicaIndex).stop();

    leaderDb.transaction(() -> {
      for (int i = 0; i < 200; i++) {
        final MutableVertex v = leaderDb.newVertex("RaftSnapshotResync");
        v.set("name", "phase2-" + i);
        v.set("phase", 2);
        v.save();
      }
    });

    assertThat(leaderDb.countType("RaftSnapshotResync", true))
        .as("Leader should have 250 records while replica is down").isEqualTo(250);

    // Phase 3: restart replica - it should receive a snapshot install (not log replay)
    // This currently fails because installSnapshot() is not implemented.
    LogManager.instance().log(this, Level.INFO, "TEST: Restarting replica %d - expecting snapshot install", replicaIndex);
    restartServer(replicaIndex);

    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("RaftSnapshotResync", true))
        .as("Replica should have all 250 records after snapshot install").isEqualTo(250);

    // Full DatabaseComparator verification
    assertClusterConsistency();
  }
}
