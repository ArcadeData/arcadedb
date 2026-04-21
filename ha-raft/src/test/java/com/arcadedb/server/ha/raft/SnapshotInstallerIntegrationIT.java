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

import java.nio.file.Path;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test verifying that {@link SnapshotInstaller} correctly installs a database
 * snapshot on a follower that has fallen behind the leader's compacted log, and that
 * marker files are properly cleaned up after installation.
 */
class SnapshotInstallerIntegrationIT extends BaseRaftHATest {

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    // Low threshold to trigger log compaction quickly
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_THRESHOLD, 10L);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void followerInstallsSnapshotViaCrashSafeFlow() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = (leaderIndex + 1) % getServerCount();

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Create type and initial data
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("SnapshotTest"))
        leaderDb.getSchema().createVertexType("SnapshotTest");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final MutableVertex v = leaderDb.newVertex("SnapshotTest");
        v.set("name", "phase1-" + i);
        v.save();
      }
    });

    assertClusterConsistency();

    // Stop replica and write enough to trigger log compaction
    LogManager.instance().log(this, Level.INFO, "TEST: Stopping replica %d", replicaIndex);
    getServer(replicaIndex).stop();

    leaderDb.transaction(() -> {
      for (int i = 0; i < 200; i++) {
        final MutableVertex v = leaderDb.newVertex("SnapshotTest");
        v.set("name", "phase2-" + i);
        v.save();
      }
    });

    assertThat(leaderDb.countType("SnapshotTest", true)).isEqualTo(250);

    // Restart replica - should trigger snapshot install via SnapshotInstaller
    LogManager.instance().log(this, Level.INFO, "TEST: Restarting replica %d - expecting snapshot install", replicaIndex);
    restartServer(replicaIndex);

    // Verify data consistency
    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("SnapshotTest", true))
        .as("Replica should have all 250 records after snapshot install").isEqualTo(250);

    // Verify no marker files left behind
    final String dbPath = getServerDatabase(replicaIndex, getDatabaseName()).getDatabasePath();
    assertThat(Path.of(dbPath).resolve(SnapshotInstaller.SNAPSHOT_PENDING_FILE)).doesNotExist();
    assertThat(Path.of(dbPath).resolve(SnapshotInstaller.SNAPSHOT_NEW_DIR)).doesNotExist();
    assertThat(Path.of(dbPath).resolve(SnapshotInstaller.SNAPSHOT_BACKUP_DIR)).doesNotExist();

    assertClusterConsistency();
  }
}
