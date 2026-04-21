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
 * Tests replica failure scenarios during snapshot installation.
 * Ported from apache-ratis branch, adapted to ha-redesign's BaseRaftHATest framework.
 */
class RaftReplicaFailureIT extends BaseRaftHATest {

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_THRESHOLD, 10L);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void replicaRecoverAfterLongAbsenceRequiringSnapshot() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int replicaIndex = (leaderIndex + 1) % getServerCount();

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("FailureTest"))
        leaderDb.getSchema().createVertexType("FailureTest");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 30; i++) {
        final MutableVertex v = leaderDb.newVertex("FailureTest");
        v.set("name", "before-" + i);
        v.save();
      }
    });

    assertClusterConsistency();

    // Stop replica
    LogManager.instance().log(this, Level.INFO, "TEST: Stopping replica %d for long absence", replicaIndex);
    getServer(replicaIndex).stop();

    // Write enough data to trigger multiple log compactions
    for (int batch = 0; batch < 5; batch++) {
      final int b = batch;
      leaderDb.transaction(() -> {
        for (int i = 0; i < 50; i++) {
          final MutableVertex v = leaderDb.newVertex("FailureTest");
          v.set("name", "during-" + b + "-" + i);
          v.save();
        }
      });
    }

    assertThat(leaderDb.countType("FailureTest", true)).isEqualTo(280);

    // Restart the long-absent replica - needs full snapshot
    LogManager.instance().log(this, Level.INFO, "TEST: Restarting replica %d after long absence", replicaIndex);
    restartServer(replicaIndex);

    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("FailureTest", true))
        .as("Replica should have all records after recovery").isEqualTo(280);

    assertClusterConsistency();
  }
}
