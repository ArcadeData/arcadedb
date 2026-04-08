/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.ratis;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 3-node cluster with none quorum.
 * Tests replica failure: write data, stop a replica (not the leader), write more data,
 * and verify the leader and remaining replica continue operating.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("IntegrationTest")
class RaftReplicaFailureIT extends BaseGraphServerTest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void writesContinueAfterReplicaStop() {
    final int leaderIndex = getLeaderIndex();
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

    waitForReplicationConvergence();

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
  protected int[] getServerToCheck() {
    final int count = getServerCount();
    int alive = 0;
    for (int i = 0; i < count; i++)
      if (getServer(i) != null && getServer(i).isStarted())
        alive++;
    final int[] result = new int[alive];
    int idx = 0;
    for (int i = 0; i < count; i++)
      if (getServer(i) != null && getServer(i).isStarted())
        result[idx++] = i;
    return result;
  }
}
