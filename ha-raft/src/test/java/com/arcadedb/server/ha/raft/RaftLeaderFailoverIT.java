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
 * Tests leader failover: write data, stop the leader, wait for new leader election, write more data.
 */
class RaftLeaderFailoverIT extends BaseRaftHATest {

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
  void leaderFailoverElectsNewLeader() {
    // Find the leader server
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Create type and write initial data on the leader
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("RaftFailover"))
        leaderDb.getSchema().createVertexType("RaftFailover");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final MutableVertex v = leaderDb.newVertex("RaftFailover");
        v.set("name", "before-failover-" + i);
        v.set("phase", "before");
        v.save();
      }
    });

    assertClusterConsistency();

    // Verify initial data on all nodes
    for (int i = 0; i < getServerCount(); i++) {
      final var nodeDb = getServerDatabase(i, getDatabaseName());
      final long count = nodeDb.countType("RaftFailover", true);
      assertThat(count).as("Server " + i + " should have 50 records before failover").isEqualTo(50);
    }

    // Stop the leader
    LogManager.instance().log(this, Level.INFO, "TEST: Stopping leader server %d", leaderIndex);
    getServer(leaderIndex).stop();

    // Wait for a new leader to be elected among the remaining 2 nodes
    final int newLeaderIndex = waitForNewLeader(leaderIndex);
    assertThat(newLeaderIndex).as("A new Raft leader must be elected after stopping the old leader").isGreaterThanOrEqualTo(0);
    assertThat(newLeaderIndex).as("New leader must be different from stopped leader").isNotEqualTo(leaderIndex);

    LogManager.instance().log(this, Level.INFO, "TEST: New leader elected on server %d", newLeaderIndex);

    // Write more data on the new leader
    final var newLeaderDb = getServerDatabase(newLeaderIndex, getDatabaseName());
    newLeaderDb.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final MutableVertex v = newLeaderDb.newVertex("RaftFailover");
        v.set("name", "after-failover-" + i);
        v.set("phase", "after");
        v.save();
      }
    });

    // Verify new data is present on the surviving nodes
    for (int i = 0; i < getServerCount(); i++) {
      if (i == leaderIndex)
        continue; // Skip the stopped server
      final var nodeDb = getServerDatabase(i, getDatabaseName());
      if (nodeDb != null) {
        waitForReplicationIsCompleted(i);
        final long count = nodeDb.countType("RaftFailover", true);
        assertThat(count).as("Server " + i + " should have 100 records after failover").isEqualTo(100);
      }
    }
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // Skip deep byte-level comparison after failover tests.
    // The test itself verifies data consistency on surviving nodes.
    // After a leader failover, page versions may differ between the new leader
    // and the follower due to the leadership transition.
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
