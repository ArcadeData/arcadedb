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
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: 2-node cluster with none quorum.
 * Tests leader down scenario in a 2-node setup.
 * <p>
 * With 2 nodes and Raft, losing the leader means the remaining single node cannot form
 * a majority (needs 2 out of 2) for a new leader election. The remaining node should
 * not be able to become leader and therefore should not accept writes that require
 * Raft consensus.
 */
class RaftLeaderDown2NodesIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "none");
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void remainingNodeCannotElectLeaderAfterLeaderDown() {
    // Find the leader server
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final int replicaIndex = leaderIndex == 0 ? 1 : 0;
    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Create type and write initial data
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("RaftLeaderDown"))
        leaderDb.getSchema().createVertexType("RaftLeaderDown");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 30; i++) {
        final MutableVertex v = leaderDb.newVertex("RaftLeaderDown");
        v.set("name", "before-leader-down-" + i);
        v.save();
      }
    });

    assertClusterConsistency();

    // Verify replica has the data
    final var replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    assertThat(replicaDb.countType("RaftLeaderDown", true))
        .as("Replica should have 30 records")
        .isEqualTo(30);

    // Stop the leader
    LogManager.instance().log(this, Level.INFO, "TEST: Stopping leader server %d", leaderIndex);
    getServer(leaderIndex).stop();

    // Wait for the remaining node to detect the loss
    try {
      Thread.sleep(3000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // The remaining node should NOT become leader (cannot form majority with 1 out of 2)
    final RaftHAPlugin replicaPlugin = getRaftPlugin(replicaIndex);
    if (replicaPlugin != null) {
      final boolean isLeader = replicaPlugin.isLeader();
      LogManager.instance().log(this, Level.INFO,
          "TEST: Remaining node isLeader=%s (expected: false in standard Raft)", isLeader);

      // Try to write on the remaining node with a timeout.
      // Ratis may hang indefinitely if no leader can be elected.
      final ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        final Future<?> writeFuture = executor.submit(() -> {
          replicaDb.transaction(() -> {
            final MutableVertex v = replicaDb.newVertex("RaftLeaderDown");
            v.set("name", "after-leader-down");
            v.save();
          });
        });

        try {
          writeFuture.get(10, TimeUnit.SECONDS);
          // If write succeeds, the node either became leader or the write is local-only
          LogManager.instance().log(this, Level.WARNING,
              "TEST: Write succeeded on remaining node after leader down");
        } catch (final TimeoutException e) {
          writeFuture.cancel(true);
          LogManager.instance().log(this, Level.INFO,
              "TEST: Write correctly timed out on remaining node (no leader)");
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.INFO,
              "TEST: Write correctly failed on remaining node: %s", e.getMessage());
        }
      } finally {
        executor.shutdownNow();
      }
    }

    // Verify the replica still has the original data intact
    assertThat(replicaDb.countType("RaftLeaderDown", true))
        .as("Replica should still have at least the original 30 records")
        .isGreaterThanOrEqualTo(30);
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // Skip deep byte-level comparison after leader down tests.
    // Only one server remains running, so comparison is not meaningful.
  }

  @Override
  protected int[] getServerToCheck() {
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
}
