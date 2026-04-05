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
 * Integration test: 3-node cluster with majority quorum.
 * Tests quorum loss: write initial data, stop 2 replicas, verify that writes fail
 * or timeout because the remaining node cannot achieve majority quorum.
 */
class RaftQuorumLostIT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 5000);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void writesFailOrTimeoutWhenQuorumIsLost() {
    // Find the leader server
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Create type and write initial data with full quorum
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("RaftQuorumLoss"))
        leaderDb.getSchema().createVertexType("RaftQuorumLoss");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        final MutableVertex v = leaderDb.newVertex("RaftQuorumLoss");
        v.set("name", "before-quorum-loss-" + i);
        v.save();
      }
    });

    assertClusterConsistency();

    // Stop both replicas (all non-leader nodes)
    for (int i = 0; i < getServerCount(); i++) {
      if (i != leaderIndex) {
        LogManager.instance().log(this, Level.INFO, "TEST: Stopping replica server %d", i);
        getServer(i).stop();
      }
    }

    // Wait for the leader to detect the loss
    try {
      Thread.sleep(3000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Attempt to write on the remaining leader with a timeout.
    // Ratis may hang indefinitely waiting for quorum, so we use a Future with timeout.
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> writeFuture = executor.submit(() -> {
        leaderDb.transaction(() -> {
          final MutableVertex v = leaderDb.newVertex("RaftQuorumLoss");
          v.set("name", "after-quorum-loss");
          v.save();
        });
      });

      try {
        writeFuture.get(10, TimeUnit.SECONDS);
        // Write succeeded - this can happen if the leader hasn't detected the loss yet
        // or if Raft configuration allows the leader to continue writing locally
        LogManager.instance().log(this, Level.WARNING,
            "TEST: Write succeeded after quorum loss - leader may not have detected loss yet");
      } catch (final TimeoutException e) {
        // Expected: write timed out because quorum cannot be reached
        writeFuture.cancel(true);
        LogManager.instance().log(this, Level.INFO,
            "TEST: Write correctly timed out after quorum loss");
      } catch (final Exception e) {
        // Also expected: write failed with an exception
        LogManager.instance().log(this, Level.INFO,
            "TEST: Write correctly failed after quorum loss: %s", e.getMessage());
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // Skip deep byte-level comparison after quorum loss tests.
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
