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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.ha.HAServer;

import java.util.logging.Level;

/**
 * Base class for Raft HA integration tests.
 * Configures servers to use the Raft HA implementation instead of the legacy HAServer.
 * Overrides lifecycle methods that depend on legacy HAServer APIs.
 */
public abstract class BaseRaftHATest extends BaseGraphServerTest {

  private static final int BASE_RAFT_PORT = 2434;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.HA_IMPLEMENTATION, "raft");
  }

  @Override
  protected String getServerAddresses() {
    // For Raft HA, the server list contains host:raftPort (not HTTP port)
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getServerCount(); i++) {
      if (i > 0)
        sb.append(",");
      sb.append("localhost:").append(BASE_RAFT_PORT + i);
    }
    return sb.toString();
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected HAServer.SERVER_ROLE getServerRole(final int serverIndex) {
    // With Raft, leader election is automatic; all nodes start as ANY
    return HAServer.SERVER_ROLE.ANY;
  }

  @Override
  protected void waitForReplicationIsCompleted(final int serverNumber) {
    // Find the leader's last applied index
    long leaderLastIndex = -1;
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin != null && plugin.isLeader()) {
        final var termIndex = plugin.getRaftHAServer().getStateMachine().getLastAppliedTermIndex();
        if (termIndex != null)
          leaderLastIndex = termIndex.getIndex();
        break;
      }
    }

    if (leaderLastIndex <= 0)
      return;

    // Wait for this server's state machine to catch up to the leader's last applied index
    final RaftHAPlugin plugin = getRaftPlugin(serverNumber);
    if (plugin == null)
      return;

    final long targetIndex = leaderLastIndex;
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      final var termIndex = plugin.getRaftHAServer().getStateMachine().getLastAppliedTermIndex();
      if (termIndex != null && termIndex.getIndex() >= targetIndex)
        return;
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    LogManager.instance().log(this, Level.WARNING, "Timeout waiting for server %d to replicate to index %d", serverNumber, targetIndex);
  }

  @Override
  protected void waitAllReplicasAreConnected() {
    // Wait for a Raft leader to be elected
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      for (int i = 0; i < getServerCount(); i++) {
        final RaftHAPlugin plugin = getRaftPlugin(i);
        if (plugin != null && plugin.isLeader()) {
          LogManager.instance().log(this, Level.INFO, "Raft leader elected on server %d", i);
          serversSynchronized = true;
          return;
        }
      }
      try {
        Thread.sleep(500);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    LogManager.instance().log(this, Level.WARNING, "Timeout waiting for Raft leader election");
    serversSynchronized = true;
  }

  /**
   * Returns the RaftHAPlugin from the specified server, or null if not available.
   */
  protected RaftHAPlugin getRaftPlugin(final int serverIndex) {
    if (getServer(serverIndex) == null || !getServer(serverIndex).isStarted())
      return null;
    for (final ServerPlugin plugin : getServer(serverIndex).getPlugins()) {
      if (plugin instanceof RaftHAPlugin raftPlugin)
        return raftPlugin;
    }
    return null;
  }

  /**
   * Waits for replication to propagate across the cluster, then verifies
   * that all server databases are identical.
   */
  protected void assertClusterConsistency() {
    for (int i = 0; i < getServerCount(); i++) {
      if (getServer(i) != null && getServer(i).isStarted())
        waitForReplicationIsCompleted(i);
    }
    checkDatabasesAreIdentical();
  }
}
