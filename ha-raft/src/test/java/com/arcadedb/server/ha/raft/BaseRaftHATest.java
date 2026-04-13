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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.ha.HAPlugin;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.util.logging.Level;

/**
 * Base class for Raft HA integration tests. Configures servers to use the Ratis-based HA
 * implementation and overrides lifecycle methods that depend on Raft-specific APIs.
 * <p>
 * Port layout (from {@link BaseGraphServerTest}):
 * <ul>
 *   <li>Server {@code i}: Raft port = {@code 2424 + i}, HTTP port = {@code 2480 + i}</li>
 *   <li>Peer ID format: {@code localhost_<raftPort>}, e.g. {@code localhost_2424}</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class BaseRaftHATest extends BaseGraphServerTest {

  /**
   * Returns the Raft peer ID for a given server index. Matches the {@code host_raftPort} format
   * used by {@link RaftPeerAddressResolver#parsePeers} internally.
   */
  protected String peerIdForIndex(final int index) {
    // BaseGraphServerTest sets HA_REPLICATION_INCOMING_PORTS = 2424 + i
    return "localhost_" + (2424 + index);
  }

  /**
   * When {@code true}, Raft storage directories are NOT removed during cleanup so that
   * {@link #restartServer(int)} can rejoin the same Raft group.
   * Default is {@code false}.
   */
  protected boolean persistentRaftStorage() {
    return false;
  }

  /**
   * Extends base cleanup to also remove Ratis storage directories when
   * {@link #persistentRaftStorage()} is {@code false}.
   */
  @Override
  protected void deleteDatabaseFolders() {
    super.deleteDatabaseFolders();
    if (!persistentRaftStorage()) {
      final String rootPath = GlobalConfiguration.SERVER_ROOT_PATH.getValueAsString();
      for (int i = 0; i < getServerCount(); i++)
        FileUtils.deleteRecursively(new File(rootPath + File.separator + "ratis-storage" + File.separator + peerIdForIndex(i)));
    }
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected void waitForReplicationIsCompleted(final int serverNumber) {
    // Find the leader's last applied index, retrying briefly during election transitions
    long leaderLastIndex = -1;
    for (int attempt = 0; attempt < 30 && leaderLastIndex <= 0; attempt++) {
      for (int i = 0; i < getServerCount(); i++) {
        final HAPlugin ha = getServer(i) != null && getServer(i).isStarted() ? getServer(i).getHA() : null;
        if (ha != null && ha.isLeader()) {
          leaderLastIndex = ha.getLastAppliedIndex();
          break;
        }
      }
      if (leaderLastIndex <= 0) {
        try {
          Thread.sleep(100);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    if (leaderLastIndex <= 0)
      return;

    // Wait for this server's state machine to catch up
    final long targetIndex = leaderLastIndex;
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      final HAPlugin ha = getServer(serverNumber) != null && getServer(serverNumber).isStarted()
          ? getServer(serverNumber).getHA()
          : null;
      if (ha == null)
        return;
      if (ha.getLastAppliedIndex() >= targetIndex)
        return;
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    LogManager.instance().log(this, Level.WARNING, "Timeout waiting for server %d to replicate to index %d",
        serverNumber, targetIndex);
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    for (int i = 0; i < getServerCount(); i++) {
      if (getServer(i) != null && getServer(i).isStarted())
        waitForReplicationIsCompleted(i);
    }
    super.checkDatabasesAreIdentical();
  }

  /**
   * Waits for replication convergence on all running servers, then verifies database identity.
   */
  protected void assertClusterConsistency() {
    for (int i = 0; i < getServerCount(); i++) {
      if (getServer(i) != null && getServer(i).isStarted())
        waitForReplicationIsCompleted(i);
    }
    checkDatabasesAreIdentical();
  }

  /**
   * Returns the index of the current Raft leader, or {@code -1} if no leader is elected.
   */
  protected int findLeaderIndex() {
    for (int i = 0; i < getServerCount(); i++) {
      final HAPlugin ha = getServer(i) != null && getServer(i).isStarted() ? getServer(i).getHA() : null;
      if (ha != null && ha.isLeader())
        return i;
    }
    return -1;
  }

  /**
   * Returns the {@link RaftHAPlugin} from the given server index, or {@code null} if the server
   * is not started or does not have the plugin.
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
   * Stops and immediately restarts the server at {@code serverIndex}, then waits for it to
   * catch up with the cluster. Only valid when {@link #persistentRaftStorage()} returns
   * {@code true}; otherwise Raft storage is removed on the next cleanup cycle.
   */
  protected void restartServer(final int serverIndex) {
    if (getServer(serverIndex).isStarted()) {
      LogManager.instance().log(this, Level.INFO, "TEST: Stopping server %d for restart", serverIndex);
      getServer(serverIndex).stop();
    }

    // Brief pause to allow the OS to release the gRPC port
    try {
      Thread.sleep(2_000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }

    LogManager.instance().log(this, Level.INFO, "TEST: Starting server %d again", serverIndex);
    getServer(serverIndex).start();

    waitForReplicationIsCompleted(serverIndex);
    LogManager.instance().log(this, Level.INFO, "TEST: Server %d restarted and caught up", serverIndex);
  }
}
