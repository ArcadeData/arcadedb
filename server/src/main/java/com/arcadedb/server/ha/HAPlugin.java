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
package com.arcadedb.server.ha;

import com.arcadedb.database.Binary;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerPlugin;

import java.util.Map;

/**
 * Interface for HA (High Availability) plugins. The server core depends only on this interface,
 * while the concrete Ratis-based implementation lives in the separate ha-raft module.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface HAPlugin extends ServerPlugin {

  boolean isLeader();

  String getClusterToken();

  long getCommitIndex();

  String getLeaderHTTPAddress();

  String getLeaderName();

  String getElectionStatus();

  String getClusterName();

  int getConfiguredServers();

  String getReplicaAddresses();

  String getServerName();

  long getLastAppliedIndex();

  /**
   * Builds a full JSON representation of the cluster status for the GET /api/v1/server endpoint.
   */
  JSONObject exportClusterStatus();

  /**
   * Replicates database creation to all cluster nodes.
   */
  void replicateCreateDatabase(String databaseName);

  /**
   * Replicates database drop to all cluster nodes.
   */
  void replicateDropDatabase(String databaseName);

  /**
   * Replicates a transaction (WAL + schema changes) to all cluster nodes.
   */
  default void replicateTransaction(String databaseName, Map<Integer, Integer> bucketRecordDelta,
      Binary walBuffer, String schemaJson, Map<Integer, String> filesToAdd, Map<Integer, String> filesToRemove) {
  }

  /**
   * Returns true if this node is the leader and has finished applying all committed entries from
   * the previous term. During the brief window after election this returns false.
   */
  default boolean isLeaderReady() {
    return true;
  }

  /**
   * If this node is the leader but not yet ready, blocks until ready or quorum timeout expires.
   */
  default void waitForLeaderReady() {
  }

  /**
   * Waits until the local state machine has applied at least {@code targetIndex}.
   * Used for READ_YOUR_WRITES consistency.
   */
  default void waitForAppliedIndex(long targetIndex) {
  }

  /**
   * Ensures this leader is still the legitimate leader before serving a linearizable read.
   * Uses the Raft read-index protocol (Section 6.4 of the Raft paper).
   */
  default void ensureLinearizableRead() {
  }

  /**
   * Waits until the local state machine has applied all currently committed entries.
   * Used as a leader read barrier and for READ_YOUR_WRITES on followers without a bookmark.
   */
  default void waitForLocalApply() {
  }

  /**
   * Initiates a graceful leadership step-down by transferring to another available peer.
   */
  default void stepDown() {
  }

  /**
   * Gracefully removes this server from the Raft cluster, transferring leadership first if needed.
   */
  default void leaveCluster() {
  }

  /**
   * Adds a new peer to the cluster at runtime.
   */
  default void addPeer(String peerId, String raftAddress, String httpAddress) {
  }

  /**
   * Removes a peer from the cluster at runtime.
   */
  default void removePeer(String peerId) {
  }

  /**
   * Transfers leadership to the specified peer within the given timeout.
   */
  default void transferLeadership(String targetPeerId, long timeoutMs) {
  }

  /**
   * Called during server startup before databases are loaded. Allows the HA implementation to
   * recover from crash-related state (e.g. pending snapshot swaps).
   */
  default void recoverBeforeDatabaseLoad(final java.nio.file.Path databaseDirectory) {
  }
}
