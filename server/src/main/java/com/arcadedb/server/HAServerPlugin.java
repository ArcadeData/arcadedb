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
package com.arcadedb.server;

import java.util.List;
import java.util.Map;

/**
 * Public interface for the High Availability server plugin. Consumed by HTTP handlers,
 * MCP tools, backup tasks, and test utilities. The single production implementation
 * is {@code RaftHAPlugin} in the ha-raft module.
 */
public interface HAServerPlugin extends ServerPlugin {

  enum QUORUM {
    NONE, ONE, TWO, THREE, MAJORITY, ALL;

    public int quorum(final int numberOfServers) {
      return switch (this) {
        case NONE -> 0;
        case ONE -> 1;
        case TWO -> 2;
        case THREE -> 3;
        case MAJORITY -> numberOfServers / 2 + 1;
        case ALL -> numberOfServers;
      };
    }
  }

  enum ELECTION_STATUS {
    DONE, VOTING_FOR_ME, VOTING_FOR_OTHERS, LEADER_WAITING_FOR_QUORUM
  }

  enum SERVER_ROLE {
    ANY, REPLICA
  }

  /**
   * Consensus-level readiness signal for the readiness probe. {@code READY}: a leader is known, this node
   * is in the current configuration and (as a follower) has caught up. {@code NOT_READY}: one of those
   * conditions does not hold. A {@code null} return from {@link #getReadinessSignal(long)} means the HA
   * implementation exposes no such signal and the probe applies no additional gating.
   */
  enum READINESS_SIGNAL {
    READY, NOT_READY
  }

  boolean isLeader();

  String getLeaderName();

  ELECTION_STATUS getElectionStatus();

  /**
   * Reports the consensus-level readiness of this node for the readiness probe. {@code READY} requires a
   * known leader (election settled), membership in the current cluster configuration, and - for a
   * follower - a local applied index within {@code maxLagEntries} of the commit index. The leader is always
   * considered caught up with itself.
   * <p>
   * Used so a node does not advertise Ready before it has (re)joined the configuration and replayed the
   * committed log. During a Kubernetes StatefulSet rolling restart, a follower that reports Ready with an
   * empty/lagging log lets the orchestrator terminate the next pod and drop the write quorum.
   * <p>
   * Returns {@code null} when this HA implementation provides no consensus readiness signal; callers treat
   * {@code null} as "no additional gating". The Raft implementation returns a concrete
   * {@link READINESS_SIGNAL}.
   *
   * @param maxLagEntries maximum tolerated {@code commitIndex - lastAppliedIndex} for a follower to be ready
   */
  default READINESS_SIGNAL getReadinessSignal(final long maxLagEntries) {
    return null;
  }

  String getClusterName();

  Map<String, Object> getStats();

  int getConfiguredServers();

  /**
   * Returns the cluster token used for inter-node request authentication.
   * May be explicitly configured or auto-derived from cluster name and root password.
   * Returns null when HA is not active or the token is not yet initialized.
   */
  default String getClusterToken() {
    return null;
  }

  /**
   * Returns the HTTP address (host:port) of the current leader, or null if unknown.
   */
  String getLeaderAddress();

  /**
   * Returns a comma-separated list of replica HTTP addresses, or empty string if none.
   */
  String getReplicaAddresses();

  /**
   * Immutable snapshot of the Bolt routing topology: the current leader's client-reachable Bolt address
   * (writer) and the non-leader replicas' Bolt addresses (readers). Both sets are derived from a single
   * leader read so a concurrent leader change cannot make them mutually inconsistent.
   */
  record BoltRoutingTable(String writer, List<String> readers) {
  }

  /**
   * Returns a single-snapshot Bolt routing table for the ROUTE response, or null when HA is inactive,
   * no leader is currently known, or the leader has no resolvable Bolt address. Readers reflect the
   * configured cluster membership (parity with {@link #getReplicaAddresses()}); a down or partitioned
   * follower is still advertised until it leaves the group, and the driver fails over. Used to build the
   * Bolt ROUTE routing table.
   */
  default BoltRoutingTable getBoltRoutingTable() {
    return null;
  }

  /**
   * Sends a shutdown command to a remote server in the cluster.
   */
  void shutdownRemoteServer(String serverName);

  /**
   * Disconnects this node from the cluster (closes Raft server and client).
   */
  void disconnectCluster();

  /**
   * Adds a new peer to the cluster at runtime.
   */
  default void addPeer(final String peerId, final String address) {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }

  /**
   * Adds a new peer to the cluster at runtime with an optional human-readable name used for logs
   * and Studio. Default implementation ignores the name and delegates to {@link #addPeer(String, String)}.
   */
  default void addPeer(final String peerId, final String address, final String name) {
    addPeer(peerId, address);
  }

  /**
   * Removes a peer from the cluster at runtime.
   */
  default void removePeer(final String peerId) {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }

  /**
   * Removes a peer from the cluster at runtime. When {@code force} is false the implementation refuses
   * a removal that would drop the cluster below its voting quorum (issue #4796); pass {@code force=true}
   * to override for an intentional scale-down.
   */
  default void removePeer(final String peerId, final boolean force) {
    removePeer(peerId);
  }

  /**
   * Transfers leadership to the specified peer.
   */
  default void transferLeadership(final String targetPeerId, final long timeoutMs) {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }

  /**
   * Steps down from leadership, transferring to any available peer.
   */
  default void stepDown() {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }

  /**
   * Gracefully leaves the cluster, transferring leadership first if this node is leader.
   */
  default void leaveCluster() {
    throw new UnsupportedOperationException("Dynamic membership not supported by this HA implementation");
  }

  /**
   * Gracefully leaves the cluster. When {@code force} is false the implementation refuses to leave if
   * doing so would drop the cluster below its voting quorum (issue #4796); pass {@code force=true} to
   * override for an intentional scale-down.
   */
  default void leaveCluster(final boolean force) {
    leaveCluster();
  }

  /**
   * Ensures linearizable read consistency on the leader by confirming the leader lease
   * via ReadIndex RPC and waiting for the local state machine to catch up.
   */
  default void ensureLinearizableRead() {
    throw new UnsupportedOperationException("Linearizable reads not supported by this HA implementation");
  }

  /**
   * Ensures linearizable read consistency on a follower by contacting the leader via
   * ReadIndex RPC to obtain the current commit index and waiting for local apply.
   */
  default void ensureLinearizableFollowerRead() {
    throw new UnsupportedOperationException("Linearizable reads not supported by this HA implementation");
  }

  /**
   * Replicates the full server-users.jsonl content across the cluster.
   * Called by {@code PostServerCommandHandler.createUser} and {@code dropUser},
   * and by {@code PostAddPeerHandler} to seed newly-joined peers. Default is a
   * no-op for non-HA setups; the Raft implementation submits a SECURITY_USERS_ENTRY
   * via the group committer.
   *
   * @param usersJsonArray a JSON array string representing the full current users list
   */
  default void replicateSecurityUsers(final String usersJsonArray) {
    // No-op by default; Raft implementation overrides.
  }
}
