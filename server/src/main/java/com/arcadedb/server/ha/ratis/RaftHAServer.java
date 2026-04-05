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
import com.arcadedb.database.Binary;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ReplicationCallback;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Manages the Ratis RaftServer lifecycle for ArcadeDB HA. This class:
 * <ul>
 *   <li>Parses ArcadeDB HA configuration into Ratis properties</li>
 *   <li>Builds and starts the RaftServer with ArcadeDBStateMachine</li>
 *   <li>Provides a RaftClient for submitting transactions</li>
 *   <li>Handles quorum configuration (MAJORITY or ALL)</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RaftHAServer {

  /** Quorum modes supported with Ratis. */
  public enum Quorum {
    MAJORITY, ALL;

    public static Quorum parse(final String value) {
      return switch (value.toLowerCase()) {
        case "majority" -> MAJORITY;
        case "all" -> ALL;
        default -> throw new ConfigurationException(
            "Unsupported HA quorum mode '" + value + "'. Only 'majority' and 'all' are supported with Ratis");
      };
    }
  }

  private final ArcadeDBServer              server;
  private final ContextConfiguration       configuration;
  private final RaftGroup                  raftGroup;
  private final RaftPeerId                 localPeerId;
  private final Quorum                     quorum;
  private final long                       quorumTimeout;
  private final Map<String, String>        peerHttpAddresses = new java.util.concurrent.ConcurrentHashMap<>();
  private       String                     clusterToken;

  private RaftServer                       raftServer;
  private RaftClient                       raftClient;
  private RaftProperties                   raftProperties;
  private ArcadeDBStateMachine             stateMachine;
  private ClusterMonitor                   clusterMonitor;
  private java.util.concurrent.ScheduledExecutorService lagMonitorExecutor;

  public RaftHAServer(final ArcadeDBServer server, final ContextConfiguration configuration) {
    this.server = server;
    this.configuration = configuration;
    this.quorum = Quorum.parse(configuration.getValueAsString(GlobalConfiguration.HA_QUORUM));
    this.quorumTimeout = configuration.getValueAsLong(GlobalConfiguration.HA_QUORUM_TIMEOUT);

    // Parse peers from HA_SERVER_LIST
    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    if (serverList == null || serverList.isEmpty())
      throw new ConfigurationException("HA server list (arcadedb.ha.serverList) is required for Ratis HA");

    final List<RaftPeer> peers = parsePeers(serverList);
    this.localPeerId = resolveLocalPeerId(peers);

    // Create Raft group using cluster name as group ID seed
    final String clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    final RaftGroupId groupId = RaftGroupId.valueOf(
        UUID.nameUUIDFromBytes(clusterName.getBytes()));
    this.raftGroup = RaftGroup.valueOf(groupId, peers);

    // Initialize cluster token for inter-node HTTP auth
    initClusterToken();

    // Initialize cluster monitor
    final int lagThreshold = configuration.getValueAsInteger(GlobalConfiguration.HA_REPLICATION_LAG_WARNING);
    this.clusterMonitor = new ClusterMonitor(lagThreshold);
  }

  /**
   * Derives a deterministic cluster token from the cluster name and root password.
   * All nodes in the same cluster compute the same token without sharing state.
   */
  private void initClusterToken() {
    final String configured = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
    if (configured != null && !configured.isEmpty()) {
      this.clusterToken = configured;
      return;
    }
    final String clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    final String rootPassword = configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PASSWORD);
    final String seed = clusterName + ":" + (rootPassword != null ? rootPassword : "");
    this.clusterToken = UUID.nameUUIDFromBytes(seed.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();
    configuration.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, this.clusterToken);
  }

  public void startService() {
    LogManager.instance().log(this, Level.INFO, "Starting Ratis HA service (cluster=%s, peers=%s, quorum=%s)...",
        configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME), raftGroup.getPeers(), quorum);

    try {
      stateMachine = new ArcadeDBStateMachine(server);

      this.raftProperties = buildRaftProperties();
      final RaftProperties properties = this.raftProperties;

      // Use RECOVER if storage exists from a previous run, FORMAT for fresh start
      final Path storagePath = Path.of(server.getRootPath(), "ratis-storage", localPeerId.toString());
      final boolean storageExists = java.nio.file.Files.exists(storagePath)
          && java.nio.file.Files.list(storagePath).findAny().isPresent();

      final var startupOption = storageExists
          ? org.apache.ratis.server.storage.RaftStorage.StartupOption.RECOVER
          : org.apache.ratis.server.storage.RaftStorage.StartupOption.FORMAT;

      HALog.log(this, HALog.BASIC, "Ratis startup: storage=%s, option=%s", storagePath, startupOption);

      raftServer = RaftServer.newBuilder()
          .setServerId(localPeerId)
          .setStateMachine(stateMachine)
          .setProperties(properties)
          .setGroup(raftGroup)
          .setOption(startupOption)
          .build();

      raftServer.start();

      // Create a client for submitting transactions
      raftClient = RaftClient.newBuilder()
          .setRaftGroup(raftGroup)
          .setProperties(properties)
          .build();

      // In K8s mode: if this is a new server (no existing storage) and other servers might already
      // be running, try to add ourselves to the existing cluster via AdminApi.
      // This handles StatefulSet scale-up where new pods need to join the existing Raft group.
      if (!storageExists && configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S))
        tryAutoJoinCluster();

      startLagMonitor();

      LogManager.instance().log(this, Level.INFO, "Ratis HA service started (serverId=%s)", localPeerId);

    } catch (final IOException e) {
      throw new ConfigurationException("Failed to start Ratis HA service", e);
    }
  }

  /**
   * Attempts to join an existing Ratis cluster by contacting a peer and adding this server.
   * Used in Kubernetes when a new pod is added via StatefulSet scale-up.
   * If no existing cluster is found (fresh deployment), this is a no-op.
   */
  private void tryAutoJoinCluster() {
    HALog.log(this, HALog.BASIC, "K8s auto-join: attempting to join existing cluster...");

    // Try each peer to find one that's already running
    for (final RaftPeer peer : raftGroup.getPeers()) {
      if (peer.getId().equals(localPeerId))
        continue;

      try {
        // Create a temporary client targeting this specific peer to check if the cluster exists
        final RaftProperties tempProps = new RaftProperties();
        tempProps.set("raft.server.rpc.type", "GRPC");

        // Build a group with just the target peer to query it
        final RaftGroup targetGroup = RaftGroup.valueOf(raftGroup.getGroupId(), peer);
        try (final RaftClient tempClient = RaftClient.newBuilder()
            .setRaftGroup(targetGroup)
            .setProperties(tempProps)
            .build()) {

          // Try to get group info from the peer - if it responds, the cluster exists
          final var groupInfo = tempClient.getGroupManagementApi(peer.getId()).info(raftGroup.getGroupId());

          if (groupInfo != null && groupInfo.isSuccess()) {
            // Cluster exists. Check if we're already a member.
            final var confOpt = groupInfo.getConf();
            if (confOpt.isPresent()) {
              final var conf = confOpt.get();
              boolean alreadyMember = false;
              for (final var p : conf.getPeersList())
                if (p.getId().toStringUtf8().equals(localPeerId.toString())) {
                  alreadyMember = true;
                  break;
                }

              if (!alreadyMember) {
                HALog.log(this, HALog.BASIC, "K8s auto-join: adding self (%s) to existing cluster via peer %s",
                    localPeerId, peer.getId());

                // Find our peer definition
                RaftPeer localPeer = null;
                for (final RaftPeer p : raftGroup.getPeers())
                  if (p.getId().equals(localPeerId)) { localPeer = p; break; }

                if (localPeer != null) {
                  // Build the new configuration: existing peers + us
                  final java.util.List<RaftPeer> newPeers = new ArrayList<>();
                  for (final var existingPeer : conf.getPeersList()) {
                    final String existingId = existingPeer.getId().toStringUtf8();
                    for (final RaftPeer p : raftGroup.getPeers())
                      if (p.getId().toString().equals(existingId)) {
                        newPeers.add(p);
                        break;
                      }
                  }
                  newPeers.add(localPeer);

                  tempClient.admin().setConfiguration(newPeers);
                  HALog.log(this, HALog.BASIC, "K8s auto-join: successfully joined cluster with %d peers", newPeers.size());
                }
              } else {
                HALog.log(this, HALog.BASIC, "K8s auto-join: already a member of the cluster");
              }
            }
            return;
          }
        }
      } catch (final Exception e) {
        HALog.log(this, HALog.DETAILED, "K8s auto-join: peer %s not reachable (%s), trying next...",
            peer.getId(), e.getMessage());
      }
    }

    HALog.log(this, HALog.BASIC, "K8s auto-join: no existing cluster found, starting as new cluster");
  }

  public void stopService() {
    LogManager.instance().log(this, Level.INFO, "Stopping Ratis HA service...");

    stopLagMonitor();

    // In K8s mode, automatically remove this peer from the Raft cluster before stopping.
    // This ensures clean scale-down without orphaned peers in the cluster configuration.
    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S))
      leaveCluster();

    try {
      if (raftClient != null) {
        raftClient.close();
        raftClient = null;
      }
      if (raftServer != null) {
        raftServer.close();
        raftServer = null;
      }
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Error stopping Ratis HA service", e);
    }
  }

  /**
   * Gracefully removes this server from the Raft cluster. If this server is the leader,
   * transfers leadership to another peer first. Then contacts the cluster to remove this peer
   * from the configuration.
   * <p>
   * This is best-effort: errors are logged but don't prevent shutdown.
   */
  public void leaveCluster() {
    if (raftServer == null || raftClient == null)
      return;

    try {
      final int peerCount = raftGroup.getPeers().size();
      if (peerCount <= 1) {
        HALog.log(this, HALog.BASIC, "Single-node cluster, skipping leave");
        return;
      }

      // If we're the leader, transfer leadership first
      if (isLeader()) {
        for (final RaftPeer peer : raftGroup.getPeers()) {
          if (!peer.getId().equals(localPeerId)) {
            HALog.log(this, HALog.BASIC, "Leaving cluster: transferring leadership to %s before removal", peer.getId());
            try {
              transferLeadership(peer.getId().toString(), 10_000);
              // Wait briefly for the transfer to take effect
              Thread.sleep(1000);
            } catch (final Exception e) {
              HALog.log(this, HALog.BASIC, "Leadership transfer failed (%s), proceeding with removal", e.getMessage());
            }
            break;
          }
        }
      }

      // Remove self from the cluster configuration
      HALog.log(this, HALog.BASIC, "Leaving cluster: removing self (%s) from Raft group", localPeerId);
      removePeer(localPeerId.toString());
      HALog.log(this, HALog.BASIC, "Successfully left the Raft cluster");

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Failed to leave cluster gracefully: %s", e.getMessage());
    }
  }

  // -- Transaction Submission --

  /**
   * Submits a transaction to the Raft cluster. The entry is replicated to all nodes and applied
   * via ArcadeDBStateMachine.applyTransaction() on each node.
   *
   * @param databaseName      target database
   * @param bucketRecordDelta per-bucket record count changes
   * @param walBuffer         WAL changes buffer from commit1stPhase
   * @param schemaJson        schema JSON (null if no schema change)
   * @param filesToAdd        files to add (null if no structural change)
   * @param filesToRemove     files to remove (null if no structural change)
   */
  public void replicateTransaction(final String databaseName, final Map<Integer, Integer> bucketRecordDelta,
      final Binary walBuffer, final String schemaJson, final Map<Integer, String> filesToAdd,
      final Map<Integer, String> filesToRemove) {

    final byte[] entry = RaftLogEntry.serializeTransaction(databaseName, bucketRecordDelta, walBuffer, schemaJson, filesToAdd,
        filesToRemove);

    HALog.log(this, HALog.TRACE, "replicateTransaction: db=%s, entrySize=%d bytes", databaseName, entry.length);
    sendToRaft(entry);
  }

  /**
   * Forwards a transaction from a non-leader node through the Raft cluster.
   *
   * @param databaseName      target database
   * @param bucketRecordDelta per-bucket record count changes
   * @param walBuffer         WAL changes buffer
   * @param indexChanges      serialized index key changes for constraint validation, or null
   */
  public void forwardTransaction(final String databaseName, final Map<Integer, Integer> bucketRecordDelta,
      final Binary walBuffer, final byte[] indexChanges) {

    final byte[] entry = RaftLogEntry.serializeTransactionForward(databaseName, bucketRecordDelta, walBuffer, indexChanges);

    sendToRaft(entry);
  }

  /**
   * Forwards a command (SQL/Cypher/etc.) to the leader for execution via the Ratis state machine query() path.
   * This does NOT go through the Raft log - the leader executes the command directly and returns the result.
   *
   * @return binary result bytes (deserialize with RaftLogEntry.deserializeCommandResult)
   */
  public byte[] forwardCommand(final String databaseName, final String language, final String command,
      final Map<String, Object> namedParams, final Object[] positionalParams) {
    final byte[] request = RaftLogEntry.serializeCommandForward(databaseName, language, command, namedParams, positionalParams);

    // sendReadOnly with explicit leader peer ID. This executes via query() on the leader
    // and does NOT create a log entry, avoiding deadlock with WAL replication inside the command.
    final var msg = Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(request));
    for (int retry = 0; retry < 3; retry++) {
      try {
        // Get the current leader and send directly to it
        final String leaderName = getLeaderName();
        final RaftClientReply reply;
        if (leaderName != null)
          reply = raftClient.io().sendReadOnly(msg, RaftPeerId.valueOf(leaderName));
        else
          reply = raftClient.io().sendReadOnly(msg);

        if (!reply.isSuccess()) {
          final String err = reply.getException() != null ? reply.getException().getMessage() : "unknown";
          throw new com.arcadedb.exception.CommandExecutionException("Command forwarding failed: " + err);
        }

        return reply.getMessage().getContent().toByteArray();

      } catch (final IOException e) {
        if (e.getMessage() != null && e.getMessage().contains("NotLeaderException") && retry < 2) {
          try { Thread.sleep(500); } catch (final InterruptedException ie) { Thread.currentThread().interrupt(); }
          continue;
        }
        throw new com.arcadedb.exception.CommandExecutionException("Command forwarding failed: " + e.getMessage());
      }
    }
    throw new com.arcadedb.exception.CommandExecutionException("Command forwarding failed after retries");
  }

  private void sendToRaft(final byte[] entry) {
    try {
      LogManager.instance().log(this, Level.FINE, "Sending %d bytes to Raft cluster (isLeader=%s)...", entry.length, isLeader());

      // Use async send with timeout to avoid hanging indefinitely
      final var future = raftClient.async().send(Message.valueOf(ByteString.copyFrom(entry)));
      final RaftClientReply reply = future.get(quorumTimeout, TimeUnit.MILLISECONDS);

      if (!reply.isSuccess())
        throw new QuorumNotReachedException(
            "Raft replication failed: " + (reply.getException() != null ? reply.getException().getMessage() : "unknown error"));

      // For ALL quorum: after MAJORITY commit, wait for ALL replicas to apply the entry
      if (quorum == Quorum.ALL) {
        final long logIndex = reply.getLogIndex();
        final RaftClientReply watchReply = raftClient.io().watch(logIndex, RaftProtos.ReplicationLevel.ALL_COMMITTED);
        if (!watchReply.isSuccess())
          throw new QuorumNotReachedException("Raft ALL quorum not reached: not all replicas acknowledged the entry");
      }

    } catch (final java.util.concurrent.TimeoutException e) {
      throw new QuorumNotReachedException("Raft replication timed out after " + quorumTimeout + "ms");
    } catch (final java.util.concurrent.ExecutionException e) {
      throw new QuorumNotReachedException("Raft replication failed: " + e.getCause().getMessage());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new QuorumNotReachedException("Raft replication interrupted");
    } catch (final IOException e) {
      throw new QuorumNotReachedException("Failed to submit transaction to Raft cluster: " + e.getMessage());
    }
  }

  // -- Status --

  /**
   * Waits until the local state machine has applied at least the specified index.
   * Used for READ_YOUR_WRITES consistency: the client sends its last known commit index (bookmark)
   * and the follower waits until it has applied up to that point before executing a read.
   */
  public void waitForAppliedIndex(final long targetIndex) {
    if (targetIndex <= 0)
      return;
    try {
      final long deadline = System.currentTimeMillis() + quorumTimeout;
      while (System.currentTimeMillis() < deadline) {
        if (getLastAppliedIndex() >= targetIndex) {
          HALog.log(this, HALog.TRACE, "Bookmark wait complete: applied >= target=%d", targetIndex);
          return;
        }
        Thread.sleep(10);
      }
      HALog.log(this, HALog.DETAILED, "waitForAppliedIndex timed out: applied=%d < target=%d",
          getLastAppliedIndex(), targetIndex);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Waits until the local state machine has applied all committed entries.
   * Used for LINEARIZABLE consistency: contacts the leader to get the current commit index,
   * then waits for the local state machine to catch up.
   */
  public void waitForLocalApply() {
    try {
      final long commitIndex = getCommitIndex();
      if (commitIndex <= 0)
        return;

      // Poll until local apply catches up to the commit index
      final long deadline = System.currentTimeMillis() + quorumTimeout;
      while (System.currentTimeMillis() < deadline) {
        final long applied = getLastAppliedIndex();
        if (applied >= commitIndex) {
          HALog.log(this, HALog.TRACE, "Local apply caught up: applied=%d >= commit=%d", applied, commitIndex);
          return;
        }
        Thread.sleep(10);
      }
      HALog.log(this, HALog.DETAILED, "waitForLocalApply timed out: applied=%d < commit=%d", getLastAppliedIndex(), commitIndex);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final Exception e) {
      HALog.log(this, HALog.DETAILED, "waitForLocalApply failed: %s", e.getMessage());
    }
  }

  public boolean isLeader() {
    if (raftServer == null)
      return false;
    try {
      final var divisionInfo = raftServer.getDivision(raftGroup.getGroupId());
      return divisionInfo.getInfo().isLeader();
    } catch (final IOException e) {
      return false;
    }
  }

  public String getLeaderName() {
    if (raftServer == null)
      return null;
    try {
      final var divisionInfo = raftServer.getDivision(raftGroup.getGroupId());
      final RaftPeerId leaderId = divisionInfo.getInfo().getLeaderId();
      return leaderId != null ? leaderId.toString() : null;
    } catch (final IOException e) {
      return null;
    }
  }

  public long getCurrentTerm() {
    if (raftServer == null)
      return -1;
    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getInfo().getCurrentTerm();
    } catch (final IOException e) {
      return -1;
    }
  }

  public long getCommitIndex() {
    if (raftServer == null)
      return -1;
    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getRaftLog().getLastCommittedIndex();
    } catch (final IOException e) {
      return -1;
    }
  }

  public long getLastAppliedIndex() {
    if (raftServer == null)
      return -1;
    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getInfo().getLastAppliedIndex();
    } catch (final IOException e) {
      return -1;
    }
  }

  /**
   * Returns per-follower replication state (only available on the leader).
   * Each entry maps a peer ID to {matchIndex, nextIndex}.
   */
  public java.util.List<java.util.Map<String, Object>> getFollowerStates() {
    if (raftServer == null || !isLeader())
      return java.util.List.of();
    try {
      final var division = raftServer.getDivision(raftGroup.getGroupId());
      final var info = division.getInfo();
      final long[] matchIndices = info.getFollowerMatchIndices();
      final long[] nextIndices = info.getFollowerNextIndices();

      // The indices arrays correspond to followers in the order returned by the group's peers (excluding self)
      final java.util.List<java.util.Map<String, Object>> result = new ArrayList<>();
      int idx = 0;
      for (final RaftPeer peer : raftGroup.getPeers()) {
        if (peer.getId().equals(localPeerId))
          continue;
        final java.util.Map<String, Object> state = new java.util.LinkedHashMap<>();
        state.put("peerId", peer.getId().toString());
        state.put("matchIndex", idx < matchIndices.length ? matchIndices[idx] : -1);
        state.put("nextIndex", idx < nextIndices.length ? nextIndices[idx] : -1);
        result.add(state);
        idx++;
      }
      return result;
    } catch (final IOException e) {
      return java.util.List.of();
    }
  }

  public ArcadeDBServer getServer() {
    return server;
  }

  public String getServerName() {
    return server.getServerName();
  }

  public Quorum getQuorum() {
    return quorum;
  }

  public String getClusterName() {
    return configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
  }

  public int getConfiguredServers() {
    return raftGroup.getPeers().size();
  }

  public String getElectionStatus() {
    if (raftServer == null)
      return "UNKNOWN";
    try {
      final var info = raftServer.getDivision(raftGroup.getGroupId()).getInfo();
      if (info.isLeader())
        return "LEADER";
      if (info.getLeaderId() != null)
        return "FOLLOWER";
      return "ELECTING";
    } catch (final IOException e) {
      return "UNKNOWN";
    }
  }

  /**
   * Returns a comma-separated list of HTTP addresses for replica peers (excluding the local peer).
   */
  public String getReplicaAddresses() {
    final StringBuilder sb = new StringBuilder();
    for (final RaftPeer peer : raftGroup.getPeers()) {
      if (peer.getId().equals(localPeerId))
        continue;
      if (!sb.isEmpty())
        sb.append(",");
      sb.append(getPeerHTTPAddress(peer.getId()));
    }
    return sb.toString();
  }

  /**
   * Returns 0 - Ratis manages its own replication queue internally.
   * Provided for compatibility with test infrastructure.
   */
  public int getMessagesInQueue() {
    return 0;
  }

  /**
   * Returns the number of online replicas (peers - 1, since Ratis manages all peers as online).
   * Provided for compatibility with test infrastructure.
   */
  public int getOnlineReplicas() {
    return raftGroup.getPeers().size() - 1;
  }

  public RaftGroup getRaftGroup() {
    return raftGroup;
  }

  public RaftPeerId getLocalPeerId() {
    return localPeerId;
  }

  public long getElectionCount() {
    return stateMachine != null ? stateMachine.getElectionCount() : 0;
  }

  public long getLastElectionTime() {
    return stateMachine != null ? stateMachine.getLastElectionTime() : 0;
  }

  public long getStartTime() {
    return stateMachine != null ? stateMachine.getStartTime() : 0;
  }

  public long getRaftLogSize() {
    if (raftServer == null)
      return -1;
    try {
      final var log = raftServer.getDivision(raftGroup.getGroupId()).getRaftLog();
      return log.getLastCommittedIndex() - log.getStartIndex() + 1;
    } catch (final IOException e) {
      return -1;
    }
  }

  public RaftServer getRaftServer() {
    return raftServer;
  }

  // -- gRPC Channel Refresh --

  /**
   * Closes the current RaftClient and creates a new one with fresh gRPC channels.
   * After a network partition, gRPC channels enter TRANSIENT_FAILURE with exponential backoff.
   * Recreating the client forces new channel creation and immediate DNS re-resolution.
   */
  public synchronized void refreshRaftClient() {
    if (raftProperties == null)
      return;
    if (raftClient != null) {
      try {
        raftClient.close();
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING, "Error closing stale RaftClient during refresh", e);
      }
    }
    raftClient = RaftClient.newBuilder()
        .setRaftGroup(raftGroup)
        .setProperties(raftProperties)
        .build();
    HALog.log(this, HALog.BASIC, "RaftClient refreshed with fresh gRPC channels after leader change");
  }

  // -- Cluster Token --

  public String getClusterToken() {
    return clusterToken;
  }

  // -- Lag Monitor --

  private void startLagMonitor() {
    if (clusterMonitor.getLagWarningThreshold() <= 0)
      return;
    lagMonitorExecutor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "arcadedb-raft-lag-monitor");
      t.setDaemon(true);
      return t;
    });
    lagMonitorExecutor.scheduleAtFixedRate(this::checkReplicaLag, 5, 5, java.util.concurrent.TimeUnit.SECONDS);
  }

  private void stopLagMonitor() {
    if (lagMonitorExecutor != null) {
      lagMonitorExecutor.shutdownNow();
      lagMonitorExecutor = null;
    }
  }

  private void checkReplicaLag() {
    try {
      if (!isLeader())
        return;
      clusterMonitor.updateLeaderCommitIndex(getCommitIndex());
      for (final var fs : getFollowerStates())
        clusterMonitor.updateReplicaMatchIndex((String) fs.get("peerId"), (Long) fs.get("matchIndex"));
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Error checking replica lag", e);
    }
  }

  public ClusterMonitor getClusterMonitor() {
    return clusterMonitor;
  }

  // -- Dynamic Membership --

  /**
   * Adds a new peer to the Raft cluster. Must be called on any server (Ratis routes to leader).
   * The new peer must already be running with the same cluster name and group ID.
   *
   * @param peerId  the new peer's ID (typically host:port)
   * @param address the new peer's Raft RPC address (host:port)
   */
  public void addPeer(final String peerId, final String address) {
    final RaftPeer newPeer = RaftPeer.newBuilder()
        .setId(RaftPeerId.valueOf(peerId))
        .setAddress(address)
        .build();

    final List<RaftPeer> newPeers = new ArrayList<>(raftGroup.getPeers());
    newPeers.add(newPeer);

    try {
      final RaftClientReply reply = raftClient.admin().setConfiguration(newPeers);
      if (!reply.isSuccess())
        throw new ConfigurationException("Failed to add peer " + peerId + ": " + reply.getException());
      LogManager.instance().log(this, Level.INFO, "Peer %s added to Raft cluster", peerId);
    } catch (final IOException e) {
      throw new ConfigurationException("Failed to add peer " + peerId, e);
    }
  }

  /**
   * Removes a peer from the Raft cluster. Must be called on any server (Ratis routes to leader).
   * The removed peer will step down automatically.
   *
   * @param peerId the peer ID to remove
   */
  public void removePeer(final String peerId) {
    final List<RaftPeer> newPeers = new ArrayList<>();
    for (final RaftPeer peer : raftGroup.getPeers())
      if (!peer.getId().toString().equals(peerId))
        newPeers.add(peer);

    if (newPeers.size() == raftGroup.getPeers().size())
      throw new ConfigurationException("Peer " + peerId + " not found in cluster");

    try {
      final RaftClientReply reply = raftClient.admin().setConfiguration(newPeers);
      if (!reply.isSuccess())
        throw new ConfigurationException("Failed to remove peer " + peerId + ": " + reply.getException());
      LogManager.instance().log(this, Level.INFO, "Peer %s removed from Raft cluster", peerId);
    } catch (final IOException e) {
      throw new ConfigurationException("Failed to remove peer " + peerId, e);
    }
  }

  /**
   * Transfers leadership to the specified peer.
   *
   * @param targetPeerId the target peer to become leader
   * @param timeoutMs    timeout in milliseconds
   */
  public void transferLeadership(final String targetPeerId, final long timeoutMs) {
    try {
      final RaftClientReply reply = raftClient.admin().transferLeadership(
          RaftPeerId.valueOf(targetPeerId), timeoutMs);
      if (!reply.isSuccess())
        throw new ConfigurationException("Failed to transfer leadership to " + targetPeerId + ": " + reply.getException());
      LogManager.instance().log(this, Level.INFO, "Leadership transferred to %s", targetPeerId);
    } catch (final IOException e) {
      throw new ConfigurationException("Failed to transfer leadership to " + targetPeerId, e);
    }
  }

  // -- Snapshot --

  /**
   * Returns the HTTP address of a peer given its Raft peer ID.
   */
  public String getPeerHTTPAddress(final RaftPeerId peerId) {
    final String httpAddr = peerHttpAddresses.get(peerId.toString());
    return httpAddr != null ? httpAddr : peerId.toString();
  }

  /**
   * Returns the HTTP address for the current leader.
   */
  public String getLeaderHTTPAddress() {
    final String leaderName = getLeaderName();
    if (leaderName == null)
      return null;
    return getPeerHTTPAddress(RaftPeerId.valueOf(leaderName));
  }

  // -- Configuration --

  private RaftProperties buildRaftProperties() {
    final RaftProperties properties = new RaftProperties();

    // Storage directory
    final Path storagePath = Path.of(server.getRootPath(), "ratis-storage", localPeerId.toString());
    try {
      Files.createDirectories(storagePath);
    } catch (final IOException e) {
      throw new ConfigurationException("Cannot create Ratis storage directory: " + storagePath, e);
    }
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storagePath.toFile()));

    // gRPC transport
    final int port = resolveLocalPort();
    GrpcConfigKeys.Server.setPort(properties, port);

    // RPC factory
    properties.set("raft.server.rpc.type", "GRPC");

    // Timeouts
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(1500, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(3000, TimeUnit.MILLISECONDS));

    // Snapshot: notification mode (ArcadeDB handles the actual transfer)
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false);
    // Auto-trigger snapshots after 100K entries
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, 100000L);

    // Log segment size (64MB, matching current ReplicationLogFile chunk size)
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf("64MB"));

    // Write buffer (must be >= appender buffer byte-limit + 8)
    RaftServerConfigKeys.Log.setWriteBufferSize(properties, SizeInBytes.valueOf("8MB"));

    // Leader lease: enables consistent reads from the leader without a round-trip to followers.
    // The leader can serve reads as long as its lease hasn't expired (based on heartbeat responses).
    RaftServerConfigKeys.Read.setLeaderLeaseEnabled(properties, true);
    RaftServerConfigKeys.Read.setLeaderLeaseTimeoutRatio(properties, 0.9);
    RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);

    // Note: Ratis uses MAJORITY consensus by default.
    // For ALL quorum mode, we use the Watch API after each write to wait for ALL replicas.
    // See sendToRaft() for the ALL quorum implementation.

    return properties;
  }

  // -- Peer Parsing --

  /**
   * Parses the HA_SERVER_LIST into RaftPeer instances.
   * Supported formats:
   * <ul>
   *   <li>"host1:raftPort,host2:raftPort" - HTTP address derived from raft host + configured HTTP port offset</li>
   *   <li>"host1:raftPort:httpPort,host2:raftPort:httpPort" - explicit HTTP port per peer</li>
   * </ul>
   * The peer ID is derived from the host:raftPort string for deterministic identification.
   */
  private List<RaftPeer> parsePeers(final String serverList) {
    final List<RaftPeer> peers = new ArrayList<>();
    final String[] entries = serverList.split(",");

    // Get the local HTTP port to compute offset for deriving HTTP addresses
    final int localHttpPort = parseFirstPort(
        configuration.getValueAsString(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT));
    final int localRaftPort = parseFirstPort(
        configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS));
    final int httpPortOffset = localHttpPort - localRaftPort;

    for (final String entry : entries) {
      final String trimmed = entry.trim();
      if (trimmed.isEmpty())
        continue;

      final String[] parts = trimmed.split(":");
      final String host = parts[0];
      final int raftPort = Integer.parseInt(parts[1]);
      final String raftAddress = host + ":" + raftPort;

      // Determine HTTP address
      final String httpAddress;
      if (parts.length >= 3)
        httpAddress = host + ":" + parts[2];
      else
        httpAddress = host + ":" + (raftPort + httpPortOffset);

      // Use underscore in peer ID to avoid JMX ObjectName issues (colon is invalid in JMX values)
      final String peerIdStr = host + "_" + raftPort;
      final RaftPeerId peerId = RaftPeerId.valueOf(peerIdStr);
      final RaftPeer peer = RaftPeer.newBuilder()
          .setId(peerId)
          .setAddress(raftAddress)
          .build();
      peers.add(peer);

      // Store HTTP address mapping separately (NOT on RaftPeer.clientAddress which Ratis uses for gRPC)
      peerHttpAddresses.put(peerIdStr, httpAddress);
    }

    if (peers.size() < 3)
      LogManager.instance().log(this, Level.WARNING,
          "Ratis HA cluster has less than 3 peers (%d). A minimum of 3 is recommended for fault tolerance", peers.size());

    return peers;
  }

  /**
   * Resolves which peer in the list corresponds to this server instance.
   * Matches by the configured server address (HA_REPLICATION_INCOMING_HOST:port).
   */
  private RaftPeerId resolveLocalPeerId(final List<RaftPeer> peers) {
    final String localHost = configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST);
    final String localPorts = configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS);
    final int localPort = parseFirstPort(localPorts);

    // Peer ID format is host_port (underscore, not colon, for JMX compatibility)
    final String localPeerId = localHost + "_" + localPort;
    for (final RaftPeer peer : peers)
      if (peer.getId().toString().equals(localPeerId))
        return peer.getId();

    // Fallback: match by port only (useful for localhost testing)
    for (final RaftPeer peer : peers) {
      final String address = peer.getAddress();
      if (address != null && address.endsWith(":" + localPort))
        return peer.getId();
    }

    throw new ConfigurationException(
        "Cannot find local server in HA_SERVER_LIST. Local address: " + localHost + ":" + localPort + ", server list: " + peers);
  }

  private int resolveLocalPort() {
    final String ports = configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS);
    return parseFirstPort(ports);
  }

  private static int parseFirstPort(final String portSpec) {
    if (portSpec.contains("-"))
      return Integer.parseInt(portSpec.split("-")[0].trim());
    if (portSpec.contains(","))
      return Integer.parseInt(portSpec.split(",")[0].trim());
    return Integer.parseInt(portSpec.trim());
  }
}
