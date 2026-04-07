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
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.stream.Stream;

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
  private final Map<String, String>        peerHttpAddresses = new ConcurrentHashMap<>();
  private volatile String                  clusterToken;

  private RaftServer                       raftServer;
  private volatile RaftClient              raftClient;
  private RaftProperties                   raftProperties;
  private ArcadeDBStateMachine             stateMachine;
  private ClusterMonitor                   clusterMonitor;
  private RaftGroupCommitter               groupCommitter;
  private final Object                     applyNotifier          = new Object();
  private final Object                     leaderChangeNotifier   = new Object();
  private ScheduledExecutorService         lagMonitorExecutor;

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

    // If this node is configured as a replica, set its priority to 0 to prevent leader election.
    // Priority 0 tells Ratis this peer should never become leader.
    final String serverRole = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_ROLE);
    if ("replica".equalsIgnoreCase(serverRole)) {
      for (int i = 0; i < peers.size(); i++) {
        if (peers.get(i).getId().equals(localPeerId)) {
          peers.set(i, RaftPeer.newBuilder()
              .setId(localPeerId)
              .setAddress(peers.get(i).getAddress())
              .setPriority(0)
              .build());
          LogManager.instance().log(this, Level.INFO,
              "Node configured as replica (priority=0, will not become leader): %s", localPeerId);
          break;
        }
      }
    }

    // Create Raft group using cluster name as group ID seed
    final String clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    final RaftGroupId groupId = RaftGroupId.valueOf(
        UUID.nameUUIDFromBytes(clusterName.getBytes(StandardCharsets.UTF_8)));
    this.raftGroup = RaftGroup.valueOf(groupId, peers);

    // Initialize cluster monitor
    final int lagThreshold = configuration.getValueAsInteger(GlobalConfiguration.HA_REPLICATION_LAG_WARNING);
    this.clusterMonitor = new ClusterMonitor(lagThreshold);
  }

  /**
   * Derives a deterministic cluster token from the cluster name and root password using PBKDF2.
   * All nodes in the same cluster compute the same token without sharing state.
   * PBKDF2 is used instead of plain SHA-256 to resist brute-force attacks if the token is captured.
   */
  private synchronized void initClusterToken() {
    if (clusterToken != null)
      return;
    final String configured = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
    if (configured != null && !configured.isEmpty()) {
      this.clusterToken = configured;
      return;
    }
    final String clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    final String rootPassword = configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PASSWORD);
    final String password = clusterName + ":" + (rootPassword != null ? rootPassword : "");
    try {
      final byte[] salt = ("arcadedb-cluster-token:" + clusterName).getBytes(StandardCharsets.UTF_8);
      final SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
      final PBEKeySpec spec = new PBEKeySpec(
          password.toCharArray(), salt, 100_000, 256);
      final byte[] hash = factory.generateSecret(spec).getEncoded();
      spec.clearPassword();
      this.clusterToken = HexFormat.of().formatHex(hash);
    } catch (final Exception e) {
      throw new RuntimeException("Failed to derive cluster token", e);
    }
    LogManager.instance().log(this, Level.WARNING,
        "Using auto-derived cluster token. Changing root password does NOT rotate this token. "
            + "To explicitly rotate, set arcadedb.ha.clusterToken=<new-value> and restart all nodes");
  }

  public void startService() {
    HALog.refreshLevel();

    LogManager.instance().log(this, Level.INFO, "Starting Ratis HA service (cluster=%s, peers=%s, quorum=%s)...",
        configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME), raftGroup.getPeers(), quorum);
    LogManager.instance().log(this, Level.WARNING,
        "Inter-node snapshot and proxy traffic uses plain HTTP. Cluster token and database data are transmitted unencrypted. "
            + "Deploy behind a secure network or VPN for production use");

    try {
      stateMachine = new ArcadeDBStateMachine(server);

      this.raftProperties = buildRaftProperties();
      final RaftProperties properties = this.raftProperties;

      // Use RECOVER if storage exists from a previous run, FORMAT for fresh start
      final Path storagePath = Path.of(server.getRootPath(), "ratis-storage", localPeerId.toString());
      boolean storageExists = false;
      if (Files.exists(storagePath))
        try (final Stream<Path> stream = Files.list(storagePath)) {
          storageExists = stream.findAny().isPresent();
        }

      final var startupOption = storageExists
          ? RaftStorage.StartupOption.RECOVER
          : RaftStorage.StartupOption.FORMAT;

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
          .setRetryPolicy(ExponentialBackoffRetry.newBuilder()
              .setBaseSleepTime(TimeDuration.valueOf(100, TimeUnit.MILLISECONDS))
              .setMaxSleepTime(TimeDuration.valueOf(5, TimeUnit.SECONDS))
              .build())
          .build();

      // In K8s mode: if this is a new server (no existing storage) and other servers might already
      // be running, try to add ourselves to the existing cluster via AdminApi.
      // This handles StatefulSet scale-up where new pods need to join the existing Raft group.
      if (!storageExists && configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S))
        tryAutoJoinCluster();

      groupCommitter = new RaftGroupCommitter(this,
          configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_BATCH_SIZE));
      groupCommitter.start();
      startLagMonitor();

      LogManager.instance().log(this, Level.INFO, "Ratis HA service started (serverId=%s)", localPeerId);

    } catch (final IOException e) {
      throw new ConfigurationException("Failed to start Ratis HA service", e);
    }
  }

  /**
   * Attempts to join an existing Ratis cluster by contacting a peer and adding this server.
   * Used in Kubernetes when a new pod is added via StatefulSet scale-up.
   * If no existing cluster is found (fresh deployment), this is a no-op - the Raft server
   * was already started with the full peer list from HA_SERVER_LIST, so there is no risk
   * of split-brain. All pods share the same Raft group configuration and will elect a
   * leader via normal Raft consensus once a majority becomes reachable.
   */
  private void tryAutoJoinCluster() {
    // Randomized jitter (0-3s) to prevent thundering herd when multiple pods start simultaneously
    // (e.g. K8s Parallel pod management policy or mass restart) and all try setConfiguration()
    // at the same time during scale-up.
    final long jitterMs = Math.abs(localPeerId.hashCode() % 3000L);
    if (jitterMs > 0) {
      HALog.log(this, HALog.BASIC, "K8s auto-join: waiting %dms jitter before probing...", jitterMs);
      try { Thread.sleep(jitterMs); } catch (final InterruptedException e) { Thread.currentThread().interrupt(); return; }
    }

    HALog.log(this, HALog.BASIC, "K8s auto-join: attempting to join existing cluster...");

    // Try each peer to find one that's already running
    for (final RaftPeer peer : raftGroup.getPeers()) {
      if (peer.getId().equals(localPeerId))
        continue;

      try {
        // Create a temporary client with short timeouts to probe if the cluster exists.
        // Without explicit timeouts, a firewalled peer blocks for the full default gRPC timeout.
        final RaftProperties tempProps = new RaftProperties();
        tempProps.set("raft.server.rpc.type", "GRPC");
        RaftServerConfigKeys.Rpc.setTimeoutMin(tempProps, TimeDuration.valueOf(3, TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(tempProps, TimeDuration.valueOf(5, TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setRequestTimeout(tempProps, TimeDuration.valueOf(5, TimeUnit.SECONDS));

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
                  final List<RaftPeer> newPeers = new ArrayList<>();
                  for (final var existingPeer : conf.getPeersList()) {
                    final String existingId = existingPeer.getId().toStringUtf8();
                    for (final RaftPeer p : raftGroup.getPeers())
                      if (p.getId().toString().equals(existingId)) {
                        newPeers.add(p);
                        break;
                      }
                  }
                  newPeers.add(localPeer);

                  final RaftClientReply joinReply = tempClient.admin().setConfiguration(newPeers);
                  if (!joinReply.isSuccess())
                    LogManager.instance().log(this, Level.WARNING, "K8s auto-join: setConfiguration rejected: %s",
                        joinReply.getException() != null ? joinReply.getException().getMessage() : "unknown");
                  else
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

    // No peers responded - this is expected on a fresh cold-start deployment where all pods
    // start simultaneously. The Raft server already has the full peer list configured
    // (from HA_SERVER_LIST), so it will participate in normal Raft leader election once
    // a majority becomes reachable. No split-brain risk because no single-node group is created.
    LogManager.instance().log(this, Level.INFO,
        "K8s auto-join: no existing cluster found. This node will participate in "
            + "Raft leader election with the configured peer group once peers are reachable");
  }

  public void stopService() {
    LogManager.instance().log(this, Level.INFO, "Stopping Ratis HA service...");

    // Take a snapshot before stopping so that on restart, reinitialize() can restore
    // lastAppliedIndex and Ratis won't replay already-applied entries.
    if (stateMachine != null) {
      try {
        stateMachine.takeSnapshot();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Failed to take snapshot during shutdown: %s", e.getMessage());
      }
    }

    if (groupCommitter != null)
      groupCommitter.stop();
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
      final Collection<RaftPeer> livePeers = getLivePeers();
      if (livePeers.size() <= 1) {
        HALog.log(this, HALog.BASIC, "Single-node cluster, skipping leave");
        return;
      }

      // If we're the leader, transfer leadership first
      if (isLeader()) {
        for (final RaftPeer peer : livePeers) {
          if (!peer.getId().equals(localPeerId)) {
            HALog.log(this, HALog.BASIC, "Leaving cluster: transferring leadership to %s before removal", peer.getId());
            try {
              transferLeadership(peer.getId().toString(), 10_000);
              // Wait for leadership change notification instead of polling
              final long deadline = System.currentTimeMillis() + 5_000;
              synchronized (leaderChangeNotifier) {
                while (isLeader()) {
                  final long remaining = deadline - System.currentTimeMillis();
                  if (remaining <= 0)
                    break;
                  leaderChangeNotifier.wait(remaining);
                }
              }
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

  /** Sends a pre-serialized Raft log entry (e.g., CREATE_DATABASE) to the cluster. */
  public void replicateRawEntry(final byte[] entry) {
    HALog.log(this, HALog.BASIC, "Replicating raw entry: %d bytes, type=%d", entry.length, entry.length > 0 ? entry[0] : -1);
    sendToRaft(entry);
  }

  /**
   * Submits a transaction to the Raft cluster. The entry is replicated to all nodes and applied
   * via ArcadeDBStateMachine.applyTransaction() on each node.
   * <p>
   * <b>Timeout semantics:</b> If this method throws {@link QuorumNotReachedException} due to a timeout,
   * the outcome is ambiguous - the transaction may or may not have been committed by the cluster.
   * The caller (ReplicatedDatabase) has already completed commit1stPhase locally, so:
   * <ul>
   *   <li>If the cluster DID commit: follower state machines will apply it normally</li>
   *   <li>If the cluster did NOT commit: the local commit is rolled back by the caller</li>
   * </ul>
   * Callers that need exactly-once semantics should use idempotency keys or check-before-retry logic.
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
        filesToRemove, localPeerId.toString());

    HALog.log(this, HALog.TRACE, "replicateTransaction: db=%s, entrySize=%d bytes", databaseName, entry.length);
    sendToRaft(entry);
  }

  /**
   * Reserved for future use: forwards a transaction from a non-leader node through the Raft cluster.
   * Currently not called from production code - followers use HTTP proxy forwarding via
   * {@code ServerIsNotTheLeaderException} instead. Kept as public API for future direct WAL forwarding.
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

  private void sendToRaft(final byte[] entry) {
    HALog.log(this, HALog.TRACE, "Sending %d bytes to Raft cluster (isLeader=%s)...", entry.length, isLeader());

    // Use group committer to batch multiple concurrent transactions into fewer Raft round-trips
    if (groupCommitter != null) {
      groupCommitter.submitAndWait(entry, quorumTimeout);
      return;
    }

    // Fallback: direct send (used during startup before group committer is initialized)
    try {
      final var future = raftClient.async().send(Message.valueOf(ByteString.copyFrom(entry)));
      final RaftClientReply reply = future.get(quorumTimeout, TimeUnit.MILLISECONDS);

      if (!reply.isSuccess())
        throw new QuorumNotReachedException(
            "Raft replication failed: " + (reply.getException() != null ? reply.getException().getMessage() : "unknown error"));

      if (quorum == Quorum.ALL) {
        final long logIndex = reply.getLogIndex();
        final RaftClientReply watchReply = raftClient.io().watch(logIndex, RaftProtos.ReplicationLevel.ALL_COMMITTED);
        if (!watchReply.isSuccess())
          throw new QuorumNotReachedException("Raft ALL quorum not reached: not all replicas acknowledged the entry");
      }

    } catch (final TimeoutException e) {
      throw new QuorumNotReachedException("Raft replication timed out after " + quorumTimeout + "ms");
    } catch (final ExecutionException e) {
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
      synchronized (applyNotifier) {
        while (getLastAppliedIndex() < targetIndex) {
          final long remaining = deadline - System.currentTimeMillis();
          if (remaining <= 0) {
            LogManager.instance().log(this, Level.WARNING,
                "READ_YOUR_WRITES consistency timeout: applied=%d < target=%d (consistency guarantee degraded to EVENTUAL)",
                getLastAppliedIndex(), targetIndex);
            return;
          }
          applyNotifier.wait(remaining);
        }
      }
      HALog.log(this, HALog.TRACE, "Bookmark wait complete: applied >= target=%d", targetIndex);
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

      final long deadline = System.currentTimeMillis() + quorumTimeout;
      synchronized (applyNotifier) {
        while (getLastAppliedIndex() < commitIndex) {
          final long remaining = deadline - System.currentTimeMillis();
          if (remaining <= 0) {
            HALog.log(this, HALog.DETAILED, "waitForLocalApply timed out: applied=%d < commit=%d",
                getLastAppliedIndex(), commitIndex);
            return;
          }
          applyNotifier.wait(remaining);
        }
      }
      HALog.log(this, HALog.TRACE, "Local apply caught up: applied=%d >= commit=%d",
          getLastAppliedIndex(), commitIndex);
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

  /** Called by ArcadeDBStateMachine after applying a log entry to wake up waiters. */
  public void notifyApplied() {
    synchronized (applyNotifier) {
      applyNotifier.notifyAll();
    }
  }

  /** Called by ArcadeDBStateMachine when the leader changes to wake up leaveCluster(). */
  public void notifyLeaderChanged() {
    synchronized (leaderChangeNotifier) {
      leaderChangeNotifier.notifyAll();
    }
  }

  /**
   * Returns per-follower replication state (only available on the leader).
   * Each entry maps a peer ID to {matchIndex, nextIndex}.
   */
  public List<Map<String, Object>> getFollowerStates() {
    if (raftServer == null || !isLeader())
      return List.of();
    try {
      final var division = raftServer.getDivision(raftGroup.getGroupId());
      final var info = division.getInfo();
      final long[] matchIndices = info.getFollowerMatchIndices();
      final long[] nextIndices = info.getFollowerNextIndices();

      // The indices arrays correspond to followers in the order returned by the group's peers (excluding self)
      final List<Map<String, Object>> result = new ArrayList<>();
      int idx = 0;
      for (final RaftPeer peer : getLivePeers()) {
        if (peer.getId().equals(localPeerId))
          continue;
        final Map<String, Object> state = new java.util.LinkedHashMap<>();
        state.put("peerId", peer.getId().toString());
        state.put("matchIndex", idx < matchIndices.length ? matchIndices[idx] : -1);
        state.put("nextIndex", idx < nextIndices.length ? nextIndices[idx] : -1);
        result.add(state);
        idx++;
      }
      return result;
    } catch (final IOException e) {
      return List.of();
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
    return getLivePeers().size();
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
    for (final RaftPeer peer : getLivePeers()) {
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
    return getLivePeers().size() - 1;
  }

  /**
   * Returns the current live peers from the Raft server's committed configuration.
   * Unlike raftGroup.getPeers() which is static from construction time, this reflects
   * dynamic membership changes from addPeer/removePeer calls.
   * Falls back to the static raftGroup if the server is not running.
   */
  public Collection<RaftPeer> getLivePeers() {
    if (raftServer != null) {
      try {
        final var division = raftServer.getDivision(raftGroup.getGroupId());
        final var conf = division.getRaftConf();
        if (conf != null)
          return conf.getCurrentPeers();
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.FINE, "Cannot read live peers from Raft server, using static list", e);
      }
    }
    return raftGroup.getPeers();
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

  public RaftClient getRaftClient() {
    return raftClient;
  }

  public long getQuorumTimeout() {
    return quorumTimeout;
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
        .setRetryPolicy(ExponentialBackoffRetry.newBuilder()
            .setBaseSleepTime(TimeDuration.valueOf(100, TimeUnit.MILLISECONDS))
            .setMaxSleepTime(TimeDuration.valueOf(5, TimeUnit.SECONDS))
            .build())
        .build();
    HALog.log(this, HALog.BASIC, "RaftClient refreshed with fresh gRPC channels after leader change");
  }

  // -- Cluster Token --

  public String getClusterToken() {
    if (clusterToken == null)
      initClusterToken();
    return clusterToken;
  }

  // -- Lag Monitor --

  private void startLagMonitor() {
    if (clusterMonitor.getLagWarningThreshold() <= 0)
      return;
    lagMonitorExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "arcadedb-raft-lag-monitor");
      t.setDaemon(true);
      return t;
    });
    lagMonitorExecutor.scheduleAtFixedRate(this::checkReplicaLag, 5, 5, TimeUnit.SECONDS);
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

    final List<RaftPeer> newPeers = new ArrayList<>(getLivePeers());
    newPeers.add(newPeer);

    try {
      final RaftClientReply reply = raftClient.admin().setConfiguration(newPeers);
      if (!reply.isSuccess())
        throw new ConfigurationException("Failed to add peer " + peerId + ": " + reply.getException());

      // Derive and store HTTP address from the Raft address (host:raftPort -> host:httpPort)
      final int colonIdx = address.lastIndexOf(':');
      if (colonIdx > 0) {
        final String host = address.substring(0, colonIdx);
        try {
          final int raftPort = Integer.parseInt(address.substring(colonIdx + 1));
          peerHttpAddresses.put(peerId, host + ":" + (raftPort + getHttpPortOffset()));
        } catch (final NumberFormatException ignored) {
          // Non-numeric port, skip HTTP address derivation
        }
      }

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
    final Collection<RaftPeer> livePeers = getLivePeers();
    final List<RaftPeer> newPeers = new ArrayList<>();
    for (final RaftPeer peer : livePeers)
      if (!peer.getId().toString().equals(peerId))
        newPeers.add(peer);

    if (newPeers.size() == livePeers.size())
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

  /**
   * Steps down from leadership by transferring to any available peer.
   * If no peer is available or the transfer fails, logs at SEVERE but does not throw.
   */
  public void stepDown() {
    final String leaderName = getLeaderName();
    for (final var peer : getLivePeers()) {
      if (!peer.getId().toString().equals(leaderName)) {
        try {
          transferLeadership(peer.getId().toString(), 10_000);
          return;
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE,
              "Failed to step down (transfer to %s): %s", peer.getId(), e.getMessage());
        }
      }
    }
    LogManager.instance().log(this, Level.SEVERE,
        "Cannot step down: no other peer available for leadership transfer");
  }

  // -- Snapshot --

  /**
   * Returns the HTTP address of a peer given its Raft peer ID.
   * If no explicit mapping exists (e.g., peer added dynamically), derives the HTTP address
   * from the peer ID (host_raftPort) using the configured HTTP/Raft port offset.
   */
  public String getPeerHTTPAddress(final RaftPeerId peerId) {
    final String httpAddr = peerHttpAddresses.get(peerId.toString());
    if (httpAddr != null)
      return httpAddr;

    // Derive HTTP address from peer ID format "host_raftPort" using port offset
    final String peerIdStr = peerId.toString();
    final int lastUnderscore = peerIdStr.lastIndexOf('_');
    if (lastUnderscore > 0 && lastUnderscore < peerIdStr.length() - 1) {
      final String host = peerIdStr.substring(0, lastUnderscore);
      try {
        final int raftPort = Integer.parseInt(peerIdStr.substring(lastUnderscore + 1));
        final int httpPort = raftPort + getHttpPortOffset();
        final String derived = host + ":" + httpPort;
        peerHttpAddresses.put(peerIdStr, derived);
        return derived;
      } catch (final NumberFormatException ignored) {
        // Fall through to return peer ID as-is
      }
    }
    return peerIdStr;
  }

  private int getHttpPortOffset() {
    final int localHttpPort = parseFirstPort(
        configuration.getValueAsString(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT));
    final int localRaftPort = parseFirstPort(
        configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS));
    return localHttpPort - localRaftPort;
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

    // Election timeouts (configurable for WAN clusters)
    final int electionMin = configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN);
    final int electionMax = configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(electionMin, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(electionMax, TimeUnit.MILLISECONDS));

    // Snapshot: chunk mode (Ratis sends the marker file, ArcadeDB downloads the actual database via HTTP).
    // The default LogAppender only supports chunk-based transfer, not notification mode.
    // When the follower receives the marker, pause() + reinitialize() triggers the HTTP download.
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, true);
    final long snapshotThreshold = configuration.getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_THRESHOLD);
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, snapshotThreshold);
    // Allow frequent snapshot creation (default 1024 gap prevents snapshots in short-lived tests)
    RaftServerConfigKeys.Snapshot.setCreationGap(properties, 0);

    // Log segment size
    final String logSegmentSize = configuration.getValueAsString(GlobalConfiguration.HA_LOG_SEGMENT_SIZE);
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf(logSegmentSize));

    // Log purging: controls how aggressively old log segments are deleted after snapshots
    final int purgeGap = configuration.getValueAsInteger(GlobalConfiguration.HA_LOG_PURGE_GAP);
    RaftServerConfigKeys.Log.setPurgeGap(properties, purgeGap);
    final boolean purgeUptoSnapshot = configuration.getValueAsBoolean(GlobalConfiguration.HA_LOG_PURGE_UPTO_SNAPSHOT);
    RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, purgeUptoSnapshot);

    // Write buffer (must be >= appender buffer byte-limit + 8)
    RaftServerConfigKeys.Log.setWriteBufferSize(properties, SizeInBytes.valueOf("8MB"));

    // AppendEntries batching: allow multiple log entries in a single gRPC call to followers.
    // Combined with the group committer, this allows many transactions to be replicated in one round-trip.
    final String appendBufferSize = configuration.getValueAsString(GlobalConfiguration.HA_APPEND_BUFFER_SIZE);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf(appendBufferSize));
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, 256);

    // Leader lease: enables consistent reads from the leader without a round-trip to followers.
    // The leader can serve reads as long as its lease hasn't expired (based on heartbeat responses).
    RaftServerConfigKeys.Read.setLeaderLeaseEnabled(properties, true);
    RaftServerConfigKeys.Read.setLeaderLeaseTimeoutRatio(properties, 0.9);
    RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);

    // Note: Ratis uses MAJORITY consensus by default.
    // For ALL quorum mode, we use the Watch API after each write to wait for ALL replicas.
    // See sendToRaft() for the ALL quorum implementation.

    // Server-side RPC request timeout: how long the leader waits for follower AppendEntries responses.
    // Generous value allows gRPC channels to recover after network partitions.
    RaftServerConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(10, TimeUnit.SECONDS));

    // Slowness timeout: how long before marking a follower as slow. Must survive partitions.
    RaftServerConfigKeys.Rpc.setSlownessTimeout(properties, TimeDuration.valueOf(300, TimeUnit.SECONDS));

    // Close threshold: how long before the server closes a follower's connection.
    // Set high to allow recovery after long network partitions without losing the peer.
    RaftServerConfigKeys.setCloseThreshold(properties, TimeDuration.valueOf(300, TimeUnit.SECONDS));

    // gRPC flow control window: larger window helps with catch-up replication after partitions
    GrpcConfigKeys.setFlowControlWindow(properties, SizeInBytes.valueOf("4MB"));

    // Client request timeout: bounds how long the Ratis client waits for a single RPC.
    // Without this, the client retries indefinitely when the majority is unreachable.
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(quorumTimeout, TimeUnit.MILLISECONDS));

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

    final int httpPortOffset = getHttpPortOffset();

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
   * Matching order:
   * 1. Exact peer ID match using incoming host + port (e.g., "myhost_2424")
   * 2. Server name match (e.g., server name "arcadedb-0" matches peer "arcadedb-0_2424")
   * 3. Hostname match via InetAddress.getLocalHost()
   * 4. Port-only match (only if a single peer uses this port, to avoid ambiguity)
   */
  private RaftPeerId resolveLocalPeerId(final List<RaftPeer> peers) {
    final String localHost = configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST);
    final String localPorts = configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS);
    final int localPort = parseFirstPort(localPorts);

    // 1. Exact match: peer ID = incomingHost_port
    final String exactId = localHost + "_" + localPort;
    for (final RaftPeer peer : peers)
      if (peer.getId().toString().equals(exactId))
        return peer.getId();

    // 2. Match by server name (e.g., "-Darcadedb.server.name=arcadedb-0" matches peer "arcadedb-0_2424")
    final String serverName = server.getServerName();
    if (serverName != null && !serverName.isEmpty()) {
      final String serverNameId = serverName + "_" + localPort;
      for (final RaftPeer peer : peers)
        if (peer.getId().toString().equals(serverNameId))
          return peer.getId();
    }

    // 3. Match by hostname
    try {
      final String hostname = java.net.InetAddress.getLocalHost().getHostName();
      final String hostnameId = hostname + "_" + localPort;
      for (final RaftPeer peer : peers)
        if (peer.getId().toString().equals(hostnameId))
          return peer.getId();
    } catch (final java.net.UnknownHostException ignored) {
    }

    // 4. Fallback: match by port only if unambiguous (useful for single-host testing)
    RaftPeerId portMatch = null;
    int portMatchCount = 0;
    for (final RaftPeer peer : peers) {
      final String address = peer.getAddress();
      if (address != null && address.endsWith(":" + localPort)) {
        portMatch = peer.getId();
        portMatchCount++;
      }
    }
    if (portMatchCount == 1)
      return portMatch;

    throw new ConfigurationException(
        "Cannot find local server in HA_SERVER_LIST. serverName=" + serverName + ", localAddress=" + localHost + ":" + localPort
            + ", server list: " + peers);
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
