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
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Manages the lifecycle of the Apache Ratis {@link RaftServer}, {@link RaftClient},
 * and {@link RaftGroupCommitter} for ArcadeDB high availability.
 * <p>
 * Owns peer configuration (parsed from {@code HA_SERVER_LIST}), quorum policy
 * ({@link Quorum#MAJORITY} or {@link Quorum#ALL}), and leadership state.
 * Provides the {@link HealthMonitor.HealthTarget} interface so the background
 * health monitor can trigger automatic recovery from stuck Ratis states.
 * <p>
 * <b>Thread-safety:</b> {@link #recoveryLock} synchronizes recovery attempts in
 * {@link #restartRatisIfNeeded()} to prevent concurrent restart races. The
 * {@link #shutdownRequested} volatile flag prevents recovery during shutdown.
 * <p>
 * <b>Security note (K8s mode):</b> When {@code HA_K8S} is enabled and gRPC is bound
 * to {@code 0.0.0.0}, any pod in the Kubernetes cluster can connect to the Raft port
 * and inject Raft log entries. Authentication for inter-node traffic relies on
 * Kubernetes NetworkPolicy. Operators should restrict access to the Raft port via
 * NetworkPolicy rules in production.
 */
public class RaftHAServer implements HealthMonitor.HealthTarget {

  /**
   * Result of parsing the HA server list. Contains the Raft peers (with raft addresses) and
   * a map from peer ID to HTTP address for replica-to-leader HTTP command forwarding.
   * The {@code httpAddresses} map is empty when no httpPort is specified in the server list.
   */
  public record ParsedPeerList(List<RaftPeer> peers, Map<RaftPeerId, String> httpAddresses) {
  }

  private final ArcadeDBServer             arcadeServer;
  private final ContextConfiguration       configuration;
  private ArcadeStateMachine               stateMachine;
  private final ClusterMonitor             clusterMonitor;
  private final Quorum                     quorum;
  private final long                       quorumTimeout;
  private final RaftGroup                  raftGroup;
  private final RaftPeerId                 localPeerId;
  private final Map<RaftPeerId, String>    httpAddresses = new HashMap<>();
  private final Map<RaftPeerId, String>    peerDisplayNames;
  private final String                     clusterName;

  private RaftServer                raftServer;
  private RaftClient                raftClient;
  private RaftProperties            raftProperties;
  private volatile RaftGroupCommitter groupCommitter;
  private ScheduledExecutorService  lagMonitorExecutor;
  private final Object              leaderChangeNotifier = new Object();
  private final Object              applyNotifier        = new Object();
  private int                       lastClusterConfigHash;
  private final Object               recoveryLock          = new Object();
  private volatile boolean           shutdownRequested     = false;
  private volatile LifeCycle.State   forcedStateForTesting = null;
  private HealthMonitor              healthMonitor;

  public RaftHAServer(final ArcadeDBServer arcadeServer, final ContextConfiguration configuration) {
    this.arcadeServer = arcadeServer;
    this.configuration = configuration;

    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    final String clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    this.clusterName = clusterName;
    final long lagWarningThreshold = configuration.getValueAsLong(GlobalConfiguration.HA_REPLICATION_LAG_WARNING);
    final int raftPort = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_PORT);

    final ParsedPeerList parsed = parsePeerList(serverList, raftPort);
    final List<RaftPeer> peers = parsed.peers();
    final String serverName = arcadeServer.getServerName();

    this.httpAddresses.putAll(parsed.httpAddresses());
    this.localPeerId = findLocalPeerId(peers, serverName, arcadeServer);
    this.raftGroup = RaftGroup.valueOf(
        RaftGroupId.valueOf(UUID.nameUUIDFromBytes(clusterName.getBytes(StandardCharsets.UTF_8))),
        peers);

    // Build human-readable display names: "ServerName-N (host:httpPort)"
    final int separatorIdx = findLastSeparatorIndex(serverName);
    final String prefix = serverName.substring(0, separatorIdx);
    final char separator = serverName.charAt(separatorIdx);
    final Map<RaftPeerId, String> displayNames = new HashMap<>(peers.size());
    for (int i = 0; i < peers.size(); i++) {
      final RaftPeerId peerId = peers.get(i).getId();
      final String nodeName = prefix + separator + i;
      final String httpAddr = this.httpAddresses.get(peerId);
      displayNames.put(peerId, httpAddr != null ? nodeName + " (" + httpAddr + ")" : nodeName);
    }
    this.peerDisplayNames = Collections.unmodifiableMap(displayNames);

    this.stateMachine = new ArcadeStateMachine();
    this.stateMachine.setServer(arcadeServer);

    this.clusterMonitor = new ClusterMonitor(lagWarningThreshold);
    this.quorum = Quorum.parse(configuration.getValueAsString(GlobalConfiguration.HA_QUORUM));
    this.quorumTimeout = configuration.getValueAsLong(GlobalConfiguration.HA_QUORUM_TIMEOUT);

    LogManager.instance().log(this, Level.INFO,
        "RaftHAServer configured: cluster='%s', localPeer='%s', peers=%d",
        clusterName, localPeerId, peers.size());
  }

  /**
   * Parses a comma-separated server list into a {@link ParsedPeerList}.
   * <p>
   * Each entry supports the following formats:
   * <ul>
   *   <li>{@code host:raftPort:httpPort:priority} — explicit Raft port, HTTP port, and leader-election priority</li>
   *   <li>{@code host:raftPort:httpPort} — explicit Raft and HTTP ports, priority defaults to 0</li>
   *   <li>{@code host:raftPort} — explicit Raft port, no HTTP address stored, priority defaults to 0</li>
   *   <li>{@code host} — Raft port defaults to {@code defaultPort}, no HTTP address, priority defaults to 0</li>
   * </ul>
   * The {@code httpAddresses} map in the result is populated only for entries with 3 or 4 parts;
   * it is keyed by the {@link RaftPeerId} of each peer.
   * <p>
   * Priority is used for Raft leader election: the node with the highest priority is preferred as leader.
   * This is a soft preference — if the preferred leader is unavailable, another node will take over.
   */
  static ParsedPeerList parsePeerList(final String serverList, final int defaultPort) {
    final String[] entries = serverList.split(",");
    final List<RaftPeer> peers = new ArrayList<>(entries.length);
    final Map<RaftPeerId, String> httpAddresses = new HashMap<>(entries.length);

    for (int i = 0; i < entries.length; i++) {
      final String entry = entries[i].trim();
      final String[] parts = entry.split(":");

      if (parts.length > 4 || parts.length == 0 || parts[0].isBlank())
        throw new ServerException(
            "Invalid peer address format '" + entry + "'. Expected host[:raftPort[:httpPort[:priority]]]");

      final String raftAddress;
      String httpAddress = null;
      int priority = 0;

      if (parts.length == 4) {
        // host:raftPort:httpPort:priority
        raftAddress = parts[0] + ":" + parts[1];
        httpAddress = parts[0] + ":" + parts[2];
        try {
          priority = Integer.parseInt(parts[3]);
        } catch (final NumberFormatException e) {
          throw new ServerException("Invalid priority value '" + parts[3] + "' in peer address '" + entry + "'");
        }
      } else if (parts.length == 3) {
        // host:raftPort:httpPort
        raftAddress = parts[0] + ":" + parts[1];
        httpAddress = parts[0] + ":" + parts[2];
      } else if (parts.length == 2) {
        // host:raftPort
        raftAddress = entry;
      } else {
        // host only - use default Raft port
        raftAddress = entry + ":" + defaultPort;
      }

      // Use host_raftPort as peer ID (underscore avoids JMX ObjectName issues with colons)
      final String peerIdStr = raftAddress.replace(':', '_');
      final RaftPeer peer = RaftPeer.newBuilder()
          .setId(peerIdStr)
          .setAddress(raftAddress)
          .setPriority(priority)
          .build();
      peers.add(peer);

      if (httpAddress != null)
        httpAddresses.put(peer.getId(), httpAddress);
    }

    // Validate: mixing localhost/127.0.0.1 with non-localhost addresses is a misconfiguration
    boolean hasLocalhost = false;
    boolean hasNonLocalhost = false;
    for (final RaftPeer peer : peers) {
      final String host = peer.getAddress().split(":")[0].trim();
      if (host.equals("localhost") || host.equals("127.0.0.1"))
        hasLocalhost = true;
      else
        hasNonLocalhost = true;
    }
    if (hasLocalhost && hasNonLocalhost)
      throw new ServerException(
          "Found a localhost (127.0.0.1) in the server list among non-localhost servers. "
              + "Please fix the server list configuration.");

    return new ParsedPeerList(Collections.unmodifiableList(peers), Collections.unmodifiableMap(httpAddresses));
  }

  /**
   * Determines the local peer ID by parsing the numeric suffix from the server name.
   * For example, "arcadedb-0" or "ArcadeDB_0" maps to index 0 in the peer list.
   */
  static RaftPeerId findLocalPeerId(final List<RaftPeer> peers, final String serverName,
      final ArcadeDBServer server) {
    final int separatorIdx = findLastSeparatorIndex(serverName);
    final int index = Integer.parseInt(serverName.substring(separatorIdx + 1));
    if (index < 0 || index >= peers.size())
      throw new IllegalArgumentException(
          "Server index " + index + " from name '" + serverName + "' is out of range [0, " + peers.size() + ")");

    return peers.get(index).getId();
  }

  /**
   * Finds the index of the last separator character ({@code '-'} or {@code '_'}) in the server name.
   * Server names follow the pattern {@code prefix-N} or {@code prefix_N} where N is the node index.
   */
  static int findLastSeparatorIndex(final String serverName) {
    final int hyphenIdx = serverName.lastIndexOf('-');
    final int underscoreIdx = serverName.lastIndexOf('_');
    final int idx = Math.max(hyphenIdx, underscoreIdx);
    if (idx < 0 || idx == serverName.length() - 1)
      throw new IllegalArgumentException("Cannot parse server index from server name: " + serverName);
    return idx;
  }

  /**
   * Returns the HTTP address for a peer, or null if not configured.
   */
  public String getPeerHttpAddress(final RaftPeerId peerId) {
    return httpAddresses.get(peerId);
  }

  /**
   * Returns a human-readable display name for a peer, e.g. "arcadedb-0 (localhost:2480)".
   * Falls back to the raw peer ID string if the peer is unknown.
   */
  public String getPeerDisplayName(final RaftPeerId peerId) {
    final String name = peerDisplayNames.get(peerId);
    return name != null ? name : peerId.toString();
  }

  /**
   * Builds the {@link RaftProperties} used to configure the Ratis server.
   * Extracted so the health-monitor recovery path can rebuild properties
   * without duplicating configuration logic.
   */
  private RaftProperties buildRaftProperties() {
    final RaftProperties properties = new RaftProperties();

    // Use the configured Raft port for the local gRPC bind address.
    // Note: the peer address in the server list may differ from the bind port when traffic
    // is routed through a proxy (e.g., Toxiproxy in e2e tests). The peer address is what
    // remote nodes use to connect; the bind port is what this node actually listens on.
    final int localRaftPort = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_PORT);
    GrpcConfigKeys.Server.setPort(properties, localRaftPort);

    // Configure Raft RPC timeouts for cluster stability
    final int electionMin = configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN);
    final int electionMax = configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(electionMin, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(electionMax, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(10, TimeUnit.SECONDS));

    final long flowControlWindow = configuration.getValueAsLong(GlobalConfiguration.HA_GRPC_FLOW_CONTROL_WINDOW);
    GrpcConfigKeys.setFlowControlWindow(properties, SizeInBytes.valueOf(flowControlWindow));

    // Staging timeout: when adding a new peer, the leader syncs it before committing the
    // config change. This bounds how long the leader waits for the new peer to catch up.
    RaftServerConfigKeys.setStagingTimeout(properties, TimeDuration.valueOf(30, TimeUnit.SECONDS));

    final long snapshotThreshold = configuration.getValueAsLong(GlobalConfiguration.HA_RAFT_SNAPSHOT_THRESHOLD);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, snapshotThreshold);
    RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true);

    // Disable Ratis built-in snapshot transfer; use notification mode
    // so ArcadeDB controls the snapshot transfer via HTTP
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false);
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);

    // AppendEntries batching: allow multiple entries per gRPC call to followers
    final String appendBufferSize = configuration.getValueAsString(GlobalConfiguration.HA_APPEND_BUFFER_SIZE);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf(appendBufferSize));
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, 256);

    // Log segment and write buffer sizes
    final String logSegmentSize = configuration.getValueAsString(GlobalConfiguration.HA_LOG_SEGMENT_SIZE);
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf(logSegmentSize));
    RaftServerConfigKeys.Log.setWriteBufferSize(properties, SizeInBytes.valueOf("8MB"));

    // Leader lease: consistent reads without round-trip
    RaftServerConfigKeys.Read.setLeaderLeaseEnabled(properties, true);
    RaftServerConfigKeys.Read.setLeaderLeaseTimeoutRatio(properties, 0.9);
    RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);

    return properties;
  }

  /**
   * Creates and starts the Ratis RaftServer and RaftClient.
   */
  public void start() throws IOException {
    // Suppress verbose Ratis internal logs — operators see ArcadeDB-level cluster events instead
    java.util.logging.Logger.getLogger("org.apache.ratis").setLevel(java.util.logging.Level.WARNING);

    final RaftProperties properties = buildRaftProperties();

    final File storageDir = new File(arcadeServer.getRootPath() + File.separator + "raft-storage-" + localPeerId);
    // Only delete existing Raft storage when persistence is not requested.
    // Persistent mode (HA_RAFT_PERSIST_STORAGE=true) is used in tests that restart nodes
    // within a single test run, so the Raft log survives across stop/start calls.
    final boolean persistStorage = configuration.getValueAsBoolean(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE);
    if (storageDir.exists() && !persistStorage)
      deleteRecursive(storageDir);
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    initClusterToken(configuration, storageDir);

    // When persistent storage is requested and the storage directory already has data,
    // use RECOVER mode so Ratis loads the existing Raft log instead of trying to format
    // (which would fail if the group directory already exists).
    final File[] storageDirs = storageDir.listFiles(f -> f.isDirectory() && !f.getName().equals("lost+found"));
    final boolean hasExistingStorage = persistStorage && storageDir.exists()
        && storageDirs != null && storageDirs.length > 0;
    final RaftStorage.StartupOption startupOption = hasExistingStorage
        ? RaftStorage.StartupOption.RECOVER
        : RaftStorage.StartupOption.FORMAT;

    final boolean hadExistingStorage = hasExistingRaftStorage();

    raftServer = RaftServer.newBuilder()
        .setServerId(localPeerId)
        .setGroup(raftGroup)
        .setStateMachine(stateMachine)
        .setProperties(properties)
        .setParameters(new Parameters())
        .setOption(startupOption)
        .build();

    raftServer.start();

    this.raftProperties = properties;

    raftClient = buildRaftClient(raftGroup, properties);

    LogManager.instance().log(this, Level.INFO, "Raft cluster joined: %d nodes %s", peerDisplayNames.size(), peerDisplayNames.values());

    final int batchSize = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_GROUP_COMMIT_BATCH_SIZE);
    groupCommitter = new RaftGroupCommitter(raftClient, quorum, quorumTimeout, batchSize);

    // K8s auto-join: if running in Kubernetes with no existing storage, try to join an existing cluster
    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S) && !hadExistingStorage)
      tryAutoJoinCluster();

    final long healthInterval = configuration.getValueAsLong(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL);
    this.healthMonitor = new HealthMonitor(this, healthInterval);
    this.healthMonitor.start();
  }

  @Override
  public LifeCycle.State getRaftLifeCycleState() {
    final LifeCycle.State forced = forcedStateForTesting;
    if (forced != null) {
      forcedStateForTesting = null;
      return forced;
    }
    if (raftServer == null)
      return LifeCycle.State.NEW;
    return raftServer.getLifeCycleState();
  }

  @Override
  public boolean isShutdownRequested() {
    return shutdownRequested;
  }

  /**
   * Recovers from a Ratis server that has entered CLOSED or CLOSING state, typically
   * after a network partition where the node was isolated long enough for Ratis to
   * give up on the group.
   * <p>
   * Triggered by {@link HealthMonitor} when it detects an unhealthy Ratis state.
   * Creates a new {@link ArcadeStateMachine} and Ratis server using
   * {@link RaftStorage.StartupOption#RECOVER} mode, which loads the existing Raft log
   * from disk instead of formatting fresh storage.
   * <p>
   * The database state is persisted on disk, so the new state machine picks up where
   * the old one left off. The {@code lastAppliedIndex} is restored from Ratis snapshot
   * metadata during {@link ArcadeStateMachine#reinitialize()}, and Ratis replays any
   * log entries beyond that point.
   * <p>
   * The {@link #recoveryLock} prevents concurrent restart attempts. If
   * {@link #shutdownRequested} is true, recovery is skipped.
   */
  @Override
  public void restartRatisIfNeeded() {
    synchronized (recoveryLock) {
      if (shutdownRequested) {
        HALog.log(this, HALog.BASIC, "Recovery skipped: shutdown requested");
        return;
      }

      final RaftClient oldClient = this.raftClient;
      final RaftServer oldServer = this.raftServer;
      final RaftGroupCommitter oldCommitter = this.groupCommitter;

      try { if (oldCommitter != null) oldCommitter.stop(); }
      catch (final Throwable t) { LogManager.instance().log(this, Level.FINE, "Error closing old committer: %s", t, t.getMessage()); }
      try { if (oldClient != null) oldClient.close(); }
      catch (final Throwable t) { LogManager.instance().log(this, Level.FINE, "Error closing old client: %s", t, t.getMessage()); }
      try { if (oldServer != null) oldServer.close(); }
      catch (final Throwable t) { LogManager.instance().log(this, Level.FINE, "Error closing old server: %s", t, t.getMessage()); }

      try {
        this.stateMachine = new ArcadeStateMachine();
        this.stateMachine.setServer(arcadeServer);

        final RaftProperties properties = buildRaftProperties();
        final File storageDir = new File(arcadeServer.getRootPath() + File.separator + "raft-storage-" + localPeerId);
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

        this.raftServer = RaftServer.newBuilder()
            .setServerId(localPeerId)
            .setGroup(raftGroup)
            .setStateMachine(stateMachine)
            .setProperties(properties)
            .setParameters(new Parameters())
            .setOption(RaftStorage.StartupOption.RECOVER)
            .build();
        this.raftServer.start();
        this.raftProperties = properties;
        this.raftClient = buildRaftClient(raftGroup, properties);

        final int batchSize = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_GROUP_COMMIT_BATCH_SIZE);
        this.groupCommitter = new RaftGroupCommitter(raftClient, quorum, quorumTimeout, batchSize);

        HALog.log(this, HALog.BASIC, "Ratis recovered successfully");
      } catch (final Throwable t) {
        LogManager.instance().log(this, Level.SEVERE, "HealthMonitor recovery failed: %s", t, t.getMessage());
      }
    }
  }

  /** Package-private test hook. Next call to getRaftLifeCycleState() returns this value, then clears. */
  void forceRaftStateForTesting(final LifeCycle.State state) {
    this.forcedStateForTesting = state;
  }

  /**
   * Stops the Raft client and server, releasing all resources.
   */
  public void stop() {
    shutdownRequested = true;
    if (healthMonitor != null) {
      healthMonitor.stop();
      healthMonitor = null;
    }
    stopLagMonitor();
    if (groupCommitter != null) {
      groupCommitter.stop();
      groupCommitter = null;
    }

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S))
      leaveCluster();

    // Suppress noisy Ratis gRPC warnings during shutdown (AlreadyClosedException, CANCELLED streams).
    // These are harmless - internal replication threads take a moment to notice the server is closed.
    final String[] noisyLoggers = {
        "org.apache.ratis.grpc.server.GrpcLogAppender",
        "org.apache.ratis.grpc.server.GrpcServerProtocolService"
    };
    final java.util.logging.Level[] previousLevels = new java.util.logging.Level[noisyLoggers.length];
    for (int i = 0; i < noisyLoggers.length; i++) {
      final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(noisyLoggers[i]);
      previousLevels[i] = logger.getLevel();
      logger.setLevel(java.util.logging.Level.SEVERE);
    }

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
    } finally {
      for (int i = 0; i < noisyLoggers.length; i++)
        java.util.logging.Logger.getLogger(noisyLoggers[i]).setLevel(previousLevels[i]);
    }

    LogManager.instance().log(this, Level.INFO, "RaftHAServer stopped");
  }

  /**
   * Returns true if this server is the current Raft leader.
   */
  public boolean isLeader() {
    if (raftServer == null)
      return false;

    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getInfo().isLeader();
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Error checking leader status", e);
      return false;
    }
  }

  /**
   * Returns the peer ID of the current Raft leader, or null if unknown.
   */
  public RaftPeerId getLeaderId() {
    if (raftServer == null)
      return null;

    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getInfo().getLeaderId();
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Error getting leader ID", e);
      return null;
    }
  }

  /**
   * Asks the Raft leader to step down, triggering a new election. This forces all servers
   * to recreate their internal gRPC log-appender channels, which resolves stale connections
   * to restarted peers whose gRPC channels are stuck in exponential backoff.
   *
   * @param timeoutMs maximum time to wait for the transfer to complete
   * @return true if the transfer succeeded
   */
  public boolean transferLeadership(final long timeoutMs) {
    if (raftClient == null)
      return false;
    try {
      final RaftClientReply reply = raftClient.admin().transferLeadership(null, timeoutMs);
      return reply.isSuccess() || !isLeader();
    } catch (final Exception e) {
      // When the transfer succeeds, notifyLeaderChanged calls refreshRaftClient() which
      // closes the old client. The in-flight RPC then fails with "is closed".
      // If we are no longer the leader, the transfer succeeded.
      if (!isLeader())
        return true;
      LogManager.instance().log(this, Level.INFO, "Leadership transfer request: %s", e.getMessage());
      return false;
    }
  }

  /**
   * Returns the HTTP address (host:port) of the current Raft leader, or {@code null} if the leader
   * is unknown or its HTTP address was not configured in the server list.
   */
  public String getLeaderHttpAddress() {
    final RaftPeerId leaderId = getLeaderId();
    if (leaderId == null)
      return null;
    return httpAddresses.get(leaderId);
  }

  public RaftClient getClient() {
    return raftClient;
  }

  public RaftGroupCommitter getGroupCommitter() {
    return groupCommitter;
  }

  /**
   * Closes the current RaftClient and creates a new one with fresh gRPC channels.
   * <p>
   * After a network partition, gRPC channels to partitioned peers enter TRANSIENT_FAILURE
   * with exponential backoff (up to ~120 s). Any Raft send on those channels fails with
   * {@code UnresolvedAddressException}, even after the partition heals and DNS is restored.
   * Re-creating the client forces new channel creation and immediate DNS re-resolution,
   * allowing the cluster to accept writes again as soon as the new leader is elected.
   */
  public synchronized void refreshRaftClient() {
    refreshRaftClient(null);
  }

  /**
   * Like {@link #refreshRaftClient()}, but seeds the new client with the known leader so
   * the very first write request routes directly to the leader without a probe round-trip.
   *
   * @param knownLeaderId the peer ID of the newly elected leader, or {@code null} to use
   *                      the Ratis group-level leader cache.
   */
  public synchronized void refreshRaftClient(final RaftPeerId knownLeaderId) {
    if (raftProperties == null)
      return;

    // Stop the group committer FIRST so its flusher thread is no longer using the old raftClient.
    // Create the replacement committer before stopping the old one to minimize the window where
    // no committer is available to concurrent callers (the field is volatile).
    final RaftClient oldClient = raftClient;

    raftClient = buildRaftClient(raftGroup, raftProperties, knownLeaderId);

    if (groupCommitter != null) {
      final int batchSize = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_GROUP_COMMIT_BATCH_SIZE);
      final RaftGroupCommitter oldCommitter = groupCommitter;
      groupCommitter = new RaftGroupCommitter(raftClient, quorum, quorumTimeout, batchSize);
      oldCommitter.stop();
    }

    if (oldClient != null) {
      try {
        oldClient.close();
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.WARNING, "Error closing stale RaftClient during refresh", e);
      }
    }

    LogManager.instance().log(this, Level.INFO, "RaftClient refreshed with fresh gRPC channels after leader change");
  }

  public ArcadeStateMachine getStateMachine() {
    return stateMachine;
  }

  public ClusterMonitor getClusterMonitor() {
    return clusterMonitor;
  }

  public Quorum getQuorum() {
    return quorum;
  }

  public long getQuorumTimeout() {
    return quorumTimeout;
  }

  public RaftGroup getRaftGroup() {
    return raftGroup;
  }

  public Map<RaftPeerId, String> getHttpAddresses() {
    return httpAddresses;
  }

  public String getClusterName() {
    return clusterName;
  }

  public int getConfiguredServers() {
    return raftGroup.getPeers().size();
  }

  public String getLeaderName() {
    final RaftPeerId leaderId = getLeaderId();
    if (leaderId == null)
      return null;
    final String display = peerDisplayNames.get(leaderId);
    return display != null ? display : leaderId.toString();
  }

  public Map<String, Object> getStats() {
    final Map<String, Object> stats = new HashMap<>();
    stats.put("localPeerId", localPeerId.toString());
    stats.put("isLeader", isLeader());
    stats.put("configuredServers", getConfiguredServers());

    if (clusterMonitor != null) {
      final Map<String, Long> lags = clusterMonitor.getReplicaLags();
      if (!lags.isEmpty())
        stats.put("replicaLags", lags);
    }

    final List<Map<String, String>> replicas = new ArrayList<>();
    for (final RaftPeer peer : raftGroup.getPeers()) {
      if (!peer.getId().equals(localPeerId)) {
        final Map<String, String> replicaInfo = new HashMap<>();
        replicaInfo.put("id", peer.getId().toString());
        replicaInfo.put("address", peer.getAddress().toString());
        final String httpAddr = httpAddresses.get(peer.getId());
        if (httpAddr != null)
          replicaInfo.put("httpAddress", httpAddr);
        replicas.add(replicaInfo);
      }
    }
    stats.put("replicas", replicas);
    return stats;
  }

  public String getReplicaAddresses() {
    final StringBuilder sb = new StringBuilder();
    for (final RaftPeer peer : raftGroup.getPeers()) {
      if (!peer.getId().equals(localPeerId)) {
        final String httpAddr = httpAddresses.get(peer.getId());
        if (httpAddr != null) {
          if (!sb.isEmpty())
            sb.append(",");
          sb.append(httpAddr);
        }
      }
    }
    return sb.toString();
  }

  public RaftPeerId getLocalPeerId() {
    return localPeerId;
  }

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

  Object getLeaderChangeNotifier() {
    return leaderChangeNotifier;
  }

  public void addPeer(final String peerId, final String address) {
    final RaftPeer newPeer = RaftPeer.newBuilder()
        .setId(RaftPeerId.valueOf(peerId))
        .setAddress(address)
        .build();

    final List<RaftPeer> newPeers = new ArrayList<>(getLivePeers());
    newPeers.add(newPeer);

    setConfigurationWithRetry(newPeers, "add peer " + peerId);

    final int colonIdx = address.lastIndexOf(':');
    if (colonIdx > 0) {
      final String host = address.substring(0, colonIdx);
      try {
        final int raftPort = Integer.parseInt(address.substring(colonIdx + 1));
        final int httpPortOffset = getHttpPortOffset();
        httpAddresses.put(RaftPeerId.valueOf(peerId), host + ":" + (raftPort + httpPortOffset));
      } catch (final NumberFormatException ignored) {
      }
    }

    LogManager.instance().log(this, Level.INFO, "Peer %s added to Raft cluster at %s", peerId, address);
  }

  public void removePeer(final String peerId) {
    final Collection<RaftPeer> livePeers = getLivePeers();
    final List<RaftPeer> newPeers = new ArrayList<>();
    for (final RaftPeer peer : livePeers)
      if (!peer.getId().toString().equals(peerId))
        newPeers.add(peer);

    if (newPeers.size() == livePeers.size())
      throw new ConfigurationException("Peer " + peerId + " not found in cluster");

    setConfigurationWithRetry(newPeers, "remove peer " + peerId);

    httpAddresses.remove(RaftPeerId.valueOf(peerId));
    LogManager.instance().log(this, Level.INFO, "Peer %s removed from Raft cluster", peerId);
  }

  /**
   * Calls {@code setConfiguration} with bounded retry on {@link ReconfigurationInProgressException}.
   * <p>
   * On a fresh cluster the newly elected leader must commit an entry from its own term before it
   * can process configuration changes (Raft protocol requirement). This method sends a no-op
   * message first to ensure the leader has committed from its current term, then issues the
   * setConfiguration call with bounded retry.
   */
  private void setConfigurationWithRetry(final List<RaftPeer> peers, final String operationDesc) {
    final long deadline = System.currentTimeMillis() + 90_000;
    long sleepMs = 200;

    while (true) {
      try {
        final RaftClientReply reply = raftClient.admin().setConfiguration(peers);
        if (reply.isSuccess())
          return;

        if (System.currentTimeMillis() < deadline) {
          LogManager.instance().log(this, Level.FINE,
              "setConfiguration failed for %s, retrying in %d ms: %s", operationDesc, sleepMs, reply.getException());
          Thread.sleep(sleepMs);
          sleepMs = Math.min(sleepMs * 2, 2_000);
          continue;
        }
        throw new ConfigurationException("Failed to " + operationDesc + ": " + reply.getException());
      } catch (final IOException e) {
        if (System.currentTimeMillis() < deadline) {
          LogManager.instance().log(this, Level.FINE,
              "setConfiguration I/O error for %s, retrying in %d ms", operationDesc, sleepMs);
          try {
            Thread.sleep(sleepMs);
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ConfigurationException("Interrupted while waiting to " + operationDesc, ie);
          }
          sleepMs = Math.min(sleepMs * 2, 2_000);
          continue;
        }
        throw new ConfigurationException("Failed to " + operationDesc, e);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConfigurationException("Interrupted while waiting to " + operationDesc, e);
      }
    }
  }

  /**
   * Builds a RaftClient with a bounded retry policy. The default Ratis retry policy is
   * retryForeverNoSleep, which causes blocking admin calls (like setConfiguration) to hang
   * indefinitely when the leader is not yet ready or the reconfiguration is in progress.
   * This uses a bounded retry so a single call returns within a reasonable time, allowing
   * our own retry loop in setConfigurationWithRetry to control the overall timeout.
   */
  private static RaftClient buildRaftClient(final RaftGroup group, final RaftProperties properties) {
    return buildRaftClient(group, properties, null);
  }

  /**
   * Like {@link #buildRaftClient(RaftGroup, RaftProperties)}, but seeds the new client with a
   * known leader peer ID so the first write request routes directly to the leader without a
   * probe round-trip.
   */
  private static RaftClient buildRaftClient(final RaftGroup group, final RaftProperties properties,
      final RaftPeerId knownLeaderId) {
    // Set the client-side RPC timeout to match the quorum timeout so a slow leader response
    // does not trigger a premature TimeoutIOException before the commit completes.
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(10, TimeUnit.SECONDS));
    final RaftClient.Builder builder = RaftClient.newBuilder()
        .setRaftGroup(group)
        .setProperties(properties)
        .setParameters(new Parameters())
        .setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(60, TimeDuration.valueOf(1, TimeUnit.SECONDS)));
    if (knownLeaderId != null)
      builder.setLeaderId(knownLeaderId);
    return builder.build();
  }

  private int getHttpPortOffset() {
    for (final RaftPeer peer : raftGroup.getPeers()) {
      final String httpAddr = httpAddresses.get(peer.getId());
      if (httpAddr != null) {
        try {
          final int httpPort = Integer.parseInt(httpAddr.substring(httpAddr.lastIndexOf(':') + 1));
          final int raftPort = Integer.parseInt(
              peer.getAddress().toString().substring(peer.getAddress().toString().lastIndexOf(':') + 1));
          return httpPort - raftPort;
        } catch (final NumberFormatException ignored) {
        }
      }
    }
    return 46;
  }

  public void transferLeadership(final String targetPeerId, final long timeoutMs) {
    LogManager.instance().log(this, Level.INFO, "Transferring leadership to %s (timeout=%d ms)", targetPeerId, timeoutMs);
    try {
      final RaftClientReply reply = raftClient.admin().transferLeadership(
          RaftPeerId.valueOf(targetPeerId), timeoutMs);
      if (!reply.isSuccess()) {
        // The leader change notification fires refreshRaftClient(), which closes the old client
        // while this call is still in flight. If the leader actually changed to the target,
        // the transfer succeeded despite the error reply.
        if (isLeaderNow(targetPeerId)) {
          LogManager.instance().log(this, Level.INFO, "Leadership transferred to %s (confirmed via leader check)", targetPeerId);
          return;
        }
        throw new ConfigurationException(
            "Failed to transfer leadership to " + targetPeerId + ": " + reply.getException());
      }
      LogManager.instance().log(this, Level.INFO, "Leadership transferred to %s", targetPeerId);
    } catch (final IOException e) {
      // When the transfer succeeds, notifyLeaderChanged calls refreshRaftClient() which closes
      // the old RaftClient. The in-flight RPC then fails with "is closed". Verify the transfer
      // actually succeeded by checking who the leader is now.
      if (isLeaderNow(targetPeerId)) {
        LogManager.instance().log(this, Level.INFO, "Leadership transferred to %s (confirmed after IOException)", targetPeerId);
        return;
      }
      throw new ConfigurationException("Failed to transfer leadership to " + targetPeerId + ": " + e.getMessage(), e);
    }
  }

  private boolean isLeaderNow(final String expectedPeerId) {
    final RaftPeerId leaderId = getLeaderId();
    return leaderId != null && leaderId.toString().equals(expectedPeerId);
  }

  public void stepDown() {
    for (final var peer : getLivePeers()) {
      if (!peer.getId().toString().equals(localPeerId.toString())) {
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

  public void leaveCluster() {
    if (raftServer == null || raftClient == null)
      return;

    try {
      final Collection<RaftPeer> livePeers = getLivePeers();
      if (livePeers.size() <= 1) {
        HALog.log(this, HALog.BASIC, "Single-node cluster, skipping leave");
        return;
      }

      if (isLeader()) {
        for (final RaftPeer peer : livePeers) {
          if (!peer.getId().equals(localPeerId)) {
            HALog.log(this, HALog.BASIC,
                "Leaving cluster: transferring leadership to %s before removal", peer.getId());
            try {
              transferLeadership(peer.getId().toString(), 10_000);
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
              HALog.log(this, HALog.BASIC,
                  "Leadership transfer failed (%s), proceeding with removal", e.getMessage());
            }
            break;
          }
        }
      }

      HALog.log(this, HALog.BASIC, "Leaving cluster: removing self (%s) from Raft group", localPeerId);
      removePeer(localPeerId.toString());
      HALog.log(this, HALog.BASIC, "Successfully left the Raft cluster");

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Failed to leave cluster gracefully: %s", e.getMessage());
    }
  }

  public void notifyApplied() {
    synchronized (applyNotifier) {
      applyNotifier.notifyAll();
    }
  }

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
                "READ_YOUR_WRITES consistency timeout: applied=%d < target=%d (consistency degraded to EVENTUAL)",
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
   * Blocks until the local state machine's last-applied index reaches the current commit index.
   * Used for {@code READ_YOUR_WRITES} consistency: the client provides a commit index bookmark
   * from a previous write, and the replica waits until that entry has been applied locally
   * before serving the read.
   * <p>
   * Uses {@link #applyNotifier} (notified by {@link ArcadeStateMachine#applyTransaction})
   * with a timeout of {@link #quorumTimeout} milliseconds. If the deadline is reached before
   * catch-up, the method returns silently (reads may be slightly stale rather than failing).
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

  public long getLastAppliedIndex() {
    if (raftServer == null)
      return -1;
    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getInfo().getLastAppliedIndex();
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

  public long getCurrentTerm() {
    if (raftServer == null)
      return -1;
    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getInfo().getCurrentTerm();
    } catch (final IOException e) {
      return -1;
    }
  }

  public List<Map<String, Object>> getFollowerStates() {
    if (raftServer == null || !isLeader())
      return List.of();
    try {
      final var division = raftServer.getDivision(raftGroup.getGroupId());
      final var info = division.getInfo();

      final var roleInfo = info.getRoleInfoProto();
      if (!roleInfo.hasLeaderInfo())
        return List.of();

      final List<RaftProtos.ServerRpcProto> followerInfos = roleInfo.getLeaderInfo().getFollowerInfoList();
      final long[] matchIndices = info.getFollowerMatchIndices();
      final long[] nextIndices = info.getFollowerNextIndices();

      // If sizes diverge, a membership change happened between calls - correlate what we can safely
      final int safeSize = Math.min(followerInfos.size(), Math.min(matchIndices.length, nextIndices.length));

      final List<Map<String, Object>> result = new ArrayList<>(safeSize);
      for (int i = 0; i < safeSize; i++) {
        final String peerId = followerInfos.get(i).getId().getId().toStringUtf8();
        final long lastRpcElapsedMs = followerInfos.get(i).getLastRpcElapsedTimeMs();
        final Map<String, Object> state = new LinkedHashMap<>();
        state.put("peerId", peerId);
        state.put("matchIndex", matchIndices[i]);
        state.put("nextIndex", nextIndices[i]);
        state.put("lastRpcElapsedMs", lastRpcElapsedMs);
        result.add(state);
      }
      return result;
    } catch (final IOException e) {
      return List.of();
    }
  }

  /**
   * Prints an ASCII table showing the current cluster configuration.
   * Called on leader changes so the operator can see the cluster state at a glance.
   */
  public void printClusterConfiguration() {
    if (!isLeader())
      return;

    try {
      final RaftPeerId leaderId = getLeaderId();
      final String leaderPeerId = leaderId != null ? leaderId.toString() : "";
      final long term = getCurrentTerm();
      final long commitIndex = getCommitIndex();
      final Collection<RaftPeer> peers = getLivePeers();
      if (peers.isEmpty())
        return;

      // Collect follower replication state (only available on leader)
      final Map<String, long[]> followerState = new HashMap<>();
      for (final Map<String, Object> f : getFollowerStates()) {
        final String peerId = (String) f.get("peerId");
        final long matchIndex = (Long) f.get("matchIndex");
        final long lastRpcMs = (Long) f.get("lastRpcElapsedMs");
        followerState.put(peerId, new long[]{matchIndex, lastRpcMs});
      }

      // Build table rows
      final List<String[]> rows = new ArrayList<>();
      for (final RaftPeer peer : peers) {
        final String peerId = peer.getId().toString();
        final boolean isPeerLeader = peerId.equals(leaderPeerId);
        final String role = isPeerLeader ? "Leader" : "Follower";
        final String address = peer.getAddress();

        String lagStr = "";
        String latencyStr = "";
        if (!isPeerLeader) {
          final long[] state = followerState.get(peerId);
          if (state != null) {
            final long lag = commitIndex - state[0];
            lagStr = lag > 0 ? String.valueOf(lag) : "0";
            final long elapsedMs = state[1];
            final long heartbeatInterval =
                configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN) / 2;
            if (elapsedMs <= heartbeatInterval)
              latencyStr = elapsedMs + " ms";
          }
        }

        rows.add(new String[]{peerId, address, role, lagStr, latencyStr});
      }

      // Calculate column widths
      final String[] headers = {"SERVER", "ADDRESS", "ROLE", "LAG", "LATENCY"};
      final int[] widths = new int[headers.length];
      for (int i = 0; i < headers.length; i++)
        widths[i] = headers[i].length();
      for (final String[] row : rows)
        for (int i = 0; i < row.length; i++)
          widths[i] = Math.max(widths[i], row[i].length());

      // Format table
      final StringBuilder sb = new StringBuilder();
      sb.append(String.format("CLUSTER CONFIGURATION (term=%d, commitIndex=%d)%n", term, commitIndex));

      appendSeparator(sb, widths);
      appendRow(sb, widths, headers);
      appendSeparator(sb, widths);
      for (final String[] row : rows)
        appendRow(sb, widths, row);
      appendSeparator(sb, widths);

      final String output = sb.toString();

      // Only print if the configuration actually changed
      final int hash = output.hashCode();
      if (hash == lastClusterConfigHash)
        return;
      lastClusterConfigHash = hash;

      LogManager.instance().log(this, Level.WARNING, "%s", output);

    } catch (final Exception e) {
      HALog.log(this, HALog.BASIC, "Error printing cluster configuration: %s", e.getMessage());
    }
  }

  private static void appendSeparator(final StringBuilder sb, final int[] widths) {
    sb.append('+');
    for (final int w : widths)
      sb.append('-').append("-".repeat(w)).append("-+");
    sb.append('\n');
  }

  private static void appendRow(final StringBuilder sb, final int[] widths, final String[] values) {
    sb.append('|');
    for (int i = 0; i < widths.length; i++)
      sb.append(' ').append(String.format("%-" + widths[i] + "s", values[i])).append(" |");
    sb.append('\n');
  }

  void tryAutoJoinCluster() {
    final long jitterMs = Math.abs(localPeerId.hashCode() % 3000L);
    if (jitterMs > 0) {
      HALog.log(this, HALog.BASIC, "K8s auto-join: waiting %dms jitter before probing...", jitterMs);
      try {
        Thread.sleep(jitterMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    HALog.log(this, HALog.BASIC, "K8s auto-join: attempting to join existing cluster...");

    for (final RaftPeer peer : raftGroup.getPeers()) {
      if (peer.getId().equals(localPeerId))
        continue;

      try {
        final RaftProperties tempProps = new RaftProperties();
        RaftServerConfigKeys.Rpc.setTimeoutMin(tempProps, TimeDuration.valueOf(3, TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(tempProps, TimeDuration.valueOf(5, TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setRequestTimeout(tempProps, TimeDuration.valueOf(5, TimeUnit.SECONDS));

        final RaftGroup targetGroup = RaftGroup.valueOf(raftGroup.getGroupId(), peer);
        try (final RaftClient tempClient = RaftClient.newBuilder()
            .setRaftGroup(targetGroup)
            .setProperties(tempProps)
            .build()) {

          final var groupInfo = tempClient.getGroupManagementApi(peer.getId())
              .info(raftGroup.getGroupId());

          if (groupInfo != null && groupInfo.isSuccess()) {
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
                HALog.log(this, HALog.BASIC,
                    "K8s auto-join: adding self (%s) to existing cluster via peer %s",
                    localPeerId, peer.getId());

                RaftPeer localPeer = null;
                for (final RaftPeer p : raftGroup.getPeers())
                  if (p.getId().equals(localPeerId)) {
                    localPeer = p;
                    break;
                  }

                if (localPeer != null) {
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
                    LogManager.instance().log(this, Level.WARNING,
                        "K8s auto-join: setConfiguration rejected: %s",
                        joinReply.getException() != null ? joinReply.getException().getMessage() : "unknown");
                  else
                    HALog.log(this, HALog.BASIC,
                        "K8s auto-join: successfully joined cluster with %d peers", newPeers.size());
                }
              } else {
                HALog.log(this, HALog.BASIC, "K8s auto-join: already a member of the cluster");
              }
            }
            return;
          }
        }
      } catch (final Exception e) {
        HALog.log(this, HALog.DETAILED,
            "K8s auto-join: peer %s not reachable (%s), trying next...",
            peer.getId(), e.getMessage());
      }
    }

    LogManager.instance().log(this, Level.WARNING,
        "K8s auto-join: no existing cluster found, starting as new cluster. "
            + "If other nodes exist but are unreachable, this may create a split-brain. Verify network connectivity");
  }

  boolean hasExistingRaftStorage() {
    final File storageDir = new File(arcadeServer.getRootPath() + File.separator + "raft-storage-" + localPeerId);
    if (!storageDir.exists())
      return false;
    final File[] subdirs = storageDir.listFiles(f -> f.isDirectory() && !f.getName().equals("lost+found"));
    return subdirs != null && subdirs.length > 0;
  }

  /**
   * Initialises the cluster token used for inter-node request forwarding.
   * <ul>
   *   <li>If {@code HA_CLUSTER_TOKEN} is already set in config, nothing changes.</li>
   *   <li>If the token file exists in {@code storageDir}, its value is loaded into config.</li>
   *   <li>Otherwise a new UUID is generated, written to the file, and set in config.</li>
   * </ul>
   */
  static void initClusterToken(final ContextConfiguration configuration, final File storageDir) {
    final String configured = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
    if (configured != null && !configured.isBlank())
      return;

    // Derive a deterministic cluster token from the cluster name and root password.
    // All nodes in the same cluster share the same cluster name and root password, so
    // they will all compute the same token — a requirement for inter-node HTTP forwarding.
    // A random per-node token (the previous approach) caused authentication failures
    // because each node stored its token in its own private Raft storage directory.
    final String clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    final String rootPassword = configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PASSWORD);
    final String password = clusterName + ":" + (rootPassword != null ? rootPassword : "");
    try {
      final byte[] salt = ("arcadedb-cluster-token:" + clusterName).getBytes(StandardCharsets.UTF_8);
      final javax.crypto.SecretKeyFactory factory = javax.crypto.SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
      final javax.crypto.spec.PBEKeySpec spec = new javax.crypto.spec.PBEKeySpec(
          password.toCharArray(), salt, 100_000, 256);
      final byte[] hash = factory.generateSecret(spec).getEncoded();
      final String token = java.util.HexFormat.of().formatHex(hash);
      configuration.setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, token);
    } catch (final Exception e) {
      throw new RuntimeException("Failed to derive cluster token", e);
    }
  }

  /**
   * Starts a periodic task that updates the {@link ClusterMonitor} with the leader's commit index.
   * Called when this node becomes the Raft leader.
   */
  void startLagMonitor() {
    if (lagMonitorExecutor != null)
      return;
    lagMonitorExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "arcadedb-raft-lag-monitor");
      t.setDaemon(true);
      return t;
    });
    lagMonitorExecutor.scheduleAtFixedRate(this::checkReplicaLag, 5, 5, TimeUnit.SECONDS);
  }

  /**
   * Stops the periodic lag monitoring task. Called when this node loses leadership.
   */
  void stopLagMonitor() {
    if (lagMonitorExecutor != null) {
      lagMonitorExecutor.shutdownNow();
      lagMonitorExecutor = null;
    }
  }

  private void checkReplicaLag() {
    try {
      final var division = raftServer.getDivision(raftGroup.getGroupId());
      final var info = division.getInfo();
      if (!info.isLeader())
        return;
      final long commitIndex = info.getLastAppliedIndex();
      clusterMonitor.updateLeaderCommitIndex(commitIndex);
    } catch (final Exception e) {
      HALog.log(this, HALog.TRACE, "Error checking replica lag", e);
    }
  }

  private static void deleteRecursive(final File file) {
    if (file.isDirectory()) {
      final File[] children = file.listFiles();
      if (children != null)
        for (final File child : children)
          deleteRecursive(child);
    }
    file.delete();
  }
}
