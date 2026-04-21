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
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.LifeCycle;
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
import java.util.logging.Logger;

/**
 * Manages the lifecycle of the Apache Ratis {@link RaftServer}, {@link RaftClient},
 * and {@link RaftTransactionBroker} for ArcadeDB high availability.
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

  private final ArcadeDBServer          arcadeServer;
  private final ContextConfiguration    configuration;
  private       ArcadeStateMachine      stateMachine;
  private final ClusterMonitor          clusterMonitor;
  private final Quorum                  quorum;
  private final long                    quorumTimeout;
  private final RaftGroup               raftGroup;
  private final RaftPeerId              localPeerId;
  private final Map<RaftPeerId, String> httpAddresses = new HashMap<>();
  private final Map<RaftPeerId, String> peerDisplayNames;
  private final String                  clusterName;

  private          RaftServer                raftServer;
  private          RaftClient                raftClient;
  private          RaftProperties            raftProperties;
  private volatile RaftTransactionBroker     transactionBroker;
  private          RaftClusterStatusExporter statusExporter;
  private          ScheduledExecutorService  lagMonitorExecutor;
  private final    Object                    leaderChangeNotifier  = new Object();
  private final    Object                    applyNotifier         = new Object();
  private          RaftClusterManager        clusterManager;
  private final    Object                    recoveryLock          = new Object();
  private volatile boolean                   shutdownRequested     = false;
  private volatile LifeCycle.State           forcedStateForTesting = null;
  private          HealthMonitor             healthMonitor;
  private          ClusterTokenProvider      tokenProvider;
  private volatile int                       restartFailureCount   = 0;

  public RaftHAServer(final ArcadeDBServer arcadeServer, final ContextConfiguration configuration) {
    this.arcadeServer = arcadeServer;
    this.configuration = configuration;

    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    final String clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    this.clusterName = clusterName;
    final long lagWarningThreshold = configuration.getValueAsLong(GlobalConfiguration.HA_REPLICATION_LAG_WARNING);
    final int raftPort = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_PORT);

    final RaftPeerAddressResolver.ParsedPeerList parsed = RaftPeerAddressResolver.parsePeerList(serverList, raftPort);
    List<RaftPeer> peers = parsed.peers();
    final String serverName = arcadeServer.getServerName();

    this.httpAddresses.putAll(parsed.httpAddresses());
    this.localPeerId = RaftPeerAddressResolver.findLocalPeerId(peers, serverName, arcadeServer);

    // If this node is configured as a replica, override its Raft peer priority to 0
    // so Ratis never elects it as leader (useful for read-scale or witness nodes).
    final String serverRole = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_ROLE);
    if ("replica".equalsIgnoreCase(serverRole)) {
      final List<RaftPeer> rebuilt = new ArrayList<>(peers.size());
      for (final RaftPeer p : peers) {
        if (p.getId().equals(localPeerId)) {
          rebuilt.add(RaftPeer.newBuilder().setId(p.getId()).setAddress(p.getAddress()).setPriority(0).build());
          LogManager.instance().log(this, Level.INFO,
              "Node configured as replica (priority=0, will not become leader): %s", localPeerId);
        } else
          rebuilt.add(p);
      }
      peers = Collections.unmodifiableList(rebuilt);
    }

    this.raftGroup = RaftGroup.valueOf(
        RaftGroupId.valueOf(UUID.nameUUIDFromBytes(clusterName.getBytes(StandardCharsets.UTF_8))),
        peers);

    // Build human-readable display names: "ServerName-N (host:httpPort)"
    final int separatorIdx = RaftPeerAddressResolver.findLastSeparatorIndex(serverName);
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

    this.clusterManager = new RaftClusterManager(this);
    this.statusExporter = new RaftClusterStatusExporter(this, this.clusterMonitor);

    LogManager.instance().log(this, Level.INFO,
        "RaftHAServer configured: cluster='%s', localPeer='%s', peers=%d",
        clusterName, localPeerId, peers.size());
  }

  /**
   * Returns the cluster token, deriving it if not yet initialized.
   * The token is used for inter-node authentication.
   */
  public String getClusterToken() {
    return tokenProvider != null ? tokenProvider.getClusterToken() : null;
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
   * Creates and starts the Ratis RaftServer and RaftClient.
   */
  public void start() throws IOException {
    // Suppress verbose Ratis internal logs - operators see ArcadeDB-level cluster events instead
    Logger.getLogger("org.apache.ratis").setLevel(Level.WARNING);

    final RaftProperties properties = RaftPropertiesBuilder.build(configuration);

    final File storageDir = new File(arcadeServer.getRootPath() + File.separator + "raft-storage-" + localPeerId);
    // Only delete existing Raft storage when persistence is not requested.
    // Persistent mode (HA_RAFT_PERSIST_STORAGE=true) is used in tests that restart nodes
    // within a single test run, so the Raft log survives across stop/start calls.
    final boolean persistStorage = configuration.getValueAsBoolean(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE);
    if (storageDir.exists() && !persistStorage)
      deleteRecursive(storageDir);
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    this.tokenProvider = new ClusterTokenProvider(configuration);
    this.tokenProvider.initClusterToken();

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

    final Parameters parameters = buildParameters(configuration);

    raftServer = RaftServer.newBuilder()
        .setServerId(localPeerId)
        .setGroup(raftGroup)
        .setStateMachine(stateMachine)
        .setProperties(properties)
        .setParameters(parameters)
        .setOption(startupOption)
        .build();

    raftServer.start();

    this.raftProperties = properties;

    raftClient = buildRaftClient(raftGroup, properties);

    LogManager.instance()
        .log(this, Level.INFO, "Raft cluster joined: %d nodes %s", peerDisplayNames.size(), peerDisplayNames.values());

    final int batchSize = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_BATCH_SIZE);
    final int queueSize = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_QUEUE_SIZE);
    final int offerTimeout = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_OFFER_TIMEOUT);
    transactionBroker = new RaftTransactionBroker(raftClient, quorum, quorumTimeout, batchSize, queueSize, offerTimeout);

    // K8s auto-join: if running in Kubernetes with no existing storage, try to join an existing cluster
    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S) && !hadExistingStorage)
      new KubernetesAutoJoin(arcadeServer, raftGroup, localPeerId, raftProperties).tryAutoJoin();

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

      final int maxRetries = configuration.getValueAsInteger(GlobalConfiguration.HA_RATIS_RESTART_MAX_RETRIES);
      if (restartFailureCount >= maxRetries) {
        LogManager.instance().log(this, Level.SEVERE,
            "Ratis restart failed %d consecutive times (max=%d). Stopping server for cluster-level recovery",
            restartFailureCount, maxRetries);
        final Thread stopThread = new Thread(() -> {
          try { arcadeServer.stop(); } catch (final Exception ignored) {}
        }, "arcadedb-restart-failure-stop");
        stopThread.setDaemon(true);
        stopThread.start();
        return;
      }

      final RaftClient oldClient = this.raftClient;
      final RaftServer oldServer = this.raftServer;
      final RaftTransactionBroker oldBroker = this.transactionBroker;

      try {
        if (oldBroker != null)
          oldBroker.stop();
      } catch (final Throwable t) {
        LogManager.instance().log(this, Level.FINE, "Error closing old broker: %s", t, t.getMessage());
      }
      try {
        if (oldClient != null)
          oldClient.close();
      } catch (final Throwable t) {
        LogManager.instance().log(this, Level.FINE, "Error closing old client: %s", t, t.getMessage());
      }
      try {
        if (oldServer != null)
          oldServer.close();
      } catch (final Throwable t) {
        LogManager.instance().log(this, Level.FINE, "Error closing old server: %s", t, t.getMessage());
      }

      try {
        this.stateMachine = new ArcadeStateMachine();
        this.stateMachine.setServer(arcadeServer);

        final RaftProperties properties = RaftPropertiesBuilder.build(configuration);
        final File storageDir = new File(arcadeServer.getRootPath() + File.separator + "raft-storage-" + localPeerId);
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

        this.raftServer = RaftServer.newBuilder()
            .setServerId(localPeerId)
            .setGroup(raftGroup)
            .setStateMachine(stateMachine)
            .setProperties(properties)
            .setParameters(buildParameters(configuration))
            .setOption(RaftStorage.StartupOption.RECOVER)
            .build();
        this.raftServer.start();
        this.raftProperties = properties;
        this.raftClient = buildRaftClient(raftGroup, properties);

        final int batchSize = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_BATCH_SIZE);
        final int queueSize = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_QUEUE_SIZE);
        final int offerTimeout = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_OFFER_TIMEOUT);
        this.transactionBroker = new RaftTransactionBroker(raftClient, quorum, quorumTimeout, batchSize, queueSize,
            offerTimeout);

        restartFailureCount = 0;
        HALog.log(this, HALog.BASIC, "Ratis recovered successfully");
      } catch (final Throwable t) {
        restartFailureCount++;
        LogManager.instance().log(this, Level.SEVERE,
            "HealthMonitor recovery failed (attempt %d/%d): %s",
            t, restartFailureCount,
            configuration.getValueAsInteger(GlobalConfiguration.HA_RATIS_RESTART_MAX_RETRIES),
            t.getMessage());
      }
    }
  }

  /**
   * Package-private test hook. Next call to getRaftLifeCycleState() returns this value, then clears.
   */
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
    if (transactionBroker != null) {
      transactionBroker.stop();
      transactionBroker = null;
    }

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S))
      leaveCluster();

    // Suppress noisy Ratis gRPC warnings during shutdown (AlreadyClosedException, CANCELLED streams).
    // These are harmless - internal replication threads take a moment to notice the server is closed.
    final String[] noisyLoggers = {
        "org.apache.ratis.grpc.server.GrpcLogAppender",
        "org.apache.ratis.grpc.server.GrpcServerProtocolService"
    };
    final Level[] previousLevels = new Level[noisyLoggers.length];
    for (int i = 0; i < noisyLoggers.length; i++) {
      final Logger logger = Logger.getLogger(noisyLoggers[i]);
      previousLevels[i] = logger.getLevel();
      logger.setLevel(Level.SEVERE);
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
        Logger.getLogger(noisyLoggers[i]).setLevel(previousLevels[i]);
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
   *
   * @return true if the transfer succeeded
   */
  public boolean transferLeadership(final long timeoutMs) {
    return clusterManager.transferLeadership(timeoutMs);
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

  public RaftTransactionBroker getTransactionBroker() {
    return transactionBroker;
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

    // Stop the transaction broker FIRST so its flusher thread is no longer using the old raftClient.
    // Create the replacement broker before stopping the old one to minimize the window where
    // no broker is available to concurrent callers (the field is volatile).
    final RaftClient oldClient = raftClient;

    raftClient = buildRaftClient(raftGroup, raftProperties, knownLeaderId);

    if (transactionBroker != null) {
      final int batchSize = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_BATCH_SIZE);
      final int queueSize = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_QUEUE_SIZE);
      final int offerTimeout = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_OFFER_TIMEOUT);
      final RaftTransactionBroker oldBroker = transactionBroker;
      transactionBroker = new RaftTransactionBroker(raftClient, quorum, quorumTimeout, batchSize, queueSize,
          offerTimeout);
      oldBroker.stop();
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

  public ContextConfiguration getConfiguration() {
    return configuration;
  }

  public ArcadeDBServer getServer() {
    return arcadeServer;
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
    clusterManager.addPeer(peerId, address);
  }

  public void removePeer(final String peerId) {
    clusterManager.removePeer(peerId);
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

  public void transferLeadership(final String targetPeerId, final long timeoutMs) {
    clusterManager.transferLeadership(targetPeerId, timeoutMs);
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
    clusterManager.leaveCluster();
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

  /**
   * Sends a ReadIndex RPC to the Raft leader and returns the confirmed commit index.
   * The leader replies only after confirming its lease with a majority, guaranteeing
   * that the returned index is committed and linearizable.
   *
   * @param expectSelfIsLeader if true and the reply indicates this node is not leader,
   *                           a {@link ReplicationException} is thrown with leader info
   */
  public long fetchReadIndex(final boolean expectSelfIsLeader) {
    try {
      final RaftClientReply reply = raftClient.io().sendReadOnly(Message.EMPTY);
      if (reply.isSuccess())
        return reply.getLogIndex();

      final NotLeaderException nle = reply.getNotLeaderException();
      if (nle != null) {
        final var suggestedLeader = nle.getSuggestedLeader();
        if (expectSelfIsLeader) {
          final String leaderAddr = suggestedLeader != null ? suggestedLeader.getId().toString() : null;
          throw new ReplicationException("Lost leadership during ReadIndex" + (leaderAddr != null ? ", new leader: " + leaderAddr : ""));
        }
        throw new ReplicationException("ReadIndex failed: leader unavailable");
      }
      throw new ReplicationException("ReadIndex failed: " + reply);
    } catch (final ReplicationException e) {
      throw e;
    } catch (final IOException e) {
      throw new ReplicationException("ReadIndex RPC failed: " + e.getMessage(), e);
    }
  }

  /**
   * Ensures linearizable read consistency for the leader. Sends a ReadIndex RPC to
   * confirm the leader lease, then waits for the local state machine to apply up to
   * the confirmed commit index.
   * <p>
   * Throws {@link ReplicationException} if leadership is lost before or after the RPC.
   */
  public void ensureLinearizableRead() {
    final long readIndex = fetchReadIndex(true);
    if (!isLeader())
      throw new ReplicationException("Lost leadership after ReadIndex confirmation");
    waitForAppliedIndex(readIndex);
  }

  /**
   * Ensures linearizable read consistency for a follower. Contacts the leader via
   * ReadIndex RPC to obtain the current commit index, then waits for the local state
   * machine to apply up to that index before allowing the read to proceed.
   */
  public void ensureLinearizableFollowerRead() {
    final long readIndex = fetchReadIndex(false);
    waitForAppliedIndex(readIndex);
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
    statusExporter.printClusterConfiguration();
  }

  boolean hasExistingRaftStorage() {
    final File storageDir = new File(arcadeServer.getRootPath() + File.separator + "raft-storage-" + localPeerId);
    if (!storageDir.exists())
      return false;
    final File[] subdirs = storageDir.listFiles(f -> f.isDirectory() && !f.getName().equals("lost+found"));
    return subdirs != null && subdirs.length > 0;
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
    lagMonitorExecutor.scheduleAtFixedRate(statusExporter::checkReplicaLag, 5, 5, TimeUnit.SECONDS);
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

  /**
   * Builds a {@link Parameters} instance with the gRPC services customizer installed.
   * The customizer adds a {@link PeerAddressAllowlistFilter} that rejects inbound Raft gRPC
   * connections from IPs not listed in {@code arcadedb.ha.serverList}.
   */
  private static Parameters buildParameters(final ContextConfiguration configuration) {
    final Parameters parameters = new Parameters();
    if (!configuration.getValueAsBoolean(GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED))
      return parameters;

    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    final long refreshMs = configuration.getValueAsLong(GlobalConfiguration.HA_GRPC_ALLOWLIST_REFRESH_MS);
    final List<String> peerHosts = PeerAddressAllowlistFilter.extractPeerHosts(serverList);
    if (peerHosts.isEmpty()) {
      LogManager.instance().log(RaftHAServer.class, Level.WARNING,
          "arcadedb.ha.peerAllowlist.enabled=true but arcadedb.ha.serverList is empty; allowlist not installed");
      return parameters;
    }
    final PeerAddressAllowlistFilter allowlistFilter = new PeerAddressAllowlistFilter(peerHosts, refreshMs);
    GrpcConfigKeys.Server.setServicesCustomizer(parameters, new RaftGrpcServicesCustomizer(allowlistFilter));
    return parameters;
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
