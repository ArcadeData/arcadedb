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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
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
import org.apache.ratis.protocol.SetConfigurationRequest;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;
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
public class RaftHAServer implements com.arcadedb.server.ha.HAPlugin {

  /**
   * Quorum modes supported with Ratis.
   */
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

  // PBKDF2 parameters for cluster token derivation (initClusterToken).
  // 100k iterations is the OWASP 2023 recommendation for PBKDF2-HMAC-SHA256.
  private static final int PBKDF2_ITERATIONS      = 100_000;
  private static final int PBKDF2_KEY_LENGTH_BITS = 256;

  // K8s auto-join parameters (tryAutoJoinCluster). Jitter is kept as a secondary mitigation
  // even though Mode.ADD is atomic, to avoid thundering-herd probe storms against the leader.
  private static final long AUTO_JOIN_JITTER_MAX_MS        = 3000L;
  private static final int  AUTO_JOIN_RPC_TIMEOUT_MIN_SECS = 3;
  private static final int  AUTO_JOIN_RPC_TIMEOUT_MAX_SECS = 5;

  // Leadership transfer timeout (ms). Generous to allow log catch-up on the target peer
  // before it can accept the leadership role.
  private static final long LEADERSHIP_TRANSFER_TIMEOUT_MS = 10_000L;

  // After requesting leadership transfer, how long to wait for the leader change notification
  // before proceeding with shutdown. Short because the transfer itself has its own timeout.
  private static final long LEADERSHIP_CHANGE_WAIT_MS = 5_000L;

  // Client retry policy for the RaftClient used to submit transactions.
  // Exponential backoff from 100ms to 5s covers transient leader unavailability.
  private static final long CLIENT_RETRY_BASE_SLEEP_MS = 100L;
  private static final long CLIENT_RETRY_MAX_SLEEP_SECS = 5L;

  // Lag monitor: checks follower replication lag every N seconds.
  private static final int LAG_MONITOR_INITIAL_DELAY_SECS = 5;
  private static final int LAG_MONITOR_INTERVAL_SECS      = 5;

  // Health monitor: checks Ratis server lifecycle state and restarts if CLOSED.
  // 3s interval balances quick recovery against CPU overhead of the lifecycle check.
  private static final int HEALTH_MONITOR_INITIAL_DELAY_SECS = 5;
  private static final int HEALTH_MONITOR_INTERVAL_SECS      = 3;

  // Ratis RPC and connection timeouts (buildRaftProperties).
  // Server-side RPC request timeout: how long the leader waits for a follower AppendEntries response.
  private static final int RPC_REQUEST_TIMEOUT_SECS = 10;
  // Slowness/close thresholds: how long before a follower is marked slow or its connection is closed.
  // Set high (5 min) to survive network partitions without prematurely evicting followers.
  private static final int FOLLOWER_SLOWNESS_TIMEOUT_SECS = 300;
  private static final int FOLLOWER_CLOSE_THRESHOLD_SECS  = 300;

  // Maximum log entries per AppendEntries RPC batch. Balances throughput vs. memory per batch.
  private static final int APPEND_ENTRIES_MAX_ELEMENTS = 256;

  // Leader lease ratio: fraction of the election timeout during which the leader considers
  // its lease valid for serving linearizable reads without a round-trip. 0.9 means the lease
  // expires at 90% of the election timeout, leaving a 10% safety margin.
  private static final double LEADER_LEASE_TIMEOUT_RATIO = 0.9;

  private              ArcadeDBServer       server;
  private              ContextConfiguration configuration;
  private              RaftGroup            raftGroup;
  private              RaftPeerId           localPeerId;
  private              Quorum               quorum;
  private              long                 quorumTimeout;
  private final        Map<String, String>  peerHttpAddresses        = new ConcurrentHashMap<>();
  private final        Set<String>          derivedAddressWarned     = ConcurrentHashMap.newKeySet();
  private volatile     String               clusterToken;
  private              boolean              active;

  private          RaftServer               raftServer;
  private volatile RaftClient               raftClient;
  private          RaftProperties           raftProperties;
  private          ArcadeDBStateMachine     stateMachine;
  private          ClusterMonitor           clusterMonitor;
  private          RaftGroupCommitter       groupCommitter;
  private final    ReentrantLock            applyLock            = new ReentrantLock();
  private final    Condition                applyCondition       = applyLock.newCondition();
  private final    AtomicInteger            applyWaiterCount     = new AtomicInteger();
  private final    Object                   leaderChangeNotifier = new Object();
  private volatile int                      lastClusterConfigHash;
  /**
   * Set to false when this node becomes leader, true once all committed entries
   * have been applied to the state machine. Reads on the leader wait for this
   * flag before returning results, preventing stale reads during leadership transitions.
   */
  private volatile boolean                  leaderReady          = true;
  private          ScheduledExecutorService lagMonitorExecutor;
  private          ScheduledExecutorService healthMonitorExecutor;
  private volatile int                      restartFailureCount;

  /**
   * ServiceLoader requires a no-arg constructor.
   */
  public RaftHAServer() {
  }

  /**
   * Constructor for programmatic creation (e.g. in tests).
   */
  public RaftHAServer(final ArcadeDBServer server, final ContextConfiguration configuration) {
    configure(server, configuration);
  }

  @Override
  public void configure(final ArcadeDBServer server, final ContextConfiguration configuration) {
    if (!configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
      return;

    this.active = true;
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
    final long lagThreshold = configuration.getValueAsLong(GlobalConfiguration.HA_REPLICATION_LAG_WARNING);
    this.clusterMonitor = new ClusterMonitor(lagThreshold);

    // Register this plugin and the database wrapper with the server
    server.setHA(this);
    server.setDatabaseWrapper(db -> new ReplicatedDatabase(server, db));
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  public PluginInstallationPriority getInstallationPriority() {
    return PluginInstallationPriority.AFTER_HTTP_ON;
  }

  @Override
  public void registerAPI(final com.arcadedb.server.http.HttpServer httpServer,
      final io.undertow.server.handlers.PathHandler routes) {
    // Snapshot endpoint (serves database files as ZIP for follower resync)
    routes.addPrefixPath("/ha/snapshot", new SnapshotHttpHandler(httpServer));

    // Dedicated REST endpoints for HA cluster management
    routes.addExactPath("/api/v1/cluster", new GetClusterHandler(httpServer, this));
    routes.addExactPath("/api/v1/cluster/peer", new PostAddPeerHandler(httpServer, this));
    routes.addPrefixPath("/api/v1/cluster/peer/", new DeletePeerHandler(httpServer, this));
    routes.addExactPath("/api/v1/cluster/leader", new PostTransferLeaderHandler(httpServer, this));
    routes.addExactPath("/api/v1/cluster/stepdown", new PostStepDownHandler(httpServer, this));
    routes.addExactPath("/api/v1/cluster/leave", new PostLeaveHandler(httpServer, this));
    routes.addPrefixPath("/api/v1/cluster/verify/", new PostVerifyDatabaseHandler(httpServer, this));
  }

  @Override
  public void recoverBeforeDatabaseLoad(final java.nio.file.Path databaseDirectory) {
    ArcadeDBStateMachine.recoverPendingSnapshotSwaps(databaseDirectory);
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
    // Check both the server's ContextConfiguration and the global default (system property)
    String rootPassword = configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PASSWORD);
    if (rootPassword == null || rootPassword.isEmpty())
      rootPassword = GlobalConfiguration.SERVER_ROOT_PASSWORD.getValueAsString();
    if (rootPassword == null || rootPassword.isEmpty())
      throw new ConfigurationException(
          "Cannot start HA mode without authentication: the auto-derived cluster token requires a root password. "
              + "Set arcadedb.server.rootPassword or provide an explicit arcadedb.ha.clusterToken");
    // Domain separation: the cluster name appears in both the PBKDF2 password and the salt.
    // In the password (clusterName + ":" + rootPassword) it ensures that two clusters with the
    // same root password produce different tokens. In the salt ("arcadedb-cluster-token:" + clusterName)
    // it provides a fixed, cluster-specific salt so that all nodes in the same cluster derive the
    // same token deterministically without sharing state. The "arcadedb-cluster-token:" prefix
    // prevents the salt from colliding with salts used for other purposes.
    final String password = clusterName + ":" + rootPassword;
    try {
      final byte[] salt = ("arcadedb-cluster-token:" + clusterName).getBytes(StandardCharsets.UTF_8);
      final SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
      final PBEKeySpec spec = new PBEKeySpec(
          password.toCharArray(), salt, PBKDF2_ITERATIONS, PBKDF2_KEY_LENGTH_BITS);
      final byte[] hash = factory.generateSecret(spec).getEncoded();
      spec.clearPassword();
      this.clusterToken = HexFormat.of().formatHex(hash);
    } catch (final Exception e) {
      throw new RuntimeException("Failed to derive cluster token", e);
    }

    if ("production".equals(configuration.getValueAsString(GlobalConfiguration.SERVER_MODE)))
      LogManager.instance().log(this, Level.WARNING,
          "Using auto-derived cluster token. Changing root password does NOT rotate this token. "
              + "To explicitly rotate, set arcadedb.ha.clusterToken=<new-value> and restart all nodes");
  }

  @Override
  public void startService() {
    if (!active)
      return;

    LogManager.instance().log(this, Level.INFO, "Starting Ratis HA service (cluster=%s, peers=%s, quorum=%s)...",
        configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME), raftGroup.getPeers(), quorum);

    if ("production".equals(configuration.getValueAsString(GlobalConfiguration.SERVER_MODE))
        && !configuration.getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL))
      LogManager.instance().log(this, Level.WARNING,
          "Inter-node snapshot and proxy traffic uses plain HTTP. Cluster token and database data are transmitted "
              + "unencrypted. Set arcadedb.ssl.enabled=true or deploy behind a secure network (VPN, private subnet)");

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S)) {
      LogManager.instance().log(this, Level.INFO,
          "K8s mode enabled. The Raft gRPC transport does not enforce cluster-token authentication. "
              + "Use a Kubernetes NetworkPolicy to restrict gRPC port access to only ArcadeDB StatefulSet pods");

      // Strongest warning for the most dangerous combination: K8s mode + gRPC bound to all interfaces.
      // In this configuration, any pod in the K8s cluster (not just ArcadeDB pods) can connect to
      // the Raft gRPC port and inject log entries without authentication.
      final String incomingHost = configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST);
      if ("0.0.0.0".equals(incomingHost) || "::".equals(incomingHost))
        LogManager.instance().log(this, Level.SEVERE,
            "SECURITY: gRPC Raft port is bound to all interfaces (%s) with K8s mode enabled. "
                + "Without a NetworkPolicy, ANY pod in the cluster can inject Raft log entries. "
                + "Either restrict arcadedb.ha.replicationIncomingHost to the pod IP, or apply a NetworkPolicy "
                + "that limits ingress on port %d to ArcadeDB StatefulSet pods only",
            incomingHost, parseFirstPort(configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS)));
    }

    // Derive the cluster token eagerly at startup rather than lazily on the first request.
    // PBKDF2 with 100k iterations is expensive and would block a request thread.
    initClusterToken();

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

      // Create a client for submitting transactions. Set leader to self since only the leader
      // uses this client (via RaftGroupCommitter). Without this, the client picks a random peer
      // and gets a noisy NotLeaderException on the first request before redirecting.
      raftClient = buildRaftClient();

      // In K8s mode: if this is a new server (no existing storage) and other servers might already
      // be running, try to add ourselves to the existing cluster via AdminApi.
      // This handles StatefulSet scale-up where new pods need to join the existing Raft group.
      if (!storageExists && configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S))
        tryAutoJoinCluster();

      groupCommitter = new RaftGroupCommitter(this,
          configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_BATCH_SIZE));
      groupCommitter.start();
      startLagMonitor();
      startRatisHealthMonitor();

      LogManager.instance().log(this, Level.INFO, "Ratis HA service started (serverId=%s)", localPeerId);

    } catch (final IOException e) {
      throw new ConfigurationException("Failed to start Ratis HA service", e);
    }
  }

  /**
   * Returns the lifecycle state of the Ratis server (RUNNING, CLOSING, CLOSED, etc.).
   */
  public org.apache.ratis.util.LifeCycle.State getRaftLifeCycleState() {
    if (raftServer == null)
      return org.apache.ratis.util.LifeCycle.State.CLOSED;
    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getInfo().getLifeCycleState();
    } catch (final Exception e) {
      return raftServer.getLifeCycleState();
    }
  }

  /**
   * Restarts the Ratis server if it has entered CLOSED or CLOSING state (e.g., after a network
   * partition caused gRPC connection failures). The existing state machine is reused since the
   * database state is on disk. The Ratis log and metadata are recovered from the persisted storage.
   */
  public synchronized void restartRatisIfNeeded() {
    if (raftServer == null)
      return;

    // Check the group-specific RaftServerImpl state, not the RaftServerProxy state.
    // The proxy can be RUNNING while the inner group impl is CLOSED after a network partition.
    org.apache.ratis.util.LifeCycle.State state;
    try {
      state = raftServer.getDivision(raftGroup.getGroupId()).getInfo().getLifeCycleState();
    } catch (final Exception e) {
      // getDivision can throw if the group is already removed
      state = raftServer.getLifeCycleState();
    }
    if (state != org.apache.ratis.util.LifeCycle.State.CLOSED && state != org.apache.ratis.util.LifeCycle.State.CLOSING) {
      restartFailureCount = 0; // Reset on healthy state
      return;
    }

    // After 10 consecutive failures, the problem is persistent (port conflict, bad storage,
    // full disk). Stop the server so the cluster can heal (other nodes take over) and
    // orchestrators (K8s, systemd) can restart the process with a clean state.
    if (restartFailureCount >= 10) {
      LogManager.instance().log(this, Level.SEVERE,
          "Ratis restart failed %d consecutive times. Stopping server for cluster-level recovery",
          restartFailureCount);
      final Thread stopThread = new Thread(() -> {
        try { server.stop(); } catch (final Exception ignored) {}
      }, "arcadedb-restart-failure-stop");
      stopThread.setDaemon(true);
      stopThread.start();
      return;
    }

    LogManager.instance().log(this, Level.WARNING,
        "Ratis server is in %s state, restarting for partition recovery (attempt %d)...",
        state, restartFailureCount + 1);

    try {
      try {
        raftClient.close();
      } catch (final Exception ignored) {
      }
      try {
        raftServer.close();
      } catch (final Exception ignored) {
      }

      // Close the old state machine to shut down its lifecycle executor and cancel
      // any in-flight snapshot downloads or async tasks before replacing it.
      final ArcadeDBStateMachine oldStateMachine = stateMachine;
      try {
        oldStateMachine.close();
      } catch (final Exception ignored) {
      }

      // Create a fresh state machine for the restart. The old state machine has a stale
      // lastAppliedTermIndex that conflicts with RECOVER mode's replay. The database state
      // on disk is the source of truth; the new state machine reads it from the snapshot.
      stateMachine = new ArcadeDBStateMachine(server);

      raftServer = RaftServer.newBuilder()
          .setServerId(localPeerId)
          .setStateMachine(stateMachine)
          .setProperties(raftProperties)
          .setGroup(raftGroup)
          .setOption(RaftStorage.StartupOption.RECOVER)
          .build();

      raftServer.start();

      raftClient = buildRaftClient();

      restartFailureCount = 0;
      LogManager.instance().log(this, Level.INFO, "Ratis server restarted successfully after partition recovery");

    } catch (final Exception e) {
      restartFailureCount++;
      if (restartFailureCount >= 10)
        LogManager.instance().log(this, Level.SEVERE,
            "Failed to restart Ratis server after %d attempts. Giving up - manual restart required: %s",
            restartFailureCount, e.getMessage());
      else
        LogManager.instance().log(this, Level.WARNING,
            "Failed to restart Ratis server (attempt %d/10): %s", restartFailureCount, e.getMessage());
    }
  }

  /**
   * Attempts to join an existing Ratis cluster by contacting a peer and adding this server.
   * Used in Kubernetes when a new pod is added via StatefulSet scale-up.
   * If no existing cluster is found (fresh deployment), this is a no-op - the Raft server
   * was already started with the full peer list from HA_SERVER_LIST, so there is no risk
   * of split-brain. All pods share the same Raft group configuration and will elect a
   * leader via normal Raft consensus once a majority becomes reachable.
   *
   * <p><b>Security note:</b> Peer discovery uses DNS resolution of the headless service hostname.
   * The cluster token authenticates HTTP-level operations (snapshot downloads, command proxying)
   * but the Raft gRPC transport does not enforce token-based authentication. Any pod that can
   * resolve the headless service DNS name and reach the gRPC port can participate in Raft
   * consensus. In production Kubernetes deployments, restrict gRPC port access to only pods in
   * the ArcadeDB StatefulSet via a NetworkPolicy. Example:
   * <pre>
   *   apiVersion: networking.k8s.io/v1
   *   kind: NetworkPolicy
   *   metadata:
   *     name: arcadedb-raft-grpc
   *   spec:
   *     podSelector:
   *       matchLabels:
   *         app: arcadedb
   *     ingress:
   *       - from:
   *           - podSelector:
   *               matchLabels:
   *                 app: arcadedb
   *         ports:
   *           - port: 2424    # gRPC/Raft port
   *             protocol: TCP
   * </pre>
   */
  private void tryAutoJoinCluster() {
    // Random jitter (100ms-3s) to spread probe traffic when multiple pods start simultaneously
    // (e.g. K8s Parallel pod management policy or mass restart).
    final long jitterMs = ThreadLocalRandom.current().nextLong(100, AUTO_JOIN_JITTER_MAX_MS);
    HALog.log(this, HALog.BASIC, "K8s auto-join: waiting %dms jitter before probing...", jitterMs);
    try {
      Thread.sleep(jitterMs);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
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
        RaftServerConfigKeys.Rpc.setTimeoutMin(tempProps, TimeDuration.valueOf(AUTO_JOIN_RPC_TIMEOUT_MIN_SECS,
            TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(tempProps, TimeDuration.valueOf(AUTO_JOIN_RPC_TIMEOUT_MAX_SECS,
            TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setRequestTimeout(tempProps, TimeDuration.valueOf(AUTO_JOIN_RPC_TIMEOUT_MAX_SECS,
            TimeUnit.SECONDS));

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
                  if (p.getId().equals(localPeerId)) {
                    localPeer = p;
                    break;
                  }

                if (localPeer != null) {
                  // Use Mode.ADD for atomic single-peer addition. Unlike setConfiguration()
                  // which replaces the entire membership list (last-write-wins race if multiple
                  // pods call it concurrently), Mode.ADD atomically appends one peer to whatever
                  // the current configuration is, so concurrent joins from different pods are safe.
                  final SetConfigurationRequest.Arguments addArgs = SetConfigurationRequest.Arguments.newBuilder()
                      .setServersInNewConf(List.of(localPeer))
                      .setMode(SetConfigurationRequest.Mode.ADD)
                      .build();

                  final RaftClientReply joinReply = tempClient.admin().setConfiguration(addArgs);
                  if (!joinReply.isSuccess())
                    LogManager.instance().log(this, Level.WARNING, "K8s auto-join: add peer rejected: %s",
                        joinReply.getException() != null ? joinReply.getException().getMessage() : "unknown");
                  else
                    HALog.log(this, HALog.BASIC, "K8s auto-join: successfully joined cluster via atomic add");
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

  @Override
  public void stopService() {
    if (!active)
      return;
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
    stopHealthMonitor();

    // In K8s mode, automatically remove this peer from the Raft cluster before stopping.
    // This ensures clean scale-down without orphaned peers in the cluster configuration.
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
              transferLeadership(peer.getId().toString(), LEADERSHIP_TRANSFER_TIMEOUT_MS);
              // Wait for leadership change notification instead of polling
              final long deadline = System.currentTimeMillis() + LEADERSHIP_CHANGE_WAIT_MS;
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

  /**
   * Sends a pre-serialized Raft log entry (e.g., CREATE_DATABASE) to the cluster.
   */
  public void replicateRawEntry(final byte[] entry) {
    HALog.log(this, HALog.BASIC, "Replicating raw entry: %d bytes, type=%d", entry.length, entry.length > 0 ?
        entry[0] : -1);
    sendToRaft(entry);
  }

  @Override
  public void replicateCreateDatabase(final String databaseName) {
    final byte[] entry = RaftLogEntry.serializeCreateDatabase(databaseName, localPeerId.toString());
    replicateRawEntry(entry);
  }

  @Override
  public void replicateDropDatabase(final String databaseName) {
    final byte[] entry = RaftLogEntry.serializeDropDatabase(databaseName, localPeerId.toString());
    replicateRawEntry(entry);
  }

  /**
   * Submits a transaction to the Raft cluster. The entry is replicated to all nodes and applied
   * via ArcadeDBStateMachine.applyTransaction() on each node.
   * <p>
   * <b>Timeout semantics:</b> When using the group committer, the effective timeout can be up to
   * 2x {@code arcadedb.ha.quorumTimeout}. The first timeout covers queue waiting and Raft dispatch;
   * if the entry has already been dispatched to Raft when the first timeout expires, a second full
   * timeout is used to await the Raft reply (to prevent phantom commits where followers apply
   * the entry but the leader never calls commit2ndPhase). Operators setting
   * {@code arcadedb.ha.quorumTimeout} should account for this 2x upper bound.
   * <p>
   * If this method throws {@link QuorumNotReachedException} due to a timeout,
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
                                   final Binary walBuffer, final String schemaJson,
                                   final Map<Integer, String> filesToAdd,
                                   final Map<Integer, String> filesToRemove) {

    final byte[] entry = RaftLogEntry.serializeTransaction(databaseName, bucketRecordDelta, walBuffer, schemaJson,
        filesToAdd,
        filesToRemove, localPeerId.toString());

    HALog.log(this, HALog.TRACE, "replicateTransaction: db=%s, entrySize=%d bytes", databaseName, entry.length);
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
            "Raft replication failed: " + (reply.getException() != null ? reply.getException().getMessage() :
                "unknown error"));

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
    applyWaiterCount.incrementAndGet();
    try {
      final long deadline = System.currentTimeMillis() + quorumTimeout;
      applyLock.lock();
      try {
        while (getLastAppliedIndex() < targetIndex) {
          final long remaining = deadline - System.currentTimeMillis();
          if (remaining <= 0)
            throw new ReplicationException(
                "READ_YOUR_WRITES consistency timeout: follower applied index " + getLastAppliedIndex()
                    + " has not reached target " + targetIndex + " within " + quorumTimeout + "ms");
          applyCondition.await(remaining, TimeUnit.MILLISECONDS);
        }
      } finally {
        applyLock.unlock();
      }
      HALog.log(this, HALog.TRACE, "Bookmark wait complete: applied >= target=%d", targetIndex);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ReplicationException("READ_YOUR_WRITES consistency wait interrupted before reaching target index " + targetIndex);
    } finally {
      applyWaiterCount.decrementAndGet();
    }
  }

  /**
   * Returns true if this node is the leader and has finished applying all committed
   * entries from the previous term. During the brief window after election, this returns
   * false until the state machine catches up.
   */
  public boolean isLeaderReady() {
    return leaderReady;
  }

  /**
   * If this node is the leader but not yet ready (catch-up in progress), blocks until ready
   * or the quorum timeout expires. No-op if the leader is already caught up or this is a follower.
   */
  public void waitForLeaderReady() {
    if (!isLeader() || leaderReady)
      return;

    final long deadline = System.currentTimeMillis() + quorumTimeout;
    synchronized (leaderChangeNotifier) {
      while (!leaderReady) {
        final long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0)
          break;
        try {
          leaderChangeNotifier.wait(remaining);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
    if (!leaderReady)
      HALog.log(this, HALog.BASIC, "waitForLeaderReady timed out after %dms", quorumTimeout);
  }

  /**
   * Ensures this leader is still the legitimate leader before serving a read,
   * using Ratis's read index protocol (Section 6.4 of the Raft paper).
   * <p>
   * Fast path: if the leader lease is still valid (received heartbeat acks recently),
   * returns immediately with no network round-trip. Slow path: sends heartbeats to
   * a majority and waits for acknowledgment (~1 RTT).
   * <p>
   * Throws {@link ServerIsNotTheLeaderException} if this node is no longer the leader
   * (e.g., after SIGSTOP/SIGCONT).
   */
  public void ensureLinearizableRead() {
    if (raftClient == null)
      throw new ServerIsNotTheLeaderException("Raft client not initialized", getLeaderHTTPAddress());
    try {
      final RaftClientReply reply = raftClient.async()
          .sendReadOnly(Message.valueOf(ByteString.EMPTY))
          .get(quorumTimeout, TimeUnit.MILLISECONDS);
      if (!reply.isSuccess()) {
        final var ex = reply.getException();
        if (ex instanceof org.apache.ratis.protocol.exceptions.NotLeaderException nle) {
          final var suggestedLeader = nle.getSuggestedLeader();
          throw new ServerIsNotTheLeaderException("Not the leader (detected via read index)",
              suggestedLeader != null ? peerHttpAddresses.get(suggestedLeader.getId().toString()) : null);
        }
        throw new ReplicationException("Linearizable read check failed: " + ex.getMessage());
      }
      // Reply success means the leader lease is valid and the read index is confirmed.
      // Double-check we're still the leader: after SIGSTOP/SIGCONT, the sendReadOnly
      // heartbeat might briefly succeed before the old leader fully steps down.
      if (!isLeader())
        throw new ServerIsNotTheLeaderException("Leadership lost after read index confirmation",
            getLeaderHTTPAddress());
      // Now wait for the local state machine to catch up to the read index.
      final long readIndex = reply.getLogIndex();
      if (readIndex > 0)
        waitForAppliedIndex(readIndex);
    } catch (final ServerIsNotTheLeaderException e) {
      throw e;
    } catch (final java.util.concurrent.TimeoutException e) {
      HALog.log(this, HALog.BASIC, "ensureLinearizableRead timed out after %dms", quorumTimeout);
      throw new ReplicationException("Linearizable read timed out after " + quorumTimeout + "ms");
    } catch (final Exception e) {
      if (e.getCause() instanceof org.apache.ratis.protocol.exceptions.NotLeaderException nle) {
        final var suggestedLeader = nle.getSuggestedLeader();
        throw new ServerIsNotTheLeaderException("Not the leader (detected via read index)",
            suggestedLeader != null ? peerHttpAddresses.get(suggestedLeader.getId().toString()) : null);
      }
      throw new ReplicationException("Linearizable read check failed: " + e.getMessage());
    }
  }

  /**
   * Waits until the local state machine has applied all committed entries.
   * Used for leader read barrier: waits until lastAppliedIndex >= commitIndex.
   * In steady state this is a fast no-op (already caught up).
   */
  public void waitForLocalApply() {
    try {
      final long commitIndex = getCommitIndex();
      if (commitIndex <= 0)
        return;

      // Fast path: no lock needed if already caught up (common case for steady-state leader)
      if (getLastAppliedIndex() >= commitIndex)
        return;

      applyWaiterCount.incrementAndGet();
      try {
        final long deadline = System.currentTimeMillis() + quorumTimeout;
        applyLock.lock();
        try {
          while (getLastAppliedIndex() < commitIndex) {
            final long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
              HALog.log(this, HALog.DETAILED, "waitForLocalApply timed out: applied=%d < commit=%d",
                  getLastAppliedIndex(), commitIndex);
              return;
            }
            applyCondition.await(remaining, TimeUnit.MILLISECONDS);
          }
        } finally {
          applyLock.unlock();
        }
        HALog.log(this, HALog.TRACE, "Local apply caught up: applied=%d >= commit=%d",
            getLastAppliedIndex(), commitIndex);
      } finally {
        applyWaiterCount.decrementAndGet();
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ReplicationException("Leader apply wait interrupted before state machine caught up");
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
   * Called by ArcadeDBStateMachine after applying a log entry to wake up waiters.
   */
  public void notifyApplied() {
    if (applyWaiterCount.get() > 0) {
      applyLock.lock();
      try {
        applyCondition.signalAll();
      } finally {
        applyLock.unlock();
      }
    }
  }

  /**
   * Called by ArcadeDBStateMachine when the leader changes to wake up leaveCluster().
   */
  public void notifyLeaderChanged() {
    if (isLeader()) {
      // New leader must apply all committed entries before serving reads.
      // Mark as not ready; the catch-up runs in the background to avoid blocking
      // the Ratis event thread (which processes heartbeats and elections).
      leaderReady = false;
      HALog.log(this, HALog.BASIC, "This node became leader, scheduling state machine catch-up in background");
      stateMachine.getLifecycleExecutor().submit(() -> {
        try {
          waitForLocalApply();
          leaderReady = true;
          HALog.log(this, HALog.BASIC, "Leader read barrier cleared: applied=%d >= commit=%d",
              getLastAppliedIndex(), getCommitIndex());
        } catch (final Exception e) {
          // Do NOT set leaderReady = true on failure. If catch-up failed, the leader's
          // state machine is stale and must not serve linearizable reads. Reads will
          // block in waitForLeaderReady() until the quorum timeout, then fail with an
          // error rather than returning stale data.
          LogManager.instance().log(this, Level.SEVERE,
              "Leader read barrier catch-up FAILED. Reads will be blocked until resolved: %s", e.getMessage());
        } finally {
          // Wake up any threads blocked in waitForLeaderReady()
          synchronized (leaderChangeNotifier) {
            leaderChangeNotifier.notifyAll();
          }
        }
      });
    } else {
      leaderReady = true;
    }
    synchronized (leaderChangeNotifier) {
      leaderChangeNotifier.notifyAll();
    }
    printClusterConfiguration();
  }

  /**
   * Prints an ASCII table showing the current cluster configuration.
   * Called on leader changes so the operator can see the cluster state at a glance.
   */
  public void printClusterConfiguration() {
    if (!isLeader())
      return;

    try {
      final String leaderPeerId = getLeaderName();
      final long term = getCurrentTerm();
      final long commitIndex = getCommitIndex();
      final Collection<RaftPeer> peers = getLivePeers();
      if (peers.isEmpty())
        return;

      // Collect follower replication state (only available on leader)
      final Map<String, long[]> followerState = new java.util.HashMap<>();
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
            // Only show latency when there's active replication traffic (recent RPC).
            // During idle periods lastRpcElapsedMs just reflects time since last heartbeat.
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

      // Only print if the configuration actually changed (avoid duplicate logs when
      // multiple servers in the same JVM each receive the same leader change event)
      final int hash = output.hashCode();
      if (hash == lastClusterConfigHash)
        return;
      lastClusterConfigHash = hash;

      LogManager.instance().log(this, Level.WARNING, "%s", output);

    } catch (final Exception e) {
      // Best-effort: don't let formatting errors disrupt the cluster
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
    for (int i = 0; i < values.length; i++)
      sb.append(' ').append(String.format("%-" + widths[i] + "s", values[i])).append(" |");
    sb.append('\n');
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

      // Snapshot the RoleInfoProto once - it contains peer IDs and last-RPC times
      // from a single point in time (the protobuf is built atomically by Ratis).
      final var roleInfo = info.getRoleInfoProto();
      if (!roleInfo.hasLeaderInfo())
        return List.of();

      final List<RaftProtos.ServerRpcProto> followerInfos = roleInfo.getLeaderInfo().getFollowerInfoList();

      // These two calls are NOT atomic with the roleInfo snapshot. A membership change
      // between them can reorder or resize the arrays. We guard against this below.
      final long[] matchIndices = info.getFollowerMatchIndices();
      final long[] nextIndices = info.getFollowerNextIndices();

      // If sizes diverge, a membership change happened between the calls.
      // Discard the result rather than risk misattributing indices to the wrong peer.
      if (followerInfos.size() != matchIndices.length || followerInfos.size() != nextIndices.length)
        return List.of();

      final List<Map<String, Object>> result = new ArrayList<>(followerInfos.size());
      for (int i = 0; i < followerInfos.size(); i++) {
        final String peerId = followerInfos.get(i).getId().getId().toStringUtf8();
        final long lastRpcElapsedMs = followerInfos.get(i).getLastRpcElapsedTimeMs();
        final Map<String, Object> state = new java.util.LinkedHashMap<>();
        state.put("peerId", peerId);
        state.put("matchIndex", matchIndices[i]);
        state.put("nextIndex", nextIndices[i]);
        state.put("lastRpcElapsedMs", lastRpcElapsedMs);
        result.add(state);
      }
      return result;
    } catch (final Exception e) {
      // Catch any exception (IOException, ConcurrentModificationException, IndexOutOfBounds)
      // from a membership change racing with the index array reads.
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

  @Override
  public com.arcadedb.serializer.json.JSONObject exportClusterStatus() {
    final var haJSON = new com.arcadedb.serializer.json.JSONObject();

    haJSON.put("protocol", "ratis");
    haJSON.put("clusterName", getClusterName());
    haJSON.put("leader", getLeaderName());
    haJSON.put("electionStatus", getElectionStatus());
    haJSON.put("isLeader", isLeader());
    haJSON.put("localPeerId", localPeerId.toString());
    haJSON.put("configuredServers", getConfiguredServers());
    haJSON.put("quorum", quorum.name());
    haJSON.put("currentTerm", getCurrentTerm());
    haJSON.put("commitIndex", getCommitIndex());
    haJSON.put("lastAppliedIndex", getLastAppliedIndex());

    // Peer list with replication state (follower indices available only on leader)
    final var followerStates = getFollowerStates();
    final var peers = new com.arcadedb.serializer.json.JSONArray();
    for (final var peer : raftGroup.getPeers()) {
      final var peerJSON = new com.arcadedb.serializer.json.JSONObject();
      final String peerId = peer.getId().toString();
      peerJSON.put("id", peerId);
      peerJSON.put("address", peer.getAddress());
      peerJSON.put("httpAddress", getPeerHTTPAddress(peer.getId()));
      peerJSON.put("isLocal", peer.getId().equals(localPeerId));
      peerJSON.put("role", peer.getId().equals(localPeerId) && isLeader() ? "LEADER"
          : peerId.equals(getLeaderName()) ? "LEADER" : "FOLLOWER");

      for (final var fs : followerStates)
        if (peerId.equals(fs.get("peerId"))) {
          peerJSON.put("matchIndex", fs.get("matchIndex"));
          peerJSON.put("nextIndex", fs.get("nextIndex"));
          if (clusterMonitor != null) {
            final var lags = clusterMonitor.getReplicaLags();
            final Long lag = lags.get(peerId);
            if (lag != null)
              peerJSON.put("lagging", lag > clusterMonitor.getLagWarningThreshold()
                  && clusterMonitor.getLagWarningThreshold() > 0);
          }
          break;
        }

      peers.put(peerJSON);
    }
    haJSON.put("peers", peers);

    // Database list
    final var databases = new com.arcadedb.serializer.json.JSONArray();
    for (final String dbName : server.getDatabaseNames()) {
      final var databaseJSON = new com.arcadedb.serializer.json.JSONObject();
      databaseJSON.put("name", dbName);
      databaseJSON.put("quorum", quorum.name());
      databases.put(databaseJSON);
    }
    haJSON.put("databases", databases);

    // Metrics
    final var metricsJSON = new com.arcadedb.serializer.json.JSONObject();
    metricsJSON.put("electionCount", getElectionCount());
    metricsJSON.put("lastElectionTime", getLastElectionTime());
    metricsJSON.put("raftLogSize", getRaftLogSize());
    metricsJSON.put("startTime", getStartTime());
    metricsJSON.put("lagWarningThreshold", clusterMonitor.getLagWarningThreshold());
    haJSON.put("metrics", metricsJSON);

    // Required by RemoteHttpComponent for cluster configuration
    haJSON.put("leaderAddress", getLeaderHTTPAddress());
    haJSON.put("replicaAddresses", getReplicaAddresses());

    return haJSON;
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
    try {
      raftClient = buildRaftClient();
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error creating RaftClient during refresh", e);
      return;
    }
    HALog.log(this, HALog.BASIC, "RaftClient refreshed with fresh gRPC channels after leader change");
  }

  private RaftClient buildRaftClient() throws IOException {
    return RaftClient.newBuilder()
        .setRaftGroup(raftGroup)
        .setLeaderId(localPeerId)
        .setProperties(raftProperties)
        .setRetryPolicy(ExponentialBackoffRetry.newBuilder()
            .setBaseSleepTime(TimeDuration.valueOf(CLIENT_RETRY_BASE_SLEEP_MS, TimeUnit.MILLISECONDS))
            .setMaxSleepTime(TimeDuration.valueOf(CLIENT_RETRY_MAX_SLEEP_SECS, TimeUnit.SECONDS))
            .build())
        .build();
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
    lagMonitorExecutor.scheduleAtFixedRate(this::checkReplicaLag,
        LAG_MONITOR_INITIAL_DELAY_SECS, LAG_MONITOR_INTERVAL_SECS, TimeUnit.SECONDS);
  }

  private void stopLagMonitor() {
    if (lagMonitorExecutor != null) {
      lagMonitorExecutor.shutdownNow();
      lagMonitorExecutor = null;
    }
  }

  // -- Ratis Health Monitor --

  private void startRatisHealthMonitor() {
    healthMonitorExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "arcadedb-ratis-health-monitor");
      t.setDaemon(true);
      return t;
    });
    healthMonitorExecutor.scheduleAtFixedRate(() -> {
      if (server.getStatus() != ArcadeDBServer.STATUS.ONLINE)
        return;
      try {
        restartRatisIfNeeded();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Health monitor error: %s", e.getMessage());
      }
    }, HEALTH_MONITOR_INITIAL_DELAY_SECS, HEALTH_MONITOR_INTERVAL_SECS, TimeUnit.SECONDS);
  }

  private void stopHealthMonitor() {
    if (healthMonitorExecutor != null) {
      healthMonitorExecutor.shutdownNow();
      healthMonitorExecutor = null;
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
   * @param peerId      the new peer's ID (typically host_port)
   * @param address     the new peer's Raft RPC address (host:port)
   * @param httpAddress optional HTTP address (host:port). If null or empty, derived from the Raft
   *                    address using the local port offset (which may be incorrect if the peer uses
   *                    a non-standard port layout)
   */
  public void addPeer(final String peerId, final String address, final String httpAddress) {
    final RaftPeer newPeer = RaftPeer.newBuilder()
        .setId(RaftPeerId.valueOf(peerId))
        .setAddress(address)
        .build();

    try {
      final SetConfigurationRequest.Arguments addArgs = SetConfigurationRequest.Arguments.newBuilder()
          .setServersInNewConf(List.of(newPeer))
          .setMode(SetConfigurationRequest.Mode.ADD)
          .build();
      final RaftClientReply reply = raftClient.admin().setConfiguration(addArgs);
      if (!reply.isSuccess())
        throw new ConfigurationException("Failed to add peer " + peerId + ": " + reply.getException());

      if (httpAddress != null && !httpAddress.isEmpty()) {
        peerHttpAddresses.put(peerId, httpAddress);
        LogManager.instance().log(this, Level.INFO, "Peer %s added with explicit HTTP address %s", peerId, httpAddress);
      } else {
        // Derive HTTP address from Raft address. This assumes the dynamically added peer uses the
        // same HTTP/Raft port gap as this node, which may be incorrect for non-standard layouts.
        try {
          final String[] addrParts = parseHostPort(address);
          final int raftPort = Integer.parseInt(addrParts[1]);
          final int httpPortOffset = getHttpPortOffset();
          final String derivedHttp = addrParts[0] + ":" + (raftPort + httpPortOffset);
          peerHttpAddresses.put(peerId, derivedHttp);
          LogManager.instance().log(this, Level.WARNING,
              "Dynamically added peer '%s': no HTTP address provided, derived as %s using local port offset (%+d). "
                  + "Use 'httpAddress' parameter for explicit control",
              peerId, derivedHttp, httpPortOffset);
        } catch (final ConfigurationException | NumberFormatException ignored) {
          LogManager.instance().log(this, Level.WARNING,
              "Dynamically added peer '%s': could not derive HTTP address from '%s'", peerId, address);
        }
      }

      LogManager.instance().log(this, Level.INFO, "Peer %s added to Raft cluster", peerId);
    } catch (final IOException e) {
      throw new ConfigurationException("Failed to add peer " + peerId, e);
    }
  }

  /**
   * Convenience overload for backward compatibility (derives HTTP address from port offset).
   */
  public void addPeer(final String peerId, final String address) {
    addPeer(peerId, address, null);
  }

  /**
   * Removes a peer from the Raft cluster. Must be called on any server (Ratis routes to leader).
   * The removed peer will step down automatically.
   *
   * @param peerId the peer ID to remove
   */
  public void removePeer(final String peerId) {
    final Collection<RaftPeer> livePeers = getLivePeers();
    final List<RaftPeer> currentPeers = new ArrayList<>();
    final List<RaftPeer> newPeers = new ArrayList<>();
    for (final RaftPeer peer : livePeers) {
      currentPeers.add(peer);
      if (!peer.getId().toString().equals(peerId))
        newPeers.add(peer);
    }

    if (newPeers.size() == livePeers.size())
      throw new ConfigurationException("Peer " + peerId + " not found in cluster");

    try {
      // Use COMPARE_AND_SET to ensure no concurrent membership change happened between
      // reading livePeers and applying the removal.
      final SetConfigurationRequest.Arguments removeArgs = SetConfigurationRequest.Arguments.newBuilder()
          .setServersInCurrentConf(currentPeers)
          .setServersInNewConf(newPeers)
          .setMode(SetConfigurationRequest.Mode.COMPARE_AND_SET)
          .build();
      final RaftClientReply reply = raftClient.admin().setConfiguration(removeArgs);
      if (!reply.isSuccess())
        throw new ConfigurationException("Failed to remove peer " + peerId + ": " + reply.getException());
      LogManager.instance().log(this, Level.INFO, "Peer %s removed from Raft cluster", peerId);
      if (clusterMonitor != null)
        clusterMonitor.removeReplica(peerId);
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
    // Create a fresh client for the admin call to avoid "client is closed" errors.
    // The existing raftClient may have been closed after a prior leadership change.
    try (final RaftClient adminClient = RaftClient.newBuilder()
        .setRaftGroup(raftGroup)
        .setProperties(raftProperties)
        .build()) {
      final RaftClientReply reply = adminClient.admin().transferLeadership(
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
          transferLeadership(peer.getId().toString(), LEADERSHIP_TRANSFER_TIMEOUT_MS);
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
   * Derived addresses are NOT cached so that a wrong derivation does not persist permanently.
   */
  public String getPeerHTTPAddress(final RaftPeerId peerId) {
    final String httpAddr = peerHttpAddresses.get(peerId.toString());
    if (httpAddr != null)
      return httpAddr;

    // Derive HTTP address from peer ID format "host_raftPort" using port offset.
    // This assumes all nodes use the same HTTP-to-Raft port gap as this node,
    // which may be wrong if remote nodes use different port configurations.
    // Deliberately not cached: if the derivation is wrong, at least it won't stick forever.
    final String peerIdStr = peerId.toString();
    final int lastUnderscore = peerIdStr.lastIndexOf('_');
    if (lastUnderscore > 0 && lastUnderscore < peerIdStr.length() - 1) {
      final String host = peerIdStr.substring(0, lastUnderscore);
      try {
        final int raftPort = Integer.parseInt(peerIdStr.substring(lastUnderscore + 1));
        final int httpPortOffset = getHttpPortOffset();
        final int httpPort = raftPort + httpPortOffset;
        final String derived = host + ":" + httpPort;
        if (derivedAddressWarned.add(peerIdStr))
          LogManager.instance().log(this, Level.WARNING,
              "No explicit HTTP address for peer '%s', deriving %s using local HTTP/Raft port offset (%+d). "
                  + "If this peer uses a different port layout, specify explicit HTTP ports in HA_SERVER_LIST (format: "
                  + "host:raftPort:httpPort)",
              peerIdStr, derived, httpPortOffset);
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
    // When the follower receives the marker, reinitialize() detects the index gap and triggers the HTTP download.
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

    // AppendEntries batching: allow multiple log entries in a single gRPC call to followers.
    // Combined with the group committer, this allows many transactions to be replicated in one round-trip.
    final String appendBufferSize = configuration.getValueAsString(GlobalConfiguration.HA_APPEND_BUFFER_SIZE);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf(appendBufferSize));

    // Write buffer (must be >= appender buffer byte-limit + 8)
    final long appendBytes = SizeInBytes.valueOf(appendBufferSize).getSize();
    final long minWriteBuffer = appendBytes + 8;
    SizeInBytes writeBuffer =
        SizeInBytes.valueOf(configuration.getValueAsString(GlobalConfiguration.HA_WRITE_BUFFER_SIZE));
    if (writeBuffer.getSize() < minWriteBuffer) {
      LogManager.instance().log(this, Level.WARNING,
          "ha.writeBufferSize (%s) is smaller than appendBufferSize + 8 (%d bytes). Adjusting to %d bytes",
          writeBuffer, minWriteBuffer, minWriteBuffer);
      writeBuffer = SizeInBytes.valueOf(minWriteBuffer);
    }
    RaftServerConfigKeys.Log.setWriteBufferSize(properties, writeBuffer);
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, APPEND_ENTRIES_MAX_ELEMENTS);

    // Leader lease: enables consistent reads from the leader without a round-trip to followers.
    // The leader can serve reads as long as its lease hasn't expired (based on heartbeat responses).
    RaftServerConfigKeys.Read.setLeaderLeaseEnabled(properties, true);
    RaftServerConfigKeys.Read.setLeaderLeaseTimeoutRatio(properties, LEADER_LEASE_TIMEOUT_RATIO);
    RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);

    // Note: Ratis uses MAJORITY consensus by default.
    // For ALL quorum mode, we use the Watch API after each write to wait for ALL replicas.
    // See sendToRaft() for the ALL quorum implementation.

    RaftServerConfigKeys.Rpc.setRequestTimeout(properties,
        TimeDuration.valueOf(RPC_REQUEST_TIMEOUT_SECS, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setSlownessTimeout(properties,
        TimeDuration.valueOf(FOLLOWER_SLOWNESS_TIMEOUT_SECS, TimeUnit.SECONDS));
    RaftServerConfigKeys.setCloseThreshold(properties,
        TimeDuration.valueOf(FOLLOWER_CLOSE_THRESHOLD_SECS, TimeUnit.SECONDS));

    // gRPC flow control window: larger window helps with catch-up replication after partitions
    final String flowControlWindow = configuration.getValueAsString(GlobalConfiguration.HA_GRPC_FLOW_CONTROL_WINDOW);
    GrpcConfigKeys.setFlowControlWindow(properties, SizeInBytes.valueOf(flowControlWindow));

    // Client request timeout: bounds how long the Ratis client waits for a single RPC.
    // Without this, the client retries indefinitely when the majority is unreachable.
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(quorumTimeout, TimeUnit.MILLISECONDS));

    return properties;
  }

  // -- Peer Parsing --

  /**
   * Parses a comma-separated server list into Raft peers.
   * <p>
   * Each entry supports the following formats:
   * <ul>
   *   <li>{@code host:raftPort:httpPort:priority} - explicit Raft port, HTTP port, and leader-election priority</li>
   *   <li>{@code host:raftPort:httpPort} - explicit Raft and HTTP ports, priority defaults to 0</li>
   *   <li>{@code host:raftPort} - explicit Raft port, HTTP derived from local offset, priority defaults to 0</li>
   * </ul>
   * Priority is used for Raft leader election: the node with the highest priority is preferred as leader.
   * This is a soft preference - if the preferred leader is unavailable, another node will take over.
   */
  private List<RaftPeer> parsePeers(final String serverList) {
    final List<RaftPeer> peers = new ArrayList<>();
    final String[] entries = serverList.split(",");

    final int httpPortOffset = getHttpPortOffset();

    for (final String entry : entries) {
      final String trimmed = entry.trim();
      if (trimmed.isEmpty())
        continue;

      final String[] parts = parseHostPort(trimmed);
      final String host = parts[0];
      final int raftPort = Integer.parseInt(parts[1]);
      final String raftAddress = host + ":" + raftPort;

      // Determine HTTP address: use explicit httpPort if provided, otherwise derive from local offset
      final String httpAddress;
      if (parts.length >= 3)
        httpAddress = host + ":" + parts[2];
      else {
        httpAddress = host + ":" + (raftPort + httpPortOffset);
        LogManager.instance().log(this, Level.INFO,
            "Peer '%s:%d': no explicit HTTP port in HA_SERVER_LIST, deriving HTTP address %s using local port offset " +
                "(%+d). "
                + "Use format 'host:raftPort:httpPort' if peers have different port layouts",
            host, raftPort, httpAddress, httpPortOffset);
      }

      // Parse optional priority (4th part): higher priority = preferred leader
      int priority = 0;
      if (parts.length >= 4) {
        try {
          priority = Integer.parseInt(parts[3]);
        } catch (final NumberFormatException e) {
          throw new ConfigurationException(
              "Invalid priority value '" + parts[3] + "' in peer address '" + trimmed + "'");
        }
      }

      // Use underscore in peer ID to avoid JMX ObjectName issues (colon is invalid in JMX values)
      final String peerIdStr = host + "_" + raftPort;
      final RaftPeerId peerId = RaftPeerId.valueOf(peerIdStr);
      final RaftPeer peer = RaftPeer.newBuilder()
          .setId(peerId)
          .setAddress(raftAddress)
          .setPriority(priority)
          .build();
      peers.add(peer);

      // Store HTTP address mapping separately (NOT on RaftPeer.clientAddress which Ratis uses for gRPC)
      peerHttpAddresses.put(peerIdStr, httpAddress);
    }

    if (peers.size() < 3)
      LogManager.instance().log(this, Level.WARNING,
          "Ratis HA cluster has less than 3 peers (%d). A minimum of 3 is recommended for fault tolerance",
          peers.size());

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

  /**
   * Parses a host:port string, supporting both IPv4/hostname and bracketed IPv6 notation.
   * <p>
   * Accepted formats:
   * <ul>
   *   <li>"hostname:port" or "hostname:port:extraPort"</li>
   *   <li>"1.2.3.4:port" or "1.2.3.4:port:extraPort"</li>
   *   <li>"[::1]:port" or "[2001:db8::1]:port:extraPort"</li>
   * </ul>
   * Bare (un-bracketed) IPv6 addresses are rejected because they are ambiguous with the port delimiter.
   *
   * @return array where [0]=host (including brackets for IPv6), [1]=first port, and optionally [2]=second port
   */
  /**
   * Validates that the given address is a well-formed host:port string with a port in the valid TCP range (1-65535).
   * Call this before passing addresses to Ratis to produce clear error messages.
   */
  public static void validatePeerAddress(final String address) {
    final String[] parts = parseHostPort(address);

    if (parts[0].isEmpty())
      throw new ConfigurationException("HA peer address has empty host: " + address);

    final String portStr = parts[1];
    final int port;
    try {
      port = Integer.parseInt(portStr);
    } catch (final NumberFormatException e) {
      throw new ConfigurationException("HA peer address has non-numeric port '" + portStr + "': " + address);
    }
    if (port < 1 || port > 65535)
      throw new ConfigurationException("HA peer address port out of range (must be 1-65535): " + port);
  }

  static String[] parseHostPort(final String address) {
    if (address == null || address.isEmpty())
      throw new ConfigurationException("HA peer address is empty");

    if (address.startsWith("[")) {
      // Bracketed IPv6: [addr]:port or [addr]:port:extraPort
      final int closeBracket = address.indexOf(']');
      if (closeBracket < 0)
        throw new ConfigurationException("Invalid IPv6 address (missing closing bracket): " + address);

      final String host = address.substring(0, closeBracket + 1);
      final String remainder = address.substring(closeBracket + 1);
      if (remainder.isEmpty() || remainder.charAt(0) != ':')
        throw new ConfigurationException("HA peer address missing port after IPv6 host: " + address);

      final String[] ports = remainder.substring(1).split(":");
      final String[] result = new String[1 + ports.length];
      result[0] = host;
      System.arraycopy(ports, 0, result, 1, ports.length);
      return result;
    }

    // Detect bare (un-bracketed) IPv6 by counting colons: more than 2 colons and no dots means
    // this is likely an IPv6 address without brackets (e.g., "::1:2424" or "2001:db8::1:2424").
    // The maximum for host:raft:http is 2 colons, so 3+ colons without dots is always IPv6.
    final long colonCount = address.chars().filter(c -> c == ':').count();
    if (colonCount > 2 && !address.contains("."))
      throw new ConfigurationException(
          "IPv6 addresses must use bracketed notation (e.g., [::1]:2424) in HA peer address: " + address);

    // IPv4 or hostname: host:port or host:port:extraPort
    final String[] parts = address.split(":");
    if (parts.length < 2)
      throw new ConfigurationException("HA peer address missing port: " + address);

    return parts;
  }
}
