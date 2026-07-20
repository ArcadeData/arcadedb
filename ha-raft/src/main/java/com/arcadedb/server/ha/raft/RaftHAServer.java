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
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.monitor.HAReplicationStatsProvider;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.impl.RatisMetricRegistryImpl;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.RaftServerRpcWithProxy;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.codahale.metrics.MetricRegistry;
import org.apache.ratis.thirdparty.com.codahale.metrics.Timer;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.TimeDuration;

import javax.net.ssl.HttpsURLConnection;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
 * {@link #stateMachine} field is volatile so readers (HTTP handlers, status exporter)
 * always see the latest instance after a recovery restart. The
 * {@link #shutdownRequested} volatile flag prevents recovery during shutdown.
 * <p>
 * <b>Security note (K8s mode):</b> When {@code HA_K8S} is enabled and gRPC is bound
 * to {@code 0.0.0.0}, any pod in the Kubernetes cluster can connect to the Raft port
 * and inject Raft log entries. Authentication for inter-node traffic relies on
 * Kubernetes NetworkPolicy. Operators should restrict access to the Raft port via
 * NetworkPolicy rules in production.
 */
public class RaftHAServer implements HealthMonitor.HealthTarget {

  // The forwarded user the leader presents (with the cluster token) for inter-node calls such as the
  // stalled-replica resync. ArcadeDB's bootstrap 'root' user is the cluster-wide superuser; named here
  // so the coupling is visible rather than scattered as a string literal.
  // Package-private so other cluster-internal RPC callers (e.g. LeaderDatabaseQuery) reuse the same constant
  // instead of re-inlining the "root" literal.
  static final String FORWARDED_ROOT_USER = "root";

  // Timeout carried by the node-local snapshot-management requests the log compaction scheduler issues.
  // Generous: the request is served on the state-machine updater thread, which may be busy applying a
  // backlog, and a timeout here only skips one compaction tick.
  private static final long SNAPSHOT_REQUEST_TIMEOUT_MS = 30_000L;

  private final    ArcadeDBServer          arcadeServer;
  private final    ContextConfiguration    configuration;
  private volatile ArcadeStateMachine      stateMachine;
  private final    ClusterMonitor          clusterMonitor;
  private final    Quorum                  quorum;
  private final    long                    quorumTimeout;
  private final    RaftGroup               raftGroup;
  private final    RaftPeerId              localPeerId;
  private final    Map<RaftPeerId, String> httpAddresses      = new HashMap<>();
  // Explicit HTTPS endpoints (optional 5th field in HA_SERVER_LIST). Used for encrypted
  // peer-to-peer transfers (snapshot download) when SSL is enabled.
  private final    Map<RaftPeerId, String> httpsAddresses     = new HashMap<>();
  // Logged at most once: warns operators that HTTP addresses are derived (not explicitly configured).
  private final    AtomicBoolean           httpFallbackWarned = new AtomicBoolean(false);
  // Logged at most once: notes that peer HTTPS endpoints are derived from this node's local HTTPS port.
  private final    AtomicBoolean           httpsFallbackWarned = new AtomicBoolean(false);
  // Client-reachable Bolt endpoints (optional object-form 'bolt' field in HA_SERVER_LIST). Advertised
  // in the Bolt ROUTE routing table so neo4j:// drivers can discover leader/followers.
  private final    Map<RaftPeerId, String> boltAddresses      = new HashMap<>();
  // Logged at most once: warns operators that Bolt routing addresses are derived (not explicitly configured).
  private final    AtomicBoolean           boltFallbackWarned = new AtomicBoolean(false);
  private final    Map<RaftPeerId, String> peerDisplayNames   = new ConcurrentHashMap<>();
  private final    String                  clusterName;

  // volatile: reassigned by the recovery path (restartRatis) and cleared by stop(), while background
  // threads - the health monitor and, since #5345, the log compaction scheduler - read it. Every reader
  // still copies it to a local before use so a concurrent reassignment cannot null it mid-method.
  private volatile RaftServer                raftServer;
  private          RaftClient                raftClient;
  private          RaftProperties            raftProperties;
  private volatile RaftTransactionBroker     transactionBroker;
  private          RaftClusterStatusExporter statusExporter;
  private          ScheduledExecutorService  lagMonitorExecutor;
  // Runs leader-driven stalled-replica resyncs off the lag-monitor thread (issue #4728). One worker is
  // enough since at most one resync fires per replica per stall streak; a small bounded queue with a
  // caller-runs policy degrades to running on the lag-monitor thread under the (unlikely) burst.
  private final    ThreadPoolExecutor        stalledResyncExecutor = createStalledResyncExecutor();
  // Inbound Raft gRPC peer allowlist, recreated on each Ratis (re)start. Periodically refreshed by the
  // health monitor tick so a returned peer's new pod IP is admitted proactively (issue #4696).
  private volatile PeerAddressAllowlistFilter allowlistFilter;
  private final    Object                    leaderChangeNotifier  = new Object();
  private final    Object                    applyNotifier         = new Object();
  private          RaftClusterManager        clusterManager;
  private final    Object                    recoveryLock          = new Object();
  private volatile boolean                   shutdownRequested     = false;
  private volatile boolean                   legacyRaftStorageWarningLogged = false;
  private volatile Thread                    autoJoinThread        = null;
  private volatile LifeCycle.State           forcedStateForTesting = null;
  private          HealthMonitor             healthMonitor;
  // Periodic Raft snapshot/log-purge trigger (issue #5345). Runs on every node, leader and follower
  // alike, because each Ratis server purges its own log against its own snapshot index.
  private          RaftLogCompactionScheduler logCompactionScheduler;
  // Client identity and call-id sequence for the local snapshot-management requests the compaction
  // scheduler issues. A stable ClientId keeps the requests distinguishable in Ratis's logs.
  private final    ClientId                  snapshotClientId      = ClientId.randomId();
  private final    AtomicLong                snapshotCallId        = new AtomicLong();
  // Resolved Raft storage directory, cached for the free-space probes the compaction scheduler runs on
  // every tick. Config-derived and constant for this server's lifetime.
  private volatile File                      cachedRaftStorageDir  = null;
  // Follower-side Raft log catch-up narrative. Driven by the health-monitor tick; only active on a
  // follower with a non-trivial apply backlog (committed-but-not-yet-applied entries) and not installing
  // a snapshot.
  private volatile FollowerResyncProgressTracker resyncProgressTracker = null;
  // Applied index observed at the previous stale-follower lag check (issue #4840). Compared against the
  // current applied index to tell a healthy-but-slow follower (applied advancing one entry at a time)
  // apart from a genuinely stuck one (applied frozen). Read/written only on the single HealthMonitor
  // tick thread, so it needs no synchronization. -1 = no prior sample (first check or just resumed
  // follower duty).
  private          long                      lastLagCheckAppliedIndex = -1;
  private          ClusterTokenProvider      tokenProvider;
  private volatile int                       restartFailureCount   = 0;
  private volatile BootstrapElection         bootstrapElection;

  public RaftHAServer(final ArcadeDBServer arcadeServer, final ContextConfiguration configuration) {
    this.arcadeServer = arcadeServer;
    this.configuration = configuration;

    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    final String clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
    this.clusterName = clusterName;
    final long lagWarningThreshold = configuration.getValueAsLong(GlobalConfiguration.HA_REPLICATION_LAG_WARNING);
    final int raftPort = configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_PORT);

    // Inside Kubernetes the DNS suffix must reach peers too, not only the self-advertised host built in
    // ArcadeDBServer.assignHostAddress; otherwise short pod names in the server list never resolve.
    final String k8sDnsSuffix = configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S)
        ? configuration.getValueAsString(GlobalConfiguration.HA_K8S_DNS_SUFFIX)
        : "";

    final RaftPeerAddressResolver.ParsedPeerList parsed = RaftPeerAddressResolver.parsePeerList(serverList, raftPort,
        k8sDnsSuffix);
    List<RaftPeer> peers = parsed.peers();
    final Map<RaftPeerId, String> configuredPeerNames = parsed.peerNames();
    final String serverName = arcadeServer.getServerName();

    this.httpAddresses.putAll(parsed.httpAddresses());
    this.httpsAddresses.putAll(parsed.httpsAddresses());
    this.boltAddresses.putAll(parsed.boltAddresses());

    RaftPeerId resolvedLocalPeerId;
    try {
      resolvedLocalPeerId = RaftPeerAddressResolver.findLocalPeerId(peers, configuredPeerNames, serverName, arcadeServer);
    } catch (final IllegalArgumentException e) {
      // Issue #4836: a Kubernetes StatefulSet scaled past the static HA_SERVER_LIST starts pods whose
      // ordinal is beyond the configured peer list. Rather than crash-loop, synthesize the local peer
      // from the pod name + DNS suffix and let KubernetesAutoJoin add it to the running cluster.
      final RaftPeer synthesized = RaftPeerAddressResolver.synthesizeK8sScaleUpPeer(
          configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S), peers, serverName,
          configuration.getValueAsString(GlobalConfiguration.HA_K8S_DNS_SUFFIX), raftPort);
      if (synthesized == null)
        // Not a K8s scale-up node: surface the original, actionable resolution error.
        throw e;

      final int configuredPeers = peers.size();
      final List<RaftPeer> augmented = new ArrayList<>(peers);
      augmented.add(synthesized);
      peers = Collections.unmodifiableList(augmented);
      resolvedLocalPeerId = synthesized.getId();
      LogManager.instance().log(this, Level.INFO,
          "K8s scale-up detected: node '%s' is beyond the configured server list (%d peers). "
              + "Synthesized local Raft peer %s; it will auto-join the existing cluster.",
          serverName, configuredPeers, synthesized.getId());
    }
    this.localPeerId = resolvedLocalPeerId;

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

    // Build human-readable display names. When a peer was given an explicit name via the
    // "name@host:port" syntax in HA_SERVER_LIST, that name is used directly; otherwise the
    // legacy synthesis "<localPrefix><sep><index>" is applied so unnamed clusters keep
    // showing names like "ArcadeDB_0", "ArcadeDB_1", etc.
    String prefix = null;
    Character separator = null;
    if (!configuredPeerNames.keySet().containsAll(peers.stream().map(RaftPeer::getId).toList())) {
      try {
        final int separatorIdx = RaftPeerAddressResolver.findLastSeparatorIndex(serverName);
        prefix = serverName.substring(0, separatorIdx);
        separator = serverName.charAt(separatorIdx);
      } catch (final IllegalArgumentException ignored) {
        // Server name has no _N/-N suffix; only named peers get a display name, others fall
        // back to their raw peer ID at render time.
      }
    }
    for (int i = 0; i < peers.size(); i++) {
      final RaftPeerId peerId = peers.get(i).getId();
      final String configured = configuredPeerNames.get(peerId);
      final String nodeName;
      if (configured != null)
        nodeName = configured;
      else if (prefix != null)
        nodeName = prefix + separator + i;
      else
        nodeName = peerId.toString();
      final String httpAddr = this.httpAddresses.get(peerId);
      this.peerDisplayNames.put(peerId, httpAddr != null ? nodeName + " (" + httpAddr + ")" : nodeName);
    }

    this.stateMachine = createStateMachine();

    final long stalledResyncDurationMs = configuration.getValueAsLong(
        GlobalConfiguration.HA_STALLED_REPLICA_RESYNC_DURATION_MS);
    final boolean resyncNarrative = configuration.getValueAsBoolean(GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING);
    final long peerUnreachableThresholdMs = configuration.getValueAsLong(GlobalConfiguration.HA_PEER_UNREACHABLE_THRESHOLD);
    final long peerChannelResetDurationMs = configuration.getValueAsLong(GlobalConfiguration.HA_PEER_CHANNEL_RESET_DURATION);
    this.clusterMonitor = new ClusterMonitor(lagWarningThreshold, stalledResyncDurationMs,
        this::forceResyncStalledReplica, resyncNarrative, peerUnreachableThresholdMs, peerChannelResetDurationMs,
        this::resetPeerReplicationChannel);
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
   * Returns the HTTP address for a peer. When the peer's HTTP port was not declared in the server
   * list, the address is derived as a best effort from the peer's Raft host plus this node's HTTP
   * port (see {@link #resolveHttpAddress(RaftPeerId)}); returns {@code null} only if the peer is
   * unknown or this node's HTTP port is not yet available.
   */
  public String getPeerHttpAddress(final RaftPeerId peerId) {
    return resolveHttpAddress(peerId);
  }

  /**
   * Returns the HTTPS address (host:httpsPort) for a peer, or {@code null} when no HTTPS endpoint can
   * be resolved. The endpoint is taken from the explicit 5th field of the server list when present;
   * otherwise, on a homogeneous cluster, it is derived from the peer's Raft host plus this node's
   * local HTTPS listening port. Returns {@code null} when SSL is disabled, this node has no HTTPS
   * listener, or the peer is unknown. Used to download snapshots over real HTTPS instead of forcing
   * an HTTPS scheme onto the plain HTTP port (issue #4470).
   */
  public String getPeerHttpsAddress(final RaftPeerId peerId) {
    return resolveHttpsAddress(peerId);
  }

  /**
   * Leader-driven recovery for a persistently STALLED replica (issue #4728). Invoked by
   * {@link ClusterMonitor} on the leader's lag-monitor thread once a replica's {@code matchIndex} has
   * not advanced for {@link GlobalConfiguration#HA_STALLED_REPLICA_RESYNC_DURATION_MS} while the
   * leader kept committing. The leader instructs the stuck follower to drop its local copy and
   * re-acquire a fresh full snapshot, the same operation an operator runs manually via
   * {@code POST /api/v1/cluster/resync/{database}} - reused here so the cluster self-heals instead of
   * logging the stall forever.
   * <p>
   * The HTTP work is done on a short-lived daemon thread so the lag-monitor thread is never blocked
   * on network I/O. Authenticated with the inter-node cluster token (the same trust mechanism used
   * for snapshot transfer), never with operator credentials.
   */
  void forceResyncStalledReplica(final String peerId) {
    if (peerId == null || !isLeader())
      return;

    final RaftPeerId targetId = RaftPeerId.valueOf(peerId);
    if (targetId.equals(localPeerId))
      return; // never resync the leader itself

    // On an SSL-enabled cluster the follower listens on HTTPS (a different port from plain HTTP), so
    // prefer its HTTPS endpoint; mirror SnapshotInstaller's behaviour and fall back to plain HTTP
    // only when no HTTPS endpoint is known.
    final boolean useSSL = configuration.getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    boolean https = false;
    String followerAddr = null;
    if (useSSL) {
      followerAddr = getPeerHttpsAddress(targetId);
      if (followerAddr != null)
        https = true;
    }
    if (followerAddr == null)
      followerAddr = getPeerHttpAddress(targetId);
    if (followerAddr == null) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot force resync of stalled replica '%s': its address is unknown", peerId);
      return;
    }

    final String clusterToken = getClusterToken();
    if (clusterToken == null || clusterToken.isEmpty()) {
      // The follower's resync endpoint only accepts inter-node calls authenticated with the cluster
      // token; without one the request is doomed to be rejected, so do not even attempt it.
      LogManager.instance().log(this, Level.WARNING,
          "Cannot force resync of stalled replica '%s': cluster token is not configured", peerId);
      return;
    }

    final String address = followerAddr;
    final boolean useHttps = https;

    stalledResyncExecutor.execute(() -> {
      // TODO (#4728): this resyncs EVERY non-reserved database on the follower. When a cluster hosts
      // many databases and only one is stuck, this forces unnecessary full snapshots of the others.
      // A per-database stall signal would let us scope the resync; acceptable as a first pass.
      for (final String dbName : arcadeServer.getDatabaseNames()) {
        // Re-check leadership on each iteration: the outer guard ran before this task was queued, and
        // leadership may have moved since. A resync request from a non-leader is harmless (the
        // follower rejects it) but would log spurious warnings, so stop early instead.
        if (!isLeader())
          return;
        if (ArcadeDBServer.isReservedDatabaseName(dbName))
          continue;
        try {
          requestRemoteResync(address, dbName, clusterToken, useHttps);
          LogManager.instance().log(this, Level.INFO,
              "Requested resync of database '%s' on stalled replica '%s' (%s)", dbName, peerId, address);
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING,
              "Failed to request resync of database '%s' on stalled replica '%s' (%s): %s",
              dbName, peerId, address, e.getMessage());
        }
      }
    });
  }

  /**
   * Leader-side recovery for a follower whose outbound replication gRPC channel has wedged on a stale
   * DNS result after the follower restarted with a new address (issue #4696, "Gap 1"). Invoked by
   * {@link ClusterMonitor} on the leader's lag-monitor thread once the follower has stayed continuously
   * unreachable (no successful RPC) for {@link GlobalConfiguration#HA_PEER_CHANNEL_RESET_DURATION}.
   * <p>
   * Ratis keeps one outbound {@code GrpcServerProtocolClient} (wrapping a gRPC {@code ManagedChannel})
   * per follower in the leader's server-RPC {@code PeerProxyMap}. When the follower's pod IP changes,
   * grpc-java can keep returning the stale/negative DNS result on that cached channel indefinitely -
   * Ratis's own error path only calls {@code resetConnectBackoff()}, which never recreates the channel
   * or re-resolves DNS - so the appender never reconnects. {@code resetProxy} closes that one channel
   * and drops it from the map; the appender's next send rebuilds a fresh channel against the same peer
   * DNS name, which re-resolves to the follower's current IP. Only this one follower's channel is
   * touched and leadership is unchanged, so there is no flapping risk (unlike a leadership transfer).
   * <p>
   * This is the automatic, less-disruptive alternative to the manual {@code transferLeadership} lever
   * that Gap 1 was previously left to.
   */
  void resetPeerReplicationChannel(final String peerId) {
    final RaftServer server = raftServer;
    if (peerId == null || server == null || shutdownRequested || !isLeader())
      return;

    final RaftPeerId targetId = RaftPeerId.valueOf(peerId);
    if (targetId.equals(localPeerId))
      return; // the leader keeps no appender channel to itself

    // ClusterMonitor already logs the operator-facing WARNING announcing the reset (with the attempt
    // count and how long the follower has been unreachable), so a success is only confirmed at FINE to
    // avoid a redundant second WARNING. A no-op is the surprising case worth surfacing at WARNING.
    if (resetPeerAppenderChannel(server.getServerRpc(), targetId))
      LogManager.instance().log(this, Level.FINE,
          "Reset the replication gRPC channel to unreachable follower '%s' to force a fresh DNS re-resolution and reconnect (issue #4696).",
          peerId);
    else
      LogManager.instance().log(this, Level.WARNING,
          "Requested a replication-channel reset for follower '%s' but the Raft server RPC is not proxy-based; cannot reset the channel.",
          peerId);
  }

  /**
   * Closes and re-creates the leader's outbound gRPC proxy (channel) for {@code peerId}, forcing a
   * fresh DNS re-resolution on the next send. Returns {@code true} when the reset was applied, or
   * {@code false} when the RPC layer is not the expected proxy-based Ratis implementation.
   * <p>
   * Package-private and static so the Ratis-coupling seam can be unit-tested without a live cluster.
   */
  static boolean resetPeerAppenderChannel(final RaftServerRpc rpc, final RaftPeerId peerId) {
    if (rpc instanceof RaftServerRpcWithProxy<?, ?> withProxy) {
      final var proxies = withProxy.getProxies();
      if (proxies != null) {
        proxies.resetProxy(peerId);
        return true;
      }
    }
    return false;
  }

  private static ThreadPoolExecutor createStalledResyncExecutor() {
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(0, 1, 30L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(16), r -> {
      final Thread t = new Thread(r, "arcadedb-raft-stalled-resync");
      t.setDaemon(true);
      return t;
    }, new ThreadPoolExecutor.CallerRunsPolicy());
    return executor;
  }

  /**
   * Sends {@code POST /api/v1/cluster/resync/{database}} to a follower, authenticated with the
   * cluster token. When {@code https} is true the connection uses the cluster SSL context (the same
   * one used for snapshot transfer). Visible for testing. Throws on a non-2xx response.
   */
  void requestRemoteResync(final String followerAddr, final String databaseName, final String clusterToken,
      final boolean https) throws IOException {
    final String url = (https ? "https://" : "http://") + followerAddr + "/api/v1/cluster/resync/"
        + URLEncoder.encode(databaseName, StandardCharsets.UTF_8);
    final HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    try {
      if (conn instanceof final HttpsURLConnection httpsConn)
        httpsConn.setSSLSocketFactory(SnapshotInstaller.buildSSLContext(arcadeServer).getSocketFactory());
      conn.setRequestMethod("POST");
      conn.setConnectTimeout(10_000);
      // The resync endpoint is synchronous: it downloads the full snapshot from the leader and only
      // then returns, so the read timeout must accommodate a snapshot transfer, not just a trigger.
      conn.setReadTimeout(120_000);
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      // Inter-node forwarded auth: the cluster token plus the forwarded root user, validated by
      // AbstractServerHttpHandler before the handler runs (same mechanism used for follower->leader
      // request forwarding). No operator credentials are sent over the wire.
      if (clusterToken != null && !clusterToken.isEmpty()) {
        conn.setRequestProperty("X-ArcadeDB-Cluster-Token", clusterToken);
        conn.setRequestProperty("X-ArcadeDB-Forwarded-User", FORWARDED_ROOT_USER);
      }
      try (final OutputStream os = conn.getOutputStream()) {
        os.write("{}".getBytes(StandardCharsets.UTF_8));
      }
      final int code = conn.getResponseCode();
      if (code < 200 || code >= 300) {
        drainErrorStream(conn);
        throw new IOException("resync endpoint returned HTTP " + code);
      }
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Drains and discards the error response body so the underlying keep-alive socket can be reused by
   * the JVM connection pool instead of being abandoned (and leaked).
   */
  private static void drainErrorStream(final HttpURLConnection conn) {
    try (final var err = conn.getErrorStream()) {
      if (err != null)
        err.readAllBytes();
    } catch (final IOException ignored) {
      // best-effort cleanup
    }
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

    final File storageDir = getRaftStorageDir();
    // Only delete existing Raft storage when persistence is not requested.
    // Persistent mode (HA_RAFT_PERSIST_STORAGE=true) is used in tests that restart nodes
    // within a single test run, so the Raft log survives across stop/start calls.
    final boolean persistStorage = resolvePersistStorage(configuration);
    if (storageDir.exists() && !persistStorage)
      deleteRecursive(storageDir);
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    this.tokenProvider = new ClusterTokenProvider(configuration);
    this.tokenProvider.initClusterToken();

    // When persistent storage is requested and the storage directory already has data,
    // use RECOVER mode so Ratis loads the existing Raft log instead of trying to format
    // (which would fail if the group directory already exists).
    final File[] storageDirs = storageDir.listFiles(f -> f.isDirectory() && !"lost+found".equals(f.getName()));
    final boolean hasExistingStorage = persistStorage && storageDir.exists()
        && storageDirs != null && storageDirs.length > 0;
    final RaftStorage.StartupOption startupOption = hasExistingStorage
        ? RaftStorage.StartupOption.RECOVER
        : RaftStorage.StartupOption.FORMAT;

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
    final long grpcMessageSizeMax = configuration.getValueAsLong(GlobalConfiguration.HA_GRPC_MESSAGE_SIZE_MAX);
    final long maxQueuedBytes = configuration.getValueAsLong(GlobalConfiguration.HA_GROUP_COMMIT_MAX_QUEUED_BYTES);
    transactionBroker = new RaftTransactionBroker(raftClient, quorum, quorumTimeout, batchSize, queueSize, offerTimeout,
        grpcMessageSizeMax, maxQueuedBytes, this::refreshRaftClient);

    // Bootstrap election (issue #4147): runs once per cluster lifetime when this peer is elected
    // leader and the Raft log is still empty. Stays a no-op if disabled or on subsequent leader
    // changes. Wired into ArcadeStateMachine.notifyLeaderChanged below via runBootstrapIfEligible.
    this.bootstrapElection = new BootstrapElection(this, arcadeServer);

    // K8s auto-join / membership self-check: probe the peers' live Raft configuration and re-ADD this
    // node if absent. Runs on EVERY Kubernetes start, not only when the local Raft storage is missing
    // (issue #5275): a node restarting with persisted storage may have been dropped from the committed
    // configuration while it was down (e.g. by the pre-#5275 shutdown auto-leave), and its own storage
    // still claiming membership meant it waited forever for a leader that would never dial a non-member.
    // When the node is still a member the probe is a cheap no-op ("already a member").
    // Runs on a background thread WITH retry (issue #5268): the one-shot probe reliably lost the race
    // against the peers' allowlist DNS refresh on pod recreation and then parked forever. Retries stop
    // as soon as a leader is visible - at that point this node is either a functioning member or a
    // cold-start election completed - or on shutdown.
    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S)) {
      final KubernetesAutoJoin autoJoin = new KubernetesAutoJoin(arcadeServer, raftGroup, localPeerId, raftProperties);
      final Thread joinThread = new Thread(
          () -> autoJoin.tryAutoJoinWithRetry(() -> !shutdownRequested && getLeaderId() == null),
          "arcadedb-k8s-auto-join");
      joinThread.setDaemon(true);
      joinThread.start();
      this.autoJoinThread = joinThread;
    }

    final long healthInterval = configuration.getValueAsLong(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL);
    final long staleFollowerLagThreshold = configuration.getValueAsLong(GlobalConfiguration.HA_STALE_FOLLOWER_LAG_THRESHOLD);
    final long staleFollowerRecoveryDurationMs = configuration.getValueAsLong(
        GlobalConfiguration.HA_STALE_FOLLOWER_RECOVERY_DURATION_MS);
    final boolean divergedFollowerRecovery = configuration.getValueAsBoolean(
        GlobalConfiguration.HA_DIVERGED_FOLLOWER_RECOVERY);
    final int divergedFollowerMaxReformats = configuration.getValueAsInteger(
        GlobalConfiguration.HA_DIVERGED_FOLLOWER_MAX_REFORMATS);
    // Reuse the Ratis restart-retry cap as the crash-loop escalation threshold (issue #5291): a RECOVER
    // restart that keeps returning to CLOSED (e.g. a term-inverted persisted log) never trips restartRatis()'s
    // own escape because the server object builds successfully each time, so the health monitor bounds the
    // non-sticking restart streak here and escalates (reformat once, then give up loudly).
    final int crashLoopRestartThreshold = configuration.getValueAsInteger(
        GlobalConfiguration.HA_RATIS_RESTART_MAX_RETRIES);
    this.healthMonitor = new HealthMonitor(this, healthInterval, staleFollowerLagThreshold, staleFollowerRecoveryDurationMs,
        divergedFollowerRecovery, divergedFollowerMaxReformats, crashLoopRestartThreshold);
    this.healthMonitor.start();

    // Periodic snapshot/log-purge trigger (issue #5345). Started on every node, not only the leader:
    // Ratis purges each server's own log against that server's own snapshot index, so a follower whose
    // snapshot index never advances fills its volume even while the leader stays healthy.
    final long snapshotInterval = configuration.getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_INTERVAL);
    final long snapshotMinEntries = configuration.getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_MIN_ENTRIES);
    final int raftStorageMinFreeSpacePerc = configuration.getValueAsInteger(
        GlobalConfiguration.HA_RAFT_STORAGE_MIN_FREE_SPACE_PERC);
    this.logCompactionScheduler = new RaftLogCompactionScheduler(new RaftLogCompactionTarget(), snapshotInterval,
        snapshotMinEntries, raftStorageMinFreeSpacePerc);
    this.logCompactionScheduler.start();

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING))
      this.resyncProgressTracker = new FollowerResyncProgressTracker(
          configuration.getValueAsLong(GlobalConfiguration.HA_RESYNC_PROGRESS_INTERVAL),
          configuration.getValueAsLong(GlobalConfiguration.HA_RESYNC_CATCHUP_LAG_THRESHOLD));
  }

  /**
   * Trigger the offline bootstrap election if conditions allow (this peer is leader, the cluster's
   * commit index is still 0, and {@code arcadedb.ha.bootstrapFromLocalDatabase=true}). Called from
   * {@link ArcadeStateMachine#notifyLeaderChanged} on every leader change. Issue #4147 phase 4.
   *
   * @return the protocol outcome (visible to tests; production code does not branch on it).
   */
  public BootstrapElection.Outcome runBootstrapIfEligible() {
    final BootstrapElection election = bootstrapElection;
    if (election == null)
      return BootstrapElection.Outcome.SKIPPED_DISABLED;
    election.onLeaderChanged();
    return election.runIfEligible();
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
    final LifeCycle.State proxyState = raftServer.getLifeCycleState();
    if (proxyState != LifeCycle.State.RUNNING)
      return proxyState;
    // The RaftServer proxy can stay RUNNING while the per-group division (RaftServerImpl) underneath
    // is CLOSED or EXCEPTION - the node then rejects every vote/append with ServerNotReadyException
    // while looking perfectly healthy to anything that only reads the proxy state. That blind spot
    // left a stranded voter CLOSED indefinitely (the health monitor saw RUNNING and never recovered
    // it), silently zeroing the cluster's fault tolerance and turning a leader restart into a
    // permanent full-cluster outage (issue #5271). Report the division's terminal states so the
    // health monitor treats a dead division exactly like a dead server.
    try {
      final RaftServer.Division division = raftServer.getDivision(raftGroup.getGroupId());
      final LifeCycle.State divisionState = division.getInfo().getLifeCycleState();
      if (divisionState == LifeCycle.State.CLOSED || divisionState == LifeCycle.State.EXCEPTION)
        return divisionState;
    } catch (final Exception e) {
      // The division cannot be read - typically a transient window while the server is starting or an
      // in-place restart is re-registering the group. Report the (RUNNING) proxy state rather than
      // CLOSED: mapping this window to CLOSED would make the health monitor restart a healthy,
      // still-initializing server in a loop. A genuinely dead division reports CLOSED/EXCEPTION from
      // getInfo() above once readable.
      LogManager.instance().log(this, Level.FINE, "Cannot read Raft division state: %s", e.getMessage());
    }
    return proxyState;
  }

  /**
   * The live Ratis division (per-group {@code RaftServerImpl}) for the cluster group.
   * Package-private: used by tests that need to fault-inject a dead division (issue #5271).
   *
   * @throws IOException when the group is not present on this server
   */
  RaftServer.Division getRaftDivision() throws IOException {
    return raftServer.getDivision(raftGroup.getGroupId());
  }

  @Override
  public boolean isShutdownRequested() {
    return shutdownRequested;
  }

  /**
   * Stale-follower detection for the {@link HealthMonitor} (issue #3893): true only when this node
   * is a running follower lagging more than {@code lagThreshold} entries behind the commit index,
   * is NOT actively catching up, has no snapshot download already pending, AND is not making forward
   * progress on its applied index (issue #4840). Returns false for the leader and whenever the Raft
   * state cannot be read.
   * <p>
   * The progress check is what distinguishes a healthy-but-slow follower from a genuinely stuck one.
   * The {@link ArcadeStateMachine} {@code catchingUp} flag only trips on a snapshot-style index jump
   * ({@code gap > 1}); a follower applying steady-state AppendEntries one entry at a time never sets it,
   * so without comparing the applied index between ticks a slow-but-progressing follower under sustained
   * writes would be misclassified as lagging and needlessly resynced. By remembering the applied index
   * seen on the previous tick, this returns false as soon as the index advances, and only true while the
   * index is stuck beyond the threshold.
   */
  @Override
  public boolean isFollowerLaggingBeyond(final long lagThreshold) {
    if (raftServer == null || shutdownRequested || isLeader()) {
      lastLagCheckAppliedIndex = -1; // not a follower right now: drop the progress baseline
      return false;
    }
    final ArcadeStateMachine sm = stateMachine;
    if (sm == null || sm.isCatchingUp() || sm.isSnapshotDownloadPending()) {
      lastLagCheckAppliedIndex = -1; // already known to be catching up: re-baseline when normal checks resume
      return false;
    }
    final long commit = getCommitIndex();
    final long applied = getLastAppliedIndex();
    final boolean lagging = isPersistentlyLagging(commit, applied, lagThreshold, lastLagCheckAppliedIndex);
    if (applied >= 0)
      lastLagCheckAppliedIndex = applied;
    return lagging;
  }

  /**
   * Pure decision function behind {@link #isFollowerLaggingBeyond(long)}, split out so the predicate can be
   * unit-tested without the Ratis state plumbing. Returns {@code true} only for a follower that is both far
   * enough behind ({@code commitIndex - appliedIndex > lagThreshold}) AND stuck (no forward progress on the
   * applied index since the previous tick). A follower applying entries one at a time keeps
   * {@code appliedIndex > previousAppliedIndex} every tick, so it returns {@code false}: healthy-but-slow,
   * must not be resynced (issue #4840). A negative {@code commitIndex}/{@code appliedIndex} (state not
   * readable this tick) or a lag within the threshold returns {@code false}. {@code previousAppliedIndex < 0}
   * means "no prior sample" (first observation or just resumed follower duty): progress cannot be determined
   * yet, so the lag alone decides - the {@link HealthMonitor}'s persistence window still requires the
   * condition to repeat before it acts.
   */
  static boolean isPersistentlyLagging(final long commitIndex, final long appliedIndex, final long lagThreshold,
      final long previousAppliedIndex) {
    if (commitIndex < 0 || appliedIndex < 0)
      return false;
    if (commitIndex - appliedIndex <= lagThreshold)
      return false;
    // Forward progress since the previous tick: the follower is actively replaying the backlog one entry
    // at a time, so it is not stuck and must not be resynced even though it still trails the leader.
    if (previousAppliedIndex >= 0 && appliedIndex > previousAppliedIndex)
      return false;
    return true;
  }

  @Override
  public void recoverFromPersistentLag() {
    final ArcadeStateMachine sm = stateMachine;
    if (sm != null)
      sm.recoverFromPersistentLag();
  }

  @Override
  public void reportResyncProgress() {
    final FollowerResyncProgressTracker tracker = resyncProgressTracker;
    if (tracker == null || raftServer == null || shutdownRequested || isLeader())
      return;
    final ArcadeStateMachine sm = stateMachine;
    if (sm == null || sm.isSnapshotDownloadPending())
      return; // the snapshot path logs its own bookends
    final long applied = getLastAppliedIndex();
    final long commit = getCommitIndex();
    final FollowerResyncProgressTracker.Tick tick = tracker.onTick(applied, commit, System.currentTimeMillis());
    if (tick.event() != FollowerResyncProgressTracker.Event.NONE)
      LogManager.instance().log(this, Level.INFO, tick.message());
  }

  /**
   * Stuck-divergence detection for the {@link HealthMonitor} (issue #4741): true only when this node
   * is a running follower that recognizes a leader at a newer term yet cannot apply the leader's
   * current-term entries because its Raft log diverged. The signal is term-based rather than a lag
   * count because the divergence can be a single entry on an otherwise idle cluster, where the
   * lag-based recoveries (HA_STALE_FOLLOWER_LAG_THRESHOLD, HA_STALLED_REPLICA_RESYNC_DURATION_MS)
   * never fire.
   * <p>
   * A healthy follower carries the leader's current-term entry (e.g. the post-election no-op), so its
   * last-applied term equals the current term. A stuck-diverged follower keeps rejecting the leader's
   * AppendEntries (term conflict), so it has applied everything it could locally commit
   * ({@code commitIndex == appliedIndex}) yet its last-applied entry is from an older term
   * ({@code currentTerm > appliedTerm}). The {@link HealthMonitor} requires this to persist for
   * {@code HA_STALE_FOLLOWER_RECOVERY_DURATION_MS} before acting, which filters out the brief window
   * around an election before the no-op commits. Returns false for the leader, when no leader is
   * known, while actively catching up or installing a snapshot, and whenever the state cannot be read.
   */
  @Override
  public boolean isFollowerStuckDiverged() {
    if (raftServer == null || shutdownRequested || isLeader())
      return false;
    final ArcadeStateMachine sm = stateMachine;
    if (sm == null)
      return false;
    final TermIndex applied = sm.getLastAppliedTermIndex();
    if (applied == null)
      return false;
    // The values below are read as separate Ratis getDivision(...) calls, so they can observe slightly
    // different moments during an election. We deliberately do not take an atomic snapshot: the
    // HealthMonitor requires the stuck condition to persist for HA_STALE_FOLLOWER_RECOVERY_DURATION_MS
    // before acting, which absorbs any one-tick inconsistency here.
    return isStuckDivergedState(getLeaderId() != null, sm.isCatchingUp(), sm.isSnapshotDownloadPending(),
        getCurrentTerm(), applied.getTerm(), applied.getIndex(), getCommitIndex());
  }

  /**
   * Pure decision function behind {@link #isFollowerStuckDiverged()}, split out so the predicate can be
   * unit-tested in isolation from the Ratis state plumbing (the destructive recovery makes the predicate
   * the highest-risk piece). Returns {@code true} for the "stuck at a stale term" signature: a follower
   * that recognizes a leader, is neither catching up nor installing a snapshot, has applied everything it
   * could locally commit ({@code commitIndex == appliedIndex}) yet at a stale term
   * ({@code currentTerm > appliedTerm}). Any negative term/index input (state not readable) yields
   * {@code false}, including a negative {@code appliedTerm}.
   * <p>
   * Note this signature is satisfied by a genuine Raft-log divergence and also, in principle, by a
   * follower that simply cannot receive the leader's current-term entries for a sustained period (a
   * one-sided outage where heartbeats still arrive). The caller relies on the persistence window to
   * separate a transient hiccup from a stuck state; both resolve to the same safe (non-data-losing)
   * recovery, so the predicate does not try to distinguish them.
   * <p>
   * A freshly (re)joined / empty node does not match: before it applies anything its applied
   * {@link TermIndex} is null (handled by the caller), while it is catching up it has
   * {@code commitIndex > appliedIndex}, and once caught up its applied term equals the current term.
   */
  static boolean isStuckDivergedState(final boolean leaderPresent, final boolean catchingUp,
      final boolean snapshotPending, final long currentTerm, final long appliedTerm, final long appliedIndex,
      final long commitIndex) {
    if (!leaderPresent || catchingUp || snapshotPending)
      return false;
    if (currentTerm < 0 || appliedTerm < 0 || appliedIndex < 0 || commitIndex < 0)
      return false;
    // We have applied everything we could locally commit, but at a stale term: we are rejecting the
    // leader's current-term entries and cannot move forward.
    return currentTerm > appliedTerm && commitIndex == appliedIndex;
  }

  @Override
  public void recoverFromDivergence() {
    if (shutdownRequested || isLeader())
      return;
    // Log the exact term/index values so an operator can confirm post-incident whether this was a genuine
    // Raft-log divergence or a follower merely stuck at a stale term for another reason (e.g. a sustained
    // one-sided network outage where heartbeats arrive but the leader's current-term entries do not).
    final ArcadeStateMachine sm = stateMachine;
    final TermIndex applied = sm != null ? sm.getLastAppliedTermIndex() : null;
    LogManager.instance().log(this, Level.WARNING,
        "Follower stuck at a stale term (currentTerm=%d, appliedTerm=%d, appliedIndex=%d, commitIndex=%d); "
            + "reformatting Raft storage and rejoining so the leader can reconcile it via snapshot-install. "
            + "If this recurs without a genuine log divergence, suspect a sustained one-sided outage to the leader.",
        getCurrentTerm(), applied != null ? applied.getTerm() : -1L, applied != null ? applied.getIndex() : -1L,
        getCommitIndex());
    restartRatis(true);
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
    restartRatis(false);
  }

  /**
   * Builds a fully wired {@link ArcadeStateMachine}. Both collaborators must be set: {@code setServer}
   * gives it the {@link ArcadeDBServer} and {@code setRaftHAServer} the owning {@code RaftHAServer}.
   * Missing the latter leaves {@code raftHAServer} null on the new machine, so the recovered node can
   * no longer install snapshots ({@code notifyInstallSnapshotFromLeader} NPEs), never marks
   * locally-originated transactions (origin-skip fails, the leader re-applies its own committed txns),
   * and silently skips leader-change handling (issue #4839). Used by both the startup constructor and
   * the {@link #restartRatis(boolean)} recovery path so the two can never drift.
   */
  private ArcadeStateMachine createStateMachine() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(arcadeServer);
    sm.setRaftHAServer(this);
    return sm;
  }

  /**
   * Restarts the local Ratis server. When {@code formatStorage} is {@code true} the Raft storage
   * directory is deleted first and the server starts with {@link RaftStorage.StartupOption#FORMAT},
   * so this peer rejoins the group with an empty log and is reconciled by the leader via the
   * snapshot-install path. This is the in-process equivalent of an operator restarting a node whose
   * Raft log diverged (issue #4741). When {@code false} the existing storage is preserved and the
   * server starts with {@link RaftStorage.StartupOption#RECOVER} (the CLOSED/EXCEPTION recovery path).
   */
  private void restartRatis(final boolean formatStorage) {
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
          try {
            arcadeServer.stop();
          } catch (final Exception ignored) {
          }
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
        this.stateMachine = createStateMachine();

        final RaftProperties properties = RaftPropertiesBuilder.build(configuration);
        final File storageDir = getRaftStorageDir();
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

        // For a divergence reformat (#4741) discard the local Raft log so this peer rejoins as a fresh
        // bootstrapping member; otherwise recover the existing log in place (CLOSED/EXCEPTION path).
        final RaftStorage.StartupOption startupOption;
        if (formatStorage) {
          if (storageDir.exists()) {
            deleteRecursive(storageDir);
            // deleteRecursive swallows per-file failures (e.g. a locked file or a permission issue on
            // Windows). FORMAT on a non-empty directory is undefined behaviour, so abort instead of
            // proceeding: the throw is caught below, increments restartFailureCount, and the divergence
            // reformat budget already counted this as an attempt - so the failure escalates through the
            // existing retry/backoff machinery rather than starting Ratis on a half-deleted directory.
            if (storageDir.exists())
              throw new IOException("Could not fully delete Raft storage directory " + storageDir.getAbsolutePath()
                  + " before reformat; aborting to avoid FORMAT on a non-empty directory");
          }
          startupOption = RaftStorage.StartupOption.FORMAT;
        } else
          startupOption = RaftStorage.StartupOption.RECOVER;

        this.raftServer = RaftServer.newBuilder()
            .setServerId(localPeerId)
            .setGroup(raftGroup)
            .setStateMachine(stateMachine)
            .setProperties(properties)
            .setParameters(buildParameters(configuration))
            .setOption(startupOption)
            .build();
        this.raftServer.start();
        this.raftProperties = properties;
        this.raftClient = buildRaftClient(raftGroup, properties);

        final int batchSize = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_BATCH_SIZE);
        final int queueSize = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_QUEUE_SIZE);
        final int offerTimeout = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_OFFER_TIMEOUT);
        final long grpcMessageSizeMax = configuration.getValueAsLong(GlobalConfiguration.HA_GRPC_MESSAGE_SIZE_MAX);
        final long maxQueuedBytes = configuration.getValueAsLong(GlobalConfiguration.HA_GROUP_COMMIT_MAX_QUEUED_BYTES);
        this.transactionBroker = new RaftTransactionBroker(raftClient, quorum, quorumTimeout, batchSize, queueSize,
            offerTimeout, grpcMessageSizeMax, maxQueuedBytes, this::refreshRaftClient);

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
    final Thread joinThread = autoJoinThread;
    if (joinThread != null) {
      // Wake the auto-join retry loop out of its backoff sleep so shutdown is not delayed (issue #5268).
      joinThread.interrupt();
      autoJoinThread = null;
    }
    if (healthMonitor != null) {
      healthMonitor.stop();
      healthMonitor = null;
    }
    if (logCompactionScheduler != null) {
      logCompactionScheduler.stop();
      logCompactionScheduler = null;
    }
    stopLagMonitor();
    stalledResyncExecutor.shutdownNow();
    if (transactionBroker != null) {
      transactionBroker.stop();
      transactionBroker = null;
    }

    // NOTE (issue #5275): this method deliberately does NOT remove the node from the Raft
    // configuration. The former Kubernetes auto-leave here meant every graceful pod shutdown (rollout,
    // eviction, kubectl delete pod) silently shrank the committed membership - and because persisted
    // Raft storage skipped the boot-time auto-join, the recreated pod (same StatefulSet name) was never
    // re-added: fault tolerance silently dropped while every status surface still showed a full cluster.
    // A pod shutdown means "temporarily unreachable", not "gone": the same pod name comes back by
    // construction. An intentional scale-down must remove the peer explicitly (POST /api/v1/cluster/leave
    // before stopping, or DELETE /api/v1/cluster/peer/<id> from a surviving node).

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
    } catch (final Exception e) {
      // Also guards the IllegalStateException Ratis throws while an in-place restart re-initializes
      // the division (issue #5271): status reads must degrade to "unknown", never propagate.
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
    } catch (final Exception e) {
      // Also guards the IllegalStateException Ratis throws while an in-place restart re-initializes
      // the division (issue #5271). Callers (including the k8s auto-join retry loop) treat null as
      // "leader unknown"; propagating here would kill background threads mid-restart.
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
   * is unknown. When the leader's HTTP port was not declared in the server list, the address is
   * derived as a best effort from the leader's Raft host plus this node's HTTP port (see
   * {@link #resolveHttpAddress(RaftPeerId)}).
   */
  public String getLeaderHttpAddress() {
    return resolveHttpAddress(getLeaderId());
  }

  /**
   * Returns the HTTPS address (host:httpsPort) of the current Raft leader, or {@code null} when no
   * HTTPS endpoint can be resolved (SSL disabled, no local HTTPS listener, or leader unknown).
   * See {@link #getPeerHttpsAddress(RaftPeerId)}.
   */
  public String getLeaderHttpsAddress() {
    return resolveHttpsAddress(getLeaderId());
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
      final long grpcMessageSizeMax = configuration.getValueAsLong(GlobalConfiguration.HA_GRPC_MESSAGE_SIZE_MAX);
      final long maxQueuedBytes = configuration.getValueAsLong(GlobalConfiguration.HA_GROUP_COMMIT_MAX_QUEUED_BYTES);
      final RaftTransactionBroker oldBroker = transactionBroker;
      transactionBroker = new RaftTransactionBroker(raftClient, quorum, quorumTimeout, batchSize, queueSize,
          offerTimeout, grpcMessageSizeMax, maxQueuedBytes, this::refreshRaftClient);
      // Transfer undispatched entries from the old broker to the new one BEFORE stopping the old
      // broker, so a brief leader hiccup (e.g. self-stepdown -> re-elected leader) does not surface
      // "Group committer shutting down" errors to in-flight callers. transferPendingTo() also halts
      // the old flusher, so the subsequent stop() just closes the now-empty queue.
      oldBroker.transferPendingTo(transactionBroker);
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

  /**
   * The live {@link RaftProperties} the Ratis server was started with, or {@code null} before
   * {@link #start()}. Package-private: used by tests that exercise {@link KubernetesAutoJoin}
   * directly against a running cluster (issue #5275).
   */
  RaftProperties getRaftProperties() {
    return raftProperties;
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

    final RaftPeerId statsLeaderId = getLeaderId();
    final RaftPeerId statsExcludeId = statsLeaderId != null ? statsLeaderId : localPeerId;
    final List<Map<String, String>> replicas = new ArrayList<>();
    for (final RaftPeer peer : raftGroup.getPeers()) {
      if (!peer.getId().equals(statsExcludeId)) {
        final Map<String, String> replicaInfo = new HashMap<>();
        replicaInfo.put("id", peer.getId().toString());
        replicaInfo.put("address", peer.getAddress());
        final String httpAddr = resolveHttpAddress(peer);
        if (httpAddr != null)
          replicaInfo.put("httpAddress", httpAddr);
        replicas.add(replicaInfo);
      }
    }
    stats.put("replicas", replicas);
    return stats;
  }

  public String getReplicaAddresses() {
    final RaftPeerId leaderId = getLeaderId();
    final RaftPeerId excludeId = leaderId != null ? leaderId : localPeerId;
    final StringBuilder sb = new StringBuilder();
    for (final RaftPeer peer : raftGroup.getPeers()) {
      if (!peer.getId().equals(excludeId)) {
        final String httpAddr = resolveHttpAddress(peer);
        if (httpAddr != null) {
          if (!sb.isEmpty())
            sb.append(",");
          sb.append(httpAddr);
        }
      }
    }
    return sb.toString();
  }

  /**
   * Builds a single-snapshot Bolt routing table (leader as writer, followers as readers) from one
   * {@link #getLeaderId()} read, so a concurrent leader change cannot make the writer and reader sets
   * mutually inconsistent. Returns {@code null} when no leader is known or the leader has no resolvable
   * Bolt address. Readers reflect the configured cluster membership, matching {@link #getReplicaAddresses()};
   * peers whose Bolt address cannot be resolved are skipped.
   */
  public HAServerPlugin.BoltRoutingTable getBoltRoutingTable() {
    final RaftPeerId leaderId = getLeaderId();
    if (leaderId == null)
      return null;
    final String writer = resolveBoltAddress(leaderId);
    if (writer == null)
      return null;
    final List<String> readers = new ArrayList<>();
    for (final RaftPeer peer : raftGroup.getPeers()) {
      if (!peer.getId().equals(leaderId)) {
        final String reader = resolveBoltAddress(peer.getId());
        if (reader != null)
          readers.add(reader);
      }
    }
    return new HAServerPlugin.BoltRoutingTable(writer, List.copyOf(readers));
  }

  /**
   * Resolves the HTTP address (host:port) of a peer. When the peer's HTTP port was declared
   * explicitly in {@link GlobalConfiguration#HA_SERVER_LIST} (the {@code host:raftPort:httpPort}
   * syntax) that value is returned. Otherwise a best-effort address is synthesized by combining the
   * peer's Raft host with <em>this</em> node's HTTP listening port. The fallback is correct only for
   * homogeneous deployments where every node listens on the same HTTP port (e.g. a Kubernetes
   * StatefulSet); a one-time WARNING is logged so operators can declare explicit HTTP ports when the
   * cluster is heterogeneous. Returns {@code null} when the peer is unknown or this node's HTTP port
   * is not yet available.
   */
  private String resolveHttpAddress(final RaftPeerId peerId) {
    if (peerId == null)
      return null;
    final String configured = httpAddresses.get(peerId);
    if (configured != null)
      return configured;
    return deriveHttpAddressWithWarning(peerRaftAddress(peerId));
  }

  /** Overload that avoids the peer lookup when the {@link RaftPeer} is already in hand. */
  private String resolveHttpAddress(final RaftPeer peer) {
    final String configured = httpAddresses.get(peer.getId());
    if (configured != null)
      return configured;
    return deriveHttpAddressWithWarning(peer.getAddress());
  }

  /**
   * Resolves the client-reachable Bolt address (host:boltPort) of a peer. Returns the address declared
   * with the object-form {@code bolt:} field when present; otherwise derives it from the peer's Raft host
   * plus this node's local Bolt port. The fallback is correct only for homogeneous deployments where every
   * node listens on the same Bolt port (e.g. a Kubernetes StatefulSet); a one-time WARNING is logged so
   * operators declare explicit Bolt ports for heterogeneous clusters. Returns {@code null} when the peer is
   * unknown or the local Bolt port is unavailable.
   */
  private String resolveBoltAddress(final RaftPeerId peerId) {
    if (peerId == null)
      return null;
    final String configured = boltAddresses.get(peerId);
    if (configured != null)
      return configured;
    final int localBoltPort = configuration.getValueAsInteger(GlobalConfiguration.BOLT_PORT);
    final String derived = deriveBoltAddress(peerRaftAddress(peerId), localBoltPort);
    if (derived != null && boltFallbackWarned.compareAndSet(false, true))
      LogManager.instance().log(this, Level.WARNING,
          "HA Bolt routing addresses are not configured in '%s': deriving peer Bolt endpoints from each peer's Raft host plus this node's Bolt port (%d). "
              + "This is correct only when every node listens on the same Bolt port (e.g. a Kubernetes StatefulSet). For clusters with heterogeneous "
              + "Bolt ports, declare them explicitly using the 'host:{raft:..,bolt:..}' object syntax in %s.",
          GlobalConfiguration.HA_SERVER_LIST.getKey(), localBoltPort, GlobalConfiguration.HA_SERVER_LIST.getKey());
    return derived;
  }

  /**
   * Derives a Bolt address (host:boltPort) by combining a peer's Raft host with the given Bolt port.
   * Returns {@code null} when the port is not positive or the host cannot be extracted. Package-private
   * for testing.
   */
  static String deriveBoltAddress(final String raftAddress, final int boltPort) {
    if (raftAddress == null || boltPort <= 0)
      return null;
    final String host = extractHost(raftAddress);
    return host != null ? host + ":" + boltPort : null;
  }

  private String deriveHttpAddressWithWarning(final String raftAddress) {
    if (raftAddress == null)
      return null;
    final HttpServer httpServer = arcadeServer.getHttpServer();
    final int httpPort = httpServer != null ? httpServer.getPort() : -1;
    final String derived = deriveHttpAddress(raftAddress, httpPort);
    if (derived != null && httpFallbackWarned.compareAndSet(false, true))
      LogManager.instance().log(this, Level.WARNING,
          "HA HTTP addresses are not configured in '%s': deriving peer HTTP endpoints from each peer's Raft host plus this node's HTTP port (%d). "
              + "This is correct only when every node listens on the same HTTP port (e.g. a Kubernetes StatefulSet). For clusters with heterogeneous "
              + "HTTP ports, declare them explicitly using the 'host:raftPort:httpPort' syntax in %s.",
          GlobalConfiguration.HA_SERVER_LIST.getKey(), httpPort, GlobalConfiguration.HA_SERVER_LIST.getKey());
    return derived;
  }

  /**
   * Resolves the HTTPS address (host:httpsPort) of a peer for encrypted snapshot transfer. Prefers
   * the explicit HTTPS endpoint from the server list (5th field); otherwise derives it from the
   * peer's Raft host plus this node's local HTTPS listening port. Returns {@code null} when SSL is
   * disabled (no local HTTPS listener), the peer is unknown, or the port is not yet available.
   */
  private String resolveHttpsAddress(final RaftPeerId peerId) {
    if (peerId == null)
      return null;
    final String configured = httpsAddresses.get(peerId);
    if (configured != null)
      return configured;
    return deriveHttpsAddressWithWarning(peerRaftAddress(peerId));
  }

  private String deriveHttpsAddressWithWarning(final String raftAddress) {
    if (raftAddress == null)
      return null;
    final HttpServer httpServer = arcadeServer.getHttpServer();
    final int httpsPort = httpServer != null ? httpServer.getHttpsPort() : -1;
    if (httpsPort <= 0)
      return null; // SSL disabled or no HTTPS listener: caller falls back to plain HTTP
    final String derived = deriveHttpAddress(raftAddress, httpsPort);
    if (derived != null && httpsFallbackWarned.compareAndSet(false, true))
      LogManager.instance().log(this, Level.INFO,
          "HA HTTPS endpoints are not configured in '%s': deriving peer HTTPS endpoints for encrypted snapshot transfer "
              + "from each peer's Raft host plus this node's HTTPS port (%d). This is correct only when every node listens on the "
              + "same HTTPS port (e.g. a Kubernetes StatefulSet). For heterogeneous clusters, declare them explicitly using the "
              + "'host:raftPort:httpPort:priority:httpsPort' syntax in %s.",
          GlobalConfiguration.HA_SERVER_LIST.getKey(), httpsPort, GlobalConfiguration.HA_SERVER_LIST.getKey());
    return derived;
  }

  /** Returns the configured Raft address (host:port) of a peer, or {@code null} if not found. */
  private String peerRaftAddress(final RaftPeerId peerId) {
    for (final RaftPeer peer : raftGroup.getPeers())
      if (peer.getId().equals(peerId))
        return peer.getAddress();
    return null;
  }

  /**
   * Combines the host of a Raft address with the given HTTP port. Returns {@code null} when the port
   * is not yet known ({@code <= 0}) or the host cannot be extracted. Package-private for testing.
   */
  static String deriveHttpAddress(final String raftAddress, final int httpPort) {
    if (httpPort <= 0)
      return null;
    final String host = extractHost(raftAddress);
    return host != null ? host + ":" + httpPort : null;
  }

  /**
   * Extracts the host portion from a {@code host:port} or {@code [ipv6]:port} address. Returns the
   * input unchanged when it carries no port, or {@code null} when blank. Package-private for testing.
   */
  static String extractHost(final String address) {
    if (address == null || address.isEmpty())
      return null;
    final int closeBracket = address.lastIndexOf(']');
    if (closeBracket >= 0)
      return address.substring(0, closeBracket + 1); // [ipv6] literal, strip any :port after the bracket
    final int colon = address.lastIndexOf(':');
    return colon > 0 ? address.substring(0, colon) : address;
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

  /**
   * Reports whether this node is ready to serve traffic for the readiness probe (issue #4834): a leader is
   * known, this node is a member of the current Raft configuration, and - for a follower - the local
   * applied index is within {@code maxLagEntries} of the commit index. The leader is always caught up with
   * itself. Returns {@code false} before the Raft server has started, during shutdown, or when the state
   * cannot be read.
   * <p>
   * All inputs are read from a single {@code getDivision(...)} snapshot so leader/membership/commit/applied
   * come from one consistent view rather than re-resolving the division per field (cheaper, and avoids a
   * torn read if leadership changes mid-evaluation). Membership uses the <em>current</em> configuration
   * (the most recent conf in the log, which during a joint-consensus change may not yet be committed); that
   * is conservative for the targeted scenario - a wiped/lagging follower is not in it either way. See
   * {@link #isReadyForTrafficState} for the pure decision.
   */
  public boolean isReadyForTraffic(final long maxLagEntries) {
    final RaftServer server = raftServer;
    if (server == null || shutdownRequested)
      return false;
    try {
      final var division = server.getDivision(raftGroup.getGroupId());
      final var info = division.getInfo();
      final var conf = division.getRaftConf();
      final boolean leaderPresent = info.getLeaderId() != null;
      final boolean localInConfig = conf != null && isPeerInConfig(conf.getCurrentPeers(), localPeerId);
      final long commitIndex = division.getRaftLog().getLastCommittedIndex();
      final long appliedIndex = info.getLastAppliedIndex();
      // A snapshot resync in flight (or a database still diverged awaiting one) means this node's data
      // may be divergent; do not advertise Ready until it clears (issue #5273).
      final ArcadeStateMachine sm = getStateMachine();
      final boolean resyncInProgress = sm != null && sm.isResyncInProgress();
      return isReadyForTrafficState(leaderPresent, localInConfig, info.isLeader(), commitIndex, appliedIndex,
          maxLagEntries, resyncInProgress);
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.FINE, "Cannot read Raft state for readiness probe", e);
      return false;
    }
  }

  /**
   * Pure decision function behind {@link #isReadyForTraffic(long)}, split out so the predicate can be
   * unit-tested without the Ratis state plumbing. Returns {@code true} only when a leader is present and
   * this node is in the current configuration, and either this node is the leader (caught up with itself by
   * definition) or - as a follower - the lag {@code commitIndex - appliedIndex} is in
   * {@code [0, maxLagEntries]}. A negative {@code commitIndex}/{@code appliedIndex} (state not readable this
   * tick) or a negative lag ({@code appliedIndex > commitIndex}, an inconsistent state) returns
   * {@code false} so the probe fails closed rather than advertising Ready on unreadable/inconsistent state.
   */
  static boolean isReadyForTrafficState(final boolean leaderPresent, final boolean localInConfig,
      final boolean leader, final long commitIndex, final long appliedIndex, final long maxLagEntries) {
    return isReadyForTrafficState(leaderPresent, localInConfig, leader, commitIndex, appliedIndex, maxLagEntries,
        false);
  }

  /**
   * Overload of {@link #isReadyForTrafficState(boolean, boolean, boolean, long, long, long)} that also
   * fails closed while a snapshot resync is in flight (issue #5273). A follower that is downloading a
   * snapshot - or that still has a database marked diverged after a WAL version gap and awaiting its
   * resync - may hold divergent data, so it must not advertise Ready until the resync clears, even if
   * its raw applied-index lag happens to be within {@code maxLagEntries}. The leader never resyncs from
   * a peer, so this gate only affects followers in practice.
   */
  static boolean isReadyForTrafficState(final boolean leaderPresent, final boolean localInConfig,
      final boolean leader, final long commitIndex, final long appliedIndex, final long maxLagEntries,
      final boolean resyncInProgress) {
    if (!leaderPresent || !localInConfig)
      return false;
    if (resyncInProgress)
      return false;
    if (leader)
      return true;
    if (commitIndex < 0 || appliedIndex < 0)
      return false;
    final long lag = commitIndex - appliedIndex;
    return lag >= 0 && lag <= maxLagEntries;
  }

  /** True when {@code peerId} is a member of the given Raft peer set (the current configuration). */
  private static boolean isPeerInConfig(final Collection<RaftPeer> peers, final RaftPeerId peerId) {
    for (final RaftPeer peer : peers)
      if (peer.getId().equals(peerId))
        return true;
    return false;
  }

  Object getLeaderChangeNotifier() {
    return leaderChangeNotifier;
  }

  public void addPeer(final String peerId, final String address) {
    addPeer(peerId, address, null);
  }

  public void addPeer(final String peerId, final String address, final String name) {
    clusterManager.addPeer(peerId, address, name);
  }

  public void removePeer(final String peerId) {
    clusterManager.removePeer(peerId);
  }

  public void removePeer(final String peerId, final boolean force) {
    clusterManager.removePeer(peerId, force);
  }

  /**
   * Registers or updates the display name for a peer. The stored value is
   * formatted as {@code name (httpAddress)} when the peer's HTTP address is known.
   */
  void registerPeerDisplayName(final RaftPeerId peerId, final String name) {
    if (name == null || name.isEmpty())
      return;
    final String httpAddr = httpAddresses.get(peerId);
    peerDisplayNames.put(peerId, httpAddr != null ? name + " (" + httpAddr + ")" : name);
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
    final List<RaftPeer> candidates = selectStepDownTargets(getLivePeers(), localPeerId, clusterMonitor);

    for (final RaftPeer peer : candidates) {
      try {
        transferLeadership(peer.getId().toString(), 10_000);
        return;
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
            "Failed to step down (transfer to %s): %s", peer.getId(), e.getMessage());
      }
    }

    // No eligible explicit target (every other peer is a priority-0 witness/replica or is lagging, or
    // every explicit transfer failed). Delegate the choice to Ratis: the no-target transfer honors Raft
    // priorities and never elects a priority-0 peer, which is safer than abandoning the step-down (issue #4808).
    LogManager.instance().log(this, Level.INFO,
        "No explicit step-down target eligible; delegating leadership-transfer target selection to Ratis");
    if (transferLeadership(10_000L))
      return;

    LogManager.instance().log(this, Level.SEVERE,
        "Cannot step down: no other peer available for leadership transfer");
  }

  /**
   * Selects, in preference order, the peers eligible to receive leadership when this leader steps
   * down (issue #4808). A peer is eligible when it is not this node, is not lagging behind the leader
   * ({@link ClusterMonitor#isReplicaLagging}), and is not a priority-0 witness/replica while peers with
   * a higher priority exist - Ratis is configured never to elect a priority-0 peer, so handing it
   * leadership would just bounce it straight back and prolong write unavailability. The list is ordered
   * by descending Raft priority so the strongest candidate is tried first; equal priorities keep the
   * cluster iteration order.
   *
   * @return an ordered list of candidate peers, possibly empty (the caller then delegates the choice to
   *         Ratis via the no-target transfer).
   */
  static List<RaftPeer> selectStepDownTargets(final Collection<RaftPeer> livePeers, final RaftPeerId localPeerId,
      final ClusterMonitor clusterMonitor) {
    final String localId = localPeerId.toString();

    // Highest priority among the other peers. When it is 0 the cluster runs with the default,
    // homogeneous priorities and every follower is equally electable; only when some peer carries an
    // explicit positive priority do the priority-0 peers become non-electable witnesses to be skipped.
    int maxPriority = 0;
    for (final RaftPeer peer : livePeers)
      if (!peer.getId().toString().equals(localId))
        maxPriority = Math.max(maxPriority, peer.getPriority());

    final List<RaftPeer> candidates = new ArrayList<>();
    for (final RaftPeer peer : livePeers) {
      final String peerId = peer.getId().toString();
      if (peerId.equals(localId))
        continue;
      // Priority-0 witness/replica while real voters exist: Ratis would never keep it as leader.
      if (maxPriority > 0 && peer.getPriority() <= 0)
        continue;
      // Lagging follower: promoting it would prolong write unavailability while it catches up.
      if (clusterMonitor != null && clusterMonitor.isReplicaLagging(peerId))
        continue;
      candidates.add(peer);
    }

    // Strongest (highest priority) first; List.sort is stable so equal priorities keep iteration order.
    candidates.sort((a, b) -> Integer.compare(b.getPriority(), a.getPriority()));
    return candidates;
  }

  public void leaveCluster() {
    clusterManager.leaveCluster();
  }

  public void leaveCluster(final boolean force) {
    clusterManager.leaveCluster(force);
  }

  public void notifyApplied() {
    synchronized (applyNotifier) {
      applyNotifier.notifyAll();
    }
  }

  /**
   * Lenient overload (READ_YOUR_WRITES / bookmark wait): on timeout it logs and returns,
   * allowing the read to proceed against possibly-stale local state. Never used by the
   * LINEARIZABLE path - see {@link #waitForAppliedIndex(long, boolean)}.
   */
  public void waitForAppliedIndex(final long targetIndex) {
    waitForAppliedIndex(targetIndex, false);
  }

  /**
   * Blocks until the local state machine has applied up to {@code targetIndex}, or the
   * {@link #quorumTimeout} elapses.
   *
   * @param throwOnTimeout caller's consistency contract on timeout:
   *                       <ul>
   *                         <li>{@code true}  (LINEARIZABLE): a timeout means the local state
   *                             machine could not be proven up-to-date, so we MUST fail the read
   *                             (throw {@link ReplicationException} -> HTTP 503) rather than serve
   *                             a value older than an already-committed write.</li>
   *                         <li>{@code false} (READ_YOUR_WRITES / bookmark): a timeout degrades to
   *                             best-effort - log and return, letting the read proceed.</li>
   *                       </ul>
   * @throws ReplicationException if {@code throwOnTimeout} is {@code true} and the deadline is
   *                              reached before the local applied index catches up.
   */
  public void waitForAppliedIndex(final long targetIndex, final boolean throwOnTimeout) {
    if (targetIndex <= 0)
      return;
    try {
      final long deadline = System.currentTimeMillis() + quorumTimeout;
      synchronized (applyNotifier) {
        while (getLastAppliedIndex() < targetIndex) {
          final long remaining = deadline - System.currentTimeMillis();
          if (remaining <= 0) {
            if (throwOnTimeout) {
              LogManager.instance().log(this, Level.WARNING,
                  "LINEARIZABLE read failed: local apply timeout applied=%d < readIndex=%d after %dms (failing read)",
                  getLastAppliedIndex(), targetIndex, quorumTimeout);
              throw new ReplicationException(
                  "LINEARIZABLE read timed out waiting for local apply: applied=" + getLastAppliedIndex()
                      + " < readIndex=" + targetIndex);
            }
            LogManager.instance().log(this, Level.WARNING,
                "READ_YOUR_WRITES consistency timeout: applied=%d < target=%d (consistency degraded to EVENTUAL)",
                getLastAppliedIndex(), targetIndex);
            return;
          }
          applyNotifier.wait(remaining);
        }
      }
      HALog.log(this, HALog.TRACE, "Apply wait complete: applied >= target=%d", targetIndex);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      if (throwOnTimeout)
        throw new ReplicationException("LINEARIZABLE read interrupted while waiting for local apply", e);
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
    } catch (final Exception e) {
      // Ratis throws IllegalStateException ("stateMachineUpdater is uninitialized") while an in-place
      // restart re-initializes the division (issue #5271); report "unknown" instead of propagating.
      return -1;
    }
  }

  public long getCommitIndex() {
    if (raftServer == null)
      return -1;
    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getRaftLog().getLastCommittedIndex();
    } catch (final Exception e) {
      // See getLastAppliedIndex: degrade to "unknown" during an in-place restart (issue #5271).
      return -1;
    }
  }

  public long getCurrentTerm() {
    if (raftServer == null)
      return -1;
    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getInfo().getCurrentTerm();
    } catch (final Exception e) {
      // See getLastAppliedIndex: degrade to "unknown" during an in-place restart (issue #5271).
      return -1;
    }
  }

  /**
   * Sends a ReadIndex RPC to the Raft leader and returns the confirmed commit index.
   * The leader replies only after confirming its lease (or a majority heartbeat round)
   * AND after committing at least one entry in its own current term, guaranteeing that
   * the returned index covers every write that committed before this call - including
   * writes committed under a prior leader/term (Raft §6.4, the new-leader hazard handled
   * by Ratis {@code LeaderStateImpl.getReadIndex} / startup no-op entry).
   * <p>
   * IMPORTANT (issue: stale LINEARIZABLE follower read): in Apache Ratis 3.2.x a read-only
   * reply does NOT carry the read index in {@link RaftClientReply#getLogIndex()} - that
   * field is only populated for write replies and defaults to {@code 0} for reads
   * ({@code RaftServerImpl.processQueryFuture} builds the reply without {@code setLogIndex}).
   * Relying on {@code getLogIndex()} therefore yielded {@code 0}, which made
   * {@link #waitForAppliedIndex(long, boolean)} a no-op and let a follower serve arbitrarily
   * stale local state. Instead we read the leader's quorum-confirmed commit index out of the
   * reply's {@code CommitInfoProto} list (every reply carries the commit index of the server
   * that produced it, which for a leader-routed read is the leader itself). The reply is only
   * produced after the leader's internal ReadIndex protocol confirmed leadership, so this
   * commit index is a safe linearizable read index.
   *
   * @param expectSelfIsLeader if true and the reply indicates this node is not leader,
   *                           a {@link ReplicationException} is thrown with leader info
   */
  public long fetchReadIndex(final boolean expectSelfIsLeader) {
    try {
      final RaftClientReply reply = raftClient.io().sendReadOnly(Message.EMPTY);
      if (reply.isSuccess())
        return extractLeaderCommitIndex(reply);

      final NotLeaderException nle = reply.getNotLeaderException();
      if (nle != null) {
        final var suggestedLeader = nle.getSuggestedLeader();
        if (expectSelfIsLeader) {
          final String leaderAddr = suggestedLeader != null ? suggestedLeader.getId().toString() : null;
          throw new ReplicationException(
              "Lost leadership during ReadIndex" + (leaderAddr != null ? ", new leader: " + leaderAddr : ""));
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
   * Extracts the quorum-confirmed commit index of the server that produced {@code reply}
   * (for a leader-routed read-only request, that is the leader) from the reply's
   * {@code CommitInfoProto} list. See {@link #fetchReadIndex(boolean)} for why we cannot use
   * {@link RaftClientReply#getLogIndex()} on a read reply (it is always {@code 0} in Ratis 3.2.x).
   *
   * @throws ReplicationException if the reply does not carry a commit index for its own server,
   *                              which must NOT be silently treated as index 0 (that would
   *                              re-introduce the stale-read bug).
   */
  private long extractLeaderCommitIndex(final RaftClientReply reply) {
    final RaftPeerId replyServerId = reply.getServerId();
    if (replyServerId != null && reply.getCommitInfos() != null) {
      final var serverIdBytes = replyServerId.toByteString();
      for (final RaftProtos.CommitInfoProto info : reply.getCommitInfos()) {
        if (info.hasServer() && serverIdBytes.equals(info.getServer().getId()))
          return info.getCommitIndex();
      }
    }
    // The read succeeded but the leader's commit index is missing from the reply. We cannot
    // prove linearizability, so fail loudly rather than serve a possibly-stale read.
    throw new ReplicationException(
        "ReadIndex reply did not include the leader commit index; cannot guarantee linearizable read");
  }

  /**
   * Ensures linearizable read consistency for the leader. Sends a ReadIndex RPC to
   * confirm the leader lease, then waits for the local state machine to apply up to
   * the confirmed commit index.
   * <p>
   * Throws {@link ReplicationException} if leadership is lost before or after the RPC, or if
   * the local state machine cannot catch up to the read index before the quorum timeout.
   */
  public void ensureLinearizableRead() {
    final long readIndex = fetchReadIndex(true);
    if (!isLeader())
      throw new ReplicationException("Lost leadership after ReadIndex confirmation");
    // LINEARIZABLE: a timeout MUST throw (HTTP 503) - never silently degrade to a stale read.
    waitForAppliedIndex(readIndex, true);
  }

  /**
   * Ensures linearizable read consistency for a follower. Contacts the leader via
   * ReadIndex RPC to obtain the current commit index, then waits for the local state
   * machine to apply up to that index before allowing the read to proceed.
   * <p>
   * Throws {@link ReplicationException} if the read index cannot be obtained, or if the local
   * state machine cannot catch up to it before the quorum timeout (never serves stale state).
   */
  public void ensureLinearizableFollowerRead() {
    final long readIndex = fetchReadIndex(false);
    // LINEARIZABLE: a timeout MUST throw (HTTP 503) - never silently degrade to a stale read.
    waitForAppliedIndex(readIndex, true);
  }

  /**
   * Aggregates {@link #getFollowerStates()} into a single replication-health snapshot for metrics
   * (issue #4809 follow-up). The {@code maxFollowerLastContactMs} field is the leading indicator of
   * election churn: it is the worst time since the leader last reached any follower, and when it
   * approaches {@code arcadedb.ha.electionTimeoutMin} an election is imminent. Returns a not-leader
   * placeholder ({@code -1}/{@code 0}) when this node is not the leader.
   */
  public HAReplicationStatsProvider.HAReplicationStats getReplicationStats() {
    if (raftServer == null || !isLeader())
      return new HAReplicationStatsProvider.HAReplicationStats(false, -1, -1, 0);

    final List<Map<String, Object>> followers = getFollowerStates();
    if (followers.isEmpty())
      return new HAReplicationStatsProvider.HAReplicationStats(true, -1, -1, 0);

    final long commitIndex = getCommitIndex();
    long maxContactMs = -1;
    long maxLag = -1;
    for (final Map<String, Object> follower : followers) {
      final Object elapsed = follower.get("lastRpcElapsedMs");
      if (elapsed instanceof Number n)
        maxContactMs = Math.max(maxContactMs, n.longValue());
      final Object matchIndex = follower.get("matchIndex");
      if (commitIndex >= 0 && matchIndex instanceof Number n)
        maxLag = Math.max(maxLag, Math.max(0L, commitIndex - n.longValue()));
    }
    return new HAReplicationStatsProvider.HAReplicationStats(true, maxContactMs, maxLag, followers.size());
  }

  /**
   * Per-follower health samples for metrics, the cluster JSON, and the lagging-follower alert (issue
   * #4812). Combines the leader's Ratis follower indices ({@link #getFollowerStates()}) with the
   * {@link ClusterMonitor}'s classification and sustained-lag duration. Empty when not the leader.
   */
  public List<HAReplicationStatsProvider.FollowerSample> getFollowerSamples() {
    if (raftServer == null || !isLeader())
      return List.of();

    final List<Map<String, Object>> followers = getFollowerStates();
    if (followers.isEmpty())
      return List.of();

    final long commitIndex = getCommitIndex();
    final List<HAReplicationStatsProvider.FollowerSample> samples = new ArrayList<>(followers.size());
    for (final Map<String, Object> follower : followers) {
      final String peerId = (String) follower.get("peerId");
      final long matchIndex = follower.get("matchIndex") instanceof Number n ? n.longValue() : -1;
      final long nextIndex = follower.get("nextIndex") instanceof Number n ? n.longValue() : -1;
      final long lastContactMs = follower.get("lastRpcElapsedMs") instanceof Number n ? n.longValue() : -1;
      final long lag = commitIndex >= 0 && matchIndex >= 0 ? Math.max(0L, commitIndex - matchIndex) : -1;
      final String status = clusterMonitor != null ? clusterMonitor.getReplicaStatus(peerId).name() : "UNKNOWN";
      final long laggingForMs = clusterMonitor != null ? clusterMonitor.getReplicaLaggingForMs(peerId) : 0;
      samples.add(new HAReplicationStatsProvider.FollowerSample(
          peerId, matchIndex, nextIndex, lag, lastContactMs, status, laggingForMs));
    }
    return samples;
  }

  /**
   * Leader-&gt;follower replication round-trip latency, in milliseconds, keyed by follower peer id.
   * Unlike {@code lastRpcElapsedMs} (age of the last RPC, which on an idle cluster just tracks the
   * heartbeat cadence - issue #5314), this is the actual measured RTT of the {@code appendEntries}
   * RPC: the full replication path (network + gRPC + TLS + serialization). Because heartbeats
   * <em>are</em> empty {@code appendEntries} RPCs, the figure stays meaningful even with no write
   * traffic, so no load is required to observe it.
   * <p>
   * The data is read from Ratis' gRPC log-appender latency timers, which the leader already maintains
   * per follower: {@code ratis_grpc.log_appender.{leaderMemberId}.{follower}_latency} (appendEntries
   * carrying entries) and {@code ...{follower}_heartbeat_latency} (empty heartbeats). Both measure the
   * same round trip; we report their count-weighted mean, so the value naturally reflects heartbeat RTT
   * while idle and shifts to the real append RTT under write load.
   * <p>
   * Best-effort observability: reaching into Ratis' internal Dropwizard registry is isolated here and
   * never allowed to disturb the cluster - any failure (not leader, registry not yet created, Ratis
   * internals changed) yields an empty map rather than an exception. Returns an empty map when this
   * node is not the leader.
   */
  public Map<String, ReplicationLatency> getReplicationLatencies() {
    final Map<String, ReplicationLatency> result = new HashMap<>();
    if (raftServer == null || !isLeader())
      return result;

    try {
      final RaftServer.Division division = raftServer.getDivision(raftGroup.getGroupId());
      final String localMemberId = division.getMemberId().toString();

      final List<Map<String, Object>> followers = getFollowerStates();
      if (followers.isEmpty())
        return result;

      // Locate THIS leader's gRPC log-appender registry. Filtering by member id is required because a
      // single JVM may host several servers in tests, each with its own registry keyed by member id.
      MetricRegistry dropWizard = null;
      for (final RatisMetricRegistry registry : MetricRegistries.global().getMetricRegistries()) {
        final MetricRegistryInfo info = registry.getMetricRegistryInfo();
        if ("ratis_grpc".equals(info.getApplicationName())
            && "log_appender".equals(info.getMetricsComponentName())
            && localMemberId.equals(info.getPrefix())
            && registry instanceof RatisMetricRegistryImpl impl) {
          dropWizard = impl.getDropWizardMetricRegistry();
          break;
        }
      }
      if (dropWizard == null)
        return result;

      final SortedMap<String, Timer> timers = dropWizard.getTimers();
      for (final Map<String, Object> follower : followers) {
        final String peerId = (String) follower.get("peerId");
        if (peerId == null)
          continue;
        final ReplicationLatency latency = meanReplicationLatency(timers, peerId);
        if (latency != null)
          result.put(peerId, latency);
      }
    } catch (final Exception e) {
      // Never let metric scraping affect replication: fall through to whatever was collected so far.
      LogManager.instance().log(this, Level.FINE, "Unable to read Ratis replication latency metrics", e);
    }
    return result;
  }

  /**
   * Aggregates a follower's appendEntries and heartbeat latency timers into a single count-weighted
   * RTT. The snapshot mean is nanoseconds; we convert to milliseconds. Returns {@code null} when
   * neither timer has recorded a single RPC yet (nothing meaningful to show).
   */
  private static ReplicationLatency meanReplicationLatency(final SortedMap<String, Timer> timers, final String follower) {
    // Names are fully qualified as ...{memberId}.{follower}_latency / ..._heartbeat_latency. The leading
    // '.' guard prevents a follower "n1" from matching another follower "xn1".
    final String appendSuffix = "." + follower + "_latency";
    final String heartbeatSuffix = "." + follower + "_heartbeat_latency";

    double weightedSumNanos = 0d;
    double weightedP99Nanos = 0d;
    double maxNanos = 0d;
    long totalCount = 0L;
    for (final Map.Entry<String, Timer> entry : timers.entrySet()) {
      final String name = entry.getKey();
      if (!name.endsWith(appendSuffix) && !name.endsWith(heartbeatSuffix))
        continue;
      final Timer timer = entry.getValue();
      final long count = timer.getCount();
      if (count <= 0)
        continue;
      final var snapshot = timer.getSnapshot();
      weightedSumNanos += snapshot.getMean() * count;
      weightedP99Nanos += snapshot.get99thPercentile() * count;
      maxNanos = Math.max(maxNanos, snapshot.getMax());
      totalCount += count;
    }
    if (totalCount == 0L)
      return null;

    final double toMs = 1_000_000d;
    return new ReplicationLatency(weightedSumNanos / totalCount / toMs, weightedP99Nanos / totalCount / toMs, maxNanos / toMs);
  }

  /**
   * Measured leader-&gt;follower replication round-trip latency in milliseconds (issue #5314): mean and
   * 99th percentile are count-weighted across the follower's appendEntries and heartbeat timers; max is
   * the worst single sample observed. All values load-independent (heartbeats keep the timers fed).
   */
  public record ReplicationLatency(double meanMs, double p99Ms, double maxMs) {
  }

  /**
   * Returns one map per follower with its {@code peerId}, {@code matchIndex}, {@code nextIndex} and
   * {@code lastRpcElapsedMs}, as seen by this leader.
   * <p>
   * The three Ratis APIs we read - the follower peer-id list ({@code RoleInfoProto.LeaderInfo}), the
   * match-index array and the next-index array - are each built from an independent snapshot of the
   * leader's internal {@code CopyOnWriteArrayList} of log appenders. They are positionally aligned
   * only while cluster membership is stable. A membership change (adding, removing or replacing a
   * peer) between the calls can shift array positions, so zipping them by index would attribute a
   * follower's match/next index to the wrong peer id (issue #4842). The old {@code min(...)} guard
   * protected length, not order.
   * <p>
   * We use a seqlock-style read: capture the peer-id ordering before and after reading the two index
   * arrays and only trust the positional correlation when the ordering is unchanged and the array
   * lengths line up. On divergence we retry a bounded number of times, then fall back to reporting
   * the peers with unknown (omitted) indices rather than misattributing them.
   */
  public List<Map<String, Object>> getFollowerStates() {
    if (raftServer == null || !isLeader())
      return List.of();
    try {
      final var division = raftServer.getDivision(raftGroup.getGroupId());
      final var info = division.getInfo();

      for (int attempt = 0; attempt < FOLLOWER_STATES_MAX_ATTEMPTS; attempt++) {
        final List<RaftProtos.ServerRpcProto> before = leaderFollowerInfos(info);
        if (before.isEmpty())
          return List.of();

        final long[] matchIndices = info.getFollowerMatchIndices();
        final long[] nextIndices = info.getFollowerNextIndices();

        final List<RaftProtos.ServerRpcProto> after = leaderFollowerInfos(info);

        final List<Map<String, Object>> correlated = correlateFollowerStates(before, matchIndices, nextIndices, after);
        if (correlated != null)
          return correlated;
        // Membership changed while we read the index arrays - retry with a fresh, consistent snapshot.
      }

      // Membership kept changing under us across every attempt: report the peers we currently know,
      // without indices, rather than risk attributing a match/next index to the wrong peer.
      return degradedFollowerStates(leaderFollowerInfos(info));
    } catch (final IOException e) {
      return List.of();
    }
  }

  /** Number of seqlock retries in {@link #getFollowerStates()} before degrading to index-less peers. */
  private static final int FOLLOWER_STATES_MAX_ATTEMPTS = 3;

  /** Returns the leader's follower peer-info list, or an empty list when this node is not a ready leader. */
  private static List<RaftProtos.ServerRpcProto> leaderFollowerInfos(final DivisionInfo info) {
    final RaftProtos.RoleInfoProto roleInfo = info.getRoleInfoProto();
    return roleInfo.hasLeaderInfo() ? roleInfo.getLeaderInfo().getFollowerInfoList() : List.of();
  }

  /**
   * Correlates the {@code before} follower peer-id snapshot with the match/next index arrays, validating
   * against the {@code after} snapshot. Returns the per-follower state maps when the snapshots are
   * consistent (same peer ordering and matching array lengths), or {@code null} when a membership change
   * raced the reads and positional correlation cannot be trusted.
   */
  static List<Map<String, Object>> correlateFollowerStates(final List<RaftProtos.ServerRpcProto> before,
      final long[] matchIndices, final long[] nextIndices, final List<RaftProtos.ServerRpcProto> after) {
    if (!samePeerOrder(before, after) || matchIndices.length != before.size() || nextIndices.length != before.size())
      return null;

    final List<Map<String, Object>> result = new ArrayList<>(before.size());
    for (int i = 0; i < before.size(); i++) {
      final Map<String, Object> state = new LinkedHashMap<>();
      state.put("peerId", before.get(i).getId().getId().toStringUtf8());
      state.put("matchIndex", matchIndices[i]);
      state.put("nextIndex", nextIndices[i]);
      state.put("lastRpcElapsedMs", before.get(i).getLastRpcElapsedTimeMs());
      result.add(state);
    }
    return result;
  }

  /**
   * Builds index-less follower states (peer id and last-RPC elapsed only) for the case where membership
   * churned faster than {@link #getFollowerStates()} could take a consistent snapshot. The match/next
   * index keys are intentionally omitted so downstream consumers treat the lag as unknown instead of
   * reading a misattributed value.
   */
  static List<Map<String, Object>> degradedFollowerStates(final List<RaftProtos.ServerRpcProto> followerInfos) {
    final List<Map<String, Object>> result = new ArrayList<>(followerInfos.size());
    for (final RaftProtos.ServerRpcProto follower : followerInfos) {
      final Map<String, Object> state = new LinkedHashMap<>();
      state.put("peerId", follower.getId().getId().toStringUtf8());
      state.put("lastRpcElapsedMs", follower.getLastRpcElapsedTimeMs());
      result.add(state);
    }
    return result;
  }

  /** Returns true when both snapshots list the same follower peer ids in the same order. */
  private static boolean samePeerOrder(final List<RaftProtos.ServerRpcProto> a, final List<RaftProtos.ServerRpcProto> b) {
    if (a.size() != b.size())
      return false;
    for (int i = 0; i < a.size(); i++) {
      if (!a.get(i).getId().getId().equals(b.get(i).getId().getId()))
        return false;
    }
    return true;
  }

  /**
   * Prints an ASCII table showing the current cluster configuration.
   * Called on leader changes so the operator can see the cluster state at a glance.
   */
  public void printClusterConfiguration() {
    statusExporter.printClusterConfiguration();
  }

  /**
   * Returns the formatted ASCII cluster configuration table, or {@code null} if not available
   * (e.g. no peers known). Useful for tests and diagnostics. Note: this builds the table
   * unconditionally (even on followers, where lag/rtt/last-contact columns will be empty).
   */
  public String getClusterConfigurationTable() {
    return statusExporter.buildClusterConfigurationTable();
  }

  boolean hasExistingRaftStorage() {
    final File storageDir = getRaftStorageDir();
    if (!storageDir.exists())
      return false;
    final File[] subdirs = storageDir.listFiles(f -> f.isDirectory() && !"lost+found".equals(f.getName()));
    return subdirs != null && subdirs.length > 0;
  }

  private File getRaftStorageDir() {
    final File dir = resolveRaftStorageDir(
        configuration.getValueAsString(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY),
        configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY),
        arcadeServer.getRootPath(), localPeerId.toString());
    warnIfLegacyRaftStorage(dir);
    return dir;
  }

  /**
   * Resolves the on-disk Raft storage directory for a peer. Kept as a package-visible static so tests
   * and the server agree on the default location (issue #5272).
   * <p>
   * Precedence:
   * <ol>
   *   <li>an explicit {@code arcadedb.ha.raftStorageDirectory} is always honored - absolute decoupling,
   *       required for read-only-root-filesystem deployments;</li>
   *   <li>otherwise the default is {@code <databaseDirectory>/.raft-storage/raft-storage-<peerId>} so
   *       that persisting the database directory (which every durable deployment already does) also
   *       persists the Raft log. The pre-#5272 default was the server root path, a <i>sibling</i> of the
   *       database directory, which on Kubernetes lands on ephemeral disk while only {@code databases/}
   *       is on the PersistentVolume - so a pod recreation silently discarded all Raft state;</li>
   *   <li>backward compatibility: if the new default does not yet exist but a legacy
   *       {@code <serverRootPath>/raft-storage-<peerId>} directory does, the legacy location is reused, so
   *       an in-place upgrade of a deployment that persisted the server root path does not FORMAT a fresh
   *       empty log and self-inflict the very divergence this fix prevents. The caller emits a one-time
   *       WARNING recommending migration or an explicit {@code raftStorageDirectory}.</li>
   * </ol>
   */
  static File resolveRaftStorageDir(final String configuredRaftDir, final String databaseDirectory,
      final String serverRootPath, final String peerId) {
    final String subdir = "raft-storage-" + peerId;

    if (configuredRaftDir != null && !configuredRaftDir.isBlank())
      return new File(configuredRaftDir, subdir);

    final File newDefault = databaseDirectory != null && !databaseDirectory.isBlank()
        ? new File(new File(databaseDirectory, ".raft-storage"), subdir)
        : new File(serverRootPath, subdir);

    if (!newDefault.exists()) {
      final File legacy = new File(serverRootPath, subdir);
      if (legacy.exists() && !legacy.equals(newDefault))
        return legacy;
    }
    return newDefault;
  }

  /**
   * Emits a one-time WARNING when the resolved Raft storage directory is the legacy pre-#5272 location
   * (directly under the server root path) rather than the new default under the database directory.
   */
  private void warnIfLegacyRaftStorage(final File resolvedDir) {
    if (legacyRaftStorageWarningLogged)
      return;
    if (configuration.getValueAsString(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY) != null
        && !configuration.getValueAsString(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY).isBlank())
      return;
    final File rootPath = new File(arcadeServer.getRootPath());
    final File parent = resolvedDir.getParentFile();
    if (parent != null && parent.equals(rootPath)) {
      legacyRaftStorageWarningLogged = true;
      LogManager.instance().log(this, Level.WARNING,
          "Raft storage is using the legacy location '%s' (under the server root path). Set '%s' explicitly "
              + "or move it under the database directory ('%s/.raft-storage') so Raft state is persisted together "
              + "with the databases (issue #5272).",
          resolvedDir.getAbsolutePath(), GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY.getKey(),
          configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY));
    }
  }

  /**
   * Decides whether the Raft storage directory must be preserved across server restarts instead of
   * being wiped and re-FORMATted on every {@link #start()} (issue #4835).
   * <p>
   * {@link GlobalConfiguration#HA_RAFT_PERSIST_STORAGE} defaults to {@code true} (durable): wiping the
   * Raft log on restart turns a follower that was merely lagging into a permanently diverged node on a
   * full-cluster cold restart, and on a single-seed cluster it can silently re-form a fresh empty
   * single-node cluster (data loss / split brain). Persisting was previously the default only under
   * Kubernetes (where a PersistentVolume made it essential - issue #4835); it is now the default
   * everywhere, so the Kubernetes special case has collapsed into the global default.
   * <p>
   * An explicit {@code raftPersistStorage=false} is always honored for a throwaway/test cluster that
   * really wants ephemeral storage. A JVM system property is read directly because a
   * {@link ContextConfiguration} built after {@link GlobalConfiguration} was initialised does not
   * necessarily reflect a late {@code System.setProperty}.
   */
  static boolean resolvePersistStorage(final ContextConfiguration configuration) {
    if (isExplicitlyConfigured(configuration, GlobalConfiguration.HA_RAFT_PERSIST_STORAGE)) {
      final String sysProp = System.getProperty(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE.getKey());
      return sysProp != null
          ? Boolean.parseBoolean(sysProp)
          : configuration.getValueAsBoolean(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE);
    }
    return GlobalConfiguration.HA_RAFT_PERSIST_STORAGE.getValueAsBoolean();
  }

  /**
   * Returns {@code true} when the operator explicitly provided a value for the given configuration
   * key, either through this {@link ContextConfiguration} (config file, programmatic set, or server
   * settings) or via a JVM system property. Used by {@link #resolvePersistStorage} to distinguish a
   * deliberate {@code raftPersistStorage=false} from the implicit default.
   */
  static boolean isExplicitlyConfigured(final ContextConfiguration configuration, final GlobalConfiguration key) {
    return configuration.hasValue(key.getKey()) || System.getProperty(key.getKey()) != null;
  }

  /**
   * Binds {@link RaftLogCompactionScheduler} to this server's Ratis instance and Raft storage volume
   * (issue #5345). Kept as an inner class so the scheduler itself stays free of Ratis and filesystem
   * details and can be unit-tested against a fake.
   */
  private final class RaftLogCompactionTarget implements RaftLogCompactionScheduler.CompactionTarget {

    @Override
    public boolean isShutdownRequested() {
      return shutdownRequested;
    }

    @Override
    public long triggerSnapshot(final long creationGap) {
      return takeLocalSnapshot(creationGap);
    }

    @Override
    public long getRaftStorageUsableSpaceBytes() {
      final File dir = raftStorageVolume();
      return dir != null ? dir.getUsableSpace() : -1L;
    }

    @Override
    public long getRaftStorageTotalSpaceBytes() {
      final File dir = raftStorageVolume();
      return dir != null ? dir.getTotalSpace() : -1L;
    }

    @Override
    public String getRaftStorageDescription() {
      final File dir = raftStorageVolume();
      return dir != null ? dir.getAbsolutePath() : "<unknown>";
    }

    /**
     * The Raft storage directory, or its nearest existing ancestor. {@code File.getUsableSpace()}
     * returns 0 for a path that does not exist, which would otherwise read as "disk full" during the
     * window between server start and Ratis creating the directory.
     * <p>
     * The configured directory itself is resolved once and cached: {@code resolveRaftStorageDir} is
     * config-derived and constant for the server's lifetime, and re-running its {@code exists()} probes
     * on every tick is pointless I/O. The ancestor walk stays per-call because its result legitimately
     * changes once Ratis creates the directory.
     */
    private File raftStorageVolume() {
      File dir = cachedRaftStorageDir;
      if (dir == null) {
        dir = getRaftStorageDir();
        cachedRaftStorageDir = dir;
      }
      while (dir != null && !dir.exists())
        dir = dir.getParentFile();
      return dir;
    }
  }

  /**
   * Asks the local Ratis server to create a snapshot, which is what authorises it to purge log
   * segments up to the new snapshot index ({@code arcadedb.ha.logPurgeUptoSnapshot}). This is a
   * node-local admin operation - it is valid on the leader and on followers alike, and does not
   * replicate.
   * <p>
   * Ratis completes the request as a no-op when fewer than {@code creationGap} entries have been
   * applied since the last snapshot, and rejects it while a snapshot install is in progress.
   *
   * @return the resulting snapshot index, or -1 when no snapshot was taken
   */
  long takeLocalSnapshot(final long creationGap) {
    final RaftServer server = raftServer;
    if (server == null || shutdownRequested)
      return -1L;
    if (server.getLifeCycleState() != LifeCycle.State.RUNNING)
      return -1L;

    try {
      final RaftClientReply reply = server.snapshotManagement(
          SnapshotManagementRequest.newCreate(snapshotClientId, localPeerId, raftGroup.getGroupId(),
              snapshotCallId.incrementAndGet(), SNAPSHOT_REQUEST_TIMEOUT_MS, creationGap));
      if (reply == null || !reply.isSuccess()) {
        LogManager.instance().log(this, Level.FINE, "Local Raft snapshot request was not successful: %s", reply);
        return -1L;
      }
      return reply.getLogIndex();
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not take a local Raft snapshot to compact the log: %s", e.getMessage());
      return -1L;
    }
  }

  /**
   * Starts a periodic task that updates the {@link ClusterMonitor} with the leader's commit index.
   * Called when this node becomes the Raft leader.
   */
  void startLagMonitor() {
    if (lagMonitorExecutor != null)
      return;
    // Fresh leadership term: discard any per-replica lag state captured while this node led a previous
    // term. Comparing the new term's (Ratis-reset) matchIndex against that stale baseline would
    // mis-classify a healthy follower as STALLED on the first tick after the election (issue #4841).
    // The executor==null guard above ensures this only fires on a genuine (re)acquisition, never on a
    // same-term re-notification while the monitor is already running.
    clusterMonitor.reset();
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
  private Parameters buildParameters(final ContextConfiguration configuration) {
    this.allowlistFilter = null;
    final Parameters parameters = new Parameters();
    if (!configuration.getValueAsBoolean(GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED))
      return parameters;

    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    final long refreshMs = configuration.getValueAsLong(GlobalConfiguration.HA_GRPC_ALLOWLIST_REFRESH_MS);
    final long startupGraceMs = configuration.getValueAsLong(GlobalConfiguration.HA_PEER_ALLOWLIST_STARTUP_GRACE_MS);
    final long stickyTtlMs = configuration.getValueAsLong(GlobalConfiguration.HA_PEER_ALLOWLIST_STICKY_TTL_MS);
    final List<String> peerHosts = PeerAddressAllowlistFilter.extractPeerHosts(serverList);
    if (peerHosts.isEmpty()) {
      LogManager.instance().log(this, Level.WARNING,
          "arcadedb.ha.peerAllowlist.enabled=true but arcadedb.ha.serverList is empty; allowlist not installed");
      return parameters;
    }
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(peerHosts, refreshMs, startupGraceMs,
        stickyTtlMs);
    this.allowlistFilter = filter;
    GrpcConfigKeys.Server.setServicesCustomizer(parameters, new RaftGrpcServicesCustomizer(filter));
    return parameters;
  }

  /**
   * Proactively reconciles the inbound Raft gRPC peer allowlist with current DNS (issue #4696).
   * Invoked from the health monitor tick on every node so a peer that restarted with a new pod IP is
   * admitted without first having to be rejected on an inbound connection - which a leader with a
   * wedged outbound appender channel may never receive. No-op when the allowlist is disabled. The
   * filter throttles the actual DNS re-resolution to its configured refresh interval.
   */
  @Override
  public void refreshPeerAllowlist() {
    final PeerAddressAllowlistFilter filter = allowlistFilter;
    if (filter != null)
      filter.proactiveRefresh();
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
