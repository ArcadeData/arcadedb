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

import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.PeerRequest;
import com.arcadedb.server.ha.raft.ratis.ApplyBackPressure;
import com.arcadedb.server.ha.raft.ratis.RatisMisdirectedRequestFilter;
import com.arcadedb.server.ha.raft.ratis.RatisRefusedEntryErrorFilter;
import com.arcadedb.server.ha.raft.ratis.RatisSnapshotDigestWarningFilter;
import com.arcadedb.server.http.handler.LeaderDial;
import com.arcadedb.server.monitor.CountingRejectionPolicy;
import com.arcadedb.server.monitor.HAReplicationStatsProvider;
import com.arcadedb.server.monitor.PoolMetrics;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.DedicatedThreadPool.PoolStats;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.impl.RatisMetricRegistryImpl;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.RaftServerRpcWithProxy;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLogBase;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.codahale.metrics.MetricRegistry;
import org.apache.ratis.thirdparty.com.codahale.metrics.Timer;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.http.HttpClient;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
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
 * to {@code 0.0.0.0}, any host that can reach the Raft port can connect to it and inject Raft log
 * entries. Three mitigations, in decreasing order of strength:
 * <ul>
 *   <li>{@code arcadedb.ha.tls.*} (issue #3890) - mutual TLS, the only one that binds peer identity to a
 *       certificate rather than to a spoofable source address, and the only one that also encrypts the
 *       traffic. Off by default; this is the supported way to secure the Raft port in production.</li>
 *   <li>{@code arcadedb.ha.peerAllowlist.enabled} - rejects inbound connections whose address does not
 *       resolve to a host in {@code HA_SERVER_LIST}. On by default, but IP-based: defeated by spoofing on a
 *       flat L2 network or by a compromised peer. A best-effort default, not a substitute for mTLS.</li>
 *   <li>A Kubernetes NetworkPolicy restricting the port to the StatefulSet pods - network-level isolation,
 *       and worth having alongside either of the above.</li>
 * </ul>
 * None of these replaces the {@code X-ArcadeDB-Cluster-Token} check on the HTTP side channels (snapshot
 * download, database verify), which is a separate control on a separate port.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RaftHAServer implements HealthMonitor.HealthTarget {

  // The forwarded user the leader presents (with the cluster token) for inter-node calls such as the
  // stalled-replica resync. ArcadeDB's bootstrap 'root' user is the cluster-wide superuser; named here
  // so the coupling is visible rather than scattered as a string literal.
  // Package-private so other cluster-internal RPC callers (e.g. LeaderDatabaseQuery) reuse the same constant
  // instead of re-inlining the "root" literal.
  static final String FORWARDED_ROOT_USER = PeerRequest.FORWARDED_ROOT_USER;

  // Timeout carried by the node-local snapshot-management requests the log compaction scheduler issues.
  // Generous: the request is served on the state-machine updater thread, which may be busy applying a
  // backlog, and a timeout here only skips one compaction tick.
  private static final long SNAPSHOT_REQUEST_TIMEOUT_MS = 30_000L;

  final ArcadeDBServer          arcadeServer;
  final ContextConfiguration    configuration;
  // One client for every database this node replicates, not one per database (review finding on PR #7650):
  // RaftHAPlugin's server.setDatabaseWrapper callback builds a new RaftReplicatedDatabase per database, and
  // each java.net.http.HttpClient owns its own default executor/selector thread when built without one - an
  // O(databases-per-node) cost for what LeaderDial.newConnectTimeoutBoundedClient's other two call sites
  // (PostBatchHandler, LeaderCommandForwarder.Transport) already keep at one-per-server.
  private final    HttpClient              forwardHttpClient;
  volatile ArcadeStateMachine      stateMachine;
  /**
   * Owned here, not by the state machine, so an in-place Ratis restart - which builds a new state machine - does
   * not forget that this node joined at runtime (issue #7819). Persisted in {@link RaftStorageFiles#runtimeJoinMarkerFile}, so a
   * process restart does not forget it either once the entry that added this node has been compacted away
   * (issue #8329). Assigned in the constructor before the first state machine is built and handed this instance.
   */
  private final    RuntimeJoinDetector     runtimeJoinDetector;
  /**
   * The drops this node's own verb is waiting on (issue #8035). Held here rather than by the state machine so a
   * Ratis restart that rebuilds the state machine mid-drop does not lose the registration.
   */
  private final    LocalDropVerbs          localDropVerbs          = new LocalDropVerbs();
  final ClusterMonitor          clusterMonitor;
  /**
   * How recently a follower must have answered this leader to be handed leadership (issue #8556); see
   * {@link #handoffReachablePeers()}.
   */
  private final    long                    handoffContactWindowMs;
  final Quorum                  quorum;
  /**
   * The RPC timeout of this node's own Raft client: a request unanswered for this long is retried with the same call
   * id. {@link PageVersionLedger#STALE_RESERVATION_MS} is derived from it, since a retry refreshes the reservation.
   */
  public static final long CLIENT_REQUEST_TIMEOUT_MS = 10_000L;

  final long                    quorumTimeout;
  final RaftGroup               raftGroup;
  final RaftPeerId              localPeerId;
  // Concurrent, not a plain HashMap: this is the one address map that is structurally modified while the
  // cluster runs - RaftClusterManager puts on a peer add and removes on a peer leave - and it is read
  // without any lock by the lag monitor, by cluster reporting, and (since issue #6191) by every write a
  // follower forwards. A HashMap resizing under a concurrent get() is how a reader sees a stale or missing
  // address, or spins. The siblings below are populated in the constructor and never written again, so a
  // final field publishes them safely.
  final Map<RaftPeerId, String> httpAddresses      = new ConcurrentHashMap<>();
  // Explicit HTTPS endpoints (optional 5th field in HA_SERVER_LIST). Used for encrypted
  // peer-to-peer transfers (snapshot download) when SSL is enabled.
  // ConcurrentHashMap like its HTTP sibling, not because anything mutates it today - it is filled once in the
  // constructor - but because it is READ from other threads (the verify endpoint's peer-query pool since issue
  // #6221, the resync executors before it), and the day an addPeer overload starts declaring an https port the
  // map type must not be the thing that has to be remembered.
  final Map<RaftPeerId, String> httpsAddresses     = new ConcurrentHashMap<>();
  // The HTTP addresses THIS node's own HA_SERVER_LIST declared, as parsed at construction and never written again.
  // httpAddresses starts from the same entries but is rewritten as peers join and leave, so it cannot say which of its
  // entries the operator configured here; recordAdmittedPeerHttpAddress needs exactly that, to never let another node's
  // declaration replace this node's own (issue #8689).
  final Map<RaftPeerId, String> serverListHttpAddresses;
  // Client-reachable Bolt endpoints (optional object-form 'bolt' field in HA_SERVER_LIST). Advertised
  // in the Bolt ROUTE routing table so neo4j:// drivers can discover leader/followers.
  final Map<RaftPeerId, String> boltAddresses      = new HashMap<>();
  // Client-reachable gRPC endpoints (optional object-form 'grpc' field in HA_SERVER_LIST). Advertised in the
  // gRPC routing table so a caller refused on a follower is told an address it can actually dial (issue #6091).
  final Map<RaftPeerId, String> grpcAddresses      = new HashMap<>();
  final Map<RaftPeerId, String> peerDisplayNames   = new ConcurrentHashMap<>();
  // The server list as configured, in order, and the names the operator gave its entries: what
  // resolvePeerIdByServerName() applies the local-peer resolution rules to (issue #7424).
  private final    List<RaftPeer>          configuredPeerList;
  private final    Map<RaftPeerId, String> configuredPeerNames;
  private final    String                  clusterName;

  // volatile: reassigned by the recovery path (restartRatis) and cleared by stop(), while background
  // threads - the health monitor and, since #5345, the log compaction scheduler - read it. Every reader
  // still copies it to a local before use so a concurrent reassignment cannot null it mid-method.
  volatile RaftServer                raftServer;
  // Issue #8901: the division's position right after the latest in-place restartRatis() (the reformat included),
  // PENDING_BASELINE from before the new server starts until it can be read, null when never restarted in place or once
  // the path was proven. Evaluations only ever compare-and-set.
  final AtomicReference<InPlaceRestartBaseline> inPlaceRestartBaseline = new AtomicReference<>();
  RaftClient                raftClient;
  volatile RaftProperties            raftProperties;
  volatile RaftTransactionBroker     transactionBroker;
  private          RaftClusterStatusExporter statusExporter;
  private          ScheduledExecutorService  lagMonitorExecutor;
  final PeerCapabilityRegistry    peerCapabilities = new PeerCapabilityRegistry();
  private volatile ApplyBackPressure         applyBackPressure;
  // What THIS node tells its peers it can decode. A field rather than PeerCapabilities.LOCAL read directly, so an
  // integration test can stand a node up that behaves like a build predating a section - which is the only way to
  // exercise a mixed-version cluster inside one JVM.
  private volatile Set<String>               advertisedCapabilities = PeerCapabilities.LOCAL;
  // This node's HTTPS client for the capability probe, rebuilt only when its truststore changes (issue #7301).
  // Per server and not static: several ArcadeDBServer instances share a JVM in every HA test, and a shared cache
  // would rebuild on each probe and could close a client another server was still sending on.
  final TrustedHttpClientCache    capabilityHttpsClients = new TrustedHttpClientCache();
  // Issue #7619: the commit index the LEADER last reported for itself, as seen from this follower, or -1 when none
  // has been learned yet. Written only by the health-monitor thread (refreshLeaderCommitIndex), read by the
  // readiness probe. Every value it ever holds is a commit index some leader actually reported, and a committed
  // index is never un-committed, so it is at most the group's true commit index at the moment it is read: a stale
  // value can only make the readiness gate below LESS strict, never report a caught-up follower as behind.
  volatile long                      leaderReportedCommitIndex = -1L;
  // How the leader's commit index is asked for (see LeaderCommitProber). A field so a unit test can drive
  // isReadyForTraffic() through a leader that reports a commit index this follower never received.
  private volatile LeaderCommitProber        leaderCommitProber = this::queryLeaderCommitIndex;
  // The Ratis client the prober above talks to the leader through, built lazily for leaderCommitProbeTarget and
  // rebuilt when the leader changes or a call fails (a failed call may mean a stale DNS resolution of a re-IPed
  // leader, which only a fresh channel re-resolves). Lock-free on purpose (review of PR #8322): stop() must never
  // wait behind a probe that is blocked in its RPC. It raises leaderCommitProbeClosed and takes the client out of
  // the reference; closing it fails the in-flight call at once. The health thread, the only builder, re-checks the
  // flag after publishing a new client, so whichever of the two runs second closes it.
  private final    AtomicReference<RaftClient> leaderCommitProbeClient = new AtomicReference<>();
  private          RaftPeer                  leaderCommitProbeTarget;
  private volatile boolean                   leaderCommitProbeClosed;
  // Failure backoff of the probe above, so an unreachable leader does not stretch every health tick by the probe
  // timeout for as long as it stays unreachable (review of PR #8322). Health-monitor thread only.
  private          int                       leaderCommitProbeFailures;
  private          int                       leaderCommitProbeSkipTicks;
  private          RaftPeerId                leaderCommitProbeLastLeader;
  // Issue #8342: this follower's own view of a zero-progress stall behind its leader, measured against
  // leaderReportedCommitIndex above. Written by the health-monitor thread (trackFollowerStall), read by
  // GET /api/v1/cluster. The clock is a field so a unit test can move time without sleeping.
  // followerStallLeader deliberately duplicates leaderCommitProbeLastLeader: the probe skips ticks while it backs
  // off, so the two hooks can see a leader change on different ticks, and each must reset on the tick IT sees it.
  // Merging them would make one of the two resets late.
  private final    FollowerStallTracker      followerStallTracker   = new FollowerStallTracker();
  private          RaftPeerId                followerStallLeader;
  private volatile LongSupplier              followerStallClock     = System::currentTimeMillis;
  // Issue #8953: whether a leader has been failing to reach this follower since its in-place restart. Written by the
  // health-monitor thread (trackLeaderReachSinceRestart), read by GET /api/v1/cluster. Measured on followerStallClock,
  // which is shared on purpose: it is the health tick's test seam for every follower-side spell, not the stall's own.
  private final    LeaderReachSinceRestartTracker leaderReachTracker = new LeaderReachSinceRestartTracker();
  /**
   * The HTTPS client that requests forwarded to the leader are sent on (issue #7508). A second cache rather than
   * a share of {@link #capabilityHttpsClients}, so the forwards keep their own connection pool. This one is asked
   * by HTTP worker threads, concurrently, so a truststore rotation happens while other threads have forwards in
   * flight on the previous client. The cache retires that client with a non-blocking {@code shutdown()} rather
   * than waiting for those forwards under its monitor, and without cancelling them (issue #8025).
   */
  private final    TrustedHttpClientCache    forwardHttpsClients    = new TrustedHttpClientCache();
  // Runs leader-driven stalled-replica resyncs off the lag-monitor thread (issue #4728). One worker is
  // enough since at most one resync fires per replica per stall streak; a small bounded queue with a
  // caller-runs policy degrades to running on the lag-monitor thread under the (unlikely) burst.
  final ThreadPoolExecutor        stalledResyncExecutor = createStalledResyncExecutor();
  // Runs replication-channel resets and their leadership-transfer escalation off the lag-monitor thread
  // (issue #5346). Deliberately NOT the resync executor: a resync task blocks on HTTP to an unhealthy
  // follower for as long as the connect timeout, and channel recovery queued behind it would be delayed by
  // exactly the outage it exists to repair. A saturated queue rejects (the default AbortPolicy: each submitter
  // catches the RejectedExecutionException and decides what a drop means for it) instead of running on the caller,
  // so it can never stall replica classification - the invariant the off-thread move buys.
  final ThreadPoolExecutor        channelRecoveryExecutor = createChannelRecoveryExecutor();
  // Issue #10299: runs the self-dial probes. Deliberately NOT the channel-recovery worker: a probe blocks for up to the Raft
  // request timeout (10 s) when the member does not answer, and any sender can cause one per member and interval by
  // writing this node's id, so on the shared worker a black-holed member plus a flood of claims would hold back every
  // channel reset and leadership hand-off behind them. Only a confirmed self-dial goes on to the channel-recovery worker.
  final ThreadPoolExecutor        selfDialProbeExecutor   = createSelfDialProbeExecutor();

  /** What a self-dial probe found out about this node's channel to the addressed member (issue #10299). */
  enum SelfDialProbeOutcome {
    /** The probe came back to this node: its channel to the member leads here, and is reset. */
    SELF,
    /**
     * The addressed member answered the probe over every connection this node holds for it: none of them leads here, so
     * the claim was not this node's own request.
     */
    MEMBER,
    /**
     * The addressed member answered the probe over every connection that could be probed, but this node could not list
     * every connection Ratis holds for it (issue #10374): the one that carried the refused request may be among those
     * left out, so the claim is neither confirmed nor ruled out.
     */
    MEMBER_ON_SOME_CHANNELS,
    /** The probe failed, or was answered by somebody else: the channel does not lead here, or not provably. */
    ELSEWHERE,
    /** No probe could be sent: there is no Raft server RPC to send it over. */
    NOT_SENT
  }

  /** A self-dial probe in flight: the member it went to, and whether this node has seen it come back. */
  record PendingSelfDialProbe(RaftPeerId addressedTo, AtomicBoolean cameBack) {
  }

  /**
   * Sends a self-dial probe over this node's channel to the member the probe's header addresses (issue #10299). The
   * default is this node's own Raft server RPC, which routes the request by its header exactly as the RPCs that came back
   * to this node were routed. Throws whatever the channel throws: a probe that comes back is refused like the requests it
   * checks. Package-private so that a test without a live Raft server can stand in for the channel.
   */
  @FunctionalInterface
  interface SelfDialProbeTransport {
    RequestVoteReplyProto send(RequestVoteRequestProto probe) throws Exception;
  }

  /**
   * The connections a self-dial probe goes over, one transport each, and whether they are every connection this node holds
   * for the member (issue #10374). Ratis keeps several gRPC channels per peer - the primary one, a separate one for the
   * heartbeat stream by default, a pool for {@code readIndex} - each resolving the peer's name on its own, so a refused
   * heartbeat can travel on a channel that leads back here while a probe over the primary one reaches the member.
   */
  record SelfDialProbeChannels(List<SelfDialProbeTransport> transports, boolean complete) {
  }
  // Whether a #8491 hand-off (a leader replacing a database) is queued or running on channelRecoveryExecutor, so a
  // health tick does not queue a second one behind it (issue #8557).
  private final    AtomicBoolean             replacingHandOffQueued = new AtomicBoolean();
  // Health ticks that found a #8491 hand-off already queued or running and did not queue another: the
  // channel_recovery row's tasks.coalesced (issue #8856). Climbing on every tick means that hand-off is not draining.
  // On this RaftHAServer, like the channel-recovery executor it describes, so it survives an in-place Ratis restart -
  // unlike the state machine pools' counters, which restart with the state machine that owns them.
  private final    AtomicLong                replacingHandOffsCoalesced = new AtomicLong();
  // Inbound Raft gRPC peer allowlist, recreated on each Ratis (re)start. Periodically refreshed by the
  // health monitor tick so a returned peer's new pod IP is admitted proactively (issue #4696).
  volatile PeerAddressAllowlistFilter allowlistFilter;
  // Installed alongside allowlistFilter and null for exactly the same reasons (issue #7250): the two are the
  // connection-time and the per-RPC half of one decision.
  volatile PeerAllowlistCallInterceptor allowlistInterceptor;
  // Ratis transport parameters (gRPC TLS conf and the inbound-allowlist services customizer) built by
  // buildParameters() on each Ratis (re)start. Kept so refreshRaftClient() can rebuild the leader's
  // self-client with the SAME transport configuration: a client built with an empty Parameters would
  // dial the Raft port in plaintext and fail every handshake once TLS is on (issue #3890).
  // Published by buildParameters() only on its LAST line, once the TLS configuration and the inbound-peer
  // allowlist customizer are both installed. Two things depend on that: a ConfigurationException from a bad
  // cert/key/trust path leaves the previous, working value in place rather than a TLS-less one, and a
  // concurrent reader (refreshRaftClient() from a leader-change callback) never sees a half-built transport
  // configuration.
  volatile Parameters                 raftParameters        = new Parameters();
  final Object                    applyNotifier         = new Object();
  RaftClusterManager        clusterManager;
  final Object                    recoveryLock          = new Object();
  // Issue #11921: read-held by every commit published outside the log for its whole length (see runPublication), write-held
  // by an in-place restart while it replaces the state machine. The publication takes the validation lock of the ledger of
  // the state machine current when it starts, so it must not span the replacement of that ledger.
  private final ReentrantReadWriteLock publicationGate   = new ReentrantReadWriteLock();
  volatile boolean                   shutdownRequested     = false;
  private volatile Thread                    autoJoinThread        = null;
  private volatile LifeCycle.State           forcedStateForTesting = null;
  // Volatile: written on the thread that starts/stops HA, read from an HTTP worker thread via
  // isCrashLoopEscalated() (issue #7622), same reasoning as raftServer above.
  private volatile HealthMonitor             healthMonitor;
  // Periodic Raft snapshot/log-purge trigger (issue #5345). Runs on every node, leader and follower
  // alike, because each Ratis server purges its own log against its own snapshot index.
  private          RaftLogCompactionScheduler logCompactionScheduler;
  // Client identity and call-id sequence for the local snapshot-management requests the compaction
  // scheduler issues. A stable ClientId keeps the requests distinguishable in Ratis's logs.
  private final    ClientId                  snapshotClientId      = ClientId.randomId();
  private final    AtomicLong                snapshotCallId        = new AtomicLong();
  // Resolved Raft storage directory, cached for the free-space probes the compaction scheduler runs on
  // every tick. Config-derived and constant for this server's lifetime.
  volatile File                      cachedRaftStorageDir  = null;
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
  volatile int                       restartFailureCount   = 0;
  // The thread still running a Ratis server close that outlived arcadedb.ha.ratisCloseTimeoutMs (issue #9561). While it
  // is alive the old server's storage lock and gRPC ports may still be held, so the in-place restart refuses to start a
  // second server beside it. Written under recoveryLock, and by stop(), which does not take it.
  volatile Thread                    stuckRatisClose;
  // Full schema loads the apply thread ran in place of the incremental refresh (issue #10020). Owned here and not by the
  // state machine because createStateMachine() builds a new one on every in-place restart, and the counts are cumulative
  // since the process started, like the restart counts above
  private final    SchemaFullLoadRecorder    schemaFullLoadRecorder = new SchemaFullLoadRecorder();
  // The databases a restore on this server holds (issue #10974), see ArcadeStateMachine.holdForRestore. Owned here for the same
  // reason as the recorder above: a hold taken before an in-place restart must still refuse entries on the machine built by it
  private final    Set<String>               databasesHeldForRestore = ConcurrentHashMap.newKeySet();
  // The Raft term in which this node, as the leader, took each hold: a hold is this node's only, so it stands while that leadership
  // does (see ensureRestoreHoldStands)
  private final    Map<String, Long>         restoreHoldTerms        = new ConcurrentHashMap<>();
  private volatile BootstrapElection         bootstrapElection;
  private final    UnverifiedClosedCopyCheck unverifiedClosedCopyCheck;
  /**
   * Outcome of the most recent {@link #runBootstrapIfEligible()} pass on this node, or {@code null} while no
   * pass has finished yet.
   * <p>
   * The distinction it publishes is "the bootstrap pass has not finished" versus "it finished and here is what
   * it decided", which nothing else on this server answers: {@link ArcadeStateMachine#getBootstrapBaseline}
   * turns non-null in the MIDDLE of the apply that may still be replacing the whole database directory from a
   * leader-shipped snapshot, and a peer on which the bootstrap was never eligible records no baseline at all,
   * so a null baseline cannot tell "not yet" from "never will".
   * <p>
   * Written only through {@link #recordBootstrapOutcome}, which never lets a later pass overwrite a recorded
   * {@code COMMITTED}. Issue #7259.
   */
  private final AtomicReference<BootstrapElection.Outcome> lastBootstrapOutcome = new AtomicReference<>();

  // The collaborators that hold the parts of the HA server with a responsibility of their own. Each one keeps a
  // reference back to this instance for the state it shares with the rest of the server.
  final MisdirectedRaftRequests misdirected = new MisdirectedRaftRequests(this);
  final PeerChannelRecovery channelRecovery = new PeerChannelRecovery(this);
  final PeerEndpoints endpoints = new PeerEndpoints(this);
  final TrafficReadiness readiness = new TrafficReadiness(this);
  final PeerCapabilityMonitor capabilityMonitor = new PeerCapabilityMonitor(this);
  final RaftStorageFiles storageFiles = new RaftStorageFiles(this);
  final PeerAdmission admission = new PeerAdmission(this);
  final RaftTransportSetup transport = new RaftTransportSetup(this);
  final ReplicationStatistics replicationStats = new ReplicationStatistics(this);
  final RatisInPlaceRestart inPlaceRestart = new RatisInPlaceRestart(this);
  final VoterQuarantineCheck voterQuarantine = new VoterQuarantineCheck(this);
  final AppliedIndexWaits appliedWaits = new AppliedIndexWaits(this);

  public RaftHAServer(final ArcadeDBServer arcadeServer, final ContextConfiguration configuration) {
    this.arcadeServer = arcadeServer;
    this.unverifiedClosedCopyCheck = new UnverifiedClosedCopyCheck(this, arcadeServer);
    this.configuration = configuration;
    this.forwardHttpClient = LeaderDial.newConnectTimeoutBoundedClient(configuration);

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
    this.serverListHttpAddresses = Map.copyOf(parsed.httpAddresses());
    this.httpsAddresses.putAll(parsed.httpsAddresses());
    this.boltAddresses.putAll(parsed.boltAddresses());
    this.grpcAddresses.putAll(parsed.grpcAddresses());

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
    this.configuredPeerList = peers;
    this.configuredPeerNames = configuredPeerNames;

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

    this.runtimeJoinDetector = createRuntimeJoinDetector();
    this.stateMachine = createStateMachine();

    final long stalledResyncDurationMs = configuration.getValueAsLong(
        GlobalConfiguration.HA_STALLED_REPLICA_RESYNC_DURATION_MS);
    final boolean resyncNarrative = configuration.getValueAsBoolean(GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING);
    final long peerUnreachableThresholdMs = configuration.getValueAsLong(GlobalConfiguration.HA_PEER_UNREACHABLE_THRESHOLD);
    final long peerChannelResetDurationMs = configuration.getValueAsLong(GlobalConfiguration.HA_PEER_CHANNEL_RESET_DURATION);
    final boolean peerChannelResetEscalation = configuration.getValueAsBoolean(
        GlobalConfiguration.HA_PEER_CHANNEL_RESET_ESCALATION);
    this.clusterMonitor = new ClusterMonitor(lagWarningThreshold, stalledResyncDurationMs,
        channelRecovery::forceResyncStalledReplica, resyncNarrative, peerUnreachableThresholdMs, peerChannelResetDurationMs,
        channelRecovery::resetPeerReplicationChannel, peerChannelResetEscalation ? channelRecovery::escalateWedgedPeerChannel : null);
    // Issue #8900: a follower whose capability advertisement is fresh answers over HTTP, so a Raft RPC path that fails
    // all the same is refused by a live node, and its channel is reset without waiting out the unreachable interval.
    // observedAtMs is compared with ClusterMonitor's clock: both are System.currentTimeMillis outside tests, and a test
    // that fakes one must fake the other or the comparison means nothing.
    this.clusterMonitor.setPeerLastAnsweredAt(PeerChannelRecovery.peerLastAnsweredAt(peerCapabilities));
    this.handoffContactWindowMs = StepDownTargets.handoffContactWindowMs(
        configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN), peerUnreachableThresholdMs);
    this.quorum = Quorum.parse(configuration.getValueAsString(GlobalConfiguration.HA_QUORUM));
    this.quorumTimeout = configuration.getValueAsLong(GlobalConfiguration.HA_QUORUM_TIMEOUT);

    this.clusterManager = new RaftClusterManager(this,
        configuration.getValueAsLong(GlobalConfiguration.HA_MEMBERSHIP_CHANGE_TIMEOUT));
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
   * port (see {@link PeerEndpoints#resolveHttpAddress(RaftPeerId)}); returns {@code null} only if the peer is
   * unknown or this node's HTTP port is not yet available.
   */
  public String getPeerHttpAddress(final RaftPeerId peerId) {
    return endpoints.resolveHttpAddress(peerId);
  }

  /**
   * Returns this node's own HTTP address (host:port), or {@code null} when it cannot be resolved right
   * now (the HTTP listener is not up yet). Resolved through the same path as every peer address, so a
   * caller comparing the two compares like with like.
   */
  public String getLocalHttpAddress() {
    return endpoints.resolveHttpAddress(localPeerId);
  }

  /**
   * True when {@code address} is the HTTP endpoint this node itself is listening on, i.e. dialing it would
   * come straight back here. Answers the one question every automatic redirect has to ask before it
   * redirects: an address resolved for a <em>peer</em> that turns out to be ours identifies nothing (issue
   * #6191). It is what the derive fallback in {@link PeerEndpoints#resolveHttpAddress(RaftPeerId)} produces on a cluster
   * whose nodes share a host and declare no {@code http} port - it combines the peer's Raft host with this
   * node's HTTP port, so every peer collapses onto this node's own address.
   * <p>
   * A textual comparison, like the peer bookkeeping it reads: both sides come from the same resolver, so a
   * derived address matches a derived one. It deliberately does not resolve hostnames - a declared host name
   * is a statement about which node owns which port that this method has no business second-guessing - with
   * the single exception of the loopback spellings, which are one socket written two ways and are what a
   * single-machine cluster is usually configured with (issue #6204).
   */
  public boolean isOwnHttpAddress(final String address) {
    return isSameHttpEndpoint(getLocalHttpAddress(), address);
  }

  /**
   * Whether two {@code host:port} addresses name the same listening socket. Case-insensitive, because host
   * names are: the two sides can come from different entries of {@code HA_SERVER_LIST} and a difference in
   * case would otherwise read as "a different node", which is the answer that lets a redirect loop live.
   * <p>
   * Beyond the text, the one equivalence it knows is the loopback host: a hand-written server list for a
   * single-machine cluster routinely spells one node {@code localhost} and the next {@code 127.0.0.1}, and
   * that is precisely the deployment where the derive fallback resolves a peer onto this node's own socket
   * (issue #6204). The equivalence is only applied when the ports are equal, which is what makes it safe -
   * two nodes cannot both be listening on one port of one host, so "loopback on both sides, same port" is
   * the same socket rather than an ambiguity worth preserving. It is not extended to a wildcard bind or to a
   * different address of the loopback range: those can genuinely name another node, and answering "that is
   * me" for one of them would refuse a write that had to be forwarded. See {@link LoopbackHosts}.
   * <p>
   * Package-private for testing.
   */
  static boolean isSameHttpEndpoint(final String address, final String other) {
    if (address == null || other == null)
      return false;
    if (address.equalsIgnoreCase(other))
      return true;

    // The last colon separates the port: an unbracketed IPv6 literal carries colons of its own, and only the
    // last one is the separator.
    final int addressPortAt = address.lastIndexOf(':');
    final int otherPortAt = other.lastIndexOf(':');
    if (addressPortAt < 0 || otherPortAt < 0)
      return false;

    // Compared in place: on a same-host cluster the port is what differs, so the common "no" costs nothing.
    // A zero-length port would compare equal to another zero-length port and satisfy the condition the whole
    // equivalence rests on without there being a port at all, so it is refused first (issue #6212).
    final int portLength = address.length() - addressPortAt - 1;
    if (portLength == 0 //
        || portLength != other.length() - otherPortAt - 1 //
        || !address.regionMatches(addressPortAt + 1, other, otherPortAt + 1, portLength))
      return false;

    return LoopbackHosts.isLoopback(address.substring(0, addressPortAt)) //
        && LoopbackHosts.isLoopback(other.substring(0, otherPortAt));
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
    return endpoints.resolveHttpsAddress(peerId);
  }

  /**
   * This node's own HTTPS address (host:port), or {@code null} when SSL is disabled or it cannot be resolved
   * right now. The HTTPS counterpart of {@link #getLocalHttpAddress()}, resolved through the same path as every
   * peer's HTTPS address so a caller comparing the two compares like with like (issue #6221).
   */
  public String getLocalHttpsAddress() {
    return endpoints.resolveHttpsAddress(localPeerId);
  }

  // PeerChannelRecovery holds the implementation of these; they stay here for the callers outside this class.

  void requestRemoteResync(final String followerAddr, final String databaseName, final String clusterToken, final boolean https) throws IOException {
    channelRecovery.requestRemoteResync(followerAddr, databaseName, clusterToken, https);
  }

  void requestRemoteResync(final String followerAddr, final String databaseName, final String clusterToken,
      final boolean https, final StalledResyncOrder order) throws IOException {
    channelRecovery.requestRemoteResync(followerAddr, databaseName, clusterToken, https, order);
  }

  // MisdirectedRaftRequests holds the implementation of these; they stay here for the callers outside this class.

  public long getMisdirectedRaftRequestsRefused() {
    return misdirected.getMisdirectedRaftRequestsRefused();
  }

  void onMisdirectedRaftRequest(final String rpc, final RaftPeerId addressedTo, final RaftPeerId requestor,
      final SocketAddress remote, final long callId) {
    misdirected.onMisdirectedRaftRequest(rpc, addressedTo, requestor, remote, callId);
  }

  SelfDialProbeOutcome probeSelfDial(final RaftPeerId addressedTo) {
    return misdirected.probeSelfDial(addressedTo);
  }

  void setSelfDialProbeTransportForTesting(final SelfDialProbeTransport transport) {
    misdirected.setSelfDialProbeTransportForTesting(transport);
  }

  void setSelfDialProbeChannelsForTesting(final SelfDialProbeChannels channels) {
    misdirected.setSelfDialProbeChannelsForTesting(channels);
  }

  public long getMisdirectedChannelResets() {
    return misdirected.getMisdirectedChannelResets();
  }

  public long getMisdirectedSelfClaims() {
    return misdirected.getMisdirectedSelfClaims();
  }

  public long getSelfDialProbesUnconfirmed() {
    return misdirected.getSelfDialProbesUnconfirmed();
  }

  public long getSelfDialProbesAnsweredByMember() {
    return misdirected.getSelfDialProbesAnsweredByMember();
  }

  /**
   * Whether {@code peerId} is declared in the server list or was a member of the live Raft configuration when a recovery
   * task last read it. Reads two in-memory copies and never the division, so it is safe on the gRPC handler thread (issue
   * #10088). A false answer is "not known yet", not "not a member": {@link #isMisdirectedPeerMember} decides that.
   */
  boolean isMemberWithoutReadingTheDivision(final RaftPeerId peerId) {
    return isPeerInConfig(raftGroup.getPeers(), peerId) || misdirected.misdirectedLiveMemberIds.contains(peerId);
  }

  /**
   * Whether {@code peerId} is declared in the server list or a member of the live Raft configuration. Runs on the channel
   * recovery worker, which is where a read that Ratis may park belongs (issue #10088). The declared list answers first, so
   * the common case - a declared peer whose name resolves back to this node - never touches the division.
   * <p>
   * A configuration that cannot be read keeps the copy of the last one that could, and is reported at FINE: during an
   * in-place restart Ratis raises {@code IllegalStateException} for the whole window by design, and the refusal this is
   * annotating is already the one line the operator needs.
   */
  boolean isMisdirectedPeerMember(final RaftPeerId peerId) {
    if (isPeerInConfig(raftGroup.getPeers(), peerId))
      return true;

    // A successful read refreshes the copy on its own way out (rememberLiveMembers), and an unreadable one leaves it as is
    readCommittedPeers(Level.FINE);
    return misdirected.misdirectedLiveMemberIds.contains(peerId);
  }

  /**
   * Records the peers of a Raft configuration this node has applied, as {@link ArcadeStateMachine#notifyConfigurationChanged}
   * hands them over: the new peers and, during a joint-consensus change, the old ones, since this node dials both. It is how
   * a peer joined after start reaches the copy the misdirected-request listener reads before this node can send that peer
   * anything (issue #10144). Never blocks and never reads the division: it runs on the Ratis apply loop.
   */
  void onRaftConfigurationApplied(final Collection<RaftPeerId> peers, final Collection<RaftPeerId> oldPeers) {
    final Set<RaftPeerId> ids = new HashSet<>((peers.size() + oldPeers.size()) * 2);
    ids.addAll(peers);
    ids.addAll(oldPeers);
    misdirected.publishLiveMemberIds(ids);
  }

  /**
   * Claims {@code key}'s slot in a per-key throttle: true, and the slot stamped with {@code now}, when the slot was
   * free or last stamped at least {@code intervalMs} ago; false while a claim inside the interval holds it. Lock-free,
   * and exactly one of several concurrent callers wins a free slot.
   */
  static boolean claimThrottleSlot(final Map<RaftPeerId, Long> slots, final RaftPeerId key, final long now,
      final long intervalMs) {
    final Long last = slots.get(key);
    if (last == null)
      return slots.putIfAbsent(key, now) == null;
    return now - last >= intervalMs && slots.replace(key, last, now);
  }

  /**
   * Resolves the peer's Raft host to its current IP addresses for diagnostics, as a single printable
   * string. Never throws: an unresolvable or unknown peer yields a descriptive placeholder, because this
   * is logging support and must not break the recovery path it annotates.
   */
  String resolvePeerAddresses(final RaftPeerId peerId) {
    // The server list only, never peerRaftAddress(): this runs on the misdirected-request recovery path, which must not
    // read the live configuration (issue #10088: the division can be parked while it initializes). A peer that joined at
    // runtime is named without its resolved addresses, which costs a diagnostic, not a decision
    String rpcAddress = null;
    for (final RaftPeer peer : raftGroup.getPeers())
      if (peer.getId().equals(peerId)) {
        rpcAddress = peer.getAddress();
        break;
      }
    if (rpcAddress == null)
      return "<unknown peer address>";
    final String host = PeerEndpoints.extractHost(rpcAddress);
    if (host == null)
      return "<unknown peer address>";
    try {
      final InetAddress[] resolved = InetAddress.getAllByName(host);
      final StringBuilder buffer = new StringBuilder(host).append("=[");
      for (int i = 0; i < resolved.length; i++) {
        if (i > 0)
          buffer.append(',');
        buffer.append(resolved[i].getHostAddress());
      }
      return buffer.append(']').toString();
    } catch (final UnknownHostException e) {
      return host + "=<unresolvable: " + e.getMessage() + ">";
    }
  }

  /**
   * Hands leadership to a healthy peer because this leader holds a database it knows is behind the committed Raft log
   * (issue #8483): a quarantine, or a read floor an incomplete snapshot install left.
   * <p>
   * Every way out of that state is a resync FROM the leader, and a leader cannot pull from itself: the targeted resync
   * and the HealthMonitor retry both refuse on the leader role (issue #6111), and since #8468 this node also refuses to
   * serve the quarantined database as a snapshot, so every follower install of it fails too. Until this, the only way
   * out was an operator's {@code POST /api/v1/cluster/leader}. Once another peer leads, this node's own
   * {@code retryUnfilledSnapshotGap} tick drives the targeted resync from it.
   * <p>
   * The peers and their order are the ones a manual step-down uses ({@link #transferLeadership(long)}). A node with no
   * eligible peer ({@link PeerChannelRecovery#hasHandoffTarget}) does not attempt it, because a Ratis step-down would only re-elect this
   * node; it logs the operator action instead, and hands off on the first tick a peer becomes eligible. Attempted at most once per {@link PeerChannelRecovery#QUARANTINE_HANDOFF_COOLDOWN_MS} (see
   * {@link PeerChannelRecovery#decideQuarantineHandoff} for when the window is taken), and run on
   * {@link #channelRecoveryExecutor} because the transfer blocks for seconds and the callers are the apply thread
   * and the HealthMonitor tick.
   *
   * @param reason what this node holds behind the log, for the log line
   */
  void handOffLeadershipToResync(final String reason) {
    if (raftServer == null || shutdownRequested || !isLeader())
      return;
    // A read-only look at the window, so a health tick inside it does not queue a task that would only be refused.
    // The window itself is taken inside the task, once a transfer is actually about to be attempted.
    if (PeerChannelRecovery.withinQuarantineHandoffCooldown(channelRecovery.lastQuarantineHandoffAtMs.get(), System.currentTimeMillis()))
      return;
    try {
      channelRecoveryExecutor.execute(() -> {
        final long now = System.currentTimeMillis();
        // Leadership may have moved while this sat in the queue, and a transfer sent through a follower's client is
        // routed to the real leader, which would then step down for a reason that is not its own (issue #7134).
        final boolean leader = !shutdownRequested && isLeader();
        switch (PeerChannelRecovery.decideQuarantineHandoff(leader, leader && PeerChannelRecovery.hasHandoffTarget(getLivePeers(), localPeerId, clusterMonitor,
            handoffReachablePeers()),
            channelRecovery.lastQuarantineHandoffAtMs, channelRecovery.lastQuarantineNoPeerLogAtMs, now)) {
        case NO_PEER_REPORT -> LogManager.instance().log(this, Level.SEVERE, "%s", PeerChannelRecovery.noHandoffPeerReport(reason, isSoleVoter()));
        case TRANSFER -> channelRecovery.transferLeadershipToResync(reason);
        default -> {
          // NOT_LEADER, NO_PEER (reported within the window already) or COOLDOWN: nothing to do
        }
        }
      });
    } catch (final RejectedExecutionException e) {
      // Nothing was admitted yet, so the next health tick simply asks again.
      LogManager.instance().log(this, Level.WARNING,
          "Recovery queue is saturated; the leadership handoff to resync %s is retried on the next health tick", reason);
    }
  }

  /** What one queued quarantine handoff does (issue #8483). */
  enum QuarantineHandoff {
    /** This node no longer leads: nothing to hand off. */
    NOT_LEADER,
    /** No other peer to hand leadership to, and the operator was not told within the window: report it. */
    NO_PEER_REPORT,
    /** No other peer to hand leadership to, already reported within the window. */
    NO_PEER,
    /** A handoff was attempted within the window. */
    COOLDOWN,
    /** Attempt the transfer; the window is taken. */
    TRANSFER
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

  /**
   * Single-worker pool for replication-channel recovery (issue #5346). Unlike the resync executor it must
   * never run a task on the caller, which is the lag-monitor thread that classifies every replica: a
   * blocking DNS lookup or a 10 s leadership transfer there would defeat the point of moving the work
   * off-thread. It therefore keeps an abort policy (one that counts, for the executor row of issue #8856)
   * and each submitter decides what a rejection means for it - see {@link PeerChannelRecovery#resetPeerReplicationChannel}
   * (free to drop, retried next interval) and
   * {@link PeerChannelRecovery#escalateWedgedPeerChannel} (a one-shot, so a drop must be surfaced to the operator).
   */
  private static ThreadPoolExecutor createChannelRecoveryExecutor() {
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(0, 1, 30L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(32), r -> {
      final Thread t = new Thread(r, "arcadedb-raft-channel-recovery");
      t.setDaemon(true);
      return t;
    }, CountingRejectionPolicy.abort());
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  /**
   * Single-worker pool for self-dial probes (issue #10299). Abort policy like {@link #createChannelRecoveryExecutor}: a
   * dropped probe releases its throttle slot and the next refused request queues it again, so a refusal costs nothing
   * but the delay, and must never run on the gRPC handler thread that submitted it.
   */
  private static ThreadPoolExecutor createSelfDialProbeExecutor() {
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(0, 1, 30L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(8), r -> {
      final Thread t = new Thread(r, "arcadedb-raft-self-dial-probe");
      t.setDaemon(true);
      return t;
    }, CountingRejectionPolicy.abort());
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  private static ThreadPoolExecutor createStalledResyncExecutor() {
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(0, 1, 30L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(16), r -> {
      final Thread t = new Thread(r, "arcadedb-raft-stalled-resync");
      t.setDaemon(true);
      return t;
    }, CountingRejectionPolicy.callerRuns());
    return executor;
  }

  /**
   * The {@code pool=stalled_resync} executor row (issue #8856). Caller-runs, so its saturations are the row's
   * {@code caller_run_fallbacks}: a resync request sent from the lag-monitor thread, delaying replica classification
   * for as long as the follower takes to answer.
   */
  PoolStats getStalledResyncPoolStats() {
    return PoolMetrics.statsOf(stalledResyncExecutor);
  }

  /** The {@code pool=self_dial_probe} executor row (issue #10299). */
  PoolStats getSelfDialProbePoolStats() {
    return PoolMetrics.statsOf(selfDialProbeExecutor);
  }

  /** Self-dial probes refused while the pool was running (issue #10299): each released its slot to be queued again. */
  long getSelfDialProbeRejections() {
    return selfDialProbeExecutor.getRejectedExecutionHandler() instanceof CountingRejectionPolicy policy ?
        policy.getSaturations() :
        0L;
  }

  /** The {@code pool=channel_recovery} executor row (issue #8856). */
  PoolStats getChannelRecoveryPoolStats() {
    return PoolMetrics.statsOf(channelRecoveryExecutor);
  }

  /**
   * Channel-recovery tasks refused while the pool was running (issue #8856): channel resets, wedged-channel
   * escalations, quarantine hand-offs and #8491 hand-offs alike. Each submitter logs what its own drop means; the
   * escalation is the one that is not retried.
   */
  long getChannelRecoveryRejections() {
    // A metrics scrape must never throw: read the count only from the policy this class installs.
    return channelRecoveryExecutor.getRejectedExecutionHandler() instanceof CountingRejectionPolicy policy ?
        policy.getSaturations() :
        0L;
  }

  /** #8491 hand-offs not queued because one was already queued or running; see {@link #replacingHandOffsCoalesced}. */
  long getReplacingHandOffsCoalesced() {
    return replacingHandOffsCoalesced.get();
  }

  /**
   * The peer whose {@code arcadedb.server.name} is {@code serverName}, or {@code null} when no peer of the
   * configured server list answers to it (issue #7424). Applies the rules a node uses to find ITSELF in the list
   * ({@link RaftPeerAddressResolver#findLocalPeerId}) to another node's name: a configured {@code name@host}
   * entry, a host equal to the name, or a {@code -N}/{@code _N} suffix naming the position in the list. Those
   * rules hold on every node because every node reads the same list, so the answer here is the peer that node
   * resolved itself to.
   */
  public RaftPeerId resolvePeerIdByServerName(final String serverName) {
    if (serverName == null || serverName.isBlank())
      return null;
    try {
      return RaftPeerAddressResolver.findLocalPeerId(configuredPeerList, configuredPeerNames, serverName, arcadeServer);
    } catch (final IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * The HTTPS clients this node uses for its peer-to-peer RPCs, built once per server and reused (issue #7301).
   */
  TrustedHttpClientCache getHttpsClients() {
    return capabilityHttpsClients;
  }

  /**
   * The HTTPS client a forward to the leader is sent on, built from this node's truststore and rebuilt only when
   * that truststore changes (issue #7508). Owned here, so it is closed with the server rather than held for the
   * life of the JVM.
   */
  HttpClient getForwardHttpsClient() throws IOException {
    return forwardHttpsClients.clientFor(arcadeServer);
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
    // Drop the one by-design SimpleStateMachineStorage warning about the missing snapshot digest,
    // without silencing that logger's genuine failures (issue #6991).
    RatisSnapshotDigestWarningFilter.install();
    // Drop the Ratis client's SEVERE for an entry the leader refused before appending it: that is an ordinary
    // retryable conflict since issue #6965, not a send failure.
    RatisRefusedEntryErrorFilter.install();
    // Drop the Ratis gRPC service's per-request stack trace for a request refused as addressed to another peer: the
    // condition is reported once per peer and period by onMisdirectedRaftRequest (issue #10016).
    RatisMisdirectedRequestFilter.install();

    final RaftProperties properties = RaftPropertiesBuilder.build(configuration);

    // Read by the leader's log appenders, which Ratis builds itself (issue #11458)
    applyBackPressure = new ApplyBackPressure(ApplyBackPressure.resolveBudgetBytes(configuration, Runtime.getRuntime().maxMemory()),
        peerCapabilities::freshAppliedIndexOf);
    ApplyBackPressure.register(localPeerId.toString(), applyBackPressure);

    final File storageDir = storageFiles.getRaftStorageDir();
    // Only delete existing Raft storage when persistence is not requested.
    // Persistent mode (HA_RAFT_PERSIST_STORAGE=true) is used in tests that restart nodes
    // within a single test run, so the Raft log survives across stop/start calls.
    final boolean persistStorage = RaftStorageFiles.resolvePersistStorage(configuration);
    if (!persistStorage)
      RaftStorageFiles.discardNonPersistentRaftStorage(storageDir);
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    this.tokenProvider = new ClusterTokenProvider(configuration);
    this.tokenProvider.initClusterToken();
    // A token derived from the root password, inline or from a file, is held to the one this node last ran with (issues
    // #10601, #10924): a password rotated in place would otherwise move this node to a token its peers do not hold,
    // silently, at this restart.
    if (persistStorage)
      this.tokenProvider.verifyDerivedTokenFingerprint(RaftStorageFiles.clusterTokenFingerprintFile(storageDir),
          configuration.getValueAsBoolean(GlobalConfiguration.HA_CLUSTER_TOKEN_ALLOW_ROTATION));

    // When persistent storage is requested and the storage directory already has data,
    // use RECOVER mode so Ratis loads the existing Raft log instead of trying to format
    // (which would fail if the group directory already exists).
    final File[] storageDirs = storageDir.listFiles(f -> f.isDirectory() && !ArcadeDBServer.FILESYSTEM_RECOVERY_DIRECTORY.equals(f.getName()));
    final boolean hasExistingStorage = persistStorage && storageDir.exists()
        && storageDirs != null && storageDirs.length > 0;
    final RaftStorage.StartupOption startupOption = hasExistingStorage
        ? RaftStorage.StartupOption.RECOVER
        : RaftStorage.StartupOption.FORMAT;

    final Parameters parameters = transport.buildParameters(configuration);

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

    // A database whose crash recovery stopped short is quarantined before this node can serve it or lead with it, and is
    // re-installed from the cluster (issue #11250)
    stateMachine.quarantineDatabasesWithIncompleteRecovery();

    raftClient = buildRaftClient(raftGroup, properties, parameters);

    LogManager.instance()
        .log(this, Level.INFO, "Raft cluster joined: %d nodes %s", peerDisplayNames.size(), peerDisplayNames.values());

    final int batchSize = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_BATCH_SIZE);
    final int queueSize = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_QUEUE_SIZE);
    final int offerTimeout = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_OFFER_TIMEOUT);
    // #4743: the effective per-entry ceiling is the SMALLER of the gRPC frame cap and the Ratis
    // appender byte limit - an entry above the latter makes the leader step down.
    final long grpcMessageSizeMax = RaftPropertiesBuilder.maxReplicatedEntrySize(configuration);
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
      final KubernetesAutoJoin autoJoin = new KubernetesAutoJoin(arcadeServer, raftGroup, localPeerId, raftProperties,
          parameters);
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
    final long divergedFollowerRecoveryDurationMs = effectiveDivergedFollowerRecoveryDurationMs(
        configuration.getValueAsLong(GlobalConfiguration.HA_DIVERGED_FOLLOWER_RECOVERY_DURATION_MS),
        RaftPropertiesBuilder.electionTimeoutMaxFor(configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN),
            configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX)));
    this.healthMonitor = new HealthMonitor(this, healthInterval, staleFollowerLagThreshold, staleFollowerRecoveryDurationMs,
        divergedFollowerRecovery, divergedFollowerMaxReformats, crashLoopRestartThreshold, divergedFollowerRecoveryDurationMs);
    this.healthMonitor.start();

    // Peer capabilities are refreshed on EVERY node, not only the leader (issue #7549). #7219 introduced the
    // monitor for a leader-side consumer - the schema-delta decision - and started it from startLagMonitor(),
    // so a follower's registry was empty by construction. Two consumers have since arrived that are not
    // leader-side: the security compare-and-set gate of #7511, which runs wherever the client landed because
    // the group and API-token routes do not forward, and GET /api/v1/cluster, which an operator polls to ask
    // whether a rolling upgrade is complete. Both had to work around the emptiness - the gate by paying one
    // synchronous probe round under the ServerSecurity monitor, the status document by simply omitting the
    // rows - and an operator could not answer "is this cluster ready for a group change" without first finding
    // the leader. A probe is a read-only peer-to-peer GET and configuredPeers() is available in every role, so
    // there was never anything leader-shaped about the asking itself.
    //
    // WHAT IT COSTS, said out loud because it is a change in shape and not only in role (review of PR #7941):
    // the fan-out was N-1 probes per REFRESH_PERIOD_MS cluster-wide and is now N*(N-1), because every node asks
    // rather than one. On the cluster sizes Raft is useful at - single-digit voters, since every write needs a
    // majority round trip - that is a handful of read-only HTTP GETs every five seconds against an endpoint
    // that answers from memory, which is why it is paid rather than avoided. It is quadratic, though, so a
    // deployment that grows the voter count well past that should know where the traffic comes from.
    capabilityMonitor.startCapabilityMonitor();

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
      return recordBootstrapOutcome(BootstrapElection.Outcome.SKIPPED_DISABLED);
    try {
      // onLeaderChanged() is INSIDE the try on purpose: runIfEligible() swallows its own Throwable into
      // FAILED, so the only way out of this method without an outcome is a throw from onLeaderChanged, and
      // leaving it outside would mean the one case this catch exists for is the one it does not cover.
      election.onLeaderChanged();
      return recordBootstrapOutcome(election.runIfEligible());
    } catch (final RuntimeException | Error e) {
      // Publish a terminal outcome anyway: a caller waiting for the pass to finish must not be left waiting
      // out its whole budget on a pass that already died.
      recordBootstrapOutcome(BootstrapElection.Outcome.FAILED);
      throw e;
    }
  }

  /**
   * Records {@code outcome} as this node's last bootstrap result, except that a recorded
   * {@link BootstrapElection.Outcome#COMMITTED} is never overwritten.
   * <p>
   * A later pass on the same node CANNOT commit a second baseline - {@code isFirstFormation} closes the moment
   * the first one is applied - so every outcome that follows a {@code COMMITTED} is a report that there was
   * nothing left to do, not a revision of what happened. Letting one overwrite would turn "this cluster
   * committed a baseline" into "it did not", and a reader waiting on the reinstall that baseline triggers would
   * be released in the middle of it.
   * <p>
   * That is not hypothetical. {@code ArcadeStateMachine.notifyLeaderChanged} submits
   * {@link #runBootstrapIfEligible()} on every notification naming this node leader, with no term guard, and
   * its own comment records that Ratis sometimes fires a same-term re-notification; the second pass then
   * returns {@code SKIPPED_NOT_FIRST_FORMATION}. Tests reach the same shape deliberately by calling
   * {@code runBootstrapIfEligible()} a second time. Issue #7259.
   */
  private BootstrapElection.Outcome recordBootstrapOutcome(final BootstrapElection.Outcome outcome) {
    // An AtomicReference rather than a volatile with a check-then-set: production only reaches this from the
    // single-threaded lifecycleExecutor, but runBootstrapIfEligible() is public and several tests call it
    // directly from their own thread while that executor may still be running the automatic pass. A lost
    // update there would drop the COMMITTED this method exists to protect.
    lastBootstrapOutcome.updateAndGet(
        current -> current == BootstrapElection.Outcome.COMMITTED ? current : outcome);
    return outcome;
  }

  /**
   * The outcome of the most recent bootstrap pass that finished on this node, or {@code null} while none has.
   * <p>
   * Only the leader runs a pass ({@link ArcadeStateMachine#notifyLeaderChanged} submits it), so on a follower
   * this stays {@code null} for the node's whole life. A non-null value means a pass RETURNED: for
   * {@link BootstrapElection.Outcome#COMMITTED} at least one {@code BOOTSTRAP_FINGERPRINT_ENTRY} was committed -
   * the commit goes through {@code RaftGroupCommitter.submitAndWait}, which blocks on the quorum reply - and
   * every other value means no pass on this node has committed anything.
   * <p>
   * {@code COMMITTED} is sticky for the node's life; see {@link #recordBootstrapOutcome}. Issue #7259.
   */
  public BootstrapElection.Outcome getLastBootstrapOutcome() {
    return lastBootstrapOutcome.get();
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
      if (isDivisionStateReported(divisionState))
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
   * How long ago this follower's division last heard from its leader (an append or a heartbeat), in milliseconds, or
   * -1 on the leader, with no known leader, or when the division cannot be read (issue #8900).
   * <p>
   * {@link #getRaftLifeCycleState()} reads this node's own division, which reports RUNNING even when the leader's
   * appends never reach it - in #8898 they kept landing on the CLOSED division of the server an in-place restart had
   * replaced. This figure is what that division's RUNNING hides: the time since the leader last reached it.
   */
  public long getLeaderContactElapsedMs() {
    final RaftServer server = raftServer;
    if (server == null)
      return -1L;
    try {
      final RaftProtos.RoleInfoProto roleInfo = server.getDivision(raftGroup.getGroupId()).getInfo().getRoleInfoProto();
      return leaderContactElapsedMs(roleInfo);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Cannot read the leader contact of the Raft division: %s", e.getMessage());
      return -1L;
    }
  }

  /** The follower's time since its leader's last contact in {@code roleInfo}, or -1 when it carries none. */
  static long leaderContactElapsedMs(final RaftProtos.RoleInfoProto roleInfo) {
    if (roleInfo == null || roleInfo.getRole() != RaftProtos.RaftPeerRole.FOLLOWER || !roleInfo.hasFollowerInfo())
      return -1L;
    final RaftProtos.FollowerInfoProto followerInfo = roleInfo.getFollowerInfo();
    if (!followerInfo.hasLeaderInfo() || !followerInfo.getLeaderInfo().hasId())
      return -1L;
    return followerInfo.getLeaderInfo().getLastRpcElapsedTimeMs();
  }

  /**
   * Whether a division state overrides the RUNNING proxy state in {@link #getRaftLifeCycleState()}: the terminal ones,
   * and CLOSING too (issue #8651) - Ratis closes the division from the dying StateMachineUpdater thread, and that close
   * can stay in CLOSING while the proxy reports RUNNING, a zombie that answered raftState=RUNNING.
   */
  static boolean isDivisionStateReported(final LifeCycle.State divisionState) {
    return divisionState == LifeCycle.State.CLOSED || divisionState == LifeCycle.State.EXCEPTION
        || divisionState == LifeCycle.State.CLOSING;
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
   * is NOT actively catching up (a catch-up whose applied index stopped moving no longer counts, issue #8341),
   * has no snapshot download already pending, AND is not making forward
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
    if (sm == null || sm.isSnapshotDownloadPending()) {
      lastLagCheckAppliedIndex = -1; // already known to be resyncing: re-baseline when normal checks resume
      return false;
    }
    // Against the leader's commit index when this follower has learned a larger one (issue #8321): a follower whose
    // inbound replication channel is wedged has its own commit index clamped to what it received, reads a lag of 0,
    // and this recovery never armed on it.
    final long commit = getFollowerCommitIndex();
    final long applied = getLastAppliedIndex();
    // The catch-up flag exempts the follower only while the catch-up is actually applying (issue #8341). The flag is
    // cleared only by an apply that reaches the commit index, so a catch-up that stops applying kept it set, and
    // kept this check off, for as long as the stall lasted. The baseline is kept across the catch-up so a stall is
    // seen on the next tick.
    if (catchUpExemptsLagCheck(sm.isCatchingUp(), applied, lastLagCheckAppliedIndex)) {
      lastLagCheckAppliedIndex = applied;
      return false;
    }
    final boolean lagging = isPersistentlyLagging(commit, applied, lagThreshold, lastLagCheckAppliedIndex);
    if (applied >= 0)
      lastLagCheckAppliedIndex = applied;
    return lagging;
  }

  /**
   * Whether the state machine's catch-up flag still exempts this follower from the persistent-lag check (issue
   * #8341): only while the applied index moves. {@code previousAppliedIndex < 0} means no prior sample, so progress
   * cannot be judged yet and the flag is trusted for this tick. Package-private for testing.
   */
  static boolean catchUpExemptsLagCheck(final boolean catchingUp, final long appliedIndex,
      final long previousAppliedIndex) {
    return catchingUp && (previousAppliedIndex < 0 || appliedIndex > previousAppliedIndex);
  }

  /**
   * Whether the state machine's catch-up flag describes a catch-up that still has something to apply (issue
   * #8341). The flag is set on an applied-index jump and cleared only by an apply that reaches the commit index,
   * so a follower that stopped applying can carry it with {@code appliedIndex >= commitIndex}, where there is
   * nothing left to catch up on. A negative index (state not readable) keeps the flag's answer. Package-private
   * for testing.
   */
  static boolean isActivelyCatchingUp(final boolean catchingUp, final long appliedIndex, final long commitIndex) {
    if (!catchingUp)
      return false;
    if (appliedIndex < 0 || commitIndex < 0)
      return true;
    return commitIndex > appliedIndex;
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
  public void retryUnfilledSnapshotGap() {
    if (raftServer == null || shutdownRequested)
      return;
    final ArcadeStateMachine sm = stateMachine;
    if (sm != null)
      sm.retryUnfilledSnapshotGap();
  }

  @Override
  public void verifyBootstrapDivergence() {
    if (raftServer == null || shutdownRequested)
      return;
    final ArcadeStateMachine sm = stateMachine;
    if (sm != null)
      sm.verifyBootstrapDivergence();
  }

  @Override
  public void retryPendingBootstrapReplacements() {
    if (raftServer == null || shutdownRequested)
      return;
    final ArcadeStateMachine sm = stateMachine;
    if (sm != null)
      sm.retryPendingBootstrapReplacements();
  }

  @Override
  public void reverifyUnverifiedClosedCopies() {
    if (raftServer == null || shutdownRequested)
      return;
    final ArcadeStateMachine sm = stateMachine;
    if (sm != null)
      sm.reverifyUnverifiedClosedCopies();
  }

  @Override
  public void handOffLeadershipWhileReplacingDatabase() {
    if (raftServer == null || shutdownRequested)
      return;
    final ArcadeStateMachine sm = stateMachine;
    if (sm != null)
      queueReplacingDatabaseHandOff(sm);
  }

  /**
   * Runs the #8491/#8529 hand-off ({@link ArcadeStateMachine#handOffLeadershipWhileReplacingDatabase()}) on
   * {@link #channelRecoveryExecutor}, the single worker the #8483 and #5346 hand-offs already run on, rather than inline
   * on the health-monitor thread (issue #8557).
   * <p>
   * Inline, it raced them: a leader that quarantines a database arms #8483 and, once the resync install it triggers
   * is running, #8491 too, both in the same health tick. Ratis keeps one pending transfer per leader and refuses a
   * second one naming another peer. On one worker the automatic hand-offs cannot overlap, and the health tick no longer
   * blocks for the length of a transfer. The state machine's own throttle still decides whether a queued run
   * transfers; this only keeps a tick from queueing a second run while one is waiting or running.
   * <p>
   * Package-private for unit tests.
   */
  void queueReplacingDatabaseHandOff(final ArcadeStateMachine sm) {
    // The common case, every tick on a healthy node: no leader service gap (issue #8529; a database being replaced,
    // issue #8491, is one kind), so nothing is queued. The episode is over, though, so its failures in a row are
    // forgotten here, where the state machine's own reset is never reached (issue #8556): one volatile read when there
    // is nothing to forget. Leadership is tested first: hasLeaderServiceGap() can stat a directory per marked
    // database. A follower's tick still pays it on EVERY tick after a hand-off that moved leadership, until the gap
    // closes: the reset re-checks the gap to decide whether to forget that hand-off. One stat per marked database.
    if (!isLeader() || !sm.hasLeaderServiceGap()) {
      sm.resetReplacingLeaderHandOffBackOff();
      return;
    }
    if (!replacingHandOffQueued.compareAndSet(false, true)) {
      replacingHandOffsCoalesced.incrementAndGet();
      return;
    }
    try {
      channelRecoveryExecutor.execute(() -> {
        try {
          // The state machine re-checks leadership and the replacement itself: either may have ended in the queue.
          // restartRatis() may also have replaced the state machine meanwhile; a stale one no longer speaks for this
          // node, and the current one is asked on the next tick.
          if (!shutdownRequested && stateMachine == sm)
            sm.handOffLeadershipWhileReplacingDatabase();
        } finally {
          replacingHandOffQueued.set(false);
        }
      });
    } catch (final RejectedExecutionException e) {
      replacingHandOffQueued.set(false);
      // Nothing was attempted, so the next health tick simply asks again.
      LogManager.instance().log(this, Level.WARNING,
          "Recovery queue is saturated; the leadership hand-off of a leader replacing a database is retried on the "
              + "next health tick (issue #8491)");
    }
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
    // The leader's commit index when it is ahead of the local one, as for isFollowerLaggingBeyond (issue #8321).
    final long commit = getFollowerCommitIndex();
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
   * {@code HA_DIVERGED_FOLLOWER_RECOVERY_DURATION_MS} before acting, which filters out the brief window
   * around an election before the no-op commits. Returns false for the leader, when no leader is
   * known, while actively catching up (a catch-up that has applied everything it committed no longer counts, issue
   * #8341) or installing a snapshot, and whenever the state cannot be read.
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
    // HealthMonitor requires the stuck condition to persist for HA_DIVERGED_FOLLOWER_RECOVERY_DURATION_MS
    // before acting, which absorbs any one-tick inconsistency here.
    // The catch-up flag counts only while there is something left to apply (issue #8341): a stale flag with
    // commitIndex == appliedIndex hid exactly the signature below.
    final long commitIndex = getCommitIndex();
    return isStuckDivergedState(getLeaderId() != null, isActivelyCatchingUp(sm.isCatchingUp(), applied.getIndex(), commitIndex),
        sm.isSnapshotDownloadPending(), getCurrentTerm(), applied.getTerm(), applied.getIndex(), commitIndex,
        leaderReportedCommitIndex);
  }

  /**
   * Debounced, operator-facing counterpart of {@link #isFollowerStuckDiverged()} for {@code GET /api/v1/cluster}
   * (issue #8289): {@code true} once the stuck-at-stale-term signature has been observed on two consecutive
   * {@link HealthMonitor} ticks. Raw and undebounced, {@link #isFollowerStuckDiverged()} can be
   * momentarily {@code true} for any follower around a normal election - before it applies the new leader's
   * current-term no-op - so publishing it as-is would report a routine leader change as an incident on every
   * status poll. This is the same filter the health monitor already applies before it starts counting toward
   * {@code HA_DIVERGED_FOLLOWER_RECOVERY_DURATION_MS}, at a fraction of that duration, so an operator sees the risk
   * long before the automatic reformat (if enabled) fires - not only in the leader's replication log, which
   * before this was the only place the "advancing at 0 entries/tick" symptom showed up at all.
   * <p>
   * {@code false} while the health monitor is not running (HA not started, {@code arcadedb.ha.healthCheckInterval}
   * disabled). Deliberately independent of {@code arcadedb.ha.divergedFollowerRecovery}: with automatic recovery
   * turned off a stuck follower never self-heals, which makes this the ONLY signal an operator has that it
   * happened.
   */
  public boolean isFollowerStuckAtStaleTermConfirmed() {
    final HealthMonitor monitor = healthMonitor;
    return monitor != null && monitor.isFollowerStuckDivergedConfirmed();
  }

  /**
   * Pure decision function behind {@link #isFollowerStuckDiverged()}, split out so the predicate can be
   * unit-tested in isolation from the Ratis state plumbing (the destructive recovery makes the predicate
   * the highest-risk piece). Returns {@code true} for the "stuck at a stale term" signature: a follower
   * that recognizes a leader, is neither catching up nor installing a snapshot, has applied everything it
   * could locally commit ({@code commitIndex == appliedIndex}) yet at a stale term
   * ({@code currentTerm > appliedTerm}). Any negative term/index input (state not readable) yields
   * {@code false}, including a negative {@code appliedTerm}. A follower whose applied index has reached the commit
   * index its leader last reported ({@code leaderReportedCommitIndex}, {@code -1} when unknown) does not match
   * either: the leader has committed nothing it could be rejecting (issue #11493).
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
      final long commitIndex, final long leaderReportedCommitIndex) {
    if (!leaderPresent || catchingUp || snapshotPending)
      return false;
    if (currentTerm < 0 || appliedTerm < 0 || appliedIndex < 0 || commitIndex < 0)
      return false;
    // Issue #11493: the leader itself has committed nothing beyond what this follower applied (an idle cluster right
    // after a leader change, before the new term's first commit), so there is nothing to reject and the applied term
    // cannot advance. Applied entries are committed, hence part of the leader's log: not divergence. A negative
    // figure means the leader has not reported one yet, and keeps the signature below.
    if (leaderReportedCommitIndex >= 0 && leaderReportedCommitIndex <= appliedIndex)
      return false;
    // We have applied everything we could locally commit, but at a stale term: we are rejecting the
    // leader's current-term entries and cannot move forward.
    return currentTerm > appliedTerm && commitIndex == appliedIndex;
  }

  /**
   * Effective persistence window of the stuck-at-stale-term reformat (issue #8375): the configured
   * {@code arcadedb.ha.divergedFollowerRecoveryDurationMs}, floored at twice {@code arcadedb.ha.electionTimeoutMax}.
   * A follower that stops hearing its leader starts an election within one election timeout, which clears the
   * leader-present half of the signature; a window at or below that could reformat a node that was only waiting for
   * the election. The floor scales with a WAN-tuned cluster rather than being a fixed number.
   */
  static long effectiveDivergedFollowerRecoveryDurationMs(final long configuredMs, final long electionTimeoutMaxMs) {
    final long floorMs = 2L * Math.max(0L, electionTimeoutMaxMs);
    if (configuredMs >= floorMs)
      return configuredMs;
    LogManager.instance().log(RaftHAServer.class, Level.WARNING,
        "arcadedb.ha.divergedFollowerRecoveryDurationMs=%d is below 2x arcadedb.ha.electionTimeoutMax, using %dms",
        configuredMs, floorMs);
    return floorMs;
  }

  /**
   * Where the division stood right after an in-place restart (issue #8901): last log entry ({@code null} for an empty
   * log), current term, and whether a leader was known.
   */
  record InPlaceRestartBaseline(TermIndex lastEntry, long term, boolean leaderKnown) {
  }

  // RatisInPlaceRestart holds the implementation of these; they stay here for the callers outside this class.

  public HAReplicationStatsProvider.InPlaceRestartStats getInPlaceRestartStats() {
    return inPlaceRestart.getInPlaceRestartStats();
  }

  int getRecoverRestartCount() {
    return inPlaceRestart.getRecoverRestartCount();
  }

  int getFormatRestartCount() {
    return inPlaceRestart.getFormatRestartCount();
  }

  /**
   * {@inheritDoc}
   * <p>
   * Proof is a new last log entry (an append reached the running server and was accepted) or a newer term with a known
   * leader (that leader's appenders dialled the running server). An unreadable division holds. Once proven the baseline
   * is dropped until the next in-place restart.
   */
  @Override
  public boolean isReplicationPathUnprovenSinceRestart() {
    return replicationPathUnproven(inPlaceRestartBaseline, inPlaceRestart::readReplicationPosition);
  }

  /**
   * One evaluation over the baseline reference, package-private for testing: resolves {@link RatisInPlaceRestart#PENDING_BASELINE}
   * (holding, nothing to compare yet) and drops the baseline once proven, both by compare-and-set.
   */
  static boolean replicationPathUnproven(final AtomicReference<InPlaceRestartBaseline> baselineRef,
      final Supplier<InPlaceRestartBaseline> positionReader) {
    final InPlaceRestartBaseline baseline = baselineRef.get();
    if (baseline == null)
      return false;
    final InPlaceRestartBaseline current = positionReader.get();
    if (baseline == RatisInPlaceRestart.PENDING_BASELINE) {
      if (current != null)
        baselineRef.compareAndSet(RatisInPlaceRestart.PENDING_BASELINE, current);
      return true;
    }
    if (current == null)
      return true;
    if (replicationPathUnproven(baseline, current.lastEntry(), current.term(), current.leaderKnown()))
      return true;
    baselineRef.compareAndSet(baseline, null);
    return false;
  }

  /**
   * Pure comparison, package-private for testing: {@code true} while a baseline exists and neither a new last entry
   * nor a newer term with a known leader has been seen, or the current term cannot be read.
   */
  static boolean replicationPathUnproven(final InPlaceRestartBaseline baseline, final TermIndex currentLastEntry,
      final long currentTerm, final boolean leaderKnown) {
    if (baseline == null)
      return false;
    if (currentTerm < 0)
      return true;
    if (!Objects.equals(currentLastEntry, baseline.lastEntry()))
      return false;
    // A term bumped by a candidate's vote request alone proves nothing: only a leader at the new term has dialled us.
    return currentTerm == baseline.term() || !leaderKnown;
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
    inPlaceRestart.restartRatis(true);
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
    inPlaceRestart.restartRatis(false);
  }

  @Override
  public String getRaftLogFailure() {
    final ArcadeStateMachine sm = stateMachine;
    if (sm == null)
      return null;
    final ArcadeStateMachine.RaftLogFailure failure = sm.getRaftLogFailure();
    if (failure != null)
      return failure.describe();
    return null;
  }

  @Override
  public String getDeadStateMachineUpdater() {
    final ArcadeStateMachine sm = stateMachine;
    return sm != null ? sm.describeDeadApplyThread() : null;
  }

  /**
   * Whether the division's Raft log has been closed. Ratis marks a log failed ({@code notifyLogFailed}) only for an I/O
   * error on its writer thread; a log CLOSED under a live division - what a dying {@code StateMachineUpdater} leaves
   * behind when the close it starts from its own thread never finishes - rejects every append with
   * {@code SegmentedRaftLog: Failed to append} and never reaches that callback, so the #7037 mark stayed empty on the
   * zombie of issue #8652. False when the log cannot be read: a transient window while the server starts or restarts.
   */
  @Override
  public boolean isRaftLogClosed() {
    final RaftServer server = raftServer;
    if (server == null || shutdownRequested)
      return false;
    try {
      return server.getDivision(raftGroup.getGroupId()).getRaftLog() instanceof RaftLogBase base && !base.isOpened();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Cannot read the Raft log state: %s", e.getMessage());
      return false;
    }
  }

  /**
   * Why the cluster's first-formation bootstrap makes this node unfit to serve clients, or {@code null} when it
   * does not (issue #7519). Delegates to {@link ArcadeStateMachine#bootstrapWindowReason()}, which holds both
   * halves of the window; see it for what the two conditions are and why each one takes the node out of the
   * Service. Reached from {@code RaftHAPlugin.getBootstrapWindowReason()}, which is what the readiness probe
   * consults.
   */
  public String getBootstrapWindowReason() {
    final ArcadeStateMachine sm = stateMachine;
    return sm != null ? sm.bootstrapWindowReason() : null;
  }

  /**
   * One-line description of the critical halt for {@code RaftHAPlugin.getCriticalHaltReason()}, which is what the
   * readiness probe reads (issue #7872). {@code null} while the state machine is applying entries.
   */
  public String getCriticalHaltReason() {
    final ArcadeStateMachine.CriticalHalt halt = getCriticalHalt();
    return halt != null ? halt.describe() : null;
  }

  /**
   * The critical halt itself rather than its one-line description (issue #7872): {@code GetClusterHandler} and
   * {@code ClusterAlerts} publish the index and the timestamp separately, which is what tells an operator whether
   * the answer is "upgrade this node" or "file a bug". {@code null} while the state machine is applying, and
   * before HA has started.
   */
  public ArcadeStateMachine.CriticalHalt getCriticalHalt() {
    final ArcadeStateMachine sm = stateMachine;
    return sm != null ? sm.getCriticalHalt() : null;
  }

  /**
   * {@inheritDoc}
   * <p>
   * "Enough" is two log segments ({@code arcadedb.ha.logSegmentSize}): Ratis rolls the open segment and
   * preallocates the next on recovery, so with less than that the restarted writer fails at the first append.
   */
  @Override
  public boolean isRaftStorageWritable() {
    final File volume = storageFiles.raftStorageVolume();
    if (volume == null)
      return true; // volume unknown: never guess "full" from it, let the restart budget bound the attempts
    final RaftProperties properties = raftProperties;
    final long segmentSize = properties != null ? RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize()
        : RaftServerConfigKeys.Log.SEGMENT_SIZE_MAX_DEFAULT.getSize();
    return RaftStorageFiles.hasRoomForSegments(volume.getUsableSpace(), volume.getTotalSpace(), segmentSize);
  }

  // RaftStorageFiles holds the implementation of these; they stay here for the callers outside this class.

  void compactRaftLogBeforeSnapshotInstall(final String databaseName) {
    storageFiles.compactRaftLogBeforeSnapshotInstall(databaseName);
  }

  File getRaftStorageDir() {
    return storageFiles.getRaftStorageDir();
  }

  /**
   * The runtime-join detector this server owns, persisted next to the Raft storage directory (issue #8329).
   * <p>
   * A restart restores the armed state only when the Raft storage itself survives restarts
   * ({@code arcadedb.ha.raftPersistStorage}, the default): a node whose log is wiped at every start rejoins from
   * nothing, and a marker left by its previous membership would describe a node that no longer exists. A storage
   * location that cannot be resolved here leaves the detector in memory only - the behaviour before issue #8329 -
   * rather than failing the construction of the server over a readiness refinement.
   */
  private RuntimeJoinDetector createRuntimeJoinDetector() {
    try {
      final File raftStorageDir = storageFiles.getRaftStorageDir();
      return new RuntimeJoinDetector(RaftStorageFiles.runtimeJoinMarkerFile(raftStorageDir), RaftStorageFiles.snapshotInstallHoldMarkerFile(raftStorageDir),
          RaftStorageFiles.resolvePersistStorage(configuration));
    } catch (final RuntimeException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot resolve the runtime-join marker location, the runtime-join state of this peer will not survive a "
              + "restart: %s", e.toString());
      return new RuntimeJoinDetector();
    }
  }

  /**
   * Logs, rate-limited, why a cluster-token hop from {@code caller} was refused (issue #10601). No-op before the token
   * is resolved.
   */
  public void reportClusterTokenRefusal(final String caller) {
    final ClusterTokenProvider p = tokenProvider;
    if (p != null && p.getSource() != null)
      p.reportClusterTokenRefusal(caller, System.currentTimeMillis());
  }

  /**
   * Builds a fully wired {@link ArcadeStateMachine}. Both collaborators must be set: {@code setServer}
   * gives it the {@link ArcadeDBServer} and {@code setRaftHAServer} the owning {@code RaftHAServer}.
   * Missing the latter leaves {@code raftHAServer} null on the new machine, so the recovered node can
   * no longer install snapshots ({@code notifyInstallSnapshotFromLeader} NPEs), never marks
   * locally-originated transactions (origin-skip fails, the leader re-applies its own committed txns),
   * and silently skips leader-change handling (issue #4839). Used by both the startup constructor and
   * the {@link RatisInPlaceRestart#restartRatis(boolean)} recovery path so the two can never drift.
   */
  ArcadeStateMachine createStateMachine() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(arcadeServer);
    sm.setRaftHAServer(this);
    sm.setSchemaFullLoadRecorder(schemaFullLoadRecorder);
    sm.setRuntimeJoinDetector(runtimeJoinDetector);
    sm.setLocalDropVerbs(localDropVerbs);
    sm.shareRestoreHolds(databasesHeldForRestore);
    return sm;
  }

  /**
   * Holds {@code databaseName} for a restore that has just claimed it on this server (issue #10974), see
   * {@link BootstrapPasses#holdForRestore}: entries of other nodes for it are refused until {@link #releaseRestoreHold}, and
   * the ones this leader accepted before are waited for, at most {@link #CLIENT_REQUEST_TIMEOUT_MS} like a DDL.
   *
   * @throws NeedRetryException when entries accepted before the hold are still not applied once the wait gives up; the hold
   *                            stands, and the caller's release ends it
   */
  void holdForRestore(final String databaseName) {
    if (isLeader())
      restoreHoldTerms.put(databaseName, getCurrentTerm());
    final ArcadeStateMachine sm = stateMachine;
    if (sm == null) {
      // No machine to accept anything yet; the hold is in place for the one created later
      databasesHeldForRestore.add(databaseName);
      return;
    }
    if (!sm.holdForRestore(databaseName, CLIENT_REQUEST_TIMEOUT_MS))
      throw new NeedRetryException("Database '" + databaseName + "' still has transactions of other servers in flight on the "
          + "leader, so the restore cannot replace it safely right now. Please retry");
  }

  /**
   * Refuses a restore that replaces a database cluster-wide when the hold {@link #holdForRestore} took does not stand (review of
   * PR #10979): it was not taken by this node as the leader, or this node is not the leader in the term it took it in any more.
   * The hold refuses entries on this node only, so a leader elected meanwhile accepted the other nodes' writes, and the swap and
   * the snapshot the restore forces would destroy them. Asked right after the claim, so a restore that cannot hold is refused
   * before its download, and again right before the swap.
   * <p>
   * A restore that replaces a database cluster-wide has to run on the leader anyway: the snapshot it forces makes every other
   * node pull the LEADER's copy. A leadership that moves after the check made before the swap is caught by the leader the drop
   * and the install reach, through the term they are fenced to ({@link #restoreFenceTerm}).
   *
   * @throws NeedRetryException when the hold does not stand
   */
  void ensureRestoreHoldStands(final String databaseName) {
    final Long heldInTerm = restoreHoldTerms.get(databaseName);
    if (heldInTerm == null || !isLeader() || getCurrentTerm() != heldInTerm)
      throw new NeedRetryException("Database '" + databaseName + "' was not replaced: "
          + (heldInTerm == null ? "this server was not the leader of the cluster when the restore claimed it" :
          "the leadership of the cluster moved while it was being restored") + ", so the writes the leader accepted meanwhile "
          + "would be lost. Nothing was changed. Please retry the restore on the current leader");
  }

  /**
   * The term a drop or an install of {@code databaseName} is fenced to (issue #10974): the term in which this node, as the leader,
   * holds it for a restore, so the leader the entry reaches refuses it from another node or in another term. {@code -1} when no
   * restore holds it as the leader, or when a peer has not advertised {@link PeerCapabilities#RESTORE_FENCE}: a build without the
   * extension framing halts on the section, and the restore is then fenced by {@link #ensureRestoreHoldStands} only, and says so at
   * WARNING. Not refused: a restore is often the way out of an incident, and an upgrade in progress must not block it for a window
   * as narrow as the one between that check and the drop. The capability is asked now ({@link #peersMissingCapabilityNow}), not read
   * from the cache, which a leader elected moments ago has not filled yet.
   */
  long restoreFenceTerm(final String databaseName) {
    final Long heldInTerm = restoreHoldTerms.get(databaseName);
    if (heldInTerm == null)
      return -1L;
    // Asked now rather than read from the cache: a leader elected moments ago has not heard from its peers yet and would read every
    // one as missing, leaving the restore unfenced on a cluster that can read the section. One bounded round, once per drop or
    // install of a restore
    final List<String> missing = peersMissingCapabilityNow(PeerCapabilities.RESTORE_FENCE);
    // The probe can wait on the peers: a leadership that moved meanwhile refuses the entry here, before it is submitted with a term
    // the next leader refuses anyway, or unfenced
    ensureRestoreHoldStands(databaseName);
    if (!missing.isEmpty()) {
      // Once per drop or install of a restore: an operator running a mixed-version cluster is told which peers leave the restore
      // fenced by the leadership check made before the swap only
      LogManager.instance().log(this, Level.WARNING,
          "The restore of database '%s' replicates its drop and install without the leadership fence of issue #10974: peer(s) %s "
              + "have not advertised '%s'. A leadership change after the check made before the swap is not caught until every "
              + "node is upgraded", databaseName, missing, PeerCapabilities.RESTORE_FENCE);
      return -1L;
    }
    return heldInTerm;
  }

  /** Ends the hold {@link #holdForRestore} took; a database not held is left alone. */
  void releaseRestoreHold(final String databaseName) {
    restoreHoldTerms.remove(databaseName);
    databasesHeldForRestore.remove(databaseName);
  }

  /**
   * Full schema loads this node's apply thread ran in place of the incremental refresh, by reason (issue #10020):
   * published by the metrics as {@code arcadedb.ha.schema.full_load}.
   */
  public HAReplicationStatsProvider.SchemaFullLoadStats getSchemaFullLoadStats() {
    return schemaFullLoadRecorder.snapshot();
  }

  /** The current Ratis server. Package-private: tests that fault-inject the server itself (issue #8900). */
  RaftServer getRaftServerForTesting() {
    return raftServer;
  }

  /**
   * Package-private test hook. Next call to getRaftLifeCycleState() returns this value, then clears.
   */
  void forceRaftStateForTesting(final LifeCycle.State state) {
    this.forcedStateForTesting = state;
  }

  /**
   * Stops the Raft client and server, releasing all resources.
   * <p>
   * <b>Every step is guarded, so one throw cannot skip the ones after it</b> (issue #7985, review on PR #8026).
   * This method is reached through {@code RaftHAPlugin.stopService()}, which
   * {@code ArcadeDBServer.stopInternal()} calls inside {@code CodeUtils.executeIgnoringExceptions} - so a throw
   * from an early step would not reach a caller that could report it, and would silently take the three HTTP
   * client releases below with it: exactly the leak this method's releases exist to close, and exactly the
   * shape {@code HttpServer.stopService()} is guarded against for the same reason. None of the steps below
   * throws today ({@code HealthMonitor.stop()}, {@code RaftLogCompactionScheduler.stop()},
   * {@link #stopLagMonitor()} and {@link #stopCapabilityMonitor()} all guard-and-{@code shutdownNow()}), so
   * this guards an invariant rather than fixing a live bug - which is the point: the invariant is one a later
   * change to any of them would otherwise reopen without a test noticing.
   */
  public void stop() {
    shutdownRequested = true;
    final ApplyBackPressure backPressure = applyBackPressure;
    if (backPressure != null) {
      ApplyBackPressure.unregister(localPeerId.toString(), backPressure);
      applyBackPressure = null;
    }
    final Thread joinThread = autoJoinThread;
    if (joinThread != null) {
      // Wake the auto-join retry loop out of its backoff sleep so shutdown is not delayed (issue #5268).
      joinThread.interrupt();
      autoJoinThread = null;
    }
    if (healthMonitor != null) {
      CodeUtils.executeIgnoringExceptions(healthMonitor::stop, "Error on stopping the HA health monitor", true);
      healthMonitor = null;
    }
    // After the monitor: its thread is the only user of this client (issue #7619).
    CodeUtils.executeIgnoringExceptions(this::closeLeaderCommitProbeClient,
        "Error on closing the HA leader commit-index probe client", true);
    if (logCompactionScheduler != null) {
      CodeUtils.executeIgnoringExceptions(logCompactionScheduler::stop,
          "Error on stopping the Raft log compaction scheduler", true);
      logCompactionScheduler = null;
    }
    CodeUtils.executeIgnoringExceptions(this::stopLagMonitor, "Error on stopping the HA lag monitor", true);
    // The capability refresh runs in every role since issue #7549, so shutdown is the only thing that ends it;
    // it used to come along with stopLagMonitor() because it only ever ran on a leader.
    CodeUtils.executeIgnoringExceptions(capabilityMonitor::stopCapabilityMonitor,
        "Error on stopping the HA peer-capability monitor", true);
    // BEFORE the HTTP clients below, not after (issue #7739). Neither executor touches those clients - the
    // stalled-resync task dials the follower with HttpURLConnection (requestRemoteResync), and the
    // channel-recovery tasks reset a Ratis gRPC channel or transfer leadership - so this is not an ordering
    // constraint on client USE. What it buys is that the executors are told to stop before anything that can
    // block: however long the three releases below take, these threads are already unwinding rather than
    // waiting behind a close that has not returned.
    CodeUtils.executeIgnoringExceptions(stalledResyncExecutor::shutdownNow,
        "Error on stopping the HA stalled-replica resync executor", true);
    CodeUtils.executeIgnoringExceptions(channelRecoveryExecutor::shutdownNow,
        "Error on stopping the HA channel-recovery executor", true);
    CodeUtils.executeIgnoringExceptions(selfDialProbeExecutor::shutdownNow,
        "Error on stopping the HA self-dial probe executor", true);
    // After stopCapabilityMonitor() and the two shutdownNow() above: nothing asks for these clients past this
    // point, and an HttpClient left behind holds a connection pool and a selector thread for the life of the JVM
    // - which in the HA suites outlives many server start/stop cycles (PR #7314 review).
    //
    // Each of the three CAN BLOCK, and each is now bounded by LeaderDial.CLIENT_RELEASE_GRACE_MS rather than by
    // whatever the straggler it is waiting on carries (issue #7985). What they used to be bounded by:
    //   - capabilityHttpsClients: a probe round already told to stand down, PeerCapabilityRegistry.PROBE_TIMEOUT_MS;
    //   - forwardHttpsClients and forwardHttpClient: the FORWARD's own deadline - arcadedb.ha.proxyCommandTimeout,
    //     one hour at its default, or arcadedb.command.timeout when that is set. A single follower-to-leader
    //     write parked on a leader that accepted the connection and went silent held all of stop() for that long,
    //     so a rolling restart or a Kubernetes SIGTERM was SIGKILLed long before the Raft server was closed.
    // Releasing them asynchronously instead would hand the shutdown path a client that outlives the server it
    // belongs to, which is the leak these calls exist to prevent; abandoning an answer this node will never read
    // is the cost that is actually worth paying.
    //
    // The grace is per client, so the arithmetic worth stating: 3 x CLIENT_RELEASE_GRACE_MS = 15s of added
    // shutdown in the pathological case where all three have a straggler that does not unwind when cancelled.
    // The two caches build nothing at all unless this cluster dials an HTTPS peer, so on a plaintext cluster
    // the ceiling is one grace. Either way it is a ceiling and not a cost: each call returns the moment its
    // client reports itself terminated.
    CodeUtils.executeIgnoringExceptions(capabilityHttpsClients::close,
        "Error on releasing the HA peer-capability HTTPS client", true);
    CodeUtils.executeIgnoringExceptions(forwardHttpsClients::close,
        "Error on releasing the HA leader-forward HTTPS client", true);
    // Same leak this method already prevents for capabilityHttpsClients/forwardHttpsClients, for the plain-HTTP
    // forward client every RaftReplicatedDatabase this server wraps a database with shares (review finding on
    // PR #7650): a fresh RaftHAServer - and a fresh forwardHttpClient - is built on every
    // RaftHAPlugin.startService(), and an unclosed one outlives it.
    CodeUtils.executeIgnoringExceptions(() -> LeaderDial.releaseBounded(forwardHttpClient),
        "Error on releasing the HA shared plain-HTTP forward client", true);
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
        inPlaceRestart.closeRatisServerOnStop(raftServer);
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
   * Returns true if this server is the current Raft leader <em>and</em> Ratis considers it ready to serve.
   * A node that has just won an election reports the leader role immediately but rejects client requests
   * with the retryable {@code LeaderNotReadyException} until it has committed a no-op entry in its current
   * term and caught its state machine up, so {@link #isLeader()} alone must not be read as "can serve now".
   */
  public boolean isLeaderReady() {
    return getLeadershipState().leaderReady();
  }

  /**
   * The raw Raft role and Ratis' readiness signal, read from one division snapshot.
   *
   * @param leader      this node currently holds the LEADER role
   * @param leaderReady this node is the leader <em>and</em> can serve now
   */
  public record LeadershipState(boolean leader, boolean leaderReady) {
  }

  /**
   * Returns {@link #isLeader()} and {@link #isLeaderReady()} read from a single {@code getDivision(...)}
   * snapshot, so a status payload carrying both can never report the impossible pair
   * {@code leader=false, leaderReady=true} produced by a leadership change landing between two separate
   * reads. Degrades to {@code (false, false)} on an unreadable division, matching the single-flag getters.
   */
  public LeadershipState getLeadershipState() {
    if (raftServer == null)
      return new LeadershipState(false, false);

    try {
      final var info = raftServer.getDivision(raftGroup.getGroupId()).getInfo();
      return new LeadershipState(info.isLeader(), info.isLeaderReady());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error checking leadership state", e);
      return new LeadershipState(false, false);
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
   * Hands this leader's leadership to the best eligible peer (the ranking {@link StepDownTargets#selectStepDownTargets} gives
   * {@link #stepDown()}), with the targeted Ratis transfer: the chosen follower starts its election at once, and the
   * other followers learn the new leader from its first heartbeat. A leadership change also makes every server
   * recreate its internal gRPC log-appender channels, which resolves stale connections to restarted peers whose
   * gRPC channels are stuck in exponential backoff.
   * <p>
   * Only when no peer is eligible, or every targeted transfer fails, does it ask Ratis for a bare step-down, and it
   * then reports success only once a different peer is seen as the leader (issue #8480): the bare step-down starts
   * no election, so on its own it leaves the cluster leaderless for a whole election timeout.
   *
   * @param timeoutMs maximum time to wait for the transfer to complete
   *
   * @return true only when leadership settled on a peer other than this one; false when this node is not the leader
   *         or no handoff happened within {@code timeoutMs}
   */
  public boolean transferLeadership(final long timeoutMs) {
    return clusterManager.transferLeadership(timeoutMs);
  }

  /**
   * {@link #transferLeadership(long)} with the no-target step-down made optional (issue #8665): with
   * {@code bareStepDownFallback} false, a leader with no eligible peer returns false instead of stepping down with no
   * target, which Ratis answers with an election that frequently re-elects this node, or elects a peer no screen
   * here vetted.
   */
  public boolean transferLeadership(final long timeoutMs, final boolean bareStepDownFallback) {
    return clusterManager.transferLeadership(timeoutMs, bareStepDownFallback);
  }

  /**
   * Returns the HTTP address (host:port) of the current Raft leader, or {@code null} if the leader
   * is unknown. When the leader's HTTP port was not declared in the server list, the address is
   * derived as a best effort from the leader's Raft host plus this node's HTTP port (see
   * {@link PeerEndpoints#resolveHttpAddress(RaftPeerId)}).
   */
  public String getLeaderHttpAddress() {
    return endpoints.resolveHttpAddress(getLeaderId());
  }

  /**
   * The leader as a refusal should name it (issues #10060, #10066): its HTTP address, unless that is this node's own, which is what a
   * cluster whose peers share a host and declare no HTTP port resolves for every peer (issue #6191) and would send the client back
   * to the node that is refusing it; then its server name, as the forwarding refusals name it. {@code null} when no leader is known,
   * which keeps the refusal the retryable one an election in flight gets.
   */
  public String getLeaderForRefusal() {
    final String address = getLeaderHttpAddress();
    if (address == null || address.isBlank())
      return null;
    return isOwnHttpAddress(address) ? getLeaderName() : address;
  }

  /**
   * The HTTPS endpoint (host:port) of the current Raft leader, for a caller that would otherwise dial its
   * plain-HTTP one; {@code null} when there is nothing better to dial than that (issue #7508).
   * <p>
   * Three ways to answer {@code null}, and none of them is a failure - the caller falls back to
   * {@link #getLeaderHttpAddress()}, the listener that is always bound:
   * <ul>
   * <li>SSL is off, so there is no HTTPS listener anywhere in the cluster to dial;</li>
   * <li>no HTTPS endpoint resolves for the leader ({@link PeerEndpoints#resolveHttpsAddress} answers {@code null} when the
   * 5th field of {@code arcadedb.ha.serverList} is absent and this node has no HTTPS port to derive one from);</li>
   * <li>the one that resolves is this node's own. The HTTP twin leaves that check to its callers, which run
   * {@link #isOwnHttpAddress}; that method speaks for the HTTP listener and cannot answer for an HTTPS endpoint,
   * so this one makes the check itself rather than handing out an address no caller can vet.</li>
   * </ul>
   * Resolved through the plain {@link PeerEndpoints#resolveHttpsAddress} rather than through
   * {@link #getUnambiguousPeerHttpsAddress}, mirroring the HTTP twin exactly: an address that names the wrong
   * node is caught by the receiving node's one-hop refusal, since the forward carries
   * {@code LeaderForwardContext.FORWARDED_TO_LEADER_HEADER} whichever scheme it travelled on (issue #6191).
   */
  public String getLeaderHttpsAddress() {
    return preferredLeaderHttpsAddress(configuration.getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL),
        endpoints.resolveHttpsAddress(getLeaderId()), getLocalHttpsAddress());
  }

  /**
   * The decision {@link #getLeaderHttpsAddress()} makes, without the resolver behind it. Package-private and pure
   * so the three ways it answers {@code null} are unit-testable without a Raft group (issue #7508).
   */
  static String preferredLeaderHttpsAddress(final boolean useSSL, final String leaderHttpsAddress,
      final String localHttpsAddress) {
    if (!useSSL || leaderHttpsAddress == null)
      return null;
    return localHttpsAddress != null && isSameHttpEndpoint(localHttpsAddress, leaderHttpsAddress)
        ? null : leaderHttpsAddress;
  }

  public RaftClient getClient() {
    return raftClient;
  }

  public RaftTransactionBroker getTransactionBroker() {
    return transactionBroker;
  }

  /**
   * {@link #stop()} clears {@code transactionBroker} while callers holding this instance - HTTP writes, admin
   * requests, leader-change notifications - can still reach it (issue #8356). Static so a Mockito double of this
   * class still runs the check against its stubbed {@link #getTransactionBroker()}.
   */
  static RaftTransactionBroker requireTransactionBroker(final RaftHAServer raft) {
    final RaftTransactionBroker broker = raft.getTransactionBroker();
    if (broker == null)
      throw new NeedRetryException("Raft transaction broker is not available (server may be stopping)");
    return broker;
  }

  /**
   * Replaces the current RaftClient with a new one with fresh gRPC channels, and closes the old one off the calling
   * thread.
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

    raftClient = buildRaftClient(raftGroup, raftProperties, raftParameters, knownLeaderId);

    if (transactionBroker != null) {
      final int batchSize = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_BATCH_SIZE);
      final int queueSize = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_QUEUE_SIZE);
      final int offerTimeout = configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_OFFER_TIMEOUT);
      // #4743: the effective per-entry ceiling is the SMALLER of the gRPC frame cap and the Ratis
      // appender byte limit - an entry above the latter makes the leader step down.
      final long grpcMessageSizeMax = RaftPropertiesBuilder.maxReplicatedEntrySize(configuration);
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

    if (oldClient != null)
      closeRetiredClient(oldClient);

    LogManager.instance().log(this, Level.INFO, "RaftClient refreshed with fresh gRPC channels after leader change");
  }

  /**
   * Closes a client {@link #refreshRaftClient} has just replaced, on a thread of its own (issue #12142).
   * <p>
   * The refresh runs from {@code notifyLeaderChanged}, which Ratis calls inside {@code ServerState.setLeader} with the
   * division's monitor held: from the synchronized {@code changeToLeader()} on a new leader, and from the synchronized
   * block of {@code appendEntries} on a follower. Closing a Ratis gRPC client shuts each channel down gracefully and
   * waits up to 3 s for the calls still in flight on it (then up to 2 s more after {@code shutdownNow}), and under load
   * there always are some: group-commit appends and watches, the ReadIndex of a LINEARIZABLE follower read. Closed
   * inline, that wait held the division for 3 s on every such leader change: the new leader sent no heartbeat and
   * committed nothing, and a follower answered no append or vote.
   * <p>
   * Nothing changes for those calls: {@code RaftClientImpl.close()} marks the client closed before it shuts the
   * channels down, so a caller still holding it sees it closed at once, and the calls in flight still either finish
   * within the graceful window or are cancelled. Only the wait has left the Raft thread. A virtual thread, as the group
   * committer's refresh callback uses: a retired client is rare (one per leader change) and the wait parks on the
   * channel's {@code CountDownLatch}, which does not pin a carrier.
   */
  private void closeRetiredClient(final RaftClient retired) {
    Thread.ofVirtual().name("arcadedb-raft-client-close").start(() -> {
      try {
        retired.close();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Error closing stale RaftClient during refresh", e);
      }
    });
  }

  /**
   * Whether this node was added to the Raft configuration by a change it applied while running, rather than being
   * a member from the first configuration it observed (issue #7819). See {@link RuntimeJoinDetector}.
   */
  public boolean hasJoinedClusterAtRuntime() {
    return runtimeJoinDetector.hasJoinedAtRuntime();
  }

  /**
   * The security documents not installed from an entry after the configuration that (last) added this node
   * (issue #8317). See {@link RuntimeJoinDetector#securityDocumentsNotInstalledSinceJoin()}.
   */
  public List<String> securityDocumentsNotInstalledSinceRuntimeJoin() {
    return runtimeJoinDetector.securityDocumentsNotInstalledSinceJoin();
  }

  /**
   * The log index of the configuration that (last) added this node, or the join boundary a snapshot install moved
   * it to; {@code -1} when none is known (issue #8414). See {@link RuntimeJoinDetector#joinIndex()}.
   */
  public long getRuntimeJoinIndex() {
    return runtimeJoinDetector.joinIndex();
  }

  /**
   * On a node that did not join at runtime, the security documents not confirmed since its latest leader-driven
   * snapshot install (issue #8432). See {@link RuntimeJoinDetector#securityDocumentsNotConfirmedSinceSnapshotInstall()}.
   */
  public List<String> securityDocumentsNotConfirmedSinceSnapshotInstall() {
    return runtimeJoinDetector.securityDocumentsNotConfirmedSinceSnapshotInstall();
  }

  /**
   * The index of the latest leader-driven snapshot install on this node while unarmed, {@code -1} when none (issue
   * #8432). See {@link RuntimeJoinDetector#lastSnapshotInstallIndex()}.
   */
  public long getLastSnapshotInstallIndex() {
    return runtimeJoinDetector.lastSnapshotInstallIndex();
  }

  /**
   * Records that the leader found this node's security documents, read at {@code appliedIndex}, equal to its own
   * (issue #8346). See {@link RuntimeJoinDetector#onSecurityDocumentsMatchedLeader(long)}.
   */
  void onSecurityDocumentsMatchedLeader(final long appliedIndex) {
    runtimeJoinDetector.onSecurityDocumentsMatchedLeader(appliedIndex);
  }

  public ArcadeStateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * Runs a commit published outside the log (issue #11921) so that no in-place restart replaces the state machine while it
   * runs: the publication and every validation it is ordered against then use one ledger. {@code publication} resolves the
   * state machine itself, under the gate.
   */
  <T> T runPublication(final Callable<T> publication) throws Exception {
    final Lock gate = publicationGate.readLock();
    gate.lock();
    try {
      return publication.call();
    } finally {
      gate.unlock();
    }
  }

  /**
   * Builds the state machine of an in-place restart once the publications in flight on the old one have returned (issue
   * #11921), waiting up to {@code waitMs} ({@code arcadedb.ha.ratisCloseTimeoutMs} for {@link RatisInPlaceRestart}): one that
   * is still running after that fails the restart, which counts toward {@code arcadedb.ha.ratisRestartMaxRetries} like a
   * close that does not finish.
   */
  void replaceStateMachineBetweenPublications(final long waitMs) throws IOException, InterruptedException {
    final Lock gate = publicationGate.writeLock();
    lockPublicationGateForReplacement(gate, waitMs);
    try {
      this.stateMachine = createStateMachine();
    } finally {
      gate.unlock();
    }
  }

  /**
   * Takes the publication gate for a state machine replacement, waiting up to {@code waitMs} ({@code 0} or less: no bound).
   *
   * @throws IOException          when a publication is still running after {@code waitMs}
   * @throws InterruptedException when the wait is interrupted, with the thread's interrupt flag set again
   */
  static void lockPublicationGateForReplacement(final Lock gate, final long waitMs) throws IOException, InterruptedException {
    final boolean locked;
    try {
      if (waitMs <= 0) {
        gate.lockInterruptibly();
        locked = true;
      } else
        locked = gate.tryLock(waitMs, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      // The throw cleared the flag, and restartRatis reads the flag to tell an abandoned restart from a failed one: without
      // it an interrupted restart would count toward arcadedb.ha.ratisRestartMaxRetries
      Thread.currentThread().interrupt();
      throw e;
    }
    if (!locked)
      throw new IOException("A commit published outside the Raft log was still running after " + waitMs
          + "ms; not replacing the state machine under it (issue #11921)");
  }

  /** Whether {@code thread} waits at the publication gate (issue #11921), for tests that must observe the restart there. */
  boolean isWaitingAtThePublicationGate(final Thread thread) {
    return publicationGate.hasQueuedThread(thread);
  }

  /**
   * Called by the database wrapper for every database the server opens (issue #11518), so a copy found damaged by an open
   * after the startup scan is quarantined like one found by the scan. Before the Raft server is built, the scan in
   * {@link #start()} covers the databases already open, and a node shutting down quarantines nothing.
   */
  void recheckOpenedDatabase(final String databaseName, final DatabaseInternal database) {
    final ArcadeStateMachine sm = getStateMachine();
    if (raftServer == null || shutdownRequested || sm == null)
      return;
    try {
      sm.recheckOpenedDatabase(databaseName, database);
    } catch (final RuntimeException e) {
      // The open itself must not fail for the check: the database is served either way, which is what it was before
      LogManager.instance().log(this, Level.WARNING, "Cannot check the local copy of database '%s' after its open: %s", e,
          databaseName, e.getMessage());
    }
  }

  /**
   * Called by the database wrapper for every database the server opens (issue #11555): on the leader, the open recovers the
   * per-record materialized views the leadership hook could not, because the database was not open when it ran. Before the Raft
   * server is built no node leads, and the leadership hook covers every database open by then.
   */
  void recoverMaterializedViewsOfOpenedDatabase(final String databaseName, final DatabaseInternal database) {
    final ArcadeStateMachine sm = getStateMachine();
    if (raftServer == null || shutdownRequested || sm == null)
      return;
    try {
      sm.recoverMaterializedViewsOfOpenedDatabase(databaseName, database);
    } catch (final RuntimeException e) {
      // The open itself must not fail for the recovery: REFRESH MATERIALIZED VIEW is the workaround the warning names
      LogManager.instance().log(this, Level.WARNING,
          "Cannot schedule the materialized view recovery of database '%s' after its open: run REFRESH MATERIALIZED VIEW on its "
              + "per-record views: %s", e, databaseName, e.getMessage());
    }
  }

  /** The bootstrap election, or {@code null} before the Raft server has started. */
  BootstrapElection getBootstrapElection() {
    return bootstrapElection;
  }

  /** What the leader asks its peers before it reopens a closed copy a resync could not verify (issue #8605). */
  UnverifiedClosedCopyCheck getUnverifiedClosedCopyCheck() {
    return unverifiedClosedCopyCheck;
  }

  /** The drops this node's own {@code drop database} verb is waiting on; see {@link LocalDropVerbs}. */
  LocalDropVerbs getLocalDropVerbs() {
    return localDropVerbs;
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

  /**
   * The live map of explicitly declared peer HTTP endpoints, keyed by peer id - not a copy: callers add an
   * entry when a peer joins and drop it when one leaves. It is a {@link ConcurrentHashMap}, so writing to it
   * while other threads resolve addresses is safe, but it rejects null keys and values.
   */
  public Map<RaftPeerId, String> getHttpAddresses() {
    return httpAddresses;
  }

  /**
   * The live map of explicitly declared peer HTTPS endpoints - the optional 5th field of
   * {@link GlobalConfiguration#HA_SERVER_LIST} - keyed by peer id, and not a copy, exactly like
   * {@link #getHttpAddresses()}. Empty when no entry declared one, in which case
   * {@link #getPeerHttpsAddress} derives the endpoint instead.
   * <p>
   * Package-private, unlike its plain-HTTP twin: the only caller is the SSL cluster fixture patching in the
   * ports each node actually bound, and nothing outside this package reads either map today
   * ({@code grep -rn 'getHttpAddresses()'} finds no hit outside {@code com.arcadedb.server.ha.raft}), so there
   * is no reason to widen the public surface for a second one (code review on PR #7838). Read an endpoint
   * through {@link #getPeerHttpsAddress} or {@link PeerDialAddress} instead, which apply the guards this raw
   * map has none of.
   */
  Map<RaftPeerId, String> getHttpsAddresses() {
    return httpsAddresses;
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

  /**
   * The peers {@code arcadedb.ha.serverList} declared, as read once at startup - NOT the live Raft
   * configuration (see {@link #configuredPeers()} for that). One of the few legitimate direct readers of the
   * static group: {@link RaftClusterStatusExporter} reconciles the two lists itself to decide which declared
   * peers are still pending a join (issue #7136).
   */
  Collection<RaftPeer> getDeclaredPeers() {
    return raftGroup.getPeers();
  }

  public String getLeaderName() {
    final RaftPeerId leaderId = getLeaderId();
    if (leaderId == null)
      return null;
    final String display = peerDisplayNames.get(leaderId);
    return display != null ? display : leaderId.toString();
  }

  /**
   * The peers every "who are the members" consumer reports: the declared server list reconciled against the live
   * Raft configuration (issue #7040). A peer the configuration dropped is not a replica - the leader does not
   * replicate to it and a client must not be routed to it - and a peer added at runtime is one even though the
   * server list does not declare it. {@link #getStats()}, {@link #getReplicaAddresses()} and
   * {@link #routingTableFor} all read this, so the three views cannot disagree about membership.
   */
  List<RaftPeer> configuredPeers() {
    return ClusterMembership.of(raftGroup.getPeers(), getLivePeers()).configuredPeers();
  }

  public Map<String, Object> getStats() {
    final Map<String, Object> stats = new HashMap<>();
    stats.put("localPeerId", localPeerId.toString());
    final LeadershipState leadership = getLeadershipState();
    stats.put("isLeader", leadership.leader());
    stats.put("leaderReady", leadership.leaderReady());
    stats.put("configuredServers", getConfiguredServers());

    if (clusterMonitor != null) {
      final Map<String, Long> lags = clusterMonitor.getReplicaLags();
      if (!lags.isEmpty())
        stats.put("replicaLags", lags);
    }

    final RaftPeerId statsLeaderId = getLeaderId();
    final RaftPeerId statsExcludeId = statsLeaderId != null ? statsLeaderId : localPeerId;
    final List<Map<String, String>> replicas = new ArrayList<>();
    for (final RaftPeer peer : configuredPeers()) {
      if (!peer.getId().equals(statsExcludeId)) {
        final Map<String, String> replicaInfo = new HashMap<>();
        replicaInfo.put("id", peer.getId().toString());
        replicaInfo.put("address", peer.getAddress());
        final String httpAddr = endpoints.resolveHttpAddress(peer);
        if (httpAddr != null)
          replicaInfo.put("httpAddress", httpAddr);
        replicas.add(replicaInfo);
      }
    }
    stats.put("replicas", replicas);
    return stats;
  }

  // PeerEndpoints holds the implementation of these; they stay here for the callers outside this class.

  public String getReplicaAddresses() {
    return endpoints.getReplicaAddresses();
  }

  public HAServerPlugin.RoutingTable getRoutingTable(final HAServerPlugin.ROUTING_PROTOCOL protocol) {
    return endpoints.getRoutingTable(protocol);
  }

  HAServerPlugin.RoutingTable routingTableFor(final HAServerPlugin.ROUTING_PROTOCOL protocol, final RaftPeerId leaderId) {
    return endpoints.routingTableFor(protocol, leaderId);
  }

  public Map<RaftPeerId, PeerHttpEndpoint> getPeerHttpEndpoints() {
    return endpoints.getPeerHttpEndpoints();
  }

  void warnAmbiguousPeerAddress(final boolean https, final String[] addresses, final boolean[] fromConfig,
      final RaftPeerId[] owners, final int count, final Map<String, int[]> claims) {
    endpoints.warnAmbiguousPeerAddress(https, addresses, fromConfig, owners, count, claims);
  }

  void warnAmbiguousRouting(final HAServerPlugin.ROUTING_PROTOCOL protocol, final String[] addresses,
      final boolean[] fromConfig, final RaftPeerId[] owners, final int count, final Map<String, int[]> claims) {
    endpoints.warnAmbiguousRouting(protocol, addresses, fromConfig, owners, count, claims);
  }

  public String getPortOffsetPeerHttpAddress(final RaftPeerId peerId) {
    return endpoints.getPortOffsetPeerHttpAddress(peerId);
  }

  /**
   * The HTTP address of {@code peerId}, but only when it identifies that peer and no other; {@code null}
   * otherwise. The same question {@link PeerEndpoints#selectUnambiguousRouting} asks of a client routing table, asked of the
   * peer-to-peer HTTP endpoint (issue #6202).
   * <p>
   * With no {@code http} port declared in {@link GlobalConfiguration#HA_SERVER_LIST}, a peer's HTTP address is
   * derived as its Raft host plus <em>this</em> node's port (see {@link PeerEndpoints#resolveHttpAddress(RaftPeerId)}), so on a
   * cluster whose nodes differ by port rather than by host every peer collapses onto one address. Two peers can
   * never legitimately answer on one {@code host:port} - they would be fighting over the socket - so an address
   * two of them resolve to identifies at most one, and the resolver has no way to say which. A declared address
   * outranks a derived one when they collide: the operator stated it, and the collision only proves the guess
   * wrong.
   * <p>
   * The plain {@link #getPeerHttpAddress(RaftPeerId)} deliberately keeps returning the best-effort address for
   * cluster reporting and for a caller that merely displays it. This variant is for the callers that act on the
   * address unattended, where a wrong-but-plausible peer is worse than no peer at all: a follower that reconciles
   * its databases from a node that is not the leader reports success and returns to the ready set carrying
   * whatever it copied.
   */
  public String getUnambiguousPeerHttpAddress(final RaftPeerId peerId) {
    return endpoints.unambiguousPeerAddress(peerId, false);
  }

  /**
   * The HTTPS address of {@code peerId}, but only when it identifies that peer and no other; {@code null}
   * otherwise. The HTTPS twin of {@link #getUnambiguousPeerHttpAddress}, and not a formality: the two endpoints
   * are read from two independent maps - the 3rd field of {@link GlobalConfiguration#HA_SERVER_LIST} against its
   * 5th - each with its own derive fallback onto <em>this</em> node's port for that protocol. A cluster that
   * declares distinct {@code http} ports and omits the {@code https} one therefore passes the HTTP check and
   * still collapses every peer's HTTPS endpoint onto this node's own, so the HTTP verdict cannot stand in for
   * this one (issue #6221).
   * <p>
   * Returns {@code null} when SSL is disabled or no HTTPS listener is up, exactly as
   * {@link #getPeerHttpsAddress} does: a caller that cannot resolve an HTTPS endpoint falls back to the plain
   * HTTP one, which is the always-present listener, and a withheld address is meant to take the same route as an
   * unknown one.
   */
  public String getUnambiguousPeerHttpsAddress(final RaftPeerId peerId) {
    return endpoints.unambiguousPeerAddress(peerId, true);
  }

  /** A peer's HTTP endpoint as this node resolves it, and whether that endpoint identifies that peer alone. */
  public record PeerHttpEndpoint(String address, boolean ambiguous) {
  }

  /** One "already warned" latch per client protocol, so a derived Bolt address does not mute the gRPC warning. */
  static Map<HAServerPlugin.ROUTING_PROTOCOL, AtomicBoolean> createRoutingProtocolLatches() {
    final Map<HAServerPlugin.ROUTING_PROTOCOL, AtomicBoolean> latches = new EnumMap<>(
        HAServerPlugin.ROUTING_PROTOCOL.class);
    for (final HAServerPlugin.ROUTING_PROTOCOL protocol : HAServerPlugin.ROUTING_PROTOCOL.values())
      latches.put(protocol, new AtomicBoolean(false));
    return latches;
  }

  /** One remembered ambiguity verdict per client protocol, for the same reason the latches above are per protocol. */
  static Map<HAServerPlugin.ROUTING_PROTOCOL, AtomicReference<String>> createRoutingProtocolVerdicts() {
    final Map<HAServerPlugin.ROUTING_PROTOCOL, AtomicReference<String>> verdicts = new EnumMap<>(
        HAServerPlugin.ROUTING_PROTOCOL.class);
    for (final HAServerPlugin.ROUTING_PROTOCOL protocol : HAServerPlugin.ROUTING_PROTOCOL.values())
      verdicts.put(protocol, new AtomicReference<>());
    return verdicts;
  }

  /**
   * Returns the Raft address (host:port) of a peer, or {@code null} if this node knows no such peer. The live Raft
   * configuration is asked first, then the server list: a peer that joined at runtime (the Kubernetes auto-join,
   * {@code addPeer}) is in the first only, and every address handed to a client or an operator derives from this - the
   * Bolt/gRPC routing table, the HTTP and HTTPS endpoints, the capability probe - so a joined peer was replicated to and
   * never routed to (issue #10861). A declared peer the configuration dropped still resolves through the second, which
   * the step-down and handoff paths rely on (issue #7040). The same precedence {@link ClusterMembership#knownPeers} gives
   * an id present in both: the address the cluster committed, not the one declared.
   */
  String peerRaftAddress(final RaftPeerId peerId) {
    for (final RaftPeer peer : getLivePeers())
      if (peer.getId().equals(peerId))
        return peer.getAddress();
    for (final RaftPeer peer : raftGroup.getPeers())
      if (peer.getId().equals(peerId))
        return peer.getAddress();
    return null;
  }

  /**
   * Every peer this node knows: the server list reconciled with the live configuration ({@link ClusterMembership#knownPeers}).
   * The view of a caller that reports, disambiguates or fans out to the addresses of members AND of declared peers that are
   * not (or no longer) members, which a peer that joined at runtime must be part of (issue #10861): the server list alone
   * left such a peer out of the cluster status, the routing table, a logout's token revocation and a cross-node verify.
   */
  List<RaftPeer> knownPeers() {
    // Through the accessors, like the loops this replaced (getRaftGroup().getPeers()), so a node built for a test answers the
    // same group here as everywhere else
    return ClusterMembership.knownPeers(getRaftGroup().getPeers(), getLivePeers());
  }

  public RaftPeerId getLocalPeerId() {
    return localPeerId;
  }

  /**
   * Whether this node is the only voter of the live Raft configuration: there is no peer to hand the leadership to and
   * none to install a database from (issue #8940). Non-voting listeners are not counted, so a single voter with
   * listeners is a sole voter. When the live configuration cannot be read this falls back to the declared server list,
   * which may name non-voting peers: a declared multi-node list answers false, the safe side. During a membership
   * change that leaves this node as the only committed voter it answers true. Its callers are the two leader-side
   * quarantine producers, the forceSnapshot replay guard (#8940) and the unexpected-apply-error handler (#9308), both
   * of which alert instead of raising a quarantine that could never lift.
   */
  public boolean isSoleVoter() {
    return isSoleVoter(getLivePeers(), localPeerId);
  }

  // @VisibleForTesting
  static boolean isSoleVoter(final Collection<RaftPeer> voters, final RaftPeerId localPeerId) {
    return voters.size() == 1 && voters.iterator().next().getId().equals(localPeerId);
  }

  // VoterQuarantineCheck holds the implementation of these; they stay here for the callers outside this class.

  public List<String> getDatabasesQuarantinedOnEveryVoter() {
    return voterQuarantine.getDatabasesQuarantinedOnEveryVoter();
  }

  public boolean isQuarantinedOnEveryVoter(final String databaseName) {
    return voterQuarantine.isQuarantinedOnEveryVoter(databaseName);
  }

  public String refuseUnlessNoOtherVoterServesNow(final String databaseName) {
    return voterQuarantine.refuseUnlessNoOtherVoterServesNow(databaseName);
  }

  public Collection<RaftPeer> getLivePeers() {
    final Collection<RaftPeer> live = getCommittedPeersOrNull();
    return live != null ? live : raftGroup.getPeers();
  }

  /**
   * The peers of the live Raft configuration, or {@code null} when it cannot be read - before the Raft server
   * starts, or while an in-place restart re-initializes the division (issue #5271).
   * <p>
   * {@link #getLivePeers()} substitutes the declared list in that case, which is the right answer for a caller
   * that just needs a peer list. It is the wrong answer for a caller that must not mistake the declared list for
   * a committed membership: {@link RaftClusterStatusExporter} records which peers have ever been committed, and
   * folding the declared list into that record would permanently silence the convergence note (issue #7136).
   * Such a caller reads this and treats {@code null} as "no information this tick".
   */
  Collection<RaftPeer> getCommittedPeersOrNull() {
    return readCommittedPeers(Level.WARNING);
  }

  /**
   * {@link #getCommittedPeersOrNull()} reporting an unreadable configuration at {@code failureLevel}. A caller that reads
   * on every refusal of an inbound RPC, and so would turn one restart window into a stack trace per heartbeat, passes a
   * level below WARNING; everyone else wants the WARNING (see the comment on the catch).
   */
  private Collection<RaftPeer> readCommittedPeers(final Level failureLevel) {
    if (raftServer != null) {
      try {
        final var division = raftServer.getDivision(raftGroup.getGroupId());
        final var conf = division.getRaftConf();
        if (conf != null) {
          final Collection<RaftPeer> peers = conf.getCurrentPeers();
          // Every successful read keeps the misdirected-request listener's copy current (issue #10144)
          misdirected.rememberLiveMembers(peers, conf.getPreviousPeers());
          return peers;
        }
      } catch (final Exception e) {
        // Catch Exception, not IOException: Ratis throws IllegalStateException ("stateMachineUpdater is
        // uninitialized") for the whole window in which an in-place restart re-initializes the division
        // (issue #5271), which is precisely the window this fallback exists for. Narrowing it to IOException
        // made every membership reader behind configuredPeers() - /api/v1/cluster, getReplicaAddresses(), the
        // Bolt/gRPC routing table - propagate instead of degrading, and aborted the health-monitor tick that
        // drives the restart (issue #7135). Membership reads must degrade, never propagate.
        // WARNING, matching the sibling guards (isLeader, getLeaderId) rather than the FINE this used to log:
        // now that the catch is wide enough to swallow a genuine bug and not only the documented Ratis
        // IllegalStateException, a silent fallback to "no membership" is how such a bug would hide.
        LogManager.instance().log(this, failureLevel,
            "Cannot read the live Raft configuration this tick; returning no membership, and it is the caller's "
                + "choice whether to substitute the declared server list", e);
      }
    }
    return null;
  }

  /**
   * True only when this server's own division holds a Raft configuration that lists peers and this server is not among
   * them: it was removed from the group ({@code leaveCluster()}, {@code DELETE /api/v1/cluster/peer/{id}}) and is still
   * running (issue #9510).
   * <p>
   * Ratis does not close the division of a removed peer. Its state machine stays open and simply stops receiving
   * appends, while its Raft client still reaches the leader, which accepts a request from a non-member: an entry this
   * server submits commits cluster-wide and is never applied here. Writers ask this before submitting anything.
   * <p>
   * The <em>current</em> configuration is read, as {@link #isReadyForTraffic} reads it: during a joint-consensus change
   * that is the new configuration, so a leaving node answers true from the moment it has seen its own removal appended.
   * Every unknown answers false - no Raft server, a requested shutdown, a configuration that cannot be read during an
   * in-place restart (issue #5271), an empty one - so this never refuses on missing information; the callers' other
   * guards (a closed state machine, {@code requireRaftServer()}) cover those states. Read on every commit, so a read
   * failure logs at FINE only: a configuration that cannot be read is already reported by the membership readers.
   * <p>
   * A node being admitted can answer true for a moment: while it catches up on the leader's log it holds the older
   * configurations that precede its own admission. It cannot apply a write of its own before that either, so the
   * retryable refusal is the right answer there too.
   */
  public boolean isRemovedFromConfiguration() {
    final RaftServer server = raftServer;
    if (server == null || shutdownRequested)
      return false;
    try {
      final var conf = server.getDivision(raftGroup.getGroupId()).getRaftConf();
      if (conf == null)
        return false;
      final Collection<RaftPeer> peers = conf.getCurrentPeers();
      return !peers.isEmpty() && !isPeerInConfig(peers, localPeerId);
    } catch (final Exception e) {
      // FINE, not WARNING: read on every commit, and the membership readers already warn about an unreadable conf.
      LogManager.instance().log(this, Level.FINE, "Cannot read the Raft configuration for the membership check; not refusing", e);
      return false;
    }
  }

  // PeerAdmission holds the implementation of these; they stay here for the callers outside this class.

  boolean recordAdmittedPeerHttpAddress(final RaftPeerId peerId, final String httpAddress) {
    return admission.recordAdmittedPeerHttpAddress(peerId, httpAddress);
  }

  public void addPeer(final String peerId, final String address) {
    admission.addPeer(peerId, address);
  }

  public void addPeer(final String peerId, final String address, final String name) {
    admission.addPeer(peerId, address, name);
  }

  void addPeer(final RaftPeer newPeer, final String name) {
    admission.addPeer(newPeer, name);
  }

  void addPeer(final RaftPeer newPeer, final String name, final String declaredHttpAddress) {
    admission.addPeer(newPeer, name, declaredHttpAddress);
  }

  public void removePeer(final String peerId) {
    admission.removePeer(peerId);
  }

  public void removePeer(final String peerId, final boolean force) {
    admission.removePeer(peerId, force);
  }

  void registerPeerDisplayName(final RaftPeerId peerId, final String name) {
    admission.registerPeerDisplayName(peerId, name);
  }

  /**
   * Whether this node's {@link HealthMonitor} has escalated a crash loop and given up automatically
   * restarting (issue #7622): see {@link HealthMonitor#isCrashLoopEscalated()}. {@code false} when there is
   * no monitor - HA not yet started, or already stopped.
   */
  public boolean isCrashLoopEscalated() {
    final HealthMonitor monitor = healthMonitor;
    return monitor != null && monitor.isCrashLoopEscalated();
  }

  /**
   * Whether a process restart is still worth asking for after a crash-loop escalation (issue #7736): see
   * {@link HealthMonitor#isCrashLoopRestartPending()}. {@code false} when there is no monitor.
   */
  public boolean isCrashLoopRestartPending() {
    final HealthMonitor monitor = healthMonitor;
    return monitor != null && monitor.isCrashLoopRestartPending();
  }

  @Override
  public boolean hasPersistedCrashLoopEscalation() {
    return storageFiles.crashLoopEscalationMarker().isFile();
  }

  /**
   * Writes and fsyncs the crash-loop escalation record (issue #7736). Only its existence is read back; the content
   * is the SEVERE message, for an operator who finds the file. Returns {@code false} - and the node then does not
   * ask for a restart - when it cannot be written, e.g. on the full volume issue #7037 is about.
   */
  @Override
  public boolean persistCrashLoopEscalation(final String reason) {
    final File marker = storageFiles.crashLoopEscalationMarker();
    try {
      final File dir = marker.getParentFile();
      if (!dir.isDirectory() && !dir.mkdirs() && !dir.isDirectory())
        throw new IOException("Cannot create directory " + dir.getAbsolutePath());
      try (final FileOutputStream out = new FileOutputStream(marker)) {
        out.write((Instant.now() + " " + reason + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
        FileUtils.sync(out.getFD());
      }
      return true;
    } catch (final IOException | RuntimeException e) {
      LogManager.instance().log(this, Level.SEVERE, "Cannot record the crash-loop escalation in '%s' (issue #7736)", e,
          marker.getAbsolutePath());
      return false;
    }
  }

  @Override
  public void clearPersistedCrashLoopEscalation() {
    final File marker = storageFiles.crashLoopEscalationMarker();
    if (!marker.exists())
      return;
    if (marker.delete() || !marker.exists())
      LogManager.instance().log(this, Level.INFO, "Raft layer is healthy again: crash-loop escalation record cleared");
    else
      LogManager.instance().log(this, Level.WARNING,
          "Cannot delete the crash-loop escalation record '%s': delete it by hand, or the next start of this server "
              + "will not re-arm the automatic crash-loop recovery (issue #7736)", marker.getAbsolutePath());
  }

  /**
   * The connect-timeout-bounded client every {@link RaftReplicatedDatabase} this node wraps a database with
   * dials the leader on - one per node, not one per database (review finding on PR #7650): see the field
   * javadoc on {@link #forwardHttpClient}.
   */
  HttpClient getForwardHttpClient() {
    return forwardHttpClient;
  }

  // TrafficReadiness holds the implementation of these; they stay here for the callers outside this class.

  public boolean isReadyForTraffic(final long maxLagEntries) {
    return readiness.isReadyForTraffic(maxLagEntries);
  }

  /**
   * The commit index this follower's leader last reported for itself, or {@code -1} when none has been learned
   * (issue #7619). See {@link #refreshLeaderCommitIndex()}.
   */
  long getLeaderReportedCommitIndex() {
    return leaderReportedCommitIndex;
  }

  /**
   * The commit index a FOLLOWER measures its own lag against (issue #8321): the larger of its own commit index and the
   * one its leader last reported. Ratis clamps a follower's commit index to the entries it holds, so on a follower
   * whose inbound replication channel is wedged the local figure stops where the channel stopped and the lag computed
   * from it reads 0; the leader's figure, learned over the health monitor's follower-to-leader probe, does not. Every
   * figure that probe keeps is a commit index a leader really reported, so the result never over-states the lag.
   * <p>
   * The local commit index, unchanged, on the leader - whose own figure is the cluster's - and whenever the local
   * figure cannot be read ({@code -1}), so a reader that treats a negative index as "unknown" still sees it as such.
   */
  long getFollowerCommitIndex() {
    return followerCommitIndex(getCommitIndex(), isLeader() ? -1L : leaderReportedCommitIndex);
  }

  /** Pure form of {@link #getFollowerCommitIndex()}. Package-private for testing. */
  static long followerCommitIndex(final long localCommitIndex, final long leaderReportedCommitIndex) {
    if (localCommitIndex < 0)
      return localCommitIndex;
    return Math.max(localCommitIndex, leaderReportedCommitIndex);
  }

  /**
   * Health-monitor hook (issue #7619): asks the current leader for its commit index, for the readiness gate in
   * {@link #isReadyForTraffic(long)} and for this follower's own stall signal ({@link #trackFollowerStall()},
   * issue #8342). A follower cannot compute this itself - Ratis clamps its commit index to its own flush index,
   * and every commit index a follower learns rides the same leader-to-follower appends whose absence is the thing
   * to detect - so it has to come from the leader over a separate call.
   * <p>
   * Runs on a follower with a known leader, whatever {@code arcadedb.server.readinessRequiresHA} says (issue
   * #8342): the stall signal in {@code GET /api/v1/cluster} reads the answer on every node, and a follower whose
   * log stopped receiving entries at the current term has no other way to see that it is behind. The cost is one
   * small in-memory group-info call to the leader per health tick ({@code arcadedb.ha.healthCheckInterval},
   * default 3 s), on a client kept open between ticks. The value is replaced rather than maxed with the previous one: every
   * sample is a commit index a leader really reported, so every sample is a safe lower bound, and replacing lets
   * a node that outlived a wholesale reset of the cluster's Raft state forget a figure from the old log. A failed
   * call keeps the previous value, which is still a lower bound.
   * <p>
   * The call runs on the health-monitor thread, so consecutive failures back off (skipping 1, 3, 7, then at most
   * {@link #LEADER_COMMIT_PROBE_MAX_SKIP_TICKS} ticks between attempts) instead of adding the probe timeout to every
   * tick while the leader stays unreachable; a success or a leader change re-arms it at once. The wedged channel
   * this exists for is leader-to-follower, so the follower-to-leader call keeps succeeding there and never backs off.
   * <p>
   * Not the ReadIndex path {@link #fetchReadIndex} uses, although that also returns a leader's commit index: a
   * ReadIndex makes the leader confirm its leadership with a heartbeat round to a majority, and it goes through the
   * shared write client and its minute-long retry policy. This needs neither - any commit index a leader reports is
   * a safe lower bound - so it uses the lighter group-info call on its own bounded client.
   */
  @Override
  public void refreshLeaderCommitIndex() {
    if (shutdownRequested)
      return;
    final RaftServer server = raftServer;
    if (server == null)
      return;
    final RaftPeer leaderPeer;
    try {
      final var division = server.getDivision(raftGroup.getGroupId());
      final var info = division.getInfo();
      if (info.isLeader())
        return;
      final RaftPeerId leaderId = info.getLeaderId();
      final var conf = division.getRaftConf();
      if (leaderId == null || conf == null)
        return;
      leaderPeer = findPeer(conf.getCurrentPeers(), leaderId);
    } catch (final Exception e) {
      // Same Ratis IllegalStateException window as isReadyForTraffic() (issue #5271): skip this tick.
      LogManager.instance().log(this, Level.FINE, "Cannot read the Raft state to probe the leader commit index", e);
      return;
    }
    if (leaderPeer == null)
      return;
    if (!leaderPeer.getId().equals(leaderCommitProbeLastLeader)) {
      // A new leader: whatever made the previous one unreachable says nothing about this one.
      leaderCommitProbeLastLeader = leaderPeer.getId();
      leaderCommitProbeFailures = 0;
      leaderCommitProbeSkipTicks = 0;
    }
    if (leaderCommitProbeSkipTicks > 0) {
      leaderCommitProbeSkipTicks--;
      return;
    }
    final long reported = leaderCommitProber.commitIndexOf(leaderPeer);
    if (reported >= 0) {
      leaderReportedCommitIndex = reported;
      leaderCommitProbeFailures = 0;
    } else {
      leaderCommitProbeFailures = Math.min(leaderCommitProbeFailures + 1, 30);
      leaderCommitProbeSkipTicks = leaderCommitProbeSkipTicksAfter(leaderCommitProbeFailures);
    }
  }

  /**
   * Health-monitor hook (issue #8342): tracks whether this follower is stalled behind its leader, from its own
   * point of view. See {@link FollowerStallTracker} for the rule, which is the one the leader's
   * {@link ClusterMonitor} applies to this same replica, with the same lag threshold
   * ({@code arcadedb.ha.replicationLagWarning}).
   * <p>
   * Only a running follower of a known leader with no resync in flight is tracked. A resync has its own
   * {@code local-resync-in-progress} alert and applies nothing while it downloads, and the leader, a node with no
   * leader and a division that is not {@code RUNNING} (whose last indices survive a close, issue #5271) have
   * nothing to be stalled behind. A leader change starts the spell over: the new leader realigns the follower's
   * log, and what the old one failed to send says nothing about it.
   * <p>
   * Measured against {@link #leaderReportedCommitIndex}, which {@link #refreshLeaderCommitIndex()} refreshes later
   * in the same tick, so a figure is at most one tick old. Every figure it holds is a commit index a leader really
   * reported, so a stale one can only under-state the lag.
   */
  @Override
  public void trackFollowerStall() {
    final RaftServer server = raftServer;
    if (server == null || shutdownRequested) {
      followerStallTracker.reset();
      return;
    }
    final boolean eligible;
    final long applied;
    final long logIndex;
    try {
      final var division = server.getDivision(raftGroup.getGroupId());
      final var info = division.getInfo();
      final RaftPeerId leaderId = info.getLeaderId();
      if (leaderId == null || !leaderId.equals(followerStallLeader)) {
        followerStallLeader = leaderId;
        followerStallTracker.reset();
      }
      final ArcadeStateMachine sm = stateMachine;
      eligible = leaderId != null && !info.isLeader() && info.getLifeCycleState() == LifeCycle.State.RUNNING
          && sm != null && !sm.isResyncInProgress();
      applied = info.getLastAppliedIndex();
      final TermIndex last = division.getRaftLog().getLastEntryTermIndex();
      logIndex = last != null ? last.getIndex() : -1L;
    } catch (final Exception e) {
      // Same Ratis IllegalStateException window as isReadyForTraffic() (issue #5271): no judgement this tick.
      LogManager.instance().log(this, Level.FINE, "Cannot read the Raft state to track a follower stall", e);
      followerStallTracker.reset();
      return;
    }
    followerStallTracker.observe(followerStallClock.getAsLong(), eligible, leaderReportedCommitIndex, applied,
        logIndex, clusterMonitor.getLagWarningThreshold());
  }

  /**
   * This follower's stall behind its leader, or {@code null} when it is not stalled (issue #8342). See
   * {@link #trackFollowerStall()}. Always {@code null} while the health monitor is not running.
   */
  FollowerStallTracker.Stall getFollowerStallBehindLeader() {
    return followerStallTracker.current();
  }

  /** Test seam for {@link #trackFollowerStall()}: the clock its spell is measured with. */
  void setFollowerStallClock(final LongSupplier clock) {
    this.followerStallClock = clock;
  }

  /**
   * Health-monitor hook (issue #8953): tracks whether a leader has been failing to reach this follower since its Raft
   * layer was restarted in place. See {@link LeaderReachSinceRestartTracker} for the rule. {@code raftState} cannot
   * show it - the new division is RUNNING - and neither can the time since the last leader contact, which a follower
   * nobody reaches resets on every rejected pre-vote.
   * <p>
   * Held to a grace of twice the effective election timeout maximum: a healthy path delivers the leader's heartbeat,
   * and any entry it is missing, well within one. A resync in flight is left to its own alert (a snapshot install
   * applies nothing for its whole length), and a division that is not RUNNING is reported by {@code raftState}.
   * Logged at WARNING when the condition starts being reported and at INFO when it clears.
   */
  @Override
  public void trackLeaderReachSinceRestart() {
    final RaftServer server = raftServer;
    if (server == null || shutdownRequested) {
      leaderReachTracker.reset();
      return;
    }
    final boolean leaderKnown;
    final boolean holds;
    try {
      final var division = server.getDivision(raftGroup.getGroupId());
      final var info = division.getInfo();
      final ArcadeStateMachine sm = stateMachine;
      if (info.getLifeCycleState() != LifeCycle.State.RUNNING || (sm != null && sm.isResyncInProgress())) {
        leaderReachTracker.reset();
        return;
      }
      final TermIndex last = division.getRaftLog().getLastEntryTermIndex();
      final long heldIndex = Math.max(last != null ? last.getIndex() : -1L, info.getLastAppliedIndex());
      leaderKnown = info.getLeaderId() != null;
      holds = LeaderReachSinceRestartTracker.leaderNotReaching(isReplicationPathUnprovenSinceRestart(), info.isLeader(),
          leaderKnown, leaderReportedCommitIndex, heldIndex);
    } catch (final Exception e) {
      // Same Ratis IllegalStateException window as isReadyForTraffic() (issue #5271): no judgement this tick.
      LogManager.instance().log(this, Level.FINE, "Cannot read the Raft state to track the leader's reach", e);
      leaderReachTracker.reset();
      return;
    }
    final boolean wasReported = leaderReachTracker.current() != null;
    leaderReachTracker.observe(followerStallClock.getAsLong(), holds, leaderKnown, leaderUnreachableGraceMs());
    final LeaderReachSinceRestartTracker.Unreachable unreachable = leaderReachTracker.current();
    if (!wasReported && unreachable != null)
      LogManager.instance().log(this, Level.WARNING,
          "No leader has reached this follower for %dms since its in-place Ratis restart: its division reports RUNNING, "
              + "but no replicated entry arrived while %s. The leader's appends may still be bound to the replaced "
              + "instance; the leader resets its channel after arcadedb.ha.peerChannelResetDuration, restart this node "
              + "if it persists (issue #8953)", unreachable.unreachableForMs(),
          leaderKnown ? "the leader reports entries this node does not hold"
              : "no leader made itself known (also the case on every node of a cluster without a quorum)");
    else if (wasReported && unreachable == null)
      LogManager.instance().log(this, Level.INFO, "A leader reaches this follower again after its in-place Ratis restart");
  }

  /**
   * A leader failing to reach this follower since its in-place restart, or {@code null} when that is not being reported
   * (issue #8953). See {@link #trackLeaderReachSinceRestart()}. Always {@code null} while the health monitor is not
   * running.
   */
  LeaderReachSinceRestartTracker.Unreachable getLeaderUnreachableSinceRestart() {
    return leaderReachTracker.current();
  }

  /** Twice the effective election timeout maximum: see {@link #trackLeaderReachSinceRestart()}. */
  private long leaderUnreachableGraceMs() {
    return 2L * RaftPropertiesBuilder.electionTimeoutMaxFor(
        configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN),
        configuration.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX));
  }

  /** Upper bound of the probe's failure backoff, in health ticks (issue #7619, review of PR #8322). */
  static final int LEADER_COMMIT_PROBE_MAX_SKIP_TICKS = 8;

  /** Ticks to skip after {@code failures} consecutive failed probes: 1, 3, 7, then {@link #LEADER_COMMIT_PROBE_MAX_SKIP_TICKS}. */
  static int leaderCommitProbeSkipTicksAfter(final int failures) {
    if (failures <= 0)
      return 0;
    return (int) Math.min(LEADER_COMMIT_PROBE_MAX_SKIP_TICKS, (1L << Math.min(failures, 4)) - 1);
  }

  /**
   * The production {@link LeaderCommitProber}: a Ratis group-info call to {@code leader}, whose reply carries the
   * leader's commit index for itself among its commit infos. Bounded by {@link #LEADER_COMMIT_PROBE_TIMEOUT_MS}
   * with no retry, because it runs on the health-monitor thread and the next tick is the retry. Returns
   * {@code -1} on any failure, after dropping the client so the next tick dials a fresh channel.
   */
  private long queryLeaderCommitIndex(final RaftPeer leader) {
    if (leaderCommitProbeClosed)
      return -1L;
    RaftClient client = leaderCommitProbeClient.get();
    try {
      if (client == null || !leader.equals(leaderCommitProbeTarget)) {
        closeProbeClient(leaderCommitProbeClient.getAndSet(null));
        client = newLeaderCommitProbeClient(leader);
        if (client == null)
          return -1L;
        leaderCommitProbeTarget = leader;
        leaderCommitProbeClient.set(client);
        if (leaderCommitProbeClosed) {
          // stop() ran while this client was being built: it may have found the reference still empty.
          closeProbeClient(leaderCommitProbeClient.getAndSet(null));
          return -1L;
        }
      }
      final var reply = client.getGroupManagementApi(leader.getId()).info(raftGroup.getGroupId());
      if (reply == null || !reply.isSuccess())
        return -1L;
      final long reported = reportedCommitIndexOf(reply.getCommitInfos(), leader.getId());
      return reported == NO_COMMIT_INFO ? -1L : reported;
    } catch (final Exception e) {
      if (!leaderCommitProbeClosed)
        LogManager.instance().log(this, Level.FINE, "Cannot read the commit index of leader %s", e, leader.getId());
      if (client != null && leaderCommitProbeClient.compareAndSet(client, null))
        closeProbeClient(client);
      return -1L;
    }
  }

  /** Per-call bound of the leader commit-index probe (issue #7619). Well under the default health interval. */
  static final long LEADER_COMMIT_PROBE_TIMEOUT_MS = 2_000L;

  private RaftClient newLeaderCommitProbeClient(final RaftPeer leader) {
    final RaftProperties shared = raftProperties;
    if (shared == null)
      return null;
    // A copy, for the same reason as newMembershipClient(): the shared properties belong to the Raft server.
    final RaftProperties properties = new RaftProperties(shared);
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties,
        TimeDuration.valueOf(LEADER_COMMIT_PROBE_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    return RaftClient.newBuilder()
        .setRaftGroup(RaftGroup.valueOf(raftGroup.getGroupId(), leader))
        .setProperties(properties)
        .setParameters(raftParameters)
        .setRetryPolicy(RetryPolicies.noRetry())
        .build();
  }

  /** Called from {@link #stop()}; never blocks on an in-flight probe, whose RPC closing the client fails at once. */
  private void closeLeaderCommitProbeClient() {
    leaderCommitProbeClosed = true;
    closeProbeClient(leaderCommitProbeClient.getAndSet(null));
  }

  private void closeProbeClient(final RaftClient client) {
    if (client != null)
      try {
        client.close();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.FINE, "Error closing the leader commit-index probe client", e);
      }
  }

  /** What {@link #reportedCommitIndexOf} answers when the commit infos carry no entry for the server asked about. */
  static final long NO_COMMIT_INFO = Long.MIN_VALUE;

  /**
   * The commit index {@code serverId} reported in a Ratis reply's commit infos, or {@link #NO_COMMIT_INFO} when
   * they carry none for it. Shared by the ReadIndex path ({@link AppliedIndexWaits#extractLeaderCommitIndex}) and the readiness
   * probe of issue #7619. Package-private so the lookup can be tested without a Ratis server.
   */
  static long reportedCommitIndexOf(final Collection<RaftProtos.CommitInfoProto> commitInfos,
      final RaftPeerId serverId) {
    if (commitInfos == null || serverId == null)
      return NO_COMMIT_INFO;
    final ByteString id = serverId.toByteString();
    for (final RaftProtos.CommitInfoProto info : commitInfos)
      if (info.hasServer() && id.equals(info.getServer().getId()))
        return info.getCommitIndex();
    return NO_COMMIT_INFO;
  }

  /** The member of {@code peers} whose id is {@code peerId}, or {@code null}. */
  private static RaftPeer findPeer(final Collection<RaftPeer> peers, final RaftPeerId peerId) {
    for (final RaftPeer peer : peers)
      if (peer.getId().equals(peerId))
        return peer;
    return null;
  }

  /**
   * How {@link #refreshLeaderCommitIndex()} asks a leader for its commit index (issue #7619): a seam so a unit
   * test can stand in for a leader that has committed entries this follower never received.
   */
  @FunctionalInterface
  interface LeaderCommitProber {
    /** The leader's commit index for itself, or a negative value when it could not be read. */
    long commitIndexOf(RaftPeer leader);
  }

  void setLeaderCommitProber(final LeaderCommitProber prober) {
    this.leaderCommitProber = prober;
  }

  /** True when {@code peerId} is a member of the given Raft peer set (the current configuration). */
  static boolean isPeerInConfig(final Collection<RaftPeer> peers, final RaftPeerId peerId) {
    for (final RaftPeer peer : peers)
      if (peer.getId().equals(peerId))
        return true;
    return false;
  }

  /**
   * Builds a RaftClient with a bounded retry policy. The default Ratis retry policy is
   * retryForeverNoSleep, which causes blocking admin calls (like setConfiguration) to hang
   * indefinitely when the leader is not yet ready or the reconfiguration is in progress.
   * This uses a bounded retry so a single call returns within a reasonable time, allowing
   * our own retry loop in setConfigurationWithRetry to control the overall timeout.
   */
  static RaftClient buildRaftClient(final RaftGroup group, final RaftProperties properties,
      final Parameters parameters) {
    return buildRaftClient(group, properties, parameters, null);
  }

  /**
   * Like {@link #buildRaftClient(RaftGroup, RaftProperties, Parameters)}, but seeds the new client with a
   * known leader peer ID so the first write request routes directly to the leader without a
   * probe round-trip.
   *
   * @param parameters the same transport {@link Parameters} the local Ratis server was built with, so the
   *                   self-client speaks TLS whenever the transport does (issue #3890).
   */
  static RaftClient buildRaftClient(final RaftGroup group, final RaftProperties properties,
      final Parameters parameters, final RaftPeerId knownLeaderId) {
    // Set the client-side RPC timeout to match the quorum timeout so a slow leader response
    // does not trigger a premature TimeoutIOException before the commit completes.
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    final RaftClient.Builder builder = RaftClient.newBuilder()
        .setRaftGroup(group)
        .setProperties(properties)
        .setParameters(parameters != null ? parameters : new Parameters())
        .setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(60, TimeDuration.valueOf(1, TimeUnit.SECONDS)));
    if (knownLeaderId != null)
      builder.setLeaderId(knownLeaderId);
    return builder.build();
  }

  /**
   * The per-call retry policy of the membership-change client (issues #7539, #7561).
   * <p>
   * Short on purpose, and short is safe here because it does not bound the operation - {@code RaftClusterManager}
   * has its own deadline and its own backoff around it. What it bounds is how many times ONE
   * {@code admin().setConfiguration()} call re-issues before control comes back, and therefore how often the
   * operator's budget is looked at. The shared {@link #raftClient} carries {@code RetryLimited(60, 1s)}, which
   * is right for a data write waiting out an election and wrong for an admin call whose caller is an HTTP
   * worker thread: it made the 90 s budget observable once a minute, so a change that could not commit cost
   * about 150 s.
   * <p>
   * <b>Three attempts is not "three seconds".</b> The sleep is 500 ms, but each attempt is also bounded by
   * {@link #CLIENT_REQUEST_TIMEOUT_MS}, so a call whose RPC hangs at the transport can take about 31 s before
   * it comes back - and the common shape, a synchronous rejection, comes back in milliseconds. The budget check
   * in {@code RaftClusterManager.canAttemptAgain} sizes itself on the OBSERVED attempt duration rather than on
   * either figure, which is why it stays correct across that spread; the numbers matter for an operator sizing
   * {@code arcadedb.ha.membershipChangeTimeout}, not for the loop.
   * <p>
   * {@code KubernetesAutoJoin.PROBE_RETRY_POLICY} is the same reasoning applied to the auto-join probe
   * (issue #5973), and the numbers are deliberately of the same order.
   */
  static final RetryPolicy MEMBERSHIP_RETRY_POLICY =
      RetryPolicies.retryUpToMaximumCountWithFixedSleep(3, TimeDuration.valueOf(500, TimeUnit.MILLISECONDS));

  /**
   * A {@link RaftClient} for one membership change, carrying {@link #MEMBERSHIP_RETRY_POLICY} instead of the
   * shared client's minute-long one (issue #7561). The caller closes it.
   * <p>
   * A client per operation rather than a long-lived second one: add- and remove-peer are operator actions, so
   * the connection setup is paid once per administrative request and there is no second client to keep in step
   * with {@code refreshRaftClient}'s leader re-seeding, TLS parameters and teardown.
   *
   * <p>
   * <b>The per-RPC timeout is set here rather than inherited, on a COPY of the properties</b> (review of PR
   * #7941). Bounding the retry COUNT bounds nothing on its own: a {@code setConfiguration} whose RPC hangs at
   * the transport with no reply never reaches a second attempt, so the deadline in {@code RaftClusterManager}
   * is not consulted until that one call returns. {@link #CLIENT_REQUEST_TIMEOUT_MS} is what makes it return,
   * and it is written onto the shared properties by {@link #buildRaftClient} - so it is already there in
   * practice, by an ordering nothing states. Writing it here says so locally.
   * <p>
   * The copy is what keeps that honest rather than merely redundant. {@link #raftProperties} is the object the
   * Raft SERVER was built from and is shared with every other client built from it, and two membership changes
   * can run at once - add-peer and remove-peer are not serialised with each other. Writing the same value from
   * two threads into one configuration map is benign only by luck; taking a copy means this method reads shared
   * state and mutates nothing, which needs no luck at all.
   *
   * @return null before {@code start()} has built the Raft properties, which is also what a unit-test harness
   * that never stood up a Raft server has - the caller then falls back to {@link #getClient()}
   */
  RaftClient newMembershipClient() {
    final RaftProperties shared = raftProperties;
    if (shared == null)
      return null;
    final RaftProperties properties = new RaftProperties(shared);
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties,
        TimeDuration.valueOf(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    final RaftClient.Builder builder = RaftClient.newBuilder()
        .setRaftGroup(raftGroup)
        .setProperties(properties)
        .setParameters(raftParameters)
        .setRetryPolicy(MEMBERSHIP_RETRY_POLICY);
    // Seeded with the leader this node believes in, so the first request does not pay a probe round trip; a
    // stale belief costs one redirect, exactly as it does for the shared client.
    final RaftPeerId leaderId = getLeaderId();
    if (leaderId != null)
      builder.setLeaderId(leaderId);
    return builder.build();
  }

  public void transferLeadership(final String targetPeerId, final long timeoutMs) {
    clusterManager.transferLeadership(targetPeerId, timeoutMs);
  }

  /**
   * Asks Ratis for a bare step-down, with no target, and reports success only once a different peer is seen as the
   * leader (issue #8480). The last resort of {@link #stepDown()} and of {@link #transferLeadership(long)}, for when no
   * peer is eligible as an explicit target: on its own it starts no election, so the cluster stays leaderless until
   * a follower's election timer fires.
   */
  boolean stepDownWithoutTarget(final long timeoutMs) {
    return clusterManager.stepDownWithoutTarget(timeoutMs);
  }

  /** Whether leadership settled on another peer within the confirmation grace; a seam for {@link #stepDown()}. */
  boolean leadershipMovedAway() {
    return clusterManager.leadershipMovedAway();
  }

  /**
   * Whether the transfer another caller has pending on this leader moved leadership to another peer within a
   * step-down's budget (issue #8557); a seam for {@link #stepDown()}.
   */
  boolean concurrentHandOffLanded() {
    return clusterManager.leadershipMovedAway(STEP_DOWN_TRANSFER_TIMEOUT_MS);
  }

  /** Budget of one targeted transfer of {@link #stepDown()}, and of its bare step-down fallback. */
  private static final long STEP_DOWN_TRANSFER_TIMEOUT_MS = 10_000L;

  /**
   * Steps this leader down by transferring leadership to the best eligible peer.
   * <p>
   * Refuses when this node is not the leader (issue #7134). A follower has nothing to step down FROM, but the
   * transfer it would issue does not fail locally: Ratis routes it to the real leader, which then holds an
   * election nobody asked for. That is reachable from an ordinary {@code POST /api/v1/cluster/stepdown} sent
   * through a Kubernetes Service, so the guard sits here - in the method every caller goes through - rather
   * than only in the handler.
   *
   * @throws NotTheLeaderRefusalException when this node is not the leader, whether that is already true on
   *                                       entry or becomes true during leadership transfer
   * @throws ReplicationException when all leadership transfers fail and this node is still the leader
   */
  public void stepDown() {
    if (!isLeader())
      throw new NotTheLeaderRefusalException("Refusing to step down", getLeaderId());

    final List<RaftPeer> candidates = StepDownTargets.selectStepDownTargets(getLivePeers(), localPeerId, clusterMonitor,
        handoffReachablePeers());

    boolean attempted = false;
    for (final RaftPeer peer : candidates) {
      try {
        transferLeadership(peer.getId().toString(), STEP_DOWN_TRANSFER_TIMEOUT_MS);
        return;
      } catch (final LeadershipTransferInProgressException inProgress) {
        // Another caller is handing this leadership over right now (issue #8557). Every other candidate would be
        // refused the same way, and the bare step-down below would pull the leadership out from under that transfer,
        // leaving the cluster leaderless for an election timeout. Its outcome is this step-down's outcome.
        if (concurrentHandOffLanded())
          return;
        if (!isLeader())
          throw new NotTheLeaderRefusalException("Refusing to step down", getLeaderId());
        throw new ReplicationException(
            "Cannot step down: another leadership transfer is in progress on this node and did not complete ("
                + inProgress.getMessage() + ")");
      } catch (final NotTheLeaderRefusalException notLeader) {
        // Leadership moved between the guard above and this attempt. When an earlier candidate's transfer was
        // already sent, that transfer may be what moved it - its call failed, yet the target won (#8487) - so a
        // settled handoff is the step-down succeeding, not a refusal (issue #8480). Otherwise propagate the refusal
        // so callers stop retrying a step-down that is already moot, and the HTTP handler reports 409 (issue #7134).
        if (attempted && leadershipMovedAway())
          return;
        throw notLeader;
      } catch (final Exception e) {
        attempted = true;
        // The call failed, but the target may have won anyway: the leader change closes the client under the RPC
        // (#8487). Moving on would start a second election against the leader just elected, so settle it here, the
        // way transferLeadership(long) does (issue #8480).
        if (!isLeader()) {
          if (leadershipMovedAway())
            return;
          throw new NotTheLeaderRefusalException("Refusing to step down", getLeaderId());
        }
        LogManager.instance().log(this, Level.SEVERE,
            "Failed to step down (transfer to %s): %s", peer.getId(), e.getMessage());
      }
    }

    // No eligible explicit target (every other peer is a priority-0 witness/replica or is lagging, or
    // every explicit transfer failed). Delegate the choice to Ratis: the no-target transfer honors Raft
    // priorities and never elects a priority-0 peer, which is safer than abandoning the step-down (issue #4808).
    // Straight to the bare step-down, not through transferLeadership(long): that would retry, against the same
    // budget, the very candidates the loop above just tried (issue #8480).
    LogManager.instance().log(this, Level.INFO,
        "No explicit step-down target eligible; delegating leadership-transfer target selection to Ratis");
    if (stepDownWithoutTarget(STEP_DOWN_TRANSFER_TIMEOUT_MS))
      return;

    // The no-target API also returns false if leadership was lost before the transfer (issue #4809).
    // Keep that distinct from an exhausted transfer failure so phase-2 recovery does not retry or stop a follower.
    if (!isLeader())
      throw new NotTheLeaderRefusalException("Refusing to step down", getLeaderId());

    throw new ReplicationException("Cannot step down: no other peer available for leadership transfer");
  }

  /**
   * The followers that answered this leader recently enough to be handed leadership (issue #8556): the reachability
   * screen of {@link StepDownTargets#selectStepDownTargets(Collection, RaftPeerId, ClusterMonitor, Set)}. Read live from Ratis rather
   * than from the {@link ClusterMonitor} tick, which lags by up to one tick and holds nothing at all for the first
   * tick after an election. Empty on a follower. Package-private and overridable so the step-down loop can be
   * unit-tested without a running division.
   */
  Set<String> handoffReachablePeers() {
    return StepDownTargets.withoutServiceGapPeers(followerContactPeers(), peerCapabilities.peersWithServiceGap());
  }

  /** The followers that answered this leader recently, without the service-gap screen of {@link #handoffReachablePeers()}. */
  Set<String> followerContactPeers() {
    return StepDownTargets.handoffReachablePeerIds(getFollowerStates(), handoffContactWindowMs);
  }

  public void leaveCluster() {
    clusterManager.leaveCluster();
  }

  public void leaveCluster(final boolean force) {
    clusterManager.leaveCluster(force);
  }

  /**
   * Wakes every thread blocked in {@link #waitForAppliedIndex(long, boolean)} or
   * {@link #waitForLocalApply()} so they can re-check whether their target index has been reached.
   * Must be called by every code path that advances the local applied index, not only
   * {@link ArcadeStateMachine#applyTransaction}: {@link ArcadeStateMachine#reinitialize()} and
   * {@link ArcadeStateMachine#notifyInstallSnapshotFromLeader} both seed the index directly after a
   * snapshot install and call this too (issue #5846 - a snapshot install used to advance the index
   * silently, leaving waiters blocked until {@link #quorumTimeout} even though their target was
   * already reached).
   */
  public void notifyApplied() {
    synchronized (applyNotifier) {
      applyNotifier.notifyAll();
    }
  }

  // AppliedIndexWaits holds the implementation of these; they stay here for the callers outside this class.

  public void notifyMembershipChanged() {
    appliedWaits.notifyMembershipChanged();
  }

  public boolean awaitApplied(final BooleanSupplier condition, final long timeoutMs) throws InterruptedException {
    return appliedWaits.awaitApplied(condition, timeoutMs);
  }

  void awaitAppliedIndex(final String databaseName, final long targetIndex, final boolean throwOnTimeout,
      final Database.READ_CONSISTENCY readConsistency) {
    appliedWaits.awaitAppliedIndex(databaseName, targetIndex, throwOnTimeout, readConsistency);
  }

  public void waitForLocalApply() {
    appliedWaits.waitForLocalApply();
  }

  public long fetchReadIndex(final boolean expectSelfIsLeader) {
    return appliedWaits.fetchReadIndex(expectSelfIsLeader);
  }

  /**
   * Lenient overload (READ_YOUR_WRITES / bookmark wait): on timeout it logs and returns,
   * allowing the read to proceed against possibly-stale local state. Never used by the
   * LINEARIZABLE path - see {@link #waitForAppliedIndex(long, boolean)}.
   */
  public void waitForAppliedIndex(final long targetIndex) {
    waitForAppliedIndex(null, targetIndex, false);
  }

  /** Database-scoped lenient overload: see {@link #waitForAppliedIndex(String, long, boolean)}. */
  public void waitForAppliedIndex(final String databaseName, final long targetIndex) {
    waitForAppliedIndex(databaseName, targetIndex, false);
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
    waitForAppliedIndex(null, targetIndex, throwOnTimeout);
  }

  /**
   * As {@link #waitForAppliedIndex(long, boolean)}, but scoped to the database the read targets so a per-database
   * read floor applies (issue #6760).
   *
   * @param databaseName the database the read targets, or {@code null} for a node-wide wait
   */
  public void waitForAppliedIndex(final String databaseName, final long targetIndex, final boolean throwOnTimeout) {
    appliedWaits.awaitAppliedIndex(databaseName, targetIndex, throwOnTimeout, null);
  }

  /**
   * The wait of a consistent read: as {@link #waitForAppliedIndex(String, long, boolean)}, strict for
   * {@code LINEARIZABLE} and lenient for {@code READ_YOUR_WRITES}, but refused retryably as soon as this node is no
   * longer a member of the Raft configuration (issue #9606).
   * <p>
   * A removed node receives no more entries, so the index a read waits for is never applied here: a read that started
   * waiting while the node was still a member blocked the whole quorum timeout before it was refused (#9590) or failed.
   * Only a read may give up this way. The commit and drop paths wait on the same index after their entry committed
   * cluster-wide, and a retryable exception there would invite a duplicate retry of a durable write - they keep
   * {@link #waitForAppliedIndex(String, long, boolean)}.
   *
   * @throws NeedRetryException when this node is, or becomes during the wait, no longer a member of the configuration
   */
  public void waitForAppliedIndexForRead(final String databaseName, final long targetIndex,
      final Database.READ_CONSISTENCY consistency) {
    appliedWaits.awaitAppliedIndex(databaseName, targetIndex, consistency == Database.READ_CONSISTENCY.LINEARIZABLE, consistency);
  }

  @Override
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

  /**
   * The applied index a read may actually rely on: {@link #getLastAppliedIndex()} clamped to the
   * stale-snapshot read floor while one is outstanding (issue #6111).
   * <p>
   * Ratis derives the value {@link #getLastAppliedIndex()} reports from the snapshot marker the moment
   * the marker is seeded, so a node that restarted onto a marker running ahead of the entries it really
   * applied ({@code ArcadeStateMachine.reinitialize()}'s snapshot-gap branch) advertises an applied
   * index covering data it does not hold. Using the raw value as the apply-wait predicate let a
   * LINEARIZABLE or READ_YOUR_WRITES read inside that gap proceed against the stale local state; the
   * clamp keeps the waiters honest until the flagged re-download lands. Reporting paths (cluster status,
   * lag detection) deliberately keep using the raw Ratis value.
   */
  public long getTrustedAppliedIndex() {
    return getTrustedAppliedIndex(null);
  }

  /**
   * As {@link #getTrustedAppliedIndex()}, additionally clamped to the read floor of {@code databaseName} when a
   * snapshot install gave up on that database (issue #6760).
   * <p>
   * The two floors are independent and both conservative, so the answer is the minimum of whichever are
   * outstanding. Passing {@code null} asks the node-wide question and consults the global floor only.
   */
  public long getTrustedAppliedIndex(final String databaseName) {
    long trusted = getLastAppliedIndex();
    final long floor = appliedWaits.getStaleSnapshotAppliedFloor();
    if (floor >= 0)
      trusted = Math.min(trusted, floor);
    final long databaseFloor = appliedWaits.getDatabaseAppliedFloor(databaseName);
    if (databaseFloor >= 0)
      trusted = Math.min(trusted, databaseFloor);
    return trusted;
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

  /**
   * This node's own Raft log start index ({@code RaftLog.getStartIndex()}), or {@code -1} when the division cannot
   * be read (same degrade-to-unknown reasoning as {@link #getCommitIndex()}, issue #5271). It does not check the role:
   * the value is only meaningful to the lag monitor on the leader, which checks {@link #isLeader()} before reading it.
   * <p>
   * Fed to {@link ClusterMonitor#updateReplicaMatchIndex(String, long, long, long, long)} so it can tell a
   * follower whose {@code nextIndex} has fallen at or below this leader's own compacted log start - the
   * condition under which {@code LogAppender.shouldInstallSnapshot} keeps re-notifying the follower instead of
   * replicating - from an ordinary lag (issue #8457).
   */
  public long getRaftLogStartIndex() {
    if (raftServer == null)
      return -1;
    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getRaftLog().getStartIndex();
    } catch (final Exception e) {
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
   * Ensures linearizable read consistency for the leader. Sends a ReadIndex RPC to
   * confirm the leader lease, then waits for the local state machine to apply up to
   * the confirmed commit index.
   * <p>
   * Throws {@link ReplicationException} if leadership is lost before or after the RPC, or if
   * the local state machine cannot catch up to the read index before the quorum timeout.
   */
  public void ensureLinearizableRead() {
    ensureLinearizableRead(null);
  }

  /** As {@link #ensureLinearizableRead()}, scoped to the database the read targets (issue #6760). */
  public void ensureLinearizableRead(final String databaseName) {
    final long readIndex = appliedWaits.fetchReadIndex(true);
    if (!isLeader())
      throw new ReplicationException("Lost leadership after ReadIndex confirmation");
    // LINEARIZABLE: a timeout MUST throw (HTTP 503) - never silently degrade to a stale read. A removal from the
    // configuration during the wait refuses the read at once (issue #9606)
    waitForAppliedIndexForRead(databaseName, readIndex, Database.READ_CONSISTENCY.LINEARIZABLE);
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
    ensureLinearizableFollowerRead(null);
  }

  /** As {@link #ensureLinearizableFollowerRead()}, scoped to the database the read targets (issue #6760). */
  public void ensureLinearizableFollowerRead(final String databaseName) {
    final long readIndex = appliedWaits.fetchReadIndex(false);
    // LINEARIZABLE: a timeout MUST throw (HTTP 503) - never silently degrade to a stale read. A removal from the
    // configuration during the wait refuses the read at once (issue #9606)
    waitForAppliedIndexForRead(databaseName, readIndex, Database.READ_CONSISTENCY.LINEARIZABLE);
  }

  // ReplicationStatistics holds the implementation of these; they stay here for the callers outside this class.

  public HAReplicationStatsProvider.HAReplicationStats getReplicationStats() {
    return replicationStats.getReplicationStats();
  }

  public List<HAReplicationStatsProvider.FollowerSample> getFollowerSamples() {
    return replicationStats.getFollowerSamples();
  }

  public List<HAReplicationStatsProvider.SchemaInstalmentSample> getSchemaInstalmentSamples() {
    return replicationStats.getSchemaInstalmentSamples();
  }

  public List<HAReplicationStatsProvider.UnreferencedFilesSample> getUnreferencedFilesSamples() {
    return replicationStats.getUnreferencedFilesSamples();
  }

  public HAReplicationStatsProvider.PendingPhase2Stats getPendingPhase2Stats() {
    return replicationStats.getPendingPhase2Stats();
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
        final ReplicationLatency latency = ReplicationStatistics.meanReplicationLatency(timers, peerId);
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
        final List<RaftProtos.ServerRpcProto> before = ReplicationStatistics.leaderFollowerInfos(info);
        if (before.isEmpty())
          return List.of();

        final long[] matchIndices = info.getFollowerMatchIndices();
        final long[] nextIndices = info.getFollowerNextIndices();

        final List<RaftProtos.ServerRpcProto> after = ReplicationStatistics.leaderFollowerInfos(info);

        final List<Map<String, Object>> correlated = ReplicationStatistics.correlateFollowerStates(before, matchIndices, nextIndices, after);
        if (correlated != null)
          return correlated;
        // Membership changed while we read the index arrays - retry with a fresh, consistent snapshot.
      }

      // Membership kept changing under us across every attempt: report the peers we currently know,
      // without indices, rather than risk attributing a match/next index to the wrong peer.
      return ReplicationStatistics.degradedFollowerStates(ReplicationStatistics.leaderFollowerInfos(info));
    } catch (final Exception e) {
      // Exception, not IOException, for the same reason as getCommittedPeersOrNull() and isReadyForTraffic():
      // this reads division.getInfo() through the identical path, so it meets the identical Ratis
      // IllegalStateException while an in-place restart re-initializes the division (issue #5271). It feeds
      // getReplicationStats(), getFollowerSamples() and the cluster-status and health endpoints, so propagating
      // here would 500 them in exactly the window the #7135 hardening exists for. No followers is the right
      // degraded answer.
      LogManager.instance().log(this, Level.FINE, "Cannot read follower states this tick", e);
      return List.of();
    }
  }

  /** Number of seqlock retries in {@link #getFollowerStates()} before degrading to index-less peers. */
  private static final int FOLLOWER_STATES_MAX_ATTEMPTS = 3;

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
      // The data files are forced here first, on the scheduler's thread (issue #11989): the checkpoint forces them again on
      // the apply thread, where every commit of the node waits for it, and only what was written in between is left there
      final ArcadeStateMachine sm = stateMachine;
      if (sm != null)
        sm.syncDataFilesAheadOfCheckpoint();
      return takeLocalSnapshot(creationGap);
    }

    @Override
    public long getRaftStorageUsableSpaceBytes() {
      final File dir = storageFiles.raftStorageVolume();
      return dir != null ? dir.getUsableSpace() : -1L;
    }

    @Override
    public long getRaftStorageTotalSpaceBytes() {
      final File dir = storageFiles.raftStorageVolume();
      return dir != null ? dir.getTotalSpace() : -1L;
    }

    @Override
    public String getRaftStorageDescription() {
      final File dir = storageFiles.raftStorageVolume();
      return dir != null ? dir.getAbsolutePath() : "<unknown>";
    }
  }

  /**
   * Package-private test hook: a compaction target bound to this server, so an integration test can
   * assert the free-space probe resolves a real volume without reaching into the scheduler's thread.
   */
  RaftLogCompactionScheduler.CompactionTarget newCompactionTargetForTesting() {
    return new RaftLogCompactionTarget();
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
    return takeLocalSnapshot(creationGap, SNAPSHOT_REQUEST_TIMEOUT_MS);
  }

  /**
   * {@link #takeLocalSnapshot(long)} with an explicit request timeout. Ratis fulfils the request on its
   * {@code StateMachineUpdater} thread the next time that thread is free, so a caller that cannot afford to wait
   * behind a long entry apply passes a short budget and treats the timeout as "not now".
   */
  long takeLocalSnapshot(final long creationGap, final long timeoutMs) {
    final RaftServer server = raftServer;
    if (server == null || shutdownRequested)
      return -1L;
    if (server.getLifeCycleState() != LifeCycle.State.RUNNING)
      return -1L;

    try {
      final RaftClientReply reply = server.snapshotManagement(
          SnapshotManagementRequest.newCreate(snapshotClientId, localPeerId, raftGroup.getGroupId(),
              snapshotCallId.incrementAndGet(), timeoutMs, creationGap));
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
    capabilityMonitor.startCapabilityMonitor();
  }

  /**
   * Stops the periodic lag monitoring task. Called when this node loses leadership.
   * <p>
   * Deliberately does NOT stop the capability monitor any more (issue #7549): it runs in every role, so that a
   * follower can answer what its peers advertise without the cluster having to locate a leader first. Losing
   * leadership ends the lag sampling, which really is the leader's view of its followers; it does not end a
   * read-only question every node can ask.
   */
  void stopLagMonitor() {
    if (lagMonitorExecutor != null) {
      lagMonitorExecutor.shutdownNow();
      lagMonitorExecutor = null;
    }
  }

  // PeerCapabilityMonitor holds the implementation of these; they stay here for the callers outside this class.

  void startCapabilityMonitor() {
    capabilityMonitor.startCapabilityMonitor();
  }

  boolean isCapabilityMonitorRunning() {
    return capabilityMonitor.isCapabilityMonitorRunning();
  }

  void stopCapabilityMonitor() {
    capabilityMonitor.stopCapabilityMonitor();
  }

  void refreshPeerCapabilities() {
    capabilityMonitor.refreshPeerCapabilities();
  }

  public boolean allPeersSupport(final String capability) {
    return capabilityMonitor.allPeersSupport(capability);
  }

  String explainMissingCapability(final String capability) {
    return capabilityMonitor.explainMissingCapability(capability);
  }

  void invalidateTxPreparedAtCapability() {
    capabilityMonitor.invalidateTxPreparedAtCapability();
  }

  void invalidateFullTextCorpusDeltaCapability() {
    capabilityMonitor.invalidateFullTextCorpusDeltaCapability();
  }

  void invalidateFixedWidthFloatArraysCapability() {
    capabilityMonitor.invalidateFixedWidthFloatArraysCapability();
  }

  public boolean canWriteFixedWidthFloatArrays() {
    return capabilityMonitor.canWriteFixedWidthFloatArrays();
  }

  void setCapabilityProber(final CapabilityProber prober) {
    capabilityMonitor.setCapabilityProber(prober);
  }

  Map<String, String> capabilityRequestHeaders() {
    return capabilityMonitor.capabilityRequestHeaders();
  }

  void offerCallerHttpAddress(final String callerPeerId, final String httpAddress, final int httpPort) {
    capabilityMonitor.offerCallerHttpAddress(callerPeerId, httpAddress, httpPort);
  }

  void offerPeerHttpAddress(final String peerId, final String address) {
    capabilityMonitor.offerPeerHttpAddress(peerId, address);
  }

  Map<String, String> getRelayablePeerHttpAddresses() {
    return capabilityMonitor.getRelayablePeerHttpAddresses();
  }

  List<String> getPeerHttpAddressCandidates(final String peerId) {
    return capabilityMonitor.getPeerHttpAddressCandidates(peerId);
  }

  /** The peer-capability cache this leader decides on (issue #7219). */
  public PeerCapabilityRegistry getPeerCapabilityRegistry() {
    return peerCapabilities;
  }

  /**
   * The peers of the current Raft configuration that have NOT proved they can decode {@code capability}, in
   * configuration order (issue #7219). Empty means every peer is covered, so the section may be written.
   * <p>
   * The list rather than a boolean, because the peer that withholds the answer is the only actionable thing an
   * operator can be told: "deltas are off" sends them to the setting, "peer arcadedb2 does not advertise
   * schema-delta" sends them to the node that has not been upgraded.
   */
  public List<String> peersMissingCapability(final String capability) {
    return peerCapabilities.peersMissing(otherConfiguredPeerIds(), capability);
  }

  /**
   * {@link #peersMissingCapability} for a framed optional section of {@code TX_ENTRY}: a peer that answered with it and has
   * only stopped answering since is not missing (issue #12122, see {@link PeerCapabilityRegistry#peersMissingFramedSection}).
   */
  List<String> peersMissingFramedSection(final String capability) {
    return peerCapabilities.peersMissingFramedSection(otherConfiguredPeerIds(), capability);
  }

  private List<String> otherConfiguredPeerIds() {
    final List<String> peerIds = new ArrayList<>();
    for (final RaftPeer peer : configuredPeers())
      if (!peer.getId().equals(localPeerId))
        peerIds.add(peer.getId().toString());
    return peerIds;
  }

  /**
   * The peers that have NOT proved they can decode {@code capability}, asking them NOW when the cached answer
   * does not already cover every one of them (issue #7511).
   * <p>
   * {@link #peersMissingCapability} alone was not enough for a caller that REFUSES on a "no", because the
   * background capability monitor used to run on the leader only - #7219's only consumer was the leader-side
   * schema-delta decision. Since issue #7549 it runs in every role, so on a node that has been up for one
   * refresh period the cached answer below is already the full one and nothing is dialled here; what remains is
   * the window before a freshly started node's first round lands, and the round below is what covers it. The
   * consumer this exists for was not leader-side when it was written: the group and API-token REST routes did not
   * forward, so {@code ServerSecurity.saveGroupClusterWide} and friends ran on whichever node the client or load
   * balancer picked. They forward to the leader since issue #8109, but a submission can still be decided on a node
   * that lost leadership between the forward and the submit, so a node's answer still has to be right in any
   * role.
   * <p>
   * So the cached answer is consulted first and one synchronous round is run only when it is not already a full
   * "yes". That keeps the leader's hot path free - a warm registry answers without dialling anything - and makes
   * a follower's answer correct at the cost of one probe round on an operation that is rare by construction
   * (group administration and token minting, both already serialised behind the {@code ServerSecurity} monitor
   * across a full Raft round trip). The round is bounded: sequential, with
   * {@link PeerCapabilityRegistry#PROBE_TIMEOUT_MS} per peer.
   * <p>
   * Re-reading after the round rather than returning what it observed is deliberate: the round records through
   * the same generation-guarded registry the monitor writes to, so the re-read is the one answer both agree on.
   * <p>
   * <b>Overlapping with the leader's background round is harmless, and deliberately not locked out</b> (PR #7555
   * review). Two rounds can fan out at once on a leader, which costs a redundant probe and nothing else: every
   * write goes through {@link PeerCapabilityRegistry}'s generation guard, and the one outcome that would matter -
   * a peer wrongly read as CAPABLE - cannot be produced by an interleaving, because recording a capability
   * requires a peer to have actually answered with it. A shared lock would remove the redundant probe at the cost
   * of a lock-order hazard worth more than it: this method runs on a request thread that already holds the
   * {@code ServerSecurity} monitor, so making the capability-monitor thread wait on the same lock would put a
   * monitor-held wait on both sides of a cycle.
   * <p>
   * The round is logged at FINE with what it cost and what it concluded. It is the one place this feature can add
   * latency an operator did not ask for - the {@code ServerSecurity} monitor is held across it, and that monitor is
   * shared with user administration, so an unreachable peer delays the next {@code createUser} as well as the next
   * group change - and a stall with nothing in the log to explain it is the thing that wastes an afternoon.
   */
  public List<String> peersMissingCapabilityNow(final String capability) {
    final List<String> cached = peersMissingCapability(capability);
    if (cached.isEmpty())
      return cached;

    final long startedAt = System.currentTimeMillis();
    capabilityMonitor.refreshPeerCapabilities();
    final List<String> missing = peersMissingCapability(capability);
    LogManager.instance().log(this, Level.FINE,
        "Asked every peer about the '%s' capability before replicating an entry that needs it (%d ms); still "
            + "missing: %s", capability, System.currentTimeMillis() - startedAt, missing);
    return missing;
  }

  final TxPreparedAtCapabilityCache txPreparedAtCapability = new TxPreparedAtCapabilityCache(
      () -> capabilityMonitor.allPeersSupportOnceAsked(PeerCapabilities.TX_PREPARED_AT_INDEX, true), System::currentTimeMillis, 1_000L);

  /**
   * Whether every peer can read the {@code tx-prepared-at-index} section, so a transaction may state it (issue #8686). Cached for a
   * second, since it is asked on every commit; a membership change drops the answer at once, see {@link TxPreparedAtCapabilityCache}.
   */
  public boolean canStateTxPreparedAt() {
    return txPreparedAtCapability.isCapable();
  }

  // The same short-lived, membership-aware verdict as the prepared-at one above, asked on every commit as well
  final TxPreparedAtCapabilityCache fullTextCorpusDeltaCapability = new TxPreparedAtCapabilityCache(
      () -> capabilityMonitor.allPeersSupportOnceAsked(PeerCapabilities.TX_FULLTEXT_CORPUS_DELTAS, true), System::currentTimeMillis, 1_000L);

  /**
   * Whether every peer can read the {@code tx-fulltext-corpus-deltas} section, so a transaction may carry the effect it had on
   * the BM25 corpus counters (issue #10018).
   */
  public boolean canStateFullTextCorpusDeltas() {
    return fullTextCorpusDeltaCapability.isCapable();
  }

  // Asked on every write of a float or double array, so the same short-lived, membership-aware verdict as the two above
  final TxPreparedAtCapabilityCache fixedWidthFloatArraysCapability = new TxPreparedAtCapabilityCache(
      () -> capabilityMonitor.allPeersSupportOnceAsked(PeerCapabilities.FIXED_WIDTH_FLOAT_ARRAYS, false), System::currentTimeMillis, 1_000L);

  /** Whether this node states the index a transaction was prepared at on the entries it replicates (issue #8686). */
  public boolean isTxSchemaCheckEnabled() {
    return configuration.getValueAsBoolean(GlobalConfiguration.HA_TX_SCHEMA_CHECK);
  }

  /** What this node tells its peers it can decode. */
  public Set<String> getAdvertisedCapabilities() {
    return advertisedCapabilities;
  }

  /**
   * Overrides what this node advertises, so an integration test can stand up a node that behaves like a build
   * predating a wire-format section. There is no other way to build a mixed-version cluster inside one JVM, and
   * the mixed-version case is the one this mechanism exists for.
   */
  // @VisibleForTesting
  void setAdvertisedCapabilities(final Set<String> capabilities) {
    this.advertisedCapabilities = Set.copyOf(capabilities);
  }

  /**
   * How one capability probe is made. A seam, not a strategy: the only production implementation is
   * {@link PeerCapabilityQuery}, and it exists so the two-pass fan-out of {@link #refreshPeerCapabilities} - whose
   * whole point is which peer an answer is credited to - can be pinned without standing up a mixed-address
   * cluster and a network (issue #7256).
   */
  @FunctionalInterface
  interface CapabilityProber {
    /**
     * @param expectedPeerId the peer this address is believed to name, or {@code null} to accept whichever peer
     *                       answers - the shared-endpoint route.
     */
    PeerCapabilityQuery.Advertisement probe(String expectedPeerId, String httpAddress, String httpsAddress,
        String clusterToken) throws IOException, InterruptedException;
  }

  // RaftTransportSetup holds the implementation of these; they stay here for the callers outside this class.

  Parameters buildParameters(final ContextConfiguration configuration) {
    return transport.buildParameters(configuration);
  }

  Parameters raftParametersForTest() {
    return transport.raftParametersForTest();
  }

  /** Package-private test hook: the Raft properties {@code start()} publishes, which {@link #refreshRaftClient} builds from. */
  void setRaftPropertiesForTest(final RaftProperties properties) {
    this.raftProperties = properties;
  }

  PeerAddressAllowlistFilter allowlistFilterForTest() {
    return transport.allowlistFilterForTest();
  }

  PeerAllowlistCallInterceptor allowlistInterceptorForTest() {
    return transport.allowlistInterceptorForTest();
  }

  void reconcileAllowlistMembership(final Collection<RaftPeer> committedPeers) {
    transport.reconcileAllowlistMembership(committedPeers);
  }

  /**
   * Proactively reconciles the inbound Raft gRPC peer allowlist with cluster membership and with current DNS
   * (issues #4696, #7132, #7225). Invoked from the health monitor tick on every node so a peer that restarted
   * with a new pod IP is admitted without first having to be rejected on an inbound connection - which a
   * leader with a wedged outbound appender channel may never receive - and so a peer removed from the group
   * loses its access without waiting for a process restart. No-op when the allowlist is disabled. The filter
   * throttles the actual DNS re-resolution to its configured refresh interval.
   */
  @Override
  public void refreshPeerAllowlist() {
    final PeerAddressAllowlistFilter filter = allowlistFilter;
    if (filter == null)
      return;

    transport.reconcileAllowlistMembership(getCommittedPeersOrNull());

    filter.proactiveRefresh();
  }
}
