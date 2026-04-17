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
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.TimeDuration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
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
public class RaftHAServer {

  // Client retry policy for the RaftClient used to submit transactions.
  // Exponential backoff from 100ms to 5s covers transient leader unavailability.
  private static final long CLIENT_RETRY_BASE_SLEEP_MS = 100L;
  private static final long CLIENT_RETRY_MAX_SLEEP_SECS = 5L;

  // These fields are set once in configure() and never changed after. They cannot be final because
  // ServiceLoader requires a no-arg constructor, and configure() is called separately by the PluginManager.
  private              ArcadeDBServer       server;
  private              ContextConfiguration configuration;
  private              RaftGroup            raftGroup;
  private              RaftPeerId           localPeerId;
  private              Quorum               quorum;
  private              long                 quorumTimeout;
  private              RaftPeerAddressResolver addressResolver;
  private              ClusterTokenProvider tokenProvider;
  private              boolean              active;

  private          RaftServer               raftServer;
  private volatile RaftClient               raftClient;
  private          RaftProperties           raftProperties;
  private          Parameters               raftParameters;
  private volatile ArcadeDBStateMachine     stateMachine;
  private          ClusterMonitor           clusterMonitor;
  private final    ReentrantLock            applyLock            = new ReentrantLock();
  private final    Condition                applyCondition       = applyLock.newCondition();
  private final    AtomicInteger            applyWaiterCount     = new AtomicInteger();
  private final    Object                   leaderReadyNotifier  = new Object();
  /**
   * True when this node is ready to serve reads. Initialized to true because a node starts
   * as a follower (followers don't gate reads on this flag). Set to false during the catch-up
   * window after winning an election, then restored to true once the state machine has applied
   * all committed entries. Reads on the leader wait for this flag via {@link #waitForLeaderReady()},
   * preventing stale reads during leadership transitions.
   */
  private volatile boolean                  leaderReady          = true;
  private          HealthMonitor            healthMonitor;
  private volatile int                      restartFailureCount;

  // Extracted collaborators (created in startService)
  private          RaftClusterManager       clusterManager;
  private          RaftTransactionBroker    transactionBroker;
  private          RaftClusterStatusExporter statusExporter;

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

  public void configure(final ArcadeDBServer server, final ContextConfiguration configuration) {
    if (!configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
      return;

    this.active = true;
    this.server = server;
    this.configuration = configuration;
    this.quorum = Quorum.parse(configuration.getValueAsString(GlobalConfiguration.HA_QUORUM));
    this.quorumTimeout = configuration.getValueAsLong(GlobalConfiguration.HA_QUORUM_TIMEOUT);
    this.addressResolver = new RaftPeerAddressResolver(server, configuration);
    this.tokenProvider = new ClusterTokenProvider(configuration);

    // Parse peers from HA_SERVER_LIST
    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    if (serverList == null || serverList.isEmpty())
      throw new ConfigurationException("HA server list (arcadedb.ha.serverList) is required for Ratis HA");

    final List<RaftPeer> peers = addressResolver.parsePeers(serverList);
    this.localPeerId = addressResolver.resolveLocalPeerId(peers);

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

    // Register the database wrapper with the server
    server.setDatabaseWrapper(db -> new ReplicatedDatabase(server, db));
  }

  public boolean isActive() {
    return active;
  }

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
            incomingHost, RaftPeerAddressResolver.parseFirstPort(configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS)));
    }

    // Derive the cluster token eagerly at startup rather than lazily on the first request.
    // PBKDF2 with 100k iterations is expensive and would block a request thread.
    tokenProvider.initClusterToken();

    try {
      stateMachine = new ArcadeDBStateMachine(server, this);

      this.raftProperties = RaftPropertiesBuilder.build(configuration, server.getRootPath(),
          localPeerId.toString(), quorumTimeout);
      this.raftParameters = RaftPropertiesBuilder.buildParameters(configuration);
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
          .setParameters(raftParameters)
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
        new KubernetesAutoJoin(server, raftGroup, localPeerId, raftProperties).tryAutoJoin();

      // Create extracted collaborators
      clusterManager = new RaftClusterManager(this, addressResolver, clusterMonitor, raftProperties);
      transactionBroker = new RaftTransactionBroker(this);
      statusExporter = new RaftClusterStatusExporter(this, clusterMonitor, configuration);

      transactionBroker.startGroupCommitter(
          configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_BATCH_SIZE),
          configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_QUEUE_SIZE),
          configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_OFFER_TIMEOUT));
      statusExporter.startLagMonitor();
      startRatisHealthMonitor();

      LogManager.instance().log(this, Level.INFO, "Ratis HA service started (serverId=%s)", localPeerId);

    } catch (final IOException e) {
      throw new ConfigurationException("Failed to start Ratis HA service", e);
    }
  }

  /**
   * Returns the lifecycle state of the Ratis server (RUNNING, CLOSING, CLOSED, etc.).
   */
  public LifeCycle.State getRaftLifeCycleState() {
    if (raftServer == null)
      return LifeCycle.State.CLOSED;
    try {
      return raftServer.getDivision(raftGroup.getGroupId()).getInfo().getLifeCycleState();
    } catch (final Exception e) {
      return raftServer.getLifeCycleState();
    }
  }

  /**
   * Checks if the Ratis server is in a terminal state and restarts it if needed.
   * <p>
   * Thread safety: the method is {@code synchronized} on this instance, and the caller
   * ({@link HealthMonitor}) runs on a single-threaded scheduled executor. Both guards
   * prevent concurrent restarts: the executor serializes health-check ticks, and the
   * lock blocks any other caller until the restart (synchronous, not spawned as a thread)
   * completes. The next tick then observes the new healthy state and returns immediately.
   */
  public synchronized void restartRatisIfNeeded() {
    if (raftServer == null)
      return;

    // Check the group-specific RaftServerImpl state, not the RaftServerProxy state.
    // The proxy can be RUNNING while the inner group impl is CLOSED after a network partition.
    LifeCycle.State state;
    try {
      state = raftServer.getDivision(raftGroup.getGroupId()).getInfo().getLifeCycleState();
    } catch (final Exception e) {
      // getDivision can throw if the group is already removed
      state = raftServer.getLifeCycleState();
    }
    if (state != LifeCycle.State.CLOSED && state != LifeCycle.State.CLOSING) {
      restartFailureCount = 0; // Reset on healthy state
      return;
    }

    // After N consecutive failures, the problem is persistent (port conflict, bad storage,
    // full disk). Stop the server so the cluster can heal (other nodes take over) and
    // orchestrators (K8s, systemd) can restart the process with a clean state.
    final int maxRetries = server.getConfiguration().getValueAsInteger(GlobalConfiguration.HA_RATIS_RESTART_MAX_RETRIES);
    if (restartFailureCount >= maxRetries) {
      LogManager.instance().log(this, Level.SEVERE,
          "Ratis restart failed %d consecutive times (max=%d). Stopping server for cluster-level recovery",
          restartFailureCount, maxRetries);
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
      // Any transactions currently in EntryState.DISPATCHED (sent to Raft but not yet acknowledged)
      // will receive errors when the client is closed. These transactions may have already been
      // committed on a majority of replicas, so the calling thread will see QuorumNotReachedException
      // even though the data is durably replicated. After restart, Ratis replays committed entries
      // via applyTransactionEntry(), which applies them correctly since isLeader() returns false.
      LogManager.instance().log(this, Level.WARNING,
          "Closing Ratis client/server for restart - in-flight transactions may report failure despite being committed on replicas");
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
      stateMachine = new ArcadeDBStateMachine(server, this);

      raftServer = RaftServer.newBuilder()
          .setServerId(localPeerId)
          .setStateMachine(stateMachine)
          .setProperties(raftProperties)
          .setParameters(raftParameters)
          .setGroup(raftGroup)
          .setOption(RaftStorage.StartupOption.RECOVER)
          .build();

      raftServer.start();

      raftClient = buildRaftClient();

      restartFailureCount = 0;
      LogManager.instance().log(this, Level.INFO, "Ratis server restarted successfully after partition recovery");

    } catch (final Exception e) {
      restartFailureCount++;
      if (restartFailureCount >= maxRetries)
        LogManager.instance().log(this, Level.SEVERE,
            "Failed to restart Ratis server after %d attempts (max=%d). Giving up - manual restart required: %s",
            restartFailureCount, maxRetries, e.getMessage());
      else
        LogManager.instance().log(this, Level.WARNING,
            "Failed to restart Ratis server (attempt %d/%d): %s", restartFailureCount, maxRetries, e.getMessage());
    }
  }

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

    if (transactionBroker != null)
      transactionBroker.stopGroupCommitter();
    if (statusExporter != null)
      statusExporter.stopLagMonitor();
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

  public void leaveCluster() {
    if (clusterManager != null)
      clusterManager.leaveCluster();
  }

  // -- Transaction Submission (delegated to RaftTransactionBroker) --

  public void replicateRawEntry(final byte[] entry) {
    transactionBroker.replicateRawEntry(entry);
  }

  public void replicateCreateDatabase(final String databaseName) {
    transactionBroker.replicateCreateDatabase(databaseName);
  }

  public void replicateDropDatabase(final String databaseName) {
    transactionBroker.replicateDropDatabase(databaseName);
  }

  public void replicateCreateUser(final String userJson) {
    transactionBroker.replicateCreateUser(userJson);
  }

  public void replicateUpdateUser(final String userJson) {
    transactionBroker.replicateUpdateUser(userJson);
  }

  public void replicateDropUser(final String userName) {
    transactionBroker.replicateDropUser(userName);
  }

  public void replicateTransaction(final String databaseName, final Map<Integer, Integer> bucketRecordDelta,
                                   final Binary walBuffer, final String schemaJson,
                                   final Map<Integer, String> filesToAdd,
                                   final Map<Integer, String> filesToRemove) {
    transactionBroker.replicateTransaction(databaseName, bucketRecordDelta, walBuffer, schemaJson, filesToAdd, filesToRemove);
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
    synchronized (leaderReadyNotifier) {
      while (!leaderReady) {
        final long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0)
          break;
        try {
          leaderReadyNotifier.wait(remaining);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
    if (!leaderReady)
      LogManager.instance().log(this, Level.WARNING, "waitForLeaderReady timed out after %dms - proceeding with potentially stale leader state", quorumTimeout);
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
    final long readIndex = fetchReadIndex(true);
    // Reply success means the leader lease is valid and the read index is confirmed.
    // Double-check we're still the leader: after SIGSTOP/SIGCONT, the sendReadOnly heartbeat
    // might briefly succeed before the old leader fully steps down.
    if (!isLeader())
      throw new ServerIsNotTheLeaderException("Leadership lost after read index confirmation",
          getLeaderHTTPAddress());
    if (readIndex > 0)
      waitForAppliedIndex(readIndex);
  }

  /**
   * Linearizable read barrier for a follower. Uses Ratis {@code sendReadOnly} to obtain the
   * leader's current committed index (the leader verifies it still holds a quorum during the
   * call), then waits for the local state machine to catch up to that index. After this
   * returns, any read served from local state reflects every write committed before the call.
   * <p>
   * Cost: one follower-to-leader RTT + the leader's quorum heartbeat (amortized across
   * concurrent ReadIndex calls by Ratis) + local apply-lag catch-up time. This is strictly
   * more expensive than {@code waitForLocalApply()} because the local commit index on a
   * follower may trail the leader's; without this round-trip a lagging follower would serve
   * stale reads even when labelled LINEARIZABLE.
   */
  public void ensureLinearizableFollowerRead() {
    if (raftClient == null)
      throw new ReplicationException("Raft client not initialized on follower");
    final long readIndex = fetchReadIndex(false);
    if (readIndex > 0)
      waitForAppliedIndex(readIndex);
  }

  /**
   * Issues a Ratis {@code sendReadOnly} call to obtain the current committed log index from
   * the leader. Returns the log index on success. Classifies failures depending on
   * {@code expectSelfIsLeader}: the leader-side caller wants a {@link ServerIsNotTheLeaderException}
   * with a redirect hint when the RPC reports a different leader; the follower-side caller
   * surfaces the same situation as a generic {@link ReplicationException} because it never
   * expected to be the leader.
   */
  private long fetchReadIndex(final boolean expectSelfIsLeader) {
    try {
      final RaftClientReply reply = raftClient.async()
          .sendReadOnly(Message.valueOf(ByteString.EMPTY))
          .get(quorumTimeout, TimeUnit.MILLISECONDS);
      if (!reply.isSuccess()) {
        final var ex = reply.getException();
        if (ex instanceof org.apache.ratis.protocol.exceptions.NotLeaderException nle && expectSelfIsLeader) {
          final var suggestedLeader = nle.getSuggestedLeader();
          throw new ServerIsNotTheLeaderException("Not the leader (detected via read index)",
              suggestedLeader != null ? addressResolver.getPeerHTTPAddress(suggestedLeader.getId()) : null);
        }
        throw new ReplicationException("Linearizable read check failed: "
            + (ex != null ? ex.getMessage() : "unknown"));
      }
      return reply.getLogIndex();
    } catch (final ServerIsNotTheLeaderException | ReplicationException e) {
      throw e;
    } catch (final java.util.concurrent.TimeoutException e) {
      HALog.log(this, HALog.BASIC, "ReadIndex RPC timed out after %dms (expectSelfIsLeader=%s)",
          quorumTimeout, expectSelfIsLeader);
      throw new ReplicationException("Linearizable read timed out after " + quorumTimeout + "ms");
    } catch (final Exception e) {
      if (e.getCause() instanceof org.apache.ratis.protocol.exceptions.NotLeaderException nle && expectSelfIsLeader) {
        final var suggestedLeader = nle.getSuggestedLeader();
        throw new ServerIsNotTheLeaderException("Not the leader (detected via read index)",
            suggestedLeader != null ? addressResolver.getPeerHTTPAddress(suggestedLeader.getId()) : null);
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
    // Synchronized on 'this' because restartRatisIfNeeded() holds the same monitor
    // when it closes the old state machine and creates a new one. Reading and
    // submitting to the lifecycle executor inside the same synchronized block
    // prevents use-after-close: the executor cannot be shut down between the
    // field read and the submit() call.
    synchronized (this) {
      final var currentStateMachine = stateMachine;
      if (isLeader() && currentStateMachine != null) {
        // New leader must apply all committed entries before serving reads.
        // Mark as not ready; the catch-up runs in the background to avoid blocking
        // the Ratis event thread (which processes heartbeats and elections).
        leaderReady = false;
        HALog.log(this, HALog.BASIC, "This node became leader, scheduling state machine catch-up in background");
        try {
          currentStateMachine.getLifecycleExecutor().submit(() -> {
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
              synchronized (leaderReadyNotifier) {
                leaderReadyNotifier.notifyAll();
              }
            }
          });
        } catch (final java.util.concurrent.RejectedExecutionException rex) {
          // The lifecycle executor has been shut down (e.g. by a concurrent
          // restartRatisIfNeeded()). We cannot run the catch-up, but we must not leave
          // leaderReady stuck at false - otherwise every subsequent read would block
          // on a state machine that will never come online. Restore leaderReady and
          // wake any waiters so they can observe the new (shutdown) state via their
          // normal timeouts.
          leaderReady = true;
          LogManager.instance().log(this, Level.WARNING,
              "Could not schedule leader read-barrier catch-up: state machine lifecycle executor is shut down (%s). "
                  + "Restoring leaderReady so subsequent reads are not stuck; this usually indicates a concurrent Ratis restart.",
              null, rex.getMessage());
          synchronized (leaderReadyNotifier) {
            leaderReadyNotifier.notifyAll();
          }
        }
      } else {
        leaderReady = true;
      }
    }
    // Notify leaveCluster() waiters (which loop on isLeader()) so they can exit
    // as soon as leadership is transferred to another node.
    if (clusterManager != null)
      clusterManager.notifyLeaderChangeForLeave();
    if (statusExporter != null)
      statusExporter.printClusterConfiguration();
  }

  // -- Status Export (delegated to RaftClusterStatusExporter) --

  public void printClusterConfiguration() {
    if (statusExporter != null)
      statusExporter.printClusterConfiguration();
  }

  public List<Map<String, Object>> getFollowerStates() {
    return statusExporter != null ? statusExporter.getFollowerStates() : List.of();
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

  public JSONObject exportClusterStatus() {
    return statusExporter.exportClusterStatus();
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
        .setParameters(raftParameters)
        .setRetryPolicy(ExponentialBackoffRetry.newBuilder()
            .setBaseSleepTime(TimeDuration.valueOf(CLIENT_RETRY_BASE_SLEEP_MS, TimeUnit.MILLISECONDS))
            .setMaxSleepTime(TimeDuration.valueOf(CLIENT_RETRY_MAX_SLEEP_SECS, TimeUnit.SECONDS))
            .build())
        .build();
  }

  // -- Cluster Token (delegated to ClusterTokenProvider) --

  public String getClusterToken() {
    return tokenProvider.getClusterToken();
  }

  // -- Ratis Health Monitor --

  private void startRatisHealthMonitor() {
    healthMonitor = new HealthMonitor(server, this::restartRatisIfNeeded);
    healthMonitor.start();
  }

  private void stopHealthMonitor() {
    if (healthMonitor != null) {
      healthMonitor.stop();
      healthMonitor = null;
    }
  }

  public ClusterMonitor getClusterMonitor() {
    return clusterMonitor;
  }

  // -- Dynamic Membership (delegated to RaftClusterManager) --

  public void addPeer(final String peerId, final String address, final String httpAddress) {
    clusterManager.addPeer(peerId, address, httpAddress);
  }

  public void addPeer(final String peerId, final String address) {
    clusterManager.addPeer(peerId, address);
  }

  public void removePeer(final String peerId) {
    clusterManager.removePeer(peerId);
  }

  public void transferLeadership(final String targetPeerId, final long timeoutMs) {
    clusterManager.transferLeadership(targetPeerId, timeoutMs);
  }

  public void stepDown() {
    clusterManager.stepDown();
  }

  // -- Snapshot --

  /**
   * Returns the HTTP address of a peer given its Raft peer ID.
   * Delegates to {@link RaftPeerAddressResolver#getPeerHTTPAddress(RaftPeerId)}.
   */
  public String getPeerHTTPAddress(final RaftPeerId peerId) {
    return addressResolver.getPeerHTTPAddress(peerId);
  }

  /**
   * Returns the HTTP address for the current leader.
   */
  public String getLeaderHTTPAddress() {
    return addressResolver.getLeaderHTTPAddress(getLeaderName());
  }

  /**
   * Delegates to {@link ClusterTokenProvider#initClusterTokenForTest(ContextConfiguration)}.
   */
  static void initClusterTokenForTest(final ContextConfiguration config) {
    ClusterTokenProvider.initClusterTokenForTest(config);
  }

  /**
   * Returns the index of the last separator character ({@code '_'} or {@code '-'}) in a server
   * name such as {@code "ArcadeDB_0"} or {@code "arcadedb-1"}. Used to extract the numeric suffix
   * (server index) from node names generated by test harnesses or Kubernetes StatefulSets.
   *
   * @throws IllegalArgumentException if the name has no separator or the separator is the last character
   */
  public static int findLastSeparatorIndex(final String name) {
    final int underscore = name.lastIndexOf('_');
    final int hyphen = name.lastIndexOf('-');
    final int idx = Math.max(underscore, hyphen);
    if (idx < 0)
      throw new IllegalArgumentException("Server name has no '_' or '-' separator: " + name);
    if (idx == name.length() - 1)
      throw new IllegalArgumentException("Server name separator is the last character: " + name);
    return idx;
  }

}
