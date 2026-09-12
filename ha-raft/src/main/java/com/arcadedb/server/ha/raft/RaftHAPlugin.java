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
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.monitor.HAReplicationStatsProvider;
import com.arcadedb.server.http.HttpServer;

import io.undertow.server.handlers.PathHandler;
import org.apache.ratis.protocol.RaftPeerId;

import com.arcadedb.database.DatabaseInternal;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * ServerPlugin implementation that bootstraps the Raft-based HA subsystem.
 * Discovered via Java ServiceLoader when either {@code HA_ENABLED=true} or
 * {@code HA_SERVER_LIST} is non-blank (a configured server list implies HA intent).
 */
public class RaftHAPlugin implements HAServerPlugin, HAReplicationStatsProvider {

  private          ArcadeDBServer       server;
  private          ContextConfiguration configuration;
  // Read by concurrent HTTP worker threads (e.g. the readiness probe via getReadinessSignal) while it is
  // (re)assigned by the server startup/shutdown thread, so the reference must be published with volatile.
  private volatile RaftHAServer         raftHAServer;

  // Databases already warned about single-bucket types, so the diagnostic is logged once per
  // database per plugin lifetime instead of on every (re)wrap.
  private final Set<String> warnedSingleBucketDatabases = ConcurrentHashMap.newKeySet();

  /** How often a cluster that cannot use the #7509 compare-and-set may say so. */
  private static final long SECURITY_PRECONDITION_WITHHELD_LOG_THROTTLE_MS = 5 * 60_000L;

  // When the "security changes are replicating without the concurrency check" line was last logged.
  private volatile long lastSecurityPreconditionWithheldLog;

  // Handlers registered by registerAPI() that own a background executor. A fresh RaftHAPlugin
  // instance (and thus fresh handler instances) is created by PluginManager on every server
  // start, so stopService() must close the ones THIS instance created rather than relying on any
  // static/shared state (issue #5890). Unlike raftHAServer above, plain (non-volatile) fields:
  // registerAPI() (called from the thread that invoked ArcadeDBServer.start()) and stopService()
  // (which can run on that same thread OR on the JVM shutdown-hook thread via stopFromShutdownHook())
  // are never concurrent with each other, but the reason is ArcadeDBServer.lifecycleLock - a
  // ReentrantLock wrapping startInternal()/stopInternal() - giving a happens-before edge across
  // threads, not thread confinement. Safe only as long as that lock still wraps both paths.
  private SnapshotHttpHandler       snapshotHttpHandler;
  private PostVerifyDatabaseHandler postVerifyDatabaseHandler;

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.configuration = configuration;
    // The HA verbose level is SCOPE.SERVER and HALog caches it in a static, so it has to be handed the server's
    // configuration here - otherwise the first log call caches whatever a -D happened to say (issue #7233).
    HALog.configure(configuration);
  }

  /**
   * Installs the Raft server this plugin delegates to, without going through {@code startService()} and a real
   * Ratis cluster.
   * <p>
   * Package-private and test-only, the same seam {@code RaftHAServer.setCapabilityProber} is. It exists so the
   * #7511 interlock can be driven through the method an operator's request actually reaches - the HTTP and gRPC
   * control planes both end at {@code replicateSecurityGroups} / {@code replicateSecurityApiTokens} - rather than
   * only through the gate helper those two call. A gate nothing calls is a gate that is not there, and only a test
   * of the caller can tell the difference.
   */
  // @VisibleForTesting
  void setRaftHAServer(final RaftHAServer raftHAServer) {
    this.raftHAServer = raftHAServer;
  }

  @Override
  public PluginInstallationPriority getInstallationPriority() {
    return PluginInstallationPriority.AFTER_HTTP_ON;
  }

  /**
   * Raft activates on classpath presence whenever high availability is requested, explicitly via
   * {@code ha.enabled} or implicitly via a non-blank {@code ha.serverList}. A deployment that configures HA
   * must not additionally have to name this plugin in {@code SERVER_PLUGINS}.
   */
  @Override
  public boolean isAutoDiscovered(final ContextConfiguration configuration) {
    return configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED) || configuration.isHAImplicitlyEnabled();
  }

  @Override
  public void startService() {
    if (!isRaftEnabled()) {
      HALog.log(this, HALog.TRACE, "Raft HA plugin not activated (HA not enabled)");
      return;
    }

    validateConfiguration();

    try {
      raftHAServer = new RaftHAServer(server, configuration);
      // The state machine is fully wired (server + raftHAServer) inside RaftHAServer itself, both at
      // construction and on every HealthMonitor-driven Ratis restart, so no external wiring is needed
      // here (issue #4839).
      raftHAServer.start();

      // Register the database wrapper so the server wraps databases with RaftReplicatedDatabase.
      // A database joining HA is also the natural point to warn (once) about single-bucket types:
      // in a cluster every write lands on the leader, so a single-bucket type serializes concurrent
      // writers on one page and drives the "Concurrent modification on page ..." retry storms.
      server.setDatabaseWrapper(db -> {
        warnIfSingleBucketTypes(db);
        return new RaftReplicatedDatabase(server, db, raftHAServer);
      });

      // Re-wrap any databases that were already loaded before this plugin started
      server.rewrapDatabases();

      // Register this plugin as the HA implementation on the server
      server.setHA(this);

      LogManager.instance().log(this, Level.INFO, "Raft HA plugin started successfully");
    } catch (final IOException e) {
      throw new RuntimeException("Failed to start Raft HA server", e);
    }
  }

  @Override
  public void stopService() {
    if (raftHAServer != null) {
      // Never issue leaveCluster() on shutdown: the K8s auto-leave (formerly in RaftHAServer.stop())
      // silently shrank the committed Raft membership on every pod recreation and the node was never
      // re-added (issue #5275); a duplicate leave here also caused redundant reconfig work and a
      // spurious WARNING on every pod shutdown (issue #4837). Membership changes are explicit-only:
      // POST /api/v1/cluster/leave or DELETE /api/v1/cluster/peer/<id>.
      raftHAServer.stop();
      raftHAServer = null;
    }
    // Clear the wrapper so databases loaded during restart don't capture a stale/null raftHAServer.
    // startService() will set a fresh wrapper and call rewrapDatabases().
    if (server != null)
      server.setDatabaseWrapper(null);

    // Close the handlers' background executors. The null-guards below are not just for the case
    // where registerAPI() never ran (e.g. a discovered-but-never-wired plugin): this same
    // stopService() is invoked TWICE on every HA-enabled shutdown - once via PluginManager.stopPlugins()
    // (RaftHAPlugin is itself a discovered ServerPlugin) and once via ArcadeDBServer.stopInternal()'s
    // direct haServer.stopService() call, since startService() above did server.setHA(this), making
    // ArcadeDBServer.haServer the very same instance. The second call must be a no-op (issue #5890).
    if (snapshotHttpHandler != null) {
      snapshotHttpHandler.close();
      snapshotHttpHandler = null;
    }
    if (postVerifyDatabaseHandler != null) {
      postVerifyDatabaseHandler.close();
      postVerifyDatabaseHandler = null;
    }
  }

  public RaftHAServer getRaftHAServer() {
    return raftHAServer;
  }

  /**
   * Logs a one-time WARNING listing the database's single-bucket types. Single-bucket types cannot
   * spread concurrent writes (round-robin and thread both reduce to bucket 0), so under HA they
   * serialize all leader-side writers onto one page. The same diagnostic is surfaced live in
   * Studio's HA panel via {@link ClusterAlerts}. Logged once per database so a restart/rewrap does
   * not spam the log.
   */
  private void warnIfSingleBucketTypes(final DatabaseInternal db) {
    try {
      if (!warnedSingleBucketDatabases.add(db.getName()))
        return;

      final List<String> singleBucketTypes = ClusterAlerts.findSingleBucketTypes(db);
      if (singleBucketTypes.isEmpty())
        return;

      LogManager.instance().log(this, Level.WARNING,
          "HA database '%s' has %d type(s) backed by a single bucket: %s. In a cluster all writes "
              + "execute on the leader, so these types serialize concurrent writers on the same page and cause "
              + "MVCC retries. Add buckets to an EXISTING type with 'ALTER TYPE <name> BUCKET +<name>_1' (repeat "
              + "once per extra bucket; the bucket is created if it does not exist) and then set "
              + "'ALTER TYPE <name> BucketSelectionStrategy `thread`' to remove the contention.",
          db.getName(), singleBucketTypes.size(), singleBucketTypes);
    } catch (final RuntimeException e) {
      // Diagnostic only: never let it interfere with wrapping a database for HA.
      LogManager.instance().log(this, Level.FINE, "Single-bucket type check skipped for '%s': %s",
          db.getName(), e.getMessage());
    }
  }

  @Override
  public void replicateSecurityUsers(final String usersJsonArray) {
    // Overridden alongside the two-argument form because the interface's default for THIS one is the no-op:
    // inheriting it would make the seed paths - PostAddPeerHandler, ServerControlPlane.connectCluster - stop
    // replicating altogether.
    replicateSecurityUsers(usersJsonArray, null);
  }

  @Override
  public boolean replicateSecurityUsers(final String usersJsonArray, final String expectedFingerprint) {
    if (raftHAServer == null)
      throw new TransactionException("Raft HA server not started");

    final boolean applied;
    try {
      applied = raftHAServer.getTransactionBroker()
          .replicateSecurityUsers(usersJsonArray, preconditionEveryPeerCanRead(expectedFingerprint));
    } catch (final TransactionException e) {
      throw e;
    } catch (final Exception e) {
      throw new TransactionException("Error sending security-users entry via Raft", e);
    }
    return reportSecurityOutcome("users", applied);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Gated on every peer having proved it can decode a {@code SECURITY_GROUPS_ENTRY} (issue #7511). The entry type
   * is new in 26.10.1 and a peer that cannot decode it HALTS rather than skips it, so during a rolling upgrade an
   * ungated group change turned a routine admin action into a partial outage. The gate runs before the broker is
   * handed anything, so a refusal submits nothing.
   */
  @Override
  public void replicateSecurityGroups(final String groupsJson) {
    replicateSecurityGroups(groupsJson, null);
  }

  @Override
  public boolean replicateSecurityGroups(final String groupsJson, final String expectedFingerprint) {
    if (raftHAServer == null)
      throw new TransactionException("Raft HA server not started");

    SecurityEntryCapabilityGate.requireEveryPeerCanDecode(server, raftHAServer, RaftLogEntryType.SECURITY_GROUPS_ENTRY,
        "group document");

    final boolean applied;
    try {
      applied = raftHAServer.getTransactionBroker()
          .replicateSecurityGroups(groupsJson, preconditionEveryPeerCanRead(expectedFingerprint));
    } catch (final TransactionException e) {
      throw e;
    } catch (final Exception e) {
      throw new TransactionException("Error sending security-groups entry via Raft", e);
    }
    return reportSecurityOutcome("groups", applied);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Gated the same way {@link #replicateSecurityGroups} is, and for the same reason (issue #7511). Worth being
   * explicit that this covers a REVOCATION as well as a mint: a revoked token is not revoked anywhere if the entry
   * carrying it halts the nodes that were still serving it.
   */
  @Override
  public void replicateSecurityApiTokens(final String apiTokensJson) {
    replicateSecurityApiTokens(apiTokensJson, null);
  }

  @Override
  public boolean replicateSecurityApiTokens(final String apiTokensJson, final String expectedFingerprint) {
    if (raftHAServer == null)
      throw new TransactionException("Raft HA server not started");

    SecurityEntryCapabilityGate.requireEveryPeerCanDecode(server, raftHAServer,
        RaftLogEntryType.SECURITY_API_TOKENS_ENTRY, "API-token document");

    final boolean applied;
    try {
      applied = raftHAServer.getTransactionBroker()
          .replicateSecurityApiTokens(apiTokensJson, preconditionEveryPeerCanRead(expectedFingerprint));
    } catch (final TransactionException e) {
      throw e;
    } catch (final Exception e) {
      throw new TransactionException("Error sending security-api-tokens entry via Raft", e);
    }
    return reportSecurityOutcome("API-tokens", applied);
  }

  /**
   * The precondition to actually write, which is {@code expectedFingerprint} only while EVERY peer has advertised
   * that it can read one (issue #7509), and null otherwise.
   * <p>
   * This is the gate {@code RaftLogEntryCodec}'s own javadoc demands of any new optional section, and it is not a
   * formality here. A peer that predates the section skips it and installs the document unconditionally, so an
   * ungated precondition during a rolling upgrade would have the losing entry REFUSED on the upgraded nodes and
   * APPLIED on the older one - the security state of the cluster diverging, which is worse than the lost update
   * #7509 is about, because the same credentials then resolve differently depending on which node answers.
   * Withholding the precondition instead keeps the pre-#7509 behaviour uniformly until the last node is upgraded,
   * at which point the compare-and-set starts working on its own with no operator step.
   * <p>
   * Read per submission rather than cached, exactly as {@code RaftReplicatedDatabase.schemaDeltaEnabled} reads it:
   * a peer that stops answering stops receiving preconditions from the next mutation on, and one that finishes
   * upgrading starts receiving them without a leader restart.
   */
  private String preconditionEveryPeerCanRead(final String expectedFingerprint) {
    return expectedFingerprint == null ?
        null :
        preconditionForPeers(expectedFingerprint,
            raftHAServer.peersMissingCapability(PeerCapabilities.SECURITY_PRECONDITION));
  }

  /**
   * The decision {@link #preconditionEveryPeerCanRead} makes, separated from the peer lookup it makes it on so it
   * can be driven directly: a precondition is written only when NO peer is missing the capability.
   * Package-private for tests.
   */
  String preconditionForPeers(final String expectedFingerprint, final List<String> peersMissingTheCapability) {
    if (expectedFingerprint == null)
      return null;
    if (peersMissingTheCapability.isEmpty())
      return expectedFingerprint;

    logSecurityPreconditionWithheld(peersMissingTheCapability);
    return null;
  }

  /**
   * Reports, at most once per {@link #SECURITY_PRECONDITION_WITHHELD_LOG_THROTTLE_MS}, that the compare-and-set is
   * not engaged and WHICH peers are the reason.
   * <p>
   * Silence would put an operator back where issue #7509 found them: believing concurrent security changes are safe
   * while they are not. Throttled because a cluster left half-upgraded is a steady state, not an event.
   */
  private void logSecurityPreconditionWithheld(final List<String> peersMissingTheCapability) {
    final long now = System.currentTimeMillis();
    if (now - lastSecurityPreconditionWithheldLog < SECURITY_PRECONDITION_WITHHELD_LOG_THROTTLE_MS)
      return;
    // Racy by design: two administrators crossing the window at the same instant cost one duplicate line.
    lastSecurityPreconditionWithheldLog = now;
    LogManager.instance().log(this, Level.INFO,
        "Security changes are replicating WITHOUT the concurrency check of issue #7509: peer(s) %s have not "
            + "advertised the '%s' capability, so a precondition could not be read there. Until they are upgraded "
            + "or reachable again, two security changes made on two nodes at the same time can still lose one of "
            + "them - the pre-#7509 behaviour. Nothing has to be turned on afterwards; the check resumes by itself.",
        peersMissingTheCapability, PeerCapabilities.SECURITY_PRECONDITION);
  }

  /**
   * Logs the outcome of a security entry (issue #7509).
   * <p>
   * The catch-up wait a refused submitter needs before it retries is deliberately NOT done here: this method runs
   * with the caller's {@code ServerSecurity} monitor held, and that monitor is shared by every cluster-wide
   * security mutation on the node. {@link #awaitLocalApply()} is called instead by
   * {@code ServerSecurity.awaitSupersededChange}, outside the monitor.
   */
  private boolean reportSecurityOutcome(final String document, final boolean applied) {
    if (applied) {
      LogManager.instance().log(this, Level.INFO, "Security %s entry committed via Raft", document);
      return true;
    }

    LogManager.instance().log(this, Level.INFO,
        "Security %s entry was refused: the document changed between this node's read and the apply, so the "
            + "caller retries against the current document", document);
    return false;
  }

  /**
   * Waits, bounded by {@code arcadedb.ha.quorumTimeout}, for this node's state machine to catch up with the
   * committed log (issue #7509). Best-effort: {@code waitForLocalApply()} returns rather than failing when the
   * deadline passes. Safe to block in, because the state-machine apply thread never takes the
   * {@code ServerSecurity} monitor - and the caller does not hold it here anyway.
   */
  @Override
  public void awaitLocalApply() {
    final RaftHAServer raft = raftHAServer;
    if (raft != null)
      raft.waitForLocalApply();
  }

  @Override
  public void registerAPI(final HttpServer httpServer, final PathHandler routes) {
    // Always register the endpoint - it returns 503 when Raft is not yet started.
    // Note: registerAPI is called before configure()/startService() for AFTER_HTTP_ON plugins,
    // so isRaftEnabled() cannot be checked here.
    routes.addExactPath("/api/v1/cluster", new GetClusterHandler(httpServer, this));
    LogManager.instance().log(this, Level.INFO, "Raft cluster status endpoint registered at /api/v1/cluster");
    // Close a previous handler before replacing it: registerAPI() is expected to run once per plugin
    // instance (a fresh RaftHAPlugin is created on every server start), but closing defensively here
    // means a repeated call can never re-leak the executor regardless of that caller-side invariant
    // (issue #5890).
    if (snapshotHttpHandler != null)
      snapshotHttpHandler.close();
    snapshotHttpHandler = new SnapshotHttpHandler(httpServer);
    routes.addPrefixPath("/api/v1/ha/snapshot/", snapshotHttpHandler);
    LogManager.instance().log(this, Level.INFO, "Raft snapshot endpoint registered at /api/v1/ha/snapshot/{database}");
    routes.addExactPath("/api/v1/cluster/peer", new PostAddPeerHandler(httpServer, this));
    routes.addPrefixPath("/api/v1/cluster/peer/", new DeletePeerHandler(httpServer, this));
    routes.addExactPath("/api/v1/cluster/leader", new PostTransferLeaderHandler(httpServer, this));
    routes.addExactPath("/api/v1/cluster/stepdown", new PostStepDownHandler(httpServer, this));
    routes.addExactPath("/api/v1/cluster/leave", new PostLeaveHandler(httpServer, this));
    // Same defensive close as snapshotHttpHandler above (issue #5890).
    if (postVerifyDatabaseHandler != null)
      postVerifyDatabaseHandler.close();
    postVerifyDatabaseHandler = new PostVerifyDatabaseHandler(httpServer, this);
    routes.addPrefixPath("/api/v1/cluster/verify/", postVerifyDatabaseHandler);
    routes.addPrefixPath("/api/v1/cluster/resync/", new PostResyncDatabaseHandler(httpServer, this));
    // Issue #4147: pre-bootstrap state RPC, used by the bootstrap leader at first cluster
    // formation to collect each peer's (fingerprint, lastTxId) per database.
    routes.addExactPath("/api/v1/cluster/bootstrap-state", new PostBootstrapStateHandler(httpServer, this));
    // Issue #7219: peer-capability advertisement RPC, polled by the leader so it can decide for itself whether
    // an optional wire-format section is safe to write. A node predating this route answers 404, and that 404 is
    // the answer - see PostCapabilitiesHandler.
    routes.addExactPath("/api/v1/cluster/capabilities", new PostCapabilitiesHandler(httpServer, this));
    LogManager.instance().log(this, Level.INFO, "Raft cluster management endpoints registered");
  }

  @Override
  public boolean isLeader() {
    return raftHAServer != null && raftHAServer.isLeader();
  }

  @Override
  public HAReplicationStats getHAReplicationStats() {
    final RaftHAServer s = raftHAServer;
    return s != null ? s.getReplicationStats() : new HAReplicationStats(false, -1, -1, 0);
  }

  @Override
  public List<FollowerSample> getFollowerSamples() {
    final RaftHAServer s = raftHAServer;
    return s != null ? s.getFollowerSamples() : List.of();
  }

  @Override
  public PendingPhase2Stats getPendingPhase2Stats() {
    final RaftHAServer s = raftHAServer;
    return s != null ? s.getPendingPhase2Stats() : new PendingPhase2Stats(0, 0, -1);
  }

  @Override
  public List<SchemaInstalmentSample> getSchemaInstalmentSamples() {
    final RaftHAServer s = raftHAServer;
    return s != null ? s.getSchemaInstalmentSamples() : List.of();
  }

  @Override
  public List<UnreferencedFilesSample> getUnreferencedFilesSamples() {
    final RaftHAServer s = raftHAServer;
    return s != null ? s.getUnreferencedFilesSamples() : List.of();
  }

  @Override
  public String getLeaderName() {
    return raftHAServer != null ? raftHAServer.getLeaderName() : null;
  }

  @Override
  public HAServerPlugin.ELECTION_STATUS getElectionStatus() {
    if (raftHAServer == null)
      return ELECTION_STATUS.DONE;
    return raftHAServer.getLeaderId() != null ? ELECTION_STATUS.DONE : ELECTION_STATUS.VOTING_FOR_ME;
  }

  @Override
  public HAServerPlugin.READINESS_SIGNAL getReadinessSignal(final long maxLagEntries) {
    final RaftHAServer s = raftHAServer;
    // Raft not started yet means this node has not joined the consensus group, so it is not ready.
    final boolean ready = s != null && s.isReadyForTraffic(maxLagEntries);
    return ready ? READINESS_SIGNAL.READY : READINESS_SIGNAL.NOT_READY;
  }

  @Override
  public String getClusterToken() {
    return raftHAServer != null ? raftHAServer.getClusterToken() : null;
  }

  @Override
  public String getClusterName() {
    return raftHAServer != null ? raftHAServer.getClusterName() : null;
  }

  @Override
  public HAServerPlugin.PeerAuthSession lookupAuthSession(final String issuerServerName, final String token)
      throws IOException {
    final RaftHAServer raft = raftHAServer;
    if (raft == null)
      throw new IOException("Raft HA is not started");
    final RaftPeerId issuer = raft.resolvePeerIdByServerName(issuerServerName);
    // Not a member, or this node itself (which does not hold the token, or it would not be asking): definitive.
    if (issuer == null || issuer.equals(raft.getLocalPeerId()))
      return null;
    return PeerAuthSessionQuery.validate(raft, issuer, token, authSessionRpcTimeoutMs());
  }

  @Override
  public void revokeAuthSession(final String token) {
    final RaftHAServer raft = raftHAServer;
    if (raft == null)
      return;
    PeerAuthSessionQuery.revokeEverywhere(raft, token, authSessionRpcTimeoutMs());
  }

  /**
   * Budget for one authentication-session RPC to a peer: the leader proxy's connect timeout, because the RPC is
   * one small request on a LAN and the caller is a client waiting on a 401-or-200 decision.
   */
  private long authSessionRpcTimeoutMs() {
    return server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT);
  }

  @Override
  public Map<String, Object> getStats() {
    return raftHAServer != null ? raftHAServer.getStats() : Collections.emptyMap();
  }

  @Override
  public int getConfiguredServers() {
    return raftHAServer != null ? raftHAServer.getConfiguredServers() : 1;
  }

  @Override
  public String getLeaderAddress() {
    return raftHAServer != null ? raftHAServer.getLeaderHttpAddress() : null;
  }

  /**
   * The HTTPS endpoint a forward to the leader should prefer, or {@code null} when the plain-HTTP one is what
   * there is (issue #7508). The policy - SSL on, an HTTPS endpoint that resolves, and not this node's own - lives
   * in {@link RaftHAServer#getLeaderHttpsAddress()}, next to the resolver it reads.
   */
  @Override
  public String getLeaderHttpsAddress() {
    return raftHAServer != null ? raftHAServer.getLeaderHttpsAddress() : null;
  }

  /**
   * The HTTPS client a forward to {@link #getLeaderHttpsAddress()} is sent on: this node's truststore, so the
   * leader's certificate is validated against the cluster's trust anchors and not against this node's own key
   * material - the same context {@code SnapshotInstaller}, the capability probe and the bootstrap-state query
   * already use (issue #4470).
   */
  @Override
  public HttpClient getPeerHttpsClient() throws IOException {
    return raftHAServer != null ? raftHAServer.getForwardHttpsClient() : null;
  }

  @Override
  public String getReplicaAddresses() {
    return raftHAServer != null ? raftHAServer.getReplicaAddresses() : "";
  }

  @Override
  public boolean isOwnHttpAddress(final String address) {
    return raftHAServer != null && raftHAServer.isOwnHttpAddress(address);
  }

  @Override
  public RoutingTable getRoutingTable(final ROUTING_PROTOCOL protocol) {
    return raftHAServer != null ? raftHAServer.getRoutingTable(protocol) : null;
  }

  @Override
  public void shutdownRemoteServer(final String serverName) {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");

    String targetAddr = null;
    for (final var peer : raftHAServer.getRaftGroup().getPeers()) {
      final String httpAddr = raftHAServer.getHttpAddresses().get(peer.getId());
      if (httpAddr != null && (peer.getId().toString().contains(serverName) || httpAddr.contains(serverName))) {
        targetAddr = httpAddr;
        break;
      }
    }
    if (targetAddr == null)
      throw new ServerException("Cannot find server '" + serverName + "' in the cluster");

    try {
      final HttpURLConnection conn = (HttpURLConnection)
          new URL("http://" + targetAddr + "/api/v1/server").openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");

      final String token = raftHAServer.getClusterToken();
      if (token != null && !token.isEmpty())
        conn.setRequestProperty("Authorization", "Bearer " + token);

      conn.getOutputStream().write("{\"command\":\"shutdown\"}".getBytes(StandardCharsets.UTF_8));
      conn.getResponseCode();
      conn.disconnect();
    } catch (final IOException e) {
      throw new RuntimeException("Failed to shutdown remote server '" + serverName + "'", e);
    }
  }

  @Override
  public void disconnectCluster() {
    if (raftHAServer != null)
      raftHAServer.stop();
  }

  /**
   * Joins the server named by {@code serverAddress} to this cluster (issue #7401).
   * <p>
   * The whole of it is {@code RaftClusterManager.addPeer} with the peer derived from one
   * {@code arcadedb.ha.serverList} entry, which is what makes the verb a thin alias for
   * {@code POST /api/v1/cluster/peer} rather than a second way to grow a cluster: the membership change
   * is the same atomic {@code Mode.ADD}, issued by the same {@code RaftClusterManager}, with the same
   * retry and the same idempotence when the peer is already a member.
   * <p>
   * It carries one thing that route cannot: the leader-election <b>priority</b>, which the object form
   * and the four-field positional form of a server-list entry can declare and the add-peer payload has
   * no field for. That is why the parsed {@link org.apache.ratis.protocol.RaftPeer} is handed over
   * whole rather than as an id and an address.
   * <p>
   * Not leader-routed, matching the add-peer route and {@code PostServerCommandHandler}, which forwards
   * neither half of the cluster pair: the Ratis client underneath {@code addPeer} sends the
   * configuration change to the leader itself.
   */
  @Override
  public void connectCluster(final String serverAddress) {
    final RaftHAServer raft = raftHAServer;
    if (raft == null)
      throw new ServerException("Raft HA server not started");

    final RaftPeerAddressResolver.JoinTarget target = RaftPeerAddressResolver.parseJoinTarget(serverAddress,
        configuration.getValueAsInteger(GlobalConfiguration.HA_RAFT_PORT),
        configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S)
            ? configuration.getValueAsString(GlobalConfiguration.HA_K8S_DNS_SUFFIX)
            : "");

    // The peer goes in whole, not as an id and an address: it also carries the leader-election
    // priority the entry may have declared, and rebuilding it from parts is how that gets lost.
    final RaftPeerId peerId = target.peer().getId();
    raft.addPeer(target.peer(), target.name());

    // After addPeer, not before: RaftClusterManager.addPeer derives an HTTP address from the Raft port
    // plus THIS node's HTTP offset, which is right only for a homogeneous cluster. An entry that
    // declared its own HTTP port said so, and that answer wins over the derived one.
    if (target.httpAddress() != null)
      raft.getHttpAddresses().put(peerId, target.httpAddress());
  }

  @Override
  public void addPeer(final String peerId, final String address) {
    addPeer(peerId, address, null);
  }

  @Override
  public void addPeer(final String peerId, final String address, final String name) {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");
    raftHAServer.addPeer(peerId, address, name);
  }

  @Override
  public void removePeer(final String peerId) {
    removePeer(peerId, false);
  }

  @Override
  public void removePeer(final String peerId, final boolean force) {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");
    raftHAServer.removePeer(peerId, force);
  }

  @Override
  public void transferLeadership(final String targetPeerId, final long timeoutMs) {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");
    raftHAServer.transferLeadership(targetPeerId, timeoutMs);
  }

  @Override
  public void stepDown() {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");
    raftHAServer.stepDown();
  }

  @Override
  public void leaveCluster() {
    leaveCluster(false);
  }

  @Override
  public void leaveCluster(final boolean force) {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");
    raftHAServer.leaveCluster(force);
  }

  private boolean isRaftEnabled() {
    return configuration != null
        && (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)
            || configuration.isHAImplicitlyEnabled());
  }

  private void validateConfiguration() {
    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    if (serverList == null || serverList.isBlank())
      throw new RuntimeException("HA_SERVER_LIST must be configured for Raft HA");

    // Validate quorum early - will throw ConfigurationException for invalid values
    final Quorum quorum = Quorum.parse(configuration.getValueAsString(GlobalConfiguration.HA_QUORUM));

    final int serverCount = serverList.split(",").length;
    if (quorum == Quorum.ALL && serverCount > 3)
      LogManager.instance().log(this, Level.WARNING,
          "HA_QUORUM=ALL with %d nodes: every node must acknowledge writes. A single slow node will throttle the cluster.",
          serverCount);
  }
}
