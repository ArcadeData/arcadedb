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

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.LocalResyncState;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.monitor.HAReplicationStatsProvider;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;

/**
 * Returns Raft cluster status: local peer, leader, and peer list with roles.
 * Registered at {@code GET /api/v1/cluster} by {@link RaftHAPlugin}.
 * <p>
 * Holds a reference to the plugin (not the server) because the endpoint is registered
 * during HTTP setup, before the Raft server is started. The actual {@link RaftHAServer}
 * is resolved lazily at request time via {@link RaftHAPlugin#getRaftHAServer()}.
 */
public class GetClusterHandler extends AbstractServerHttpHandler {

  /** Per-peer timeout for the opt-in presence fan-out; kept short so a hung peer cannot block the worker thread. */
  private static final long PRESENCE_QUERY_TIMEOUT_MS = 5_000L;

  private final RaftHAPlugin plugin;

  /**
   * {@code role} of a peer that {@code arcadedb.ha.serverList} declares but the live Raft configuration does not
   * contain (issue #7040). The other two values are {@code LEADER} and {@code FOLLOWER}.
   */
  static final String ROLE_NOT_IN_CONFIGURATION = "NOT_IN_CONFIGURATION";

  public GetClusterHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  /**
   * Every request, not just the expensive one (issues #7861, #7902).
   * <p>
   * The opt-in {@code ?presence=true} fan-out is the obvious blocker: one synchronous bootstrap-state RPC per
   * peer, each bounded by {@link #PRESENCE_QUERY_TIMEOUT_MS}, so worst case it holds its thread for
   * {@code peers x 5s}. On an Undertow IO thread - a shared selector - everything multiplexed onto it waits behind
   * that, the kubelet readiness and liveness probes included.
   * <p>
   * This was written as a per-REQUEST override on the argument that the ordinary status poll touches no disk, so
   * the cheap auto-poll Studio's HA panel runs could keep the IO-thread fast path. That argument does not hold,
   * and the review of PR #7953 is what established it:
   * <ul>
   *   <li>{@code ClusterAlerts} classifies the bootstrap-unreconciled set by whether each marked database is
   *       still here, which stats a directory per marked database (issue #7902);</li>
   *   <li>and the per-database rows above call {@code getBootstrapBaseline}, which lazily reads
   *       {@code .raft/bootstrap-baselines} off disk the first time anything asks - so even before #7902 the
   *       "no disk" premise was only true after that first read.</li>
   * </ul>
   * Both are cheap and both are rare, but neither is bounded by anything this handler controls, and the node most
   * likely to have marked databases is the node whose storage is misbehaving - the worst possible moment to park
   * a selector. A conditional dispatch would have to encode which of the callees below can reach a file, which is
   * exactly the kind of premise that goes stale silently; it already did, under this very method.
   * <p>
   * The cost of being unconditional is one worker handoff per poll, on a route polled every few seconds by a
   * dashboard. That is what nearly every other route in the server already pays.
   */
  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(503, new JSONObject().put("error", "Raft HA not started yet").toString());

    final JSONObject response = new JSONObject();

    response.put("implementation", "raft");
    response.put("clusterName", httpServer.getServer().getConfiguration().getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME));

    final RaftPeerId localPeerId = raftHAServer.getLocalPeerId();
    response.put("localPeerId", localPeerId.toString());

    // What THIS node can decode (issue #7219). Published next to the peer list below, which carries the same
    // field for every peer the LEADER has an answer for, so "which node is holding the cluster back" is one diff
    // rather than a poll of every node in turn. Answered by a follower the peer rows carry only this node's own,
    // because a follower has no answer about anyone else - and reporting one anyway is what defeated that diff
    // (issue #7301).
    response.put("capabilities", capabilitiesArray(raftHAServer.getAdvertisedCapabilities()));

    // Local Raft lifecycle state (division-aware, issue #5271): a node whose group member is CLOSED
    // or EXCEPTION cannot vote or accept a leader's contact - surfacing it here is the only way an
    // operator can see that the cluster is running without failover margin.
    response.put("raftState", raftHAServer.getRaftLifeCycleState().name());

    // The raw role is not the same as the ability to serve: a node that has just won an election
    // rejects writes with the retryable LeaderNotReadyException until it has committed its
    // current-term no-op. A client that writes as soon as it sees isLeader can burn its whole
    // arcadedb.ha.quorumTimeout budget on retries, so readiness is published separately (issue #5453).
    // One snapshot feeds both fields, so the pair can never be contradictory.
    final RaftHAServer.LeadershipState leadership = raftHAServer.getLeadershipState();
    final boolean isLeader = leadership.leader();
    response.put("isLeader", isLeader);
    response.put("leaderReady", leadership.leaderReady());

    final RaftPeerId leaderId = raftHAServer.getLeaderId();
    response.put("leaderId", leaderId != null ? leaderId.toString() : JSONObject.NULL);

    final String leaderHttpAddress = raftHAServer.getLeaderHttpAddress();
    response.put("leaderHttpAddress", leaderHttpAddress != null ? leaderHttpAddress : JSONObject.NULL);

    final ArcadeStateMachine stateMachine = raftHAServer.getStateMachine();
    response.put("electionCount", stateMachine.getElectionCount());
    response.put("lastElectionTime", stateMachine.getLastElectionTime());
    response.put("uptime", System.currentTimeMillis() - stateMachine.getStartTime());

    // This node's own Raft position (issue #7136). The per-peer lag below comes from getFollowerSamples(),
    // which is the LEADER's view of its followers, so a follower's own response used to carry no lag figure
    // about itself at all - the one node an operator polls when that node is the suspect. Both values are -1
    // when the division cannot be read (an in-place restart, issue #5271), and the lag is -1 rather than a
    // fabricated difference whenever either side is unknown.
    // Deliberately the raw Ratis applied index, not getTrustedAppliedIndex(): reporting paths keep the raw
    // value (see the module's CLAUDE.md). On a node holding a stale snapshot marker that value covers entries
    // it does not have - which is exactly what localResync.snapshotAppliedFloor below says out loud.
    final long localAppliedIndex = raftHAServer.getLastAppliedIndex();
    final long localCommitIndex = raftHAServer.getCommitIndex();
    response.put("localAppliedIndex", localAppliedIndex);
    response.put("localCommitIndex", localCommitIndex);
    response.put("localReplicationLag",
        localAppliedIndex >= 0 && localCommitIndex >= 0 ? localCommitIndex - localAppliedIndex : -1L);

    // This node stuck at a stale term after a snapshot install (issue #8289): it has applied everything it
    // could locally commit, so localReplicationLag above reads 0 and this node looks caught up, yet it keeps
    // rejecting the leader's current-term entries and does not count toward quorum. Debounced (see
    // RaftHAServer.isFollowerStuckAtStaleTermConfirmed) so a normal leader change is not reported as one.
    final boolean stuckAtStaleTerm = raftHAServer.isFollowerStuckAtStaleTermConfirmed();
    response.put("localStuckAtStaleTerm", stuckAtStaleTerm);

    // This follower stalled behind its leader at the current term (issue #8342): its log stopped receiving entries
    // with no term change, so localReplicationLag above can read 0 and localStuckAtStaleTerm false, and only the
    // leader's answer used to carry the stall. Measured against the commit index the leader reports over the
    // health monitor's follower-to-leader probe, with the leader's own STALLED rule. -1 on the leader (whose own
    // localCommitIndex is the figure) and on a follower that has not learned one yet. Masked on the leader too, for
    // the up to one health tick between an election and the tick that drops the stall this node had as a follower.
    final FollowerStallTracker.Stall stalledBehindLeader = isLeader ? null : raftHAServer.getFollowerStallBehindLeader();
    response.put("leaderCommitIndex", isLeader ? -1L : raftHAServer.getLeaderReportedCommitIndex());
    response.put("localStalledBehindLeader", stalledBehindLeader != null);

    // Per-follower replication health (leader only): replication lag, classified status, heartbeat
    // latency, and how long the follower has been lagging - so Studio and operators can pinpoint a
    // constantly-slow node instead of grepping logs (issue #4812). Keyed by peer id for the loop below.
    final List<HAReplicationStatsProvider.FollowerSample> followerSamples = raftHAServer.getFollowerSamples();
    final Map<String, HAReplicationStatsProvider.FollowerSample> followerHealth = new HashMap<>();
    final long lagWarningThreshold = raftHAServer.getClusterMonitor() != null
        ? raftHAServer.getClusterMonitor().getLagWarningThreshold() : 0;
    for (final HAReplicationStatsProvider.FollowerSample sample : followerSamples)
      followerHealth.put(sample.peerId(), sample);

    // Real measured leader->follower replication round-trip latency (issue #5314). Distinct from
    // lastContactMs (RPC staleness): this is the appendEntries/heartbeat RTT and stays meaningful on an
    // idle cluster. Previously no latency figure was exposed in this JSON at all.
    final Map<String, RaftHAServer.ReplicationLatency> replicationLatencies = raftHAServer.getReplicationLatencies();

    // Each peer's HTTP endpoint, plus whether that endpoint identifies that peer and no other. With no 'http'
    // port declared in arcadedb.ha.serverList a peer's endpoint is derived as its Raft host plus THIS node's
    // port, so on a cluster whose nodes differ by port every peer collapses onto one address. Reporting the
    // address alone would show an operator a plausible endpoint per peer with nothing to say that it names
    // none of them, and they would find out only when a resync or a verify refuses to dial (issue #6267).
    // Resolved for the whole group in one pass: the question is about the group, since an address identifies a
    // peer only if no other peer resolves to it, so asking per peer would resolve the group once per peer.
    final Map<RaftPeerId, RaftHAServer.PeerHttpEndpoint> httpEndpoints = raftHAServer.getPeerHttpEndpoints();

    // The static group is what the operator declared; the live configuration is what the cluster committed. A
    // peer removed from the configuration (DELETE /api/v1/cluster/peer/<id>, or a shrink by a pre-#5275 build)
    // used to keep reading here as a healthy FOLLOWER with an address, because only the static list was consulted
    // (issue #7040). Report both: every known peer, each flagged with its membership, so monitoring built on this
    // endpoint can tell a working member from a peer the cluster no longer counts on.
    final ClusterMembership membership = ClusterMembership.of(raftHAServer.getRaftGroup().getPeers(),
        raftHAServer.getLivePeers());

    final JSONArray peers = new JSONArray();
    for (final RaftPeer peer : membership.peers()) {
      final JSONObject peerJson = new JSONObject();
      final String peerId = peer.getId().toString();
      peerJson.put("id", peerId);
      peerJson.put("address", peer.getAddress());
      final boolean inConfiguration = membership.isInConfiguration(peer.getId());
      peerJson.put("inConfiguration", inConfiguration);
      // Both fields are written only when they have something to say: a peer whose endpoint cannot be resolved
      // carries neither, and a correctly declared cluster carries no flag.
      final RaftHAServer.PeerHttpEndpoint httpEndpoint = httpEndpoints.get(peer.getId());
      if (httpEndpoint != null) {
        peerJson.put("httpAddress", httpEndpoint.address());
        if (httpEndpoint.ambiguous())
          peerJson.put("httpAddressAmbiguous", true);
      }

      final boolean peerIsLeader = leaderId != null && peer.getId().equals(leaderId);
      // A peer outside the configuration is not a follower: the leader does not replicate to it and it cannot
      // vote. Naming that state in the role keeps a consumer that only reads roles from mistaking it for one.
      peerJson.put("role", peerIsLeader ? "LEADER" : inConfiguration ? "FOLLOWER" : ROLE_NOT_IN_CONFIGURATION);

      // Every node polls for capabilities since issue #7549, so every node can report what each peer advertises.
      // It used to be the leader alone - #7219's only consumer was the leader-side schema-delta decision - and
      // the cost of that was borne by the question an operator actually asks this endpoint: "is this cluster
      // ready for a rolling-upgrade-gated operation". That question had to find the leader before it could be
      // answered at all, on an endpoint neither the client nor a load balancer routes to the leader. A peer with
      // no fresh answer still omits the field rather than reporting an empty set, which would read as "this peer
      // can decode nothing".
      final PeerCapabilityRegistry.Advertisement advertisement =
          raftHAServer.getPeerCapabilityRegistry().freshAdvertisementOf(peerId);
      final boolean published = putPeerCapabilities(peerJson, peerId, localPeerId.toString(), advertisement,
          raftHAServer.getAdvertisedCapabilities());
      if (!published) {
        // An absent capabilities field reads the same whether this peer runs a build that predates the route or
        // was never asked because its address identifies no single peer - and the remedies are nothing alike, the
        // second being "declare each node's 'http' port" (#6202) rather than "finish the upgrade". Written
        // whenever this node has a reason to give (issues #7256, #7549); unknownReasonOf answers null when it has
        // none, which is what a node that has not finished its first round has.
        final String unknownReason = raftHAServer.getPeerCapabilityRegistry().unknownReasonOf(peerId);
        if (unknownReason != null)
          peerJson.put("capabilitiesUnknownReason", unknownReason);
      }

      final HAReplicationStatsProvider.FollowerSample health = followerHealth.get(peerId);
      if (!peerIsLeader && health != null) {
        peerJson.put("matchIndex", health.matchIndex());
        peerJson.put("nextIndex", health.nextIndex());
        peerJson.put("replicationLag", health.replicationLag());
        peerJson.put("lastContactMs", health.lastContactMs());
        peerJson.put("replicaStatus", health.status());
        peerJson.put("laggingForMs", health.laggingForMs());
        peerJson.put("lagging", lagWarningThreshold > 0 && health.replicationLag() > lagWarningThreshold);
        final RaftHAServer.ReplicationLatency rtt = replicationLatencies.get(peerId);
        if (rtt != null) {
          peerJson.put("replicationRttMs", rtt.meanMs());
          peerJson.put("replicationRttP99Ms", rtt.p99Ms());
        }
      }

      peers.put(peerJson);
    }
    response.put("peers", peers);

    // Per-database list, used by Studio to render per-database actions (e.g. the emergency
    // "Resync from Leader" control on followers) and to surface bootstrap baselines when present.
    // Scoped to the caller: the cluster status is server-level, but a database row carries that
    // database's transaction id and bootstrap fingerprint, which belong to its tenant alone.
    // Reserved internal databases (the Raft control directory '.raft') are not operator-visible state: the
    // presence matrix and the bootstrap-state RPC both skip them, and offering Studio a per-database action
    // row for one would be meaningless. Dropped once, here, rather than inside the loop below, so that the
    // databases array and the alerts payload built from the same set agree on what exists.
    final Set<String> authorizedDatabases = filterAuthorizedDatabases(user, httpServer.getServer().getDatabaseNames());
    authorizedDatabases.removeIf(ArcadeDBServer::isReservedDatabaseName);

    final JSONArray databases = new JSONArray();
    for (final String dbName : authorizedDatabases) {
      final JSONObject dbJson = new JSONObject();
      dbJson.put("name", dbName);
      final ArcadeStateMachine.BootstrapBaseline baseline = stateMachine.getBootstrapBaseline(dbName);
      if (baseline != null) {
        dbJson.put("bootstrapLastTxId", baseline.lastTxId());
        dbJson.put("bootstrapFingerprint", baseline.fingerprint());
      }
      // Per-database auto-acquisition status (issue #4727), when this node has reconciled against a leader.
      final DatabaseReconciler.AcquireStatus acquire = stateMachine.getReconciler().getAcquireStatus(dbName);
      if (acquire != null) {
        dbJson.put("acquireStatus", acquire.state().name());
        dbJson.put("acquireTimestamp", acquire.timestamp());
        if (acquire.error() != null)
          dbJson.put("acquireError", acquire.error());
      }
      databases.put(dbJson);
    }
    response.put("databases", databases);

    // Optional per-database x per-node presence matrix (issue #4727), gated behind ?presence=true so the
    // cheap auto-poll never triggers the peer fan-out. Built on the leader; followers return only their own.
    // Root-only, and checked before the fan-out runs: the matrix answers a whole-cluster question ("which
    // node is missing which database") that no single tenant can act on - every remedy it points to (resync,
    // transfer leadership) is itself root-only - and each request costs one bootstrap-state RPC per peer,
    // which may open a closed database on the far side.
    if (isPresenceRequested(exchange)) {
      checkRootUser(user);
      if (isLeader)
        response.put("databasePresence", buildPresenceMatrix(raftHAServer, localPeerId));
    }

    // Cluster-level health alerts (e.g. single-bucket types that serialize concurrent writes on the
    // leader). Surfaced in Studio's HA panel so operators see actionable warnings without log-grepping.
    // Scoped like the database list above, because the alert payloads name databases and, for the
    // single-bucket check, their type names too.
    // The membership divergence is a cluster-level condition: the only one on this endpoint that nothing else
    // flags, and the one an operator most needs told rather than left to diff the peer list by eye (issue #7040).
    // The local node's resync / WAL-gap quarantine state (issue #7136): the invariant is that anything making
    // readiness answer 503 is visible in this DOCUMENT. ArcadeStateMachine.isResyncInProgress() - the readiness
    // gate - is LocalResyncState.inProgress() on this very object, so the two cannot drift apart. Sampled once
    // and shared with the alert scan below, so the document cannot report the two halves from different instants.
    // This member carries the resync inputs of that invariant and not the whole of it: the two terminal ones are
    // criticalHalt and raftLogFailure just below (issue #7872).
    final LocalResyncState localResync = stateMachine.getLocalResyncState();
    response.put("localResync", buildLocalResync(localResync, authorizedDatabases));

    // The two remaining readiness inputs the #7136 invariant did not publish (issue #7872). Both are terminal
    // node-level conditions with no per-tenant component, so neither is scoped: a node whose state machine has
    // halted, or whose Raft log writer has failed, serves nothing correctly for anybody. Until this, a monitoring
    // rule built on the documented invariant - watch alerts and localResync.inProgress - read a perfectly healthy
    // node while /api/v1/ready was pinned at 503, and waited forever.
    // Written as null rather than omitted when absent, like leaderId above, so a client can tell "healthy" from
    // "this build does not report it".
    // The CONDITION is node-level and reaches every caller; the raw text behind it does not (review on PR #7953).
    // Both strings are exception text this node did not compose: Ratis's own cause for the log failure, and for
    // the halt an arbitrary Throwable's toString(). Either can carry a filesystem path, and the halt's can carry
    // the name of whichever database was being applied - which is precisely the cross-tenant disclosure the
    // visible() machinery on this endpoint exists to prevent. A tenant learns THAT this node has stopped, which
    // is what the #7136 invariant owes them and all they can act on; an operator gets the detail that says
    // whether the answer is "upgrade this node" or "file a bug".
    // Sampled ONCE and shared with the alert scan below, for the reason localResync is: two reads of a live
    // field can disagree, and the document would then carry a null criticalHalt next to a
    // halted-after-critical-error alert, or the reverse.
    final ClusterAlerts.NodeStatus nodeStatus = new ClusterAlerts.NodeStatus(stateMachine.getCriticalHalt(),
        stateMachine.getRaftLogFailure(), raftHAServer.isCrashLoopEscalated(), isRootUser(user));
    response.put("criticalHalt", buildCriticalHalt(nodeStatus.halt(), nodeStatus.detailedDiagnostics()));
    response.put("raftLogFailure", buildRaftLogFailure(nodeStatus.logFailure(), nodeStatus.detailedDiagnostics()));
    // The liveness counterpart (issue #7622): an escalation is what fails /api/v1/health (once, issue #7736), and
    // it was equally invisible here. Same reasoning, same scoping - none.
    response.put("crashLoopEscalated", nodeStatus.crashLoopEscalated());

    response.put("alerts",
        ClusterAlerts.scan(httpServer.getServer(), stateMachine, followerSamples, authorizedDatabases, membership,
            localPeerId.toString(), localResync, nodeStatus, stuckAtStaleTerm, stalledBehindLeader));

    return new ExecutionResponse(200, response.toString());
  }

  /**
   * Renders {@link LocalResyncState} for the status document (issue #7136).
   * <p>
   * {@code inProgress} is the node-level answer and is never suppressed: whether this node serves traffic is not
   * a per-tenant fact, and it is the field a readiness-aware monitoring rule keys on. The database <em>names</em>
   * are reduced to {@code visibleDatabases}, exactly like the alert payloads, because a caller scoped to one
   * database must not learn another tenant's database name from a status poll.
   * <p>
   * Package-private so the document's shape and that scoping can be unit-tested without a live cluster.
   */
  static JSONObject buildLocalResync(final LocalResyncState state,
      final Set<String> visibleDatabases) {
    // The same scoping predicate the alert payloads use, so the document body and alerts cannot disagree
    // about which database names this caller may see.
    return new JSONObject()
        .put("inProgress", state.inProgress())
        .put("snapshotDownloadQueued", state.snapshotDownloadQueued())
        .put("snapshotDownloadInProgress", state.snapshotDownloadInProgress())
        .put("divergedDatabases", ClusterAlerts.namesArray(ClusterAlerts.visible(state.divergedDatabases(), visibleDatabases)))
        // Why each of them was quarantined (issue #7741): the names alone read as a replication problem even
        // when the cause is this node's own unreadable log segment.
        .put("divergenceCauses", ClusterAlerts.causesObject(state, visibleDatabases))
        .put("snapshotAppliedFloor", state.snapshotAppliedFloor())
        .put("databaseAppliedFloors", ClusterAlerts.visibleFloors(state.databaseAppliedFloors(), visibleDatabases));
  }

  /**
   * Renders the critical halt for the status document (issue #7872), or {@link JSONObject#NULL} when this node's
   * state machine is still applying entries.
   * <p>
   * Explicitly null rather than absent, like {@code leaderId}: a member that disappears reads the same whether
   * the node is healthy or the build does not report it, and those are not the same answer.
   * <p>
   * Package-private, and built as one chained expression, for the same reason {@link #buildLocalResync} is: the
   * shape can then be pinned against {@code PluginApiSpec} without a live cluster, which is what keeps a member
   * from reaching the response and not the contract - the exact defect #7741 had and #7872 repeated.
   */
  static Object buildCriticalHalt(final ArcadeStateMachine.CriticalHalt halt, final boolean detailed) {
    if (halt == null)
      return JSONObject.NULL;
    return new JSONObject()
        .put("index", halt.index())
        .put("reason", detailed ? halt.reason() : REDACTED_REASON)
        .put("timestamp", halt.timestamp());
  }

  /**
   * Renders the persistent Raft log-write failure for the status document (issue #7872, publishing the #7037
   * signal the #7118 readiness gate already reads), or {@link JSONObject#NULL} while the log writer is healthy.
   * Same shape and same reasoning as {@link #buildCriticalHalt}.
   */
  static Object buildRaftLogFailure(final ArcadeStateMachine.RaftLogFailure failure, final boolean detailed) {
    if (failure == null)
      return JSONObject.NULL;
    return new JSONObject()
        .put("index", failure.index())
        .put("cause", detailed ? failure.cause() : REDACTED_REASON)
        .put("timestamp", failure.timestamp());
  }

  /**
   * What a non-root caller reads in place of the raw exception text (review on PR #7953). Deliberately says the
   * text was withheld rather than going absent or empty: a field that disappears reads as "this build does not
   * report it", and an operator chasing an incident needs to know the detail exists and who can see it.
   */
  static final String REDACTED_REASON = "<available to the root user>";

  /**
   * Whether this caller may be shown the raw diagnostic text, without throwing the way
   * {@code checkRootUser} does - this is a per-field reduction inside a response the caller is entitled to, not
   * a refusal of the request.
   * <p>
   * A null user is the non-HTTP caller (and the unauthenticated path, which does not reach here), and it gets the
   * reduced view: the safe default is the one that discloses less.
   */
  private static boolean isRootUser(final ServerSecurityUser user) {
    return user != null && "root".equals(user.getName());
  }

  /**
   * Writes {@code peer}'s capabilities into its row, and says whether anything was written (issue #7301).
   * <p>
   * There are exactly two sources of a TRUE answer, and no third. The registry, which only a leader fills because
   * it is the only node that probes; and this node's own advertised set, which is true for this node's own row
   * whatever role it holds. The first version of this guard tested "the peer being rendered is the leader" and
   * published the LOCAL set under the leader's id, which on a follower - where the registry is empty by design -
   * is the local node's answer wearing another node's id. That defeats the field's whole documented purpose: an
   * operator diffing the rows during a rolling upgrade is told the wrong node is holding the cluster back, and
   * there is no {@code version} field on such a row to contradict it.
   * <p>
   * The local row is published whether or not this node leads, since it is a true statement either way and the
   * one row every node can answer for. It carries {@code version} for the same reason the registry-backed rows
   * do - {@link PostCapabilitiesHandler} reports the same {@link Constants#getVersion()} to a probing leader, so
   * a row read off the node itself and the same row read off its leader say the same thing.
   * <p>
   * The peer ids come in as strings and are compared here rather than by the caller, so "is this the local node"
   * cannot drift back into "is this the leader" without this failing.
   *
   * @return true when a {@code capabilities} field was written, i.e. when there was something true to say.
   */
  // @VisibleForTesting
  static boolean putPeerCapabilities(final JSONObject peerJson, final String peerId, final String localPeerId,
      final PeerCapabilityRegistry.Advertisement advertisement, final Set<String> localCapabilities) {
    if (advertisement != null) {
      peerJson.put("capabilities", capabilitiesArray(advertisement.capabilities()));
      if (!advertisement.version().isEmpty())
        peerJson.put("version", advertisement.version());
      return true;
    }

    if (peerId.equals(localPeerId)) {
      peerJson.put("capabilities", capabilitiesArray(localCapabilities));
      peerJson.put("version", Constants.getVersion());
      return true;
    }

    return false;
  }

  /** A capability set as a stable, sorted JSON array, so two peers' documents can be diffed by eye. */
  private static JSONArray capabilitiesArray(final Set<String> capabilities) {
    final JSONArray array = new JSONArray();
    for (final String capability : new TreeSet<>(capabilities))
      array.put(capability);
    return array;
  }

  private static boolean isPresenceRequested(final HttpServerExchange exchange) {
    final Deque<String> values = exchange.getQueryParameters().get("presence");
    if (values == null || values.isEmpty())
      return false;
    final String v = values.getFirst();
    return v == null || v.isEmpty() || "true".equalsIgnoreCase(v) || "1".equals(v);
  }

  /**
   * Builds a per-database x per-node presence matrix (issue #4727) by fanning out the bootstrap-state RPC to
   * every peer. Returns {@code {nodes:[peerId...], unreachable:[peerId...], databases:[{name, present:[...],
   * missing:[...]}]}}. A peer that cannot be reached is reported in {@code unreachable} and omitted from the
   * present/missing accounting so a transient blip is not mistaken for a dropped database.
   * <p>
   * The fan-out is sequential on an Undertow worker thread - which {@link #mustExecuteOnWorkerThread()}
   * is what makes true (issue #7861) - with a short per-peer timeout
   * ({@link #PRESENCE_QUERY_TIMEOUT_MS}), so worst-case latency is {@code peers x 5s}. This is acceptable because
   * it is opt-in ({@code ?presence=true}) and leader-only, not part of the cheap auto-poll; a parallel fan-out
   * would bound it for very large clusters. If parallelized later, honor the CLAUDE.md concurrency rule - do not
   * use {@code ForkJoinPool.commonPool()} for server-internal work; use a dedicated bounded pool wired into
   * {@code PoolMetrics}.
   * Note the queried peer's bootstrap-state handler may open a closed database to fingerprint it, so this path -
   * unlike the no-open cheap poll - can trigger a database load on the remote peer.
   */
  private JSONObject buildPresenceMatrix(final RaftHAServer raftHAServer, final RaftPeerId localPeerId) {
    final ArcadeDBServer server = httpServer.getServer();
    final String clusterToken = raftHAServer.getClusterToken();
    // Use a short per-peer timeout (not HA_BOOTSTRAP_TIMEOUT_MS, which defaults to 120s): this fan-out runs on an
    // Undertow worker thread (the dispatch above), and a peer that accepts the connection but then hangs would
    // otherwise tie up the worker for the full bootstrap budget per peer. A worker is a far cheaper thing to hold
    // than the IO thread this used to run on, but it is still one of a bounded pool, so the bound stays short.
    // A few seconds is plenty for a peer to list its databases; a
    // slower peer is simply reported unreachable in the matrix.
    final long timeoutMs = PRESENCE_QUERY_TIMEOUT_MS;

    // Preserve a stable node order; collect each reachable peer's database set.
    final Set<String> nodes = new LinkedHashSet<>();
    final Set<String> unreachable = new TreeSet<>();
    final Map<String, Set<String>> dbsByNode = new TreeMap<>();
    final Set<String> allDbs = new TreeSet<>();

    for (final RaftPeer peer : raftHAServer.getRaftGroup().getPeers()) {
      final RaftPeerId peerId = peer.getId();
      final String peerIdStr = peerId.toString();
      nodes.add(peerIdStr);

      final Set<String> dbNames = new TreeSet<>();
      if (peerId.equals(localPeerId)) {
        for (final String dbName : server.getDatabaseNames())
          if (!dbName.startsWith(ArcadeDBServer.RESERVED_DATABASE_PREFIX))
            dbNames.add(dbName);
      } else {
        // The guarded address, not the best-effort one (issue #6267). This fan-out attributes whatever comes back
        // to peerIdStr, so an address that resolves to the wrong node - or to this one - fills the matrix with a
        // reassuring answer nobody asked for: on a cluster whose peers collapse onto one derived address, every
        // peer would report the local node's databases and the matrix would show them present everywhere. Same
        // guard the resync and verify paths use, so the three cannot drift apart.
        final PeerDialAddress dial = PeerDialAddress.resolve(raftHAServer, peerId, "peer");
        if (dial.refused()) {
          LogManager.instance().log(this, Level.WARNING,
              "Presence matrix: not querying peer '%s': %s", peerIdStr, dial.refusal());
          unreachable.add(peerIdStr);
          continue;
        }
        try {
          final List<LeaderDatabaseQuery.DatabaseInfo> infos =
              LeaderDatabaseQuery.fetch(dial.httpAddress(), dial.httpsAddress(), clusterToken, timeoutMs, server)
                  .databases();
          for (final LeaderDatabaseQuery.DatabaseInfo info : infos)
            dbNames.add(info.name());
        } catch (final InterruptedException e) {
          // Preserve the interrupt so the worker thread can be shut down cleanly.
          Thread.currentThread().interrupt();
          LogManager.instance().log(this, Level.WARNING, "Presence matrix query interrupted for peer '%s'", peerIdStr);
          unreachable.add(peerIdStr);
          continue;
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING,
              "Presence matrix: could not query peer '%s' (%s)", peerIdStr, e.getMessage());
          unreachable.add(peerIdStr);
          continue;
        }
      }
      dbsByNode.put(peerIdStr, dbNames);
      allDbs.addAll(dbNames);
    }

    final JSONArray databases = new JSONArray();
    for (final String dbName : allDbs) {
      final JSONArray present = new JSONArray();
      final JSONArray missing = new JSONArray();
      for (final Map.Entry<String, Set<String>> e : dbsByNode.entrySet()) {
        if (e.getValue().contains(dbName))
          present.put(e.getKey());
        else
          missing.put(e.getKey());
      }
      databases.put(new JSONObject().put("name", dbName).put("present", present).put("missing", missing));
    }

    final JSONArray nodesArray = new JSONArray();
    for (final String n : nodes)
      nodesArray.put(n);
    final JSONArray unreachableArray = new JSONArray();
    for (final String n : unreachable)
      unreachableArray.put(n);

    return new JSONObject()
        .put("nodes", nodesArray)
        .put("unreachable", unreachableArray)
        .put("databases", databases);
  }
}
