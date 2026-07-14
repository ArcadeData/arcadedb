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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
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

  public GetClusterHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
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

    // Local Raft lifecycle state (division-aware, issue #5271): a node whose group member is CLOSED
    // or EXCEPTION cannot vote or accept a leader's contact - surfacing it here is the only way an
    // operator can see that the cluster is running without failover margin.
    response.put("raftState", raftHAServer.getRaftLifeCycleState().name());

    final boolean isLeader = raftHAServer.isLeader();
    response.put("isLeader", isLeader);

    final RaftPeerId leaderId = raftHAServer.getLeaderId();
    response.put("leaderId", leaderId != null ? leaderId.toString() : JSONObject.NULL);

    final String leaderHttpAddress = raftHAServer.getLeaderHttpAddress();
    response.put("leaderHttpAddress", leaderHttpAddress != null ? leaderHttpAddress : JSONObject.NULL);

    final ArcadeStateMachine stateMachine = raftHAServer.getStateMachine();
    response.put("electionCount", stateMachine.getElectionCount());
    response.put("lastElectionTime", stateMachine.getLastElectionTime());
    response.put("uptime", System.currentTimeMillis() - stateMachine.getStartTime());

    // Per-follower replication health (leader only): replication lag, classified status, heartbeat
    // latency, and how long the follower has been lagging - so Studio and operators can pinpoint a
    // constantly-slow node instead of grepping logs (issue #4812). Keyed by peer id for the loop below.
    final List<HAReplicationStatsProvider.FollowerSample> followerSamples = raftHAServer.getFollowerSamples();
    final Map<String, HAReplicationStatsProvider.FollowerSample> followerHealth = new HashMap<>();
    final long lagWarningThreshold = raftHAServer.getClusterMonitor() != null
        ? raftHAServer.getClusterMonitor().getLagWarningThreshold() : 0;
    for (final HAReplicationStatsProvider.FollowerSample sample : followerSamples)
      followerHealth.put(sample.peerId(), sample);

    final JSONArray peers = new JSONArray();
    for (final RaftPeer peer : raftHAServer.getRaftGroup().getPeers()) {
      final JSONObject peerJson = new JSONObject();
      final String peerId = peer.getId().toString();
      peerJson.put("id", peerId);
      peerJson.put("address", peer.getAddress());

      final boolean peerIsLeader = leaderId != null && peer.getId().equals(leaderId);
      peerJson.put("role", peerIsLeader ? "LEADER" : "FOLLOWER");

      final HAReplicationStatsProvider.FollowerSample health = followerHealth.get(peerId);
      if (!peerIsLeader && health != null) {
        peerJson.put("matchIndex", health.matchIndex());
        peerJson.put("nextIndex", health.nextIndex());
        peerJson.put("replicationLag", health.replicationLag());
        peerJson.put("lastContactMs", health.lastContactMs());
        peerJson.put("replicaStatus", health.status());
        peerJson.put("laggingForMs", health.laggingForMs());
        peerJson.put("lagging", lagWarningThreshold > 0 && health.replicationLag() > lagWarningThreshold);
      }

      peers.put(peerJson);
    }
    response.put("peers", peers);

    // Per-database list, used by Studio to render per-database actions (e.g. the emergency
    // "Resync from Leader" control on followers) and to surface bootstrap baselines when present.
    final JSONArray databases = new JSONArray();
    for (final String dbName : httpServer.getServer().getDatabaseNames()) {
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
    if (isLeader && isPresenceRequested(exchange))
      response.put("databasePresence", buildPresenceMatrix(raftHAServer, localPeerId));

    // Cluster-level health alerts (e.g. single-bucket types that serialize concurrent writes on the
    // leader). Surfaced in Studio's HA panel so operators see actionable warnings without log-grepping.
    response.put("alerts", ClusterAlerts.scan(httpServer.getServer(), stateMachine, followerSamples));

    return new ExecutionResponse(200, response.toString());
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
   * The fan-out is sequential on the Undertow worker thread, with a short per-peer timeout
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
    // Undertow worker thread, and a peer that accepts the connection but then hangs would otherwise tie up the
    // worker for the full bootstrap budget per peer. A few seconds is plenty for a peer to list its databases; a
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
        final String httpAddr = raftHAServer.getPeerHttpAddress(peerId);
        if (httpAddr == null) {
          unreachable.add(peerIdStr);
          continue;
        }
        final String httpsAddr = raftHAServer.getPeerHttpsAddress(peerId);
        try {
          final List<LeaderDatabaseQuery.DatabaseInfo> infos =
              LeaderDatabaseQuery.fetch(httpAddr, httpsAddr, clusterToken, timeoutMs, server);
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
