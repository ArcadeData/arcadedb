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
import org.apache.ratis.protocol.RaftPeer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * Exports cluster status as JSON, prints cluster configuration tables, and manages
 * the replication lag monitor.
 */
class RaftClusterStatusExporter {

  // Lag monitor: checks follower replication lag every N seconds.
  private static final int LAG_MONITOR_INITIAL_DELAY_SECS = 5;
  private static final int LAG_MONITOR_INTERVAL_SECS      = 5;

  private final    RaftHAServer   haServer;
  private final    ClusterMonitor clusterMonitor;
  private volatile int            lastClusterConfigHash;

  RaftClusterStatusExporter(final RaftHAServer haServer, final ClusterMonitor clusterMonitor) {
    this.haServer = haServer;
    this.clusterMonitor = clusterMonitor;
  }

  // -- Status Export --

  JSONObject exportClusterStatus() {
    final var haJSON = new JSONObject();

    haJSON.put("protocol", "ratis");
    haJSON.put("clusterName", haServer.getClusterName());
    haJSON.put("leader", haServer.getLeaderName());
    haJSON.put("isLeader", haServer.isLeader());
    haJSON.put("localPeerId", haServer.getLocalPeerId().toString());
    haJSON.put("configuredServers", haServer.getConfiguredServers());
    haJSON.put("quorum", haServer.getQuorum().name());
    haJSON.put("currentTerm", haServer.getCurrentTerm());
    haJSON.put("commitIndex", haServer.getCommitIndex());
    haJSON.put("lastAppliedIndex", haServer.getLastAppliedIndex());

    // Peer list with replication state (follower indices available only on leader)
    final var followerStates = haServer.getFollowerStates();
    final var peers = new JSONArray();
    for (final RaftPeer peer : haServer.getLivePeers()) {
      final var peerJSON = new JSONObject();
      final String peerId = peer.getId().toString();
      peerJSON.put("id", peerId);
      peerJSON.put("address", peer.getAddress());
      peerJSON.put("httpAddress", haServer.getPeerHttpAddress(peer.getId()));
      peerJSON.put("isLocal", peer.getId().equals(haServer.getLocalPeerId()));
      peerJSON.put("role", peer.getId().equals(haServer.getLocalPeerId()) && haServer.isLeader() ? "LEADER"
          : peerId.equals(haServer.getLeaderName()) ? "LEADER" : "FOLLOWER");

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
    final var databases = new JSONArray();
    for (final String dbName : haServer.getServer().getDatabaseNames()) {
      final var databaseJSON = new JSONObject();
      databaseJSON.put("name", dbName);
      databaseJSON.put("quorum", haServer.getQuorum().name());
      databases.put(databaseJSON);
    }
    haJSON.put("databases", databases);

    // Metrics
    final var stateMachine = haServer.getStateMachine();
    final var metricsJSON = new JSONObject();
    metricsJSON.put("electionCount", stateMachine.getElectionCount());
    metricsJSON.put("lastElectionTime", stateMachine.getLastElectionTime());
    metricsJSON.put("startTime", stateMachine.getStartTime());
    metricsJSON.put("lagWarningThreshold", clusterMonitor.getLagWarningThreshold());
    haJSON.put("metrics", metricsJSON);

    // Required by RemoteHttpComponent for cluster configuration
    haJSON.put("leaderAddress", haServer.getLeaderHttpAddress());
    haJSON.put("replicaAddresses", haServer.getReplicaAddresses());

    return haJSON;
  }

  // -- Cluster Configuration Printing --

  /**
   * Prints an ASCII table showing the current cluster configuration.
   * Called on leader changes so the operator can see the cluster state at a glance.
   */
  void printClusterConfiguration() {
    if (!haServer.isLeader())
      return;

    try {
      final String leaderPeerId = haServer.getLeaderName();
      final long term = haServer.getCurrentTerm();
      final long commitIndex = haServer.getCommitIndex();
      final Collection<RaftPeer> peers = haServer.getLivePeers();
      if (peers.isEmpty())
        return;

      // Collect follower replication state (only available on leader)
      final Map<String, long[]> followerState = new HashMap<>();
      for (final Map<String, Object> f : haServer.getFollowerStates()) {
        final String peerId = (String) f.get("peerId");
        final long matchIndex = (Long) f.get("matchIndex");
        final long lastRpcMs = (Long) f.get("lastRpcElapsedMs");
        followerState.put(peerId, new long[] { matchIndex, lastRpcMs });
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
                haServer.getConfiguration().getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN) / 2;
            if (elapsedMs <= heartbeatInterval)
              latencyStr = elapsedMs + " ms";
          }
        }

        rows.add(new String[] { peerId, address, role, lagStr, latencyStr });
      }

      // Calculate column widths
      final String[] headers = { "SERVER", "ADDRESS", "ROLE", "LAG", "LATENCY" };
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

      // Use warning level on purpose for a few releases until the whole HA module has been road tested
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

  // -- Lag Monitor --

  void checkReplicaLag() {
    try {
      if (!haServer.isLeader())
        return;
      clusterMonitor.updateLeaderCommitIndex(haServer.getCommitIndex());
      for (final var fs : haServer.getFollowerStates())
        clusterMonitor.updateReplicaMatchIndex((String) fs.get("peerId"), (Long) fs.get("matchIndex"));
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Error checking replica lag", e);
    }
  }
}
