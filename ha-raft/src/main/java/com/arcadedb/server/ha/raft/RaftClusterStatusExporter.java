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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  private volatile int            lastStableSignature;

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
    final RaftPeerId leaderId = haServer.getLeaderId();
    for (final RaftPeer peer : haServer.getLivePeers()) {
      final var peerJSON = new JSONObject();
      final String peerId = peer.getId().toString();
      peerJSON.put("id", peerId);
      peerJSON.put("address", peer.getAddress());
      peerJSON.put("httpAddress", haServer.getPeerHttpAddress(peer.getId()));
      peerJSON.put("isLocal", peer.getId().equals(haServer.getLocalPeerId()));
      peerJSON.put("role", leaderId != null && peer.getId().equals(leaderId) ? "LEADER" : "FOLLOWER");

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
            // Studio renders this as a colored badge in the cluster view, so a STALLED follower
            // jumps out at the operator without having to compare numbers in their head.
            peerJSON.put("replicaStatus", clusterMonitor.getReplicaStatus(peerId).name());
          }
          break;
        }

      peers.put(peerJSON);
    }
    haJSON.put("peers", peers);

    // Database list
    final var databases = new JSONArray();
    final var stateMachineForBaseline = haServer.getStateMachine();
    for (final String dbName : haServer.getServer().getDatabaseNames()) {
      // Never expose reserved internal databases (e.g. the Raft control directory '.raft').
      if (ArcadeDBServer.isReservedDatabaseName(dbName))
        continue;
      final var databaseJSON = new JSONObject();
      databaseJSON.put("name", dbName);
      databaseJSON.put("quorum", haServer.getQuorum().name());

      // Surface the bootstrap baseline applied via BOOTSTRAP_FINGERPRINT_ENTRY (#4147 phase 7).
      // Null when no bootstrap entry has been committed for this database yet, which is the
      // normal case for clusters that pre-date #4147 or that never engaged the bootstrap path.
      final var baseline = stateMachineForBaseline != null
          ? stateMachineForBaseline.getBootstrapBaseline(dbName) : null;
      if (baseline != null) {
        databaseJSON.put("bootstrapLastTxId", baseline.lastTxId());
        databaseJSON.put("bootstrapFingerprint", baseline.fingerprint());
      }
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
   * Immutable snapshot of everything the cluster configuration table renders: the Raft term, the
   * commit index, one row per committed member ({@code SERVER, ADDRESS, ROLE, LAG, LATENCY, STATUS})
   * and one row per database with a bootstrap baseline. Extracted so the re-emit decision and the
   * rendering are pure functions testable without a live Raft cluster (issue #5304).
   */
  static final class ConfigSnapshot {
    final long           term;
    final long           commitIndex;
    final List<String[]> rows;
    final List<String[]> baselineRows;
    final int            configuredServers;

    ConfigSnapshot(final long term, final long commitIndex, final List<String[]> rows,
        final List<String[]> baselineRows, final int configuredServers) {
      this.term = term;
      this.commitIndex = commitIndex;
      this.rows = rows;
      this.baselineRows = baselineRows;
      this.configuredServers = configuredServers;
    }
  }

  /**
   * Prints an ASCII table showing the current cluster configuration. Called when this node becomes
   * leader and from every lag-monitor tick, so the logged view converges to the committed membership
   * instead of freezing on the state captured at election time (issue #5304: a single bootstrap-window
   * emission raced a member's join by 112 ms and permanently showed 2 of 3 members).
   * <p>
   * Deduplicated on a STABLE signature (term, member ids, addresses, roles, replica statuses) that
   * excludes the volatile LAG/LATENCY columns and the commit index: a membership change, role change
   * or replica-status transition re-emits the table, while ordinary lag fluctuation between ticks
   * does not flood the log. This also keeps duplicate leader-change events (multiple servers in the
   * same JVM) from printing the same table twice.
   */
  void printClusterConfiguration() {
    try {
      final ConfigSnapshot snapshot = collectSnapshot(true);
      if (snapshot == null)
        return;

      final int signature = stableSignature(snapshot);
      if (signature == lastStableSignature)
        return;
      lastStableSignature = signature;

      emit(renderTable(snapshot));

    } catch (final Exception e) {
      // Best-effort: don't let formatting errors disrupt the cluster
      HALog.log(this, HALog.BASIC, "Error printing cluster configuration: %s", e.getMessage());
    }
  }

  /** Log-emission seam, overridable in tests. */
  void emit(final String output) {
    // Use warning level on purpose for a few releases until the whole HA module has been road tested
    LogManager.instance().log(this, Level.WARNING, "%s", output);
  }

  /**
   * Builds the ASCII cluster configuration table as a string, unconditionally (even on followers,
   * where lag/latency columns will be empty). Returns {@code null} when there are no peers to render.
   * Used by {@link RaftHAServer#getClusterConfigurationTable()} for tests and diagnostics.
   */
  String buildClusterConfigurationTable() {
    final ConfigSnapshot snapshot = collectSnapshot(false);
    return snapshot == null ? null : renderTable(snapshot);
  }

  /**
   * Collects the current cluster configuration from the local Raft division. Returns {@code null}
   * when there are no peers to render, or - with {@code leaderOnly} - when this node is not the
   * leader. Overridable in tests.
   */
  ConfigSnapshot collectSnapshot(final boolean leaderOnly) {
    if (leaderOnly && !haServer.isLeader())
      return null;

    final RaftPeerId leaderId = haServer.getLeaderId();
    final long term = haServer.getCurrentTerm();
    final long commitIndex = haServer.getCommitIndex();
    final Collection<RaftPeer> peers = haServer.getLivePeers();
    if (peers.isEmpty())
      return null;

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
      final boolean isPeerLeader = leaderId != null && peer.getId().equals(leaderId);
      final String role = isPeerLeader ? "Leader" : "Follower";
      final String address = peer.getAddress();

      String lagStr = "";
      String latencyStr = "";
      String statusStr = "";
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
        if (clusterMonitor != null)
          statusStr = clusterMonitor.getReplicaStatus(peerId).name();
      }

      rows.add(new String[] { peerId, address, role, lagStr, latencyStr, statusStr });
    }

    // Deterministic row order: getLivePeers() gives no ordering guarantee, and a mere reordering must
    // not change the stable signature (it would re-emit an identical membership picture).
    rows.sort((a, b) -> a[0].compareTo(b[0]));

    return new ConfigSnapshot(term, commitIndex, rows, collectBootstrapBaselines(), haServer.getConfiguredServers());
  }

  /**
   * Hash over the stable columns only: the term plus each member's id, address, role and replica
   * status. LAG, LATENCY and the commit index are deliberately excluded - they move on nearly every
   * lag-monitor tick and would defeat the deduplication (issue #5304).
   */
  static int stableSignature(final ConfigSnapshot snapshot) {
    int h = Long.hashCode(snapshot.term);
    for (final String[] row : snapshot.rows) {
      h = 31 * h + Objects.hashCode(row[0]); // SERVER
      h = 31 * h + Objects.hashCode(row[1]); // ADDRESS
      h = 31 * h + Objects.hashCode(row[2]); // ROLE
      h = 31 * h + Objects.hashCode(row[5]); // STATUS
    }
    return h;
  }

  /** Renders the ASCII cluster configuration table for the given snapshot. Pure function. */
  static String renderTable(final ConfigSnapshot snapshot) {
    final String[] headers = { "SERVER", "ADDRESS", "ROLE", "LAG", "LATENCY", "STATUS" };
    final int[] widths = new int[headers.length];
    for (int i = 0; i < headers.length; i++)
      widths[i] = headers[i].length();
    for (final String[] row : snapshot.rows)
      for (int i = 0; i < row.length; i++)
        widths[i] = Math.max(widths[i], row[i].length());

    final StringBuilder sb = new StringBuilder();
    sb.append(String.format("CLUSTER CONFIGURATION (term=%d, commitIndex=%d)%n", snapshot.term, snapshot.commitIndex));

    appendSeparator(sb, widths);
    appendRow(sb, widths, headers);
    appendSeparator(sb, widths);
    for (final String[] row : snapshot.rows)
      appendRow(sb, widths, row);
    appendSeparator(sb, widths);

    // Issue #5304: never present a bootstrap-window snapshot as authoritative. While the committed
    // membership is smaller than the configured server list, say so explicitly; the table is
    // re-emitted when the missing members join.
    if (snapshot.rows.size() < snapshot.configuredServers)
      sb.append(String.format("NOTE: %d of %d configured servers are in the committed membership - not yet converged; "
          + "this table is logged again when membership or replica status changes.%n",
          snapshot.rows.size(), snapshot.configuredServers));

    // Issue #4147 phase 7: bootstrap baselines per database. Only printed when at least one
    // database has a baseline; otherwise the section is omitted to keep the output uncluttered
    // for clusters that pre-date the bootstrap feature.
    appendBootstrapBaselines(sb, snapshot.baselineRows);

    return sb.toString();
  }

  /**
   * Collects one row per database with a committed bootstrap baseline. Empty when there are none, so
   * existing log output stays unchanged for clusters that never engaged the bootstrap path.
   */
  private List<String[]> collectBootstrapBaselines() {
    final var stateMachine = haServer.getStateMachine();
    if (stateMachine == null)
      return List.of();

    final List<String[]> rows = new ArrayList<>();
    for (final String dbName : haServer.getServer().getDatabaseNames()) {
      final var baseline = stateMachine.getBootstrapBaseline(dbName);
      if (baseline == null)
        continue;
      rows.add(new String[] { dbName, String.valueOf(baseline.lastTxId()), abbreviate(baseline.fingerprint()) });
    }
    return rows;
  }

  /** If any database has a committed bootstrap baseline, print a "BOOTSTRAP BASELINES" section. */
  private static void appendBootstrapBaselines(final StringBuilder sb, final List<String[]> rows) {
    if (rows.isEmpty())
      return;

    final String[] headers = { "DATABASE", "BOOTSTRAP_LAST_TX_ID", "BOOTSTRAP_FINGERPRINT" };
    final int[] widths = new int[headers.length];
    for (int i = 0; i < headers.length; i++)
      widths[i] = headers[i].length();
    for (final String[] row : rows)
      for (int i = 0; i < row.length; i++)
        widths[i] = Math.max(widths[i], row[i].length());

    sb.append('\n');
    appendSeparator(sb, widths);
    appendRow(sb, widths, headers);
    appendSeparator(sb, widths);
    for (final String[] row : rows)
      appendRow(sb, widths, row);
    appendSeparator(sb, widths);
  }

  /** Abbreviate a 64-char SHA-256 hex fingerprint for human-friendly display. */
  private static String abbreviate(final String fingerprint) {
    if (fingerprint == null || fingerprint.length() <= 16)
      return String.valueOf(fingerprint);
    return fingerprint.substring(0, 8) + "..." + fingerprint.substring(fingerprint.length() - 8);
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
        clusterMonitor.updateReplicaMatchIndex((String) fs.get("peerId"), (Long) fs.get("matchIndex"),
            (Long) fs.get("lastRpcElapsedMs"));
      // Issue #5304: with the per-replica classifications refreshed, re-emit the CLUSTER CONFIGURATION
      // table when the stable picture (membership, roles, statuses, term) changed since the last
      // emission, so the logged view converges instead of freezing on the election-time snapshot.
      printClusterConfiguration();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Error checking replica lag", e);
    }
  }
}
