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

import com.arcadedb.log.LogManager;
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
 * Prints the cluster configuration table and manages the replication lag monitor.
 * <p>
 * It used to also build a cluster-status JSON, which nothing called: the live status endpoint is
 * {@link GetClusterHandler}, which assembles its own. A second, unreachable builder of the same document is how
 * two views of one cluster drift apart - and it did, since the reachable one reports the peer-address ambiguity
 * of issue #6267 and this one never would have - so it was removed rather than kept in step by hand.
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

  /**
   * LAG column value for a follower whose match index could not be read consistently on this tick (issue
   * #7041). Distinct from {@code "0"} and from the blank the leader row carries: the operator is told the
   * number is unavailable rather than shown a fabricated one. Excluded from the stable signature like every
   * other LAG value, so a transient unknown never re-emits the table.
   */
  static final String LAG_UNKNOWN = "?";

  /**
   * One follower's replication state as read from a {@link RaftHAServer#getFollowerStates()} entry. The
   * match index is optional: a degraded entry (membership changed while the indices were being read, issue
   * #4842) carries none, and {@code matchIndexKnown} says so, because Ratis's own {@code -1} means
   * "never appended" and cannot double as "unknown".
   */
  static final class FollowerReplicationState {
    final long    matchIndex;
    final boolean matchIndexKnown;
    final long    lastRpcMs;

    FollowerReplicationState(final long matchIndex, final boolean matchIndexKnown, final long lastRpcMs) {
      this.matchIndex = matchIndex;
      this.matchIndexKnown = matchIndexKnown;
      this.lastRpcMs = lastRpcMs;
    }

    static FollowerReplicationState of(final Map<String, Object> state) {
      return new FollowerReplicationState(RaftHAServer.followerStateIndex(state, "matchIndex"),
          RaftHAServer.hasFollowerStateIndex(state, "matchIndex"),
          RaftHAServer.followerStateIndex(state, "lastRpcElapsedMs"));
    }
  }

  // -- Cluster Configuration Printing --

  /**
   * Immutable snapshot of everything the cluster configuration table renders: the Raft term, the
   * commit index, one row per committed member ({@code SERVER, ADDRESS, ROLE, LAG, RTT, LAST CONTACT,
   * STATUS}) and one row per database with a bootstrap baseline. Extracted so the re-emit decision and
   * the rendering are pure functions testable without a live Raft cluster (issue #5304).
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
   * excludes the volatile LAG/RTT/LAST CONTACT columns and the commit index: a membership change, role
   * change or replica-status transition re-emits the table, while ordinary lag fluctuation between ticks
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
   * where lag/rtt/last-contact columns will be empty). Returns {@code null} when there are no peers to render.
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

    // Collect follower replication state (only available on leader). The entries are read defensively
    // (issue #7041): while membership is changing, getFollowerStates() degrades to entries without a
    // match index rather than misattributing one, and a raw cast on that entry used to throw inside the
    // catch-all above and suppress the whole table. Such a peer keeps its row, with the lag marked unknown.
    final Map<String, FollowerReplicationState> followerState = new HashMap<>();
    for (final Map<String, Object> f : haServer.getFollowerStates())
      followerState.put((String) f.get("peerId"), FollowerReplicationState.of(f));

    // Measured leader->follower replication RTT per follower (issue #5314): the real appendEntries/
    // heartbeat round-trip, load-independent, distinct from the LAST CONTACT staleness figure below.
    final Map<String, RaftHAServer.ReplicationLatency> rttByPeer = haServer.getReplicationLatencies();

    // Build table rows
    final List<String[]> rows = new ArrayList<>();
    for (final RaftPeer peer : peers) {
      final String peerId = peer.getId().toString();
      final boolean isPeerLeader = leaderId != null && peer.getId().equals(leaderId);
      final String role = isPeerLeader ? "Leader" : "Follower";
      final String address = peer.getAddress();

      String lagStr = "";
      String rttStr = "";
      String lastContactStr = "";
      String statusStr = "";
      if (!isPeerLeader) {
        final FollowerReplicationState state = followerState.get(peerId);
        if (state != null) {
          if (state.matchIndexKnown) {
            final long lag = commitIndex - state.matchIndex;
            lagStr = lag > 0 ? String.valueOf(lag) : "0";
          } else
            lagStr = LAG_UNKNOWN;
          // LAST CONTACT = time since the leader last heard from this follower (issue #5314). On an idle
          // cluster it just tracks the heartbeat cadence, and it is most useful precisely when it grows
          // large (an election is imminent as it nears electionTimeoutMin), so it is always shown - it is
          // not, and never was, the network latency the old "LATENCY" header implied.
          lastContactStr = state.lastRpcMs + " ms";
        }
        final RaftHAServer.ReplicationLatency rtt = rttByPeer.get(peerId);
        if (rtt != null)
          rttStr = String.format("%.2f ms", rtt.meanMs());
        if (clusterMonitor != null)
          statusStr = clusterMonitor.getReplicaStatus(peerId).name();
      }

      rows.add(new String[] { peerId, address, role, lagStr, rttStr, lastContactStr, statusStr });
    }

    // Deterministic row order: getLivePeers() gives no ordering guarantee, and a mere reordering must
    // not change the stable signature (it would re-emit an identical membership picture).
    rows.sort((a, b) -> a[0].compareTo(b[0]));

    return new ConfigSnapshot(term, commitIndex, rows, collectBootstrapBaselines(), haServer.getConfiguredServers());
  }

  /**
   * Hash over the stable columns only: the term plus each member's id, address, role and replica
   * status. LAG, RTT, LAST CONTACT and the commit index are deliberately excluded - they move on nearly
   * every lag-monitor tick and would defeat the deduplication (issue #5304).
   */
  static int stableSignature(final ConfigSnapshot snapshot) {
    int h = Long.hashCode(snapshot.term);
    for (final String[] row : snapshot.rows) {
      h = 31 * h + Objects.hashCode(row[0]); // SERVER
      h = 31 * h + Objects.hashCode(row[1]); // ADDRESS
      h = 31 * h + Objects.hashCode(row[2]); // ROLE
      h = 31 * h + Objects.hashCode(row[6]); // STATUS
    }
    return h;
  }

  /** Renders the ASCII cluster configuration table for the given snapshot. Pure function. */
  static String renderTable(final ConfigSnapshot snapshot) {
    final String[] headers = { "SERVER", "ADDRESS", "ROLE", "LAG", "RTT", "LAST CONTACT", "STATUS" };
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
      for (final Map<String, Object> fs : haServer.getFollowerStates()) {
        final FollowerReplicationState state = FollowerReplicationState.of(fs);
        // A degraded entry (issue #4842) carries no match index. Feeding the monitor a placeholder would
        // classify the peer from a number Ratis never reported - -1 is its never-appended sentinel and would
        // read as a dead replication path - and the raw cast it replaces aborted the whole tick (issue
        // #7041). Leave this peer's classification as it was and carry on with the others.
        if (!state.matchIndexKnown) {
          LogManager.instance().log(this, Level.FINE,
              "Replica lag tick: match index of '%s' unavailable while membership is changing; keeping its last classification",
              fs.get("peerId"));
          continue;
        }
        clusterMonitor.updateReplicaMatchIndex((String) fs.get("peerId"), state.matchIndex, state.lastRpcMs);
      }
      // Issue #5304: with the per-replica classifications refreshed, re-emit the CLUSTER CONFIGURATION
      // table when the stable picture (membership, roles, statuses, term) changed since the last
      // emission, so the logged view converges instead of freezing on the election-time snapshot.
      printClusterConfiguration();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Error checking replica lag", e);
    }
  }
}
