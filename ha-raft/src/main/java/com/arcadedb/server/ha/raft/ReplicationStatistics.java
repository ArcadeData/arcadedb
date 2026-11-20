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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.ha.raft.RaftHAServer.ReplicationLatency;
import com.arcadedb.server.monitor.HAReplicationStatsProvider;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.thirdparty.com.codahale.metrics.Timer;

/**
 * The replication figures this node publishes: the follower match and commit indexes as the leader sees them, the
 * replication latencies, and the per-database samples of schema instalments, unreferenced files and pending phase-2
 * commits that the metrics and the cluster status read.
 * <p>
 * Split out of {@link RaftHAServer}, which exposes them through its getters.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ReplicationStatistics {
  private final RaftHAServer ha;

  ReplicationStatistics(final RaftHAServer ha) {
    this.ha = ha;
  }

  /**
   * Aggregates {@link RaftHAServer#getFollowerStates()} into a single replication-health snapshot for metrics
   * (issue #4809 follow-up). The {@code maxFollowerLastContactMs} field is the leading indicator of
   * election churn: it is the worst time since the leader last reached any follower, and when it
   * approaches {@code arcadedb.ha.electionTimeoutMin} an election is imminent. Returns a not-leader
   * placeholder ({@code -1}/{@code 0}) when this node is not the leader.
   */
  public HAReplicationStatsProvider.HAReplicationStats getReplicationStats() {
    if (ha.raftServer == null || !ha.isLeader())
      return new HAReplicationStatsProvider.HAReplicationStats(false, -1, -1, 0);

    final List<Map<String, Object>> followers = ha.getFollowerStates();
    if (followers.isEmpty())
      return new HAReplicationStatsProvider.HAReplicationStats(true, -1, -1, 0);

    final long commitIndex = ha.getCommitIndex();
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
   * #4812). Combines the leader's Ratis follower indices ({@link RaftHAServer#getFollowerStates()}) with the
   * {@link ClusterMonitor}'s classification and sustained-lag duration. Empty when not the leader.
   */
  public List<HAReplicationStatsProvider.FollowerSample> getFollowerSamples() {
    if (ha.raftServer == null || !ha.isLeader())
      return List.of();

    final List<Map<String, Object>> followers = ha.getFollowerStates();
    if (followers.isEmpty())
      return List.of();

    final long commitIndex = ha.getCommitIndex();
    final List<HAReplicationStatsProvider.FollowerSample> samples = new ArrayList<>(followers.size());
    for (final Map<String, Object> follower : followers) {
      final String peerId = (String) follower.get("peerId");
      final long matchIndex = followerStateIndex(follower, "matchIndex");
      final long nextIndex = followerStateIndex(follower, "nextIndex");
      final long lastContactMs = followerStateIndex(follower, "lastRpcElapsedMs");
      final long lag = commitIndex >= 0 && matchIndex >= 0 ? Math.max(0L, commitIndex - matchIndex) : -1;
      final String status = ha.clusterMonitor != null ? ha.clusterMonitor.getReplicaStatus(peerId).name() : "UNKNOWN";
      final long laggingForMs = ha.clusterMonitor != null ? ha.clusterMonitor.getReplicaLaggingForMs(peerId) : 0;
      samples.add(new HAReplicationStatsProvider.FollowerSample(
          peerId, matchIndex, nextIndex, lag, lastContactMs, status, laggingForMs));
    }
    return samples;
  }

  /**
   * Reads one index of a {@link RaftHAServer#getFollowerStates()} entry, or {@code -1} when the entry does not carry it.
   * <p>
   * The match/next index keys are deliberately absent from the degraded entries
   * {@link #degradedFollowerStates} builds when membership churned faster than a consistent snapshot could
   * be taken (issue #4842). Every consumer must therefore read them through this method rather than with a
   * raw {@code (Long)} cast: the cast unboxes the missing value into a {@code NullPointerException}, and
   * because the consumers run inside catch-alls the failure did not crash anything - it silently dropped
   * the whole cluster-configuration table and aborted the lag-monitor tick, precisely while membership was
   * changing (issue #7041). Callers that need to tell "unknown" from Ratis's own {@code -1} never-appended
   * sentinel check {@link #hasFollowerStateIndex} first.
   */
  static long followerStateIndex(final Map<String, Object> state, final String key) {
    return state.get(key) instanceof Number n ? n.longValue() : -1L;
  }

  /** Whether a {@link RaftHAServer#getFollowerStates()} entry carries {@code key} at all (see {@link #followerStateIndex}). */
  static boolean hasFollowerStateIndex(final Map<String, Object> state, final String key) {
    return state.get(key) instanceof Number;
  }

  /**
   * Per-database schema-WAL instalment counters (issue #6144). Reported on EVERY node, not just the
   * leader, for the same reason the phase-2 holds below are: the instalments a node shipped while it
   * was the leader are what explain a stall its writers saw, and that question outlives the term.
   * <p>
   * Skips databases that are not currently in memory ({@code allowLoad=false}): a metrics refresh must
   * not turn into a database-open workload, and a database nobody has opened has shipped nothing.
   */
  public List<HAReplicationStatsProvider.SchemaInstalmentSample> getSchemaInstalmentSamples() {
    final List<HAReplicationStatsProvider.SchemaInstalmentSample> samples = new ArrayList<>();
    forEachOpenReplicatedDatabase((name, db) -> samples.add(new HAReplicationStatsProvider.SchemaInstalmentSample(
        name, db.getSchemaWalInstalmentsShipped(), db.getSchemaWalInstalmentTotalTimeMs(),
        db.getSchemaWalInstalmentMaxTimeMs())));
    return samples;
  }

  /**
   * Per-database count of files this node holds that no schema component claims (issue #6143). The
   * node that LOST leadership mid-session is the only one that logs the files it could not retire;
   * this is how the nodes still holding them say so, which is what makes the SEVERE line actionable
   * without reading a data directory by hand.
   * <p>
   * The count is memoized on the database behind a (file modification count, schema version) gate (issue #6168),
   * so this refresh - every 5 seconds, per open database, forever - walks the schema only when one of the two has
   * actually moved.
   */
  public List<HAReplicationStatsProvider.UnreferencedFilesSample> getUnreferencedFilesSamples() {
    final List<HAReplicationStatsProvider.UnreferencedFilesSample> samples = new ArrayList<>();
    forEachOpenReplicatedDatabase((name, db) -> samples.add(
        new HAReplicationStatsProvider.UnreferencedFilesSample(name, db.getUnreferencedFilesCount())));
    return samples;
  }

  /**
   * Visits every non-reserved database this server currently holds OPEN, as its Raft wrapper. Shared by the two
   * per-database metric collectors above so they agree on which databases exist and neither of them can open one.
   * A database being concurrently dropped or unloaded is skipped rather than allowed to fail the whole refresh.
   */
  private void forEachOpenReplicatedDatabase(final BiConsumer<String, RaftReplicatedDatabase> visitor) {
    if (ha.arcadeServer == null)
      return;

    for (final String dbName : ha.arcadeServer.getDatabaseNames()) {
      if (ArcadeDBServer.isReservedDatabaseName(dbName))
        continue;
      try {
        final ServerDatabase db = ha.arcadeServer.getDatabase(dbName, false, false);
        if (db != null && db.getWrappedDatabaseInstance() instanceof RaftReplicatedDatabase replicated)
          visitor.accept(dbName, replicated);
      } catch (final RuntimeException e) {
        LogManager.instance().log(ha, Level.FINE,
            "Skipping per-database HA metrics for '%s' (likely being dropped or unloaded): %s", dbName,
            e.getMessage());
      }
    }
  }

  /**
   * Transactions this node originated whose pages the Raft apply thread has not published yet (issue #6965): the
   * leader-side phase 2 in flight. Since the apply thread publishes them at the entry's log position, before the
   * applied index moves past it, no snapshot checkpoint is ever held back by them any more.
   */
  public HAReplicationStatsProvider.PendingPhase2Stats getPendingPhase2Stats() {
    final ArcadeStateMachine sm = ha.stateMachine;
    if (sm == null)
      return new HAReplicationStatsProvider.PendingPhase2Stats(0, 0);
    return new HAReplicationStatsProvider.PendingPhase2Stats(sm.pendingLocalCommits(), sm.oldestPendingLocalCommitMs());
  }

  /**
   * Aggregates a follower's appendEntries and heartbeat latency timers into a single count-weighted
   * RTT. The snapshot mean is nanoseconds; we convert to milliseconds. Returns {@code null} when
   * neither timer has recorded a single RPC yet (nothing meaningful to show).
   */
  static ReplicationLatency meanReplicationLatency(final SortedMap<String, Timer> timers, final String follower) {
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

  /** Returns the leader's follower peer-info list, or an empty list when this node is not a ready leader. */
  static List<RaftProtos.ServerRpcProto> leaderFollowerInfos(final DivisionInfo info) {
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
   * churned faster than {@link RaftHAServer#getFollowerStates()} could take a consistent snapshot. The match/next
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
}
