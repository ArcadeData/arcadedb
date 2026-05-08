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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Monitors replication lag per replica in a Raft cluster.
 * <p>
 * On every check tick (every 5 s in production via {@code RaftClusterStatusExporter#checkReplicaLag}),
 * the monitor classifies each replica into one of:
 * <ul>
 *   <li><b>STALLED</b> - replica's matchIndex did not advance at all while the leader's commit index
 *       grew. This is the dangerous pre-churn state: the leader is replicating but the replica is not
 *       persisting, so heartbeats / append-entries are getting starved by the actual replication
 *       traffic. Logged at {@code SEVERE} the first time we detect it (throttled per replica).</li>
 *   <li><b>FALLING_BEHIND</b> - lag grew since last tick. Logged at {@code WARNING} (throttled).</li>
 *   <li><b>CATCHING_UP</b> - lag shrank but is still over the threshold. Logged at {@code INFO}
 *       (throttled) so the operator sees recovery progress.</li>
 *   <li><b>HEALTHY</b> - lag is at or below the threshold. No log.</li>
 * </ul>
 * Each per-replica log is throttled to one line per {@link #LAG_LOG_THROTTLE_MS} so a sustained
 * bulk load doesn't spam the log. The intent is to give the operator a clear early signal BEFORE
 * the replica's heartbeat times out and triggers a leader election.
 */
public class ClusterMonitor {

  /** Throttle window for per-replica lag warnings, ms. */
  static final long LAG_LOG_THROTTLE_MS = 30_000;

  /**
   * Per-replica replication status reported in the cluster status table and the lag warning.
   * Order matches severity: {@link #HEALTHY} → {@link #STALLED}.
   */
  public enum ReplicaStatus {
    /** No tick recorded yet for this replica. */
    UNKNOWN,
    /** Lag is at or below {@code arcadedb.ha.replicationLagWarning}. */
    HEALTHY,
    /** Lag is over the threshold but shrinking - the follower is closing the gap. */
    CATCHING_UP,
    /** Lag is over the threshold and growing tick-over-tick. */
    FALLING_BEHIND,
    /** Replica's matchIndex did not advance at all while the leader's commit index grew. */
    STALLED
  }

  private final    long                            lagWarningThreshold;
  private volatile long                            leaderCommitIndex;
  private final    ConcurrentHashMap<String, ReplicaState> replicaStates = new ConcurrentHashMap<>();

  public ClusterMonitor(final long lagWarningThreshold) {
    this.lagWarningThreshold = lagWarningThreshold;
  }

  public void updateLeaderCommitIndex(final long commitIndex) {
    this.leaderCommitIndex = commitIndex;
  }

  public void updateReplicaMatchIndex(final String replicaId, final long matchIndex) {
    final long now = System.currentTimeMillis();
    final long leaderIdx = leaderCommitIndex;
    final long lag = leaderIdx - matchIndex;

    final ReplicaState state = replicaStates.computeIfAbsent(replicaId, k -> new ReplicaState(matchIndex, leaderIdx));

    final long replicaDelta = matchIndex - state.lastMatchIndex;
    final long leaderDelta = leaderIdx - state.lastLeaderCommitIndex;
    final long previousLag = state.lastLag;

    // Compute current status based on this tick.
    final ReplicaStatus status;
    if (lag <= lagWarningThreshold)
      status = ReplicaStatus.HEALTHY;
    else if (replicaDelta <= 0 && leaderDelta > 0)
      status = ReplicaStatus.STALLED;
    else if (lag > previousLag)
      status = ReplicaStatus.FALLING_BEHIND;
    else
      status = ReplicaStatus.CATCHING_UP;

    // Update snapshot before logging so concurrent readers (e.g. the cluster status table) see
    // fresh values.
    state.lastMatchIndex = matchIndex;
    state.lastLeaderCommitIndex = leaderIdx;
    state.lastLag = lag;
    state.status = status;

    // HEALTHY: nothing to say.
    if (status == ReplicaStatus.HEALTHY)
      return;

    // Throttle per replica so a sustained bulk load doesn't spam the log.
    if (now - state.lastWarnAtMs < LAG_LOG_THROTTLE_MS)
      return;
    state.lastWarnAtMs = now;

    switch (status) {
      case STALLED -> LogManager.instance().log(this, Level.SEVERE,
          "Replica '%s' STALLED: matchIndex stuck at %d while leader advanced by %d (current lag=%d). "
              + "This will trigger a leader election if it continues. Likely cause: replica disk "
              + "saturation or network stall. Check replica I/O, GC, and heartbeat connectivity.",
          replicaId, matchIndex, leaderDelta, lag);
      case FALLING_BEHIND -> LogManager.instance().log(this, Level.WARNING,
          "Replica '%s' falling behind: lag=%d (was %d, growing). Replica advanced %d entries; "
              + "leader advanced %d. Reduce per-batch size or raise arcadedb.ha.electionTimeoutMin/Max "
              + "to avoid churn under load.",
          replicaId, lag, previousLag, replicaDelta, leaderDelta);
      case CATCHING_UP -> LogManager.instance().log(this, Level.INFO,
          "Replica '%s' catching up: lag=%d (was %d), advancing at %d entries/tick (leader: %d/tick).",
          replicaId, lag, previousLag, replicaDelta, leaderDelta);
      default -> {
        /* HEALTHY/UNKNOWN handled above */
      }
    }
  }

  /**
   * Returns the latest classified status for {@code replicaId}, or {@link ReplicaStatus#UNKNOWN}
   * if no tick has been recorded yet (e.g. just-started leader, or peer that has never replied).
   */
  public ReplicaStatus getReplicaStatus(final String replicaId) {
    final ReplicaState s = replicaStates.get(replicaId);
    return s == null ? ReplicaStatus.UNKNOWN : s.status;
  }

  public void removeReplica(final String replicaId) {
    replicaStates.remove(replicaId);
  }

  public Map<String, Long> getReplicaLags() {
    if (replicaStates.isEmpty())
      return Collections.emptyMap();

    final Map<String, Long> lags = new ConcurrentHashMap<>();
    for (final Map.Entry<String, ReplicaState> entry : replicaStates.entrySet())
      lags.put(entry.getKey(), leaderCommitIndex - entry.getValue().lastMatchIndex);
    return lags;
  }

  public boolean isReplicaLagging(final String replicaId) {
    final ReplicaState state = replicaStates.get(replicaId);
    if (state == null)
      return false;
    return (leaderCommitIndex - state.lastMatchIndex) > lagWarningThreshold;
  }

  public long getLeaderCommitIndex() {
    return leaderCommitIndex;
  }

  public long getLagWarningThreshold() {
    return lagWarningThreshold;
  }

  /** Per-replica tracking. Mutated only from the single lag-monitor thread. */
  private static final class ReplicaState {
    long          lastMatchIndex;
    long          lastLeaderCommitIndex;
    long          lastLag;
    long          lastWarnAtMs;
    ReplicaStatus status = ReplicaStatus.UNKNOWN;

    ReplicaState(final long initialMatchIndex, final long initialLeaderCommitIndex) {
      this.lastMatchIndex = initialMatchIndex;
      this.lastLeaderCommitIndex = initialLeaderCommitIndex;
      this.lastLag = initialLeaderCommitIndex - initialMatchIndex;
    }
  }
}
