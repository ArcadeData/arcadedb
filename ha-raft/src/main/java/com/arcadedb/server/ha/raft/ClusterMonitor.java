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
import java.util.function.Consumer;
import java.util.function.LongSupplier;
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
  private final    long                            stalledResyncDurationMs;
  private final    Consumer<String>                stalledReplicaHandler;
  private final    boolean                         resyncNarrativeEnabled;
  private final    long                            peerUnreachableThresholdMs;
  private volatile long                            leaderCommitIndex;
  private final    ConcurrentHashMap<String, ReplicaState> replicaStates = new ConcurrentHashMap<>();
  // Injectable clock for deterministic tests; defaults to the wall clock. Volatile because the test
  // thread writes it while the lag-monitor thread reads it (consistent with the other volatile fields).
  private volatile LongSupplier                    clock         = System::currentTimeMillis;

  public ClusterMonitor(final long lagWarningThreshold) {
    this(lagWarningThreshold, 0L, null);
  }

  /**
   * @param lagWarningThreshold     lag (in Raft log entries) above which a replica is reported as lagging.
   * @param stalledResyncDurationMs how long a replica must stay {@link ReplicaStatus#STALLED} continuously
   *                                before {@code stalledReplicaHandler} is invoked to force a recovery.
   *                                {@code <= 0} disables leader-driven recovery (detection/logging still run).
   * @param stalledReplicaHandler   callback invoked (once per stall streak) with the stalled replica's id when
   *                                the stall has persisted for {@code stalledResyncDurationMs}. May be {@code null}.
   */
  public ClusterMonitor(final long lagWarningThreshold, final long stalledResyncDurationMs,
      final Consumer<String> stalledReplicaHandler) {
    this(lagWarningThreshold, stalledResyncDurationMs, stalledReplicaHandler, false, 0L);
  }

  public ClusterMonitor(final long lagWarningThreshold, final long stalledResyncDurationMs,
      final Consumer<String> stalledReplicaHandler, final boolean resyncNarrativeEnabled,
      final long peerUnreachableThresholdMs) {
    if (stalledResyncDurationMs > 0 && stalledReplicaHandler == null)
      throw new IllegalArgumentException(
          "stalledReplicaHandler must be non-null when stalledResyncDurationMs > 0 (leader-driven recovery enabled)");
    this.lagWarningThreshold = lagWarningThreshold;
    this.stalledResyncDurationMs = stalledResyncDurationMs;
    this.stalledReplicaHandler = stalledReplicaHandler;
    this.resyncNarrativeEnabled = resyncNarrativeEnabled;
    this.peerUnreachableThresholdMs = peerUnreachableThresholdMs;
  }

  /** Package-private test hook to drive the stall-duration logic deterministically. */
  void setClock(final LongSupplier clock) {
    this.clock = clock;
  }

  public void updateLeaderCommitIndex(final long commitIndex) {
    this.leaderCommitIndex = commitIndex;
  }

  public void updateReplicaMatchIndex(final String replicaId, final long matchIndex, final long lastRpcElapsedMs) {
    final long now = clock.getAsLong();
    trackReachabilityForNarrative(replicaId, matchIndex, lastRpcElapsedMs, now);
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

    // Track how long this replica has been continuously non-HEALTHY (issue #4812). Start the clock
    // on the first non-healthy tick, keep it across CATCHING_UP/FALLING_BEHIND/STALLED, and reset it
    // the moment it recovers - so the JSON/alert/metrics can report a persistence duration, not just
    // a point-in-time snapshot.
    if (status == ReplicaStatus.HEALTHY)
      state.laggingSinceMs = -1;
    else if (state.laggingSinceMs == -1)
      state.laggingSinceMs = now;

    // Leader-driven recovery (#4728): if the replica stays stuck long enough, the leader actively
    // forces it to resync instead of merely logging forever. Tracked regardless of the log throttle.
    trackStallForRecovery(replicaId, state, matchIndex, replicaDelta, lag, now);

    // HEALTHY: nothing to say.
    if (status == ReplicaStatus.HEALTHY)
      return;

    // Throttle per replica so a sustained bulk load doesn't spam the log.
    if (now - state.lastWarnAtMs < LAG_LOG_THROTTLE_MS)
      return;
    state.lastWarnAtMs = now;

    switch (status) {
      case STALLED -> LogManager.instance().log(this, Level.SEVERE,
          """
          Replica '%s' STALLED: matchIndex stuck at %d while leader advanced by %d (current lag=%d). \
          This will trigger a leader election if it continues. Likely cause: replica disk \
          saturation or network stall. Check replica I/O, GC, and heartbeat connectivity.""",
          replicaId, matchIndex, leaderDelta, lag);
      case FALLING_BEHIND -> LogManager.instance().log(this, Level.WARNING,
          """
          Replica '%s' falling behind: lag=%d (was %d, growing). Replica advanced %d entries; \
          leader advanced %d. Reduce per-batch size or raise arcadedb.ha.electionTimeoutMin/Max \
          to avoid churn under load.""",
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
   * Drives the leader-driven recovery for a replica whose {@code matchIndex} is stuck (issue #4728).
   * A replica whose {@code matchIndex} never advances (e.g. stuck at -1) while it is far behind will
   * otherwise stay stuck forever, since the follower's own commit index does not advance and its
   * follower-side stale-recovery never fires. Here the leader, which is the node that actually
   * observes the stall, triggers the recovery once the stall has persisted for
   * {@link #stalledResyncDurationMs}.
   * <p>
   * The streak is intentionally decoupled from the per-tick {@link ReplicaStatus#STALLED}
   * classification, which requires the leader to have advanced <i>this</i> tick ({@code leaderDelta > 0}).
   * On a low-traffic cluster many ticks have no new commits, so keying the streak on that would reset
   * it on every quiet tick and a genuinely stuck replica would never reach the trigger duration. Instead
   * the streak persists as long as the replica stays over the lag threshold and its {@code matchIndex}
   * has not advanced at all since the streak began; it resets the moment the replica catches up (lag
   * drops to the threshold) or its {@code matchIndex} moves. The handler fires at most once per streak
   * and re-arms only after a reset.
   */
  private void trackStallForRecovery(final String replicaId, final ReplicaState state, final long matchIndex,
      final long replicaDelta, final long lag, final long now) {
    if (stalledResyncDurationMs <= 0 || stalledReplicaHandler == null)
      return; // detection/logging still run, but leader-driven recovery is disabled

    final boolean behind = lag > lagWarningThreshold;

    if (state.stalledSinceMs == -1) {
      // Not in a streak: start one when the replica is over the lag threshold and its matchIndex did
      // not advance this tick. The duration guard below absorbs transient blips.
      if (behind && replicaDelta <= 0) {
        state.stalledSinceMs = now;
        state.stalledAtMatchIndex = matchIndex;
      }
      return;
    }

    // In a streak: the replica recovered if it is no longer behind or its matchIndex moved at all.
    if (!behind || matchIndex > state.stalledAtMatchIndex) {
      state.stalledSinceMs = -1;
      state.resyncTriggered = false;
      return;
    }

    if (!state.resyncTriggered && now - state.stalledSinceMs >= stalledResyncDurationMs) {
      state.resyncTriggered = true;
      LogManager.instance().log(this, Level.WARNING,
          "Replica '%s' stuck (matchIndex=%d, lag=%d) for %dms: leader is forcing a resync to recover it.",
          replicaId, matchIndex, lag, now - state.stalledSinceMs);
      try {
        stalledReplicaHandler.accept(replicaId);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Leader-driven resync trigger for replica '%s' failed: %s", replicaId, e.getMessage());
      }
    }
  }

  /**
   * Emits a concise per-follower unreachable/reconnected narrative driven by the time since the last
   * successful RPC to the follower. Onset and reconnect are logged once each; while unreachable, a
   * reminder is logged at most once per {@link #LAG_LOG_THROTTLE_MS}. This replaces the raw Ratis
   * per-retry appender flood (which is suppressed by pinning the
   * {@code org.apache.ratis.grpc.server.GrpcLogAppender} level to SEVERE in arcadedb-log.properties)
   * and never changes Raft membership.
   */
  private void trackReachabilityForNarrative(final String replicaId, final long matchIndex, final long lastRpcElapsedMs,
      final long now) {
    if (!resyncNarrativeEnabled || peerUnreachableThresholdMs <= 0)
      return;

    final ReplicaState state = replicaStates.computeIfAbsent(replicaId,
        k -> new ReplicaState(matchIndex, leaderCommitIndex));

    final boolean unreachable = lastRpcElapsedMs >= peerUnreachableThresholdMs;

    if (unreachable) {
      if (state.unreachableSinceMs == -1) {
        state.unreachableSinceMs = now;
        state.lastUnreachableWarnAtMs = now;
        LogManager.instance().log(this, Level.INFO,
            "Follower '%s' unreachable (no successful RPC for %dms); holding replication and retrying. It will recover automatically when it returns.",
            replicaId, lastRpcElapsedMs);
      } else if (now - state.lastUnreachableWarnAtMs >= LAG_LOG_THROTTLE_MS) {
        state.lastUnreachableWarnAtMs = now;
        LogManager.instance().log(this, Level.INFO,
            "Follower '%s' still unreachable for %dms (matchIndex=%d).",
            replicaId, now - state.unreachableSinceMs, state.lastMatchIndex);
      }
    } else if (state.unreachableSinceMs != -1) {
      final long downMs = now - state.unreachableSinceMs;
      state.unreachableSinceMs = -1;
      LogManager.instance().log(this, Level.INFO,
          "Follower '%s' reconnected after %dms; replication resuming (matchIndex=%d).",
          replicaId, downMs, state.lastMatchIndex);
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

  /**
   * Discards all per-replica tracking and the cached leader commit index. Called whenever this node
   * (re)acquires leadership so the lag classifier starts from a clean baseline for the new term
   * (issue #4841).
   * <p>
   * A {@link ReplicaState} baseline ({@code lastMatchIndex}, {@code lastLeaderCommitIndex}, lag streak,
   * {@code laggingSinceMs}) is only meaningful within a single leadership term. After a re-election
   * Ratis resets each follower's {@code matchIndex} (it climbs again from the leader's probe), so the
   * first tick of the new term would compare the fresh low {@code matchIndex} against the high baseline
   * captured during a previous term, producing a large negative {@code replicaDelta} against a positive
   * {@code leaderDelta} and mis-classifying a healthy follower as {@link ReplicaStatus#STALLED} - which
   * can also trip the leader-driven resync (#4728). Clearing the map makes the next tick re-seed each
   * replica with {@code computeIfAbsent}, yielding {@code replicaDelta == 0} and {@code leaderDelta == 0}.
   * <p>
   * Safe to call concurrently with the lag-monitor thread: {@code replicaStates} is a
   * {@link ConcurrentHashMap} and {@code leaderCommitIndex} is volatile (and re-set on the next tick
   * before any replica is classified).
   */
  public void reset() {
    replicaStates.clear();
    leaderCommitIndex = 0;
  }

  public Map<String, Long> getReplicaLags() {
    if (replicaStates.isEmpty())
      return Collections.emptyMap();

    final Map<String, Long> lags = new ConcurrentHashMap<>();
    for (final Map.Entry<String, ReplicaState> entry : replicaStates.entrySet())
      lags.put(entry.getKey(), leaderCommitIndex - entry.getValue().lastMatchIndex);
    return lags;
  }

  /**
   * Milliseconds this replica has been continuously non-HEALTHY, or {@code 0} if it is healthy or
   * unknown (issue #4812). Lets callers distinguish a transient blip from a node that is constantly
   * slow.
   */
  public long getReplicaLaggingForMs(final String replicaId) {
    final ReplicaState s = replicaStates.get(replicaId);
    if (s == null || s.laggingSinceMs == -1)
      return 0;
    return Math.max(0, clock.getAsLong() - s.laggingSinceMs);
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
    // Wall-clock time (ms) when this replica first went non-HEALTHY in the current spell; -1 = healthy.
    // Read cross-thread (status JSON / metrics), written on the lag-monitor thread - same tolerated-
    // staleness contract as the snapshot fields above (issue #4812: surface "how long it's been slow").
    long          laggingSinceMs    = -1;
    // Stall-streak state for leader-driven recovery. Unlike the snapshot fields above (which the
    // status table / lag map read cross-thread, tolerating staleness), these three are BOTH written
    // and read only from the single lag-monitor thread, so they need no synchronization. Any future
    // change that reads or mutates them from another thread MUST add it.
    // Wall-clock time (ms) when the current uninterrupted stall streak began; -1 = not stalled.
    long          stalledSinceMs        = -1;
    // matchIndex observed when the streak began; the streak ends as soon as matchIndex moves past it.
    long          stalledAtMatchIndex   = -1;
    // True once the leader-driven resync has been fired for the current streak (re-armed on recovery).
    boolean       resyncTriggered       = false;
    // Reachability-narrative state, mutated only from the single lag-monitor thread.
    long          unreachableSinceMs      = -1;
    long          lastUnreachableWarnAtMs = 0;

    ReplicaState(final long initialMatchIndex, final long initialLeaderCommitIndex) {
      this.lastMatchIndex = initialMatchIndex;
      this.lastLeaderCommitIndex = initialLeaderCommitIndex;
      this.lastLag = initialLeaderCommitIndex - initialMatchIndex;
    }
  }
}
