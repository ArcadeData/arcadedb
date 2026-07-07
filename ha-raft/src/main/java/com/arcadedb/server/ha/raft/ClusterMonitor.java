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
   * Maximum number of replication-channel resets attempted for a single continuous unreachable streak
   * (issue #4696). One reset fires per {@code peerChannelResetDurationMs} the follower stays unreachable,
   * so a first attempt that does not stick (e.g. the rebuilt channel still resolves stale DNS inside the
   * JVM positive-cache TTL) is retried instead of stranding the follower; after this many attempts the
   * monitor gives up and logs once for operator intervention. Re-armed when the follower reconnects.
   */
  static final int CHANNEL_RESET_MAX_ATTEMPTS = 5;

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
  private final    long                            peerChannelResetDurationMs;
  private final    Consumer<String>                unreachablePeerChannelHandler;
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
    this(lagWarningThreshold, stalledResyncDurationMs, stalledReplicaHandler, resyncNarrativeEnabled,
        peerUnreachableThresholdMs, 0L, null);
  }

  /**
   * @param peerChannelResetDurationMs    how long a follower must stay continuously unreachable (no
   *                                      successful RPC for at least {@code peerUnreachableThresholdMs})
   *                                      before {@code unreachablePeerChannelHandler} is invoked to reset
   *                                      its replication channel (issue #4696). {@code <= 0} disables the
   *                                      channel-reset recovery. Independent of the resync narrative.
   * @param unreachablePeerChannelHandler callback invoked (once per unreachable streak) with the peer id
   *                                      when the streak reaches {@code peerChannelResetDurationMs}. Must be
   *                                      non-null when {@code peerChannelResetDurationMs > 0}.
   */
  public ClusterMonitor(final long lagWarningThreshold, final long stalledResyncDurationMs,
      final Consumer<String> stalledReplicaHandler, final boolean resyncNarrativeEnabled,
      final long peerUnreachableThresholdMs, final long peerChannelResetDurationMs,
      final Consumer<String> unreachablePeerChannelHandler) {
    if (stalledResyncDurationMs > 0 && stalledReplicaHandler == null)
      throw new IllegalArgumentException(
          "stalledReplicaHandler must be non-null when stalledResyncDurationMs > 0 (leader-driven recovery enabled)");
    if (peerChannelResetDurationMs > 0 && unreachablePeerChannelHandler == null)
      throw new IllegalArgumentException(
          "unreachablePeerChannelHandler must be non-null when peerChannelResetDurationMs > 0 (channel-reset recovery enabled)");
    // The channel-reset recovery derives its "unreachable" signal from peerUnreachableThresholdMs, so a
    // disabled threshold silently prevents the reset from ever firing. Surface that misconfiguration
    // instead of letting the recovery look enabled while it can never trigger (issue #4696 review).
    if (peerChannelResetDurationMs > 0 && peerUnreachableThresholdMs <= 0)
      LogManager.instance().log(this, Level.WARNING,
          "Channel-reset recovery is enabled (arcadedb.ha.peerChannelResetDuration=%d) but the peer-unreachable threshold "
              + "(arcadedb.ha.peerUnreachableThreshold=%d) is disabled; the channel reset can never trigger. Set "
              + "arcadedb.ha.peerUnreachableThreshold > 0 to activate it.",
          peerChannelResetDurationMs, peerUnreachableThresholdMs);
    this.lagWarningThreshold = lagWarningThreshold;
    this.stalledResyncDurationMs = stalledResyncDurationMs;
    this.stalledReplicaHandler = stalledReplicaHandler;
    this.resyncNarrativeEnabled = resyncNarrativeEnabled;
    this.peerUnreachableThresholdMs = peerUnreachableThresholdMs;
    this.peerChannelResetDurationMs = peerChannelResetDurationMs;
    this.unreachablePeerChannelHandler = unreachablePeerChannelHandler;
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

    // Channel-level recovery (#4696): if the follower stays unreachable long enough, the leader resets
    // that follower's replication gRPC channel so a wedged appender re-resolves DNS and reconnects.
    // Independent of the resync narrative and of the lag-based stall recovery above.
    trackUnreachableForChannelReset(replicaId, state, lastRpcElapsedMs, now);

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
   * Drives the leader-side channel-level recovery for a follower whose outbound replication gRPC
   * channel has wedged (issue #4696). When a follower restarts with a new address (e.g. a Kubernetes
   * pod-IP change), grpc-java can keep returning the stale/negative DNS result on the leader's cached
   * appender channel, so the follower stays unreachable indefinitely even though DNS has healed. Once
   * the follower has been continuously unreachable for {@link #peerChannelResetDurationMs}, the leader
   * fires {@link #unreachablePeerChannelHandler} to close that one channel and force a fresh
   * re-resolution; unlike a leadership transfer this touches only the unreachable peer, so it cannot
   * flap the cluster.
   * <p>
   * The streak starts on the first unreachable tick and re-arms the moment the follower is reachable
   * again. Unlike a strict one-shot, it fires again every {@link #peerChannelResetDurationMs} the
   * follower stays unreachable, up to {@link #CHANNEL_RESET_MAX_ATTEMPTS} attempts, so a first reset
   * that does not stick (e.g. the rebuilt channel re-resolves stale DNS while the JVM positive-cache
   * TTL has not expired) does not leave the follower stranded - the exact failure mode this recovery
   * targets. After the cap it gives up and logs once (SEVERE) for operator intervention. Decoupled from
   * the resync narrative ({@link #resyncNarrativeEnabled}) so it works even with narrative logging off.
   * State is mutated only from the single lag-monitor thread.
   * <p>
   * The log announces the reset as <i>triggering</i> (intent), not as a completed action: the handler
   * ({@link RaftHAServer#resetPeerReplicationChannel}) owns and logs the concrete outcome, so a rare
   * no-op (e.g. a non-proxy RPC layer) is not over-reported here.
   */
  private void trackUnreachableForChannelReset(final String replicaId, final ReplicaState state,
      final long lastRpcElapsedMs, final long now) {
    if (peerChannelResetDurationMs <= 0 || unreachablePeerChannelHandler == null || peerUnreachableThresholdMs <= 0)
      return; // channel-reset recovery disabled

    final boolean unreachable = lastRpcElapsedMs >= peerUnreachableThresholdMs;

    if (!unreachable) {
      // Reachable again: end the streak and re-arm so a later stall can trigger a fresh reset cycle.
      state.channelUnreachableSinceMs = -1;
      state.channelLastResetAtMs = -1;
      state.channelResetCount = 0;
      state.channelResetGaveUp = false;
      return;
    }

    if (state.channelUnreachableSinceMs == -1) {
      // First unreachable tick: start the streak and measure the first reset interval from here.
      state.channelUnreachableSinceMs = now;
      state.channelLastResetAtMs = now;
      return;
    }

    // Only act once a full interval has elapsed since the streak start (first attempt) or the last reset
    // (subsequent attempts). A follower that reconnects within an interval clears the streak above and is
    // never reset, so an in-progress reconnection is not disrupted.
    if (now - state.channelLastResetAtMs < peerChannelResetDurationMs)
      return;

    if (state.channelResetCount >= CHANNEL_RESET_MAX_ATTEMPTS) {
      if (!state.channelResetGaveUp) {
        state.channelResetGaveUp = true;
        LogManager.instance().log(this, Level.SEVERE,
            "Follower '%s' still unreachable after %d replication-channel resets over %dms; giving up automatic channel "
                + "recovery - operator intervention (e.g. a leadership transfer) is required.",
            replicaId, state.channelResetCount, now - state.channelUnreachableSinceMs);
      }
      return;
    }

    state.channelResetCount++;
    state.channelLastResetAtMs = now;
    LogManager.instance().log(this, Level.WARNING,
        "Follower '%s' unreachable for %dms; triggering a replication-channel reset (attempt %d/%d) to force a fresh "
            + "DNS re-resolution and reconnect.",
        replicaId, now - state.channelUnreachableSinceMs, state.channelResetCount, CHANNEL_RESET_MAX_ATTEMPTS);
    try {
      unreachablePeerChannelHandler.accept(replicaId);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Channel-reset recovery for follower '%s' failed: %s", replicaId, e.getMessage());
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
    // Channel-reset streak state (#4696), mutated only from the single lag-monitor thread. Tracks how
    // long the follower has been continuously unreachable, when the last reset fired, how many resets
    // this streak has attempted, and whether the bounded budget has been exhausted (logged once).
    long          channelUnreachableSinceMs = -1;
    long          channelLastResetAtMs      = -1;
    int           channelResetCount         = 0;
    boolean       channelResetGaveUp        = false;

    ReplicaState(final long initialMatchIndex, final long initialLeaderCommitIndex) {
      this.lastMatchIndex = initialMatchIndex;
      this.lastLeaderCommitIndex = initialLeaderCommitIndex;
      this.lastLag = initialLeaderCommitIndex - initialMatchIndex;
    }
  }
}
