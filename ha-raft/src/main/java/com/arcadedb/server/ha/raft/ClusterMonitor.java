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
import java.util.function.ToLongFunction;
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
 *       traffic. Logged at {@code SEVERE} the first time we detect it (throttled per replica). A replica
 *       over the lag threshold whose matchIndex has not moved for {@link #ZERO_PROGRESS_STALL_GRACE_MS} is
 *       STALLED too, whether or not the leader advanced (issue #8341): see that constant. A replica whose
 *       {@code nextIndex} has fallen at or below this leader's own compacted log start is STALLED regardless
 *       of the numeric lag (issue #8457): {@code LogAppender.shouldInstallSnapshot} re-notifies an
 *       install-snapshot boundary the follower cannot use to resume ordinary replication, and the lag between
 *       the two can be small enough to hide under the warning threshold forever. Detection only: unlike the
 *       other STALLED causes, the leader-driven resync does not engage for this one (see
 *       {@link #trackStallForRecovery} for why), so recovering it is still a manual, operator-driven step.</li>
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
   * Fallback grace window (ms) a follower may sit at the never-appended sentinel ({@code matchIndex < 0}
   * while the leader holds committed entries, issue #5295) before it is reported {@link ReplicaStatus#STALLED}
   * instead of {@link ReplicaStatus#HEALTHY}. Used only when leader-driven resync is disabled
   * ({@code stalledResyncDurationMs <= 0}); when it is enabled the resync duration is used as the grace so the
   * status flips exactly when the leader acts. The grace absorbs the brief window of a normal join or an
   * in-progress snapshot install, where the follower is legitimately still at {@code -1} for a few ticks.
   */
  static final long NEVER_APPENDED_STALL_GRACE_MS = 30_000L;

  /**
   * How long (ms) a replica over the lag threshold may go without its {@code matchIndex} moving at all before it
   * is reported {@link ReplicaStatus#STALLED} even on ticks where the leader's commit index did not advance
   * (issue #8341). Two production lag ticks (5 s each).
   * <p>
   * The per-tick STALLED rule needs {@code leaderDelta > 0}. That misses the most dangerous case: when the stuck
   * replica is the vote the leader is missing, the leader cannot commit, its commit index stays flat, and the
   * replica read {@link ReplicaStatus#CATCHING_UP} ("advancing at 0 entries/tick") for as long as the outage
   * lasted, which the {@code lagging-followers} alert ignores. A replica that is behind and not moving is not
   * catching up, whatever the leader does. The grace keeps the first tick of a fresh baseline (a new leader, whose
   * probe has not landed yet) from being judged on a single sample. That is why this path waits for a grace while
   * the leader-advancing rule fires on the first tick: there, the leader moving while the replica does not is
   * already a comparison of two samples.
   */
  static final long ZERO_PROGRESS_STALL_GRACE_MS = 10_000L;

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
    /**
     * Replica's matchIndex did not advance at all while the leader's commit index grew, or has not advanced for
     * {@link #ZERO_PROGRESS_STALL_GRACE_MS} while the replica is over the lag threshold (issue #8341).
     */
    STALLED
  }

  private final    long                            lagWarningThreshold;
  private final    long                            stalledResyncDurationMs;
  private final    Consumer<String>                stalledReplicaHandler;
  private final    boolean                         resyncNarrativeEnabled;
  private final    long                            peerUnreachableThresholdMs;
  private final    long                            peerChannelResetDurationMs;
  private final    Consumer<String>                unreachablePeerChannelHandler;
  private final    Consumer<String>                exhaustedPeerChannelHandler;
  private volatile long                            leaderCommitIndex;
  private final    ConcurrentHashMap<String, ReplicaState> replicaStates = new ConcurrentHashMap<>();
  // Source of ReplicaState.stallGeneration (issue #8490). Monitor-wide rather than per replica so a streak started
  // after reset() - which discards every ReplicaState - can never reuse the generation of one started before it.
  // Written only from the single lag-monitor thread.
  private          long                            stallGenerationCounter;
  // Injectable clock for deterministic tests; defaults to the wall clock. Volatile because the test
  // thread writes it while the lag-monitor thread reads it (consistent with the other volatile fields).
  private volatile LongSupplier                    clock         = System::currentTimeMillis;
  // When each peer last answered OUTSIDE Raft (its HTTP capability advertisement), or -1 when unknown (issue #8900).
  // A follower that keeps answering there while every Raft RPC to it fails is up and refusing, not down: its channel
  // is reset at once instead of after a full peerChannelResetDurationMs. Volatile: wired after construction.
  private volatile ToLongFunction<String>          peerLastAnsweredAtMs;

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
    this(lagWarningThreshold, stalledResyncDurationMs, stalledReplicaHandler, resyncNarrativeEnabled,
        peerUnreachableThresholdMs, peerChannelResetDurationMs, unreachablePeerChannelHandler, null);
  }

  /**
   * @param exhaustedPeerChannelHandler callback invoked (once per unreachable streak) with the peer id when
   *                                    the bounded {@link #CHANNEL_RESET_MAX_ATTEMPTS} reset budget is
   *                                    exhausted and the follower is still unreachable (issue #5346). Lets
   *                                    the leader escalate - e.g. transfer leadership so a fresh appender is
   *                                    built - instead of only logging. {@code null} keeps the previous
   *                                    log-and-stop behaviour.
   */
  public ClusterMonitor(final long lagWarningThreshold, final long stalledResyncDurationMs,
      final Consumer<String> stalledReplicaHandler, final boolean resyncNarrativeEnabled,
      final long peerUnreachableThresholdMs, final long peerChannelResetDurationMs,
      final Consumer<String> unreachablePeerChannelHandler, final Consumer<String> exhaustedPeerChannelHandler) {
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
    this.exhaustedPeerChannelHandler = exhaustedPeerChannelHandler;
  }

  /**
   * Sets where the monitor learns when a peer last answered outside Raft (issue #8900): a time in the clock's units, or
   * -1 when unknown. {@code null} turns the early channel reset off.
   */
  void setPeerLastAnsweredAt(final ToLongFunction<String> peerLastAnsweredAtMs) {
    this.peerLastAnsweredAtMs = peerLastAnsweredAtMs;
  }

  /** Package-private test hook to drive the stall-duration logic deterministically. */
  void setClock(final LongSupplier clock) {
    this.clock = clock;
  }

  public void updateLeaderCommitIndex(final long commitIndex) {
    this.leaderCommitIndex = commitIndex;
  }

  public void updateReplicaMatchIndex(final String replicaId, final long matchIndex, final long lastRpcElapsedMs) {
    updateReplicaMatchIndex(replicaId, matchIndex, lastRpcElapsedMs, -1, -1);
  }

  /**
   * Same as {@link #updateReplicaMatchIndex(String, long, long)}, plus the two values needed to detect a
   * leader install-snapshot notify loop (issue #8457): this replica's {@code nextIndex} and this leader's own
   * current Raft log start index. Pass {@code -1} for either when it is not known this tick (e.g. a degraded
   * {@link RaftHAServer#getFollowerStates()} entry, or a membership snapshot with no leader log to read); the
   * install-snapshot-loop check is then skipped for this tick rather than compared against a fabricated value.
   *
   * @param nextIndex          this replica's {@code nextIndex} as tracked by the leader's log appender, or
   *                           {@code -1} if unknown.
   * @param leaderLogStartIndex this leader's own {@code RaftLog.getStartIndex()}, or {@code -1} if unknown.
   */
  public void updateReplicaMatchIndex(final String replicaId, final long matchIndex, final long lastRpcElapsedMs,
      final long nextIndex, final long leaderLogStartIndex) {
    final long now = clock.getAsLong();
    trackReachabilityForNarrative(replicaId, matchIndex, lastRpcElapsedMs, now);
    final long leaderIdx = leaderCommitIndex;
    final long lag = leaderIdx - matchIndex;

    final ReplicaState state = replicaStates.computeIfAbsent(replicaId, k -> new ReplicaState(matchIndex, leaderIdx));

    final long replicaDelta = matchIndex - state.lastMatchIndex;
    final long leaderDelta = leaderIdx - state.lastLeaderCommitIndex;
    final long previousLag = state.lastLag;

    // Issue #5295: a follower still at the never-appended sentinel (matchIndex < 0) while the leader
    // already holds committed entries (leaderIdx >= 0) has a dead replication path - not a single
    // AppendEntries has ever landed on it. Its numeric lag (leaderIdx - (-1)) can sit below the warning
    // threshold and mask it as HEALTHY, which also starves both leader-driven recoveries (they key on a
    // large lag). Track how long it has stayed at the sentinel so a brief join / snapshot-install window
    // is not misreported, then classify it STALLED once it persists past the grace.
    final boolean neverAppended = matchIndex < 0 && leaderIdx >= 0;
    if (!neverAppended)
      state.neverAppendedSinceMs = -1;
    else if (state.neverAppendedSinceMs == -1)
      state.neverAppendedSinceMs = now;
    final long neverAppendedGraceMs = stalledResyncDurationMs > 0 ? stalledResyncDurationMs : NEVER_APPENDED_STALL_GRACE_MS;
    final boolean neverAppendedStalled = neverAppended && now - state.neverAppendedSinceMs >= neverAppendedGraceMs;

    // Issue #5291: a follower can be caught up by matchIndex (lag <= threshold) yet not have answered a
    // single RPC for a long time - e.g. its Ratis member is CLOSED and crash-looping, so appends fail with
    // ServerNotReadyException while matchIndex stays frozen at its last high-water mark. Keying health on
    // lag alone reports it HEALTHY/lag-0 while it crashes ~20x/min. A follower that has not answered an RPC
    // within the unreachable threshold is not live: matchIndex is a stale high-water mark, not health.
    // Reuse the existing peer-unreachable threshold (0 = disabled, preserving the lag-only behaviour).
    final boolean unreachableStale = peerUnreachableThresholdMs > 0 && lastRpcElapsedMs >= peerUnreachableThresholdMs;

    // Issue #8341: how long the replica has been over the threshold without its matchIndex moving at all. Tracked
    // on every tick, independently of whether the leader advanced and of the leader-driven recovery streak (which
    // exists only when recovery is enabled), so the classification below does not depend on either. A replica still
    // at the never-appended sentinel is left to its own rule above (#5295), whose longer grace absorbs a join or a
    // snapshot install and whose log line names the real problem.
    if (neverAppended || lag <= lagWarningThreshold || replicaDelta > 0)
      state.zeroProgressSinceMs = -1;
    else if (state.zeroProgressSinceMs == -1)
      state.zeroProgressSinceMs = now;
    final boolean zeroProgressStalled =
        state.zeroProgressSinceMs != -1 && now - state.zeroProgressSinceMs >= ZERO_PROGRESS_STALL_GRACE_MS;

    // Issue #8457: a follower whose nextIndex has fallen at or below the leader's own compacted log start can
    // no longer be caught up by ordinary AppendEntries - LogAppender.shouldInstallSnapshot keeps answering true
    // for it, so the leader repeatedly re-notifies the same install-snapshot boundary instead of replicating.
    // leaderLogStartIndex > 0 requires the leader to have actually compacted at least once: a brand-new,
    // never-compacted log has start index 0, where nextIndex == 0 <= 0 would otherwise misclassify a perfectly
    // healthy, empty cluster on its very first tick. It shares neverAppendedGraceMs so a
    // genuinely in-progress (and progressing) snapshot install is not interrupted mid-flight.
    // In the loop the follower keeps answering (every ALREADY_INSTALLED reply refreshes its last-RPC time), so an
    // unreachable follower is excluded: it is a partition, owned by the reachability narrative and channel reset.
    final boolean installSnapshotLoopActive =
        !unreachableStale && nextIndex >= 0 && leaderLogStartIndex > 0 && nextIndex <= leaderLogStartIndex;
    if (!installSnapshotLoopActive)
      state.installSnapshotLoopSinceMs = -1;
    else if (state.installSnapshotLoopSinceMs == -1)
      state.installSnapshotLoopSinceMs = now;
    final boolean installSnapshotLoopStalled =
        installSnapshotLoopActive && now - state.installSnapshotLoopSinceMs >= neverAppendedGraceMs;

    // Compute current status based on this tick.
    final ReplicaStatus status;
    if (neverAppendedStalled || installSnapshotLoopStalled)
      status = ReplicaStatus.STALLED;
    else if (unreachableStale && lag <= lagWarningThreshold)
      // Caught up but not responding: report STALLED rather than masking it as HEALTHY (issue #5291).
      status = ReplicaStatus.STALLED;
    else if (lag <= lagWarningThreshold)
      status = ReplicaStatus.HEALTHY;
    else if (replicaDelta <= 0 && (leaderDelta > 0 || zeroProgressStalled))
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
    // The never-appended case (#5295) counts as stuck even with a small numeric lag, so the resync the
    // #4728 doc already promises for "matchIndex stuck at -1" actually engages here.
    trackStallForRecovery(replicaId, state, matchIndex, replicaDelta, lag, neverAppended, unreachableStale, now);

    // Channel-level recovery (#4696): if the follower stays unreachable long enough, the leader resets
    // that follower's replication gRPC channel so a wedged appender re-resolves DNS and reconnects.
    // Independent of the resync narrative and of the lag-based stall recovery above.
    trackUnreachableForChannelReset(replicaId, state, lastRpcElapsedMs, now);

    // HEALTHY: nothing to say. A caught-up-but-unreachable replica is now reported STALLED for status and
    // metrics (issue #5291), but the reachability narrative (and, if enabled, the channel-reset path) already
    // own its logging, so skip the lag-warning switch below to avoid a duplicate, mislabeled "disk stall" log.
    if (status == ReplicaStatus.HEALTHY || (unreachableStale && lag <= lagWarningThreshold))
      return;

    // Throttle per replica so a sustained bulk load doesn't spam the log.
    if (now - state.lastWarnAtMs < LAG_LOG_THROTTLE_MS)
      return;
    state.lastWarnAtMs = now;

    switch (status) {
      case STALLED -> {
        if (installSnapshotLoopStalled)
          // Issue #8457: the leader keeps re-notifying the same install-snapshot boundary and this replica keeps
          // answering ALREADY_INSTALLED without ever calling its state machine again - ordinary AppendEntries
          // cannot resume until nextIndex clears the leader's log start, which will not happen on its own.
          // Unlike the other STALLED causes below, POST /api/v1/cluster/resync/{database} does NOT clear this:
          // it only replaces database files over HTTP and never touches the Raft-log position that is actually
          // stuck, so no automatic recovery is offered here yet.
          LogManager.instance().log(this, Level.SEVERE,
              """
              Replica '%s' stuck behind an install-snapshot notify loop: nextIndex has not cleared this leader's \
              log start for %dms (lag=%d). The leader is repeatedly re-notifying the same snapshot boundary and \
              the replica keeps answering ALREADY_INSTALLED without progressing. POST /api/v1/cluster/resync/{database} \
              will NOT clear this (it only replaces database files, not the Raft log position). If the replica is \
              not in the middle of a snapshot download (check GET /api/v1/cluster on it), remove and re-add this \
              peer to the Raft configuration, or stop it, delete its Raft storage directory and restart it, to force \
              a fresh join.""",
              replicaId, now - state.installSnapshotLoopSinceMs, lag);
        else if (neverAppendedStalled)
          // Issue #5295: distinct message - this is not a slow/disk-saturated replica, it has never
          // received a single append. The leader-driven resync (if enabled) re-engages it; a leadership
          // transfer, which rebuilds the appender, is the operator fallback.
          LogManager.instance().log(this, Level.SEVERE, unreachableStale
                  // Issue #8490: an unreachable follower is down or partitioned, not wedged. It catches up through
                  // Raft when it returns, so no resync is forced while it is away.
                  ? """
                  Replica '%s' has NEVER received a single append (matchIndex=%d) while the leader committed up to \
                  %d, for %dms, and it is unreachable. No resync is forced while it is down: it catches up through \
                  Raft when it returns, and the resync engages only if it then stays reachable without progressing."""
                  : """
                  Replica '%s' has NEVER received a single append (matchIndex=%d) while the leader committed up to \
                  %d, for %dms: its replication path is dead. The leader will force a resync to re-engage it; if it \
                  persists, transfer leadership to that follower's healthy peer to rebuild the appender.""",
              replicaId, matchIndex, leaderIdx, now - state.neverAppendedSinceMs);
        else if (leaderDelta <= 0)
          // Issue #8341: the leader did not advance either. Most likely because it cannot: the stuck replica is
          // the vote the commit is waiting for. Say so, rather than calling it a catch-up at 0 entries/tick.
          LogManager.instance().log(this, Level.SEVERE,
              """
              Replica '%s' STALLED: matchIndex stuck at %d for %dms (lag=%d) and the leader did not advance either. \
              While it stays stuck it does not count toward the quorum: if the leader is waiting on its vote, writes \
              are blocked, and one more lost node stops them. %s""",
              replicaId, matchIndex, now - state.zeroProgressSinceMs, lag,
              stalledResyncDurationMs > 0 && stalledReplicaHandler != null
                  ? "The leader-driven resync (arcadedb.ha.stalledReplicaResyncDurationMs) recovers it; "
                      + "POST /api/v1/cluster/resync/{database} forces it now."
                  : "Leader-driven resync is disabled (arcadedb.ha.stalledReplicaResyncDurationMs=0): resync it with "
                      + "POST /api/v1/cluster/resync/{database}.");
        else
          LogManager.instance().log(this, Level.SEVERE,
              """
              Replica '%s' STALLED: matchIndex stuck at %d while leader advanced by %d (current lag=%d). \
              This will trigger a leader election if it continues. Likely cause: replica disk \
              saturation or network stall. Check replica I/O, GC, and heartbeat connectivity.""",
              replicaId, matchIndex, leaderDelta, lag);
      }
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
   * <p>
   * A follower that is UNREACHABLE (no successful RPC for {@link #peerUnreachableThresholdMs}) is never in a streak
   * (issue #8490). A stopped or partitioned node is not a stuck one: it catches up through ordinary Raft replication
   * once it returns, and forcing it to drop its databases only throws away a copy that was about to be current - the
   * order is carried out whenever the follower next answers, long after it was decided. Its {@code matchIndex} does
   * not move while it is away, so without this a node held down for longer than {@link #stalledResyncDurationMs}
   * always tripped the recovery. The streak re-arms from scratch when it reconnects, so the resync engages only for a
   * follower that stays reachable without progressing for the full duration. With the unreachable threshold disabled
   * ({@code <= 0}) reachability is unknown and the previous behaviour is kept.
   */
  private void trackStallForRecovery(final String replicaId, final ReplicaState state, final long matchIndex,
      final long replicaDelta, final long lag, final boolean neverAppended, final boolean unreachable, final long now) {
    if (stalledResyncDurationMs <= 0 || stalledReplicaHandler == null)
      return; // detection/logging still run, but leader-driven recovery is disabled

    state.unreachable = unreachable;
    if (unreachable) {
      state.stalledSinceMs = -1;
      state.resyncTriggered = false;
      return;
    }

    // A follower is "behind" enough to recover either when it lags past the warning threshold, or when it
    // has never received a single append (issue #5295): the latter is a dead replication path whose tiny
    // numeric lag would otherwise never reach the threshold, so the resync #4728 promises for it would
    // never fire. Both share the same duration guard below.
    //
    // Deliberately NOT extended to installSnapshotLoopStalled (issue #8457): forceResyncStalledReplica /
    // resyncDatabaseFromLeader only replace this follower's database FILES over HTTP - they never touch
    // Ratis's own log-matching state (matchIndex/nextIndex/the installed-snapshot marker Ratis's
    // SnapshotInstallationHandler consults), which is what is actually stuck here. Wiring this condition into
    // that recovery would fire once, replace files that were never wrong, and leave the notify loop exactly as
    // stuck as before. A recovery that actually clears it needs to reset this follower's Raft-log position
    // (e.g. the same storage-reformat-and-rejoin HA_DIVERGED_FOLLOWER_RECOVERY already does for a different
    // detection signature) - tracked as a follow-up rather than guessed at here.
    final boolean behind = lag > lagWarningThreshold || neverAppended;

    if (state.stalledSinceMs == -1) {
      // Not in a streak: start one when the replica is over the lag threshold and its matchIndex did
      // not advance this tick. The duration guard below absorbs transient blips.
      if (behind && replicaDelta <= 0) {
        state.stallGeneration = ++stallGenerationCounter;
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
   * targets. After the cap it fires {@link #exhaustedPeerChannelHandler} once so the leader can escalate
   * (issue #5346); with no such handler it logs once (SEVERE) for operator intervention. The escalation is
   * genuinely terminal for the streak: a wedged channel never becomes reachable again on its own, so
   * without it the recovery would sit in the given-up state until the leader process restarts. Decoupled from
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
      state.channelEarlyResetDone = false;
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
    // Issue #8900: one EXTRA reset, at once, when the follower answered outside Raft after the streak began. It is up
    // and refusing every Raft RPC - in #8898 the leader's appends kept reaching a CLOSED division through the
    // connection of a server that had been replaced - and a fresh channel reconnects to whatever holds the port now.
    // It is not counted against CHANNEL_RESET_MAX_ATTEMPTS and does not move channelLastResetAtMs, so the regular
    // schedule and the #5346 escalation (a leadership transfer) keep exactly the timing they had: "answers over HTTP"
    // also matches a slow in-place restart or a Raft-port-only network problem, which must not escalate any sooner.
    if (!state.channelEarlyResetDone && state.channelResetCount == 0
        && answeredOutsideRaftSince(replicaId, state.channelUnreachableSinceMs)) {
      state.channelEarlyResetDone = true;
      LogManager.instance().log(this, Level.WARNING,
          "Follower '%s' answers over HTTP but no Raft RPC to it has succeeded for %dms; resetting its replication "
              + "channel now so the leader reconnects to the server holding its Raft port.",
          replicaId, lastRpcElapsedMs);
      try {
        unreachablePeerChannelHandler.accept(replicaId);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Channel-reset recovery for follower '%s' failed: %s", replicaId, e.getMessage());
      }
      return;
    }
    if (now - state.channelLastResetAtMs < peerChannelResetDurationMs)
      return;

    if (state.channelResetCount >= CHANNEL_RESET_MAX_ATTEMPTS) {
      if (!state.channelResetGaveUp) {
        // Latch BEFORE invoking the handler so a handler that throws is not retried on every later tick
        // (issue #5346): the budget is spent either way, and a tight escalation loop would be worse than
        // the single failed attempt.
        state.channelResetGaveUp = true;
        if (exhaustedPeerChannelHandler == null) {
          LogManager.instance().log(this, Level.SEVERE,
              "Follower '%s' still unreachable after %d replication-channel resets over %dms; giving up automatic channel "
                  + "recovery - operator intervention (e.g. a leadership transfer) is required.",
              replicaId, state.channelResetCount, now - state.channelUnreachableSinceMs);
          return;
        }
        LogManager.instance().log(this, Level.SEVERE,
            "Follower '%s' still unreachable after %d replication-channel resets over %dms; escalating to rebuild the "
                + "appender for it.",
            replicaId, state.channelResetCount, now - state.channelUnreachableSinceMs);
        try {
          exhaustedPeerChannelHandler.accept(replicaId);
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING,
              "Channel-reset escalation for follower '%s' failed: %s", replicaId, e.getMessage());
        }
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

  /** Whether {@code replicaId} answered outside Raft at or after {@code sinceMs} (issue #8900). Never throws. */
  private boolean answeredOutsideRaftSince(final String replicaId, final long sinceMs) {
    final ToLongFunction<String> source = peerLastAnsweredAtMs;
    if (source == null || sinceMs < 0)
      return false;
    try {
      return source.applyAsLong(replicaId) >= sinceMs;
    } catch (final RuntimeException e) {
      return false;
    }
  }

  /**
   * Whether the leader-driven resync fired for {@code replicaId} is still warranted, read just before the leader
   * sends it (issue #8490). The order is decided on the lag-monitor thread and carried out on another one, possibly
   * much later: true only while the SAME stall streak is still running ({@code stallGeneration} is the one the order
   * was decided in, so neither a recovery nor a later streak can revive it, and a leadership change wiped it), the
   * replica is still reachable, and its {@code matchIndex} has not moved past the value the decision was based on.
   * <p>
   * The fields are read without a lock, so the combination is not an atomic snapshot of one tick. That is enough for
   * what this is - the leader's last look before sending - because the follower checks the order again against its
   * own state before it drops anything.
   */
  boolean isStalledResyncStillWarranted(final String replicaId, final long stallGeneration,
      final long observedMatchIndex) {
    final ReplicaState s = replicaStates.get(replicaId);
    return s != null && s.stallGeneration == stallGeneration && s.resyncTriggered && s.stalledSinceMs != -1
        && !s.unreachable && s.lastMatchIndex <= observedMatchIndex;
  }

  /**
   * The generation of {@code replicaId}'s current stall streak, or {@code -1} when no tick has been recorded yet.
   * Captured with a resync order so {@link #isStalledResyncStillWarranted} can tell that streak from a later one.
   */
  long getStallGeneration(final String replicaId) {
    final ReplicaState s = replicaStates.get(replicaId);
    return s == null ? -1 : s.stallGeneration;
  }

  /** The last {@code matchIndex} recorded for {@code replicaId}, or {@code -1} if no tick has been recorded yet. */
  long getReplicaMatchIndex(final String replicaId) {
    final ReplicaState s = replicaStates.get(replicaId);
    return s == null ? -1 : s.lastMatchIndex;
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
    volatile long lastMatchIndex;
    long          lastLeaderCommitIndex;
    long          lastLag;
    long          lastWarnAtMs;
    ReplicaStatus status = ReplicaStatus.UNKNOWN;
    // Wall-clock time (ms) when this replica first went non-HEALTHY in the current spell; -1 = healthy.
    // Read cross-thread (status JSON / metrics), written on the lag-monitor thread - same tolerated-
    // staleness contract as the snapshot fields above (issue #4812: surface "how long it's been slow").
    long          laggingSinceMs    = -1;
    // Stall-streak state for leader-driven recovery. Written only from the single lag-monitor thread; volatile
    // because the resync task re-reads stalledSinceMs, resyncTriggered and unreachable from the resync executor
    // just before it sends the order (isStalledResyncStillWarranted, issue #8490).
    // Wall-clock time (ms) when the current uninterrupted stall streak began; -1 = not stalled.
    volatile long    stalledSinceMs        = -1;
    // matchIndex observed when the streak began; the streak ends as soon as matchIndex moves past it. Not volatile,
    // unlike its neighbours: only trackStallForRecovery reads it, on the lag-monitor thread. Make it volatile before
    // reading it from anywhere else.
    long             stalledAtMatchIndex   = -1;
    // Identifies the current stall streak (issue #8490): taken from ClusterMonitor.stallGenerationCounter when the
    // streak starts, so a resync order decided in one streak is never taken for one decided in the next.
    volatile long    stallGeneration       = 0;
    // True once the leader-driven resync has been fired for the current streak (re-armed on recovery).
    volatile boolean resyncTriggered       = false;
    // Whether the replica was unreachable on the last tick the recovery looked at (issue #8490).
    volatile boolean unreachable           = false;
    // Wall-clock time (ms) when the replica was first observed still at the never-appended sentinel
    // (matchIndex < 0 while the leader already holds committed entries); -1 = it has appended at least
    // once or the leader has no committed entries yet (issue #5295).
    long          neverAppendedSinceMs  = -1;
    // Wall-clock time (ms) when the replica was first seen over the lag threshold with a matchIndex that has not
    // moved since; -1 = within the threshold or advancing (issue #8341). Lag-monitor thread only.
    long          zeroProgressSinceMs   = -1;
    // Wall-clock time (ms) when this replica's nextIndex was first seen at or below the leader's own log start
    // index; -1 = nextIndex is above the log start, or either value is unknown this tick (issue #8457).
    long          installSnapshotLoopSinceMs = -1;
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
    // Whether this streak already had its #8900 early reset of a follower that answers outside Raft.
    boolean       channelEarlyResetDone     = false;

    ReplicaState(final long initialMatchIndex, final long initialLeaderCommitIndex) {
      this.lastMatchIndex = initialMatchIndex;
      this.lastLeaderCommitIndex = initialLeaderCommitIndex;
      this.lastLag = initialLeaderCommitIndex - initialMatchIndex;
    }
  }
}
