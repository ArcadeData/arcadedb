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
import org.apache.ratis.util.LifeCycle;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * Background health check that detects a Ratis server stuck in CLOSED or EXCEPTION
 * state after a network partition and triggers in-place recovery via
 * {@link HealthTarget#restartRatisIfNeeded()}.
 * <p>
 * It also detects a persistently lagging follower (issue #3893): a follower that diverged and
 * whose snapshot download failed on a quiet cluster, where no new log entry arrives to re-trigger
 * recovery. When the lag has persisted for the configured duration, it re-arms the download via
 * {@link HealthTarget#recoverFromPersistentLag()}. Disabled when {@code staleFollowerLagThreshold <= 0}.
 */
public final class HealthMonitor {

  /**
   * Minimal surface of RaftHAServer that the monitor depends on. Kept small for testing.
   */
  public interface HealthTarget {
    LifeCycle.State getRaftLifeCycleState();

    boolean isShutdownRequested();

    void restartRatisIfNeeded();

    /**
     * Returns {@code true} when this node is a follower lagging more than {@code lagThreshold}
     * entries behind the commit index while NOT actively catching up and with no snapshot
     * download already pending. Implementations must return {@code false} for the leader and
     * whenever the state cannot be determined.
     */
    default boolean isFollowerLaggingBeyond(final long lagThreshold) {
      return false;
    }

    /**
     * Re-arms a snapshot download from the leader for a persistently lagging follower.
     */
    default void recoverFromPersistentLag() {
    }

    /**
     * Returns {@code true} when this node is a running follower that is stuck against the leader: it
     * recognizes a leader at a newer term but cannot apply the leader's current-term entries because
     * its Raft log diverged (issue #4741). Unlike {@link #isFollowerLaggingBeyond(long)} this is not a
     * lag count - the divergence can be a single entry on an idle cluster - so it has no threshold.
     * Implementations must return {@code false} for the leader and whenever the state cannot be read.
     */
    default boolean isFollowerStuckDiverged() {
      return false;
    }

    /**
     * Recovers a follower that is stuck-diverged by reformatting its Raft storage and rejoining the
     * group as a fresh peer, which lets the leader reconcile it via the snapshot-install path.
     */
    default void recoverFromDivergence() {
    }

    /**
     * Proactively reconciles the inbound Raft gRPC peer allowlist with current DNS so a peer that
     * restarted with a new pod IP is admitted without first being rejected (issue #4696). No-op when
     * the allowlist is disabled; the filter itself throttles the DNS re-resolution.
     */
    default void refreshPeerAllowlist() {
    }

    /**
     * Logs Raft log catch-up resync progress when this node is a follower that is behind the leader.
     * No-op on the leader, when resync logging is disabled, or while a snapshot install is in progress.
     */
    default void reportResyncProgress() {
    }

    /**
     * Retries the snapshot resync of a follower that restarted onto a snapshot marker running ahead of
     * the entries it actually applied, and whose download has not succeeded yet (issue #6111). No-op
     * when there is no such gap, when one is already running, or on the leader.
     * <p>
     * This cannot be folded into {@link #isFollowerLaggingBeyond(long)} /
     * {@link #recoverFromPersistentLag()}: those are driven by {@code commitIndex - appliedIndex}, and
     * Ratis derives the applied index from the very marker that is ahead - so a node sitting on an
     * unfilled gap reports zero lag and looks perfectly caught up. Without its own hook the only thing
     * that would ever retry such a node is a leader election.
     */
    default void retryUnfilledSnapshotGap() {
    }

    /**
     * Describes a persistent Raft log write failure on this node, or returns {@code null} while the log writer
     * is healthy (issue #7037). Once Ratis's log worker hits an I/O error ({@code No space left on device} being
     * the reported one) it marks the log failed at that index and rejects every later append with
     * {@code RaftLogIOException: Log already failed}, while the division stays {@code RUNNING} - so none of the
     * lifecycle, lag or divergence checks can see it, and without this hook the node stays wedged until an
     * operator restarts it. Implementations must return {@code null} when the state cannot be read.
     */
    default String getRaftLogFailure() {
      return null;
    }

    /**
     * Whether the Raft storage volume currently has enough free space for the log writer to resume after an
     * in-place restart. A restart on a still-full volume fails the same way at once, so the monitor defers it
     * until the periodic log compaction (or the operator) has freed room. Implementations that cannot read the
     * volume must return {@code true}: the restart is then bounded by the crash-loop budget rather than never
     * attempted.
     */
    default boolean isRaftStorageWritable() {
      return true;
    }

    /**
     * Re-verifies, against the leader, every database this node kept through the bootstrap
     * "local is fresher, refuse to overwrite" guard (issue #6124), clearing the mark once the two
     * copies match and otherwise re-raising an operator-visible alert. No-op when no database took that
     * branch, on the leader, and between throttled attempts.
     * <p>
     * It needs its own hook for the same structural reason {@link #retryUnfilledSnapshotGap()} does: the
     * node is not lagging and not diverged in the Raft sense - it applies every entry it is sent - so
     * none of the lag- or divergence-driven checks can ever see it.
     */
    default void verifyBootstrapDivergence() {
    }
  }

  // How long (as a multiple of the recovery duration) the follower must look healthy before a prior
  // reformat episode is considered resolved and the bounded reformat budget re-arms.
  private static final long REFORMAT_EPISODE_RESET_MULTIPLIER = 5L;

  /**
   * Log-writer recovery (issue #7037): once the log writer has been healthy for this long after a recovery, the
   * incident is over and the restart budget re-arms, so a volume that fills again much later is recovered again
   * instead of hitting a budget spent on an old incident. A failure that never clears never re-arms it.
   * Package-private for tests.
   */
  static final long LOG_FAILURE_EPISODE_RESET_MS = 10L * 60_000L;

  /** At most one "deferred: volume still full" line per this window, matching the compaction scheduler's throttle. */
  static final long LOG_FAILURE_DEFERRED_WARNING_THROTTLE_MS = 60_000L;

  private final    HealthTarget             target;
  private final    long                     intervalMs;
  private final    long                     staleFollowerLagThreshold;
  private final    long                     staleFollowerRecoveryDurationMs;
  private final    boolean                  divergedFollowerRecoveryEnabled;
  private final    int                      divergedFollowerMaxReformats;
  // Crash-loop escalation (issue #5291): how many consecutive CLOSED/EXCEPTION restarts may fail to stick
  // before the monitor escalates (reformat once, then give up). 0 disables the escalation (legacy behaviour:
  // restart on every unhealthy tick forever).
  private final    int                      crashLoopRestartThreshold;
  private volatile ScheduledExecutorService executor;
  // Wall-clock time (ms) when the current uninterrupted lag streak was first observed; -1 = not lagging.
  private          long                     lagObservedSinceMs          = -1;
  // Wall-clock time (ms) when the current uninterrupted stuck-divergence streak was first observed; -1 = not stuck.
  private          long                     stuckObservedSinceMs        = -1;
  // Bounded reformat budget (#4741 review): reformats fired in the current divergence episode, the
  // time the follower started looking healthy again, and whether the budget is exhausted (logged once).
  private          int                      divergenceReformatCount     = 0;
  private          long                     divergenceHealthySinceMs    = -1;
  private          boolean                  divergenceRecoveryExhausted = false;
  // Crash-loop tracking (#5291): consecutive CLOSED/EXCEPTION restarts in the current unhealthy streak,
  // whether we already escalated to a one-shot reformat, and whether we have given up (logged once).
  private          int                      crashRestartStreak          = 0;
  private          boolean                  crashLoopReformatTried       = false;
  private          boolean                  crashLoopEscalated           = false;
  // Log-writer recovery (#7037): in-place restarts fired in the current failure episode, since when the writer
  // has been observed healthy again after them (-1 = not yet, or no episode), when the "deferred" line was last
  // logged, and whether the budget for this episode is spent (logged once).
  private          int                      logFailureRestarts           = 0;
  private          long                     logFailureClearSinceMs       = -1;
  private          long                     lastLogFailureDeferredWarnMs = -1;
  private          boolean                  logFailureEscalated          = false;
  // Injectable for deterministic tests; defaults to the system clock.
  private          LongSupplier             clock                       = System::currentTimeMillis;

  public HealthMonitor(final HealthTarget target, final long intervalMs) {
    this(target, intervalMs, 0L, 0L, false, 0);
  }

  public HealthMonitor(final HealthTarget target, final long intervalMs, final long staleFollowerLagThreshold,
      final long staleFollowerRecoveryDurationMs) {
    this(target, intervalMs, staleFollowerLagThreshold, staleFollowerRecoveryDurationMs, false, 0);
  }

  public HealthMonitor(final HealthTarget target, final long intervalMs, final long staleFollowerLagThreshold,
      final long staleFollowerRecoveryDurationMs, final boolean divergedFollowerRecoveryEnabled) {
    this(target, intervalMs, staleFollowerLagThreshold, staleFollowerRecoveryDurationMs, divergedFollowerRecoveryEnabled, 0);
  }

  public HealthMonitor(final HealthTarget target, final long intervalMs, final long staleFollowerLagThreshold,
      final long staleFollowerRecoveryDurationMs, final boolean divergedFollowerRecoveryEnabled,
      final int divergedFollowerMaxReformats) {
    this(target, intervalMs, staleFollowerLagThreshold, staleFollowerRecoveryDurationMs, divergedFollowerRecoveryEnabled,
        divergedFollowerMaxReformats, 0);
  }

  public HealthMonitor(final HealthTarget target, final long intervalMs, final long staleFollowerLagThreshold,
      final long staleFollowerRecoveryDurationMs, final boolean divergedFollowerRecoveryEnabled,
      final int divergedFollowerMaxReformats, final int crashLoopRestartThreshold) {
    this.target = target;
    this.intervalMs = intervalMs;
    this.staleFollowerLagThreshold = staleFollowerLagThreshold;
    this.staleFollowerRecoveryDurationMs = staleFollowerRecoveryDurationMs;
    this.divergedFollowerRecoveryEnabled = divergedFollowerRecoveryEnabled;
    this.divergedFollowerMaxReformats = divergedFollowerMaxReformats;
    this.crashLoopRestartThreshold = crashLoopRestartThreshold;
  }

  /** Package-private test hook to drive the persistence logic deterministically. */
  void setClock(final LongSupplier clock) {
    this.clock = clock;
  }

  public void start() {
    if (intervalMs <= 0) {
      LogManager.instance().log(this, Level.FINE, "HealthMonitor disabled (interval=%d)", intervalMs);
      return;
    }
    executor = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "arcadedb-raft-health-monitor");
      t.setDaemon(true);
      return t;
    });
    // Startup grace period: skip the first 2*intervalMs while Ratis completes its initial election.
    executor.scheduleWithFixedDelay(this::tickSafely, intervalMs * 2, intervalMs, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    final ScheduledExecutorService current = executor;
    if (current != null) {
      current.shutdownNow();
      executor = null;
    }
  }

  /**
   * Package-private for tests. Runs one check synchronously.
   */
  void tick() {
    if (target.isShutdownRequested())
      return;
    // Reconcile the inbound peer allowlist with current DNS every tick, regardless of Ratis lifecycle
    // state, so a returned peer's new pod IP is admitted proactively (issue #4696). The filter throttles
    // the actual re-resolution to its configured refresh interval.
    target.refreshPeerAllowlist();
    target.reportResyncProgress();
    // Independent of the lag checks below, which are structurally blind to an unfilled snapshot gap
    // (issue #6111). Self-throttled by the target.
    target.retryUnfilledSnapshotGap();
    // Likewise invisible to the lag and divergence checks below: a node the bootstrap overwrite guard
    // left with its own copy applies every entry it is sent and reports perfect health (issue #6124).
    // Self-throttled by the target, and free when no database took that branch.
    target.verifyBootstrapDivergence();
    final LifeCycle.State state = target.getRaftLifeCycleState();
    if (state == LifeCycle.State.CLOSED || state == LifeCycle.State.EXCEPTION) {
      handleUnhealthyState(state);
      return;
    }
    // Healthy lifecycle observed: a restart stuck, so a genuinely new incident later starts a fresh streak.
    crashRestartStreak = 0;
    crashLoopReformatTried = false;
    crashLoopEscalated = false;
    // A wedged log writer keeps the lifecycle RUNNING, so it is checked here, after the lifecycle branch and
    // before the follower checks: a node that rejects every append is behind for a reason neither a snapshot
    // re-arm nor a storage reformat can fix (issue #7037).
    final String logFailure = target.getRaftLogFailure();
    if (logFailure != null) {
      handleFailedLogWriter(logFailure);
      return;
    }
    noteLogWriterHealthy();
    // checkStaleFollower (lag: commit - applied > threshold) and checkStuckFollower (divergence:
    // commit == applied) are mutually exclusive by construction, so at most one arms per tick.
    checkStaleFollower();
    checkStuckFollower();
  }

  /**
   * Handles a Ratis lifecycle stuck in {@code CLOSED}/{@code EXCEPTION} with crash-loop escalation (issue
   * #5291). A plain {@link HealthTarget#restartRatisIfNeeded()} uses Ratis {@code RECOVER}, which reloads
   * the existing storage. When that storage is self-inconsistent - e.g. a term-inverted persisted log or a
   * poisoned snapshot-install where an entry {@code (t:3, i:4946)} is applied after {@code (t:4, i:4945)} -
   * the {@code StateMachineUpdater} throws {@code Failed updateLastAppliedTermIndex} a few hundred ms after
   * the Ratis server object builds successfully. Because the build itself succeeded,
   * {@code restartRatis()}'s own consecutive-failure escape never fires and the node returns to CLOSED and
   * is restarted every tick indefinitely.
   * <p>
   * With {@code crashLoopRestartThreshold > 0}, once restarts stop sticking (the node has been CLOSED on
   * every tick of the streak past the threshold) we escalate exactly once to a reformat-and-rejoin (which
   * fixes purely-local corruption by letting the leader reconcile this node via snapshot-install), and if
   * it still crash-loops we stop restarting and raise a single SEVERE alert for operator intervention - a
   * poisoned snapshot/log served by the leader re-poisons every fresh join, so a coordinated full-cluster
   * reformat is required. With the threshold at 0 the escalation is disabled and every unhealthy tick just
   * restarts, preserving the pre-#5291 behaviour.
   */
  private void handleUnhealthyState(final LifeCycle.State state) {
    crashRestartStreak++;

    // Already escalated and gave up: do not resume the restart churn. The node stays down (readiness
    // fails) and the SEVERE alert already told the operator; a pod/process restart is the way out.
    if (crashLoopEscalated)
      return;

    if (crashLoopRestartThreshold > 0 && crashRestartStreak > crashLoopRestartThreshold) {
      if (divergedFollowerRecoveryEnabled && !crashLoopReformatTried) {
        crashLoopReformatTried = true;
        LogManager.instance().log(this, Level.WARNING,
            "Ratis crash-loop: %d consecutive %s restarts did not stick; escalating to a Raft-storage reformat "
                + "+ rejoin so the leader can reconcile this node via snapshot-install (issue #5291)",
            crashRestartStreak, state);
        target.recoverFromDivergence();
        // Give the reformat a full fresh streak to prove it stuck before counting toward give-up.
        resetStreaksAfterRestart();
        crashRestartStreak = 0;
        return;
      }
      // Reformat already attempted (or divergence recovery disabled) and it still crash-loops: the
      // corruption is not local. Stop restarting and surface it once for operator intervention.
      crashLoopEscalated = true;
      LogManager.instance().log(this, Level.SEVERE,
          "Ratis crash-loop persists after %d restarts%s; giving up automatic restart - operator intervention "
              + "required. A follower that keeps returning to %s (e.g. 'Failed updateLastAppliedTermIndex: newTI "
              + "< oldTI') usually indicates a term-inverted Raft log or snapshot served by the leader; a "
              + "coordinated full-cluster Raft-storage reformat may be required (issue #5291)",
          crashRestartStreak, crashLoopReformatTried ? " and a storage reformat" : "", state);
      resetStreaksAfterRestart();
      return;
    }

    HALog.log(this, HALog.BASIC, "Health monitor detected Ratis %s state, attempting recovery", state);
    target.restartRatisIfNeeded();
    resetStreaksAfterRestart();
  }

  /**
   * Recovers a node whose Raft log writer failed persistently (issue #7037, follow-up to #5345). Ratis never
   * recovers the writer by itself: the first I/O error pins a {@code RaftLogIOException} that every later task
   * throws, and only a fresh server clears it. The recovery is a plain in-place {@code RECOVER} restart, the
   * same one the CLOSED/EXCEPTION branch uses, with two guards:
   * <ul>
   *   <li>it waits for the Raft storage volume to have room ({@link HealthTarget#isRaftStorageWritable()}),
   *       because a restart on a still-full volume wedges again at the first append. The periodic compaction
   *       (#5345) is what frees the room; until then a throttled SEVERE says the restart is deferred;</li>
   *   <li>it is bounded per episode by the same {@code crashLoopRestartThreshold} the lifecycle branch uses, so
   *       a failure that is not about space (a dying disk, a permission change) does not restart the server on
   *       every tick forever. The budget re-arms after {@link #LOG_FAILURE_EPISODE_RESET_MS} of quiet.</li>
   * </ul>
   * The two budgets are deliberately separate counters sharing one threshold: a CLOSED/EXCEPTION lifecycle and a
   * failed log writer are different incidents with different exits (a reformat escalation for the first, a
   * free-space gate for the second), and a node that goes through both in a row has had two incidents, not one.
   */
  private void handleFailedLogWriter(final String failure) {
    final long now = clock.getAsLong();
    logFailureClearSinceMs = -1; // the incident is ongoing: the healthy streak, if any, is over

    if (!target.isRaftStorageWritable()) {
      if (lastLogFailureDeferredWarnMs < 0 || now - lastLogFailureDeferredWarnMs >= LOG_FAILURE_DEFERRED_WARNING_THROTTLE_MS) {
        lastLogFailureDeferredWarnMs = now;
        LogManager.instance().log(this, Level.SEVERE,
            "Raft log writer failed %s and the Raft storage volume is still short of space: this node rejects every "
                + "append until restarted. Deferring the in-place restart until the periodic log compaction "
                + "(arcadedb.ha.snapshotInterval) or the operator frees room on the volume (issue #7037)", failure);
      }
      return;
    }

    if (crashLoopRestartThreshold > 0 && logFailureRestarts >= crashLoopRestartThreshold) {
      if (!logFailureEscalated) {
        logFailureEscalated = true;
        LogManager.instance().log(this, Level.SEVERE,
            "Raft log writer failed again (%s) after %d in-place restarts with free space available; the failure is "
                + "not about space. Giving up automatic restart - operator intervention required (check the Raft "
                + "storage volume for I/O errors or permission changes)",
            failure, logFailureRestarts);
      }
      return;
    }

    logFailureRestarts++;
    LogManager.instance().log(this, Level.WARNING,
        "Raft log writer failed %s; the Raft storage volume has room again, restarting Ratis in place to resume "
            + "replication (attempt %d, issue #7037)", failure, logFailureRestarts);
    target.restartRatisIfNeeded();
    resetStreaksAfterRestart();
  }

  /**
   * Healthy log writer observed. The restart budget of a past incident re-arms only once the writer has stayed
   * healthy for {@link #LOG_FAILURE_EPISODE_RESET_MS}: a restart clears the mark (fresh state machine), so a
   * failure that comes straight back is the same incident and keeps consuming the same budget, while one that
   * returns much later is a new incident. A budget exhausted while the failure persists is never re-armed by
   * time alone.
   */
  private void noteLogWriterHealthy() {
    if (logFailureRestarts == 0)
      return;
    final long now = clock.getAsLong();
    if (logFailureClearSinceMs < 0)
      logFailureClearSinceMs = now;
    else if (now - logFailureClearSinceMs >= LOG_FAILURE_EPISODE_RESET_MS) {
      logFailureRestarts = 0;
      logFailureClearSinceMs = -1;
      logFailureEscalated = false;
    }
  }

  /**
   * Drops any pending stale-follower / stuck-divergence streak (and the reformat budget) after a Ratis
   * restart, because the restart reinitializes the state machine and re-detects any snapshot gap, so a
   * lag/divergence recovery fired right after would be redundant.
   */
  private void resetStreaksAfterRestart() {
    lagObservedSinceMs = -1;
    stuckObservedSinceMs = -1;
    divergenceReformatCount = 0;
    divergenceHealthySinceMs = -1;
    divergenceRecoveryExhausted = false;
  }

  /**
   * Re-arms a snapshot download when a follower has lagged beyond the threshold, without actively
   * catching up, for at least {@link #staleFollowerRecoveryDurationMs}. The first observation only
   * starts the streak (so a single transient tick never triggers), and any tick where the lag is
   * gone resets it (issue #3893).
   */
  private void checkStaleFollower() {
    if (staleFollowerLagThreshold <= 0)
      return; // disabled

    if (!target.isFollowerLaggingBeyond(staleFollowerLagThreshold)) {
      lagObservedSinceMs = -1;
      return;
    }

    final long now = clock.getAsLong();
    if (lagObservedSinceMs == -1) {
      lagObservedSinceMs = now; // first observation; require persistence before acting
      return;
    }

    if (now - lagObservedSinceMs >= staleFollowerRecoveryDurationMs) {
      LogManager.instance().log(this, Level.WARNING,
          "Persistent follower lag beyond threshold %d for %dms, triggering snapshot recovery",
          staleFollowerLagThreshold, now - lagObservedSinceMs);
      target.recoverFromPersistentLag();
      lagObservedSinceMs = -1; // reset; the next streak re-arms only if the lag persists again
    }
  }

  /**
   * Reformats and rejoins a follower that has been stuck-diverged from the leader for at least
   * {@link #staleFollowerRecoveryDurationMs} (issue #4741). Mirrors {@link #checkStaleFollower()}:
   * the first observation only starts the streak, any tick where the stuck condition clears resets
   * it, and the recovery fires at most once per streak. Reuses the stale-follower recovery duration
   * so both self-healing paths share the same "must persist this long" knob.
   */
  private void checkStuckFollower() {
    if (!divergedFollowerRecoveryEnabled)
      return; // disabled

    final long now = clock.getAsLong();

    if (!target.isFollowerStuckDiverged()) {
      stuckObservedSinceMs = -1;
      // Forget a prior reformat episode once the follower has looked healthy long enough that the
      // divergence is considered resolved, re-arming the bounded reformat budget for any genuinely
      // new divergence later.
      if (divergenceReformatCount > 0) {
        if (divergenceHealthySinceMs == -1)
          divergenceHealthySinceMs = now;
        else if (now - divergenceHealthySinceMs >= staleFollowerRecoveryDurationMs * REFORMAT_EPISODE_RESET_MULTIPLIER) {
          divergenceReformatCount = 0;
          divergenceHealthySinceMs = -1;
          divergenceRecoveryExhausted = false;
        }
      }
      return;
    }

    divergenceHealthySinceMs = -1; // still stuck: the episode is ongoing

    if (stuckObservedSinceMs == -1) {
      stuckObservedSinceMs = now; // first observation; require persistence before acting
      return;
    }

    if (now - stuckObservedSinceMs < staleFollowerRecoveryDurationMs)
      return; // not persisted long enough yet

    // Bounded reformat budget (#4741 review): a reformat that restarts cleanly resets the shared Ratis
    // restart-retry counter, so a node whose divergence keeps reproducing would otherwise reformat +
    // full-snapshot-install every persistence window forever. Cap reformats per episode; once exhausted,
    // stop and surface it (SEVERE, once) for operator action instead of looping silently.
    if (divergedFollowerMaxReformats > 0 && divergenceReformatCount >= divergedFollowerMaxReformats) {
      if (!divergenceRecoveryExhausted) {
        divergenceRecoveryExhausted = true;
        LogManager.instance().log(this, Level.SEVERE,
            "Follower still stuck-diverged after %d automatic Raft-storage reformats; giving up auto-recovery - operator intervention required",
            divergedFollowerMaxReformats);
      }
      stuckObservedSinceMs = -1; // re-arm the persistence streak but do not reformat again
      return;
    }

    divergenceReformatCount++;
    LogManager.instance().log(this, Level.WARNING,
        "Follower stuck-diverged from leader for %dms, reformatting Raft storage and rejoining (attempt %d)",
        now - stuckObservedSinceMs, divergenceReformatCount);
    target.recoverFromDivergence();
    stuckObservedSinceMs = -1; // reset; the next streak re-arms only if the divergence persists again
  }

  private void tickSafely() {
    try {
      tick();
    } catch (final Throwable t) {
      LogManager.instance().log(this, Level.SEVERE, "Error in HealthMonitor tick: %s", t, t.getMessage());
    }
  }
}
