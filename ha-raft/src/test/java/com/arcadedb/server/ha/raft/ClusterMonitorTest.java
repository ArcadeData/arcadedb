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

import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterMonitorTest {

  private CapturingLogger captured;
  private Logger          previousLogger;

  @BeforeEach
  void installCapturingLogger() {
    captured = new CapturingLogger();
    // Save the current ArcadeDB logger so we can restore it after the test (other tests in the
    // same JVM may rely on the standard DefaultLogger).
    previousLogger = new DefaultLogger();
    LogManager.instance().setLogger(captured);
  }

  @AfterEach
  void restoreLogger() {
    LogManager.instance().setLogger(previousLogger);
  }

  @Test
  void replicationLagIsZeroWhenEmpty() {
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    assertThat(monitor.getReplicaLags()).isEmpty();
  }

  @Test
  void tracksReplicaLag() {
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 90, 0L);
    monitor.updateReplicaMatchIndex("replica2", 50, 0L);

    final Map<String, Long> lags = monitor.getReplicaLags();
    assertThat(lags).containsEntry("replica1", 10L);
    assertThat(lags).containsEntry("replica2", 50L);
  }

  @Test
  void identifiesLaggingReplicas() {
    final ClusterMonitor monitor = new ClusterMonitor(20L);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 90, 0L);
    monitor.updateReplicaMatchIndex("replica2", 50, 0L);

    assertThat(monitor.isReplicaLagging("replica1")).isFalse();
    assertThat(monitor.isReplicaLagging("replica2")).isTrue();
  }

  /**
   * Sustained-lag tracking (issue #4812): laggingForMs is 0 while healthy, grows from the first
   * non-HEALTHY tick across the spell, and resets the moment the replica recovers.
   */
  @Test
  void tracksHowLongAReplicaHasBeenLagging() {
    final long[] now = { 1_000L };
    final ClusterMonitor monitor = new ClusterMonitor(50L);
    monitor.setClock(() -> now[0]);

    // Healthy tick: no lagging duration.
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    assertThat(monitor.getReplicaLaggingForMs("replica1")).isZero();

    // Goes over the threshold (lag 1000 > 50): the lagging clock starts now.
    now[0] = 2_000L;
    monitor.updateLeaderCommitIndex(1100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);

    now[0] = 5_000L; // 3s later, still lagging
    assertThat(monitor.getReplicaLaggingForMs("replica1")).isEqualTo(3_000L);

    // Recovers (lag back under threshold): the duration resets to 0.
    monitor.updateLeaderCommitIndex(1100);
    monitor.updateReplicaMatchIndex("replica1", 1100, 0L);
    assertThat(monitor.getReplicaLaggingForMs("replica1")).isZero();
  }

  @Test
  void removesReplica() {
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.updateReplicaMatchIndex("replica1", 50, 0L);
    monitor.removeReplica("replica1");
    assertThat(monitor.getReplicaLags()).isEmpty();
  }

  @Test
  void lagUpdatesWhenLeaderAdvances() {
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 95, 0L);
    assertThat(monitor.getReplicaLags().get("replica1")).isEqualTo(5L);

    monitor.updateLeaderCommitIndex(200);
    assertThat(monitor.getReplicaLags().get("replica1")).isEqualTo(105L);
  }

  /**
   * Replica's matchIndex doesn't advance while the leader's commit index grows = pre-churn signal.
   * Logged at SEVERE so it stands out in the operator's log.
   */
  @Test
  void stalledReplicaLogsSevereOnce() {
    final ClusterMonitor monitor = new ClusterMonitor(50L);
    // Tick 1: seed state - replica is at 100, leader at 100, no lag.
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    captured.clear(); // ignore the seeding tick

    // Tick 2: leader advances 1000 entries, replica is stuck at 100. Lag now 1000 (well over 50).
    monitor.updateLeaderCommitIndex(1100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);

    final List<CapturedLine> severe = captured.linesAtLevel(Level.SEVERE);
    assertThat(severe).hasSize(1);
    assertThat(severe.getFirst().message).contains("STALLED").contains("replica1");
  }

  /**
   * Lag is growing tick-over-tick: WARNING (not SEVERE). Throttled to one log per 30s.
   */
  @Test
  void growingLagLogsWarning() {
    final ClusterMonitor monitor = new ClusterMonitor(50L);
    // Seed: replica at 100, leader at 100.
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    captured.clear();

    // Tick: leader +500, replica +200. Replica IS advancing but slower. Lag = 300 (over 50).
    monitor.updateLeaderCommitIndex(600);
    monitor.updateReplicaMatchIndex("replica1", 300, 0L);

    final List<CapturedLine> warns = captured.linesAtLevel(Level.WARNING);
    assertThat(warns).hasSize(1);
    assertThat(warns.getFirst().message).contains("falling behind").contains("replica1");
    assertThat(captured.linesAtLevel(Level.SEVERE)).isEmpty();
  }

  /**
   * Healthy lag (<= threshold): no log. Confirms we don't spam when there's nothing to say.
   */
  @Test
  void healthyLagDoesNotLog() {
    final ClusterMonitor monitor = new ClusterMonitor(1000L);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 95, 0L);
    captured.clear();

    monitor.updateLeaderCommitIndex(200);
    monitor.updateReplicaMatchIndex("replica1", 195, 0L);

    assertThat(captured.lines).isEmpty();
  }

  /**
   * Same replica reported twice in quick succession produces only one log line.
   */
  @Test
  void perReplicaThrottlePreventsSpam() {
    final ClusterMonitor monitor = new ClusterMonitor(50L);
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    captured.clear();

    // First over-threshold tick: should log.
    monitor.updateLeaderCommitIndex(1100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    final int afterFirst = captured.lines.size();
    assertThat(afterFirst).isEqualTo(1);

    // Second over-threshold tick within the 30s throttle window: should NOT log.
    monitor.updateLeaderCommitIndex(2100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    assertThat(captured.lines).hasSize(afterFirst);
  }

  /**
   * Issue #4728: a replica stuck STALLED long enough must make the leader force a resync. The first
   * stalled tick only starts the streak; the callback fires once after the configured duration and
   * not before.
   */
  @Test
  void stalledReplicaTriggersLeaderDrivenResyncAfterDuration() {
    final List<String> resynced = new ArrayList<>();
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(50L, 30_000L, resynced::add);
    monitor.setClock(now::get);

    // Seed: replica at 100, leader at 100 (healthy).
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);

    // First STALLED tick (t=0): leader jumps to 1100, replica stuck at 100. Streak starts, no fire.
    monitor.updateLeaderCommitIndex(1100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    assertThat(resynced).isEmpty();

    // Still stalled at t=10s: duration (30s) not elapsed, no fire.
    now.set(10_000);
    monitor.updateLeaderCommitIndex(1110);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    assertThat(resynced).isEmpty();

    // Still stalled at t=31s: duration elapsed, fire exactly once.
    now.set(31_000);
    monitor.updateLeaderCommitIndex(1120);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    assertThat(resynced).containsExactly("replica1");

    // Still stalled at t=40s: already fired for this streak, no second fire.
    now.set(40_000);
    monitor.updateLeaderCommitIndex(1130);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    assertThat(resynced).containsExactly("replica1");
  }

  /** A replica that recovers resets the streak, so a later stall re-arms the leader-driven resync. */
  @Test
  void recoveryResetsStalledStreakSoItCanReArm() {
    final List<String> resynced = new ArrayList<>();
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(50L, 30_000L, resynced::add);
    monitor.setClock(now::get);

    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);

    // Stall, fire once at t=31s.
    monitor.updateLeaderCommitIndex(1100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    now.set(31_000);
    monitor.updateLeaderCommitIndex(1110);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    assertThat(resynced).hasSize(1);

    // Replica catches up fully (healthy) -> streak resets.
    now.set(40_000);
    monitor.updateLeaderCommitIndex(1110);
    monitor.updateReplicaMatchIndex("replica1", 1110, 0L);

    // New stall streak begins.
    now.set(50_000);
    monitor.updateLeaderCommitIndex(2110);
    monitor.updateReplicaMatchIndex("replica1", 1110, 0L);
    assertThat(resynced).hasSize(1); // first observation of the new streak, no fire yet

    now.set(90_000); // >30s after the new streak began
    monitor.updateLeaderCommitIndex(2120);
    monitor.updateReplicaMatchIndex("replica1", 1110, 0L);
    assertThat(resynced).hasSize(2);
  }

  /**
   * On a low-traffic cluster many lag-monitor ticks have no new leader commits. The stall streak must
   * NOT reset on those quiet ticks, otherwise a genuinely stuck replica would never reach the trigger
   * duration (#4728 - the reported cluster only advanced a few entries per 30s).
   */
  @Test
  void stallStreakSurvivesQuietLeaderTicks() {
    final List<String> resynced = new ArrayList<>();
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(50L, 30_000L, resynced::add);
    monitor.setClock(now::get);

    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);

    // Stall begins: leader jumps ahead, replica stuck at 100.
    monitor.updateLeaderCommitIndex(1100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);

    // Several quiet ticks where the leader does NOT advance (leaderDelta == 0) and the replica is
    // still stuck. The streak must keep counting from when it began.
    for (int t = 5; t <= 25; t += 5) {
      now.set(t * 1000L);
      monitor.updateLeaderCommitIndex(1100); // no new commits this tick
      monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    }
    assertThat(resynced).as("must not fire before the duration elapses").isEmpty();

    // Past the duration, still stuck on a quiet tick: must fire.
    now.set(31_000);
    monitor.updateLeaderCommitIndex(1100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    assertThat(resynced).containsExactly("replica1");
  }

  /** With the duration set to 0 (disabled), the leader never forces a resync however long the stall. */
  @Test
  void leaderDrivenResyncDisabledWhenDurationZero() {
    final List<String> resynced = new ArrayList<>();
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(50L, 0L, resynced::add);
    monitor.setClock(now::get);

    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    for (int i = 1; i <= 10; i++) {
      now.set(i * 60_000L);
      monitor.updateLeaderCommitIndex(100 + i * 1000L);
      monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    }
    assertThat(resynced).isEmpty();
  }

  /**
   * Issue #4841: per-replica state must not survive a leadership term boundary. After a re-election,
   * Ratis resets a follower's matchIndex (it climbs again from the leader's probe). If the monitor
   * still carries the high matchIndex baseline captured while this node led a previous term, the first
   * tick of the new term sees a large negative replicaDelta against a positive leaderDelta and
   * mis-classifies a perfectly healthy follower as STALLED. {@link ClusterMonitor#reset()} (invoked on
   * every fresh leadership acquisition) clears that baseline.
   */
  @Test
  void resetClearsStaleStateSoReElectionDoesNotFalselyReportStalled() {
    final ClusterMonitor monitor = new ClusterMonitor(50L);

    // Term 1: this node is leader, replica fully caught up at index 5000 (healthy, no log).
    monitor.updateLeaderCommitIndex(5000);
    monitor.updateReplicaMatchIndex("replica1", 5000, 0L);
    assertThat(monitor.getReplicaStatus("replica1")).isEqualTo(ClusterMonitor.ReplicaStatus.HEALTHY);
    captured.clear();

    // Leadership is lost and later re-acquired: the lag monitor restarts and resets its state.
    monitor.reset();
    assertThat(monitor.getReplicaStatus("replica1")).isEqualTo(ClusterMonitor.ReplicaStatus.UNKNOWN);
    assertThat(monitor.getReplicaLags()).isEmpty();

    // Term N first tick: the cluster log advanced to 20000 under other leaders while this node was a
    // follower, and Ratis reports the follower's matchIndex reset low (probe just started).
    monitor.updateLeaderCommitIndex(20000);
    monitor.updateReplicaMatchIndex("replica1", 0, 0L);

    // The follower is genuinely catching up, NOT stalled: a fresh baseline yields replicaDelta == 0
    // and leaderDelta == 0, so the STALLED rule (replicaDelta <= 0 && leaderDelta > 0) cannot fire.
    assertThat(monitor.getReplicaStatus("replica1")).isNotEqualTo(ClusterMonitor.ReplicaStatus.STALLED);
    assertThat(captured.linesAtLevel(Level.SEVERE)).isEmpty();
  }

  /**
   * Companion to {@link #resetClearsStaleStateSoReElectionDoesNotFalselyReportStalled()}: without the
   * reset, the carried-over baseline DOES produce the false STALLED classification. This documents the
   * defect the reset prevents.
   */
  @Test
  void withoutResetStaleBaselineFalselyReportsStalledAfterReElection() {
    final ClusterMonitor monitor = new ClusterMonitor(50L);

    monitor.updateLeaderCommitIndex(5000);
    monitor.updateReplicaMatchIndex("replica1", 5000, 0L);

    // No reset() here: the new term's low matchIndex is compared against the stale 5000 baseline.
    monitor.updateLeaderCommitIndex(20000);
    monitor.updateReplicaMatchIndex("replica1", 0, 0L);

    assertThat(monitor.getReplicaStatus("replica1")).isEqualTo(ClusterMonitor.ReplicaStatus.STALLED);
  }

  /**
   * Issue #4841: a stale lag streak must not survive into the next term either. If a replica was mid
   * stall-streak when this node lost leadership, {@link ClusterMonitor#reset()} must clear the streak
   * so the leader-driven resync (#4728) does not fire spuriously on the first tick of the new term.
   */
  @Test
  void resetClearsStallStreakSoLeaderDrivenResyncDoesNotFireOnReElection() {
    final List<String> resynced = new ArrayList<>();
    final AtomicLong now = new AtomicLong(0);
    final ClusterMonitor monitor = new ClusterMonitor(50L, 30_000L, resynced::add);
    monitor.setClock(now::get);

    // Build a stall streak in term 1.
    monitor.updateLeaderCommitIndex(100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);
    monitor.updateLeaderCommitIndex(1100);
    monitor.updateReplicaMatchIndex("replica1", 100, 0L);

    // Re-election: reset wipes the streak.
    monitor.reset();

    // Long after the old streak would have elapsed, the new term's first ticks must not fire a resync
    // off the cleared streak (the replica is healthy again under the new baseline).
    now.set(1_000_000);
    monitor.updateLeaderCommitIndex(20000);
    monitor.updateReplicaMatchIndex("replica1", 20000, 0L);
    assertThat(resynced).isEmpty();
  }

  @Test
  void emitsUnreachableThenReconnectedNarrative() {
    final AtomicLong now = new AtomicLong(100_000L);
    final ClusterMonitor monitor = new ClusterMonitor(10L, 0L, null, true, 10_000L);
    monitor.setClock(now::get);
    monitor.updateLeaderCommitIndex(1000L);

    captured.clear();
    // Reachable tick: lastRpcElapsedMs below threshold -> no unreachable line.
    monitor.updateReplicaMatchIndex("replica-2", 1000L, 200L);
    assertThat(captured.linesContaining("unreachable")).isEmpty();

    // Goes unreachable: lastRpcElapsedMs over threshold -> one onset line.
    now.addAndGet(1_000L);
    monitor.updateReplicaMatchIndex("replica-2", 1000L, 12_000L);
    assertThat(captured.linesContaining("unreachable")).hasSize(1);

    // Still unreachable within the 30s throttle window -> no new line.
    now.addAndGet(1_000L);
    monitor.updateReplicaMatchIndex("replica-2", 1000L, 13_000L);
    assertThat(captured.linesContaining("unreachable")).hasSize(1);

    // Comes back: lastRpcElapsedMs below threshold -> one reconnected line.
    now.addAndGet(1_000L);
    monitor.updateReplicaMatchIndex("replica-2", 1005L, 150L);
    assertThat(captured.linesContaining("reconnected")).hasSize(1);
  }

  @Test
  void noNarrativeWhenDisabled() {
    final ClusterMonitor monitor = new ClusterMonitor(10L, 0L, null, false, 10_000L);
    monitor.updateLeaderCommitIndex(1000L);
    captured.clear();
    monitor.updateReplicaMatchIndex("replica-2", 1000L, 12_000L);
    assertThat(captured.linesContaining("unreachable")).isEmpty();
  }

  @Test
  void noNarrativeWhenThresholdZero() {
    final ClusterMonitor monitor = new ClusterMonitor(10L, 0L, null, true, 0L);
    monitor.updateLeaderCommitIndex(1000L);
    captured.clear();
    monitor.updateReplicaMatchIndex("replica-2", 1000L, 12_000L);
    assertThat(captured.linesContaining("unreachable")).isEmpty();
  }

  /**
   * Channel-reset recovery (issue #4696): a follower that stays continuously unreachable fires the first
   * reset after the reset duration, then retries once per interval while it stays unreachable, and the
   * counter re-arms after it reconnects. The narrative is disabled here to prove the recovery is
   * decoupled from it.
   */
  @Test
  void resetsPeerChannelAfterSustainedUnreachability() {
    final AtomicLong now = new AtomicLong(100_000L);
    final List<String> resets = new ArrayList<>();
    // narrative disabled, unreachableThreshold=10s, channelResetDuration=30s.
    final ClusterMonitor monitor = new ClusterMonitor(10L, 0L, null, false, 10_000L, 30_000L, resets::add);
    monitor.setClock(now::get);
    monitor.updateLeaderCommitIndex(1000L);

    // Reachable: no streak.
    monitor.updateReplicaMatchIndex("replica-2", 1000L, 200L);
    assertThat(resets).isEmpty();

    // Goes unreachable: streak starts, but the reset must not fire yet.
    now.addAndGet(5_000L);
    monitor.updateReplicaMatchIndex("replica-2", 1000L, 12_000L);
    assertThat(resets).isEmpty();

    // Still unreachable, but only 20s into the streak (< 30s): no reset yet.
    now.addAndGet(20_000L);
    monitor.updateReplicaMatchIndex("replica-2", 1000L, 32_000L);
    assertThat(resets).isEmpty();

    // Crosses the 30s reset duration: the first reset fires.
    now.addAndGet(11_000L);
    monitor.updateReplicaMatchIndex("replica-2", 1000L, 43_000L);
    assertThat(resets).containsExactly("replica-2");

    // Still unreachable a full interval later: the reset is retried (a first attempt that did not stick
    // must not strand the follower).
    now.addAndGet(31_000L);
    monitor.updateReplicaMatchIndex("replica-2", 1000L, 74_000L);
    assertThat(resets).containsExactly("replica-2", "replica-2");

    // Reconnects: the streak (and its attempt counter) clears.
    now.addAndGet(1_000L);
    monitor.updateReplicaMatchIndex("replica-2", 1005L, 150L);

    // Goes unreachable again and crosses the interval: it re-arms and fires a fresh reset.
    now.addAndGet(1_000L);
    monitor.updateReplicaMatchIndex("replica-2", 1005L, 12_000L);   // streak restarts
    now.addAndGet(31_000L);
    monitor.updateReplicaMatchIndex("replica-2", 1005L, 43_000L);   // crosses 30s again
    assertThat(resets).containsExactly("replica-2", "replica-2", "replica-2");
  }

  /**
   * The periodic retry is bounded: after {@link ClusterMonitor#CHANNEL_RESET_MAX_ATTEMPTS} resets that
   * do not restore the follower, the monitor gives up (SEVERE, once) instead of resetting forever.
   */
  @Test
  void boundsChannelResetAttemptsThenGivesUp() {
    final AtomicLong now = new AtomicLong(100_000L);
    final List<String> resets = new ArrayList<>();
    final ClusterMonitor monitor = new ClusterMonitor(10L, 0L, null, false, 10_000L, 30_000L, resets::add);
    monitor.setClock(now::get);
    monitor.updateLeaderCommitIndex(1000L);

    // Start the unreachable streak.
    monitor.updateReplicaMatchIndex("replica-2", 1000L, 12_000L);

    // Drive many intervals while the follower stays unreachable; the reset must fire at most CAP times.
    for (int i = 0; i < ClusterMonitor.CHANNEL_RESET_MAX_ATTEMPTS + 3; i++) {
      now.addAndGet(31_000L);
      monitor.updateReplicaMatchIndex("replica-2", 1000L, 40_000L + i * 31_000L);
    }

    assertThat(resets).hasSize(ClusterMonitor.CHANNEL_RESET_MAX_ATTEMPTS);
    final List<CapturedLine> gaveUp = captured.linesAtLevel(Level.SEVERE);
    assertThat(gaveUp).hasSize(1);
    assertThat(gaveUp.getFirst().message).contains("giving up").contains("replica-2");
  }

  @Test
  void noChannelResetWhenDisabled() {
    final AtomicLong now = new AtomicLong(100_000L);
    final List<String> resets = new ArrayList<>();
    // channelResetDuration=0 disables the recovery even though the peer is unreachable.
    final ClusterMonitor monitor = new ClusterMonitor(10L, 0L, null, true, 10_000L, 0L, resets::add);
    monitor.setClock(now::get);
    monitor.updateLeaderCommitIndex(1000L);

    monitor.updateReplicaMatchIndex("replica-2", 1000L, 12_000L);
    now.addAndGet(120_000L);
    monitor.updateReplicaMatchIndex("replica-2", 1000L, 132_000L);
    assertThat(resets).isEmpty();
  }

  @Test
  void channelResetHandlerRequiredWhenEnabled() {
    assertThatThrownBy(() -> new ClusterMonitor(10L, 0L, null, true, 10_000L, 30_000L, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unreachablePeerChannelHandler");
  }

  /**
   * Misconfiguration guard (issue #4696 review): enabling the channel reset while the unreachable
   * threshold - the signal it depends on - is disabled logs a WARNING at construction, since the reset
   * could otherwise look enabled while never triggering.
   */
  @Test
  void warnsWhenChannelResetEnabledButUnreachableThresholdDisabled() {
    captured.clear();
    new ClusterMonitor(10L, 0L, null, false, 0L, 30_000L, s -> { });
    final List<CapturedLine> warnings = captured.linesAtLevel(Level.WARNING);
    assertThat(warnings).hasSize(1);
    assertThat(warnings.getFirst().message).contains("peerChannelResetDuration").contains("peerUnreachableThreshold");
  }

  @Test
  void noWarningWhenChannelResetAndThresholdBothEnabled() {
    captured.clear();
    new ClusterMonitor(10L, 0L, null, false, 10_000L, 30_000L, s -> { });
    assertThat(captured.linesContaining("peerUnreachableThreshold")).isEmpty();
  }

  /** ArcadeDB Logger that captures everything in memory for test assertions. */
  private static final class CapturingLogger implements Logger {
    final List<CapturedLine> lines = new ArrayList<>();

    @Override
    public void log(final Object req, final Level level, final String msg, final Throwable t, final String ctx,
        final Object a1, final Object a2, final Object a3, final Object a4, final Object a5,
        final Object a6, final Object a7, final Object a8, final Object a9, final Object a10,
        final Object a11, final Object a12, final Object a13, final Object a14, final Object a15,
        final Object a16, final Object a17) {
      final boolean hasParams = a1 != null || a2 != null || a3 != null || a4 != null || a5 != null
          || a6 != null || a7 != null || a8 != null || a9 != null || a10 != null
          || a11 != null || a12 != null || a13 != null || a14 != null || a15 != null
          || a16 != null || a17 != null;
      final String formatted = hasParams
          ? msg.formatted(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17)
          : msg;
      lines.add(new CapturedLine(level, formatted));
    }

    @Override
    public void log(final Object req, final Level level, final String msg, final Throwable t, final String ctx,
        final Object... args) {
      lines.add(new CapturedLine(level, args.length > 0 ? msg.formatted(args) : msg));
    }

    @Override
    public void flush() {
    }

    void clear() {
      lines.clear();
    }

    List<CapturedLine> linesAtLevel(final Level level) {
      return lines.stream().filter(l -> l.level.equals(level)).toList();
    }

    List<String> linesContaining(final String needle) {
      final List<String> out = new ArrayList<>();
      for (final CapturedLine l : lines)
        if (l.message().contains(needle))
          out.add(l.message());
      return out;
    }
  }

  private record CapturedLine(Level level, String message) {
  }
}
