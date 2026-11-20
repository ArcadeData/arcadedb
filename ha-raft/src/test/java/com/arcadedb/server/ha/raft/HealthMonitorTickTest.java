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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.monitor.HAReplicationStatsProvider.FollowerSample;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tick-by-tick behavior of the health monitor: streak counters, stuck flag, zero-progress catch-up and crash-loop escalation.
 */
class HealthMonitorTickTest {

  /**
   * Regression test for issue #10087: a streak counter means "seen on every tick since it started", so a tick that did
   * not evaluate the signal cannot extend it. The dead-updater and closed-log streaks only reset on the line after their
   * own check, so every early return above them (shutdown, {@code CLOSING}, {@code CLOSED}/{@code EXCEPTION}, a dead
   * updater) left the streak standing and two sightings separated by such a tick fired a restart that no two consecutive
   * ticks had earned. The table is over pairs of tick states rather than one test per counter, so a counter added to
   * {@code tick()} later is covered by construction.

   */
  @Nested
  class TickStreaks {

    /** What one tick of the monitor can find. */
    enum Tick {
      HEALTHY,
      SHUTDOWN_REQUESTED,
      CLOSING,
      CLOSED,
      DEAD_UPDATER,
      LOG_CLOSED
    }

    /** The signals whose streak needs the guard of consecutive ticks, i.e. those that must be seen twice to act. */
    enum Streak {
      CLOSING_STATE(Tick.CLOSING),
      DEAD_UPDATER_THREAD(Tick.DEAD_UPDATER),
      CLOSED_RAFT_LOG(Tick.LOG_CLOSED);

      final Tick tick;

      Streak(final Tick tick) {
        this.tick = tick;
      }
    }

    /** A target whose findings are scripted tick by tick. */
    private static final class ScriptedTarget extends HealthMonitorTest.FakeHealthTarget {
      volatile boolean logClosed = false;

      void apply(final Tick tick) {
        state.set(tick == Tick.CLOSING ? LifeCycle.State.CLOSING : tick == Tick.CLOSED ? LifeCycle.State.CLOSED : LifeCycle.State.RUNNING);
        shutdownRequested = tick == Tick.SHUTDOWN_REQUESTED;
        deadUpdater = tick == Tick.DEAD_UPDATER ? "the Ratis StateMachineUpdater thread 'x' has terminated" : null;
        logClosed = tick == Tick.LOG_CLOSED;
      }

      @Override
      public boolean isRaftLogClosed() {
        return logClosed;
      }
    }

    private static int restartsAfter(final List<Tick> ticks) {
      final ScriptedTarget target = new ScriptedTarget();
      final HealthMonitor monitor = new HealthMonitor(target, 0);
      for (final Tick tick : ticks) {
        target.apply(tick);
        monitor.tick();
      }
      return target.recoveryCalls.get();
    }

    /** What the in-between tick does on its own: a CLOSED division is restarted at once, nothing else acts on one tick. */
    private static int restartsOfOneTickAlone(final Tick tick) {
      return tick == Tick.CLOSED ? 1 : 0;
    }

    // Regression for #10087: TickStreaks
    @ParameterizedTest
    @EnumSource(Streak.class)
    void theSignalOnTwoConsecutiveTicksStillActs(final Streak streak) {
      assertThat(restartsAfter(List.of(streak.tick, streak.tick))).as("%s on consecutive ticks", streak).isEqualTo(1);
    }

    // Regression for #10087: TickStreaks
    @ParameterizedTest
    @EnumSource(Streak.class)
    void aTickThatDidNotEvaluateTheSignalDropsItsStreak(final Streak streak) {
      final List<String> offenders = new ArrayList<>();
      for (final Tick between : Tick.values()) {
        if (between == streak.tick)
          continue;
        final int restarts = restartsAfter(List.of(streak.tick, between, streak.tick));
        if (restarts != restartsOfOneTickAlone(between))
          offenders.add(between + " -> " + restarts + " restarts, expected " + restartsOfOneTickAlone(between));
      }
      assertThat(offenders).as("%s seen, then a tick in between, then %s again: no two consecutive ticks saw it", streak, streak)
          .isEmpty();
    }

    // Regression for #10087: TickStreaks
    @Test
    void aClosedLogReopenedByAnInPlaceRestartIsNotMistakenForAZombie() {
      // (log closed), (division CLOSED then recovered by the monitor), (log closed again): the restart in between is the
      // one the CLOSED tick asked for, and the third tick is the first sighting of a new streak
      assertThat(restartsAfter(List.of(Tick.LOG_CLOSED, Tick.CLOSED, Tick.LOG_CLOSED))).isEqualTo(1);
    }

    // Regression for #10087: TickStreaks
    @Test
    void aStreakSurvivesNothingThatDroppedItAcrossSeveralTicks() {
      // the dropped streak starts again from one, it does not resume where it was: a fresh pair still acts
      assertThat(restartsAfter(List.of(Tick.DEAD_UPDATER, Tick.SHUTDOWN_REQUESTED, Tick.DEAD_UPDATER, Tick.DEAD_UPDATER))).isEqualTo(1);
      assertThat(restartsAfter(List.of(Tick.LOG_CLOSED, Tick.CLOSING, Tick.LOG_CLOSED, Tick.LOG_CLOSED))).isEqualTo(1);
    }
  }

  /**
   * Issue #8389: the debounced stuck-at-stale-term flag, and the observation streaks behind it, must only describe ticks
   * that actually evaluated the follower checks. {@code tick()} returns early on a requested shutdown, on a
   * CLOSED/EXCEPTION division and on a failed Raft log writer, and each of those ticks skips
   * {@code checkStaleFollower()}/{@code checkStuckFollower()}. A flag left standing across them kept
   * {@code GET /api/v1/cluster} answering {@code localStuckAtStaleTerm: true} with a critical alert promising a
   * self-heal the same early returns made unreachable; a streak left standing across them fired a reformat (or a
   * snapshot re-arm) on the very first tick back, without the fresh second observation the streak exists to demand.
   */
  @Nested
  class StuckFlagClearedOnEarlyReturn {

    private static final long DURATION_MS = 5_000L;

    static final class FakeTarget implements HealthMonitor.HealthTarget {
      final    AtomicReference<LifeCycle.State> state                = new AtomicReference<>(LifeCycle.State.RUNNING);
      final    AtomicInteger                    restarts             = new AtomicInteger();
      final    AtomicInteger                    divergenceRecover    = new AtomicInteger();
      final    AtomicInteger                    persistentLagRecover = new AtomicInteger();
      volatile boolean                          shutdownRequested    = false;
      volatile boolean                          stuckDiverged        = false;
      volatile boolean                          lagging              = false;
      volatile String                           logFailure           = null;
      volatile boolean                          storageWritable      = true;

      @Override
      public LifeCycle.State getRaftLifeCycleState() {
        return state.get();
      }

      @Override
      public boolean isShutdownRequested() {
        return shutdownRequested;
      }

      @Override
      public void restartRatisIfNeeded() {
        restarts.incrementAndGet();
        logFailure = null; // a restart builds a fresh state machine, which is what clears the log-writer mark
      }

      @Override
      public boolean isFollowerStuckDiverged() {
        return stuckDiverged;
      }

      @Override
      public void recoverFromDivergence() {
        divergenceRecover.incrementAndGet();
      }

      @Override
      public boolean isFollowerLaggingBeyond(final long lagThreshold) {
        return lagging;
      }

      @Override
      public void recoverFromPersistentLag() {
        persistentLagRecover.incrementAndGet();
      }

      @Override
      public String getRaftLogFailure() {
        return logFailure;
      }

      @Override
      public boolean isRaftStorageWritable() {
        return storageWritable;
      }
    }

    private static HealthMonitor monitor(final FakeTarget target, final AtomicLong clock, final boolean divergedRecovery,
        final long lagThreshold, final int restartThreshold) {
      final HealthMonitor monitor = new HealthMonitor(target, 1000L, lagThreshold, DURATION_MS, divergedRecovery, 0,
          restartThreshold);
      monitor.setClock(clock::get);
      return monitor;
    }

    /** Two ticks on the stuck signature: the flag is confirmed, which is the precondition of every scenario below. */
    private static void confirmStuck(final HealthMonitor monitor, final FakeTarget target, final AtomicLong clock) {
      target.stuckDiverged = true;
      monitor.tick();
      clock.addAndGet(1_000L);
      monitor.tick();
      assertThat(monitor.isFollowerStuckDivergedConfirmed()).as("precondition: stuck confirmed on two ticks").isTrue();
    }

    // Regression for #8389: StuckFlagClearedOnEarlyReturn
    @Test
    void aFailedLogWriterWaitingForSpaceDropsTheConfirmedFlag() {
      final FakeTarget target = new FakeTarget();
      final AtomicLong clock = new AtomicLong(0L);
      final HealthMonitor monitor = monitor(target, clock, true, 0L, 10);
      confirmStuck(monitor, target, clock);

      target.logFailure = "at index 4946: java.io.IOException: No space left on device";
      target.storageWritable = false;
      clock.addAndGet(1_000L);
      monitor.tick();

      assertThat(target.restarts.get()).as("the restart is deferred, so nothing resets the streaks on the way").isZero();
      assertThat(monitor.isFollowerStuckDivergedConfirmed())
          .as("a tick that returned before checkStuckFollower() observed nothing and must not leave the flag standing")
          .isFalse();
    }

    // Regression for #8389: StuckFlagClearedOnEarlyReturn
    @Test
    void aFailedLogWriterWithItsRestartBudgetSpentDropsTheConfirmedFlag() {
      final FakeTarget target = new FakeTarget();
      final AtomicLong clock = new AtomicLong(0L);
      final HealthMonitor monitor = monitor(target, clock, true, 0L, 1);

      // Spend the one-restart budget: the restart clears the mark, and the episode stays open while the writer has been
      // healthy for less than LOG_FAILURE_EPISODE_RESET_MS.
      target.logFailure = "at index 12: java.io.IOException: Input/output error";
      monitor.tick();
      assertThat(target.restarts.get()).isEqualTo(1);

      clock.addAndGet(1_000L);
      confirmStuck(monitor, target, clock);

      // The failure comes straight back: the budget is spent, so handleFailedLogWriter() gives up and returns.
      target.logFailure = "at index 13: java.io.IOException: Input/output error";
      clock.addAndGet(1_000L);
      monitor.tick();

      assertThat(target.restarts.get()).as("budget spent: no second restart").isEqualTo(1);
      assertThat(monitor.isFollowerStuckDivergedConfirmed()).isFalse();
    }

    // Regression for #8389: StuckFlagClearedOnEarlyReturn
    @Test
    void aRequestedShutdownDropsTheConfirmedFlag() {
      final FakeTarget target = new FakeTarget();
      final AtomicLong clock = new AtomicLong(0L);
      final HealthMonitor monitor = monitor(target, clock, true, 0L, 10);
      confirmStuck(monitor, target, clock);

      target.shutdownRequested = true;
      clock.addAndGet(1_000L);
      monitor.tick();

      assertThat(monitor.isFollowerStuckDivergedConfirmed()).isFalse();
    }

    // Regression for #8389: StuckFlagClearedOnEarlyReturn
    @Test
    void anUnhealthyDivisionDropsTheConfirmedFlag() {
      // Pins the CLOSED/EXCEPTION arm too: today the restart and escalation paths reach resetStreaksAfterRestart(), but
      // the flag must not depend on which sub-arm of handleUnhealthyState() a tick happens to take.
      for (final LifeCycle.State unhealthy : new LifeCycle.State[] { LifeCycle.State.CLOSED, LifeCycle.State.EXCEPTION }) {
        final FakeTarget target = new FakeTarget();
        final AtomicLong clock = new AtomicLong(0L);
        final HealthMonitor monitor = monitor(target, clock, true, 0L, 1);
        confirmStuck(monitor, target, clock);

        target.state.set(unhealthy);
        for (int i = 0; i < 5; i++) {
          clock.addAndGet(1_000L);
          monitor.tick();
          assertThat(monitor.isFollowerStuckDivergedConfirmed()).as("%s tick %d", unhealthy, i).isFalse();
        }
        assertThat(monitor.isCrashLoopEscalated()).as("%s: the crash loop reached its escalated arm", unhealthy).isTrue();
      }
    }

    // Regression for #8389: StuckFlagClearedOnEarlyReturn
    @Test
    void aStuckStreakDoesNotSurviveAnEarlyReturnAndFireAReformatOnTheFirstTickBack() {
      final FakeTarget target = new FakeTarget();
      final AtomicLong clock = new AtomicLong(0L);
      final HealthMonitor monitor = monitor(target, clock, true, 0L, 10);
      confirmStuck(monitor, target, clock); // streak started at t=0

      // A log-writer incident well past the recovery duration, during which no tick evaluates the stuck signature.
      target.logFailure = "at index 4946: java.io.IOException: No space left on device";
      target.storageWritable = false;
      for (int i = 0; i < 10; i++) {
        clock.addAndGet(1_000L);
        monitor.tick();
      }

      // The writer is healthy again and the follower still looks stuck. This is ONE observation: it may start a streak,
      // not act on one that began before the checks went blind.
      target.logFailure = null;
      clock.addAndGet(1_000L);
      monitor.tick();

      assertThat(target.divergenceRecover.get()).as("no reformat on the first tick back").isZero();
      assertThat(monitor.isFollowerStuckDivergedConfirmed()).as("a single observation is never confirmed").isFalse();

      // The streak re-arms normally: a second observation confirms it, and the reformat waits out a full duration.
      clock.addAndGet(1_000L);
      monitor.tick();
      assertThat(monitor.isFollowerStuckDivergedConfirmed()).isTrue();
      assertThat(target.divergenceRecover.get()).isZero();

      clock.addAndGet(DURATION_MS);
      monitor.tick();
      assertThat(target.divergenceRecover.get()).isEqualTo(1);
    }

    // Regression for #8389: StuckFlagClearedOnEarlyReturn
    @Test
    void aLagStreakDoesNotSurviveAnEarlyReturnAndFireASnapshotReArmOnTheFirstTickBack() {
      final FakeTarget target = new FakeTarget();
      final AtomicLong clock = new AtomicLong(0L);
      final HealthMonitor monitor = monitor(target, clock, false, 100L, 10);

      target.lagging = true;
      monitor.tick(); // lag streak starts at t=0
      assertThat(target.persistentLagRecover.get()).isZero();

      target.logFailure = "at index 4946: java.io.IOException: No space left on device";
      target.storageWritable = false;
      for (int i = 0; i < 10; i++) {
        clock.addAndGet(1_000L);
        monitor.tick();
      }

      target.logFailure = null;
      clock.addAndGet(1_000L);
      monitor.tick();
      assertThat(target.persistentLagRecover.get()).as("no snapshot re-arm on the first tick back").isZero();

      clock.addAndGet(DURATION_MS);
      monitor.tick();
      assertThat(target.persistentLagRecover.get()).as("a fresh streak still recovers once it has persisted").isEqualTo(1);
    }
  }

  /**
   * Issue #8341: a follower that is catching up with ZERO progress was classified as healthy catch-up on both
   * sides of the cluster, so nothing reported it while it silently cost the cluster its fault tolerance.
   * <ul>
   *   <li>Leader side ({@link ClusterMonitor}): a replica over the lag threshold whose {@code matchIndex} does not
   *   move was {@code STALLED} only on a tick where the leader's commit index advanced. When the stuck replica is
   *   the vote the leader is missing, the leader cannot commit at all, so the replica read {@code CATCHING_UP}
   *   ("advancing at 0 entries/tick") forever and the {@code lagging-followers} alert never fired.</li>
   *   <li>Follower side ({@link RaftHAServer}): the {@code catchingUp} flag of the state machine is sticky - it is
   *   cleared only by an apply that reaches the commit index - so a catch-up that stops applying kept exempting
   *   the node from both the stale-term and the persistent-lag checks.</li>
   * </ul>
   */
  @Nested
  class ZeroProgressCatchUp {

    private static final String REPLICA = "proxy_8661";

    // -- Leader side: ClusterMonitor classification --

    /**
     * The step-16 shape from the issue: a new leader (fresh baseline after {@link ClusterMonitor#reset()}) sees a
     * replica 26792 entries behind that does not move, and cannot advance its own commit index because that replica
     * is the missing quorum vote. It must be reported STALLED once the zero-progress spell outlasts the grace.
     */
    // Regression for #8341: ZeroProgressCatchUp
    @Test
    void zeroProgressReplicaIsStalledEvenWhenTheLeaderCannotCommit() {
      final AtomicLong now = new AtomicLong(0);
      final ClusterMonitor monitor = new ClusterMonitor(1000L);
      monitor.setClock(now::get);
      monitor.reset();

      monitor.updateLeaderCommitIndex(362_000);
      monitor.updateReplicaMatchIndex(REPLICA, 335_208, 0L);
      assertThat(monitor.getReplicaStatus(REPLICA)).as("first tick of a fresh baseline cannot judge progress")
          .isNotEqualTo(ClusterMonitor.ReplicaStatus.STALLED);

      now.set(5_000);
      monitor.updateLeaderCommitIndex(362_000); // no quorum: the leader is stuck too
      monitor.updateReplicaMatchIndex(REPLICA, 335_208, 0L);
      assertThat(monitor.getReplicaStatus(REPLICA)).as("still inside the zero-progress grace")
          .isNotEqualTo(ClusterMonitor.ReplicaStatus.STALLED);

      now.set(ClusterMonitor.ZERO_PROGRESS_STALL_GRACE_MS);
      monitor.updateLeaderCommitIndex(362_000);
      monitor.updateReplicaMatchIndex(REPLICA, 335_208, 0L);
      assertThat(monitor.getReplicaStatus(REPLICA)).isEqualTo(ClusterMonitor.ReplicaStatus.STALLED);
    }

    /** A replica that is behind but advances every tick is catching up, however quiet the leader is. */
    // Regression for #8341: ZeroProgressCatchUp
    @Test
    void progressingReplicaWithAQuietLeaderIsCatchingUp() {
      final AtomicLong now = new AtomicLong(0);
      final ClusterMonitor monitor = new ClusterMonitor(1000L);
      monitor.setClock(now::get);

      long match = 100_000;
      monitor.updateLeaderCommitIndex(200_000);
      monitor.updateReplicaMatchIndex(REPLICA, match, 0L);
      for (int tick = 1; tick <= 10; tick++) {
        now.set(tick * 5_000L);
        match += 500;
        monitor.updateLeaderCommitIndex(200_000);
        monitor.updateReplicaMatchIndex(REPLICA, match, 0L);
        assertThat(monitor.getReplicaStatus(REPLICA)).as("tick %d", tick).isEqualTo(ClusterMonitor.ReplicaStatus.CATCHING_UP);
      }
    }

    /** Any movement of matchIndex ends the zero-progress spell, so two short pauses never add up to a stall. */
    // Regression for #8341: ZeroProgressCatchUp
    @Test
    void progressRestartsTheZeroProgressSpell() {
      final AtomicLong now = new AtomicLong(0);
      final ClusterMonitor monitor = new ClusterMonitor(1000L);
      monitor.setClock(now::get);

      monitor.updateLeaderCommitIndex(50_000);
      monitor.updateReplicaMatchIndex(REPLICA, 10_000, 0L); // t=0, spell starts
      now.set(ClusterMonitor.ZERO_PROGRESS_STALL_GRACE_MS - 1);
      monitor.updateReplicaMatchIndex(REPLICA, 10_000, 0L);
      now.set(ClusterMonitor.ZERO_PROGRESS_STALL_GRACE_MS);
      monitor.updateReplicaMatchIndex(REPLICA, 10_001, 0L); // moved: spell over
      assertThat(monitor.getReplicaStatus(REPLICA)).isEqualTo(ClusterMonitor.ReplicaStatus.CATCHING_UP);

      now.set(2 * ClusterMonitor.ZERO_PROGRESS_STALL_GRACE_MS - 1);
      monitor.updateReplicaMatchIndex(REPLICA, 10_001, 0L);
      assertThat(monitor.getReplicaStatus(REPLICA)).as("a new spell has not reached the grace yet")
          .isNotEqualTo(ClusterMonitor.ReplicaStatus.STALLED);

      now.set(3 * ClusterMonitor.ZERO_PROGRESS_STALL_GRACE_MS);
      monitor.updateReplicaMatchIndex(REPLICA, 10_001, 0L);
      assertThat(monitor.getReplicaStatus(REPLICA)).isEqualTo(ClusterMonitor.ReplicaStatus.STALLED);
    }

    /**
     * A replica still at the never-appended sentinel keeps the dedicated #5295 rule and its longer grace: the
     * zero-progress path must not flag it after only {@link ClusterMonitor#ZERO_PROGRESS_STALL_GRACE_MS}.
     */
    // Regression for #8341: ZeroProgressCatchUp
    @Test
    void neverAppendedReplicaKeepsItsOwnGrace() {
      final AtomicLong now = new AtomicLong(0);
      final ClusterMonitor monitor = new ClusterMonitor(1000L);
      monitor.setClock(now::get);
      for (long t = 0; t < ClusterMonitor.NEVER_APPENDED_STALL_GRACE_MS; t += 5_000) {
        now.set(t);
        monitor.updateLeaderCommitIndex(50_000);
        monitor.updateReplicaMatchIndex(REPLICA, -1, 0L);
        assertThat(monitor.getReplicaStatus(REPLICA)).as("t=%d", t).isNotEqualTo(ClusterMonitor.ReplicaStatus.STALLED);
      }
      now.set(ClusterMonitor.NEVER_APPENDED_STALL_GRACE_MS);
      monitor.updateLeaderCommitIndex(50_000);
      monitor.updateReplicaMatchIndex(REPLICA, -1, 0L);
      assertThat(monitor.getReplicaStatus(REPLICA)).isEqualTo(ClusterMonitor.ReplicaStatus.STALLED);
    }

    /** An idle cluster where the replica is within the lag threshold stays HEALTHY: no progress is needed there. */
    // Regression for #8341: ZeroProgressCatchUp
    @Test
    void idleCaughtUpReplicaStaysHealthy() {
      final AtomicLong now = new AtomicLong(0);
      final ClusterMonitor monitor = new ClusterMonitor(1000L);
      monitor.setClock(now::get);
      for (int tick = 0; tick <= 10; tick++) {
        now.set(tick * 5_000L);
        monitor.updateLeaderCommitIndex(5_000);
        monitor.updateReplicaMatchIndex(REPLICA, 4_990, 0L);
        assertThat(monitor.getReplicaStatus(REPLICA)).isEqualTo(ClusterMonitor.ReplicaStatus.HEALTHY);
      }
    }

    /**
     * What an operator reads: the status the monitor reports feeds the {@code lagging-followers} alert of
     * {@code GET /api/v1/cluster}. Before the fix the stuck replica read CATCHING_UP, which that alert ignores.
     */
    // Regression for #8341: ZeroProgressCatchUp
    @Test
    void zeroProgressReplicaRaisesTheCriticalLaggingFollowersAlert() {
      final AtomicLong now = new AtomicLong(0);
      final ClusterMonitor monitor = new ClusterMonitor(1000L);
      monitor.setClock(now::get);
      for (long t = 0; t <= ClusterMonitor.ZERO_PROGRESS_STALL_GRACE_MS; t += 5_000) {
        now.set(t);
        monitor.updateLeaderCommitIndex(362_000);
        monitor.updateReplicaMatchIndex(REPLICA, 335_208, 0L);
      }

      final JSONArray alerts = new JSONArray();
      ClusterAlerts.addLaggingFollowerAlert(List.of(new FollowerSample(REPLICA, 335_208, 335_209,
          monitor.getReplicaLags().get(REPLICA), 50, monitor.getReplicaStatus(REPLICA).name(),
          monitor.getReplicaLaggingForMs(REPLICA))), alerts);

      assertThat(alerts.length()).isEqualTo(1);
      final JSONObject alert = alerts.getJSONObject(0);
      assertThat(alert.getString("id")).isEqualTo("lagging-followers");
      assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
      assertThat(alert.getJSONObject("details").getJSONArray("nodes").getJSONObject(0).getString("peerId")).isEqualTo(REPLICA);
    }

    /**
     * The recovery the issue asks for already exists leader-side and must keep engaging in this shape: the
     * leader-driven resync (#4728) fires for a zero-progress replica on a leader that cannot commit.
     */
    // Regression for #8341: ZeroProgressCatchUp
    @Test
    void zeroProgressReplicaTriggersTheLeaderDrivenResync() {
      final List<String> resynced = new ArrayList<>();
      final AtomicLong now = new AtomicLong(0);
      final ClusterMonitor monitor = new ClusterMonitor(1000L, 30_000L, resynced::add);
      monitor.setClock(now::get);
      for (long t = 0; t <= 30_000; t += 5_000) {
        now.set(t);
        monitor.updateLeaderCommitIndex(362_000);
        monitor.updateReplicaMatchIndex(REPLICA, 335_208, 0L);
      }
      assertThat(resynced).containsExactly(REPLICA);
    }

    // -- Follower side: the sticky catchingUp flag --

    // Regression for #8341: ZeroProgressCatchUp
    @Test
    void catchUpFlagDoesNotExemptAFollowerThatHasAppliedEverythingItCommitted() {
      assertThat(RaftHAServer.isActivelyCatchingUp(true, 10, 10)).isFalse();
      assertThat(RaftHAServer.isActivelyCatchingUp(true, 8, 10)).isTrue();
      assertThat(RaftHAServer.isActivelyCatchingUp(false, 8, 10)).isFalse();
      // Unreadable state: keep the flag's answer rather than guess.
      assertThat(RaftHAServer.isActivelyCatchingUp(true, -1, 10)).isTrue();
      assertThat(RaftHAServer.isActivelyCatchingUp(true, 10, -1)).isTrue();
    }

    // Regression for #8341: ZeroProgressCatchUp
    @Test
    void catchUpFlagExemptsTheLagCheckOnlyWhileTheAppliedIndexMoves() {
      assertThat(RaftHAServer.catchUpExemptsLagCheck(true, 100, -1)).as("no prior sample").isTrue();
      assertThat(RaftHAServer.catchUpExemptsLagCheck(true, 150, 100)).as("advancing").isTrue();
      assertThat(RaftHAServer.catchUpExemptsLagCheck(true, 100, 100)).as("zero progress").isFalse();
      assertThat(RaftHAServer.catchUpExemptsLagCheck(false, 150, 100)).as("not catching up").isFalse();
    }

    /**
     * The caller, not only the helper: a follower whose state machine still carries a stale {@code catchingUp}
     * flag, with {@code commitIndex == appliedIndex} at a stale term, is reported stuck.
     */
    // Regression for #8341: ZeroProgressCatchUp
    @Test
    void staleCatchUpFlagNoLongerHidesTheStuckAtStaleTermSignature() throws Exception {
      // catchingUp: set by an earlier gap, never cleared
      final ArcadeStateMachine sm = new FakeArcadeStateMachine().catchingUp(true).lastAppliedTermIndex(TermIndex.valueOf(8, 228_631));

      final RaftHAServer server = followerWith(sm, 9, 228_631, 228_631);

      assertThat(server.isFollowerStuckDiverged()).isTrue();
    }

    /** The caller, not only the predicate (issue #11493): the leader's reported commit index equals the applied index. */
    // Regression for #8341: ZeroProgressCatchUp
    @Test
    void followerCaughtUpToTheLeaderReportedCommitIsNotStuckDiverged() throws Exception {
      final ArcadeStateMachine sm = new FakeArcadeStateMachine().lastAppliedTermIndex(TermIndex.valueOf(35, 216_693));

      final RaftHAServer server = followerWith(sm, 36, 216_693, 216_693);
      setField(server, "leaderReportedCommitIndex", 216_693L);

      assertThat(server.isFollowerStuckDiverged()).isFalse();
    }

    /**
     * The caller of the lag check: a follower whose catch-up stopped applying (commit far ahead, applied frozen) is
     * reported lagging on the second tick instead of being exempted forever by the sticky flag.
     */
    // Regression for #8341: ZeroProgressCatchUp
    @Test
    void stalledCatchUpIsReportedLaggingOnTheSecondTick() throws Exception {
      final ArcadeStateMachine sm = new FakeArcadeStateMachine().catchingUp(true);

      final FakeRaftHAServer server = followerWith(sm, 9, 250_000, 228_631);
      setField(server, "lastLagCheckAppliedIndex", -1L);

      assertThat(server.isFollowerLaggingBeyond(1000L)).as("first tick only records the baseline").isFalse();
      assertThat(server.isFollowerLaggingBeyond(1000L)).as("zero progress while catching up").isTrue();
    }

    // Regression for #8341: ZeroProgressCatchUp
    @Test
    void progressingCatchUpIsStillExemptFromTheLagCheck() throws Exception {
      final ArcadeStateMachine sm = new FakeArcadeStateMachine().catchingUp(true);

      final FakeRaftHAServer server = followerWith(sm, 9, 250_000, 228_631);
      setField(server, "lastLagCheckAppliedIndex", -1L);

      assertThat(server.isFollowerLaggingBeyond(1000L)).isFalse();
      server.lastAppliedIndex(230_000L);
      assertThat(server.isFollowerLaggingBeyond(1000L)).isFalse();
      server.lastAppliedIndex(231_000L);
      assertThat(server.isFollowerLaggingBeyond(1000L)).isFalse();
    }

    /**
     * A follower whose Raft view is set by the test; the checks under test - isFollowerStuckDiverged,
     * isFollowerLaggingBeyond, getFollowerCommitIndex - are the real server's own.
     */
    private static FakeRaftHAServer followerWith(final ArcadeStateMachine sm, final long currentTerm, final long commitIndex,
        final long appliedIndex) throws Exception {
      final FakeRaftHAServer server = FakeRaftHAServer.detached().leader(false).leaderId(RaftPeerId.valueOf("leader"))
          .currentTerm(currentTerm).commitIndex(commitIndex).lastAppliedIndex(appliedIndex).stateMachine(sm);
      // The checks read the Ratis server's presence; no division is ever asked for here
      setField(server, "raftServer", mock(RaftServer.class));
      setField(server, "shutdownRequested", false);
      // The lag check reads the commit index through getFollowerCommitIndex (issue #8321); no leader figure is learned.
      setField(server, "leaderReportedCommitIndex", -1L);
      return server;
    }

    private static void setField(final Object target, final String name, final Object value) throws Exception {
      final Field f = RaftHAServer.class.getDeclaredField(name);
      f.setAccessible(true);
      f.set(target, value);
    }
  }

  /**
   * Regression tests for issue #7736. Issue #7622 made the liveness probe fail once {@link HealthMonitor} escalated a
   * crash loop, but every escalation field was in-memory and the monitor is built fresh per process. So each pod
   * restart reset the ladder and walked it again on the same, still poisoned, storage: more non-sticking restarts, a
   * Raft-storage reformat, a full snapshot download from the leader, a new escalation, liveness red again - a
   * perpetual {@code CrashLoopBackOff} for a cause served by the leader that no restart of this node can cure.
   * <p>
   * The escalation is now recorded next to the Raft storage. The process that escalated asks for ONE restart
   * ({@link HealthMonitor#isCrashLoopRestartPending()}); the restarted process inherits the record, skips the ladder
   * - no restart, no reformat - and does not ask for another one.
   */
  @Nested
  class CrashLoopEscalationSurvivesRestart {

    private static final int THRESHOLD = 3;

    /** A lifecycle target whose escalation record lives in {@link #record}, standing in for the Raft storage. */
    static final class RecordingTarget implements HealthMonitor.HealthTarget {
      final    AtomicReference<LifeCycle.State> state             = new AtomicReference<>(LifeCycle.State.CLOSED);
      final    AtomicReference<String>          record            = new AtomicReference<>();
      final    AtomicInteger                    restarts          = new AtomicInteger();
      final    AtomicInteger                    reformats         = new AtomicInteger();
      volatile boolean                          recordWritable    = true;

      @Override
      public LifeCycle.State getRaftLifeCycleState() {
        return state.get();
      }

      @Override
      public boolean isShutdownRequested() {
        return false;
      }

      @Override
      public void restartRatisIfNeeded() {
        restarts.incrementAndGet();
      }

      @Override
      public void recoverFromDivergence() {
        reformats.incrementAndGet();
      }

      @Override
      public boolean hasPersistedCrashLoopEscalation() {
        return record.get() != null;
      }

      @Override
      public boolean persistCrashLoopEscalation(final String reason) {
        if (!recordWritable)
          return false;
        record.set(reason);
        return true;
      }

      @Override
      public void clearPersistedCrashLoopEscalation() {
        record.set(null);
      }
    }

    private static HealthMonitor newLifetime(final HealthMonitor.HealthTarget target) {
      return new HealthMonitor(target, 1000, 0L, 5000L, true, 2, THRESHOLD);
    }

    /** Walks a fresh monitor through the whole #5291 ladder: restarts, the one reformat, restarts, give-up. */
    private static void escalate(final HealthMonitor monitor) {
      for (int i = 0; i < 2 * (THRESHOLD + 1); i++)
        monitor.tick();
      assertThat(monitor.isCrashLoopEscalated()).as("the ladder ran to the give-up").isTrue();
    }

    // Regression for #7736: CrashLoopEscalationSurvivesRestart
    @Test
    void theEscalatingLifetimeRecordsItAndAsksForExactlyOneRestart() {
      final RecordingTarget target = new RecordingTarget();
      final HealthMonitor first = newLifetime(target);

      escalate(first);

      assertThat(target.restarts.get()).isEqualTo(2 * THRESHOLD);
      assertThat(target.reformats.get()).isEqualTo(1);
      assertThat(target.record.get()).as("the escalation is recorded").contains("Ratis crash-loop persists");
      assertThat(first.isCrashLoopRestartPending()).as("this lifetime asks for the one restart").isTrue();
    }

    // Regression for #7736: CrashLoopEscalationSurvivesRestart
    @Test
    void theRestartedLifetimeInheritsTheEscalationAndDoesNotWalkTheLadderAgain() {
      final RecordingTarget target = new RecordingTarget();
      escalate(newLifetime(target));
      final int restartsBefore = target.restarts.get();

      // The process restart: a new monitor on the same storage, whose Raft layer comes back to the same crash loop.
      final HealthMonitor second = newLifetime(target);
      assertThat(second.isCrashLoopEscalated()).as("escalated from the first tick on, not after a new ladder").isTrue();
      assertThat(second.isCrashLoopRestartPending())
          .as("a restart was already tried on this storage: liveness must stay green, not loop").isFalse();

      for (int i = 0; i < 50; i++)
        second.tick();

      assertThat(target.restarts.get()).as("no Ratis restart in the inherited lifetime").isEqualTo(restartsBefore);
      assertThat(target.reformats.get()).as("no second storage reformat and snapshot download").isEqualTo(1);
      assertThat(second.isCrashLoopEscalated()).isTrue();
      assertThat(second.isCrashLoopRestartPending()).isFalse();
      assertThat(target.record.get()).as("the record stays for the next lifetime").isNotNull();
    }

    // Regression for #7736: CrashLoopEscalationSurvivesRestart
    @Test
    void aDivisionThatOnlyLooksUpForATickDoesNotRearmTheReformat() {
      // Restarted on poisoned storage, a division can be RUNNING for a tick or two before the state machine applies
      // the bad entry and it drops back to CLOSED. Clearing the record on that tick would hand the next escalation a
      // fresh reformat, a fresh snapshot download and a fresh request for a process restart - the loop again.
      final RecordingTarget target = new RecordingTarget();
      escalate(newLifetime(target));
      final int restartsBefore = target.restarts.get();

      final AtomicLong clock = new AtomicLong(0);
      target.state.set(LifeCycle.State.RUNNING);
      final HealthMonitor second = newLifetime(target);
      second.setClock(clock::get);
      second.tick();
      clock.addAndGet(3_000);
      second.tick();
      assertThat(second.isCrashLoopEscalated()).as("the division is up, so it is not escalated right now").isFalse();
      assertThat(target.record.get()).as("but one healthy tick does not end the incident").isNotNull();

      target.state.set(LifeCycle.State.CLOSED);
      for (int i = 0; i < 50; i++) {
        clock.addAndGet(3_000);
        second.tick();
      }

      assertThat(target.restarts.get() - restartsBefore).as("a bounded run of in-place restarts only").isEqualTo(THRESHOLD);
      assertThat(target.reformats.get()).as("no second reformat and snapshot download").isEqualTo(1);
      assertThat(second.isCrashLoopEscalated()).isTrue();
      assertThat(second.isCrashLoopRestartPending()).as("still the inherited incident: no second restart").isFalse();
    }

    // Regression for #7736: CrashLoopEscalationSurvivesRestart
    @Test
    void aDivisionThatStaysUpClearsTheRecordAndRearmsTheLadder() {
      final RecordingTarget target = new RecordingTarget();
      escalate(newLifetime(target));

      // The restart did cure it this time (or the operator fixed the leader): the division comes up and stays up.
      final AtomicLong clock = new AtomicLong(0);
      target.state.set(LifeCycle.State.RUNNING);
      final HealthMonitor second = newLifetime(target);
      second.setClock(clock::get);
      second.tick();
      clock.addAndGet(HealthMonitor.CRASH_LOOP_RECORD_RESET_MS - 1);
      second.tick();
      assertThat(target.record.get()).as("not yet: the window is not over").isNotNull();
      clock.addAndGet(1);
      second.tick();
      assertThat(target.record.get()).as("record cleared once the division stayed up for the whole window").isNull();

      // A genuinely new incident later gets the whole ladder again, reformat included, and its own one restart.
      target.state.set(LifeCycle.State.CLOSED);
      final int restartsBefore = target.restarts.get();
      escalate(second);
      assertThat(target.restarts.get() - restartsBefore).isEqualTo(2 * THRESHOLD);
      assertThat(target.reformats.get()).isEqualTo(2);
      assertThat(target.record.get()).isNotNull();
      assertThat(second.isCrashLoopRestartPending()).as("a new escalation in this lifetime asks for its restart").isTrue();
    }

    // Regression for #7736: CrashLoopEscalationSurvivesRestart
    @Test
    void anEscalationThatCouldNotBeRecordedDoesNotAskForARestart() {
      // A restart after an unrecorded escalation would walk the whole ladder again - reformat and snapshot download
      // included - which is the loop this issue is about. The node parks instead, as it did before #7622.
      final RecordingTarget target = new RecordingTarget();
      target.recordWritable = false;
      final HealthMonitor monitor = newLifetime(target);

      escalate(monitor);

      assertThat(monitor.isCrashLoopRestartPending()).isFalse();
    }

    // Regression for #7736: CrashLoopEscalationSurvivesRestart
    @Test
    void aRecordIsDiscardedWhenTheEscalationIsDisabled() {
      // crashLoopRestartThreshold = 0 is the operator asking for the legacy restart-forever behaviour; a record left
      // by an earlier run with the escalation enabled must neither stop those restarts nor survive to disarm the
      // ladder the day the escalation is enabled again.
      final RecordingTarget target = new RecordingTarget();
      target.record.set("left by an earlier run");
      final HealthMonitor monitor = new HealthMonitor(target, 1000, 0L, 5000L, true, 2, 0);

      assertThat(monitor.isCrashLoopEscalated()).isFalse();
      assertThat(target.record.get()).isNull();
      for (int i = 0; i < 5; i++)
        monitor.tick();
      assertThat(target.restarts.get()).isEqualTo(5);
    }

    // --- The record on disk: RaftHAServer is the real HealthTarget ---

    private static RaftHAServer detachedServer(final File raftStorageRoot) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");
      config.setValue(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY, raftStorageRoot.getAbsolutePath());
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.SERVER_NAME, "localhost");
      final ArcadeDBServer server = TestServerHelper.unstartedServer(raftStorageRoot.toPath(), configuration);
      return new RaftHAServer(server, config);
    }

    // Regression for #7736: CrashLoopEscalationSurvivesRestart
    @Test
    void raftHAServerRecordsTheEscalationNextToItsRaftStorageDirectory(@TempDir final Path root) throws Exception {
      final RaftHAServer first = detachedServer(root.toFile());
      assertThat(first.hasPersistedCrashLoopEscalation()).isFalse();

      assertThat(first.persistCrashLoopEscalation("Ratis crash-loop persists after 11 restarts")).isTrue();

      // A sibling of the peer's Raft storage directory, never inside it (issue #8380).
      final File[] records = root.toFile().listFiles(f -> f.isFile() && f.getName().endsWith(
          "." + RaftStorageFiles.CRASH_LOOP_ESCALATION_MARKER));
      assertThat(records).as("the record next to the peer's own Raft storage directory").hasSize(1);
      final File marker = records[0];
      assertThat(marker).isFile();
      assertThat(Files.readString(marker.toPath())).contains("Ratis crash-loop persists after 11 restarts");

      // A new process on the same storage sees it; that is the whole point of writing it to disk.
      final RaftHAServer second = detachedServer(root.toFile());
      assertThat(second.hasPersistedCrashLoopEscalation()).isTrue();

      second.clearPersistedCrashLoopEscalation();
      assertThat(marker).doesNotExist();
      assertThat(detachedServer(root.toFile()).hasPersistedCrashLoopEscalation()).isFalse();
      second.clearPersistedCrashLoopEscalation(); // idempotent
    }

    // Regression for #7736: CrashLoopEscalationSurvivesRestart
    @Test
    void aProcessRestartOnTheSameStorageDoesNotReformatAgain(@TempDir final Path root) {
      // End to end through the real record: the first lifetime escalates against a RaftHAServer-backed store, the
      // second is a new RaftHAServer and a new monitor on the same directory, as after a pod restart.
      final RecordingTarget lifecycle = new RecordingTarget();
      final HealthMonitor.HealthTarget firstProcess = onDisk(lifecycle, detachedServer(root.toFile()));
      final HealthMonitor first = newLifetime(firstProcess);
      escalate(first);
      assertThat(first.isCrashLoopRestartPending()).isTrue();
      assertThat(lifecycle.reformats.get()).isEqualTo(1);

      final HealthMonitor second = newLifetime(onDisk(lifecycle, detachedServer(root.toFile())));
      for (int i = 0; i < 50; i++)
        second.tick();

      assertThat(lifecycle.reformats.get()).as("the reformat ran once per incident, not once per process").isEqualTo(1);
      assertThat(second.isCrashLoopEscalated()).isTrue();
      assertThat(second.isCrashLoopRestartPending()).isFalse();
    }

    /** The lifecycle of {@code lifecycle}, with the escalation record kept on disk by a real {@link RaftHAServer}. */
    private static HealthMonitor.HealthTarget onDisk(final RecordingTarget lifecycle, final RaftHAServer store) {
      return new HealthMonitor.HealthTarget() {
        @Override
        public LifeCycle.State getRaftLifeCycleState() {
          return lifecycle.getRaftLifeCycleState();
        }

        @Override
        public boolean isShutdownRequested() {
          return false;
        }

        @Override
        public void restartRatisIfNeeded() {
          lifecycle.restartRatisIfNeeded();
        }

        @Override
        public void recoverFromDivergence() {
          lifecycle.recoverFromDivergence();
        }

        @Override
        public boolean hasPersistedCrashLoopEscalation() {
          return store.hasPersistedCrashLoopEscalation();
        }

        @Override
        public boolean persistCrashLoopEscalation(final String reason) {
          return store.persistCrashLoopEscalation(reason);
        }

        @Override
        public void clearPersistedCrashLoopEscalation() {
          store.clearPersistedCrashLoopEscalation();
        }
      };
    }
  }
}
