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
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8342: a follower whose log stops receiving entries while the term does not change had
 * no follower-local signal. Its commit index equals its applied index (Ratis clamps a follower's commit index to the
 * entries it holds), its applied term is the current term, and it did not know the leader's commit index, so its own
 * {@code GET /api/v1/cluster} read healthy and only the leader's answer carried the stall (#8341).
 * <p>
 * The fix measures the follower against the commit index its leader reports over the health monitor's
 * follower-to-leader probe (the #7619 channel, now always on for a follower), with the rule the leader's
 * {@link ClusterMonitor} applies to the same replica: over {@code arcadedb.ha.replicationLagWarning} behind and no
 * progress for {@link ClusterMonitor#ZERO_PROGRESS_STALL_GRACE_MS}.
 */
class Issue8342FollowerLocalStallSignalTest {

  private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
  private static final long   GRACE       = ClusterMonitor.ZERO_PROGRESS_STALL_GRACE_MS;
  private static final long   THRESHOLD   = 1_000L;

  // ---- the rule, on the tracker alone ----------------------------------------------------------------------------

  @Test
  void aFollowerFarBehindTheLeaderWithNoProgressIsStalledOnlyOnceTheGraceHasElapsed() {
    final FollowerStallTracker tracker = new FollowerStallTracker();

    tracker.observe(0L, true, 5_000L, 100L, 100L, THRESHOLD);
    assertThat(tracker.current()).as("first sample: the spell starts, nothing judged yet").isNull();
    tracker.observe(GRACE - 1, true, 5_000L, 100L, 100L, THRESHOLD);
    assertThat(tracker.current()).as("one millisecond short of the grace").isNull();
    tracker.observe(GRACE, true, 5_000L, 100L, 100L, THRESHOLD);

    final FollowerStallTracker.Stall stall = tracker.current();
    assertThat(stall).isNotNull();
    assertThat(stall.leaderCommitIndex()).isEqualTo(5_000L);
    assertThat(stall.appliedIndex()).isEqualTo(100L);
    assertThat(stall.lag()).isEqualTo(4_900L);
    assertThat(stall.stalledForMs()).isEqualTo(GRACE);
  }

  @Test
  void appliedProgressRestartsTheSpell() {
    final FollowerStallTracker tracker = stalledTracker();

    tracker.observe(2 * GRACE, true, 5_000L, 101L, 101L, THRESHOLD);
    assertThat(tracker.current()).as("one entry applied: catching up, not stalled").isNull();
    // Like the leader's rule, the fresh spell starts at the first tick with no progress after the one that moved.
    tracker.observe(2 * GRACE + 3_000L, true, 5_000L, 101L, 101L, THRESHOLD);
    tracker.observe(3 * GRACE + 3_000L - 1, true, 5_000L, 101L, 101L, THRESHOLD);
    assertThat(tracker.current()).as("a fresh spell needs its own grace").isNull();
    tracker.observe(3 * GRACE + 3_000L, true, 5_000L, 101L, 101L, THRESHOLD);
    assertThat(tracker.current()).isNotNull();
  }

  @Test
  void appendsThatStillArriveAreProgressEvenWhenNothingIsApplied() {
    // The state machine is busy with one large entry while the leader keeps appending: the leader's matchIndex
    // moves, so the leader calls this replica healthy, and so must the follower.
    final FollowerStallTracker tracker = new FollowerStallTracker();
    long log = 100L;
    for (long now = 0L; now <= 3 * GRACE; now += 3_000L) {
      tracker.observe(now, true, 5_000L, 100L, log, THRESHOLD);
      log += 50L;
      assertThat(tracker.current()).as("log index %d at %dms", log, now).isNull();
    }
  }

  @Test
  void aLagWithinTheThresholdIsNeverAStall() {
    final FollowerStallTracker tracker = new FollowerStallTracker();
    for (long now = 0L; now <= 3 * GRACE; now += 3_000L)
      tracker.observe(now, true, 100L + THRESHOLD, 100L, 100L, THRESHOLD);
    assertThat(tracker.current()).as("exactly the threshold behind, like the leader's HEALTHY").isNull();
  }

  @Test
  void anUnknownLeaderCommitOrAnIneligibleTickClearsTheStall() {
    final FollowerStallTracker unknown = stalledTracker();
    unknown.observe(2 * GRACE, true, -1L, 100L, 100L, THRESHOLD);
    assertThat(unknown.current()).isNull();

    final FollowerStallTracker ineligible = stalledTracker();
    ineligible.observe(2 * GRACE, false, 5_000L, 100L, 100L, THRESHOLD);
    assertThat(ineligible.current()).isNull();
    ineligible.observe(2 * GRACE + GRACE - 1, true, 5_000L, 100L, 100L, THRESHOLD);
    assertThat(ineligible.current()).as("an ineligible tick starts the spell over").isNull();
  }

  // ---- end to end through RaftHAServer ---------------------------------------------------------------------------

  @Test
  void theStuckFollowerFromTheIssueReportsItselfStalledThroughTheRealHooks() throws Exception {
    // The issue's shape: readiness does not consult HA (the default), commit == applied == 100 locally, applied
    // term current, leader at 5000.
    final Fixture f = new Fixture();

    assertThat(f.raft.isFollowerLaggingBeyond(THRESHOLD)).as("local lag 0: the persistent-lag check is blind")
        .isFalse();

    f.tick(0L);
    f.tick(3_000L);
    assertThat(f.raft.getLeaderReportedCommitIndex()).as("the probe runs with readinessRequiresHA off")
        .isEqualTo(5_000L);
    assertThat(f.raft.getFollowerStallBehindLeader()).as("not before the grace").isNull();

    f.tick(3_000L + GRACE);

    final FollowerStallTracker.Stall stall = f.raft.getFollowerStallBehindLeader();
    assertThat(stall).isNotNull();
    assertThat(stall.lag()).isEqualTo(4_900L);
  }

  @Test
  void aProgressingFollowerIsNotStalled() throws Exception {
    final Fixture f = new Fixture();
    long applied = 100L;
    for (long now = 0L; now <= 3 * GRACE; now += 3_000L) {
      when(f.info.getLastAppliedIndex()).thenReturn(applied);
      f.tick(now);
      applied += 10L;
    }
    assertThat(f.raft.getFollowerStallBehindLeader()).isNull();
  }

  @Test
  void aFollowerWhoseLogStillGrowsIsNotStalled() throws Exception {
    final Fixture f = new Fixture();
    long log = 100L;
    for (long now = 0L; now <= 3 * GRACE; now += 3_000L) {
      when(f.log.getLastEntryTermIndex()).thenReturn(TermIndex.valueOf(2L, log));
      f.tick(now);
      log += 10L;
    }
    assertThat(f.raft.getFollowerStallBehindLeader()).isNull();
  }

  @Test
  void aResyncInFlightIsLeftToItsOwnAlert() throws Exception {
    final Fixture f = f(stalled -> when(stalled.stateMachine.isResyncInProgress()).thenReturn(true));
    assertThat(f.raft.getFollowerStallBehindLeader()).isNull();
  }

  @Test
  void aNodeThatBecameLeaderDropsTheStall() throws Exception {
    final Fixture f = f(stalled -> when(stalled.info.isLeader()).thenReturn(true));
    assertThat(f.raft.getFollowerStallBehindLeader()).isNull();
  }

  @Test
  void aClosedDivisionDropsTheStall() throws Exception {
    final Fixture f = f(stalled -> when(stalled.info.getLifeCycleState()).thenReturn(LifeCycle.State.CLOSED));
    assertThat(f.raft.getFollowerStallBehindLeader()).isNull();
  }

  @Test
  void aLeaderChangeStartsTheSpellOver() throws Exception {
    final Fixture f = f(stalled -> when(stalled.info.getLeaderId()).thenReturn(RaftPeerId.valueOf("third")));
    assertThat(f.raft.getFollowerStallBehindLeader()).as("the new leader gets its own grace").isNull();

    f.tick(3 * GRACE);
    assertThat(f.raft.getFollowerStallBehindLeader()).as("still stuck under the new leader").isNotNull();
  }

  @Test
  void theHealthTickTracksTheStallEvenOnATickThatReturnsEarly() {
    final AtomicLong tracked = new AtomicLong();
    final HealthMonitor.HealthTarget target = new HealthMonitor.HealthTarget() {
      @Override
      public LifeCycle.State getRaftLifeCycleState() {
        return LifeCycle.State.CLOSED;
      }

      @Override
      public boolean isShutdownRequested() {
        return false;
      }

      @Override
      public void restartRatisIfNeeded() {
      }

      @Override
      public void trackFollowerStall() {
        tracked.incrementAndGet();
      }
    };
    final HealthMonitor monitor = new HealthMonitor(target, 0);

    monitor.tick();
    monitor.tick();

    assertThat(tracked.get()).isEqualTo(2);
  }

  // ---- the alert -------------------------------------------------------------------------------------------------

  @Test
  void theStallRaisesACriticalAlertWithTheFiguresBehindIt() {
    final JSONArray alerts = ClusterAlerts.scan(emptyServer(), null, List.of(), Set.of(), null, null, null,
        new ClusterAlerts.NodeStatus(null, null, false, true), false,
        new FollowerStallTracker.Stall(5_000L, 100L, 4_900L, 12_000L));

    final JSONObject alert = alertById(alerts, "follower-stalled-behind-leader");
    assertThat(alert).isNotNull();
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
    assertThat(alert.getString("message")).contains("4900").contains("5000").contains("12s");
    final JSONObject details = alert.getJSONObject("details");
    assertThat(details.getLong("leaderCommitIndex")).isEqualTo(5_000L);
    assertThat(details.getLong("appliedIndex")).isEqualTo(100L);
    assertThat(details.getLong("lag")).isEqualTo(4_900L);
    assertThat(details.getLong("stalledForMs")).isEqualTo(12_000L);
  }

  @Test
  void noStallNoAlert() {
    final JSONArray alerts = ClusterAlerts.scan(emptyServer(), null, List.of(), Set.of(), null, null, null,
        new ClusterAlerts.NodeStatus(null, null, false, true), false, null);
    assertThat(alertById(alerts, "follower-stalled-behind-leader")).isNull();
  }

  @Test
  void theStaleTermAlertAloneSpeaksForANodeStuckAtAStaleTerm() {
    final JSONArray alerts = ClusterAlerts.scan(emptyServer(), null, List.of(), Set.of(), null, null, null,
        new ClusterAlerts.NodeStatus(null, null, false, true), true,
        new FollowerStallTracker.Stall(5_000L, 100L, 4_900L, 12_000L));
    assertThat(alertById(alerts, "follower-stuck-at-stale-term")).isNotNull();
    assertThat(alertById(alerts, "follower-stalled-behind-leader")).isNull();
  }

  // ---- helpers ---------------------------------------------------------------------------------------------------

  private static ArcadeDBServer emptyServer() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getDatabaseNames()).thenReturn(Set.of());
    return server;
  }

  private static FollowerStallTracker stalledTracker() {
    final FollowerStallTracker tracker = new FollowerStallTracker();
    tracker.observe(0L, true, 5_000L, 100L, 100L, THRESHOLD);
    tracker.observe(GRACE, true, 5_000L, 100L, 100L, THRESHOLD);
    assertThat(tracker.current()).isNotNull();
    return tracker;
  }

  /** A fixture already reporting a stall, then changed by {@code change} and ticked once more. */
  private static Fixture f(final FixtureChange change) throws Exception {
    final Fixture f = new Fixture();
    f.tick(0L);
    f.tick(3_000L);
    f.tick(3_000L + GRACE);
    assertThat(f.raft.getFollowerStallBehindLeader()).as("precondition: stalled").isNotNull();
    change.apply(f);
    f.tick(2 * GRACE);
    return f;
  }

  @FunctionalInterface
  private interface FixtureChange {
    void apply(Fixture f);
  }

  private static JSONObject alertById(final JSONArray alerts, final String id) {
    for (int i = 0; i < alerts.length(); i++)
      if (id.equals(alerts.getJSONObject(i).getString("id")))
        return alerts.getJSONObject(i);
    return null;
  }

  private static RaftPeer peer(final String id, final String address) {
    return RaftPeer.newBuilder().setId(id).setAddress(address).build();
  }

  /**
   * A {@link RaftHAServer} that believes it is a running follower of a three-peer group, at commit == applied == 100
   * locally, whose leader answers the commit-index probe with 5000. Readiness does not consult HA, the default.
   */
  private static final class Fixture {
    final RaftHAServer        raft;
    final DivisionInfo        info;
    final RaftLog             log;
    final ArcadeStateMachine  stateMachine;
    final AtomicLong          now = new AtomicLong();

    Fixture() throws Exception {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
      config.setValue(GlobalConfiguration.HA_REPLICATION_LAG_WARNING, THRESHOLD);
      config.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, false);

      final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
      when(mockServer.getServerName()).thenReturn("ArcadeDB_0");
      raft = new RaftHAServer(mockServer, config);

      final RaftPeer self = peer(raft.getLocalPeerId().toString(), "localhost:2434");
      final RaftPeer leader = peer("leader", "localhost:2435");
      final RaftPeer third = peer("third", "localhost:2436");

      info = mock(DivisionInfo.class);
      when(info.getLeaderId()).thenReturn(leader.getId());
      when(info.isLeader()).thenReturn(false);
      when(info.getLastAppliedIndex()).thenReturn(100L);
      when(info.getLifeCycleState()).thenReturn(LifeCycle.State.RUNNING);

      final RaftConfiguration conf = mock(RaftConfiguration.class);
      when(conf.getCurrentPeers()).thenReturn(List.of(self, leader, third));

      log = mock(RaftLog.class);
      when(log.getLastCommittedIndex()).thenReturn(100L);
      when(log.getLastEntryTermIndex()).thenReturn(TermIndex.valueOf(2L, 100L));

      final RaftServer.Division division = mock(RaftServer.Division.class);
      when(division.getInfo()).thenReturn(info);
      when(division.getRaftConf()).thenReturn(conf);
      when(division.getRaftLog()).thenReturn(log);

      final RaftServer ratis = mock(RaftServer.class);
      when(ratis.getDivision(any())).thenReturn(division);

      final Field field = RaftHAServer.class.getDeclaredField("raftServer");
      field.setAccessible(true);
      field.set(raft, ratis);

      stateMachine = mock(ArcadeStateMachine.class);
      when(stateMachine.isResyncInProgress()).thenReturn(false);
      final Field smField = RaftHAServer.class.getDeclaredField("stateMachine");
      smField.setAccessible(true);
      smField.set(raft, stateMachine);

      raft.setLeaderCommitProber(target -> 5_000L);
      raft.setFollowerStallClock(now::get);
    }

    /** The two hooks a health tick runs, in its order: track (on the previous probe), then probe. */
    void tick(final long atMs) {
      now.set(atMs);
      raft.trackFollowerStall();
      raft.refreshLeaderCommitIndex();
    }
  }
}
