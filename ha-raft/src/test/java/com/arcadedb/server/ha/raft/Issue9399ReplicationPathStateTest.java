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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #9399, which consolidates #9013 and #8953: the state of a follower's replication path after an in-place Ratis
 * restart, as machine-readable fields of {@code GET /api/v1/cluster}.
 * <ul>
 *   <li>#9013: the {@code follower-stuck-at-stale-term} alert could not tell a node the health monitor holds back from
 *   reformatting (its replication path is unproven since an in-place restart, so it needs a restart by hand) from one
 *   that self-heals. The alert now carries {@code details.replicationPathUnproven} and a recommendation keyed on it,
 *   and the document carries {@code localReplicationPathUnproven}.</li>
 *   <li>#8953: a follower the leader cannot reach after an in-place restart reported {@code raftState=RUNNING} and
 *   nothing else. The health monitor now tracks it ({@link RaftHAServer#trackLeaderReachSinceRestart()}) and the
 *   document carries {@code localLeaderUnreachableSinceRestart}, with its own alert.</li>
 * </ul>
 */
class Issue9399ReplicationPathStateTest {

  private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
  private static final long   TERM        = 3L;
  private static final LeaderReachSinceRestartTracker.Unreachable KNOWN =
      new LeaderReachSinceRestartTracker.Unreachable(25_000L, true);

  // ---- #8953: the rule, on the predicate and the tracker alone ----------------------------------------------------

  @Test
  void theLeaderIsNotReachingOnlyWhileThePathIsUnprovenAndThereIsEvidence() {
    assertThat(LeaderReachSinceRestartTracker.leaderNotReaching(false, false, false, 5_000L, 100L))
        .as("a proven path, or no in-place restart at all, is never reported").isFalse();
    assertThat(LeaderReachSinceRestartTracker.leaderNotReaching(true, true, false, 5_000L, 100L))
        .as("a leader has nobody to be reached by").isFalse();
    assertThat(LeaderReachSinceRestartTracker.leaderNotReaching(true, false, false, -1L, 100L))
        .as("no leader has made itself known since the restart").isTrue();
    assertThat(LeaderReachSinceRestartTracker.leaderNotReaching(true, false, true, 5_000L, 100L))
        .as("the leader reports entries this node does not hold, and none arrived").isTrue();
    assertThat(LeaderReachSinceRestartTracker.leaderNotReaching(true, false, true, 100L, 100L))
        .as("an idle leader with nothing new to send gives no evidence either way").isFalse();
    assertThat(LeaderReachSinceRestartTracker.leaderNotReaching(true, false, true, -1L, 100L))
        .as("no leader commit index learned yet").isFalse();
    assertThat(LeaderReachSinceRestartTracker.leaderNotReaching(true, false, true, 5_000L, -1L))
        .as("an unreadable local position").isFalse();
  }

  @Test
  void theTrackerReportsOnlyOnceTheConditionOutlastsTheGrace() {
    final LeaderReachSinceRestartTracker tracker = new LeaderReachSinceRestartTracker();
    tracker.observe(1_000L, true, true, 10_000L);
    assertThat(ms(tracker)).isEqualTo(-1L);
    tracker.observe(10_999L, true, true, 10_000L);
    assertThat(ms(tracker)).as("not before the grace").isEqualTo(-1L);
    tracker.observe(11_000L, true, true, 10_000L);
    assertThat(ms(tracker)).isEqualTo(10_000L);

    tracker.observe(12_000L, false, true, 10_000L);
    assertThat(ms(tracker)).as("one tick without the condition clears it").isEqualTo(-1L);
    tracker.observe(13_000L, true, true, 10_000L);
    assertThat(ms(tracker)).as("and the next spell gets its own grace").isEqualTo(-1L);
  }

  // ---- #8953: end to end through RaftHAServer's health hook --------------------------------------------------------

  @Test
  void aFollowerTheLeaderDoesNotReachAfterAnInPlaceRestartIsReportedAfterTheGrace() throws Exception {
    final Fixture f = new Fixture();
    f.restartInPlace();

    f.tick(0L); // the first probe runs after the hook: no leader commit index known yet
    f.tick(3_000L); // the leader reports 5000, past the 100 this node holds: the spell starts
    f.tick(3_000L + f.grace - 1);
    assertThat(unreachableMs(f.raft)).as("not before the grace").isEqualTo(-1L);

    f.tick(3_000L + f.grace);
    assertThat(unreachableMs(f.raft)).isEqualTo(f.grace);
  }

  @Test
  void aFollowerWithNoLeaderKnownSinceAnInPlaceRestartIsReported() throws Exception {
    final Fixture f = new Fixture();
    when(f.info.getLeaderId()).thenReturn(null);
    f.restartInPlace();

    f.tick(0L);
    f.tick(f.grace);
    assertThat(unreachableMs(f.raft)).isEqualTo(f.grace);
    assertThat(f.raft.getLeaderUnreachableSinceRestart().leaderKnown())
        .as("reported as the weaker arm, which a cluster without a quorum shows too").isFalse();
  }

  @Test
  void aLeaderMakingItselfKnownWithNothingNewToSendClearsTheLeaderlessReport() throws Exception {
    // The quorum-loss shape: leaderless after the restart, then a leader appears whose commit index this node holds.
    final Fixture f = new Fixture();
    f.leaderCommit.set(100L);
    when(f.info.getLeaderId()).thenReturn(null);
    f.restartInPlace();
    f.tick(0L);
    f.tick(f.grace);
    assertThat(unreachableMs(f.raft)).as("precondition: reported").isEqualTo(f.grace);

    when(f.info.getLeaderId()).thenReturn(RaftPeerId.valueOf("leader"));
    f.tick(f.grace + 3_000L); // the leader is known; its commit index is learned after this hook
    f.tick(f.grace + 6_000L);

    assertThat(f.raft.isReplicationPathUnprovenSinceRestart()).as("no entry yet, same term").isTrue();
    assertThat(unreachableMs(f.raft)).as("but a known leader with nothing new to send is no evidence").isEqualTo(-1L);
  }

  @Test
  void anIdleClusterDoesNotReadAsAnUnreachableLeader() throws Exception {
    // The case that rules out reporting "unproven" on its own: a healthy leader with nothing to send.
    final Fixture f = new Fixture();
    f.leaderCommit.set(100L);
    f.restartInPlace();

    for (long now = 0L; now <= 3 * f.grace; now += 3_000L)
      f.tick(now);

    assertThat(f.raft.isReplicationPathUnprovenSinceRestart()).as("the path is still unproven").isTrue();
    assertThat(unreachableMs(f.raft)).as("but nothing says the leader is not reaching it")
        .isEqualTo(-1L);
  }

  @Test
  void anEntryArrivingClearsTheReport() throws Exception {
    final Fixture f = reported();
    when(f.log.getLastEntryTermIndex()).thenReturn(TermIndex.valueOf(TERM, 101L));
    f.tick(2 * f.grace);
    assertThat(f.raft.isReplicationPathUnprovenSinceRestart()).isFalse();
    assertThat(unreachableMs(f.raft)).isEqualTo(-1L);
  }

  @Test
  void aNodeNeverRestartedInPlaceIsNeverReported() throws Exception {
    final Fixture f = new Fixture();
    for (long now = 0L; now <= 3 * f.grace; now += 3_000L)
      f.tick(now);
    assertThat(unreachableMs(f.raft)).isEqualTo(-1L);
  }

  @Test
  void aResyncInFlightIsLeftToItsOwnAlert() throws Exception {
    final Fixture f = reported();
    when(f.stateMachine.isResyncInProgress()).thenReturn(true);
    f.tick(2 * f.grace);
    assertThat(unreachableMs(f.raft)).isEqualTo(-1L);
  }

  @Test
  void aClosedDivisionIsLeftToRaftState() throws Exception {
    final Fixture f = reported();
    when(f.info.getLifeCycleState()).thenReturn(LifeCycle.State.CLOSED);
    f.tick(2 * f.grace);
    assertThat(unreachableMs(f.raft)).isEqualTo(-1L);
  }

  @Test
  void theHealthTickTracksTheLeaderReachEvenOnATickThatReturnsEarly() {
    final AtomicInteger tracked = new AtomicInteger();
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
      public void trackLeaderReachSinceRestart() {
        tracked.incrementAndGet();
      }
    };
    final HealthMonitor monitor = new HealthMonitor(target, 0);

    monitor.tick();
    monitor.tick();

    assertThat(tracked.get()).isEqualTo(2);
  }

  // ---- the alerts --------------------------------------------------------------------------------------------------

  @Test
  void theStaleTermAlertSaysWhetherTheReformatIsHeld() {
    final JSONObject held = alertById(scan(true, null, true, null), "follower-stuck-at-stale-term");
    assertThat(held).isNotNull();
    assertThat(held.getJSONObject("details").getBoolean("stuckAtStaleTerm")).isTrue();
    assertThat(held.getJSONObject("details").getBoolean("replicationPathUnproven")).isTrue();
    assertThat(held.getString("recommendation")).startsWith("Restart this node by hand");

    final JSONObject selfHealing = alertById(scan(true, null, false, null), "follower-stuck-at-stale-term");
    assertThat(selfHealing).isNotNull();
    assertThat(selfHealing.getJSONObject("details").getBoolean("replicationPathUnproven")).isFalse();
    assertThat(selfHealing.getString("recommendation")).contains("this self-heals");
  }

  @Test
  void anUnreachableLeaderRaisesACriticalAlertWithItsDuration() {
    final JSONArray alerts = scan(false, null, true, KNOWN);
    final JSONObject alert = alertById(alerts, "follower-leader-unreachable-since-restart");
    assertThat(alert).isNotNull();
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
    assertThat(alert.getString("message")).contains("25s");
    assertThat(alert.getJSONObject("details").getBoolean("leaderUnreachableSinceRestart")).isTrue();
    assertThat(alert.getJSONObject("details").getLong("unreachableForMs")).isEqualTo(25_000L);
    assertThat(alert.getJSONObject("details").getBoolean("leaderKnown")).isTrue();
  }

  /**
   * No leader known is also what every node of a cluster without a quorum sees, so that arm is a warning that says so,
   * not a critical alert on every node of every quorum loss (review on PR #9451).
   */
  @Test
  void noLeaderKnownIsOnlyAWarningThatPointsAtTheQuorumFirst() {
    final JSONObject alert = alertById(scan(false, null, true, new LeaderReachSinceRestartTracker.Unreachable(25_000L, false)),
        "follower-leader-unreachable-since-restart");
    assertThat(alert).isNotNull();
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_WARNING);
    assertThat(alert.getString("message")).contains("no leader has made itself known");
    assertThat(alert.getString("recommendation")).startsWith("First check whether any node of the cluster has a leader");
    assertThat(alert.getJSONObject("details").getBoolean("leaderKnown")).isFalse();
  }

  @Test
  void oneAlertPerConditionMostSpecificCauseFirst() {
    final FollowerStallTracker.Stall stall = new FollowerStallTracker.Stall(5_000L, 100L, 4_900L, 12_000L);

    final JSONArray unreachable = scan(false, stall, true, KNOWN);
    assertThat(alertById(unreachable, "follower-leader-unreachable-since-restart")).isNotNull();
    assertThat(alertById(unreachable, "follower-stalled-behind-leader"))
        .as("the stall is the unreachable leader's symptom").isNull();

    final JSONArray stuck = scan(true, stall, true, KNOWN);
    assertThat(alertById(stuck, "follower-stuck-at-stale-term")).isNotNull();
    assertThat(alertById(stuck, "follower-leader-unreachable-since-restart")).isNull();
    assertThat(alertById(stuck, "follower-stalled-behind-leader")).isNull();

    final JSONArray stalledOnly = scan(false, stall, false, null);
    assertThat(alertById(stalledOnly, "follower-stalled-behind-leader")).isNotNull();
  }

  @Test
  void noUnreachableLeaderNoAlert() {
    assertThat(alertById(scan(false, null, true, null), "follower-leader-unreachable-since-restart")).isNull();
  }

  // ---- helpers -----------------------------------------------------------------------------------------------------

  private static JSONArray scan(final boolean stuck, final FollowerStallTracker.Stall stall, final boolean unproven,
      final LeaderReachSinceRestartTracker.Unreachable unreachable) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getDatabaseNames()).thenReturn(Set.of());
    return ClusterAlerts.scan(server, null, List.of(), Set.of(), null, null, null,
        new ClusterAlerts.NodeStatus(null, null, false, true), stuck, stall, unproven, unreachable);
  }

  private static long unreachableMs(final RaftHAServer raft) {
    final LeaderReachSinceRestartTracker.Unreachable unreachable = raft.getLeaderUnreachableSinceRestart();
    return unreachable != null ? unreachable.unreachableForMs() : -1L;
  }

  private static long ms(final LeaderReachSinceRestartTracker tracker) {
    final LeaderReachSinceRestartTracker.Unreachable unreachable = tracker.current();
    return unreachable != null ? unreachable.unreachableForMs() : -1L;
  }

  private static JSONObject alertById(final JSONArray alerts, final String id) {
    for (int i = 0; i < alerts.length(); i++)
      if (id.equals(alerts.getJSONObject(i).getString("id")))
        return alerts.getJSONObject(i);
    return null;
  }

  /** A fixture restarted in place whose leader has been reported as not reaching it. */
  private static Fixture reported() throws Exception {
    final Fixture f = new Fixture();
    f.restartInPlace();
    f.tick(0L);
    f.tick(3_000L);
    f.tick(3_000L + f.grace);
    assertThat(unreachableMs(f.raft)).as("precondition: reported").isGreaterThanOrEqualTo(0L);
    return f;
  }

  private static RaftPeer peer(final String id, final String address) {
    return RaftPeer.newBuilder().setId(id).setAddress(address).build();
  }

  /**
   * A {@link RaftHAServer} that believes it is a running follower of a three-peer group holding entries up to 100 at
   * term {@value #TERM}, whose leader answers the commit-index probe with {@link #leaderCommit} (5000 by default).
   */
  private static final class Fixture {
    final RaftHAServer       raft;
    final DivisionInfo       info;
    final RaftLog            log;
    final ArcadeStateMachine stateMachine;
    final AtomicLong         now          = new AtomicLong();
    final AtomicLong         leaderCommit = new AtomicLong(5_000L);
    final long               grace;

    Fixture() throws Exception {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
      config.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, false);
      grace = 2L * RaftPropertiesBuilder.electionTimeoutMaxFor(
          config.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN),
          config.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX));

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
      when(info.getCurrentTerm()).thenReturn(TERM);
      when(info.getLifeCycleState()).thenReturn(LifeCycle.State.RUNNING);

      final RaftConfiguration conf = mock(RaftConfiguration.class);
      when(conf.getCurrentPeers()).thenReturn(List.of(self, leader, third));

      log = mock(RaftLog.class);
      when(log.getLastCommittedIndex()).thenReturn(100L);
      when(log.getLastEntryTermIndex()).thenReturn(TermIndex.valueOf(TERM, 100L));

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

      raft.setLeaderCommitProber(target -> leaderCommit.get());
      raft.setFollowerStallClock(now::get);
    }

    /** What {@code restartRatis(false)} records: the division's position right after the new server started. */
    @SuppressWarnings("unchecked")
    void restartInPlace() throws Exception {
      final Field field = RaftHAServer.class.getDeclaredField("inPlaceRestartBaseline");
      field.setAccessible(true);
      ((AtomicReference<RaftHAServer.InPlaceRestartBaseline>) field.get(raft)).set(
          new RaftHAServer.InPlaceRestartBaseline(TermIndex.valueOf(TERM, 100L), TERM, info.getLeaderId() != null));
    }

    /** The hooks a health tick runs, in its order: track (on the previous probe), then probe. */
    void tick(final long atMs) {
      now.set(atMs);
      raft.trackFollowerStall();
      raft.trackLeaderReachSinceRestart();
      raft.refreshLeaderCommitIndex();
    }
  }
}
