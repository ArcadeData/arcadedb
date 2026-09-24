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
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.ProtoUtils;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.arcadedb.server.ha.raft.RaftHAServer.isBehindLeaderCommit;
import static com.arcadedb.server.ha.raft.RaftHAServer.isEmptyLogInMultiPeerCluster;
import static com.arcadedb.server.ha.raft.RaftHAServer.isReadyForTrafficState;
import static com.arcadedb.server.ha.raft.RaftHAServer.reportedCommitIndexOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #7619: the issue #7131 readiness gate ({@code isEmptyLogInMultiPeerCluster}) was
 * strictly subsumed by the {@code commitIndex < 0} check that follows it, and the case #7131 was actually about -
 * a follower whose replication channel is wedged, so heartbeats keep a leader known but no entries arrive - still
 * answered Ready. Ratis clamps a follower's commit index to its own flush index, so such a follower has
 * {@code commitIndex == appliedIndex} locally and reports lag {@code 0} however far the leader has moved on.
 * <p>
 * The fix measures the follower's applied index against the commit index the LEADER reports, learned by the
 * health monitor over a follower-to-leader call. These tests drive it with raw indices rather than pre-reduced
 * booleans - the gap the issue called out in the #7131 tests - and through {@link RaftHAServer#isReadyForTraffic}
 * itself, with only the Ratis division and the leader call stood in for.
 */
class Issue7619ReadinessAgainstLeaderCommitTest {

  private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
  private static final long   MAX_LAG     = 100L;

  // ---- the #7131 gate, fed a real commitIndex: it cannot change the answer --------------------------------------

  @Test
  void theEmptyLogFlagNeverChangesTheAnswerWhenComputedFromTheSameCommitIndex() {
    for (final long commitIndex : new long[] { -1L, 0L, 1L, 100L, 5000L })
      for (final long appliedIndex : new long[] { -1L, 0L, 1L, 100L, 5000L })
        for (final int peers : new int[] { 0, 1, 3 }) {
          final boolean withFlag = isReadyForTrafficState(true, true, false, commitIndex, appliedIndex, MAX_LAG, false,
              false, isEmptyLogInMultiPeerCluster(peers, commitIndex));
          final boolean withoutFlag = isReadyForTrafficState(true, true, false, commitIndex, appliedIndex, MAX_LAG,
              false, false, false);
          assertThat(withFlag).as("commit=%d applied=%d peers=%d", commitIndex, appliedIndex, peers)
              .isEqualTo(withoutFlag);
        }
  }

  // ---- the new gate, from raw indices ---------------------------------------------------------------------------

  @Test
  void aFollowerWhoseLocalLogStoppedGrowingIsNotReadyOnceTheLeaderReportsMore() {
    // The wedged channel: local commit == local applied == 100 (lag 0), leader has committed 5000.
    final long local = 100L;
    assertThat(isReadyForTrafficState(true, true, false, local, local, MAX_LAG, false, false, false, true, false,
        isBehindLeaderCommit(5000L, local, MAX_LAG))).isFalse();
    // Without the leader's figure the same follower reads Ready - exactly the bug.
    assertThat(isReadyForTrafficState(true, true, false, local, local, MAX_LAG, false, false, false, true, false,
        false)).isTrue();
  }

  @Test
  void theLeaderGateIsBoundedByMaxLagInclusive() {
    assertThat(isBehindLeaderCommit(1100L, 1000L, MAX_LAG)).as("exactly maxLag behind").isFalse();
    assertThat(isBehindLeaderCommit(1101L, 1000L, MAX_LAG)).as("one past maxLag").isTrue();
    assertThat(isBehindLeaderCommit(1000L, 1000L, 0L)).as("caught up with maxLag 0").isFalse();
    assertThat(isBehindLeaderCommit(1001L, 1000L, 0L)).as("one behind with maxLag 0").isTrue();
  }

  @Test
  void theLeaderGateAbstainsWithoutALeaderFigureAndOnAStaleOne() {
    assertThat(isBehindLeaderCommit(-1L, 1000L, MAX_LAG)).as("no leader commit learned yet").isFalse();
    assertThat(isBehindLeaderCommit(500L, 1000L, MAX_LAG)).as("a sample older than local progress").isFalse();
    assertThat(isBehindLeaderCommit(5000L, -1L, MAX_LAG)).as("unreadable applied index: refused elsewhere").isFalse();
  }

  @Test
  void theLeaderGateAlsoCatchesTheEmptyLogCaseItWasMeantToCover() {
    // A re-joined follower with an empty-but-valid log (applied 0) in a cluster at 5000: the #7131 flag misses it
    // (commitIndex 0 is not < 0), the leader figure does not.
    assertThat(isEmptyLogInMultiPeerCluster(3, 0L)).isFalse();
    assertThat(isReadyForTrafficState(true, true, false, 0L, 0L, MAX_LAG, false, false, false, true, false,
        isBehindLeaderCommit(5000L, 0L, MAX_LAG))).isFalse();
  }

  @Test
  void theLeaderBranchIgnoresTheGate() {
    assertThat(isReadyForTrafficState(true, true, true, 1000L, 1000L, MAX_LAG, false, true, false, true, false, true))
        .isTrue();
  }

  @Test
  void theLeaderCommitIsReadFromTheLeadersOwnCommitInfo() {
    final RaftPeer leader = peer("leader", "localhost:2434");
    final RaftPeer other = peer("other", "localhost:2435");
    final List<RaftProtos.CommitInfoProto> infos = List.of(ProtoUtils.toCommitInfoProto(other, 12L),
        ProtoUtils.toCommitInfoProto(leader, 5000L));

    assertThat(reportedCommitIndexOf(infos, leader.getId())).isEqualTo(5000L);
    assertThat(reportedCommitIndexOf(infos, RaftPeerId.valueOf("absent"))).isEqualTo(-1L);
    assertThat(reportedCommitIndexOf(null, leader.getId())).isEqualTo(-1L);
  }

  // ---- end to end through RaftHAServer ---------------------------------------------------------------------------

  @Test
  void aWedgedFollowerStopsAnsweringReadyOnceTheHealthTickLearnsTheLeaderCommit() throws Exception {
    final Fixture f = new Fixture(true, 100L, 100L);
    f.leaderCommit = 5000L;

    assertThat(f.raft.isReadyForTraffic(MAX_LAG)).as("before the leader figure is known: local lag only").isTrue();

    f.raft.refreshLeaderCommitIndex();

    assertThat(f.probes.get()).isEqualTo(1);
    assertThat(f.raft.getLeaderReportedCommitIndex()).isEqualTo(5000L);
    assertThat(f.raft.isReadyForTraffic(MAX_LAG)).as("local lag 0, leader 4900 entries ahead").isFalse();
  }

  @Test
  void aFollowerWithinMaxLagOfTheLeaderStaysReady() throws Exception {
    final Fixture f = new Fixture(true, 1000L, 1000L);
    f.leaderCommit = 1050L;

    f.raft.refreshLeaderCommitIndex();

    assertThat(f.raft.isReadyForTraffic(MAX_LAG)).isTrue();
  }

  @Test
  void aFailedLeaderCallKeepsThePreviousFigure() throws Exception {
    final Fixture f = new Fixture(true, 100L, 100L);
    f.leaderCommit = 5000L;
    f.raft.refreshLeaderCommitIndex();
    f.leaderCommit = -1L; // the next call fails

    f.raft.refreshLeaderCommitIndex();

    assertThat(f.raft.getLeaderReportedCommitIndex()).isEqualTo(5000L);
    assertThat(f.raft.isReadyForTraffic(MAX_LAG)).isFalse();
  }

  @Test
  void theLeaderIsNotAskedWhenReadinessDoesNotConsultHa() throws Exception {
    final Fixture f = new Fixture(false, 100L, 100L);
    f.leaderCommit = 5000L;

    f.raft.refreshLeaderCommitIndex();

    assertThat(f.probes.get()).as("no call to the leader when nothing reads the answer").isZero();
  }

  @Test
  void theLeaderDoesNotProbeItself() throws Exception {
    final Fixture f = new Fixture(true, 100L, 100L);
    when(f.info.isLeader()).thenReturn(true);

    f.raft.refreshLeaderCommitIndex();

    assertThat(f.probes.get()).isZero();
  }

  @Test
  void theHealthTickAsksForTheLeaderCommit() {
    final AtomicInteger refreshes = new AtomicInteger();
    final HealthMonitor.HealthTarget target = new HealthMonitor.HealthTarget() {
      @Override
      public LifeCycle.State getRaftLifeCycleState() {
        return LifeCycle.State.RUNNING;
      }

      @Override
      public boolean isShutdownRequested() {
        return false;
      }

      @Override
      public void restartRatisIfNeeded() {
      }

      @Override
      public void refreshLeaderCommitIndex() {
        refreshes.incrementAndGet();
      }
    };
    final HealthMonitor monitor = new HealthMonitor(target, 0);

    monitor.tick();
    monitor.tick();

    assertThat(refreshes.get()).isEqualTo(2);
  }

  private static RaftPeer peer(final String id, final String address) {
    return RaftPeer.newBuilder().setId(id).setAddress(address).build();
  }

  /**
   * A {@link RaftHAServer} that believes it is a running follower of a three-peer group with the given local
   * commit/applied indices, whose leader answers the commit-index probe with {@link #leaderCommit}.
   */
  private static final class Fixture {
    final RaftHAServer  raft;
    final DivisionInfo  info;
    final AtomicInteger probes = new AtomicInteger();
    volatile long       leaderCommit;

    Fixture(final boolean readinessRequiresHa, final long localCommit, final long localApplied) throws Exception {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
      config.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, readinessRequiresHa);

      final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
      when(mockServer.getServerName()).thenReturn("ArcadeDB_0");
      raft = new RaftHAServer(mockServer, config);

      final RaftPeer self = peer(raft.getLocalPeerId().toString(), "localhost:2434");
      final RaftPeer leader = peer("leader", "localhost:2435");
      final RaftPeer third = peer("third", "localhost:2436");

      info = mock(DivisionInfo.class);
      when(info.getLeaderId()).thenReturn(leader.getId());
      when(info.isLeader()).thenReturn(false);
      when(info.getLastAppliedIndex()).thenReturn(localApplied);
      when(info.getLifeCycleState()).thenReturn(LifeCycle.State.RUNNING);

      final RaftConfiguration conf = mock(RaftConfiguration.class);
      when(conf.getCurrentPeers()).thenReturn(List.of(self, leader, third));

      final RaftLog log = mock(RaftLog.class);
      when(log.getLastCommittedIndex()).thenReturn(localCommit);

      final RaftServer.Division division = mock(RaftServer.Division.class);
      when(division.getInfo()).thenReturn(info);
      when(division.getRaftConf()).thenReturn(conf);
      when(division.getRaftLog()).thenReturn(log);

      final RaftServer ratis = mock(RaftServer.class);
      when(ratis.getDivision(any())).thenReturn(division);

      final Field field = RaftHAServer.class.getDeclaredField("raftServer");
      field.setAccessible(true);
      field.set(raft, ratis);

      // Neither resyncing nor halted: the gates this test is not about stay open.
      final ArcadeStateMachine stateMachine = mock(ArcadeStateMachine.class);
      when(stateMachine.isResyncInProgress()).thenReturn(false);
      when(stateMachine.isHaltedAfterCriticalError()).thenReturn(false);
      final Field smField = RaftHAServer.class.getDeclaredField("stateMachine");
      smField.setAccessible(true);
      smField.set(raft, stateMachine);

      raft.setLeaderCommitProber(target -> {
        assertThat(target.getId()).isEqualTo(leader.getId());
        probes.incrementAndGet();
        return leaderCommit;
      });
    }
  }
}
