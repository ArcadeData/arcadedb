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
import com.arcadedb.server.TestServerHelper;
import java.lang.reflect.Field;
import java.util.List;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

import static com.arcadedb.server.ha.raft.RaftHAServer.isPersistentlyLagging;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link RaftHAServer#isPersistentlyLagging} predicate behind the issue #4840 fix.
 * The recovery it gates ({@code recoverFromPersistentLag()} -> {@code triggerSnapshotDownload()}) closes
 * and re-downloads the live database, so it must fire only for a follower that is genuinely stuck, never
 * for one that is merely slow but still applying. Before the fix a follower applying steady-state
 * AppendEntries one entry at a time never set the {@code catchingUp} flag, so a sustained-write workload
 * misclassified a healthy slow follower as lagging and resynced it needlessly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftHAServerStaleFollowerLagTest {

  // Argument order: commitIndex, appliedIndex, lagThreshold, previousAppliedIndex

  @Test
  void stuckFollowerBeyondThresholdFires() {
    // Applied frozen at 100 while commit raced to 2000, lag 1900 > 1000, no progress since last tick.
    assertThat(isPersistentlyLagging(2000, 100, 1000, 100)).isTrue();
  }

  @Test
  void slowButProgressingFollowerDoesNotFire() {
    // Applied advanced 100 -> 200 since the previous tick. Still 1900 behind commit, but actively
    // replaying the backlog one entry at a time: healthy-but-slow, must not be resynced (issue #4840).
    assertThat(isPersistentlyLagging(2100, 200, 1000, 100)).isFalse();
  }

  @Test
  void evenSingleEntryProgressDoesNotFire() {
    // A single applied entry since the previous tick is enough to prove the follower is not stuck.
    assertThat(isPersistentlyLagging(2100, 101, 1000, 100)).isFalse();
  }

  @Test
  void lagWithinThresholdDoesNotFire() {
    // commit - applied == lagThreshold is NOT beyond the threshold (strict >).
    assertThat(isPersistentlyLagging(1100, 100, 1000, 100)).isFalse();
    // Comfortably within the threshold.
    assertThat(isPersistentlyLagging(150, 100, 1000, 100)).isFalse();
  }

  @Test
  void firstObservationWithoutPriorSampleFiresOnLagAlone() {
    // previousAppliedIndex < 0: no baseline to measure progress against, so the lag alone decides. The
    // HealthMonitor's persistence window still requires the condition to repeat before it acts, and the
    // next tick (which has a baseline) detects any progress.
    assertThat(isPersistentlyLagging(2000, 100, 1000, -1)).isTrue();
  }

  @Test
  void caughtUpFollowerDoesNotFire() {
    assertThat(isPersistentlyLagging(100, 100, 1000, 90)).isFalse();
  }

  @Test
  void unreadableStateDoesNotFire() {
    assertThat(isPersistentlyLagging(-1, 100, 1000, 100)).as("negative commitIndex").isFalse();
    assertThat(isPersistentlyLagging(2000, -1, 1000, 100)).as("negative appliedIndex").isFalse();
  }

  @Test
  void sustainedWriteWorkloadNeverFiresAcrossManyTicks() {
    // Simulate the issue scenario: a healthy follower applies one entry per tick while the leader keeps
    // committing ahead of it, so the absolute lag stays large forever. Threading the applied index from
    // each tick into the next (as isFollowerLaggingBeyond does), the predicate must report "not stuck"
    // on every tick after the first, so the HealthMonitor streak never persists and no resync triggers.
    long previousApplied = -1;
    int laggingTicks = 0;
    for (int tick = 0; tick < 20; tick++) {
      final long applied = 100 + tick;     // advances by one every tick
      final long commit = 2000 + tick * 2; // leader commits faster: lag grows, follower never stuck
      if (isPersistentlyLagging(commit, applied, 1000, previousApplied))
        laggingTicks++;
      previousApplied = applied;
    }
    // Only the very first tick (no baseline) can report lagging; every subsequent tick sees progress.
    assertThat(laggingTicks).isEqualTo(1);
  }

  @Test
  void genuinelyStuckFollowerFiresEveryTick() {
    // The applied index is frozen (e.g. apply thread wedged) while commit keeps advancing.
    long previousApplied = -1;
    int laggingTicks = 0;
    for (int tick = 0; tick < 20; tick++) {
      final long applied = 100;            // frozen
      final long commit = 2000 + tick * 2; // commit advances
      if (isPersistentlyLagging(commit, applied, 1000, previousApplied))
        laggingTicks++;
      previousApplied = applied;
    }
    assertThat(laggingTicks).isEqualTo(20);
  }
  // ---- #8321 ----
  // Issue #8321, follow-up to #7619: Ratis clamps a follower's commit index to the entries it holds, so a follower whose
  // inbound replication channel is wedged computes a local lag of 0. #7619 moved the readiness probe onto the commit
  // index the leader reports; the other follower-side readers of {@code commit - applied} - the stale-follower
  // self-recovery ({@link RaftHAServer#isFollowerLaggingBeyond}), the catch-up progress log and the
  // {@code localReplicationLag} of the follower's own {@code GET /api/v1/cluster} - kept the clamped figure. They now all
  // measure against {@link RaftHAServer#getFollowerCommitIndex()}.

  private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
  private static final long   THRESHOLD   = 1_000L;

  // Regression for #8321: the larger of the two commit indexes is the one lag is measured against
  @Test
  void theLargerOfTheTwoCommitIndexesIsTheOneLagIsMeasuredAgainst() {
    assertThat(RaftHAServer.followerCommitIndex(100L, 5_000L)).isEqualTo(5_000L);
    assertThat(RaftHAServer.followerCommitIndex(5_000L, 100L)).as("a stale leader figure never lowers it").isEqualTo(5_000L);
    assertThat(RaftHAServer.followerCommitIndex(100L, -1L)).as("nothing learned from the leader yet").isEqualTo(100L);
    assertThat(RaftHAServer.followerCommitIndex(-1L, 5_000L)).as("the local state is unreadable: still unknown").isEqualTo(-1L);
  }

  // Regression for #8321: the stale follower recovery arms on A wedged follower
  @Test
  void theStaleFollowerRecoveryArmsOnAWedgedFollower() throws Exception {
    final Fixture f = new Fixture();

    assertThat(f.raft.isFollowerLaggingBeyond(THRESHOLD)).as("before the leader was asked: local lag 0").isFalse();

    f.raft.refreshLeaderCommitIndex();

    assertThat(f.raft.getFollowerCommitIndex()).isEqualTo(5_000L);
    assertThat(f.raft.isFollowerLaggingBeyond(THRESHOLD))
        .as("4900 entries behind the leader's commit index and applying nothing")
        .isTrue();
    assertThat(f.raft.isFollowerLaggingBeyond(THRESHOLD)).as("and still stuck on the next tick").isTrue();
  }

  // Regression for #8321: a follower that is applying is not recovered
  @Test
  void aFollowerThatIsApplyingIsNotRecovered() throws Exception {
    final Fixture f = new Fixture();
    f.raft.refreshLeaderCommitIndex();

    assertThat(f.raft.isFollowerLaggingBeyond(THRESHOLD)).isTrue();
    when(f.info.getLastAppliedIndex()).thenReturn(150L);
    assertThat(f.raft.isFollowerLaggingBeyond(THRESHOLD)).as("it applied 50 entries since the last tick").isFalse();
  }

  // Regression for #8321: a follower within the threshold of the leader is not lagging
  @Test
  void aFollowerWithinTheThresholdOfTheLeaderIsNotLagging() throws Exception {
    final Fixture f = new Fixture();
    f.raft.setLeaderCommitProber(target -> 100L + THRESHOLD);
    f.raft.refreshLeaderCommitIndex();

    assertThat(f.raft.isFollowerLaggingBeyond(THRESHOLD)).isFalse();
  }

  // Regression for #8321: on the leader A figure learned as A follower is ignored
  @Test
  void onTheLeaderAFigureLearnedAsAFollowerIsIgnored() throws Exception {
    final Fixture f = new Fixture();
    f.raft.refreshLeaderCommitIndex();
    assertThat(f.raft.getFollowerCommitIndex()).isEqualTo(5_000L);

    when(f.info.isLeader()).thenReturn(true);

    assertThat(f.raft.getFollowerCommitIndex()).as("the leader's own commit index is the cluster's").isEqualTo(100L);
    assertThat(f.raft.isFollowerLaggingBeyond(THRESHOLD)).isFalse();
  }

  private static RaftPeer peer(final String id, final String address) {
    return RaftPeer.newBuilder().setId(id).setAddress(address).build();
  }

  /**
   * A {@link RaftHAServer} that believes it is a running follower of a three-peer group, at commit == applied == 100
   * locally - the wedged channel - whose leader answers the commit-index probe with 5000.
   */
  private static final class Fixture {
    final RaftHAServer raft;
    final DivisionInfo info;

    Fixture() throws Exception {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);

      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
      raft = new RaftHAServer(arcadeServer, config);

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

      final RaftLog log = mock(RaftLog.class);
      when(log.getLastCommittedIndex()).thenReturn(100L);

      final RaftServer.Division division = mock(RaftServer.Division.class);
      when(division.getInfo()).thenReturn(info);
      when(division.getRaftConf()).thenReturn(conf);
      when(division.getRaftLog()).thenReturn(log);

      final RaftServer ratis = mock(RaftServer.class);
      when(ratis.getDivision(any())).thenReturn(division);

      final Field field = RaftHAServer.class.getDeclaredField("raftServer");
      field.setAccessible(true);
      field.set(raft, ratis);

      final ArcadeStateMachine stateMachine = new ArcadeStateMachine();
      final Field smField = RaftHAServer.class.getDeclaredField("stateMachine");
      smField.setAccessible(true);
      smField.set(raft, stateMachine);

      raft.setLeaderCommitProber(target -> 5_000L);
    }
  }
}
