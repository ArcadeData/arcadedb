/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8321, follow-up to #7619: Ratis clamps a follower's commit index to the entries it holds, so a follower whose
 * inbound replication channel is wedged computes a local lag of 0. #7619 moved the readiness probe onto the commit
 * index the leader reports; the other follower-side readers of {@code commit - applied} - the stale-follower
 * self-recovery ({@link RaftHAServer#isFollowerLaggingBeyond}), the catch-up progress log and the
 * {@code localReplicationLag} of the follower's own {@code GET /api/v1/cluster} - kept the clamped figure. They now all
 * measure against {@link RaftHAServer#getFollowerCommitIndex()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8321FollowerLagAgainstLeaderCommitTest {
  private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
  private static final long   THRESHOLD   = 1_000L;

  @Test
  void theLargerOfTheTwoCommitIndexesIsTheOneLagIsMeasuredAgainst() {
    assertThat(RaftHAServer.followerCommitIndex(100L, 5_000L)).isEqualTo(5_000L);
    assertThat(RaftHAServer.followerCommitIndex(5_000L, 100L)).as("a stale leader figure never lowers it").isEqualTo(5_000L);
    assertThat(RaftHAServer.followerCommitIndex(100L, -1L)).as("nothing learned from the leader yet").isEqualTo(100L);
    assertThat(RaftHAServer.followerCommitIndex(-1L, 5_000L)).as("the local state is unreadable: still unknown").isEqualTo(-1L);
  }

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

  @Test
  void aFollowerThatIsApplyingIsNotRecovered() throws Exception {
    final Fixture f = new Fixture();
    f.raft.refreshLeaderCommitIndex();

    assertThat(f.raft.isFollowerLaggingBeyond(THRESHOLD)).isTrue();
    when(f.info.getLastAppliedIndex()).thenReturn(150L);
    assertThat(f.raft.isFollowerLaggingBeyond(THRESHOLD)).as("it applied 50 entries since the last tick").isFalse();
  }

  @Test
  void aFollowerWithinTheThresholdOfTheLeaderIsNotLagging() throws Exception {
    final Fixture f = new Fixture();
    f.raft.setLeaderCommitProber(target -> 100L + THRESHOLD);
    f.raft.refreshLeaderCommitIndex();

    assertThat(f.raft.isFollowerLaggingBeyond(THRESHOLD)).isFalse();
  }

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

      final ArcadeStateMachine stateMachine = mock(ArcadeStateMachine.class);
      final Field smField = RaftHAServer.class.getDeclaredField("stateMachine");
      smField.setAccessible(true);
      smField.set(raft, stateMachine);

      raft.setLeaderCommitProber(target -> 5_000L);
    }
  }
}
