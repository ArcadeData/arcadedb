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

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Issue #9464: {@link FakeRaftHAServer} starts where an unstubbed mock did and answers what the test set. */
class FakeRaftHAServerTest {
  private static final RaftPeerId LEADER = RaftPeerId.valueOf("peer-b");

  @Test
  void aBareFakeAnswersLikeANodeWithNoDivision() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();

    assertThat(raft.isLeader()).isFalse();
    assertThat(raft.isLeaderReady()).isFalse();
    assertThat(raft.getLeaderId()).isNull();
    assertThat(raft.getLeaderHttpAddress()).isNull();
    assertThat(raft.getLocalHttpAddress()).isNull();
    assertThat(raft.getClusterToken()).isNull();
    assertThat(raft.getCurrentTerm()).isEqualTo(-1);
    assertThat(raft.getCommitIndex()).isEqualTo(-1);
  }

  @Test
  void aFollowerAnswersItsLeaderAndTheLeadersAddress() {
    final FakeRaftHAServer raft = FakeRaftHAServer.followerOf("peer-b", "peer-b:2480").localHttpAddress("localhost:2480")
        .currentTerm(3).commitIndex(42).clusterToken("token");

    assertThat(raft.isLeader()).isFalse();
    assertThat(raft.getLeaderId()).isEqualTo(LEADER);
    assertThat(raft.getLeaderHttpAddress()).isEqualTo("peer-b:2480");
    assertThat(raft.getUnambiguousPeerHttpAddress(LEADER)).isEqualTo("peer-b:2480");
    assertThat(raft.getPeerHttpAddress(LEADER)).isEqualTo("peer-b:2480");
    assertThat(raft.getCurrentTerm()).isEqualTo(3);
    assertThat(raft.getCommitIndex()).isEqualTo(42);
    assertThat(raft.getClusterToken()).isEqualTo("token");
  }

  @Test
  void theDerivedAnswersFollowTheValuesSet() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leader(true).localHttpAddress("127.0.0.1:2480");

    assertThat(raft.isLeaderReady()).isTrue();
    assertThat(raft.isOwnHttpAddress("localhost:2480")).as("the real loopback comparison, over the address set here").isTrue();
    assertThat(raft.isOwnHttpAddress("peer-b:2480")).isFalse();
  }

  @Test
  void severalAddressesAreAnsweredInOrderThenTheLastOneSticks() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().peerHttpAddress(LEADER, "peer-b:2480", "localhost:2480");

    assertThat(raft.getUnambiguousPeerHttpAddress(LEADER)).isEqualTo("peer-b:2480");
    assertThat(raft.getUnambiguousPeerHttpAddress(LEADER)).isEqualTo("localhost:2480");
    assertThat(raft.getPeerHttpAddress(LEADER)).isEqualTo("localhost:2480");
  }

  @Test
  void thePluginDelegatesToTheFake() {
    final FakeRaftHAServer raft = FakeRaftHAServer.followerOf("peer-b", "peer-b:2480");
    final RaftHAPlugin plugin = raft.plugin();

    assertThat(plugin.getRaftHAServer()).isSameAs(raft);
    assertThat(plugin.isLeader()).isFalse();
    assertThat(plugin.getLeaderAddress()).isEqualTo("peer-b:2480");
    raft.leader(true);
    assertThat(plugin.isLeader()).isTrue();
  }

  @Test
  void leadershipRequestsAreRecordedPerOverloadAndAnswerTheMockDefaults() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();

    assertThat(raft.transferLeadership(1_000L)).isFalse();
    assertThat(raft.transferLeadership(1_000L, false)).isFalse();
    raft.transferLeadership("peer-b", 2_000L);
    raft.stepDown();

    assertThat(raft.calls("transferLeadership")).containsExactly(List.of(1_000L), List.of(1_000L, false), List.of("peer-b", 2_000L));
    assertThat(raft.calls("stepDown")).hasSize(1);
    assertThat(raft.isSoleVoter()).as("a member of a real cluster, unless a test says otherwise").isFalse();
  }

  @Test
  void aGetterAnswersAFunctionOfTheMomentWhenOneIsSet() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().leaderId(LEADER);
    assertThat(raft.getLeaderId()).as("the value set, while no function is").isEqualTo(LEADER);

    raft.on("getLeaderId", CallLog.inOrder(null, null, LEADER));
    assertThat(raft.getLeaderId()).isNull();
    assertThat(raft.getLeaderId()).isNull();
    assertThat(raft.getLeaderId()).isEqualTo(LEADER);
    assertThat(raft.getLeaderId()).as("the last value sticks").isEqualTo(LEADER);
  }

  @Test
  void anEmptySequenceIsRefused() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();

    assertThatThrownBy(raft::leader).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(raft::commitIndex).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(raft::currentTerm).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void theRunningClusterStateMachineHasAppliedEntriesWhereTheDetachedOneHasNot() {
    assertThat(FakeRaftHAServer.detached().getStateMachine().hasNeverAppliedApplicationEntry()).isTrue();

    final FakeRaftHAServer raft = FakeRaftHAServer.detached().stateMachine(FakeRaftHAServer.stateMachineOfARunningCluster());
    assertThat(raft.getStateMachine().hasNeverAppliedApplicationEntry()).isFalse();
  }

  @Test
  void aLeaderAddressSetDirectlyWinsOverTheLeaderIdsAddress() {
    assertThat(FakeRaftHAServer.detached().leaderHttpAddress("leader:2480").getLeaderHttpAddress()).isEqualTo("leader:2480");
    assertThat(FakeRaftHAServer.followerOf("peer-b", "peer-b:2480").leaderHttpAddress("other:2480").getLeaderHttpAddress())
        .isEqualTo("other:2480");
  }
}
