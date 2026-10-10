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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9255 (#9229), against a running cluster whose Raft and HTTP ports are not in step: a node holding no address for
 * a member - a node that never heard the member's declaration, or one whose entry a removal dropped - derives one from the
 * member's Raft host plus its OWN HTTP port, which on this fixture is the node itself, so it could not reach the member at
 * all: no capability answer, so a security entry gated on it was refused, and every other dial of the member went
 * nowhere. The node now learns the member's address the way every node does - from the member's own capability requests,
 * or relayed by a member that holds it - and confirms it with a probe the member answers under its own id.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9255PeerHttpAddressLearningIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void aNodeThatLostAMembersAddressLearnsItFromTheMembersOwnRequests() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = (leader + 1) % getServerCount();
    final int member = (leader + 2) % getServerCount();

    forgetAndAwaitRelearnt(follower, member);
  }

  @Test
  void aNodeLearnsAMembersAddressFromAnotherMembersRelayWhenTheMemberIsSilent() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = (leader + 1) % getServerCount();
    final int member = (leader + 2) % getServerCount();

    // The member sends no capability request at all, so it pushes nothing: only the leader's relay can carry its address
    final RaftHAServer silent = getRaftPlugin(member).getRaftHAServer();
    silent.stopCapabilityMonitor();
    try {
      forgetAndAwaitRelearnt(follower, member);
    } finally {
      silent.startCapabilityMonitor();
    }
  }

  private void forgetAndAwaitRelearnt(final int nodeIndex, final int memberIndex) {
    final RaftHAServer node = getRaftPlugin(nodeIndex).getRaftHAServer();
    final RaftPeerId memberId = RaftPeerId.valueOf(peerIdForIndex(memberIndex));
    final int memberHttpPort = getServerHttpPort(memberIndex);

    node.getHttpAddresses().remove(memberId);
    assertThat(RaftHAServer.extractPort(node.getPeerHttpAddress(memberId)))
        .as("derived from the member's Raft host plus this node's own port, which names this node").isNotEqualTo(memberHttpPort);

    Awaitility.await("the node learns the member's HTTP address back").atMost(60, TimeUnit.SECONDS)
        .pollInterval(250, TimeUnit.MILLISECONDS)
        .until(() -> RaftHAServer.extractPort(node.getHttpAddresses().get(memberId)) == memberHttpPort);
    assertThat(node.getPeerHttpAddress(memberId)).endsWith(":" + memberHttpPort);
    Awaitility.await("and the member's capabilities with it").atMost(30, TimeUnit.SECONDS)
        .pollInterval(250, TimeUnit.MILLISECONDS)
        .until(() -> node.getPeerCapabilityRegistry().freshAdvertisementOf(memberId.toString()) != null);
  }
}
