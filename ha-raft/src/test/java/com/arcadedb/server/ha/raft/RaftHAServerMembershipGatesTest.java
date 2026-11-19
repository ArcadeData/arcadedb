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

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests of the membership gates of {@link RaftHAServer}: the sole-voter escape that keeps a node with nobody to hand
 * off to out of a permanent quarantine, and the removed-from-configuration check that fails open.
 */
class RaftHAServerMembershipGatesTest {

  private static final RaftPeerId LOCAL = RaftPeerId.valueOf("local");

  // Regression for #8940: isSoleVoter is true only for a single voter that is this node
  /**
   * {@link RaftHAServer#isSoleVoter(java.util.Collection, RaftPeerId)}: the gate that keeps a node with nobody to hand
   * off to or install from out of a permanent quarantine (issue #8940).
   */
  @Test
  void aSingleVoterThatIsThisNodeIsSole() {
    assertThat(RaftHAServer.isSoleVoter(List.of(peer("local")), LOCAL)).isTrue();
  }

  // Regression for #8940: isSoleVoter is true only for a single voter that is this node
  @Test
  void aSingleVoterThatIsAnotherNodeIsNotSole() {
    assertThat(RaftHAServer.isSoleVoter(List.of(peer("other")), LOCAL)).isFalse();
  }

  // Regression for #8940: isSoleVoter is true only for a single voter that is this node
  @Test
  void severalVotersAreNeverSole() {
    assertThat(RaftHAServer.isSoleVoter(List.of(peer("local"), peer("other")), LOCAL)).isFalse();
  }

  // Regression for #8940: isSoleVoter is true only for a single voter that is this node
  @Test
  void noVotersIsNotSole() {
    assertThat(RaftHAServer.isSoleVoter(List.of(), LOCAL)).isFalse();
  }

  private static RaftPeer peer(final String id) {
    return RaftPeer.newBuilder().setId(id).setAddress("localhost:0").build();
  }

  // Regression for #9510: a server with no Raft server is not considered removed from the configuration
  /**
   * Issue #9510: {@link RaftHAServer#isRemovedFromConfiguration()} refuses only on a configuration it has read. A server
   * whose Raft server has not started has no configuration at all, and must not be taken for a removed one - every
   * commit on it would be refused. The true answer, on a live cluster, is covered by
   * {@code HaRemovedNodeIT}.
   */
  @Test
  void aServerWithNoRaftServerIsNotRemoved() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    assertThat(raft.isRemovedFromConfiguration()).isFalse();
  }
}
