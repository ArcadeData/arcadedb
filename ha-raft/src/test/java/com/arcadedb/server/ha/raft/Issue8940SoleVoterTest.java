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
 * {@link RaftHAServer#isSoleVoter(java.util.Collection, RaftPeerId)}: the gate that keeps a node with nobody to hand
 * off to or install from out of a permanent quarantine (issue #8940).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8940SoleVoterTest {
  private static final RaftPeerId LOCAL = RaftPeerId.valueOf("local");

  @Test
  void aSingleVoterThatIsThisNodeIsSole() {
    assertThat(RaftHAServer.isSoleVoter(List.of(peer("local")), LOCAL)).isTrue();
  }

  @Test
  void aSingleVoterThatIsAnotherNodeIsNotSole() {
    assertThat(RaftHAServer.isSoleVoter(List.of(peer("other")), LOCAL)).isFalse();
  }

  @Test
  void severalVotersAreNeverSole() {
    assertThat(RaftHAServer.isSoleVoter(List.of(peer("local"), peer("other")), LOCAL)).isFalse();
  }

  @Test
  void noVotersIsNotSole() {
    assertThat(RaftHAServer.isSoleVoter(List.of(), LOCAL)).isFalse();
  }

  private static RaftPeer peer(final String id) {
    return RaftPeer.newBuilder().setId(id).setAddress("localhost:0").build();
  }
}
