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

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for the resolver every snapshot resync path goes through since issue #6202:
 * {@link RaftHAServer#getUnambiguousPeerHttpAddress}.
 * <p>
 * Two peers can never legitimately answer on one {@code host:port} - they would be fighting over the socket - so
 * an address two of them resolve to identifies at most one, and nothing can say which. The plain
 * {@code getPeerHttpAddress} keeps returning the best-effort address for cluster reporting and for a human
 * reading it; this variant is for the callers that act on it unattended, where reconciling every database from
 * the wrong node cannot be undone.
 * <p>
 * It is the same rule {@link RaftHAServer#selectUnambiguousRouting} applies to client routing tables (#6183),
 * now sharing its claim-counting core with it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6202UnambiguousPeerHttpAddressTest {

  /** Distinct declared HTTP ports: every peer owns its address, so each one is handed out. */
  @Test
  void anAddressOnlyOnePeerClaimsIsReturned() {
    final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");

    assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("localhost_2435"))).isEqualTo("localhost:2481");
    assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("localhost_2436"))).isEqualTo("localhost:2482");
  }

  /**
   * The shape the snapshot path has to refuse: two peers resolve to one address, so it names at most one of them.
   * A follower that reconciled from it would report success while carrying another node's - possibly its own -
   * databases.
   */
  @Test
  void anAddressTwoPeersClaimIsWithheld() {
    final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2490,localhost:2436:2490");

    assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("localhost_2435")))
        .as("both peers answer to localhost:2490, so neither is identified by it")
        .isNull();
    assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("localhost_2436"))).isNull();
    assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("localhost_2434")))
        .as("the unaffected peer is still identified by its own address")
        .isEqualTo("localhost:2480");
  }

  /** An unknown peer resolves to nothing, which is a refusal rather than a best-effort guess. */
  @Test
  void anUnknownPeerResolvesToNothing() {
    final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2481");

    assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("nobody_9999"))).isNull();
    assertThat(raft.getUnambiguousPeerHttpAddress(null)).isNull();
  }

  /**
   * A {@link RaftHAServer} built from {@code serverList} with Ratis never started: the peer group and the
   * declared HTTP addresses are both populated by the constructor, which is all this resolver reads. The node
   * names itself with the {@code prefix_N} convention so a same-host server list still resolves a local peer.
   */
  private static RaftHAServer newDetachedServer(final String serverList) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList);

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("ArcadeDB_0");

    return new RaftHAServer(mockServer, config);
  }
}
