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
 * Which HTTPS endpoint the bootstrap-divergence probe of {@code ArcadeStateMachine.verifyBootstrapDivergence}
 * may dial, once that probe is allowed to choose a scheme at all (issues #7546 / #7563).
 * <p>
 * The plain-HTTP half of that call site asks the two questions issue #6202 requires of an address acted on
 * unattended - it must identify one peer, and it must not be our own - through
 * {@link RaftHAServer#getUnambiguousPeerHttpAddress}. The encrypted half has to be asked the same two
 * separately: the endpoints come from two independent fields of {@code arcadedb.ha.serverList} (the 3rd and
 * the 5th), each with its own derive fallback, so an HTTP verdict says nothing about the HTTPS one.
 * <p>
 * {@link RaftHAServer#getLeaderHttpsAddress()} answers only the second of the two, and says so: it resolves
 * through the raw resolver on the argument that "an address that names the wrong node is caught by the
 * receiving node's one-hop refusal, since the forward carries FORWARDED_TO_LEADER_HEADER". That argument is
 * the leader <em>forward</em>'s, and it does not transfer here - {@code POST /api/v1/cluster/bootstrap-state}
 * answers with the receiving node's own state and refuses nothing - so the probe needs the guard the
 * forward can do without.
 * <p>
 * {@link PeerDialAddress#resolve} is where both questions are already answered for both schemes, from one
 * peer identity. What this test pins is that on a topology the HTTP guard waves through, the two are not
 * interchangeable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7563DivergenceProbeLeaderIdentityTest {

  /** This node is the first entry; the two others declare distinct http ports and SHARE one https port. */
  private static final String SHARED_HTTPS_PORT_CLUSTER =
      "localhost:2434:2480:0:2490,localhost:2435:2481:0:2491,localhost:2436:2482:0:2491";

  private static final RaftPeerId LEADER = RaftPeerId.valueOf("localhost_2435");

  /**
   * The gap: the HTTP address of the leader identifies it alone, so the call site proceeds, while the HTTPS
   * address two peers answer to identifies neither - and the scheme selection prefers it.
   */
  @Test
  void theLeaderHttpsAddressOfThisClusterIdentifiesNoPeerYetTheHttpOneDoes() {
    final RaftHAServer raft = newDetachedServer(SHARED_HTTPS_PORT_CLUSTER);

    assertThat(raft.getUnambiguousPeerHttpAddress(LEADER))
        .as("distinct http ports: the plain guard has nothing to object to")
        .isEqualTo("localhost:2481");
    assertThat(raft.isOwnHttpAddress("localhost:2481")).isFalse();

    assertThat(raft.getUnambiguousPeerHttpsAddress(LEADER))
        .as("two peers answer to localhost:2491, so it identifies neither")
        .isNull();
    assertThat(raft.getPeerHttpsAddress(LEADER))
        .as("the raw resolver hands it out all the same")
        .isEqualTo("localhost:2491");
  }

  /**
   * The composition both proposed fixes use - the raw resolver behind {@code getLeaderHttpsAddress()}, whose
   * only filter is "is it our own" - keeps the shared address, so the probe would dial a node that may not be
   * the leader and read its answer as the leader's.
   */
  @Test
  void theSelfCheckAloneDoesNotWithholdAnAddressSharedWithAnotherPeer() {
    final RaftHAServer raft = newDetachedServer(SHARED_HTTPS_PORT_CLUSTER);

    assertThat(RaftHAServer.preferredLeaderHttpsAddress(true, raft.getPeerHttpsAddress(LEADER),
        raft.getLocalHttpsAddress()))
        .as("localhost:2491 is not ours - it is localhost_2436's just as much as the leader's")
        .isEqualTo("localhost:2491");
  }

  /** The guard that does withhold it, from one peer identity and for both schemes at once. */
  @Test
  void theSharedHttpsEndpointIsWithheldWhenResolvedThroughPeerDialAddress() {
    final RaftHAServer raft = newDetachedServer(SHARED_HTTPS_PORT_CLUSTER);

    final PeerDialAddress dial = PeerDialAddress.resolve(raft, LEADER, "leader");

    assertThat(dial.refused()).as("the plain listener is still reachable, so this is no refusal").isFalse();
    assertThat(dial.httpAddress()).isEqualTo("localhost:2481");
    assertThat(dial.httpsAddress())
        .as("withheld, so the probe falls back to the guarded plain endpoint instead of dialling an unknown node")
        .isNull();
  }

  /** And the guard costs a correctly declared SSL cluster nothing: its own https endpoint is handed over. */
  @Test
  void aLeaderWithAnHttpsEndpointOfItsOwnIsStillProbedOverTls() {
    final RaftHAServer raft = newDetachedServer(
        "localhost:2434:2480:0:2490,localhost:2435:2481:0:2491,localhost:2436:2482:0:2492");

    final PeerDialAddress dial = PeerDialAddress.resolve(raft, LEADER, "leader");

    assertThat(dial.httpAddress()).isEqualTo("localhost:2481");
    assertThat(dial.httpsAddress()).isEqualTo("localhost:2491");
  }

  /**
   * A {@link RaftHAServer} built from {@code serverList} with Ratis never started: the peer group and both
   * declared address maps are populated by the constructor, which is all these accessors read.
   */
  private static RaftHAServer newDetachedServer(final String serverList) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, true);

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("ArcadeDB_0");

    return new RaftHAServer(mockServer, config);
  }
}
