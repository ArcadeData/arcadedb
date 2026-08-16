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
 * Unit test for {@link PeerDialAddress#resolve}, the one place the question "may this node dial that one, and at
 * which address?" is answered (issue #6221).
 * <p>
 * It was being answered separately by every caller that acts on a resolved peer address unattended, in versions
 * that had drifted: the snapshot resync refused three ways, the cluster-verify fan-out refused none of them and
 * reported the resulting self-comparison as the peer agreeing.
 * <p>
 * Driven against a real {@link RaftHAServer} built from a server list with Ratis never started - the peer group
 * and the declared HTTP addresses are both populated by the constructor, which is all the resolver reads - so
 * these assertions bind the composition rather than a stubbed answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6221PeerDialAddressTest {

  /** Distinct declared HTTP ports: every peer owns its address, so it is handed out to be dialled. */
  @Test
  void aPeerWithAnAddressOfItsOwnIsDialled() {
    final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");

    final PeerDialAddress dial = PeerDialAddress.resolve(raft, RaftPeerId.valueOf("localhost_2435"), "peer");

    assertThat(dial.refused()).isFalse();
    assertThat(dial.httpAddress()).isEqualTo("localhost:2481");
    assertThat(dial.refusal()).isNull();
  }

  /**
   * The shape the derive fallback produces on a cluster whose nodes share a host: two peers answering to one
   * {@code host:port}, which two listening sockets cannot both own. It identifies at most one of them and nothing
   * can say which, so the fan-out that dialled it would report a node it never contacted.
   */
  @Test
  void aPeerWhoseAddressIsSharedWithAnotherIsRefused() {
    final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2490,localhost:2436:2490");

    final PeerDialAddress dial = PeerDialAddress.resolve(raft, RaftPeerId.valueOf("localhost_2435"), "peer");

    assertThat(dial.refused()).isTrue();
    assertThat(dial.httpAddress()).isNull();
    assertThat(dial.refusal())
        .contains("identifies peer localhost_2435")
        .contains(GlobalConfiguration.HA_SERVER_LIST.getKey());
    assertThat(PeerDialAddress.resolve(raft, RaftPeerId.valueOf("localhost_2436"), "peer").refused())
        .as("neither of the two peers sharing the address is identified by it")
        .isTrue();
  }

  /**
   * The refusal the ambiguity check cannot make, and the reason the self-address check stands behind it: the
   * address is claimed by exactly one peer - so nothing about it is ambiguous - and it is still this node's own,
   * spelled the other way round. A single-machine cluster routinely writes one node {@code localhost} and the
   * next {@code 127.0.0.1} (issue #6204); the claim count compares text and cannot see that they are one socket.
   */
  @Test
  void aPeerWhoseUnambiguousAddressIsThisNodesOwnIsRefused() {
    final RaftHAServer raft = newDetachedServer("127.0.0.1:2434:2480,localhost:2435:2480,localhost:2436:2482");

    final PeerDialAddress dial = PeerDialAddress.resolve(raft, RaftPeerId.valueOf("localhost_2435"), "peer");

    assertThat(dial.refused())
        .as("localhost:2480 and 127.0.0.1:2480 are one listening socket, and it is ours")
        .isTrue();
    assertThat(dial.refusal()).contains("is this node's own");
  }

  /** A node is not a peer of itself: dialling it would come straight back here, whatever it resolves to. */
  @Test
  void thisNodeIsRefusedAsAPeerOfItself() {
    final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2481");

    final PeerDialAddress dial = PeerDialAddress.resolve(raft, RaftPeerId.valueOf("localhost_2434"), "peer");

    assertThat(dial.refused()).isTrue();
    assertThat(dial.refusal()).contains("is this node itself");
  }

  /**
   * No peer at all - an unelected leader, an empty group - reads as "nobody has been elected yet" rather than as
   * a configuration fault, and the role word the caller passes is what says which.
   */
  @Test
  void anAbsentPeerIsRefusedInTheCallersOwnWords() {
    final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2481");

    assertThat(PeerDialAddress.resolve(raft, null, "leader").refusal()).isEqualTo("the leader is unknown");
    assertThat(PeerDialAddress.resolve(raft, null, "peer").refusal()).isEqualTo("the peer is unknown");
  }

  /** An id no peer answers to resolves to nothing, which is a refusal rather than a best-effort guess. */
  @Test
  void anUnknownPeerIsRefused() {
    final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2481");

    final PeerDialAddress dial = PeerDialAddress.resolve(raft, RaftPeerId.valueOf("nobody_9999"), "peer");

    assertThat(dial.refused()).isTrue();
    assertThat(dial.refusal()).contains("identifies peer nobody_9999");
  }

  /** A hand-built refusal carries no address of either kind, so a caller cannot dial one by mistake. */
  @Test
  void aHandBuiltRefusalCarriesNoAddress() {
    final PeerDialAddress refusal = PeerDialAddress.refuse("this node is the leader");

    assertThat(refusal.refused()).isTrue();
    assertThat(refusal.httpAddress()).isNull();
    assertThat(refusal.httpsAddress()).isNull();
    assertThat(refusal.refusal()).isEqualTo("this node is the leader");
  }

  /**
   * The HTTPS endpoint is asked the same two questions, and it has to be asked <em>separately</em>: it is read
   * from the 5th field of the server list where the HTTP one is read from the 3rd, each with its own derive
   * fallback onto this node's port for that protocol. A cluster that declares distinct {@code http} ports and
   * shares an {@code https} one therefore passes the HTTP check with an HTTPS endpoint that identifies nobody -
   * the exact combination an HTTP-only guard would wave through.
   */
  @Test
  void anAmbiguousHttpsEndpointIsWithheldEvenWhenTheHttpOneIsAccepted() {
    // host:raftPort:httpPort:priority:httpsPort - distinct HTTP ports, one shared HTTPS port.
    final RaftHAServer raft = newDetachedServer(
        "localhost:2434:2480:0:2490,localhost:2435:2481:0:2491,localhost:2436:2482:0:2491");

    final PeerDialAddress dial = PeerDialAddress.resolve(raft, RaftPeerId.valueOf("localhost_2435"), "peer");

    assertThat(dial.refused()).as("the HTTP endpoint identifies the peer, so the dial is allowed").isFalse();
    assertThat(dial.httpAddress()).isEqualTo("localhost:2481");
    assertThat(dial.httpsAddress())
        .as("but two peers answer to localhost:2491, so it identifies neither and must not be dialled")
        .isNull();
  }

  /** A declared HTTPS endpoint of its own is handed over, so the guard does not cost an SSL cluster its TLS. */
  @Test
  void aPeerWithAnHttpsEndpointOfItsOwnKeepsIt() {
    final RaftHAServer raft = newDetachedServer(
        "localhost:2434:2480:0:2490,localhost:2435:2481:0:2491,localhost:2436:2482:0:2492");

    final PeerDialAddress dial = PeerDialAddress.resolve(raft, RaftPeerId.valueOf("localhost_2435"), "peer");

    assertThat(dial.httpAddress()).isEqualTo("localhost:2481");
    assertThat(dial.httpsAddress()).isEqualTo("localhost:2491");
  }

  /** And the HTTPS endpoint that resolves to this node's own listener is withheld for the same reason. */
  @Test
  void anHttpsEndpointThatIsThisNodesOwnIsWithheld() {
    final RaftHAServer raft = newDetachedServer(
        "127.0.0.1:2434:2480:0:2490,localhost:2435:2481:0:2490,localhost:2436:2482:0:2492");

    final PeerDialAddress dial = PeerDialAddress.resolve(raft, RaftPeerId.valueOf("localhost_2435"), "peer");

    assertThat(dial.refused()).isFalse();
    assertThat(dial.httpsAddress())
        .as("localhost:2490 and 127.0.0.1:2490 are one socket, and it is ours")
        .isNull();
  }

  /**
   * A {@link RaftHAServer} built from {@code serverList} with Ratis never started. The node names itself with the
   * {@code prefix_N} convention, so it is the FIRST entry of the list.
   */
  private static RaftHAServer newDetachedServer(final String serverList) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList);

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("ArcadeDB_0");

    return new RaftHAServer(mockServer, config);
  }
}
