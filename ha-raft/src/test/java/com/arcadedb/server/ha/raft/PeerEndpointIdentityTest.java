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
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static com.arcadedb.server.HAServerPlugin.ROUTING_PROTOCOL.GRPC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Which endpoint a peer is reachable on and whether this node may dial it: routing-table ambiguity,
 * unambiguous peer HTTP address resolution, loopback spellings, dial-address refusals, the leader HTTPS endpoint
 * and server-list address validation.
 */
class PeerEndpointIdentityTest {

  /**
   * Two peers cannot both be listening on one {@code host:port} for one protocol, so a routing view that maps two
   * of them to the same address has resolved at most one of them and cannot say which (issue #6183). These are the
   * cases {@link PeerEndpoints#selectUnambiguousRouting} has to separate, exercised directly because the shape that
   * matters - a follower advertising itself as the leader - is what an in-process cluster produces by default and
   * what a heterogeneous one never can.
   * <p>
   * Index 0 is the writer throughout, matching the caller.
   */
  @Nested
  class AmbiguousRoutingTable {

    // Regression for #6183: distinct addresses are all advertised
    @Test
    void distinctAddressesAreAllAdvertised() {
      final HAServerPlugin.RoutingTable table = select(
          new String[] { "db0:50051", "db1:50051", "db2:50051" },
          new boolean[] { false, false, false });

      assertThat(table).isNotNull();
      assertThat(table.protocol()).isEqualTo(GRPC);
      assertThat(table.writer()).isEqualTo("db0:50051");
      assertThat(table.readers()).containsExactly("db1:50051", "db2:50051");
    }

    /**
     * The reachable defect: every node on one host, nothing declared, so every peer derives to this node's own
     * port. Advertising that would hand a caller the address of the node that just refused it.
     */
    // Regression for #6183: a derived view that cannot tell peers apart advertises nothing
    @Test
    void aDerivedViewThatCannotTellPeersApartAdvertisesNothing() {
      final HAServerPlugin.RoutingTable table = select(
          new String[] { "localhost:50051", "localhost:50051", "localhost:50051" },
          new boolean[] { false, false, false });

      assertThat(table).isNull();
    }

    /** A declared address is a statement of fact; a derived one is a guess. The collision only indicts the guess. */
    // Regression for #6183: a declared writer outranks a colliding derived follower
    @Test
    void aDeclaredWriterOutranksACollidingDerivedFollower() {
      final HAServerPlugin.RoutingTable table = select(
          new String[] { "localhost:50051", "localhost:50051" },
          new boolean[] { true, false });

      assertThat(table).isNotNull();
      assertThat(table.writer()).isEqualTo("localhost:50051");
      assertThat(table.readers()).as("the follower's derived address is the one proven wrong").isEmpty();
    }

    /** Mirror image: the leader's address is the guess, so there is no writer left to advertise. */
    // Regression for #6183: a derived writer colliding with a declared follower suppresses the table
    @Test
    void aDerivedWriterCollidingWithADeclaredFollowerSuppressesTheTable() {
      final HAServerPlugin.RoutingTable table = select(
          new String[] { "localhost:50051", "localhost:50051" },
          new boolean[] { false, true });

      assertThat(table).isNull();
    }

    /** Two peers declared the same endpoint: a configuration error with no defensible winner. */
    // Regression for #6183: two declared claims on one address cancel each other out
    @Test
    void twoDeclaredClaimsOnOneAddressCancelEachOtherOut() {
      final HAServerPlugin.RoutingTable table = select(
          new String[] { "db0:50051", "db0:50051" },
          new boolean[] { true, true });

      assertThat(table).isNull();
    }

    /** Ambiguity among followers costs only those followers: the writer is still worth advertising. */
    // Regression for #6183: an unambiguous writer survives ambiguous followers
    @Test
    void anUnambiguousWriterSurvivesAmbiguousFollowers() {
      final HAServerPlugin.RoutingTable table = select(
          new String[] { "db0:50051", "db1:50051", "db1:50051", "db2:50051" },
          new boolean[] { true, false, false, true });

      assertThat(table).isNotNull();
      assertThat(table.writer()).isEqualTo("db0:50051");
      assertThat(table.readers()).containsExactly("db2:50051");
    }

    /** A single-node cluster has nothing to be ambiguous with. */
    // Regression for #6183: a lone writer is never ambiguous
    @Test
    void aLoneWriterIsNeverAmbiguous() {
      final HAServerPlugin.RoutingTable table = select(new String[] { "db0:50051" }, new boolean[] { false });

      assertThat(table).isNotNull();
      assertThat(table.writer()).isEqualTo("db0:50051");
      assertThat(table.readers()).isEmpty();
    }

    private static HAServerPlugin.RoutingTable select(final String[] addresses, final boolean[] fromConfig) {
      return PeerEndpoints.selectUnambiguousRouting(GRPC, addresses, fromConfig, addresses.length);
    }
  }

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
   * It is the same rule {@link PeerEndpoints#selectUnambiguousRouting} applies to client routing tables (#6183),
   * now sharing its claim-counting core with it.
   */
  @Nested
  class UnambiguousPeerHttpAddress {

    /** Distinct declared HTTP ports: every peer owns its address, so each one is handed out. */
    // Regression for #6202: an address only one peer claims is returned
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
    // Regression for #6202: an address two peers claim is withheld
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
    // Regression for #6202: an unknown peer resolves to nothing
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

      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");

      return new RaftHAServer(arcadeServer, config);
    }
  }

  /**
   * A single-machine cluster is the commonest way to meet the address the self-forward guard exists to catch
   * (issue #6191), and it is also the one place where the same socket gets written two ways: one entry of
   * {@code HA_SERVER_LIST} says {@code localhost}, the next says {@code 127.0.0.1}. The guard compares text, so
   * before issue #6204 those two read as different nodes and the guard stayed silent on exactly the deployment
   * that needs it.
   * <p>
   * The equivalence stops at the spellings of <em>the</em> loopback address. A distinct loopback address
   * ({@code 127.0.0.2}), a wildcard bind ({@code 0.0.0.0}) and a declared host name are all still different
   * endpoints: those can genuinely name another node, and answering "that is me" for one of them would refuse a
   * write that had to be forwarded.
   */
  @Nested
  class LoopbackEndpoint {

    // Regression for #6204: localhost and the ipv4 loopback are the same endpoint
    @Test
    void localhostAndTheIpv4LoopbackAreTheSameEndpoint() {
      assertThat(RaftHAServer.isSameHttpEndpoint("localhost:2480", "127.0.0.1:2480")).isTrue();
      assertThat(RaftHAServer.isSameHttpEndpoint("127.0.0.1:2480", "localhost:2480")).isTrue();
    }

    // Regression for #6204: the ipv6 loopback is the same endpoint too
    @Test
    void theIpv6LoopbackIsTheSameEndpointToo() {
      assertThat(RaftHAServer.isSameHttpEndpoint("localhost:2480", "[::1]:2480")).isTrue();
      assertThat(RaftHAServer.isSameHttpEndpoint("127.0.0.1:2480", "[0:0:0:0:0:0:0:1]:2480")).isTrue();
      // Unbracketed, which is how a hand-written server list tends to spell it: the last colon is still the
      // port separator, so the host is read as "::1" and not as a truncated address.
      assertThat(RaftHAServer.isSameHttpEndpoint("localhost:2480", "::1:2480")).isTrue();
    }

    /** Host names are case-insensitive on both sides of the equivalence, not only when they match textually. */
    // Regression for #6204: case is still ignored
    @Test
    void caseIsStillIgnored() {
      assertThat(RaftHAServer.isSameHttpEndpoint("LOCALHOST:2480", "127.0.0.1:2480")).isTrue();
    }

    /**
     * The port is what tells apart the nodes of a same-host cluster, and it is the reason the equivalence is
     * safe: two of them cannot be listening on one port of one host, so equal port plus loopback on both sides
     * is the same socket.
     */
    // Regression for #6204: the port still decides
    @Test
    void thePortStillDecides() {
      assertThat(RaftHAServer.isSameHttpEndpoint("localhost:2480", "127.0.0.1:2481")).isFalse();
      assertThat(RaftHAServer.isSameHttpEndpoint("[::1]:2480", "localhost:2481")).isFalse();
    }

    /**
     * {@code 127.0.0.2} is a loopback address but not the same one: binding two nodes to different addresses of
     * the loopback range with a shared port is a legitimate way to run a cluster on one machine, and unifying
     * them would refuse the forwarding between them.
     */
    // Regression for #6204: another loopback address is a different socket
    @Test
    void anotherLoopbackAddressIsADifferentSocket() {
      assertThat(RaftHAServer.isSameHttpEndpoint("127.0.0.1:2480", "127.0.0.2:2480")).isFalse();
    }

    /**
     * A wildcard bind accepts connections on every local interface, so it is tempting to read it as "any
     * address is mine". It is not: two nodes on two machines both bound to {@code 0.0.0.0:2480} would then
     * refuse to forward to each other, turning a working cluster into a read-only one.
     */
    // Regression for #6204: the wildcard bind is not every peer
    @Test
    void theWildcardBindIsNotEveryPeer() {
      assertThat(RaftHAServer.isSameHttpEndpoint("0.0.0.0:2480", "db1.example.com:2480")).isFalse();
      assertThat(RaftHAServer.isSameHttpEndpoint("0.0.0.0:2480", "127.0.0.1:2480")).isFalse();
    }

    /** A declared host name is a statement about which node owns which port, and it is not overruled here. */
    // Regression for #6204: a declared host name is not unified with an ip
    @Test
    void aDeclaredHostNameIsNotUnifiedWithAnIp() {
      assertThat(RaftHAServer.isSameHttpEndpoint("db0.example.com:2480", "127.0.0.1:2480")).isFalse();
    }

    /** Without a port there is no endpoint to compare, so the answer falls back to the text. */
    // Regression for #6204: a bare host is not an endpoint
    @Test
    void aBareHostIsNotAnEndpoint() {
      assertThat(RaftHAServer.isSameHttpEndpoint("localhost", "127.0.0.1")).isFalse();
      assertThat(RaftHAServer.isSameHttpEndpoint("localhost", "localhost")).isTrue();
    }

    /**
     * A trailing colon leaves an empty port on both sides, and an empty port matches an empty port - which would
     * satisfy the "same port" condition the whole equivalence rests on without there being a port at all (issue
     * #6212). A hand-written server list is exactly where a trailing colon comes from.
     */
    // Regression for #6204: an empty port is not a port
    @Test
    void anEmptyPortIsNotAPort() {
      assertThat(RaftHAServer.isSameHttpEndpoint("localhost:", "127.0.0.1:")).isFalse();
      assertThat(RaftHAServer.isSameHttpEndpoint("localhost:", "localhost:2480")).isFalse();
      // Identical text stays a match: there the caller is comparing one address with itself, whatever it is.
      assertThat(RaftHAServer.isSameHttpEndpoint("localhost:", "localhost:")).isTrue();
    }

    // Regression for #6204: the loopback spellings are recognised on their own
    @Test
    void theLoopbackSpellingsAreRecognisedOnTheirOwn() {
      assertThat(LoopbackHosts.isLoopback("localhost")).isTrue();
      assertThat(LoopbackHosts.isLoopback("LocalHost")).isTrue();
      assertThat(LoopbackHosts.isLoopback("127.0.0.1")).isTrue();
      assertThat(LoopbackHosts.isLoopback("::1")).isTrue();
      assertThat(LoopbackHosts.isLoopback("[::1]")).isTrue();
      assertThat(LoopbackHosts.isLoopback("0:0:0:0:0:0:0:1")).isTrue();

      assertThat(LoopbackHosts.isLoopback("127.0.0.2")).isFalse();
      assertThat(LoopbackHosts.isLoopback("0.0.0.0")).isFalse();
      assertThat(LoopbackHosts.isLoopback("db0")).isFalse();
      assertThat(LoopbackHosts.isLoopback("[")).isFalse();
      assertThat(LoopbackHosts.isLoopback("")).isFalse();
      assertThat(LoopbackHosts.isLoopback(null)).isFalse();
    }
  }

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
   */
  @Nested
  class PeerDialAddressResolution {

    /** Distinct declared HTTP ports: every peer owns its address, so it is handed out to be dialled. */
    // Regression for #6221: a peer with an address of its own is dialled
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
    // Regression for #6221: a peer whose address is shared with another is refused
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
    // Regression for #6221: a peer whose unambiguous address is this nodes own is refused
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
    // Regression for #6221: this node is refused as a peer of itself
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
    // Regression for #6221: an absent peer is refused in the callers own words
    @Test
    void anAbsentPeerIsRefusedInTheCallersOwnWords() {
      final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2481");

      assertThat(PeerDialAddress.resolve(raft, null, "leader").refusal()).isEqualTo("the leader is unknown");
      assertThat(PeerDialAddress.resolve(raft, null, "peer").refusal()).isEqualTo("the peer is unknown");
    }

    /** An id no peer answers to resolves to nothing, which is a refusal rather than a best-effort guess. */
    // Regression for #6221: an unknown peer is refused
    @Test
    void anUnknownPeerIsRefused() {
      final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2481");

      final PeerDialAddress dial = PeerDialAddress.resolve(raft, RaftPeerId.valueOf("nobody_9999"), "peer");

      assertThat(dial.refused()).isTrue();
      assertThat(dial.refusal()).contains("identifies peer nobody_9999");
    }

    /** A hand-built refusal carries no address of either kind, so a caller cannot dial one by mistake. */
    // Regression for #6221: a hand built refusal carries no address
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
    // Regression for #6221: an ambiguous https endpoint is withheld even when the http one is accepted
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
    // Regression for #6221: a peer with an https endpoint of its own keeps it
    @Test
    void aPeerWithAnHttpsEndpointOfItsOwnKeepsIt() {
      final RaftHAServer raft = newDetachedServer(
          "localhost:2434:2480:0:2490,localhost:2435:2481:0:2491,localhost:2436:2482:0:2492");

      final PeerDialAddress dial = PeerDialAddress.resolve(raft, RaftPeerId.valueOf("localhost_2435"), "peer");

      assertThat(dial.httpAddress()).isEqualTo("localhost:2481");
      assertThat(dial.httpsAddress()).isEqualTo("localhost:2491");
    }

    /** And the HTTPS endpoint that resolves to this node's own listener is withheld for the same reason. */
    // Regression for #6221: an https endpoint that is this nodes own is withheld
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

      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");

      return new RaftHAServer(arcadeServer, config);
    }
  }

  /**
   * Regression for issue #7508: the endpoint a forward to the leader is dialled on.
   * <p>
   * {@code RaftHAServer.getLeaderHttpsAddress()} is what tells the shared {@code LeaderDial} decision that the cluster
   * has an encrypted endpoint for the leader. It has to withhold one in three cases, each of which sends the caller
   * back to the plain-HTTP address - the listener {@code HttpServer.buildUndertowServer} always binds - rather than
   * refusing the forward outright. That is the same withhold-rather-than-refuse rule
   * {@link PeerDialAddress#encryptedEndpointOf} applies to every other peer-to-peer dial (issue #6221).
   */
  @Nested
  class LeaderHttpsEndpoint {

    private static final String LEADER_HTTPS = "leader.example.com:2490";
    private static final String LOCAL_HTTPS  = "follower.example.com:2490";

    // Regression for #7508: the leader https endpoint is offered on an ssl cluster that resolves one
    @Test
    void theLeaderHttpsEndpointIsOfferedOnAnSslClusterThatResolvesOne() {
      assertThat(RaftHAServer.preferredLeaderHttpsAddress(true, LEADER_HTTPS, LOCAL_HTTPS))
          .isEqualTo(LEADER_HTTPS);
    }

    // Regression for #7508: nothing is offered when ssl is off
    @Test
    void nothingIsOfferedWhenSslIsOff() {
      // The 5th field of arcadedb.ha.serverList can name an https port on a cluster that never enabled SSL, so the
      // address resolving is not on its own a reason to dial it.
      assertThat(RaftHAServer.preferredLeaderHttpsAddress(false, LEADER_HTTPS, LOCAL_HTTPS)).isNull();
    }

    // Regression for #7508: nothing is offered when no https endpoint resolves for the leader
    @Test
    void nothingIsOfferedWhenNoHttpsEndpointResolvesForTheLeader() {
      assertThat(RaftHAServer.preferredLeaderHttpsAddress(true, null, LOCAL_HTTPS)).isNull();
    }

    // Regression for #7508: nothing is offered when the resolved https endpoint is this nodes own
    @Test
    void nothingIsOfferedWhenTheResolvedHttpsEndpointIsThisNodesOwn() {
      // What the derive fallback produces on a cluster whose peers share a host and declare no 'https' port: every
      // peer's HTTPS endpoint collapses onto this node's own, and dialling it comes straight back here. The caller
      // cannot catch this itself - isOwnHttpAddress compares against the HTTP listener, not the HTTPS one.
      //
      // It is also the state a node that has JUST BECOME the leader is in: getLeaderHttpsAddress() then resolves
      // getLeaderId() and localPeerId to the same peer, so both arguments are one resolve() of one id. That is why a
      // write arriving during that window cannot be refused for "the cluster requires TLS and I cannot reach it" -
      // there is no endpoint to require (PR #7554 review).
      assertThat(RaftHAServer.preferredLeaderHttpsAddress(true, LOCAL_HTTPS, LOCAL_HTTPS)).isNull();

      // Loopback spelled two ways on one port is one socket, which isSameHttpEndpoint already knows (issue #6204).
      assertThat(RaftHAServer.preferredLeaderHttpsAddress(true, "127.0.0.1:2490", "localhost:2490")).isNull();
    }

    // Regression for #7508: the endpoint is still offered when this node has no https address to compare against
    @Test
    void theEndpointIsStillOfferedWhenThisNodeHasNoHttpsAddressToCompareAgainst() {
      // getLocalHttpsAddress() degrades to null while the local HTTPS listener is coming up. The self-check is then
      // inactive, exactly as PeerDialAddress.resolve leaves the HTTP one, rather than withholding every endpoint.
      assertThat(RaftHAServer.preferredLeaderHttpsAddress(true, LEADER_HTTPS, null)).isEqualTo(LEADER_HTTPS);
    }
  }

  /**
   * Issue #10761: {@code parsePeerList} never applied {@code validatePeerAddress}, the legal-address rule of its own class,
   * and split the positional form on every colon. A bracketed IPv6 entry parsed into a Raft peer at {@code [:} while the
   * gRPC allowlist resolved {@code ::1} from the same line, and a non-numeric or out-of-range port became a peer address.
   * <p>
   * The census below is over every entry shape the javadoc of {@code parsePeerList} documents, each with a DNS name, an
   * IPv4 and a bracketed IPv6 host: the host the allowlist derives equals the host of the Raft address, and every address
   * built passes {@code validatePeerAddress}.
   */
  @Nested
  class PeerListAddressValidation {

    /** Every documented shape, with {@code %s} for the host. */
    private static final List<String> SHAPES = List.of(
        "%s",
        "%s:2434",
        "%s:2434:2480",
        "%s:2434:2480:5",
        "%s:2434:2480:5:2490",
        "%s:{raft:2434}",
        "%s:{raft:2434,http:2480,https:2490,bolt:7687,grpc:50051,priority:10}",
        "%s:{}",
        "frankfurt@%s:2434:2480",
        "frankfurt@%s:{raft:2434,http:2480}");

    private static final List<String> HOSTS = List.of("arcadedb-0", "10.0.0.1", "[2001:db8::1]", "[::1]");

    // Regression for #10761: the allowlist and the raft peer derive one host from every documented shape
    @Test
    void theAllowlistAndTheRaftPeerDeriveOneHostFromEveryDocumentedShape() {
      final List<String> expectedEntries = List.of(
          "arcadedb-0",
          "arcadedb-0:2434",
          "arcadedb-0:2434:2480",
          "arcadedb-0:2434:2480:5",
          "arcadedb-0:2434:2480:5:2490",
          "arcadedb-0:{raft:2434}",
          "arcadedb-0:{raft:2434,http:2480,https:2490,bolt:7687,grpc:50051,priority:10}",
          "arcadedb-0:{}",
          "frankfurt@arcadedb-0:2434:2480",
          "frankfurt@arcadedb-0:{raft:2434,http:2480}",
          "10.0.0.1",
          "10.0.0.1:2434",
          "10.0.0.1:2434:2480",
          "10.0.0.1:2434:2480:5",
          "10.0.0.1:2434:2480:5:2490",
          "10.0.0.1:{raft:2434}",
          "10.0.0.1:{raft:2434,http:2480,https:2490,bolt:7687,grpc:50051,priority:10}",
          "10.0.0.1:{}",
          "frankfurt@10.0.0.1:2434:2480",
          "frankfurt@10.0.0.1:{raft:2434,http:2480}",
          "[2001:db8::1]",
          "[2001:db8::1]:2434",
          "[2001:db8::1]:2434:2480",
          "[2001:db8::1]:2434:2480:5",
          "[2001:db8::1]:2434:2480:5:2490",
          "[2001:db8::1]:{raft:2434}",
          "[2001:db8::1]:{raft:2434,http:2480,https:2490,bolt:7687,grpc:50051,priority:10}",
          "[2001:db8::1]:{}",
          "frankfurt@[2001:db8::1]:2434:2480",
          "frankfurt@[2001:db8::1]:{raft:2434,http:2480}",
          "[::1]",
          "[::1]:2434",
          "[::1]:2434:2480",
          "[::1]:2434:2480:5",
          "[::1]:2434:2480:5:2490",
          "[::1]:{raft:2434}",
          "[::1]:{raft:2434,http:2480,https:2490,bolt:7687,grpc:50051,priority:10}",
          "[::1]:{}",
          "frankfurt@[::1]:2434:2480",
          "frankfurt@[::1]:{raft:2434,http:2480}");
      final List<String> checked = new ArrayList<>();
      for (final String host : HOSTS)
        for (final String shape : SHAPES) {
          final String entry = shape.formatted(host);
          final RaftPeerAddressResolver.ParsedPeerList parsed = RaftPeerAddressResolver.parsePeerList(entry, 2424);
          assertThat(parsed.peers()).as(entry).hasSize(1);
          final RaftPeer peer = parsed.peers().getFirst();

          final String raftHost = PeerEndpoints.extractHost(peer.getAddress());
          assertThat(raftHost).as("the host of the Raft address of '%s'", entry).isEqualTo(host);
          assertThat(PeerAddressAllowlistFilter.extractPeerHosts(entry)).as("the allowlist host of '%s'", entry)
              .containsExactly(RaftPeerAddressResolver.unbracketed(raftHost));

          assertLegal(entry, peer.getAddress());
          assertLegal(entry, parsed.httpAddresses().get(peer.getId()));
          assertLegal(entry, parsed.httpsAddresses().get(peer.getId()));
          assertLegal(entry, parsed.boltAddresses().get(peer.getId()));
          assertLegal(entry, parsed.grpcAddresses().get(peer.getId()));
          checked.add(entry);
        }
      assertThat(checked).containsExactlyInAnyOrderElementsOf(expectedEntries);
    }

    /** The two entries of the report, verbatim: the positional IPv6 form keeps its host whole in every field. */
    // Regression for #10761: a positional ipv6 entry keeps its host whole
    @Test
    void aPositionalIpv6EntryKeepsItsHostWhole() {
      final RaftPeerAddressResolver.ParsedPeerList parsed = RaftPeerAddressResolver.parsePeerList("[::1]:2434:2480", 2424);
      final RaftPeer peer = parsed.peers().getFirst();

      assertThat(peer.getAddress()).isEqualTo("[::1]:2434");
      assertThat(peer.getPriority()).isZero();
      assertThat(parsed.httpAddresses().get(peer.getId())).isEqualTo("[::1]:2480");
      assertThat(parsed.httpsAddresses()).isEmpty();
      // The id still follows the one rule every node applies to the Raft address
      assertThat(peer.getId()).isEqualTo(RaftPeerId.valueOf(RaftPeerAddressResolver.peerIdForAddress("[::1]:2434")));

      final RaftPeer second = RaftPeerAddressResolver.parsePeerList("[2001:db8::1]:2434", 2424).peers().getFirst();
      assertThat(second.getAddress()).as("no longer 'Invalid priority value 1]'").isEqualTo("[2001:db8::1]:2434");
    }

    // Regression for #10761: a valid positional port is kept as written so the derived peer id does not change
    @Test
    void aValidPositionalPortIsKeptAsWrittenSoTheDerivedPeerIdDoesNotChange() {
      final RaftPeer peer = RaftPeerAddressResolver.parsePeerList("host:02434:2480", 2424).peers().getFirst();

      assertThat(peer.getAddress()).isEqualTo("host:02434");
      assertThat(peer.getId().toString()).isEqualTo("host_02434");
    }

    // Regression for #10761: an illegal port is refused naming the field
    @ParameterizedTest
    @ValueSource(strings = { "host:abc", "host:-1:2480", "host:0:2480", "host:2434:99999", "host:2434:notaport",
        "host:2434:2480:0:0", "host:2434:", "host:{raft:0}", "host:{http:70000}", "host:{bolt:x}", "fe80::1:2434" })
    void anIllegalPortIsRefusedNamingTheField(final String entry) {
      assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList(entry, 2424))
          .isInstanceOf(ServerException.class)
          .hasMessageContaining(entry)
          .hasMessageContaining(" port");
    }

    // Regression for #10761: a malformed host is refused
    @ParameterizedTest
    @ValueSource(strings = { "[::1", "[]:2434", "[::1]2434", "fe80::1:{raft:2434}", ":2434", "[::1]:2434:{http:2480}",
        "host:2434:{http:2480}" })
    void aMalformedHostIsRefused(final String entry) {
      assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList(entry, 2424)).isInstanceOf(ServerException.class);
      assertThat(PeerAddressAllowlistFilter.extractPeerHosts(entry)).as("an entry that names no peer resolves nothing")
          .isEmpty();
    }

    // Regression for #10761: an unbracketed ipv6 entry is refused with the bracket hint
    @Test
    void anUnbracketedIpv6EntryIsRefusedWithTheBracketHint() {
      assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("fe80::1:2434", 2424))
          .isInstanceOf(ServerException.class)
          .hasMessageContaining("written in brackets");
    }

    // Regression for #10761: a dns suffix is not applied to a bracketed host
    @Test
    void aDnsSuffixIsNotAppliedToABracketedHost() {
      final RaftPeer peer = RaftPeerAddressResolver.parsePeerList("[fd00::5]:2434:2480", 2424, ".svc.cluster.local").peers()
          .getFirst();

      assertThat(peer.getAddress()).isEqualTo("[fd00::5]:2434");
    }

    private static void assertLegal(final String entry, final String address) {
      if (address != null)
        assertThatCode(() -> RaftPeerAddressResolver.validatePeerAddress(address))
            .as("'%s' built from '%s'", address, entry).doesNotThrowAnyException();
    }
  }
}
