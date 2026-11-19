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
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The warning a node logs when two peers resolve to one {@code host:port}, and when it is logged again.
 */
class PeerAddressAmbiguityWarningTest {

  /**
   * A withheld peer-to-peer endpoint says so, once (issue #6267).
   * <p>
   * {@code getUnambiguousPeerHttpAddress} / {@code getUnambiguousPeerHttpsAddress} refuse an address two peers
   * resolve to by answering {@code null}, and each caller then decided for itself whether to say anything - so the
   * misconfiguration was visible only where one happened to log it. Neither pre-existing warning covers it: the
   * derive warnings fire whenever an address is derived <em>at all</em>, which a healthy homogeneous Kubernetes
   * StatefulSet also does, and {@code warnAmbiguousRouting} says exactly this but about the client routing tables
   * of issue #6183.
   * <p>
   * Driven against a real {@link RaftHAServer} built from a server list with Ratis never started - the peer group
   * and the declared addresses are both populated by the constructor, which is all the resolver reads.
   */
  @Nested
  class WithheldEndpointWarning {

    private static final String AMBIGUOUS_HTTP  = "HA HTTP peer endpoints are ambiguous";
    private static final String AMBIGUOUS_HTTPS = "HA HTTPS peer endpoints are ambiguous";

    private CapturingTestLogger log;

    @BeforeEach
    void installLogger() {
      log = CapturingTestLogger.install();
    }

    @AfterEach
    void restoreLogger() {
      log.uninstall();
    }

    /**
     * Two peers deriving to one {@code host:port}: the address is withheld from both, and the operator is told
     * which peers could not be told apart, on which address, and what to write instead.
     */
    // Regression for #6267: a withheld http address is reported once naming the peers that share it
    @Test
    void aWithheldHttpAddressIsReportedOnceNamingThePeersThatShareIt() {
      final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2490,localhost:2436:2490");

      assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("localhost_2435"))).isNull();
      assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("localhost_2436"))).isNull();
      // The resync and verify paths ask on every attempt; a misconfiguration that has not changed since the last
      // attempt must not be re-reported on each one.
      assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("localhost_2435"))).isNull();

      assertThat(log.countFormattedContaining(AMBIGUOUS_HTTP, "localhost_2435", "localhost_2436", "localhost:2490",
          GlobalConfiguration.HA_SERVER_LIST.getKey()))
          .as("one warning, naming both peers, the address they share and the setting that fixes it")
          .isEqualTo(1);
    }

    /**
     * A cluster with two <em>independent</em> collisions gets both named in the one line it is allowed. Reporting
     * only the collision the caller happened to ask about would send an operator to declare two ports and leave the
     * other pair withheld and unmentioned - and since the memory is keyed on the line itself (issue #6297), a
     * partial line would also make the remaining collision look like something already reported.
     */
    // Regression for #6267: one warning names every collision it found
    @Test
    void oneWarningNamesEveryCollisionItFound() {
      final RaftHAServer raft = newDetachedServer(
          "localhost:2434:2480,localhost:2435:2490,localhost:2436:2490,localhost:2437:2491,localhost:2438:2491");

      final Map<RaftPeerId, RaftHAServer.PeerHttpEndpoint> endpoints = raft.getPeerHttpEndpoints();

      assertThat(endpoints.values().stream().filter(RaftHAServer.PeerHttpEndpoint::ambiguous))
          .as("both pairs are withheld; this node's own address is its alone")
          .hasSize(4);
      assertThat(log.countFormattedContaining(AMBIGUOUS_HTTP,
          "localhost_2435, localhost_2436 -> localhost:2490", "localhost_2437, localhost_2438 -> localhost:2491"))
          .as("one line, both collisions")
          .isEqualTo(1);
    }

    /** The address every peer owns is handed out, and nothing is logged: a correct cluster stays quiet. */
    // Regression for #6267: an unambiguous cluster is not warned about
    @Test
    void anUnambiguousClusterIsNotWarnedAbout() {
      final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");

      assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("localhost_2435"))).isEqualTo("localhost:2481");

      assertThat(log.countContaining(AMBIGUOUS_HTTP)).isZero();
      assertThat(log.countContaining(AMBIGUOUS_HTTPS)).isZero();
    }

    /**
     * The HTTPS endpoint has its own latch. A cluster that declares distinct {@code http} ports and shares an
     * {@code https} one passes the HTTP check with every HTTPS endpoint collapsed, and that must still be
     * reported - the HTTP verdict cannot stand in for it, and neither can the HTTP latch.
     */
    // Regression for #6267: the https warning is not muted by the http one
    @Test
    void theHttpsWarningIsNotMutedByTheHttpOne() {
      // host:raftPort:httpPort:priority:httpsPort - distinct HTTP ports, one shared HTTPS port.
      final RaftHAServer raft = newDetachedServer(
          "localhost:2434:2480:0:2490,localhost:2435:2481:0:2491,localhost:2436:2482:0:2491");

      assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("localhost_2435"))).isEqualTo("localhost:2481");
      assertThat(raft.getUnambiguousPeerHttpsAddress(RaftPeerId.valueOf("localhost_2435"))).isNull();

      assertThat(log.countContaining(AMBIGUOUS_HTTP))
          .as("the HTTP endpoints are fine, so nothing is said about them")
          .isZero();
      assertThat(log.countFormattedContaining(AMBIGUOUS_HTTPS, "localhost_2435", "localhost_2436", "localhost:2491"))
          .isEqualTo(1);
    }

    /**
     * The group-wide resolver the cluster-status endpoint uses answers exactly what the per-peer accessors do,
     * for every peer, on both an ambiguous and a correctly declared cluster. It exists only to avoid resolving
     * the group once per peer, so the one thing it must never do is answer differently.
     */
    // Regression for #6267: the group wide endpoints agree with the per peer accessors
    @Test
    void theGroupWideEndpointsAgreeWithThePerPeerAccessors() {
      for (final String serverList : new String[] {
          "localhost:2434:2480,localhost:2435:2490,localhost:2436:2490",  // two peers share an address
          "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482" }) {
        final RaftHAServer raft = newDetachedServer(serverList);

        final Map<RaftPeerId, RaftHAServer.PeerHttpEndpoint> endpoints = raft.getPeerHttpEndpoints();

        assertThat(endpoints).as("every peer of '%s' resolves to something", serverList).hasSize(3);
        for (final Map.Entry<RaftPeerId, RaftHAServer.PeerHttpEndpoint> entry : endpoints.entrySet()) {
          final RaftPeerId peerId = entry.getKey();
          assertThat(entry.getValue().address()).isEqualTo(raft.getPeerHttpAddress(peerId));
          assertThat(entry.getValue().ambiguous())
              .as("peer %s of '%s'", peerId, serverList)
              .isEqualTo(raft.getUnambiguousPeerHttpAddress(peerId) == null);
        }
      }
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
   * Issue #6297: the ambiguity warnings must speak again when the verdict changes.
   * <p>
   * A "log this once" latch is right until the operator acts on it: they read the line, declare the two ports it
   * named, and the next collision - a different pair, or a peer added at runtime onto an address already taken - is
   * the one nobody is told about, because the latch is spent. Keying on the rendered verdict instead of on a boolean
   * (or on a membership-change event) reports every change to the picture and stays quiet whenever the picture is
   * unchanged, whatever the cluster did in between.
   */
  @Nested
  class AmbiguityWarningRearm {

    private static final RaftPeerId PEER_A = RaftPeerId.valueOf("peer_a");
    private static final RaftPeerId PEER_B = RaftPeerId.valueOf("peer_b");
    private static final RaftPeerId PEER_C = RaftPeerId.valueOf("peer_c");
    private static final RaftPeerId PEER_D = RaftPeerId.valueOf("peer_d");

    // Regression for #6297: an unchanged verdict is reported once however often it is asked
    @Test
    void anUnchangedVerdictIsReportedOnceHoweverOftenItIsAsked() {
      final AtomicReference<String> reported = new AtomicReference<>();
      final String verdict = "peer_a, peer_b -> host:2480";

      assertThat(PeerEndpoints.isNewAmbiguityVerdict(reported, verdict)).isTrue();
      assertThat(PeerEndpoints.isNewAmbiguityVerdict(reported, verdict)).isFalse();
      assertThat(PeerEndpoints.isNewAmbiguityVerdict(reported, verdict)).isFalse();
    }

    /** The case a latch cannot serve: the first collision is fixed, a second one appears, and it must be reported. */
    // Regression for #6297: a different collision is reported even after one already was
    @Test
    void aDifferentCollisionIsReportedEvenAfterOneAlreadyWas() {
      final AtomicReference<String> reported = new AtomicReference<>();

      assertThat(PeerEndpoints.isNewAmbiguityVerdict(reported, "peer_a, peer_b -> host:2480")).isTrue();
      assertThat(PeerEndpoints.isNewAmbiguityVerdict(reported, "peer_c, peer_d -> host:2481")).isTrue();
      assertThat(PeerEndpoints.isNewAmbiguityVerdict(reported, "peer_c, peer_d -> host:2481")).isFalse();
    }

    /**
     * A verdict that grows or shrinks - a third peer joining an existing collision, one of a pair leaving - is a
     * different picture and gets its own line.
     */
    // Regression for #6297: a collision that gains or loses a peer is reported again
    @Test
    void aCollisionThatGainsOrLosesAPeerIsReportedAgain() {
      final AtomicReference<String> reported = new AtomicReference<>();

      assertThat(PeerEndpoints.isNewAmbiguityVerdict(reported, "peer_a, peer_b -> host:2480")).isTrue();
      assertThat(PeerEndpoints.isNewAmbiguityVerdict(reported, "peer_a, peer_b, peer_c -> host:2480")).isTrue();
      assertThat(PeerEndpoints.isNewAmbiguityVerdict(reported, "peer_a, peer_b -> host:2480")).isTrue();
    }

    /**
     * A clean pass reports nothing and forgets, so the same misconfiguration reintroduced later is announced instead
     * of being swallowed as "already said".
     */
    // Regression for #6297: a clean pass says nothing and clears the memory
    @Test
    void aCleanPassSaysNothingAndClearsTheMemory() {
      final AtomicReference<String> reported = new AtomicReference<>();
      final String verdict = "peer_a, peer_b -> host:2480";

      assertThat(PeerEndpoints.isNewAmbiguityVerdict(reported, verdict)).isTrue();
      assertThat(PeerEndpoints.isNewAmbiguityVerdict(reported, "")).isFalse();
      assertThat(reported.get()).isNull();
      assertThat(PeerEndpoints.isNewAmbiguityVerdict(reported, verdict))
          .as("the collision came back, so it is news again")
          .isTrue();
    }

    /**
     * The verdict is the memory key, so its rendering has to be stable: peers sorted within an address and addresses
     * sorted between them, because Ratis hands the group over in whatever order it holds it. Two nodes looking at the
     * same misconfiguration must produce the same string, or every pass would look like a change.
     */
    // Regression for #6297: the verdict is rendered in a stable order whatever order the peers arrive in
    @Test
    void theVerdictIsRenderedInAStableOrderWhateverOrderThePeersArriveIn() {
      final String[] addresses = { "host:2481", "host:2480", "host:2481", "host:2480" };
      final boolean[] fromConfig = new boolean[4];
      final RaftPeerId[] owners = { PEER_D, PEER_B, PEER_C, PEER_A };

      assertThat(PeerEndpoints.describeAmbiguity(addresses, fromConfig, owners, 4,
          PeerEndpoints.claimsByAddress(addresses, fromConfig, 4)))
          .isEqualTo("peer_a, peer_b -> host:2480; peer_c, peer_d -> host:2481");
    }

    /** Declared beats derived: the peer that stated the address owns it, so it is not among the withheld. */
    // Regression for #6297: a declared address is not named as withheld
    @Test
    void aDeclaredAddressIsNotNamedAsWithheld() {
      final String[] addresses = { "host:2480", "host:2480", "host:2482" };
      final boolean[] fromConfig = { true, false, false };
      final RaftPeerId[] owners = { PEER_A, PEER_B, PEER_C };

      assertThat(PeerEndpoints.describeAmbiguity(addresses, fromConfig, owners, 3,
          PeerEndpoints.claimsByAddress(addresses, fromConfig, 3)))
          .isEqualTo("peer_b -> host:2480");
    }

    /** A view in which every address identifies its own peer renders to nothing, which is what keeps the log quiet. */
    // Regression for #6297: a clean view renders to the empty verdict
    @Test
    void aCleanViewRendersToTheEmptyVerdict() {
      final String[] addresses = { "host:2480", "host:2481", "host:2482" };
      final boolean[] fromConfig = new boolean[3];
      final RaftPeerId[] owners = { PEER_A, PEER_B, PEER_C };

      assertThat(PeerEndpoints.describeAmbiguity(addresses, fromConfig, owners, 3,
          PeerEndpoints.claimsByAddress(addresses, fromConfig, 3))).isEmpty();
    }

    /**
     * The wiring, not the decision function: the memory is only cleared by a pass that reaches it with nothing to
     * say, so a caller that consults the warning ONLY when the current view is already ambiguous can never clear it.
     * The verdict then stays behind forever and the same collision, reintroduced after the operator fixes it, is
     * swallowed as "already reported" - which is the case issue #6297 exists to fix, defeated by its own caller.
     * Both warnings are therefore handed every resolved view, and these two tests are what says so.
     */
    // Regression for #6297: the routing caller hands over a clean pass too
    @Test
    void theRoutingCallerHandsOverACleanPassToo() {
      final CapturingTestLogger log = CapturingTestLogger.install();
      try {
        // Declared, distinct gRPC ports: the routing view this server resolves is unambiguous.
        final RaftHAServer raft = newDetachedServer(
            "localhost:{raft:2434,grpc:5434},localhost:{raft:2435,grpc:5435},localhost:{raft:2436,grpc:5436}");

        warnRouting(raft, COLLIDING);
        assertThat(log.countFormattedContaining(AMBIGUOUS_ROUTING)).as("a collision is reported").isEqualTo(1);
        warnRouting(raft, COLLIDING);
        assertThat(log.countFormattedContaining(AMBIGUOUS_ROUTING)).as("unchanged, so nothing new").isEqualTo(1);

        // The real caller, on a healthy cluster. Nothing is logged - and that is not what is under test: what is
        // under test is that it REACHED the warning at all, which is the only thing that clears the memory.
        assertThat(raft.routingTableFor(GRPC, RaftPeerId.valueOf("localhost_2434"))).isNotNull();
        assertThat(log.countFormattedContaining(AMBIGUOUS_ROUTING)).as("a clean pass says nothing").isEqualTo(1);

        warnRouting(raft, COLLIDING);
        assertThat(log.countFormattedContaining(AMBIGUOUS_ROUTING))
            .as("the clean pass cleared the memory, so the collision coming back is news again")
            .isEqualTo(2);
      } finally {
        log.uninstall();
      }
    }

    /**
     * The same for the peer-to-peer endpoint, driven through the public accessor. Over HTTPS on purpose:
     * {@code getPeerHttpEndpoints} hands over every HTTP view already, so the HTTP memory was being cleared by
     * accident of that one caller - the HTTPS memory is reached only through the per-peer accessor, which returned
     * before the warning whenever the peer it was asked about was fine, and so had nothing clearing it at all.
     */
    // Regression for #6297: the peer address caller hands over a clean pass too
    @Test
    void thePeerAddressCallerHandsOverACleanPassToo() {
      final CapturingTestLogger log = CapturingTestLogger.install();
      try {
        // host:raftPort:httpPort:priority:httpsPort - distinct HTTPS ports, so the HTTPS view is unambiguous.
        final RaftHAServer raft = newDetachedServer(
            "localhost:2434:2480:0:2490,localhost:2435:2481:0:2491,localhost:2436:2482:0:2492");

        warnHttps(raft, COLLIDING);
        assertThat(log.countFormattedContaining(AMBIGUOUS_HTTPS)).isEqualTo(1);
        warnHttps(raft, COLLIDING);
        assertThat(log.countFormattedContaining(AMBIGUOUS_HTTPS)).as("unchanged, so nothing new").isEqualTo(1);

        assertThat(raft.getUnambiguousPeerHttpsAddress(RaftPeerId.valueOf("localhost_2435")))
            .isEqualTo("localhost:2491");
        assertThat(log.countFormattedContaining(AMBIGUOUS_HTTPS)).as("a clean pass says nothing").isEqualTo(1);

        warnHttps(raft, COLLIDING);
        assertThat(log.countFormattedContaining(AMBIGUOUS_HTTPS))
            .as("the clean pass cleared the memory, so the collision coming back is news again")
            .isEqualTo(2);
      } finally {
        log.uninstall();
      }
    }

    private static void warnRouting(final RaftHAServer raft, final String[] addresses) {
      raft.warnAmbiguousRouting(GRPC, addresses, DERIVED, THREE_PEERS, 3,
          PeerEndpoints.claimsByAddress(addresses, DERIVED, 3));
    }

    private static void warnHttps(final RaftHAServer raft, final String[] addresses) {
      raft.warnAmbiguousPeerAddress(true, addresses, DERIVED, THREE_PEERS, 3,
          PeerEndpoints.claimsByAddress(addresses, DERIVED, 3));
    }

    private static final HAServerPlugin.ROUTING_PROTOCOL GRPC = HAServerPlugin.ROUTING_PROTOCOL.GRPC;

    /** Peers B and C share an address; A owns its own. */
    private static final String[]     COLLIDING   = { "host:2480", "host:2490", "host:2490" };
    private static final boolean[]    DERIVED     = new boolean[3];
    private static final RaftPeerId[] THREE_PEERS = { PEER_A, PEER_B, PEER_C };

    private static final String AMBIGUOUS_ROUTING = "HA GRPC routing is ambiguous";
    private static final String AMBIGUOUS_HTTPS   = "HA HTTPS peer endpoints are ambiguous";

    /**
     * A {@link RaftHAServer} built from {@code serverList} with Ratis never started: the peer group and the declared
     * addresses are both populated by the constructor, which is all these paths read. The node names itself with the
     * {@code prefix_N} convention, so it is the FIRST entry of the list.
     */
    private static RaftHAServer newDetachedServer(final String serverList) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList);

      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");

      return new RaftHAServer(arcadeServer, config);
    }
  }
}
