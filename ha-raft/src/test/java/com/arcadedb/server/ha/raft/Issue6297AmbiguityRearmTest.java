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

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #6297: the ambiguity warnings must speak again when the verdict changes.
 * <p>
 * A "log this once" latch is right until the operator acts on it: they read the line, declare the two ports it
 * named, and the next collision - a different pair, or a peer added at runtime onto an address already taken - is
 * the one nobody is told about, because the latch is spent. Keying on the rendered verdict instead of on a boolean
 * (or on a membership-change event) reports every change to the picture and stays quiet whenever the picture is
 * unchanged, whatever the cluster did in between.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6297AmbiguityRearmTest {

  private static final RaftPeerId PEER_A = RaftPeerId.valueOf("peer_a");
  private static final RaftPeerId PEER_B = RaftPeerId.valueOf("peer_b");
  private static final RaftPeerId PEER_C = RaftPeerId.valueOf("peer_c");
  private static final RaftPeerId PEER_D = RaftPeerId.valueOf("peer_d");

  @Test
  void anUnchangedVerdictIsReportedOnceHoweverOftenItIsAsked() {
    final AtomicReference<String> reported = new AtomicReference<>();
    final String verdict = "peer_a, peer_b -> host:2480";

    assertThat(RaftHAServer.isNewAmbiguityVerdict(reported, verdict)).isTrue();
    assertThat(RaftHAServer.isNewAmbiguityVerdict(reported, verdict)).isFalse();
    assertThat(RaftHAServer.isNewAmbiguityVerdict(reported, verdict)).isFalse();
  }

  /** The case a latch cannot serve: the first collision is fixed, a second one appears, and it must be reported. */
  @Test
  void aDifferentCollisionIsReportedEvenAfterOneAlreadyWas() {
    final AtomicReference<String> reported = new AtomicReference<>();

    assertThat(RaftHAServer.isNewAmbiguityVerdict(reported, "peer_a, peer_b -> host:2480")).isTrue();
    assertThat(RaftHAServer.isNewAmbiguityVerdict(reported, "peer_c, peer_d -> host:2481")).isTrue();
    assertThat(RaftHAServer.isNewAmbiguityVerdict(reported, "peer_c, peer_d -> host:2481")).isFalse();
  }

  /**
   * A verdict that grows or shrinks - a third peer joining an existing collision, one of a pair leaving - is a
   * different picture and gets its own line.
   */
  @Test
  void aCollisionThatGainsOrLosesAPeerIsReportedAgain() {
    final AtomicReference<String> reported = new AtomicReference<>();

    assertThat(RaftHAServer.isNewAmbiguityVerdict(reported, "peer_a, peer_b -> host:2480")).isTrue();
    assertThat(RaftHAServer.isNewAmbiguityVerdict(reported, "peer_a, peer_b, peer_c -> host:2480")).isTrue();
    assertThat(RaftHAServer.isNewAmbiguityVerdict(reported, "peer_a, peer_b -> host:2480")).isTrue();
  }

  /**
   * A clean pass reports nothing and forgets, so the same misconfiguration reintroduced later is announced instead
   * of being swallowed as "already said".
   */
  @Test
  void aCleanPassSaysNothingAndClearsTheMemory() {
    final AtomicReference<String> reported = new AtomicReference<>();
    final String verdict = "peer_a, peer_b -> host:2480";

    assertThat(RaftHAServer.isNewAmbiguityVerdict(reported, verdict)).isTrue();
    assertThat(RaftHAServer.isNewAmbiguityVerdict(reported, "")).isFalse();
    assertThat(reported.get()).isNull();
    assertThat(RaftHAServer.isNewAmbiguityVerdict(reported, verdict))
        .as("the collision came back, so it is news again")
        .isTrue();
  }

  /**
   * The verdict is the memory key, so its rendering has to be stable: peers sorted within an address and addresses
   * sorted between them, because Ratis hands the group over in whatever order it holds it. Two nodes looking at the
   * same misconfiguration must produce the same string, or every pass would look like a change.
   */
  @Test
  void theVerdictIsRenderedInAStableOrderWhateverOrderThePeersArriveIn() {
    final String[] addresses = { "host:2481", "host:2480", "host:2481", "host:2480" };
    final boolean[] fromConfig = new boolean[4];
    final RaftPeerId[] owners = { PEER_D, PEER_B, PEER_C, PEER_A };

    assertThat(RaftHAServer.describeAmbiguity(addresses, fromConfig, owners, 4,
        RaftHAServer.claimsByAddress(addresses, fromConfig, 4)))
        .isEqualTo("peer_a, peer_b -> host:2480; peer_c, peer_d -> host:2481");
  }

  /** Declared beats derived: the peer that stated the address owns it, so it is not among the withheld. */
  @Test
  void aDeclaredAddressIsNotNamedAsWithheld() {
    final String[] addresses = { "host:2480", "host:2480", "host:2482" };
    final boolean[] fromConfig = { true, false, false };
    final RaftPeerId[] owners = { PEER_A, PEER_B, PEER_C };

    assertThat(RaftHAServer.describeAmbiguity(addresses, fromConfig, owners, 3,
        RaftHAServer.claimsByAddress(addresses, fromConfig, 3)))
        .isEqualTo("peer_b -> host:2480");
  }

  /** A view in which every address identifies its own peer renders to nothing, which is what keeps the log quiet. */
  @Test
  void aCleanViewRendersToTheEmptyVerdict() {
    final String[] addresses = { "host:2480", "host:2481", "host:2482" };
    final boolean[] fromConfig = new boolean[3];
    final RaftPeerId[] owners = { PEER_A, PEER_B, PEER_C };

    assertThat(RaftHAServer.describeAmbiguity(addresses, fromConfig, owners, 3,
        RaftHAServer.claimsByAddress(addresses, fromConfig, 3))).isEmpty();
  }

  /**
   * The wiring, not the decision function: the memory is only cleared by a pass that reaches it with nothing to
   * say, so a caller that consults the warning ONLY when the current view is already ambiguous can never clear it.
   * The verdict then stays behind forever and the same collision, reintroduced after the operator fixes it, is
   * swallowed as "already reported" - which is the case issue #6297 exists to fix, defeated by its own caller.
   * Both warnings are therefore handed every resolved view, and these two tests are what says so.
   */
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
        RaftHAServer.claimsByAddress(addresses, DERIVED, 3));
  }

  private static void warnHttps(final RaftHAServer raft, final String[] addresses) {
    raft.warnAmbiguousPeerAddress(true, addresses, DERIVED, THREE_PEERS, 3,
        RaftHAServer.claimsByAddress(addresses, DERIVED, 3));
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

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("ArcadeDB_0");

    return new RaftHAServer(mockServer, config);
  }
}
