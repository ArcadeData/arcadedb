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

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

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
}
