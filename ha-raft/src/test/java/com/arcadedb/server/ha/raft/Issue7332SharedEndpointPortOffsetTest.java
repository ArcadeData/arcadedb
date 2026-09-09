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

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import static com.arcadedb.server.ha.raft.Issue7256SharedAddressCapabilityProbeTest.newDetachedServer;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7332: the shared-endpoint recovery #7256 added could not fire on the deployment shape its own
 * documentation named.
 * <p>
 * With no {@code http} port declared, a peer's HTTP endpoint is derived as ITS Raft host plus THIS node's HTTP
 * port. On a cluster whose peers differ by port rather than by host that address is this node's own for every
 * peer, so {@code sharedEndpointOf} withheld it every single time - correctly, dialling ourselves comes straight
 * back - and the second pass never had an endpoint to probe. Every piece of code was individually right and the
 * feature still could not work where it was written to work.
 * <p>
 * The signal left on that shape is the port offset: a multi-node cluster on one host is configured by moving both
 * ports together, so {@code localHttpPort + (peerRaftPort - localRaftPort)} names the peer's listener when the
 * convention holds and names nothing when it does not. A guess is admissible here for exactly one reason, and the
 * tests below are mostly about keeping that reason true: the candidate reaches only
 * {@link PeerDialAddress#sharedEndpoint()}, never {@link PeerDialAddress#httpAddress()}, so the only caller that
 * can act on it is the read-only probe that credits whoever ANSWERS rather than whoever it addressed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7332SharedEndpointPortOffsetTest {

  /** Local node first (raft 2434), then two peers differing only by Raft port. No http port anywhere. */
  private static final String COLLAPSED_BY_PORT = "localhost:2434,localhost:2435,localhost:2436";
  private static final String LOCAL             = "localhost_2434";
  private static final String PEER_A            = "localhost_2435";
  private static final String PEER_B            = "localhost_2436";

  private CapturingTestLogger logger;

  @BeforeEach
  void installLogger() {
    logger = CapturingTestLogger.install();
  }

  @AfterEach
  void uninstallLogger() {
    logger.uninstall();
  }

  // ---------------------------------------------------------------------------------------------------------
  // The candidate itself
  // ---------------------------------------------------------------------------------------------------------

  /** The whole point: on the target shape every peer now has an endpoint worth asking. */
  @Test
  void eachPeerCollapsedOntoThisNodesOwnAddressGetsItsOwnCandidate() {
    final RaftHAServer raft = newDetachedServer(COLLAPSED_BY_PORT, 2480);

    assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf(PEER_A))).isEqualTo("localhost:2481");
    assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf(PEER_B))).isEqualTo("localhost:2482");
  }

  /** Never this node itself, whichever way it is asked. */
  @Test
  void thereIsNoCandidateForThisNodeOrForAPeerThatDoesNotExist() {
    final RaftHAServer raft = newDetachedServer(COLLAPSED_BY_PORT, 2480);

    assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf(LOCAL))).isNull();
    assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf("nobody_9999"))).isNull();
    assertThat(raft.getPortOffsetPeerHttpAddress(null)).isNull();
  }

  /**
   * A declared HTTP port is a statement about which node owns which port, and guessing over it would be replacing
   * an operator's answer with ours. A declared address that STILL collides is a configuration fault to report -
   * which the refusal already does, naming HA_SERVER_LIST - not one to work around.
   */
  @Test
  void aPeerThatDeclaredItsHttpPortIsNeverGuessedAt() {
    final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2490,localhost:2436:2490", 2480);

    assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf(PEER_A))).isNull();
    assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf(PEER_B))).isNull();
  }

  /**
   * Peers that differ by HOST share a Raft port, so the derivation never collapsed them onto one another in the
   * first place and there is nothing to recover. An offset of zero would also name our own port.
   */
  @Test
  void peersThatDifferByHostRatherThanByPortHaveNoOffsetToCarry() {
    final RaftHAServer raft = newDetachedServer("hostA:2434,hostB:2434,hostC:2434", 2480);

    assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf("hostB_2434"))).isNull();
    assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf("hostC_2434"))).isNull();
  }

  /** No local HTTP listener yet means no offset to carry it over, rather than a port derived from -1. */
  @Test
  void thereIsNoCandidateBeforeTheLocalHttpListenerIsUp() {
    final RaftHAServer raft = newDetachedServer(COLLAPSED_BY_PORT, -1);

    assertThat(raft.getPortOffsetPeerHttpAddress(RaftPeerId.valueOf(PEER_A))).isNull();
  }

  /** Arithmetic that leaves the port range is not a port, and must not silently wrap into one that looks real. */
  @Test
  void anOffsetThatLeavesThePortRangeYieldsNoCandidate() {
    // 2480 + (1 - 65000) is negative; 2480 + (65535 - 100) is past 65535.
    assertThat(newDetachedServer("localhost:65000,localhost:1", 2480)
        .getPortOffsetPeerHttpAddress(RaftPeerId.valueOf("localhost_1"))).isNull();
    assertThat(newDetachedServer("localhost:100,localhost:65535", 2480)
        .getPortOffsetPeerHttpAddress(RaftPeerId.valueOf("localhost_65535"))).isNull();
  }

  // ---------------------------------------------------------------------------------------------------------
  // How it reaches the probe, and how it must not reach anything else
  // ---------------------------------------------------------------------------------------------------------

  /** The candidate is offered as a shared endpoint and never as a dial address. */
  @Test
  void theCandidateIsOfferedOnlyThroughSharedEndpoint() {
    final RaftHAServer raft = newDetachedServer(COLLAPSED_BY_PORT, 2480);

    final PeerDialAddress dial = PeerDialAddress.resolve(raft, RaftPeerId.valueOf(PEER_A), "peer");

    assertThat(dial.refused())
        .as("a resync, a verify or a forwarded write must still refuse: nothing here identifies the peer")
        .isTrue();
    assertThat(dial.httpAddress()).isNull();
    assertThat(dial.httpsAddress()).isNull();
    assertThat(dial.sharedEndpoint().httpAddress()).isEqualTo("localhost:2481");
    assertThat(dial.sharedEndpoint().httpsAddress())
        .as("an offset between two Raft ports says nothing about a third port")
        .isNull();
  }

  /**
   * End to end: the round that used to make no second-pass probe at all now asks each candidate and credits the
   * peer that answers. This is the assertion the issue is actually about - everything above is the shape of it.
   */
  @Test
  void theSecondPassNowProbesAndCreditsAPeerOnTheCollapsedShape() {
    final RaftHAServer raft = newDetachedServer(COLLAPSED_BY_PORT, 2480);
    final ScriptedProber prober = new ScriptedProber();
    prober.sharedAnswers.put("localhost:2481",
        new PeerCapabilityQuery.Advertisement(PEER_A, "26.10.1", Set.of(PeerCapabilities.SCHEMA_DELTA)));
    raft.setCapabilityProber(prober);

    raft.refreshPeerCapabilities();

    assertThat(prober.calls)
        .as("one probe per candidate, on the shared-endpoint route (expectedPeerId null)")
        .containsExactlyInAnyOrder("null@localhost:2481", "null@localhost:2482");
    assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_A))
        .as("the peer that identified itself behind a collapsed address is negotiable after all")
        .isNotNull();
    assertThat(raft.peersMissingCapability(PeerCapabilities.SCHEMA_DELTA))
        .as("and the one nothing answered for is still a 'no', so the mechanism still fails the cheap way")
        .containsExactly(PEER_B);
  }

  /**
   * The second pass's diagnostic goes through {@code describeProbeFailure} like the first pass's does. A bare
   * {@code SocketTimeoutException} carries no message, so {@code getMessage()} printed "null" for precisely the
   * failure that helper was written to describe.
   */
  @Test
  void aSecondPassProbeFailureWithNoMessageStillSaysWhat() {
    final RaftHAServer raft = newDetachedServer(COLLAPSED_BY_PORT, 2480);
    raft.setCapabilityProber(new ScriptedProber());

    raft.refreshPeerCapabilities();

    assertThat(logger.formattedAt(Level.FINE))
        .filteredOn(line -> line.contains("No peer identified itself at the shared address"))
        .as("the reason survives; it used to read 'null', which is the one thing a reason must never be")
        .isNotEmpty()
        .allSatisfy(line -> assertThat(line).contains("SocketTimeoutException").doesNotContain(": null"));
  }

  // ---------------------------------------------------------------------------------------------------------
  // The port parser the offset rests on
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void extractPortReadsTheLastColonAndRefusesEverythingElse() {
    assertThat(RaftHAServer.extractPort("localhost:2424")).isEqualTo(2424);
    assertThat(RaftHAServer.extractPort("[::1]:2424")).as("a bracketed IPv6 literal keeps its own colons")
        .isEqualTo(2424);
    assertThat(RaftHAServer.extractPort("fe80::1:2424")).as("an unbracketed one is separated by the LAST colon")
        .isEqualTo(2424);
    assertThat(RaftHAServer.extractPort("[::1]")).as("a bracketed literal with no port has none").isEqualTo(-1);
    assertThat(RaftHAServer.extractPort("localhost")).isEqualTo(-1);
    assertThat(RaftHAServer.extractPort("localhost:")).isEqualTo(-1);
    assertThat(RaftHAServer.extractPort("localhost:http")).isEqualTo(-1);
    assertThat(RaftHAServer.extractPort("")).isEqualTo(-1);
    assertThat(RaftHAServer.extractPort(null)).isEqualTo(-1);
  }

  /**
   * A prober that answers from a per-address script on the shared-endpoint route and fails with a MESSAGE-LESS
   * exception everywhere else, which is what the diagnostic assertion above needs.
   */
  private static final class ScriptedProber implements RaftHAServer.CapabilityProber {
    private final List<String>                                    calls        = new ArrayList<>();
    private final java.util.Map<String, PeerCapabilityQuery.Advertisement> sharedAnswers = new java.util.HashMap<>();

    @Override
    public PeerCapabilityQuery.Advertisement probe(final String expectedPeerId, final String httpAddress,
        final String httpsAddress, final String clusterToken) throws IOException {
      calls.add(expectedPeerId + "@" + httpAddress);
      final PeerCapabilityQuery.Advertisement answer = expectedPeerId == null ? sharedAnswers.get(httpAddress) : null;
      if (answer == null)
        throw new SocketTimeoutException();
      return answer;
    }
  }
}
