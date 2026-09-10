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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7256: a cluster whose peers all derive onto one HTTP address could never negotiate capabilities, so
 * #6989's schema delta was unreachable on that deployment shape - silently, and permanently.
 * <p>
 * {@link PeerDialAddress} withholds an address that identifies no single peer, which is right for every request
 * that acts on the peer it addressed: a resync or a verify sent to the wrong node answers for a node that was
 * never asked. The capability probe is not one of those. It is read-only, and its reply NAMES its author - the
 * check {@code PeerCapabilityQuery.parse} already made - so the answer can be bound to whoever gave it instead of
 * to whoever it was meant for. That is the whole fix: ask the withheld address once, credit the peer that
 * answered.
 * <p>
 * What must not change is the direction it fails in. Everything the fan-out cannot account for stays a "no", so
 * an answer is never credited to a peer that did not give it - the three arms below (a stranger, this node, a
 * peer that already answered for itself) are that guarantee, and each of them is invisible when it misfires.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7256SharedAddressCapabilityProbeTest {

  /** Local node first, then two peers that both derive onto {@code localhost:2490}. */
  private static final String COLLAPSED_LIST = "localhost:2434:2480,localhost:2435:2490,localhost:2436:2490";
  private static final String LOCAL          = "localhost_2434";
  private static final String PEER_A         = "localhost_2435";
  private static final String PEER_B         = "localhost_2436";
  private static final String SHARED_ADDRESS = "localhost:2490";

  /** One probe of the shared address identifies the peer behind it, and only that peer is credited. */
  @Test
  void thePeerThatAnswersAtASharedAddressIsCreditedWithItsAnswer() {
    final RaftHAServer raft = newDetachedServer(COLLAPSED_LIST);
    final RecordingProber prober = new RecordingProber(advertisement(PEER_A));
    raft.setCapabilityProber(prober);

    raft.refreshPeerCapabilities();

    assertThat(prober.calls)
        .as("the two peers collapsed onto one address cost ONE probe, on the shared-endpoint route")
        .containsExactly("null@" + SHARED_ADDRESS);
    assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_A))
        .as("the peer that identified itself is negotiable after all")
        .isNotNull();
    assertThat(raft.peersMissingCapability(PeerCapabilities.SCHEMA_DELTA))
        .as("and the one that did not is still a 'no', so the section is still withheld from this cluster")
        .containsExactly(PEER_B);
  }

  /** The peer nobody answered for keeps the refusal as its reason, which is what an operator can act on. */
  @Test
  void aPeerLeftUnidentifiedReportsWhyItsCapabilitiesAreUnknown() {
    final RaftHAServer raft = newDetachedServer(COLLAPSED_LIST);
    raft.setCapabilityProber(new RecordingProber(advertisement(PEER_A)));

    raft.refreshPeerCapabilities();

    assertThat(raft.getPeerCapabilityRegistry().unknownReasonOf(PEER_B))
        .as("'no capabilities' and 'no address that identifies this peer' have nothing in common as remedies")
        .contains("shared with another peer")
        .contains(GlobalConfiguration.HA_SERVER_LIST.getKey());
    assertThat(raft.getPeerCapabilityRegistry().unknownReasonOf(PEER_A))
        .as("a peer that answered has nothing left to explain")
        .isNull();
  }

  /** An answer from a node outside the configuration is not credited to anyone. */
  @Test
  void anAnswerFromAPeerOutsideTheConfigurationIsDiscarded() {
    final RaftHAServer raft = newDetachedServer(COLLAPSED_LIST);
    raft.setCapabilityProber(new RecordingProber(advertisement("some_other_node")));

    raft.refreshPeerCapabilities();

    assertThat(raft.peersMissingCapability(PeerCapabilities.SCHEMA_DELTA)).containsExactlyInAnyOrder(PEER_A, PEER_B);
    assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf("some_other_node")).isNull();
  }

  /** Nor is one from this node itself: a probe that came straight back here proves nothing about a peer. */
  @Test
  void anAnswerFromThisNodeIsDiscarded() {
    final RaftHAServer raft = newDetachedServer(COLLAPSED_LIST);
    raft.setCapabilityProber(new RecordingProber(advertisement(LOCAL)));

    raft.refreshPeerCapabilities();

    assertThat(raft.peersMissingCapability(PeerCapabilities.SCHEMA_DELTA)).containsExactlyInAnyOrder(PEER_A, PEER_B);
  }

  /**
   * A peer that answered for itself on its own unambiguous address is not the peer living behind a collapsed one,
   * so a shared-endpoint answer naming it is ignored rather than allowed to overwrite what it really said.
   */
  @Test
  void anAnswerNamingAPeerThatAlreadyAnsweredForItselfIsIgnored() {
    // localhost_2435 owns port 2481; localhost_2436 and localhost_2437 collapse onto 2490.
    final RaftHAServer raft = newDetachedServer(
        "localhost:2434:2480,localhost:2435:2481,localhost:2436:2490,localhost:2437:2490");
    final RecordingProber prober = new RecordingProber(advertisement(PEER_A));
    prober.guardedAnswer = new PeerCapabilityQuery.Advertisement(PEER_A, "26.10.1", Set.of());
    raft.setCapabilityProber(prober);

    raft.refreshPeerCapabilities();

    assertThat(prober.calls)
        .as("the guarded pass runs first, and the shared address is asked once afterwards")
        .containsExactlyInAnyOrder(PEER_A + "@localhost:2481", "null@" + SHARED_ADDRESS);
    assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_A).capabilities())
        .as("what the peer said on its own address stands; the shared-address answer naming it is not believed")
        .isEmpty();
  }

  /** A cluster that declares its ports never reaches the second pass at all. */
  @Test
  void aCorrectlyDeclaredClusterStillProbesEachPeerByName() {
    final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");
    final RecordingProber prober = new RecordingProber(null);
    prober.guardedAnswer = advertisement(PEER_A);
    raft.setCapabilityProber(prober);

    raft.refreshPeerCapabilities();

    assertThat(prober.calls)
        .as("no address is withheld, so nothing is dialled without knowing which peer is meant")
        .containsExactlyInAnyOrder(PEER_A + "@localhost:2481", PEER_B + "@localhost:2482");
  }

  /**
   * Two peers whose HTTP halves collide while their declared HTTPS halves do not still get a probe each. On an
   * SSL cluster the HTTPS endpoint is the one actually dialled, so deduplicating on the HTTP address alone would
   * have thrown away the second peer's usable endpoint and left it permanently unknown for no reason.
   */
  @Test
  void twoPeersSharingOnlyTheirHttpHalfAreBothProbed() {
    // host:raftPort:httpPort:priority:httpsPort - 2435 and 2436 share http 2495, with distinct declared https.
    final RaftHAServer raft = newDetachedServer(
        "localhost:2434:2480:0:2490,localhost:2435:2495:0:2491,localhost:2436:2495:0:2492");
    final RecordingProber prober = new RecordingProber(advertisement(PEER_A));
    raft.setCapabilityProber(prober);

    raft.refreshPeerCapabilities();

    assertThat(prober.httpsAsked)
        .as("one probe per distinct endpoint, not per distinct HTTP address")
        .containsExactlyInAnyOrder("localhost:2491", "localhost:2492");
  }

  /** A probe failure with no message still leaves a reason: a null one would CLEAR the reason, not record it. */
  @Test
  void aProbeFailureWithNoMessageStillReportsWhy() {
    final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");
    raft.setCapabilityProber((expectedPeerId, httpAddress, httpsAddress, clusterToken) -> {
      throw new SocketTimeoutException();
    });

    raft.refreshPeerCapabilities();

    assertThat(raft.getPeerCapabilityRegistry().unknownReasonOf(PEER_A))
        .as("a bare SocketTimeoutException carries no message, and 'unknown for no stated reason' is exactly "
            + "what capabilitiesUnknownReason exists to prevent")
        .isEqualTo("SocketTimeoutException");
  }

  // ---------------------------------------------------------------------------------------------------------
  // The two halves of the guard this relaxation leans on, pinned where they live.
  // ---------------------------------------------------------------------------------------------------------

  /** The refusal says WHICH refusal it is: an address shared with another peer, not the absence of one. */
  @Test
  void aSharedAddressRefusalCarriesTheAddressAndAnUnresolvableOneDoesNot() {
    final RaftHAServer raft = newDetachedServer(COLLAPSED_LIST);

    final PeerDialAddress shared = PeerDialAddress.resolve(raft, RaftPeerId.valueOf(PEER_A), "peer");
    assertThat(shared.refused()).isTrue();
    assertThat(shared.sharedEndpoint()).isNotNull();
    assertThat(shared.sharedEndpoint().httpAddress()).isEqualTo(SHARED_ADDRESS);

    assertThat(PeerDialAddress.resolve(raft, RaftPeerId.valueOf("nobody_9999"), "peer").sharedEndpoint())
        .as("an unknown peer has no address to recover, shared or otherwise")
        .isNull();
    assertThat(PeerDialAddress.resolve(raft, null, "peer").sharedEndpoint()).isNull();
    assertThat(PeerDialAddress.refuse("this node is the leader").sharedEndpoint()).isNull();
    assertThat(PeerDialAddress.resolve(raft, RaftPeerId.valueOf(LOCAL), "peer").sharedEndpoint()).isNull();
  }

  /**
   * A collapsed address that is this node's own is never offered AS ITSELF: dialling it comes straight back here.
   * <p>
   * What is offered in its place is the port-offset candidate #7332 added, because on this deployment shape - no
   * declared {@code http} port, every node on one host - the collapsed address is this node's own for every peer,
   * so withholding it and stopping there left the second pass with nothing to ask and this whole mechanism dead
   * on its own target. The dial address itself is still refused, which is the part that must not move.
   * <p>
   * The candidate itself, and every case in which there is none, is pinned in
   * {@link Issue7332SharedEndpointPortOffsetTest}.
   */
  @Test
  void aSharedAddressThatIsThisNodesOwnIsReplacedByAPortOffsetCandidate() {
    // Nothing declares an http port, so every peer derives onto this node's own 2480 listener.
    final RaftHAServer raft = newDetachedServer("localhost:2434,localhost:2435,localhost:2436", 2480);

    final PeerDialAddress dial = PeerDialAddress.resolve(raft, RaftPeerId.valueOf(PEER_A), "peer");

    assertThat(dial.refused()).as("the address that identifies no peer is still not an address to dial").isTrue();
    assertThat(dial.httpAddress()).isNull();
    assertThat(dial.sharedEndpoint()).isNotNull();
    assertThat(dial.sharedEndpoint().httpAddress())
        .as("2435 - 2434 carried over to the HTTP port names the peer's own listener")
        .isEqualTo("localhost:2481");
  }

  /** The parser takes any author when no peer was expected, and still refuses a document that names none. */
  @Test
  void theSharedEndpointRouteAcceptsWhicheverPeerAnswers() throws IOException {
    final String body = PostCapabilitiesHandler.advertisement(PEER_B, Set.of(PeerCapabilities.SCHEMA_DELTA))
        .toString();

    assertThat(PeerCapabilityQuery.parse(null, body, "http://" + SHARED_ADDRESS).peerId()).isEqualTo(PEER_B);

    assertThatThrownBy(() -> PeerCapabilityQuery.parse(null, "{\"capabilities\":[]}", "http://" + SHARED_ADDRESS))
        .as("an answer that names no peer cannot be credited to anyone")
        .isInstanceOf(IOException.class)
        .hasMessageContaining("names no peer");
  }

  /**
   * Every unknown has a reason, including the one no failed probe produced. An advertisement that simply ages out
   * means the leader stopped ASKING - a node that lost leadership and regained it has a window of exactly that
   * shape, because {@code stopCapabilityMonitor} ends the refresh while the answers it took stay in the registry.
   */
  @Test
  void everyKindOfUnknownReportsWhichKindItIs() {
    final AtomicLong now = new AtomicLong(1_000_000L);
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry(PeerCapabilityRegistry.ADVERTISEMENT_TTL_MS);
    registry.setClock(now::get);

    assertThat(registry.unknownReasonOf(PEER_A))
        .as("a peer that was never asked about has nothing to explain")
        .isNull();

    registry.record(registry.generation(), PEER_A, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1");
    assertThat(registry.unknownReasonOf(PEER_A)).as("nor does one with a fresh answer").isNull();

    now.addAndGet(PeerCapabilityRegistry.ADVERTISEMENT_TTL_MS + 1);
    assertThat(registry.unknownReasonOf(PEER_A))
        .as("an answer that aged out with no probe behind it is still a 'no', and now it says which 'no'")
        .contains("older than");

    registry.forget(registry.generation(), PEER_A, "the probe failed");
    assertThat(registry.unknownReasonOf(PEER_A))
        .as("a recorded reason outranks the staleness note, being the more specific of the two")
        .isEqualTo("the probe failed");

    registry.record(registry.generation(), PEER_A, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1");
    assertThat(registry.unknownReasonOf(PEER_A)).as("and a fresh answer clears it").isNull();
  }

  private static PeerCapabilityQuery.Advertisement advertisement(final String peerId) {
    return new PeerCapabilityQuery.Advertisement(peerId, "26.10.1", Set.of(PeerCapabilities.SCHEMA_DELTA));
  }

  /**
   * A {@link RaftHAServer.CapabilityProber} that records every question asked as {@code <expectedPeerId>@<address>}
   * and answers from a fixed script, so the assertions are about which peer an answer is credited to.
   */
  private static final class RecordingProber implements RaftHAServer.CapabilityProber {
    private final List<String>                       calls      = new ArrayList<>();
    /** The HTTPS endpoint offered with each question, so "which endpoint was dialled" can be asserted too. */
    private final List<String>                       httpsAsked = new ArrayList<>();
    private final PeerCapabilityQuery.Advertisement  sharedAnswer;
    private       PeerCapabilityQuery.Advertisement  guardedAnswer;

    private RecordingProber(final PeerCapabilityQuery.Advertisement sharedAnswer) {
      this.sharedAnswer = sharedAnswer;
    }

    @Override
    public PeerCapabilityQuery.Advertisement probe(final String expectedPeerId, final String httpAddress,
        final String httpsAddress, final String clusterToken) throws IOException {
      calls.add(expectedPeerId + "@" + httpAddress);
      if (httpsAddress != null)
        httpsAsked.add(httpsAddress);
      final PeerCapabilityQuery.Advertisement answer = expectedPeerId == null ? sharedAnswer : guardedAnswer;
      if (answer == null)
        throw new IOException("capability query to " + httpAddress + " returned HTTP 404");
      return answer;
    }
  }

  /**
   * A {@link RaftHAServer} built from {@code serverList} with Ratis never started - the peer group and the declared
   * HTTP addresses are both populated by the constructor, and the live configuration falls back to the declared
   * one, which is all the capability refresh reads.
   */
  private static RaftHAServer newDetachedServer(final String serverList) {
    return newDetachedServer(serverList, -1);
  }

  /**
   * @param localHttpPort the port this node's HTTP listener reports, or {@code -1} for no listener at all - which
   *                      is what a mocked server answers by default, and what makes every DERIVED address resolve
   *                      to null. A test about a derived address has to declare one.
   */
  static RaftHAServer newDetachedServer(final String serverList, final int localHttpPort) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList);

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("ArcadeDB_0");
    if (localHttpPort > 0) {
      final HttpServer httpServer = mock(HttpServer.class);
      when(httpServer.getPort()).thenReturn(localHttpPort);
      when(mockServer.getHttpServer()).thenReturn(httpServer);
    }

    return new RaftHAServer(mockServer, config);
  }
}
