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

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.protocol.RaftPeerId;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Probing peers for their capabilities: shared addresses, reporting, forgetting at probe failure and the monitor
 * in every role.
 */
class PeerCapabilityProbeTest {

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
   */
  @Nested
  class SharedAddressCapabilityProbe {
    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();


    /** Local node first, then two peers that both derive onto {@code localhost:2490}. */
    private static final String COLLAPSED_LIST = "localhost:2434:2480,localhost:2435:2490,localhost:2436:2490";
    private static final String LOCAL          = "localhost_2434";
    private static final String PEER_A         = "localhost_2435";
    private static final String PEER_B         = "localhost_2436";
    private static final String SHARED_ADDRESS = "localhost:2490";

    /** One probe of the shared address identifies the peer behind it, and only that peer is credited. */
    // Regression for #7256: the peer that answers at a shared address is credited with its answer
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
    // Regression for #7256: a peer left unidentified reports why its capabilities are unknown
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
    // Regression for #7256: an answer from a peer outside the configuration is discarded
    @Test
    void anAnswerFromAPeerOutsideTheConfigurationIsDiscarded() {
      final RaftHAServer raft = newDetachedServer(COLLAPSED_LIST);
      raft.setCapabilityProber(new RecordingProber(advertisement("some_other_node")));

      raft.refreshPeerCapabilities();

      assertThat(raft.peersMissingCapability(PeerCapabilities.SCHEMA_DELTA)).containsExactlyInAnyOrder(PEER_A, PEER_B);
      assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf("some_other_node")).isNull();
    }

    /** Nor is one from this node itself: a probe that came straight back here proves nothing about a peer. */
    // Regression for #7256: an answer from this node is discarded
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
    // Regression for #7256: an answer naming a peer that already answered for itself is ignored
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
    // Regression for #7256: a correctly declared cluster still probes each peer by name
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
    // Regression for #7256: two peers sharing only their http half are both probed
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
    // Regression for #7256: a probe failure with no message still reports why
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
    // Regression for #7256: a shared address refusal carries the address and an unresolvable one does not
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
     * {@link PeerHttpAddressDiscoveryTest.SharedEndpointPortOffset}.
     */
    // Regression for #7256: a shared address that is this nodes own is replaced by a port offset candidate
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
    // Regression for #7256: the shared endpoint route accepts whichever peer answers
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
     * means this node stopped ASKING while keeping what it had - the shape {@code stopCapabilityMonitor} leaves
     * behind, since it ends the refresh while the answers it took stay in the registry.
     */
    // Regression for #7256: every kind of unknown reports which kind it is
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

      final FakeArcadeDBServer arcadeServer = FakeArcadeDBServer.create("ArcadeDB_0", new ContextConfiguration());
      if (localHttpPort > 0) {
        final HttpServer httpServer = HTTP_SERVERS.listeningOn(arcadeServer, localHttpPort);
        arcadeServer.httpServer(httpServer);
      }

      return new RaftHAServer(arcadeServer, config);
    }
  }

  /**
   * Issue #7301, a follow-up to #7219: what {@code GET /api/v1/cluster} is entitled to say about a peer's
   * capabilities, when the leader's cache stops being believed, and what the probe rebuilds on every round.
   * <p>
   * The first item is the one with an operator-facing wrong answer. The guard was written for the leader rendering
   * its own row and tested "the peer being rendered is the leader" rather than "the peer being rendered is this
   * node", so a FOLLOWER answering the endpoint published its own capabilities under the leader's id: on a rolling
   * upgrade the un-upgraded leader reads as capable, with no {@code version} field to contradict it, and the diff
   * the field exists to make possible names the wrong node.
   */
  @Nested
  class CapabilityReporting {

    private static final String LEADER    = "arcadedb0";
    private static final String FOLLOWER  = "arcadedb1";
    private static final String TRUSTSTORE_DIR = "./target/test-truststore-7301";

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(TRUSTSTORE_DIR));
    }

    // ---------------------------------------------------------------------------------------------------------
    // 1. A follower must not publish its own capabilities under the leader's row
    // ---------------------------------------------------------------------------------------------------------

    /**
     * The registry is empty on a follower by design - only the leader probes - so the leader's row has nothing
     * true to carry, and an omitted field is the honest answer. Publishing the local set there tells an operator
     * that the node they did NOT ask can decode what the node they DID ask can.
     */
    // Regression for #7301: a follower publishes nothing under the leaders row
    @Test
    void aFollowerPublishesNothingUnderTheLeadersRow() {
      final JSONObject peerJson = new JSONObject();

      final boolean published = GetClusterHandler.putPeerCapabilities(peerJson, LEADER, FOLLOWER, null,
          Set.of(PeerCapabilities.SCHEMA_DELTA));

      assertThat(published).isFalse();
      assertThat(peerJson.has("capabilities"))
          .as("a follower has no answer about the leader, and the local node's answer is not one")
          .isFalse();
      assertThat(peerJson.has("version")).isFalse();
    }

    /**
     * The local row is the one every node can answer for, leader or not, so it is published either way - a true
     * statement in both roles, and the row an operator polling this node is actually asking about.
     */
    // Regression for #7301: the local row carries its own capabilities whatever role this node holds
    @Test
    void theLocalRowCarriesItsOwnCapabilitiesWhateverRoleThisNodeHolds() {
      final JSONObject peerJson = new JSONObject();

      final boolean published = GetClusterHandler.putPeerCapabilities(peerJson, FOLLOWER, FOLLOWER, null,
          Set.of(PeerCapabilities.SCHEMA_DELTA));

      assertThat(published).isTrue();
      assertThat(peerJson.getJSONArray("capabilities").toList()).containsExactly(PeerCapabilities.SCHEMA_DELTA);
      assertThat(peerJson.getString("version"))
          .as("the same version a probing leader would have recorded for this node, so the two rows agree")
          .isEqualTo(Constants.getVersion());
    }

    /** A probed answer is published as given, and outranks anything the local node could say about itself. */
    // Regression for #7301: a probed advertisement is published as given
    @Test
    void aProbedAdvertisementIsPublishedAsGiven() {
      final JSONObject peerJson = new JSONObject();

      final boolean published = GetClusterHandler.putPeerCapabilities(peerJson, FOLLOWER, LEADER,
          new PeerCapabilityRegistry.Advertisement(Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1", 0L), Set.of());

      assertThat(published).isTrue();
      assertThat(peerJson.getJSONArray("capabilities").toList()).containsExactly(PeerCapabilities.SCHEMA_DELTA);
      assertThat(peerJson.getString("version")).isEqualTo("26.10.1");
    }

    /** An older build answers with no version string; the field is omitted rather than reported empty. */
    // Regression for #7301: an advertisement with no version omits the field
    @Test
    void anAdvertisementWithNoVersionOmitsTheField() {
      final JSONObject peerJson = new JSONObject();

      GetClusterHandler.putPeerCapabilities(peerJson, FOLLOWER, LEADER,
          new PeerCapabilityRegistry.Advertisement(Set.of(), "", 0L), Set.of(PeerCapabilities.SCHEMA_DELTA));

      assertThat(peerJson.getJSONArray("capabilities").toList()).isEmpty();
      assertThat(peerJson.has("version")).isFalse();
    }

    // ---------------------------------------------------------------------------------------------------------
    // 2. Advertisements from the previous leadership term are not inherited
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Starting the refresh drops whatever was believed while nobody was asking: an answer taken before the monitor
     * stopped must not be acted on until a round has re-confirmed it, because that is the one window in which an
     * optional wire-format section could be written to a peer whose build has since gone backwards.
     * <p>
     * Since issue #7549 the monitor runs in every role and is started once, at server start, so this is a
     * start-of-life invalidation rather than the per-leadership-term one #7301 introduced - and on a node whose
     * monitor never stopped, {@code startCapabilityMonitor} returns at its guard and keeps the warm registry.
     * <p>
     * The seeded entry is the LOCAL peer's, deliberately: the refresh round skips this node and
     * {@code retainOnly} keeps it (it is a configured peer), so nothing the background round does can remove it.
     * Only the invalidation can, which is what makes the assertion below say what it claims.
     */
    // Regression for #7301: starting the capability monitor drops what was believed while nobody was asking
    @Test
    void startingTheCapabilityMonitorDropsWhatWasBelievedWhileNobodyWasAsking() {
      final RaftHAServer raft = newDetachedServer();
      // Nothing answers: the round this starts can only ever forget, never record.
      raft.setCapabilityProber((peerId, http, https, token) -> {
        throw new IOException("no answer");
      });

      final PeerCapabilityRegistry registry = raft.getPeerCapabilityRegistry();
      registry.record(registry.generation(), raft.getLocalPeerId().toString(), Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1");
      assertThat(registry.freshAdvertisementOf(raft.getLocalPeerId().toString())).isNotNull();

      raft.startCapabilityMonitor();
      try {
        assertThat(registry.freshAdvertisementOf(raft.getLocalPeerId().toString()))
            .as("an answer taken while the monitor was not running is not inherited by the round that starts it")
            .isNull();
      } finally {
        raft.stopCapabilityMonitor();
      }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 3. The log-throttle shadow is pruned with the entry it shadows
    // ---------------------------------------------------------------------------------------------------------

    /**
     * The throttle exists so a steady state - a peer permanently on an older build - does not print a line every
     * five seconds. It must not outlive the peer: held outside the registry, nothing pruned it, and a peer removed
     * and later re-added had its first advertisement suppressed as "unchanged" against what it said before it left.
     * That first advertisement is the one an operator watching a rejoin most wants to see.
     */
    // Regression for #7301: a re added peers first advertisement is reported again
    @Test
    void aReAddedPeersFirstAdvertisementIsReportedAgain() {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
      final Set<String> capabilities = Set.of(PeerCapabilities.SCHEMA_DELTA);

      assertThat(registry.record(registry.generation(), FOLLOWER, capabilities, "26.10.1")).as("the first answer is a change").isTrue();
      assertThat(registry.record(registry.generation(), FOLLOWER, capabilities, "26.10.1")).as("the same answer again is not").isFalse();

      assertThat(registry.forget(registry.generation(), FOLLOWER, "the probe failed")).as("the transition into failure is").isTrue();
      assertThat(registry.forget(registry.generation(), FOLLOWER, "the probe failed")).as("staying failed is not").isFalse();

      registry.retainOnly(registry.generation(), List.of(LEADER));
      assertThat(registry.record(registry.generation(), FOLLOWER, capabilities, "26.10.1"))
          .as("a peer that left and came back is new again, whatever it advertised before it left")
          .isTrue();
    }

    /**
     * The other half of the same window, raised in review of PR #7314. {@code stopCapabilityMonitor} ends the
     * refresh with {@code shutdownNow()} and does NOT wait for the round in flight, so a probe that was already
     * dialling when leadership was lost can answer after the next term has cleared the registry - recording the
     * previous term's answer over it, which is exactly what the clear exists to prevent.
     * <p>
     * Every write carries the generation its round started in, so the straggler is dropped. Closing it this way
     * rather than by waiting on the executor keeps a leadership transition from blocking on a network timeout.
     */
    // Regression for #7301: an answer from an ended leadership term is dropped
    @Test
    void anAnswerFromAnEndedLeadershipTermIsDropped() {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
      final long previousTerm = registry.generation();

      // The new term starts while the previous term's round is still dialling.
      registry.clear();
      assertThat(registry.generation()).isNotEqualTo(previousTerm);

      assertThat(registry.record(previousTerm, FOLLOWER, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1"))
          .as("the straggler is dropped, so it is not a transition to report either")
          .isFalse();
      assertThat(registry.freshAdvertisementOf(FOLLOWER))
          .as("and above all it is not believed: this is the window the clear exists to close")
          .isNull();

      assertThat(registry.forget(previousTerm, FOLLOWER, "the previous term's probe failed")).isFalse();
      assertThat(registry.unknownReasonOf(FOLLOWER))
          .as("nor does an ended term get to explain a peer this term has not asked about")
          .isNull();

      registry.record(registry.generation(), LEADER, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1");
      registry.retainOnly(previousTerm, List.of(FOLLOWER));
      assertThat(registry.freshAdvertisementOf(LEADER))
          .as("a previous term's configuration cannot prune this term's answers either")
          .isNotNull();

      assertThat(registry.record(registry.generation(), FOLLOWER, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1"))
          .as("this term's own round writes normally")
          .isTrue();
    }

    /** Acquiring leadership clears the shadow too, or the first round of the new term reports nothing at all. */
    // Regression for #7301: clearing the registry also clears what was last reported
    @Test
    void clearingTheRegistryAlsoClearsWhatWasLastReported() {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();

      assertThat(registry.record(registry.generation(), FOLLOWER, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1")).isTrue();
      registry.clear();

      assertThat(registry.freshAdvertisementOf(FOLLOWER)).isNull();
      assertThat(registry.unknownReasonOf(FOLLOWER)).as("a peer nobody has asked yet has nothing to explain").isNull();
      assertThat(registry.record(registry.generation(), FOLLOWER, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1"))
          .as("the first answer of the new term is reported, not swallowed as unchanged")
          .isTrue();
    }

    // ---------------------------------------------------------------------------------------------------------
    // 4. The HTTPS client is built once, not once per peer per refresh period
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Every probe used to build a fresh {@code SSLContext} from the truststore on disk - a file read, a
     * certificate-chain parse and an init - plus a fresh client whose connection pool it then threw away. Once per
     * peer, every five seconds, for the whole life of the leadership.
     */
    // Regression for #7301: the https client is reused until the truststore changes
    @Test
    void theHttpsClientIsReusedUntilTheTruststoreChanges() throws Exception {
      final File truststore = writeEmptyTruststore();
      final ArcadeDBServer server = serverWithTruststore(truststore, "changeit");
      final TrustedHttpClientCache cache = new TrustedHttpClientCache();

      final HttpClient first = cache.clientFor(server);
      assertThat(cache.clientFor(server))
          .as("an unchanged truststore costs a stat, not a certificate-chain parse")
          .isSameAs(first);

      // A rotation: same path, new bytes. The check has to notice, or a rotated certificate is never picked up.
      truststore.setLastModified(System.currentTimeMillis() + 5_000L);
      final HttpClient afterRotation = cache.clientFor(server);
      assertThat(afterRotation).as("a rotated truststore is picked up").isNotSameAs(first);

      // A different truststore is a different set of trust anchors, whatever its timestamps say. The password is
      // part of the material too, but it cannot be varied on its own here: a keystore's integrity check is computed
      // FROM the password, so a file opened with a different one does not load at all.
      final File other = new File(TRUSTSTORE_DIR, "rotated.jks");
      Files.copy(truststore.toPath(), other.toPath());
      assertThat(cache.clientFor(serverWithTruststore(other, "changeit"))).isNotSameAs(afterRotation);
    }

    /**
     * Raised in review of PR #7314: the cache was a bare {@code static}, and {@code BaseGraphServerTest} and every
     * HA suite start several servers in ONE JVM. Each server's probe would then see the other's trust material as a
     * change - rebuilding on every probe - and, since the request is sent outside the cache's monitor, could close a
     * client another server was still using. One cache per server is what makes "has the truststore changed" the
     * question it reads as.
     */
    // Regression for #7301: two servers in one jvm do not invalidate each others client
    @Test
    void twoServersInOneJvmDoNotInvalidateEachOthersClient() throws Exception {
      final File truststore = writeEmptyTruststore();
      final File other = new File(TRUSTSTORE_DIR, "other.jks");
      Files.copy(truststore.toPath(), other.toPath());

      final TrustedHttpClientCache first = new TrustedHttpClientCache();
      final TrustedHttpClientCache second = new TrustedHttpClientCache();
      final ArcadeDBServer firstServer = serverWithTruststore(truststore, "changeit");
      final ArcadeDBServer secondServer = serverWithTruststore(other, "changeit");

      final HttpClient firstClient = first.clientFor(firstServer);
      final HttpClient secondClient = second.clientFor(secondServer);

      assertThat(secondClient).as("each server builds its own from its own truststore").isNotSameAs(firstClient);
      assertThat(first.clientFor(firstServer))
          .as("and the other server's probe is not a reason to rebuild - or to close - this one's")
          .isSameAs(firstClient);
      assertThat(second.clientFor(secondServer)).isSameAs(secondClient);
    }

    /**
     * The client holds a connection pool and a selector thread, so a JVM that starts and stops many servers - what
     * the HA suites do - must not keep one per server that ever probed an HTTPS peer. {@code RaftHAServer.stop()}
     * closes it; this pins that closing works, is idempotent, and that the cache refuses to build another
     * afterwards - a client built after that single close() would be one nothing releases (PR #7314 review).
     */
    // Regression for #7301: the client is released on close and the cache refuses to build another
    @Test
    void theClientIsReleasedOnCloseAndTheCacheRefusesToBuildAnother() throws Exception {
      final File truststore = writeEmptyTruststore();
      final ArcadeDBServer server = serverWithTruststore(truststore, "changeit");
      final TrustedHttpClientCache cache = new TrustedHttpClientCache();

      cache.clientFor(server);
      cache.close();
      cache.close();

      // A probe that outlived stopCapabilityMonitor()'s shutdownNow() can still reach the cache after stop() made
      // its one close() call. Building it a client then would leak the one thing this class exists to release, so
      // the closed cache refuses - which the refresh round handles exactly as it handles an unreachable peer.
      assertThatThrownBy(() -> cache.clientFor(server))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("shutting down");

      // A cache that never built anything has nothing to release, and must not fail saying so.
      new TrustedHttpClientCache().close();
    }

    private static File writeEmptyTruststore() throws Exception {
      final File dir = new File(TRUSTSTORE_DIR);
      dir.mkdirs();
      final File store = new File(dir, "truststore.jks");
      final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
      ks.load(null, null);
      try (final OutputStream out = Files.newOutputStream(store.toPath())) {
        ks.store(out, "changeit".toCharArray());
      }
      return store;
    }

    private static ArcadeDBServer serverWithTruststore(final File truststore, final String password) {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, truststore.getAbsolutePath());
      configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, password);

      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, configuration);
      return server;
    }

    /** A {@link RaftHAServer} whose constructor has run but whose Ratis server was never started. */
    private static RaftHAServer newDetachedServer() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481");

      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
      return new RaftHAServer(arcadeServer, config);
    }
  }

  /**
   * Issue #7331: the capability probe buffered its failures and applied them only after the second pass, so for
   * the rest of the round a peer that had just failed to answer was still believed capable - and could be sent a
   * schema delta it cannot decode.
   * <p>
   * That is the exact scenario the mechanism exists for. A rolling upgrade restarts a node onto an older build,
   * which answers 404 on the capability route; the leader's own javadoc states, in bold, that <i>every failure
   * forgets rather than keeps</i>, because a peer that stopped answering may have been replaced by an older
   * build. Buffering suspended that guarantee for the width of a probe round.
   * <p>
   * The deferral had a real motive, and it is kept: the second pass is worth running before anything is REPORTED,
   * so a peer it identifies at a shared address is not warned about for the refusal that sent us looking for it.
   * The split is that the belief moves at the failure and only the report waits -
   * {@link PeerCapabilityRegistry#suspend} against {@link PeerCapabilityRegistry#forget}.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7331">issue #7331</a>
   */
  @Nested
  class CapabilityForgottenAtProbeFailure {

    /** Local node, one peer on its own address, and two that collapse onto localhost:2490 so pass 2 runs. */
    private static final String MIXED_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2490,localhost:2437:2490";
    private static final String PEER_A     = "localhost_2435";
    private static final String PEER_C     = "localhost_2436";

    /**
     * The repro. Round 1 records a capable advertisement for the peer with its own address. Round 2 has that peer
     * answer 404 - it has been restarted onto an older build - and samples what the registry says about it from
     * INSIDE the second pass, which is the window the buffering held open.
     */
    // Regression for #7331: a peer that just failed is no longer believed while the round is still running
    @Test
    void aPeerThatJustFailedIsNoLongerBelievedWhileTheRoundIsStillRunning() {
      final RaftHAServer raft = PeerCapabilityProbeTest.SharedAddressCapabilityProbe.newDetachedServer(MIXED_LIST, -1);
      final ScriptedProber prober = new ScriptedProber(raft);

      // Round 1: the peer on its own address advertises schema-delta.
      prober.guardedAnswer = advertisement(PEER_A);
      raft.setCapabilityProber(prober);
      raft.refreshPeerCapabilities();

      assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_A))
          .as("the round has to establish the belief, or the next one cannot show it being dropped")
          .isNotNull();

      // Round 2: the same peer answers 404. The shared-endpoint probe of pass 2 samples the registry.
      prober.guardedAnswer = null;
      prober.sampleDuringSharedPass = true;
      raft.refreshPeerCapabilities();

      assertThat(prober.seenDuringSharedPass.get())
          .as("the belief must be gone by the time the round is still probing: a schema change committed in this "
              + "window used to be shipped as a delta to a node that had already answered 404")
          .isNull();
      assertThat(raft.peersMissingCapability(PeerCapabilities.SCHEMA_DELTA))
          .as("and it is still missing after the round, which never regressed")
          .contains(PEER_A);
    }

    /** The reason an operator reads is set at the failure too, not only once the round has settled. */
    // Regression for #7331: the reason is available as soon as the probe fails
    @Test
    void theReasonIsAvailableAsSoonAsTheProbeFails() {
      final RaftHAServer raft = PeerCapabilityProbeTest.SharedAddressCapabilityProbe.newDetachedServer(MIXED_LIST, -1);
      final ScriptedProber prober = new ScriptedProber(raft);
      prober.guardedAnswer = null;
      prober.sampleReasonDuringSharedPass = true;
      raft.setCapabilityProber(prober);

      raft.refreshPeerCapabilities();

      assertThat(prober.reasonDuringSharedPass.get())
          .as("'unknown, and here is why' is the whole point of #7256's reason field; it cannot arrive a pass late")
          .contains("404");
    }

    // ---------------------------------------------------------------------------------------------------------
    // The registry contract the split rests on, pinned where it lives.
    // ---------------------------------------------------------------------------------------------------------

    /**
     * {@code suspend} drops the belief and leaves the report shadow alone; {@code forget} settles it.
     * <p>
     * Both halves matter. Dropping the belief is the fix. NOT settling the shadow is what keeps the fix from
     * costing a warning and a re-advertisement every five seconds on a cluster whose peers all dial through one
     * collapsed address - which is the cost the buffering was there to avoid, and the reason it is the LOG that
     * waits rather than the state.
     */
    // Regression for #7331: suspend moves the belief without settling the report
    @Test
    void suspendMovesTheBeliefWithoutSettlingTheReport() {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
      final long generation = registry.generation();

      registry.record(generation, PEER_A, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1");
      assertThat(registry.freshAdvertisementOf(PEER_A)).isNotNull();

      registry.suspend(generation, PEER_A, "capability query returned HTTP 404");
      assertThat(registry.freshAdvertisementOf(PEER_A))
          .as("the belief is gone immediately")
          .isNull();
      assertThat(registry.unknownReasonOf(PEER_A)).isEqualTo("capability query returned HTTP 404");

      assertThat(registry.record(generation, PEER_A, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1"))
          .as("a peer the second pass recovers said nothing new, so there is nothing to announce")
          .isFalse();
      assertThat(registry.freshAdvertisementOf(PEER_A)).as("and it is believed again").isNotNull();

      registry.suspend(generation, PEER_A, "capability query returned HTTP 404");
      assertThat(registry.forget(generation, PEER_A, "capability query returned HTTP 404"))
          .as("a peer the second pass does NOT recover is a transition, and is reported once")
          .isTrue();
      assertThat(registry.forget(generation, PEER_A, "capability query returned HTTP 404"))
          .as("and not again while it persists")
          .isFalse();
    }

    /** An answer from an ended leadership term must not resurrect - or drop - this term's state. */
    // Regression for #7331: suspend is dropped when its round belongs to an ended term
    @Test
    void suspendIsDroppedWhenItsRoundBelongsToAnEndedTerm() {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
      final long staleGeneration = registry.generation();
      registry.clear();

      registry.record(registry.generation(), PEER_A, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1");
      registry.suspend(staleGeneration, PEER_A, "a probe from the previous term");

      assertThat(registry.freshAdvertisementOf(PEER_A))
          .as("a failure observed under the previous leadership says nothing about this one")
          .isNotNull();
      assertThat(registry.unknownReasonOf(PEER_A)).isNull();
    }

    private static PeerCapabilityQuery.Advertisement advertisement(final String peerId) {
      return new PeerCapabilityQuery.Advertisement(peerId, "26.10.1", Set.of(PeerCapabilities.SCHEMA_DELTA));
    }

    /**
     * Answers the guarded (pass 1) probe from a script and, when asked to, reads the registry from inside the
     * shared-endpoint (pass 2) probe - the only place a test can observe the state MID-ROUND, which is where the
     * defect lived.
     */
    private final class ScriptedProber implements RaftHAServer.CapabilityProber {
      private final RaftHAServer                                    raft;
      private final List<String>                                    calls                       = new ArrayList<>();
      private       PeerCapabilityQuery.Advertisement               guardedAnswer;
      private       boolean                                         sampleDuringSharedPass;
      private       boolean                                         sampleReasonDuringSharedPass;
      private final AtomicReference<PeerCapabilityRegistry.Advertisement> seenDuringSharedPass  = new AtomicReference<>();
      private final AtomicReference<String>                         reasonDuringSharedPass      = new AtomicReference<>();

      private ScriptedProber(final RaftHAServer raft) {
        this.raft = raft;
      }

      @Override
      public PeerCapabilityQuery.Advertisement probe(final String expectedPeerId, final String httpAddress,
          final String httpsAddress, final String clusterToken) throws IOException {
        calls.add(expectedPeerId + "@" + httpAddress);

        if (expectedPeerId == null) {
          // Pass 2. Sample what the leader currently believes about the peer whose pass-1 probe just failed.
          if (sampleDuringSharedPass)
            seenDuringSharedPass.set(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_A));
          if (sampleReasonDuringSharedPass)
            reasonDuringSharedPass.set(raft.getPeerCapabilityRegistry().unknownReasonOf(PEER_A));
          return advertisement(PEER_C);
        }

        if (guardedAnswer == null)
          throw new IOException("capability query to " + httpAddress + " returned HTTP 404");
        return guardedAnswer;
      }
    }
  }

  /**
   * Issue #7549: the peer-capability registry is filled in EVERY role, so a rolling-upgrade readiness check does
   * not have to locate the leader before it can be answered.
   * <p>
   * The monitor was started from {@code startLagMonitor} and stopped from {@code stopLagMonitor}, i.e. it lived
   * exactly as long as a leadership term, because #7219's only consumer - the schema-delta decision - was
   * leader-side. Two consumers have since arrived that are not: the security compare-and-set gate of #7511, which
   * runs on whichever node the client landed on because the group and API-token routes do not forward, and
   * {@code GET /api/v1/cluster}, which is what an operator polls to ask whether the cluster is ready for a group
   * change. On a follower both saw an empty registry - the gate paid a synchronous probe round to work around it,
   * the status document simply omitted the rows.
   * <p>
   * The end-to-end half - a follower's {@code /api/v1/cluster} carrying the {@code capabilities} row of a peer
   * that is not itself - is {@code HaSecurityMutationRoutingTwoNodeIT}'s #7549 test. What is pinned here is the lifecycle
   * it rests on, which needs no cluster.
   */
  @Nested
  class CapabilityMonitorInEveryRole {

    private static final String PEER = "localhost_2435";

    /**
     * The lifecycle change itself: losing leadership ends the lag sampling and does NOT end the capability
     * refresh. Before this, a node that was demoted stopped asking, its answers aged out within
     * {@code ADVERTISEMENT_TTL_MS}, and from then on it could report nothing about any peer but itself.
     */
    // Regression for #7549: losing leadership does not stop the capability refresh
    @Test
    void losingLeadershipDoesNotStopTheCapabilityRefresh() {
      final RaftHAServer raft = newDetachedServer();
      // Nothing answers, so the rounds this starts cost a failed probe each and record nothing: the assertion is
      // about whether the monitor is still running, not about what it collected.
      raft.setCapabilityProber((peerId, http, https, token) -> {
        throw new IOException("no answer");
      });

      raft.startLagMonitor();
      try {
        assertThat(raft.isCapabilityMonitorRunning()).as("a leader refreshes capabilities").isTrue();

        raft.stopLagMonitor();

        assertThat(raft.isCapabilityMonitorRunning())
            .as("a demoted node keeps refreshing: every role now has a consumer for what it records")
            .isTrue();
      } finally {
        raft.stopCapabilityMonitor();
      }
      assertThat(raft.isCapabilityMonitorRunning()).as("only shutdown ends it").isFalse();
    }

    /**
     * A node that never led records what its peers advertise. Driven through the real
     * {@code refreshPeerCapabilities} round with a stub prober, on a server that is not the leader of anything -
     * {@code configuredPeers()} and the probe are read-only and available in any role, which is the premise the
     * whole change rests on.
     */
    // Regression for #7549: a node that is not the leader records what its peers advertise
    @Test
    void aNodeThatIsNotTheLeaderRecordsWhatItsPeersAdvertise() {
      final RaftHAServer raft = newDetachedServer();
      assertThat(raft.isLeader()).as("nothing here has won an election").isFalse();

      raft.setCapabilityProber((peerId, http, https, token) ->
          new PeerCapabilityQuery.Advertisement(PEER, "26.10.1", Set.of(PeerCapabilities.SCHEMA_DELTA)));

      raft.refreshPeerCapabilities();

      final PeerCapabilityRegistry.Advertisement advertisement = raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER);
      assertThat(advertisement).as("a follower knows what its peer advertises").isNotNull();
      assertThat(advertisement.capabilities()).contains(PeerCapabilities.SCHEMA_DELTA);
      assertThat(raft.getPeerCapabilityRegistry().unknownReasonOf(PEER))
          .as("nothing is unknown about a peer that answered").isNull();
    }

    /**
     * And when the peer does NOT answer, the node that asked has a reason to publish - the field
     * {@code GET /api/v1/cluster} writes since issue #7256, which used to be written only on the leader because
     * only the leader ever had one.
     */
    // Regression for #7549: a node that is not the leader also has a reason to publish when a peer does not answer
    @Test
    void aNodeThatIsNotTheLeaderAlsoHasAReasonToPublishWhenAPeerDoesNotAnswer() {
      final RaftHAServer raft = newDetachedServer();

      raft.setCapabilityProber((peerId, http, https, token) -> {
        throw new IOException("connection refused");
      });

      raft.refreshPeerCapabilities();

      assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER)).isNull();
      assertThat(raft.getPeerCapabilityRegistry().unknownReasonOf(PEER))
          .as("a follower can say WHY a peer is unknown, not merely omit the row")
          .isNotNull()
          .contains("connection refused");
    }

    /**
     * Two callers can start the monitor at once now, and only one executor may result (review of PR #7941).
     * <p>
     * Since the monitor is started from {@code start()} as well as from {@code startLagMonitor}, a node that wins
     * an election immediately - {@code raftServer.start()} does not wait for one, so a single-node bootstrap
     * always does - reaches both from different threads. The guard was a plain check-then-act on a non-volatile
     * field, so both could see null, both create a {@link java.util.concurrent.ScheduledExecutorService}, and the
     * second assignment orphan the first: a daemon thread and its 5-second probe schedule that
     * {@code stopCapabilityMonitor} can no longer reach.
     * <p>
     * Counted in live threads rather than in the field, because the field is exactly what a lost executor is no
     * longer reachable through - the leak is invisible to any assertion made on it.
     */
    // Regression for #7549: concurrent starts create exactly one monitor
    @Test
    void concurrentStartsCreateExactlyOneMonitor() throws Exception {
      // Threads are found by name, JVM-wide, so a monitor another test in the same fork left running (or is still winding
      // down) would be counted as this one's. Waiting for none failed whenever an earlier test leaked one for good: count
      // only the monitors that did not exist before this test started any.
      final Set<Thread> earlier = capabilityMonitorThreads();

      final RaftHAServer raft = newDetachedServer();
      raft.setCapabilityProber((peerId, http, https, token) -> {
        throw new IOException("no answer");
      });

      final int starters = 8;
      final CountDownLatch go = new CountDownLatch(1);
      final CountDownLatch done = new CountDownLatch(starters);
      for (int i = 0; i < starters; i++) {
        final Thread starter = new Thread(() -> {
          try {
            go.await();
            raft.startCapabilityMonitor();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
        }, "issue7549-starter-" + i);
        starter.setDaemon(true);
        starter.start();
      }

      try {
        go.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).as("every starter finished").isTrue();

        assertThat(capabilityMonitorThreadsStartedSince(earlier))
            .as("eight concurrent starts must leave one monitor, not eight")
            .isEqualTo(1);
      } finally {
        raft.stopCapabilityMonitor();
      }

      // The one that was created is also the one that can be stopped, which is the half a leaked executor fails.
      Awaitility.await().atMost(10, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(capabilityMonitorThreadsStartedSince(earlier))
              .as("and stopping it leaves none behind").isZero());
    }

    /** The capability-monitor threads this JVM currently has, by the name the factory gives them. */
    private static Set<Thread> capabilityMonitorThreads() {
      return Thread.getAllStackTraces().keySet().stream()
          .filter(Thread::isAlive)
          .filter(t -> "arcadedb-raft-capability-monitor".equals(t.getName()))
          .collect(Collectors.toSet());
    }

    /** How many live capability-monitor threads are not among {@code earlier}. */
    private static long capabilityMonitorThreadsStartedSince(final Set<Thread> earlier) {
      return capabilityMonitorThreads().stream().filter(t -> !earlier.contains(t)).count();
    }

    private static RaftHAServer newDetachedServer() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481");

      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
      return new RaftHAServer(arcadeServer, config);
    }
  }
}
