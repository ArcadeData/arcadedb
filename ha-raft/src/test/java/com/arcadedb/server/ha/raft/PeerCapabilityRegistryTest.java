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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.ha.raft.PeerCapabilityRegistry.UnknownKind;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The peer capability registry and its advertisement: caching, unknown kinds, cold registry and capability
 * state held across a peer outage.
 */
class PeerCapabilityRegistryTest {

  /**
   * Issue #7219: the leader may write an optional wire-format section only when EVERY peer has proved it can
   * decode it. This pins the arms of that decision.
   * <p>
   * The arms are pinned here rather than in the cluster, for the same reason
   * {@code RaftReplicatedDatabase.baseIsUsable} is: every one of them fails SILENTLY. A registry that answers
   * "capable" for a peer it has never heard of does not break replication - it makes the leader write a section the
   * peer cannot read, which shows up later as a schema that quietly stopped matching. Nothing in an integration
   * test goes red on that.
   */
  @Nested
  class Registry {

    private static final String CAP   = PeerCapabilities.SCHEMA_DELTA;
    private static final String PEER1 = "arcadedb1";
    private static final String PEER2 = "arcadedb2";

    private final AtomicLong             now      = new AtomicLong(1_000_000L);
    private final PeerCapabilityRegistry registry = newRegistry();

    private PeerCapabilityRegistry newRegistry() {
      final PeerCapabilityRegistry created = new PeerCapabilityRegistry(PeerCapabilityRegistry.ADVERTISEMENT_TTL_MS);
      created.setClock(now::get);
      return created;
    }

    // Regression for #7219: a cluster with no other peer is capable
    @Test
    void aClusterWithNoOtherPeerIsCapable() {
      // A single-node cluster has nobody who could fail to understand the bytes. Vacuously true, and it has to be
      // true or a one-node deployment would never use any optional section.
      assertThat(registry.allPeersSupport(List.of(), CAP)).isTrue();
      assertThat(registry.peersMissing(List.of(), CAP)).isEmpty();
    }

    // Regression for #7219: an unknown peer blocks the capability
    @Test
    void anUnknownPeerBlocksTheCapability() {
      // Never probed: a peer just added to the configuration, or one whose address PeerDialAddress refuses to
      // hand out because it identifies no single node (#6202). Both arrive here as "no entry".
      assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isFalse();
      assertThat(registry.peersMissing(List.of(PEER1), CAP)).containsExactly(PEER1);
    }

    // Regression for #7219: a peer that advertised the capability is capable
    @Test
    void aPeerThatAdvertisedTheCapabilityIsCapable() {
      registry.record(registry.generation(), PEER1, Set.of(CAP), "26.10.1");

      assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isTrue();
      assertThat(registry.freshAdvertisementOf(PEER1).version()).isEqualTo("26.10.1");
    }

    // Regression for #7219: a peer that answered without the capability blocks it
    @Test
    void aPeerThatAnsweredWithoutTheCapabilityBlocksIt() {
      // A build that HAS the capability endpoint but does not decode this particular section - the shape every
      // future capability will arrive in, and the one a version comparison would get wrong.
      registry.record(registry.generation(), PEER1, Set.of("some-other-capability"), "26.10.1");

      assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isFalse();
      assertThat(registry.peersMissing(List.of(PEER1), CAP)).containsExactly(PEER1);
    }

    // Regression for #7219: one incapable peer blocks the whole cluster
    @Test
    void oneIncapablePeerBlocksTheWholeCluster() {
      registry.record(registry.generation(), PEER1, Set.of(CAP), "26.10.1");

      assertThat(registry.allPeersSupport(List.of(PEER1, PEER2), CAP))
          .as("PEER2 has never answered, so the cluster is not covered even though PEER1 is")
          .isFalse();
      assertThat(registry.peersMissing(List.of(PEER1, PEER2), CAP)).containsExactly(PEER2);
    }

    // Regression for #7219: an expired advertisement blocks the capability
    @Test
    void anExpiredAdvertisementBlocksTheCapability() {
      registry.record(registry.generation(), PEER1, Set.of(CAP), "26.10.1");
      assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isTrue();

      // Still inside the window: several missed refresh rounds are absorbed on purpose, so a GC pause on one peer
      // does not cost the whole cluster its deltas.
      now.addAndGet(PeerCapabilityRegistry.ADVERTISEMENT_TTL_MS);
      assertThat(registry.allPeersSupport(List.of(PEER1), CAP))
          .as("an answer exactly at the TTL boundary is still believed")
          .isTrue();

      // Past it: a peer rolled back to an older build sits behind the same id, and this is the bound on how long
      // the leader keeps writing sections it can no longer read.
      now.addAndGet(1L);
      assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isFalse();
      assertThat(registry.freshAdvertisementOf(PEER1)).isNull();
    }

    // Regression for #7219: a peer that lost the capability blocks it again
    @Test
    void aPeerThatLostTheCapabilityBlocksItAgain() {
      registry.record(registry.generation(), PEER1, Set.of(CAP), "26.10.1");
      assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isTrue();

      // What a failed probe does - a 404 from a build without the route, or an unreachable peer. Forgetting rather
      // than keeping matters: the peer may have been replaced by an older build, and believing its last answer
      // until the TTL ran out would be believing it for a reason that no longer holds.
      registry.forget(registry.generation(), PEER1, "the probe failed");

      assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isFalse();
    }

    // Regression for #7219: a peer outside the configuration does not block
    @Test
    void aPeerOutsideTheConfigurationDoesNotBlock() {
      registry.record(registry.generation(), PEER1, Set.of(CAP), "26.10.1");

      // PEER2 was never asked about, because the question is only ever about the peers the leader replicates to.
      assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isTrue();
    }

    // Regression for #7219: retain only drops peers no longer in the configuration
    @Test
    void retainOnlyDropsPeersNoLongerInTheConfiguration() {
      registry.record(registry.generation(), PEER1, Set.of(CAP), "26.10.1");
      registry.record(registry.generation(), PEER2, Set.of(CAP), "26.10.1");

      registry.retainOnly(registry.generation(), List.of(PEER1));

      assertThat(registry.freshAdvertisementOf(PEER1)).isNotNull();
      assertThat(registry.freshAdvertisementOf(PEER2))
          .as("a peer the configuration dropped must not keep an advertisement for the leader's whole uptime")
          .isNull();
    }

    // Regression for #7219: a recorded capability set cannot be mutated by its caller
    @Test
    void aRecordedCapabilitySetCannotBeMutatedByItsCaller() {
      // The refresh loop parses into its own set; a recorded answer that shared that set could change under the
      // gate between two DDLs.
      final Set<String> parsed = new java.util.LinkedHashSet<>(Set.of(CAP));
      registry.record(registry.generation(), PEER1, parsed, "26.10.1");
      parsed.clear();

      assertThat(registry.allPeersSupport(List.of(PEER1), CAP)).isTrue();
    }

    // Regression for #7219: every missing peer is named
    @Test
    void everyMissingPeerIsNamed() {
      // peersMissing is not just a boolean in disguise: it is what the operator log names, and "which node is
      // holding the cluster back" is the only actionable half of the answer.
      registry.record(registry.generation(), PEER1, Set.of(CAP), "26.10.1");

      assertThat(registry.peersMissing(List.of(PEER1, PEER2, "arcadedb3"), CAP))
          .containsExactly(PEER2, "arcadedb3");
    }
  }

  /**
   * Issue #7219: the advertisement document is a wire contract between two nodes that may be running different
   * builds, so its shape - and the identity check the reader makes on it - are pinned here rather than left to an
   * integration test that only ever exercises two identical nodes.
   */
  @Nested
  class Advertisement {

    private static final String URL = "http://localhost:2480/api/v1/cluster/capabilities";

    // Regression for #7219: this build advertises that it decodes schema deltas
    @Test
    void thisBuildAdvertisesThatItDecodesSchemaDeltas() {
      // The decoder has been unconditional since #6989, so this build must say so - otherwise a cluster of nodes
      // that can all read a delta would never agree to write one.
      assertThat(PeerCapabilities.LOCAL).contains(PeerCapabilities.SCHEMA_DELTA);
    }

    // Regression for #7219: the advertisement names the peer its version and its capabilities
    @Test
    void theAdvertisementNamesThePeerItsVersionAndItsCapabilities() {
      final JSONObject document = PostCapabilitiesHandler.advertisement("arcadedb0", Set.of(PeerCapabilities.SCHEMA_DELTA));

      assertThat(document.getString("peerId")).isEqualTo("arcadedb0");
      assertThat(document.getString("version", "")).isNotEmpty();
      assertThat(document.getJSONArray("capabilities").toList()).containsExactly(PeerCapabilities.SCHEMA_DELTA);
    }

    // Regression for #7219: capabilities are sorted so two peers documents can be diffed
    @Test
    void capabilitiesAreSortedSoTwoPeersDocumentsCanBeDiffed() {
      final Set<String> unsorted = new LinkedHashSet<>();
      unsorted.add("zeta");
      unsorted.add("alpha");
      unsorted.add("mu");

      assertThat(PostCapabilitiesHandler.advertisement("arcadedb0", unsorted).getJSONArray("capabilities").toList())
          .containsExactly("alpha", "mu", "zeta");
    }

    // Regression for #7219: an advertisement is read back from its own document
    @Test
    void anAdvertisementIsReadBackFromItsOwnDocument() throws IOException {
      final String body = PostCapabilitiesHandler.advertisement("arcadedb1", Set.of(PeerCapabilities.SCHEMA_DELTA))
          .toString();

      final PeerCapabilityQuery.Advertisement parsed = PeerCapabilityQuery.parse("arcadedb1", body, URL);

      assertThat(parsed.peerId()).isEqualTo("arcadedb1");
      assertThat(parsed.capabilities()).containsExactly(PeerCapabilities.SCHEMA_DELTA);
    }

    // Regression for #7219: an advertisement from another peer is rejected
    @Test
    void anAdvertisementFromAnotherPeerIsRejected() {
      // With no 'http' port declared in arcadedb.ha.serverList a peer's endpoint is DERIVED, and several peers can
      // collapse onto one address (#6202, #6267). PeerDialAddress withholds such an address, and this is the
      // independent second half of that guard: an answer that came back from the wrong node must not be recorded
      // against the node that was asked, or the leader would credit a capability to a peer that never claimed it.
      final String body = PostCapabilitiesHandler.advertisement("arcadedb0", Set.of(PeerCapabilities.SCHEMA_DELTA))
          .toString();

      assertThatThrownBy(() -> PeerCapabilityQuery.parse("arcadedb1", body, URL))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("arcadedb1")
          .hasMessageContaining("arcadedb0");
    }

    // Regression for #7219: an advertisement with no capability array is empty rather than a failure
    @Test
    void anAdvertisementWithNoCapabilityArrayIsEmptyRatherThanAFailure() throws IOException {
      // Forward compatibility in the other direction: a future build may answer with fields this one does not know
      // about, and it must read as "nothing advertised" rather than throwing, which is a strictly safer answer.
      final PeerCapabilityQuery.Advertisement parsed =
          PeerCapabilityQuery.parse("arcadedb1", "{\"peerId\":\"arcadedb1\",\"somethingNew\":42}", URL);

      assertThat(parsed.capabilities()).isEmpty();
      assertThat(parsed.version()).isEmpty();
    }

    // Regression for #7219: the endpoint is preferred over https only when ssl is on
    @Test
    void theEndpointIsPreferredOverHttpsOnlyWhenSslIsOn() {
      assertThat(PeerCapabilityQuery.chooseUrl("h:1", "h:2", false)).isEqualTo("http://h:1/api/v1/cluster/capabilities");
      assertThat(PeerCapabilityQuery.chooseUrl("h:1", "h:2", true)).isEqualTo("https://h:2/api/v1/cluster/capabilities");
      // SSL on but no HTTPS address resolved: fall back to the listener that is always there, exactly as
      // LeaderDatabaseQuery does, rather than refusing to ask at all.
      assertThat(PeerCapabilityQuery.chooseUrl("h:1", null, true)).isEqualTo("http://h:1/api/v1/cluster/capabilities");
      assertThat(PeerCapabilityQuery.chooseUrl(null, null, false)).isNull();
    }
  }

  /**
   * Issue #8686: the cached "every peer can read the tx-prepared-at-index section" answer must be served within its TTL, recomputed
   * after it, and never outlive a membership change - not even when the change lands while the answer is being computed.
   */
  @Nested
  class TxPreparedAtCapabilityCaching {

    private final AtomicLong    clock    = new AtomicLong(1_000_000L);
    private final AtomicBoolean answer   = new AtomicBoolean(true);
    private final AtomicInteger computed = new AtomicInteger();

    private TxPreparedAtCapabilityCache cache() {
      return new TxPreparedAtCapabilityCache(() -> {
        computed.incrementAndGet();
        return answer.get();
      }, clock::get, 1_000L);
    }

    // Regression for #8686: the answer is served within the ttl and recomputed after it
    @Test
    void theAnswerIsServedWithinTheTtlAndRecomputedAfterIt() {
      final TxPreparedAtCapabilityCache cache = cache();

      assertThat(cache.isCapable()).isTrue();
      assertThat(cache.isCapable()).isTrue();
      assertThat(computed.get()).as("one computation for the two commits").isEqualTo(1);

      answer.set(false);
      clock.addAndGet(500);
      assertThat(cache.isCapable()).as("still the cached answer").isTrue();

      clock.addAndGet(600);
      assertThat(cache.isCapable()).as("expired, so asked again").isFalse();
      assertThat(computed.get()).isEqualTo(2);
    }

    // Regression for #8686: a membership change drops the answer at once
    @Test
    void aMembershipChangeDropsTheAnswerAtOnce() {
      final TxPreparedAtCapabilityCache cache = cache();
      assertThat(cache.isCapable()).isTrue();

      answer.set(false);
      cache.invalidate();

      assertThat(cache.isCapable()).as("a peer that just joined is asked again, not served the old yes").isFalse();
    }

    /** The race: the membership changes while the answer is being computed under the old one. */
    // Regression for #8686: an answer computed across a membership change is not served again
    @Test
    void anAnswerComputedAcrossAMembershipChangeIsNotServedAgain() {
      final TxPreparedAtCapabilityCache[] holder = new TxPreparedAtCapabilityCache[1];
      final AtomicBoolean firstCall = new AtomicBoolean(true);
      holder[0] = new TxPreparedAtCapabilityCache(() -> {
        computed.incrementAndGet();
        final boolean old = answer.get();
        if (firstCall.getAndSet(false)) {
          holder[0].invalidate();
          answer.set(false);
        }
        return old;
      }, clock::get, 1_000L);

      assertThat(holder[0].isCapable()).as("this caller gets the answer it computed").isTrue();
      assertThat(holder[0].isCapable()).as("but the next one must not be served it: it predates the change").isFalse();
      assertThat(computed.get()).isEqualTo(2);
    }
  }

  /**
   * Issue #8655: a follower's {@code GET /api/v1/cluster} said WHY it had no capabilities for a peer only as a sentence,
   * so Studio could not tell a peer whose build predates the capability route (a 404 every node's probe gets, the
   * leader's included) from one only this node could not reach (which the leader may reach fine). #8540 had to read
   * both as "unverified" on a follower and leave Create enabled, so a half-finished rolling upgrade surfaced as the
   * leader's 409 instead of a disabled button.
   * <p>
   * The registry now records a machine-readable kind next to the reason, and the cluster row publishes it as
   * {@code capabilitiesUnknownKind}. Each test drives one entry point that writes or reads it.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8655">issue #8655</a>
   */
  @Nested
  class UnknownCapabilityKind {

    /** Local node, one peer on its own address, and two that collapse onto localhost:2490 so their dial is refused. */
    private static final String MIXED_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2490,localhost:2437:2490";
    private static final String PEER_A     = "localhost_2435";
    private static final String PEER_C     = "localhost_2436";
    private static final String PEER_D     = "localhost_2437";

    // ---------------------------------------------------------------------------------------------------------
    // The probe: a 404 is told apart from every other failure where it is raised
    // ---------------------------------------------------------------------------------------------------------

    // Regression for #8655: a404 is raised as route missing
    @Test
    void a404IsRaisedAsRouteMissing() {
      assertThatThrownBy(() -> PeerCapabilityQuery.checkStatus(404, "http://peer/api/v1/cluster/capabilities"))
          .isInstanceOf(PeerCapabilityQuery.RouteMissingException.class)
          .hasMessage("capability query to http://peer/api/v1/cluster/capabilities returned HTTP 404");
    }

    // Regression for #8655: any other status is a plain probe failure
    @Test
    void anyOtherStatusIsAPlainProbeFailure() {
      for (final int status : new int[] { 401, 403, 500, 503 })
        assertThatThrownBy(() -> PeerCapabilityQuery.checkStatus(status, "http://peer/x"))
            .as("HTTP %d is this node's view of the path to the peer, not the peer saying it predates the route", status)
            .isInstanceOf(IOException.class)
            .isNotInstanceOf(PeerCapabilityQuery.RouteMissingException.class)
            .hasMessageContaining("returned HTTP " + status);
    }

    // Regression for #8655: a200 is not a failure
    @Test
    void a200IsNotAFailure() throws IOException {
      PeerCapabilityQuery.checkStatus(200, "http://peer/x");
    }

    // ---------------------------------------------------------------------------------------------------------
    // The refresh round: each failure path records its own kind
    // ---------------------------------------------------------------------------------------------------------

    // Regression for #8655: a peer that answers404 is recorded as route missing
    @Test
    void aPeerThatAnswers404IsRecordedAsRouteMissing() {
      final RaftHAServer raft = PeerCapabilityProbeTest.SharedAddressCapabilityProbe.newDetachedServer(MIXED_LIST, -1);
      raft.setCapabilityProber(guardedFailure(new PeerCapabilityQuery.RouteMissingException(
          "capability query to http://localhost:2481/api/v1/cluster/capabilities returned HTTP 404")));

      raft.refreshPeerCapabilities();

      final PeerCapabilityRegistry registry = raft.getPeerCapabilityRegistry();
      assertThat(registry.unknownKindOf(PEER_A)).isEqualTo(UnknownKind.ROUTE_MISSING);
      assertThat(registry.unknownReasonOf(PEER_A))
          .as("the sentence an operator reads is unchanged")
          .contains("returned HTTP 404");
    }

    // Regression for #8655: a peer this node cannot reach is recorded as unreachable
    @Test
    void aPeerThisNodeCannotReachIsRecordedAsUnreachable() {
      final RaftHAServer raft = PeerCapabilityProbeTest.SharedAddressCapabilityProbe.newDetachedServer(MIXED_LIST, -1);
      raft.setCapabilityProber(guardedFailure(new ConnectException("Connection refused")));

      raft.refreshPeerCapabilities();

      assertThat(raft.getPeerCapabilityRegistry().unknownKindOf(PEER_A))
          .as("a transport failure says nothing about what the leader's probe gets")
          .isEqualTo(UnknownKind.UNREACHABLE);
    }

    // Regression for #8655: another http status is recorded as unreachable
    @Test
    void anotherHttpStatusIsRecordedAsUnreachable() {
      final RaftHAServer raft = PeerCapabilityProbeTest.SharedAddressCapabilityProbe.newDetachedServer(MIXED_LIST, -1);
      raft.setCapabilityProber(guardedFailure(new IOException("capability query to x returned HTTP 401")));

      raft.refreshPeerCapabilities();

      assertThat(raft.getPeerCapabilityRegistry().unknownKindOf(PEER_A))
          .as("only the RouteMissingException type decides ROUTE_MISSING, never the message text")
          .isEqualTo(UnknownKind.UNREACHABLE);
    }

    // Regression for #8655: an interrupted probe is recorded as unreachable
    @Test
    void anInterruptedProbeIsRecordedAsUnreachable() {
      final RaftHAServer raft = PeerCapabilityProbeTest.SharedAddressCapabilityProbe.newDetachedServer(MIXED_LIST, -1);
      raft.setCapabilityProber((expectedPeerId, httpAddress, httpsAddress, clusterToken) -> {
        throw new InterruptedException();
      });

      try {
        raft.refreshPeerCapabilities();
      } finally {
        Thread.interrupted(); // the round re-asserts the flag; clear it so it cannot leak into the next test
      }

      assertThat(raft.getPeerCapabilityRegistry().unknownKindOf(PEER_A)).isEqualTo(UnknownKind.UNREACHABLE);
    }

    /**
     * The collapsed peers are refused before any probe, and the shared-address probe of pass 2 then answers 404. They
     * keep ADDRESS_REFUSED: nothing answered FOR them, so the 404 at the shared address is not their answer.
     */
    // Regression for #8655: a peer with no dialable address is recorded as address refused even when the shared address answers404
    @Test
    void aPeerWithNoDialableAddressIsRecordedAsAddressRefusedEvenWhenTheSharedAddressAnswers404() {
      final RaftHAServer raft = PeerCapabilityProbeTest.SharedAddressCapabilityProbe.newDetachedServer(MIXED_LIST, -1);
      raft.setCapabilityProber((expectedPeerId, httpAddress, httpsAddress, clusterToken) -> {
        if (expectedPeerId == null)
          throw new PeerCapabilityQuery.RouteMissingException("capability query to " + httpAddress + " returned HTTP 404");
        return new PeerCapabilityQuery.Advertisement(expectedPeerId, "26.10.1", Set.of(PeerCapabilities.SCHEMA_DELTA));
      });

      raft.refreshPeerCapabilities();

      final PeerCapabilityRegistry registry = raft.getPeerCapabilityRegistry();
      assertThat(registry.unknownKindOf(PEER_A)).as("the peer that answered has nothing to explain").isNull();
      assertThat(registry.unknownKindOf(PEER_C)).isEqualTo(UnknownKind.ADDRESS_REFUSED);
      assertThat(registry.unknownKindOf(PEER_D)).isEqualTo(UnknownKind.ADDRESS_REFUSED);
    }

    // ---------------------------------------------------------------------------------------------------------
    // The registry: the kind lives and dies with its reason
    // ---------------------------------------------------------------------------------------------------------

    // Regression for #8655: the kind follows the reason through every write
    @Test
    void theKindFollowsTheReasonThroughEveryWrite() {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
      final long generation = registry.generation();

      assertThat(registry.unknownKindOf(PEER_A)).as("a peer nobody has asked has no kind").isNull();

      registry.suspend(generation, PEER_A, "capability query returned HTTP 404", UnknownKind.ROUTE_MISSING);
      assertThat(registry.unknownKindOf(PEER_A)).isEqualTo(UnknownKind.ROUTE_MISSING);

      registry.forget(generation, PEER_A, "Connection refused", UnknownKind.UNREACHABLE);
      assertThat(registry.unknownOf(PEER_A))
          .as("the reason and its kind are one read, so they always come from the same probe")
          .isEqualTo(new PeerCapabilityRegistry.Unknown("Connection refused", UnknownKind.UNREACHABLE));

      registry.record(generation, PEER_A, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1");
      assertThat(registry.unknownKindOf(PEER_A)).as("a fresh answer clears it").isNull();

      registry.forget(generation, PEER_A, "capability query returned HTTP 404", UnknownKind.ROUTE_MISSING);
      registry.retainOnly(generation, Set.of());
      assertThat(registry.unknownKindOf(PEER_A)).as("a peer that left the configuration takes its kind with it").isNull();

      registry.forget(generation, PEER_A, "capability query returned HTTP 404", UnknownKind.ROUTE_MISSING);
      registry.clear();
      assertThat(registry.unknownKindOf(PEER_A)).as("a new leadership term starts from nothing").isNull();
    }

    // Regression for #8655: a write from an ended term does not set a kind
    @Test
    void aWriteFromAnEndedTermDoesNotSetAKind() {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
      final long stale = registry.generation();
      registry.clear();

      registry.suspend(stale, PEER_A, "capability query returned HTTP 404", UnknownKind.ROUTE_MISSING);
      registry.forget(stale, PEER_A, "capability query returned HTTP 404", UnknownKind.ROUTE_MISSING);

      assertThat(registry.unknownKindOf(PEER_A)).isNull();
    }

    // Regression for #8655: an answer that aged out is stale
    @Test
    void anAnswerThatAgedOutIsStale() {
      final AtomicLong now = new AtomicLong(1_000L);
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry(100L);
      registry.setClock(now::get);
      registry.record(registry.generation(), PEER_A, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1");

      now.addAndGet(101L);

      assertThat(registry.unknownKindOf(PEER_A)).isEqualTo(UnknownKind.STALE);
      assertThat(registry.unknownReasonOf(PEER_A)).contains("older than the 100ms");
    }

    // Regression for #8655: the three argument writes record unreachable
    @Test
    void theThreeArgumentWritesRecordUnreachable() {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
      registry.forget(registry.generation(), PEER_A, "something failed");
      assertThat(registry.unknownKindOf(PEER_A))
          .as("the kind that claims nothing about the peer itself, so a caller that does not classify cannot hard-gate")
          .isEqualTo(UnknownKind.UNREACHABLE);
    }

    /** Studio and the OpenAPI enum match these literals; a rename must break a test, not the gate. */
    // Regression for #8655: the kind names are the ones the contract publishes
    @Test
    void theKindNamesAreTheOnesTheContractPublishes() {
      assertThat(Arrays.stream(UnknownKind.values()).map(Enum::name))
          .containsExactly("ROUTE_MISSING", "UNREACHABLE", "ADDRESS_REFUSED", "STALE");
    }

    // ---------------------------------------------------------------------------------------------------------
    // GET /api/v1/cluster: the row carries the kind next to the reason
    // ---------------------------------------------------------------------------------------------------------

    // Regression for #8655: the peer row carries the kind next to the reason
    @Test
    void thePeerRowCarriesTheKindNextToTheReason() {
      final JSONObject peerJson = new JSONObject();

      GetClusterHandler.putPeerCapabilitiesUnknown(peerJson,
          new PeerCapabilityRegistry.Unknown("capability query returned HTTP 404", UnknownKind.ROUTE_MISSING));

      assertThat(peerJson.getString("capabilitiesUnknownReason")).isEqualTo("capability query returned HTTP 404");
      assertThat(peerJson.getString("capabilitiesUnknownKind")).isEqualTo("ROUTE_MISSING");
    }

    // Regression for #8655: a peer with nothing to explain carries neither
    @Test
    void aPeerWithNothingToExplainCarriesNeither() {
      final JSONObject peerJson = new JSONObject();

      GetClusterHandler.putPeerCapabilitiesUnknown(peerJson, null);

      assertThat(peerJson.has("capabilitiesUnknownReason")).isFalse();
      assertThat(peerJson.has("capabilitiesUnknownKind")).isFalse();
    }

    private static RaftHAServer.CapabilityProber guardedFailure(final IOException failure) {
      return (expectedPeerId, httpAddress, httpsAddress, clusterToken) -> {
        if (expectedPeerId == null)
          // Pass 2, for the two collapsed peers: nobody identifies themselves there.
          throw new ConnectException("Connection refused");
        throw failure;
      };
    }
  }

  /**
   * Issue #10856: a float or double array is written in its fixed-width form, which a build that predates it cannot read. On
   * a replicated database the record bytes reach every peer in the pages a transaction ships, so no node writes the form
   * until every peer has advertised {@link PeerCapabilities#FIXED_WIDTH_FLOAT_ARRAYS}; until then the varint form, which
   * every build reads, is written. A peer that joins invalidates the answer at once.
   */
  @Nested
  class FixedWidthFloatArrayCapability {
    private static final int DIM = 256;

    private final String           path = "target/databases/issue10856-fixed-width-capability";
    private       LocalDatabase    database;
    private       FakeRaftHAServer raft;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(path));
      database = (LocalDatabase) new DatabaseFactory(path).create();
      database.getSchema().createDocumentType("Doc").createProperty("embedding", Type.ARRAY_OF_FLOATS);
      raft = FakeRaftHAServer.detached();
      new RaftReplicatedDatabase(null, database, raft);
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    // Regression for #10856: this build advertises the form
    @Test
    void thisBuildAdvertisesTheForm() {
      assertThat(PeerCapabilities.LOCAL).contains(PeerCapabilities.FIXED_WIDTH_FLOAT_ARRAYS);
    }

    // Regression for #10856: the varint form is written while a peer has not advertised the fixed one
    @Test
    void theVarintFormIsWrittenWhileAPeerHasNotAdvertisedTheFixedOne() {
      raft.returns("peersMissingCapability", List.of("old-peer"));

      assertThat(serializedSize()).as("5 bytes per float: the form every build reads").isGreaterThanOrEqualTo(5 * DIM);
      assertThat(raft.calls("peersMissingCapability")).extracting(call -> call.getFirst())
          .contains(PeerCapabilities.FIXED_WIDTH_FLOAT_ARRAYS);
    }

    // Regression for #10856: the fixed form is written once every peer advertises it
    @Test
    void theFixedFormIsWrittenOnceEveryPeerAdvertisesIt() {
      raft.returns("peersMissingCapability", List.of());

      assertThat(serializedSize()).isBetween(4 * DIM, 4 * DIM + 32);
    }

    /** The cached verdict is dropped when the membership changes: a peer that just joined has not answered yet. */
    // Regression for #10856: a membership change drops the cached verdict
    @Test
    void aMembershipChangeDropsTheCachedVerdict() {
      raft.returns("peersMissingCapability", List.of());
      assertThat(serializedSize()).isLessThan(5 * DIM);

      raft.returns("peersMissingCapability", List.of("joined-peer"));
      assertThat(serializedSize()).as("still the cached verdict, within its second").isLessThan(5 * DIM);

      raft.invalidateFixedWidthFloatArraysCapability();
      assertThat(serializedSize()).isGreaterThanOrEqualTo(5 * DIM);
    }

    private int serializedSize() {
      final float[] vector = new float[DIM];
      for (int i = 0; i < DIM; i++)
        vector[i] = (float) Math.sin(i + 1) / 16;
      final MutableDocument doc = database.newDocument("Doc").set("embedding", vector);
      return database.getSerializer().serialize(database, doc).size();
    }
  }

  /**
   * Issue #11196: the three optional TX_ENTRY sections (tx-prepared-at-index, tx-fulltext-corpus-deltas, fixed-width-float-arrays)
   * are emitted by any node, so a registry that has not been filled yet must not read as "a peer does not support it". A peer that
   * was never asked is a third state: it is asked once, then the answer decides. A peer that answered no, or could not be reached,
   * is still a "no" and is not probed again on the commit path.
   */
  @Nested
  class ColdRegistryTxEntryCapability {

    private static final String PEER = "localhost_2435";

    // Regression for #11196: a cold registry is asked once instead of reading as no
    @Test
    void aColdRegistryIsAskedOnceInsteadOfReadingAsNo() {
      final RaftHAServer raft = newDetachedServer();
      final AtomicInteger probes = new AtomicInteger();
      raft.setCapabilityProber((peerId, http, https, token) -> {
        probes.incrementAndGet();
        return new PeerCapabilityQuery.Advertisement(PEER, "26.11.1", PeerCapabilities.LOCAL);
      });

      assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER)).as("cold registry").isNull();

      assertThat(raft.canStateTxPreparedAt()).as("tx-prepared-at-index").isTrue();
      assertThat(raft.canStateFullTextCorpusDeltas()).as("tx-fulltext-corpus-deltas").isTrue();
      assertThat(raft.canWriteFixedWidthFloatArrays()).as("fixed-width-float-arrays").isTrue();
      assertThat(probes.get()).as("one round filled the registry for all three").isEqualTo(1);
    }

    // Regression for #11196: a peer that answered no is not probed again on the commit path
    @Test
    void aPeerThatAnsweredNoIsNotProbedAgainOnTheCommitPath() {
      final RaftHAServer raft = newDetachedServer();
      final AtomicInteger probes = new AtomicInteger();
      raft.setCapabilityProber((peerId, http, https, token) -> {
        probes.incrementAndGet();
        return new PeerCapabilityQuery.Advertisement(PEER, "26.9.1", Set.of(PeerCapabilities.SCHEMA_DELTA));
      });

      assertThat(raft.canStateTxPreparedAt()).isFalse();
      raft.invalidateTxPreparedAtCapability();
      assertThat(raft.canStateTxPreparedAt()).isFalse();
      assertThat(raft.canStateFullTextCorpusDeltas()).isFalse();

      assertThat(probes.get()).as("a real 'no' is a settled answer, only the first read asked").isEqualTo(1);
    }

    // Regression for #11196: an unreachable peer is asked once and stays a no
    @Test
    void anUnreachablePeerIsAskedOnceAndStaysANo() {
      final RaftHAServer raft = newDetachedServer();
      final AtomicInteger probes = new AtomicInteger();
      raft.setCapabilityProber((peerId, http, https, token) -> {
        probes.incrementAndGet();
        throw new IOException("connection refused");
      });

      assertThat(raft.canStateTxPreparedAt()).isFalse();
      raft.invalidateTxPreparedAtCapability();
      assertThat(raft.canStateTxPreparedAt()).isFalse();

      assertThat(probes.get()).as("a failed probe is recorded, so a down peer does not cost a round per commit").isEqualTo(1);
    }

    // Regression for #11196: concurrent commits on a cold registry share one round
    @Test
    void concurrentCommitsOnAColdRegistryShareOneRound() throws Exception {
      final RaftHAServer raft = newDetachedServer();
      final AtomicInteger probes = new AtomicInteger();
      raft.setCapabilityProber((peerId, http, https, token) -> {
        probes.incrementAndGet();
        Thread.sleep(100);
        return new PeerCapabilityQuery.Advertisement(PEER, "26.11.1", PeerCapabilities.LOCAL);
      });

      final int committers = 8;
      final CountDownLatch go = new CountDownLatch(1);
      final AtomicInteger capable = new AtomicInteger();
      final List<Thread> threads = new ArrayList<>();
      for (int i = 0; i < committers; i++) {
        final Thread thread = new Thread(() -> {
          try {
            go.await();
            if (raft.canStateTxPreparedAt())
              capable.incrementAndGet();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
        thread.start();
        threads.add(thread);
      }
      go.countDown();
      for (final Thread thread : threads) {
        thread.join(30_000);
        assertThat(thread.isAlive()).as("the committer finished").isFalse();
      }

      assertThat(capable.get()).as("every committer got the answer of the one round").isEqualTo(committers);
      assertThat(probes.get()).as("single flight").isEqualTo(1);
    }

    // Regression for #11196: the warning says why each peer is missing
    @Test
    void theWarningSaysWhyEachPeerIsMissing() {
      final RaftHAServer raft = newDetachedServer();
      assertThat(raft.explainMissingCapability(PeerCapabilities.TX_PREPARED_AT_INDEX))
          .as("never asked").contains(PEER).containsIgnoringCase("not been asked");

      raft.setCapabilityProber((peerId, http, https, token) ->
          new PeerCapabilityQuery.Advertisement(PEER, "26.9.1", Set.of(PeerCapabilities.SCHEMA_DELTA)));
      raft.refreshPeerCapabilities();
      assertThat(raft.explainMissingCapability(PeerCapabilities.TX_PREPARED_AT_INDEX))
          .as("answered without it").contains(PEER).containsIgnoringCase("older build");

      raft.setCapabilityProber((peerId, http, https, token) -> {
        throw new IOException("connection refused");
      });
      raft.refreshPeerCapabilities();
      assertThat(raft.explainMissingCapability(PeerCapabilities.TX_PREPARED_AT_INDEX))
          .as("unreachable").contains(PEER).contains("connection refused");
    }

    private static RaftHAServer newDetachedServer() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481");

      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
      return new RaftHAServer(arcadeServer, config);
    }
  }

  /**
   * Issue #12122 (and #12119): a peer that is merely DOWN must not switch off the framed TX_ENTRY sections for every other node.
   * <p>
   * The registry dropped a peer's advertisement at the first failed probe, so one node's outage made every other node stop stating
   * {@code tx-prepared-at-index} (the #8686 refusal: a follower transaction racing CREATE INDEX was accepted and its record never
   * entered the index on any node) and {@code tx-fulltext-corpus-deltas} (BM25 counters drifting). The capabilities of a peer's
   * last answer are now held for those two framed sections while the peer cannot be reached, and given up only on an answer:
   * a reachable peer without them, a 404 on the capability route, or the peer leaving the configuration. Everything else that
   * reads the registry, the record-level float form above all, stays as strict as before.
   */
  @Nested
  class SectionCapabilityHeldThroughOutage {

    private static final String PEER = "localhost_2435";
    /** Local node first, then two peers that both derive onto {@code localhost:2490} (as in PeerCapabilityProbeTest.SharedAddressCapabilityProbe). */
    private static final String COLLAPSED_LIST    = "localhost:2434:2480,localhost:2435:2490,localhost:2436:2490";
    private static final String SHARED_PEER       = "localhost_2435";
    private static final String UNIDENTIFIED_PEER = "localhost_2436";

    // Regression for #12122: an unreachable peer keeps the framed sections it advertised past the ttl
    @Test
    void anUnreachablePeerKeepsTheFramedSectionsItAdvertisedPastTheTtl() {
      final AtomicLong now = new AtomicLong(1_000_000L);
      final RaftHAServer raft = newDetachedServer(now);
      final AtomicReference<IOException> failure = new AtomicReference<>();
      raft.setCapabilityProber((peerId, http, https, token) -> {
        if (failure.get() != null)
          throw failure.get();
        return new PeerCapabilityQuery.Advertisement(PEER, "26.11.1", PeerCapabilities.LOCAL);
      });
      raft.refreshPeerCapabilities();
      assertThat(raft.canStateTxPreparedAt()).as("every peer answered with the section").isTrue();

      // The peer goes down: the probe fails with a transport error, and its answer then ages past the TTL
      failure.set(new IOException("connection refused"));
      raft.refreshPeerCapabilities();
      now.addAndGet(PeerCapabilityRegistry.ADVERTISEMENT_TTL_MS * 10);
      raft.refreshPeerCapabilities();
      invalidateAll(raft);

      assertThat(raft.canStateTxPreparedAt()).as("tx-prepared-at-index survives an outage of the peer").isTrue();
      assertThat(raft.canStateFullTextCorpusDeltas()).as("tx-fulltext-corpus-deltas survives an outage of the peer").isTrue();
      assertThat(raft.canWriteFixedWidthFloatArrays()).as("the record-level float form is not a framed section: still strict")
          .isFalse();
      assertThat(raft.peersMissingCapability(PeerCapabilities.SCHEMA_DELTA)).as("every other reader stays strict").containsExactly(PEER);
      assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER)).as("the peer is still reported unknown").isNull();
    }

    /**
     * A cluster that declares no {@code http} ports and puts two peers on one host refuses each peer's own dial address on every
     * round (ADDRESS_REFUSED) and reaches the peer only at the shared address (#7256). That refusal is a property of this node's
     * configuration, not of the peer, and the peer is still asked every round: dropping its held answer there made the fix inert on
     * exactly that deployment shape, so a peer that is down keeps it as one with an address of its own does.
     */
    // Regression for #12122: a peer asked at a shared address keeps the framed sections through an outage
    @Test
    void aPeerAskedAtASharedAddressKeepsTheFramedSectionsThroughAnOutage() {
      final RaftHAServer raft = PeerCapabilityProbeTest.SharedAddressCapabilityProbe.newDetachedServer(COLLAPSED_LIST, -1);
      final AtomicReference<IOException> failure = new AtomicReference<>();
      raft.setCapabilityProber((peerId, http, https, token) -> {
        if (failure.get() != null)
          throw failure.get();
        return new PeerCapabilityQuery.Advertisement(SHARED_PEER, "26.11.1", PeerCapabilities.LOCAL);
      });
      raft.refreshPeerCapabilities();
      assertThat(raft.peersMissingFramedSection(PeerCapabilities.TX_PREPARED_AT_INDEX)).as("the peer answered at the shared address")
          .containsExactly(UNIDENTIFIED_PEER);

      failure.set(new IOException("connection refused"));
      raft.refreshPeerCapabilities();

      assertThat(raft.peersMissingFramedSection(PeerCapabilities.TX_PREPARED_AT_INDEX))
          .as("the peer is down, and its own address was refused only because it is shared: it keeps what it last advertised")
          .containsExactly(UNIDENTIFIED_PEER);
      assertThat(raft.peersMissingCapability(PeerCapabilities.SCHEMA_DELTA)).as("the strict read is unchanged")
          .contains(SHARED_PEER);
      assertThat(raft.getPeerCapabilityRegistry().unknownOf(SHARED_PEER).kind()).as("and the refusal is still what is reported")
          .isEqualTo(PeerCapabilityRegistry.UnknownKind.ADDRESS_REFUSED);
    }

    /** A 404 at the shared address names no peer, so every peer behind it gives up its hold, as a 404 on its own address would. */
    // Regression for #12122: a route missing answer at a shared address ends the hold of every peer behind it
    @Test
    void aRouteMissingAnswerAtASharedAddressEndsTheHoldOfEveryPeerBehindIt() {
      final RaftHAServer raft = PeerCapabilityProbeTest.SharedAddressCapabilityProbe.newDetachedServer(COLLAPSED_LIST, -1);
      final AtomicReference<IOException> failure = new AtomicReference<>();
      raft.setCapabilityProber((peerId, http, https, token) -> {
        if (failure.get() != null)
          throw failure.get();
        return new PeerCapabilityQuery.Advertisement(SHARED_PEER, "26.11.1", PeerCapabilities.LOCAL);
      });
      raft.refreshPeerCapabilities();

      failure.set(new PeerCapabilityQuery.RouteMissingException("HTTP 404"));
      raft.refreshPeerCapabilities();

      assertThat(raft.peersMissingFramedSection(PeerCapabilities.TX_PREPARED_AT_INDEX))
          .as("the address answered as a build without the capability route: nothing is held behind it")
          .containsExactlyInAnyOrder(SHARED_PEER, UNIDENTIFIED_PEER);
    }

    // Regression for #12122: a route missing answer ends the held capabilities
    @Test
    void aRouteMissingAnswerEndsTheHeldCapabilities() {
      final RaftHAServer raft = newDetachedServer(new AtomicLong(1_000_000L));
      final AtomicReference<IOException> failure = new AtomicReference<>();
      raft.setCapabilityProber((peerId, http, https, token) -> {
        if (failure.get() != null)
          throw failure.get();
        return new PeerCapabilityQuery.Advertisement(PEER, "26.11.1", PeerCapabilities.LOCAL);
      });
      raft.refreshPeerCapabilities();

      // The peer came back on a build without the capability route: its own answer, so the held capabilities are gone
      failure.set(new PeerCapabilityQuery.RouteMissingException("HTTP 404"));
      raft.refreshPeerCapabilities();
      invalidateAll(raft);

      assertThat(raft.canStateTxPreparedAt()).isFalse();
      assertThat(raft.canStateFullTextCorpusDeltas()).isFalse();

      // ...and an outage after that does not bring them back
      failure.set(new IOException("connection refused"));
      raft.refreshPeerCapabilities();
      invalidateAll(raft);
      assertThat(raft.canStateTxPreparedAt()).isFalse();
    }

    // Regression for #12122: a reachable peer that answers without the sections is a no even through a later outage
    @Test
    void aReachablePeerThatAnswersWithoutTheSectionsIsANoEvenThroughALaterOutage() {
      final RaftHAServer raft = newDetachedServer(new AtomicLong(1_000_000L));
      final AtomicReference<Set<String>> answer = new AtomicReference<>(PeerCapabilities.LOCAL);
      final AtomicReference<IOException> failure = new AtomicReference<>();
      raft.setCapabilityProber((peerId, http, https, token) -> {
        if (failure.get() != null)
          throw failure.get();
        return new PeerCapabilityQuery.Advertisement(PEER, "26.9.1", answer.get());
      });
      raft.refreshPeerCapabilities();

      // A downgrade: the peer answers, and its answer does not list the sections
      answer.set(Set.of(PeerCapabilities.SCHEMA_DELTA));
      raft.refreshPeerCapabilities();
      invalidateAll(raft);
      assertThat(raft.canStateTxPreparedAt()).isFalse();
      assertThat(raft.canStateFullTextCorpusDeltas()).isFalse();

      failure.set(new IOException("connection refused"));
      raft.refreshPeerCapabilities();
      invalidateAll(raft);
      assertThat(raft.canStateTxPreparedAt()).as("what is held is the LAST answer, not the best one").isFalse();
      assertThat(raft.explainMissingCapability(PeerCapabilities.TX_PREPARED_AT_INDEX)).contains(PEER).contains("connection refused");
    }

    // Regression for #12122: a peer that never answered is still a no
    @Test
    void aPeerThatNeverAnsweredIsStillANo() {
      final RaftHAServer raft = newDetachedServer(new AtomicLong(1_000_000L));
      raft.setCapabilityProber((peerId, http, https, token) -> {
        throw new IOException("connection refused");
      });
      assertThat(raft.canStateTxPreparedAt()).isFalse();
      assertThat(raft.canStateFullTextCorpusDeltas()).isFalse();
    }

    // Regression for #12122: a peer leaving the configuration or a refused address or a new registry term drops what was held
    @Test
    void aPeerLeavingTheConfigurationOrARefusedAddressOrANewRegistryTermDropsWhatWasHeld() {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();

      registry.record(registry.generation(), PEER, PeerCapabilities.LOCAL, "26.11.1");
      registry.suspend(registry.generation(), PEER, "connection refused", PeerCapabilityRegistry.UnknownKind.UNREACHABLE);
      assertThat(registry.peersMissingFramedSection(Set.of(PEER), PeerCapabilities.TX_PREPARED_AT_INDEX)).as("unreachable: held")
          .isEmpty();
      assertThat(registry.peersMissing(Set.of(PEER), PeerCapabilities.TX_PREPARED_AT_INDEX)).as("strict read").containsExactly(PEER);

      registry.retainOnly(registry.generation(), Set.of());
      assertThat(registry.peersMissingFramedSection(Set.of(PEER), PeerCapabilities.TX_PREPARED_AT_INDEX)).as("left the configuration")
          .containsExactly(PEER);

      registry.record(registry.generation(), PEER, PeerCapabilities.LOCAL, "26.11.1");
      registry.forget(registry.generation(), PEER, "no address", PeerCapabilityRegistry.UnknownKind.ADDRESS_REFUSED);
      assertThat(registry.peersMissingFramedSection(Set.of(PEER), PeerCapabilities.TX_PREPARED_AT_INDEX)).as("address refused")
          .containsExactly(PEER);

      registry.record(registry.generation(), PEER, PeerCapabilities.LOCAL, "26.11.1");
      registry.forget(registry.generation(), PEER, "shared address", PeerCapabilityRegistry.UnknownKind.ADDRESS_REFUSED, true);
      assertThat(registry.peersMissingFramedSection(Set.of(PEER), PeerCapabilities.TX_PREPARED_AT_INDEX))
          .as("an address refused while the peer is still asked elsewhere keeps the hold").isEmpty();

      registry.record(registry.generation(), PEER, PeerCapabilities.LOCAL, "26.11.1");
      registry.forget(registry.generation(), PEER, "HTTP 404", PeerCapabilityRegistry.UnknownKind.ROUTE_MISSING);
      assertThat(registry.peersMissingFramedSection(Set.of(PEER), PeerCapabilities.TX_PREPARED_AT_INDEX)).as("route missing")
          .containsExactly(PEER);

      registry.record(registry.generation(), PEER, PeerCapabilities.LOCAL, "26.11.1");
      registry.clear();
      assertThat(registry.peersMissingFramedSection(Set.of(PEER), PeerCapabilities.TX_PREPARED_AT_INDEX)).as("cleared")
          .containsExactly(PEER);
    }

    // Regression for #12122: a failed probe from an ended term does not drop what this term holds
    @Test
    void aFailedProbeFromAnEndedTermDoesNotDropWhatThisTermHolds() {
      final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
      final long endedTerm = registry.generation();
      registry.clear();
      registry.record(registry.generation(), PEER, PeerCapabilities.LOCAL, "26.11.1");

      registry.forget(endedTerm, PEER, "HTTP 404", PeerCapabilityRegistry.UnknownKind.ROUTE_MISSING);
      assertThat(registry.peersMissingFramedSection(Set.of(PEER), PeerCapabilities.TX_PREPARED_AT_INDEX)).isEmpty();
    }

    private static void invalidateAll(final RaftHAServer raft) {
      raft.invalidateTxPreparedAtCapability();
      raft.invalidateFullTextCorpusDeltaCapability();
      raft.invalidateFixedWidthFloatArraysCapability();
    }

    private static RaftHAServer newDetachedServer(final AtomicLong now) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481");

      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
      final RaftHAServer raft = new RaftHAServer(arcadeServer, config);
      raft.getPeerCapabilityRegistry().setClock(now::get);
      return raft;
    }
  }
}
