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
import com.arcadedb.server.TestServerHelper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue12122SectionCapabilityHeldThroughOutageTest {

  private static final String PEER = "localhost_2435";
  /** Local node first, then two peers that both derive onto {@code localhost:2490} (as in Issue7256SharedAddressCapabilityProbeTest). */
  private static final String COLLAPSED_LIST    = "localhost:2434:2480,localhost:2435:2490,localhost:2436:2490";
  private static final String SHARED_PEER       = "localhost_2435";
  private static final String UNIDENTIFIED_PEER = "localhost_2436";

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
  @Test
  void aPeerAskedAtASharedAddressKeepsTheFramedSectionsThroughAnOutage() {
    final RaftHAServer raft = Issue7256SharedAddressCapabilityProbeTest.newDetachedServer(COLLAPSED_LIST, -1);
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
  @Test
  void aRouteMissingAnswerAtASharedAddressEndsTheHoldOfEveryPeerBehindIt() {
    final RaftHAServer raft = Issue7256SharedAddressCapabilityProbeTest.newDetachedServer(COLLAPSED_LIST, -1);
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
        .containsExactly(SHARED_PEER, UNIDENTIFIED_PEER);
  }

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

  @Test
  void aPeerThatNeverAnsweredIsStillANo() {
    final RaftHAServer raft = newDetachedServer(new AtomicLong(1_000_000L));
    raft.setCapabilityProber((peerId, http, https, token) -> {
      throw new IOException("connection refused");
    });
    assertThat(raft.canStateTxPreparedAt()).isFalse();
    assertThat(raft.canStateFullTextCorpusDeltas()).isFalse();
  }

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
