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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ha.raft.PeerCapabilityRegistry.UnknownKind;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue8655CapabilityUnknownKindTest {

  /** Local node, one peer on its own address, and two that collapse onto localhost:2490 so their dial is refused. */
  private static final String MIXED_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2490,localhost:2437:2490";
  private static final String PEER_A     = "localhost_2435";
  private static final String PEER_C     = "localhost_2436";
  private static final String PEER_D     = "localhost_2437";

  // ---------------------------------------------------------------------------------------------------------
  // The probe: a 404 is told apart from every other failure where it is raised
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void a404IsRaisedAsRouteMissing() {
    assertThatThrownBy(() -> PeerCapabilityQuery.checkStatus(404, "http://peer/api/v1/cluster/capabilities"))
        .isInstanceOf(PeerCapabilityQuery.RouteMissingException.class)
        .hasMessage("capability query to http://peer/api/v1/cluster/capabilities returned HTTP 404");
  }

  @Test
  void anyOtherStatusIsAPlainProbeFailure() {
    for (final int status : new int[] { 401, 403, 500, 503 })
      assertThatThrownBy(() -> PeerCapabilityQuery.checkStatus(status, "http://peer/x"))
          .as("HTTP %d is this node's view of the path to the peer, not the peer saying it predates the route", status)
          .isInstanceOf(IOException.class)
          .isNotInstanceOf(PeerCapabilityQuery.RouteMissingException.class)
          .hasMessageContaining("returned HTTP " + status);
  }

  @Test
  void a200IsNotAFailure() throws IOException {
    PeerCapabilityQuery.checkStatus(200, "http://peer/x");
  }

  // ---------------------------------------------------------------------------------------------------------
  // The refresh round: each failure path records its own kind
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void aPeerThatAnswers404IsRecordedAsRouteMissing() {
    final RaftHAServer raft = Issue7256SharedAddressCapabilityProbeTest.newDetachedServer(MIXED_LIST, -1);
    raft.setCapabilityProber(guardedFailure(new PeerCapabilityQuery.RouteMissingException(
        "capability query to http://localhost:2481/api/v1/cluster/capabilities returned HTTP 404")));

    raft.refreshPeerCapabilities();

    final PeerCapabilityRegistry registry = raft.getPeerCapabilityRegistry();
    assertThat(registry.unknownKindOf(PEER_A)).isEqualTo(UnknownKind.ROUTE_MISSING);
    assertThat(registry.unknownReasonOf(PEER_A))
        .as("the sentence an operator reads is unchanged")
        .contains("returned HTTP 404");
  }

  @Test
  void aPeerThisNodeCannotReachIsRecordedAsUnreachable() {
    final RaftHAServer raft = Issue7256SharedAddressCapabilityProbeTest.newDetachedServer(MIXED_LIST, -1);
    raft.setCapabilityProber(guardedFailure(new ConnectException("Connection refused")));

    raft.refreshPeerCapabilities();

    assertThat(raft.getPeerCapabilityRegistry().unknownKindOf(PEER_A))
        .as("a transport failure says nothing about what the leader's probe gets")
        .isEqualTo(UnknownKind.UNREACHABLE);
  }

  @Test
  void anotherHttpStatusIsRecordedAsUnreachable() {
    final RaftHAServer raft = Issue7256SharedAddressCapabilityProbeTest.newDetachedServer(MIXED_LIST, -1);
    raft.setCapabilityProber(guardedFailure(new IOException("capability query to x returned HTTP 401")));

    raft.refreshPeerCapabilities();

    assertThat(raft.getPeerCapabilityRegistry().unknownKindOf(PEER_A))
        .as("only the RouteMissingException type decides ROUTE_MISSING, never the message text")
        .isEqualTo(UnknownKind.UNREACHABLE);
  }

  @Test
  void anInterruptedProbeIsRecordedAsUnreachable() {
    final RaftHAServer raft = Issue7256SharedAddressCapabilityProbeTest.newDetachedServer(MIXED_LIST, -1);
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
  @Test
  void aPeerWithNoDialableAddressIsRecordedAsAddressRefusedEvenWhenTheSharedAddressAnswers404() {
    final RaftHAServer raft = Issue7256SharedAddressCapabilityProbeTest.newDetachedServer(MIXED_LIST, -1);
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

  @Test
  void aWriteFromAnEndedTermDoesNotSetAKind() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    final long stale = registry.generation();
    registry.clear();

    registry.suspend(stale, PEER_A, "capability query returned HTTP 404", UnknownKind.ROUTE_MISSING);
    registry.forget(stale, PEER_A, "capability query returned HTTP 404", UnknownKind.ROUTE_MISSING);

    assertThat(registry.unknownKindOf(PEER_A)).isNull();
  }

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

  @Test
  void theThreeArgumentWritesRecordUnreachable() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    registry.forget(registry.generation(), PEER_A, "something failed");
    assertThat(registry.unknownKindOf(PEER_A))
        .as("the kind that claims nothing about the peer itself, so a caller that does not classify cannot hard-gate")
        .isEqualTo(UnknownKind.UNREACHABLE);
  }

  /** Studio and the OpenAPI enum match these literals; a rename must break a test, not the gate. */
  @Test
  void theKindNamesAreTheOnesTheContractPublishes() {
    assertThat(Arrays.stream(UnknownKind.values()).map(Enum::name))
        .containsExactly("ROUTE_MISSING", "UNREACHABLE", "ADDRESS_REFUSED", "STALE");
  }

  // ---------------------------------------------------------------------------------------------------------
  // GET /api/v1/cluster: the row carries the kind next to the reason
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void thePeerRowCarriesTheKindNextToTheReason() {
    final JSONObject peerJson = new JSONObject();

    GetClusterHandler.putPeerCapabilitiesUnknown(peerJson,
        new PeerCapabilityRegistry.Unknown("capability query returned HTTP 404", UnknownKind.ROUTE_MISSING));

    assertThat(peerJson.getString("capabilitiesUnknownReason")).isEqualTo("capability query returned HTTP 404");
    assertThat(peerJson.getString("capabilitiesUnknownKind")).isEqualTo("ROUTE_MISSING");
  }

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
