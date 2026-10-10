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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.http.HttpServer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9255 (#9229, #9230): every node learns a member's HTTP address the same way, whichever node admitted it and
 * whether or not anyone declared its port. A member that does not answer on the address this node resolves for it is
 * asked on the candidates offered for it - the port it pushed in its own capability requests, an address another member
 * relayed - and the first one it answers on, under its own id, becomes its address on this node.
 * <p>
 * The cluster below differs by host and declares no HTTP port, with every peer's listener on 2490 and this node's on 2480:
 * the derived address of each peer (its Raft host plus THIS node's port) names nothing at all, which is the shape where
 * neither the old leader-side derive nor the follower's {@code raftPort + offset} guess was right.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9255PeerHttpAddressLearningTest {
  @RegisterExtension
  static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

  private static final String SERVER_LIST = "hostA:2434,hostB:2434,hostC:2434";
  private static final String LOCAL       = "hostA_2434";
  private static final String PEER_B      = "hostB_2434";
  private static final String PEER_C      = "hostC_2434";

  @Test
  void aPeerUnreachableOnItsDerivedAddressIsLearntFromThePortItPushed() {
    final RaftHAServer raft = newDetachedServer(SERVER_LIST);
    final ListeningPeers peers = new ListeningPeers().listen("hostB:2490", PEER_B).listen("hostC:2490", PEER_C);
    raft.setCapabilityProber(peers);

    // What PostCapabilitiesHandler hands over when B's capability request arrives: B resolves no address for itself here,
    // but its listener's port pairs with the Raft host this node already knows it by
    raft.offerCallerHttpAddress(PEER_B, null, 2490);
    raft.refreshPeerCapabilities();

    assertThat(peers.calls).as("the derived address first, then the candidate").containsSubsequence(PEER_B + "@hostB:2480",
        PEER_B + "@hostB:2490");
    assertThat(raft.getPeerHttpAddress(RaftPeerId.valueOf(PEER_B))).isEqualTo("hostB:2490");
    assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_B)).isNotNull();
    assertThat(raft.getPeerHttpAddressCandidates(PEER_B)).as("nothing offered is needed once the peer answered").isEmpty();
    // C pushed nothing and nobody relayed it: still unknown, and still derived
    assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_C)).isNull();
    assertThat(raft.getRelayablePeerHttpAddresses()).containsExactly(Map.entry(PEER_B, "hostB:2490"));
  }

  /** A member's relay is enough, within the same round: C relays the address it confirmed for B. */
  @Test
  void aPeerIsLearntFromTheAddressAnotherMemberRelays() {
    final RaftHAServer raft = newDetachedServer(SERVER_LIST);
    raft.offerCallerHttpAddress(PEER_C, "hostC:2490", -1);
    final ListeningPeers peers = new ListeningPeers().listen("hostB:2490", PEER_B)
        .listen("hostC:2490", PEER_C, Map.of(PEER_B, "hostB:2490"));
    raft.setCapabilityProber(peers);

    raft.refreshPeerCapabilities();

    assertThat(raft.getPeerHttpAddress(RaftPeerId.valueOf(PEER_C))).isEqualTo("hostC:2490");
    assertThat(raft.getPeerHttpAddress(RaftPeerId.valueOf(PEER_B))).isEqualTo("hostB:2490");
    assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_B)).isNotNull();
  }

  /**
   * An offered address is believed only when the peer it was offered for answers on it: an address that another node
   * answers is not recorded, and is not dialled again on the next round.
   */
  @Test
  void aCandidateAnsweredByAnotherPeerIsNeverRecorded() {
    final RaftHAServer raft = newDetachedServer(SERVER_LIST);
    final ListeningPeers peers = new ListeningPeers().listen("hostB:2490", PEER_C);
    raft.setCapabilityProber(peers);

    raft.offerPeerHttpAddress(PEER_B, "hostB:2490");
    raft.refreshPeerCapabilities();

    assertThat(raft.getHttpAddresses()).doesNotContainKey(RaftPeerId.valueOf(PEER_B));
    assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_B)).isNull();

    peers.calls.clear();
    raft.refreshPeerCapabilities();
    assertThat(peers.calls).as("the failed candidate is left alone for a while").doesNotContain(PEER_B + "@hostB:2490");
  }

  /** The address this node's own server list declares is the operator's: nothing heard from another node replaces it. */
  @Test
  void aServerListDeclarationInForceIsNeverReplaced() {
    final RaftHAServer raft = newDetachedServer("hostA:2434:2480,hostB:2434:2470,hostC:2434:2490");
    final ListeningPeers peers = new ListeningPeers().listen("hostB:2490", PEER_B);
    raft.setCapabilityProber(peers);

    raft.offerPeerHttpAddress(PEER_B, "hostB:2490");
    assertThat(raft.getPeerHttpAddressCandidates(PEER_B)).isEmpty();
    raft.refreshPeerCapabilities();
    assertThat(raft.getPeerHttpAddress(RaftPeerId.valueOf(PEER_B))).isEqualTo("hostB:2470");

    // Once the entry is gone - the peer was removed and admitted again - the declaration no longer holds
    raft.getHttpAddresses().remove(RaftPeerId.valueOf(PEER_B));
    raft.offerPeerHttpAddress(PEER_B, "hostB:2490");
    raft.refreshPeerCapabilities();
    assertThat(raft.getPeerHttpAddress(RaftPeerId.valueOf(PEER_B))).isEqualTo("hostB:2490");
  }

  /** Neither this node nor a stranger outside the configuration can be offered an address. */
  @Test
  void anOfferForThisNodeOrForANonMemberIsDropped() {
    final RaftHAServer raft = newDetachedServer(SERVER_LIST);

    raft.offerCallerHttpAddress(LOCAL, "hostA:2499", 2499);
    raft.offerCallerHttpAddress("stranger_2434", "stranger:2490", 2490);
    raft.offerPeerHttpAddress(PEER_B, "not an address");

    assertThat(raft.getPeerHttpAddressCandidates(LOCAL)).isEmpty();
    assertThat(raft.getPeerHttpAddressCandidates("stranger_2434")).isEmpty();
    assertThat(raft.getPeerHttpAddressCandidates(PEER_B)).isEmpty();
  }

  /** What this node says about itself in every capability request. */
  @Test
  void theCapabilityRequestCarriesThisNodesOwnEndpoint() {
    final JSONObject document = newDetachedServer(SERVER_LIST).capabilityRequestDocument();

    assertThat(document.getString(PostCapabilitiesHandler.CALLER_PEER_ID, "")).isEqualTo(LOCAL);
    assertThat(document.getString(PostCapabilitiesHandler.CALLER_HTTP_ADDRESS, "")).isEqualTo("hostA:2480");
    assertThat(document.getInt(PostCapabilitiesHandler.CALLER_HTTP_PORT, -1)).isEqualTo(2480);
  }

  /** A peer answering on the address this node derives is confirmed there and relayed from then on. */
  @Test
  void aPeerAnsweringOnItsDerivedAddressIsRecordedThere() {
    final RaftHAServer raft = newDetachedServer(SERVER_LIST);
    raft.setCapabilityProber(new ListeningPeers().listen("hostB:2480", PEER_B).listen("hostC:2480", PEER_C));

    raft.refreshPeerCapabilities();

    assertThat(raft.getRelayablePeerHttpAddresses()).containsExactlyInAnyOrderEntriesOf(
        Map.of(PEER_B, "hostB:2480", PEER_C, "hostC:2480"));
  }

  /** Every peer of the list listens on one of these addresses; a probe anywhere else finds nothing. */
  private static final class ListeningPeers implements RaftHAServer.CapabilityProber {
    private final List<String>                     calls     = new ArrayList<>();
    private final Map<String, String>              listeners = new HashMap<>();
    private final Map<String, Map<String, String>> relays    = new HashMap<>();

    ListeningPeers listen(final String address, final String peerId) {
      return listen(address, peerId, Map.of());
    }

    ListeningPeers listen(final String address, final String peerId, final Map<String, String> relayed) {
      listeners.put(address, peerId);
      relays.put(address, relayed);
      return this;
    }

    @Override
    public PeerCapabilityQuery.Advertisement probe(final String expectedPeerId, final String httpAddress,
        final String httpsAddress, final String clusterToken) throws IOException {
      calls.add(expectedPeerId + "@" + httpAddress);
      final String answering = listeners.get(httpAddress);
      if (answering == null)
        throw new IOException("connection refused: " + httpAddress);
      // The binding PeerCapabilityQuery.parse enforces on a real reply
      if (expectedPeerId != null && !expectedPeerId.equals(answering))
        throw new IOException("capability query to " + httpAddress + " for peer '" + expectedPeerId
            + "' was answered by peer '" + answering + "'");
      return new PeerCapabilityQuery.Advertisement(answering, "test", Set.of(PeerCapabilities.SCHEMA_DELTA), false, Set.of(),
          relays.get(httpAddress));
    }
  }

  private static RaftHAServer newDetachedServer(final String serverList) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList);
    final FakeArcadeDBServer arcadeServer = FakeArcadeDBServer.create("ArcadeDB_0", new ContextConfiguration());
    final HttpServer httpServer = HTTP_SERVERS.listeningOn(arcadeServer, 2480);
    arcadeServer.httpServer(httpServer);
    return new RaftHAServer(arcadeServer, config);
  }
}
