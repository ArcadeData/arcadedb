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

import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.apache.ratis.protocol.RaftPeerId;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Peer addressing tests that REWRITE a node's live resolved-HTTP-address map ({@link RaftHAServer#getHttpAddresses()}),
 * on the per-test {@link BaseRaftHATest} fixture: three servers, no configuration override. Merged from
 * {@code Issue6267AmbiguousAddressVisibilityIT} (two of three tests), {@code Issue10060SchemaRefusalNamesTheLeaderIT}
 * (one of two), {@code Issue10066SecurityRefusalNamesTheLeaderIT} (one of two) and
 * {@code Issue9255PeerHttpAddressLearningIT}. Their correctly-addressed controls are in {@link HaPeerAddressRoutingIT},
 * on a shared cluster.
 * <ul>
 * <li><b>#6267</b>: with no {@code http} port declared every peer derives to this node's own endpoint. The status
 * endpoint must flag each such peer as {@code httpAddressAmbiguous}, and the presence matrix must report a peer it
 * cannot identify as unreachable instead of dialling the shared address and crediting the peer with the local node's
 * databases (the same false all-clear the verify endpoint gave before #6221). The ambiguity is injected by emptying the
 * leader's map for the duration of one test, as {@link Issue6221VerifyFanOutGuardIT} does, rather than by starting the
 * cluster misconfigured, so cluster startup is not part of what is under test.</li>
 * <li><b>#10060</b> / <b>#10066</b>: a follower that cannot tell its peers apart (a same-host cluster declaring no HTTP
 * ports, #6191, reproduced with {@link #refuseForwardedWritesOn(int)}) resolves the leader to its OWN address. Its
 * refusal of a schema change (#10060) or of a {@code SecurityManager} user mutation (#10066) used to name that address,
 * sending the client back to the node that refused it; it now names the leader's server name.</li>
 * <li><b>#9255</b> (#9229): on a cluster whose Raft and HTTP ports are not in step, a node holding no address for a
 * member (one that never heard the declaration, or whose entry a removal dropped) derived one from the member's Raft
 * host plus its OWN HTTP port, i.e. itself, so it could not reach the member at all: no capability answer, so a security
 * entry gated on it was refused. The node now learns the address from the member's own capability requests, or relayed
 * by a member that holds it, confirmed by a probe the member answers under its own id; only a peer holding the cluster
 * token can offer one.</li>
 * </ul>
 * Why not on a shared cluster: every test here empties or edits the live map and pauses a capability monitor (directly
 * or through {@link #refuseForwardedWritesOn(int)}). The monitor learns real addresses back (#9255), and
 * {@code stopCapabilityMonitor()} does not wait for a round already in flight, so a round started by the previous test's
 * {@code startCapabilityMonitor()} can heal the next test's injection: seen in 3 of 6 runs when the same kind of tests
 * shared a cluster in {@link HaForwardedWriteIdempotencyIT}. Restoring the map is provable, draining a round is not.
 * The merge is for organization only and saves no cluster boot.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HaPeerAddressRoutingMapMutationIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  // ------------------------------------------------------------------ ambiguous address visibility (#6267)

  /** Issue #6267, from {@code Issue6267AmbiguousAddressVisibilityIT}. */
  @Test
  @Timeout(120)
  void anAmbiguousPeerAddressIsFlaggedInTheClusterStatus() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final RaftHAServer raft = getRaftPlugin(leader).getRaftHAServer();
    // The live map, by contract (see getHttpAddresses): writing to it is how this test puts the running node
    // into the misconfigured state.
    final Map<RaftPeerId, String> httpAddresses = raft.getHttpAddresses();
    final Map<RaftPeerId, String> declared = new HashMap<>(httpAddresses);
    try {
      // Paused for the window: the capability round would otherwise learn the real address back from the peers
      // (issue #9255) and undo the misconfiguration this test injects
      raft.stopCapabilityMonitor();
      httpAddresses.clear(); // nothing declared: every peer now derives to this node's own endpoint

      final JSONArray peers = HaPeerAddressRoutingIT.getCluster(getServer(leader), "").getJSONArray("peers");
      assertThat(peers.length()).isEqualTo(getServerCount());
      for (int i = 0; i < peers.length(); i++) {
        final JSONObject peer = peers.getJSONObject(i);
        assertThat(peer.getBoolean("httpAddressAmbiguous", false))
            .as("every peer resolves to this node's endpoint, so none of them is identified by it: %s", peer)
            .isTrue();
        assertThat(peer.getString("httpAddress", ""))
            .as("the resolved address is still reported - it is what the flag is about")
            .isNotEmpty();
      }
    } finally {
      httpAddresses.putAll(declared);
      raft.startCapabilityMonitor();
    }
  }

  /**
   * Issue #6267, from {@code Issue6267AmbiguousAddressVisibilityIT}.
   * <p>
   * The presence matrix asks each peer which databases it holds and attributes the answer to that peer, which makes it
   * exactly the kind of unattended dial {@code PeerDialAddress} exists to guard. Unguarded, every peer was queried on
   * the leader's own address and reported the leader's databases as its own.
   */
  @Test
  @Timeout(120)
  void thePresenceMatrixReportsAPeerItCannotIdentifyAsUnreachable() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final RaftHAServer raft = getRaftPlugin(leader).getRaftHAServer();
    final Map<RaftPeerId, String> httpAddresses = raft.getHttpAddresses();
    final Map<RaftPeerId, String> declared = new HashMap<>(httpAddresses);
    try {
      // Paused for the window: the capability round would otherwise learn the real address back from the peers
      // (issue #9255) and undo the misconfiguration this test injects
      raft.stopCapabilityMonitor();
      httpAddresses.clear();

      final JSONObject presence = HaPeerAddressRoutingIT.getCluster(getServer(leader), "?presence=true")
          .getJSONObject("databasePresence");
      final JSONArray unreachable = presence.getJSONArray("unreachable");

      assertThat(unreachable.length())
          .as("the two peers the leader cannot identify were not queried: %s", presence)
          .isEqualTo(getServerCount() - 1);
      for (int i = 0; i < unreachable.length(); i++)
        assertThat(unreachable.getString(i)).isNotEqualTo(raft.getLocalPeerId().toString());

      // And nothing was attributed to them: a peer that was never asked holds no databases in the matrix.
      final JSONArray databases = presence.getJSONArray("databases");
      for (int i = 0; i < databases.length(); i++) {
        final JSONObject database = databases.getJSONObject(i);
        final JSONArray present = database.getJSONArray("present");
        assertThat(present.length())
            .as("only the local node answered for '%s': %s", database.getString("name"), present)
            .isEqualTo(1);
        assertThat(present.getString(0)).isEqualTo(raft.getLocalPeerId().toString());
      }
    } finally {
      httpAddresses.putAll(declared);
      raft.startCapabilityMonitor();
    }
  }

  // ------------------------------------------------------------------ refusals that name the leader (#10060, #10066)

  /** Issue #10060, from {@code Issue10060SchemaRefusalNamesTheLeaderIT}. */
  @Test
  @Timeout(120)
  void aSchemaRefusalOnAFollowerThatCannotTellItsPeersApartNamesTheLeaderByName() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final RaftHAServer raft = getRaftPlugin(follower).getRaftHAServer();
    final Runnable restore = refuseForwardedWritesOn(follower);
    try {
      final ServerIsNotTheLeaderException refusal = HaPeerAddressRoutingIT.schemaRefusalOn(
          getServerDatabase(follower, getDatabaseName()), "Issue10060Ambiguous");
      final String leaderName = raft.getLeaderName();
      assertThat(leaderName).as("the follower must know which node leads").isNotBlank();
      assertThat(raft.isOwnHttpAddress(refusal.getLeaderAddress()))
          .as("the refusal must not send the client back to the follower that refused it (named '%s')", refusal.getLeaderAddress())
          .isFalse();
      assertThat(refusal.getLeaderAddress()).isEqualTo(leaderName);
    } finally {
      restore.run();
    }
  }

  /** Issue #10066, from {@code Issue10066SecurityRefusalNamesTheLeaderIT}. */
  @Test
  @Timeout(120)
  void aSecurityRefusalOnAFollowerThatCannotTellItsPeersApartNamesTheLeaderByName() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final RaftHAServer raft = getRaftPlugin(follower).getRaftHAServer();
    final Runnable restore = refuseForwardedWritesOn(follower);
    try {
      final ServerIsNotTheLeaderException refusal = HaPeerAddressRoutingIT.securityRefusalOn(
          getServer(follower).getSecurity(), "issue10066ambiguous");
      final String leaderName = raft.getLeaderName();
      assertThat(leaderName).as("the follower must know which node leads").isNotBlank();
      assertThat(raft.isOwnHttpAddress(refusal.getLeaderAddress()))
          .as("the refusal must not send the client back to the follower that refused it (named '%s')", refusal.getLeaderAddress())
          .isFalse();
      assertThat(refusal.getLeaderAddress()).isEqualTo(leaderName);
    } finally {
      restore.run();
    }
  }

  // ------------------------------------------------------------------ peer HTTP address learning (#9255)

  /** Issue #9255, from {@code Issue9255PeerHttpAddressLearningIT}. */
  @Test
  void aNodeThatLostAMembersAddressLearnsItFromTheMembersOwnRequests() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = (leader + 1) % getServerCount();
    final int member = (leader + 2) % getServerCount();

    forgetAndAwaitRelearnt(follower, member);
  }

  /** Issue #9255, from {@code Issue9255PeerHttpAddressLearningIT}. */
  @Test
  void aNodeLearnsAMembersAddressFromAnotherMembersRelayWhenTheMemberIsSilent() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = (leader + 1) % getServerCount();
    final int member = (leader + 2) % getServerCount();

    // The member sends no capability request at all, so it pushes nothing: only the leader's relay can carry its address
    final RaftHAServer silent = getRaftPlugin(member).getRaftHAServer();
    silent.stopCapabilityMonitor();
    try {
      forgetAndAwaitRelearnt(follower, member);
    } finally {
      silent.startCapabilityMonitor();
    }
  }

  /**
   * Issue #9255, from {@code Issue9255PeerHttpAddressLearningIT}.
   * <p>
   * The push is a statement one peer makes to another: the follow-up probe carries the cluster token, so a root user
   * authenticated with Basic credentials must not be able to plant an address, and a request with a wrong token is
   * refused before it is read. A peer's request, with the token, is heard.
   */
  @Test
  void onlyAPeerHoldingTheClusterTokenCanOfferAnAddress() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int member = (leader + 1) % getServerCount();
    final RaftHAServer node = getRaftPlugin(leader).getRaftHAServer();
    final String memberId = peerIdForIndex(member);

    // Paused so a capability round cannot clear the candidates between the request and the assertion. The member's entry
    // is dropped, as a removal does: while it equals this node's server-list declaration no offer is taken at all, which
    // would make the refusals below pass for the wrong reason
    final RaftPeerId memberPeer = RaftPeerId.valueOf(memberId);
    node.stopCapabilityMonitor();
    final String recorded = node.getHttpAddresses().remove(memberPeer);
    try {
      assertThat(capabilities(leader, memberId, b -> b.header("Authorization", basicRoot())).statusCode()).isEqualTo(200);
      assertThat(node.getPeerHttpAddressCandidates(memberId)).as("a Basic-auth root request plants nothing").isEmpty();

      assertThat(capabilities(leader, memberId, b -> b.header("Authorization", basicRoot())
          .header("X-ArcadeDB-Cluster-Token", "not-the-cluster-token")).statusCode()).isEqualTo(401);
      assertThat(node.getPeerHttpAddressCandidates(memberId)).as("a wrong token is refused before the body is read").isEmpty();

      assertThat(capabilities(leader, memberId, b -> b.header("X-ArcadeDB-Cluster-Token", node.getClusterToken())
          .header("X-ArcadeDB-Forwarded-User", "root")).statusCode()).isEqualTo(200);
      assertThat(node.getPeerHttpAddressCandidates(memberId)).as("a peer's request is heard").containsExactly("localhost:1");
    } finally {
      if (recorded != null)
        node.getHttpAddresses().put(memberPeer, recorded);
      node.startCapabilityMonitor();
    }
  }

  /** #9255: a capability request whose caller claims to be {@code callerPeerId} listening on {@code localhost:1}. */
  private HttpResponse<String> capabilities(final int serverIndex, final String callerPeerId,
      final UnaryOperator<HttpRequest.Builder> authenticate) throws Exception {
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:" + getServerHttpPort(serverIndex) + "/api/v1/cluster/capabilities"))
        .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json")
        .header(PostCapabilitiesHandler.CALLER_PEER_ID_HEADER, callerPeerId)
        .header(PostCapabilitiesHandler.CALLER_HTTP_ADDRESS_HEADER, "localhost:1")
        .POST(HttpRequest.BodyPublishers.ofString("{}"));
    try (final HttpClient client = HttpClient.newHttpClient()) {
      return client.send(authenticate.apply(builder).build(), HttpResponse.BodyHandlers.ofString());
    }
  }

  private static String basicRoot() {
    return "Basic " + Base64.getEncoder().encodeToString(
        ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private void forgetAndAwaitRelearnt(final int nodeIndex, final int memberIndex) {
    final RaftHAServer node = getRaftPlugin(nodeIndex).getRaftHAServer();
    final RaftPeerId memberId = RaftPeerId.valueOf(peerIdForIndex(memberIndex));
    final int memberHttpPort = getServerHttpPort(memberIndex);

    node.getHttpAddresses().remove(memberId);
    assertThat(RaftHAServer.extractPort(node.getPeerHttpAddress(memberId)))
        .as("derived from the member's Raft host plus this node's own port, which names this node").isNotEqualTo(memberHttpPort);

    Awaitility.await("the node learns the member's HTTP address back").atMost(60, TimeUnit.SECONDS)
        .pollInterval(250, TimeUnit.MILLISECONDS)
        .until(() -> RaftHAServer.extractPort(node.getHttpAddresses().get(memberId)) == memberHttpPort);
    assertThat(node.getPeerHttpAddress(memberId)).endsWith(":" + memberHttpPort);
    Awaitility.await("and the member's capabilities with it").atMost(30, TimeUnit.SECONDS)
        .pollInterval(250, TimeUnit.MILLISECONDS)
        .until(() -> node.getPeerCapabilityRegistry().freshAdvertisementOf(memberId.toString()) != null);
  }
}
