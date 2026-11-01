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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Tag;
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
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The sibling of {@link HaForwardedWriteIdempotencyIT} for the tests that make a follower resolve the WRONG address for
 * the leader, by rewriting its live resolved-HTTP-address map ({@code RaftHAServer.getHttpAddresses()}) for the length of
 * one test. Merged from {@code Issue7516BasicAuthForwardLoopIT} (one of three tests) and
 * {@code Issue6191FollowerForwardLoopIT} (two of five); their other tests are in {@link HaForwardedWriteIdempotencyIT}.
 * <ul>
 * <li><b>#6191</b>: when {@code arcadedb.ha.serverList} declares no {@code http} port, the address is derived from the
 * peer's Raft host plus THIS node's HTTP port, so on a cluster whose nodes differ by port every peer resolves to the node
 * doing the resolving, and a forwarded POST cycled, one held request thread per hop. The dial side now refuses an
 * address that is this node's own, and the receiving side refuses a write a peer already forwarded, which is the
 * general bound for an address that names the wrong PEER.</li>
 * <li><b>#7516</b>: {@code LeaderCommandForwarder} sent the cluster token, and with it the one-hop marker, only on the
 * branch that swaps a session token for a forwarded-user assertion, so a Basic-auth or API-token write relayed with the
 * client's own {@code Authorization} header carried no marker. Two followers whose resolved leader addresses name each
 * other - which neither node's {@code isOwnHttpAddress} check can see - forwarded it back and forth forever. The
 * cluster token now proves the hop, while identity substitution stays tied to {@code X-ArcadeDB-Forwarded-User}.</li>
 * </ul>
 * The ambiguity is injected into the live map rather than configured, so that cluster startup - which dials those same
 * addresses to probe peers - is not part of what is under test; the map is a {@code ConcurrentHashMap} the cluster's own
 * threads read throughout.
 * <p>
 * Stays on the per-test {@link BaseRaftHATest} fixture, so the merge saves no cluster boot. Each test restores the map
 * from a snapshot, but #6191's tests pause the capability monitor for the window (it would relearn the real address,
 * issue #9255) and restart it in the {@code finally}, and a restarted monitor runs its first round at once.
 * {@code stopCapabilityMonitor()} does not stop a round already in flight, so on a shared cluster that round healed the
 * NEXT injecting test's map mid-test (3 of 6 runs failed that way while these tests shared
 * {@link HaForwardedWriteIdempotencyIT}'s cluster).
 * <p>
 * The test tagged {@code slow} was tagged so in its source class; #6191's were not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HaForwardedWriteMisaddressedLeaderIT extends BaseRaftHATest {

  /**
   * #7516's request timeout. Short on purpose: a build without the fix ping-pongs the request between the two followers
   * forever, so this timeout - not an assertion - is what ends that test, long before the {@link Timeout} that fails it.
   */
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(20);

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  private static final String PASSWORD = "issue7516password";

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * Issue #7516, from {@code Issue7516BasicAuthForwardLoopIT}.
   * <p>
   * The reported defect. Both followers are told the leader lives where the other one does, so every hop lands on a
   * node that is not the leader and resolves the same wrong address back. The request has to come to rest in one hop
   * with the typed refusal, and nothing may have been written on the way.
   */
  @Tag("slow")
  @Test
  @Timeout(180)
  void aBasicAuthWriteBetweenTwoFollowersThatNameEachOtherIsRefusedInOneHop() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerA = firstFollower(leader);
    final int followerB = secondFollower(leader, followerA);

    final RaftHAServer raftA = getRaftPlugin(followerA).getRaftHAServer();
    final RaftHAServer raftB = getRaftPlugin(followerB).getRaftHAServer();
    final Map<RaftPeerId, String> addressesA = raftA.getHttpAddresses();
    final Map<RaftPeerId, String> addressesB = raftB.getHttpAddresses();
    final Map<RaftPeerId, String> declaredA = new HashMap<>(addressesA);
    final Map<RaftPeerId, String> declaredB = new HashMap<>(addressesB);
    final RaftPeerId leaderPeer = RaftPeerId.valueOf(peerIdForIndex(leader));

    final String created = "issue7516nevercreated";
    try {
      addressesA.put(leaderPeer, "localhost:" + getServer(followerB).getHttpServer().getPort());
      addressesB.put(leaderPeer, "localhost:" + getServer(followerA).getHttpServer().getPort());

      // Neither node's own self-address check can see this: each is dialling the other, not itself.
      assertThat(raftA.isOwnHttpAddress(raftA.getLeaderHttpAddress()))
          .as("the cycle this reproduces is exactly the one the self-address check cannot catch").isFalse();
      assertThat(raftB.isOwnHttpAddress(raftB.getLeaderHttpAddress())).isFalse();

      assertRefusedInOneHop(asRoot(followerA, "POST", "/api/v1/server/users",
          new JSONObject().put("name", created).put("password", PASSWORD).toString()), "POST /server/users");

      // The other family of call sites through the same forwarder: the POST /api/v1/server command path,
      // which has forwarded to the leader since issue #6191 and which the three REST routes only joined in
      // issue #7380. A fix that covered the routes and not the command would satisfy the line above.
      assertRefusedInOneHop(asRoot(followerA, "POST", "/api/v1/server",
          new JSONObject().put("command", "create user { \"name\": \"" + created + "\", \"password\": \""
              + PASSWORD + "\", \"databases\": {} }").toString()), "POST /server create user");
    } finally {
      addressesA.clear();
      addressesA.putAll(declaredA);
      addressesB.clear();
      addressesB.putAll(declaredB);
    }

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServer(i).getSecurity().existsUser(created))
          .as("server %d must not hold a user the forward never delivered", i).isFalse();
  }

  /**
   * Issue #6191, from {@code Issue6191FollowerForwardLoopIT}.
   * <p>
   * The reachable deployment: peers on one host, no {@code http} port declared. Before the fix this POSTed to itself and
   * never returned, so the timeout is the assertion of last resort.
   */
  @Test
  @Timeout(120)
  void aFollowerRefusesAWriteWhoseLeaderAddressResolvesToItself() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);

    final RaftHAServer raft = getRaftPlugin(follower).getRaftHAServer();
    // The live map, by contract (see getHttpAddresses): writing to it is how this test puts the running node
    // into the misconfigured state. A defensive copy there would leave these tests passing while injecting
    // nothing, so if that accessor ever starts returning one, this is the line that has to change with it.
    final Map<RaftPeerId, String> httpAddresses = raft.getHttpAddresses();
    final Map<RaftPeerId, String> declared = new HashMap<>(httpAddresses);
    try {
      // Paused for the window: the capability round would otherwise learn the real address back from the peers
      // (issue #9255) and undo the misconfiguration this test injects
      raft.stopCapabilityMonitor();
      httpAddresses.clear(); // nothing declared: every peer now derives to this node's own endpoint

      final String ownAddress = "localhost:" + getServer(follower).getHttpServer().getPort();
      assertThat(raft.getLeaderHttpAddress())
          .as("the derived leader address is this follower's own endpoint - the defect being guarded")
          .isEqualTo(ownAddress);
      assertThat(raft.isOwnHttpAddress(raft.getLeaderHttpAddress())).isTrue();

      assertThatThrownBy(() -> getServerDatabase(follower, getDatabaseName())
          .command("sql", "CREATE VERTEX TYPE Issue6191SelfForward"))
          .as("the write must be refused, not POSTed back to this node to be forwarded again")
          .isInstanceOf(ServerIsNotTheLeaderException.class)
          .hasMessageContaining("is this node's own")
          .hasMessageContaining(GlobalConfiguration.HA_SERVER_LIST.getKey());
    } finally {
      httpAddresses.putAll(declared);
      raft.startCapabilityMonitor();
    }
  }

  /**
   * Issue #6191, from {@code Issue6191FollowerForwardLoopIT}.
   * <p>
   * The cycle a local self-check cannot see: the resolved address names a real peer, just not the leader. The second
   * node has to be the one that stops it, and the refusal has to reach the first as itself - a
   * {@link ServerIsNotTheLeaderException} the caller can retry on, not a flattened transaction failure.
   * <p>
   * The <b>type</b> is the assertion that matters and it travels in the error body's {@code exception} field, which is
   * emitted in every server mode. The message text travels in {@code detail}, which
   * {@code AbstractServerHttpHandler.buildErrorBody} conceals when the server runs in production mode (pre-existing
   * behaviour, not specific to this path), so the text assertion here holds because tests run in the default
   * development mode.
   */
  @Test
  @Timeout(120)
  void aWriteForwardedOntoAFollowerIsRefusedInsteadOfForwardedAgain() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);
    final int otherFollower = secondFollower(leader, follower);

    final RaftHAServer raft = getRaftPlugin(follower).getRaftHAServer();
    final Map<RaftPeerId, String> httpAddresses = raft.getHttpAddresses();
    final Map<RaftPeerId, String> declared = new HashMap<>(httpAddresses);
    try {
      // Paused for the window: the capability round would otherwise learn the real address back from the peers
      // (issue #9255) and undo the misconfiguration this test injects
      raft.stopCapabilityMonitor();
      // This follower now believes the leader listens where the OTHER follower does.
      httpAddresses.put(RaftPeerId.valueOf(peerIdForIndex(leader)),
          "localhost:" + getServer(otherFollower).getHttpServer().getPort());

      assertThatThrownBy(() -> getServerDatabase(follower, getDatabaseName())
          .command("sql", "CREATE VERTEX TYPE Issue6191CrossForward"))
          .as("the node that received the forwarded write must refuse it rather than forward it on")
          .isInstanceOf(ServerIsNotTheLeaderException.class)
          .hasMessageContaining("already forwarded");
    } finally {
      httpAddresses.putAll(declared);
      raft.startCapabilityMonitor();
    }
  }

  // ---------------------------------------------------------------------------------------------

  private void assertRefusedInOneHop(final HttpResponse<String> response, final String route) {
    assertThat(response.statusCode())
        .as("%s: the second hop must refuse the write, not relay it back, body: %s", route, response.body())
        .isEqualTo(400);
    assertThat(response.body())
        .as("%s: the refusal the first follower relays to the client must say why", route)
        .contains("already forwarded");
  }

  private HttpResponse<String> asRoot(final int serverIndex, final String method, final String path,
      final String body) throws Exception {
    return send(serverIndex, method, path, body,
        b -> b.header("Authorization", basic("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)));
  }

  private HttpResponse<String> send(final int serverIndex, final String method, final String path,
      final String body, final UnaryOperator<HttpRequest.Builder> decorate) throws Exception {
    final int port = getServer(serverIndex).getHttpServer().getPort();
    HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + port + path))
        .timeout(REQUEST_TIMEOUT);
    builder = decorate.apply(builder);
    if (body != null) {
      builder.header("Content-Type", "application/json");
      builder.method(method, HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
    } else
      builder.method(method, HttpRequest.BodyPublishers.noBody());
    return HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }

  private static String basic(final String user, final String password) {
    return "Basic " + Base64.getEncoder()
        .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  private int firstFollower(final int leader) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader)
        return i;
    throw new IllegalStateException("no follower in a " + getServerCount() + "-node cluster");
  }

  private int secondFollower(final int leader, final int firstFollower) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader && i != firstFollower)
        return i;
    throw new IllegalStateException("no second follower in a " + getServerCount() + "-node cluster");
  }
}
