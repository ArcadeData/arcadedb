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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.LeaderForwardContext;
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
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #7516: the one-hop rule that stops a follower-to-leader forward from cycling travels as the
 * {@code X-ArcadeDB-Forwarded-To-Leader} request header, and {@link LeaderForwardContext} honours it only
 * beside a valid cluster token. {@code LeaderCommandForwarder} used to send the token only on the branch
 * that swaps a per-node session token for a forwarded-user assertion; a write authenticated with
 * <b>Basic auth or an API token</b> is relayed with the client's own {@code Authorization} header instead,
 * and so carried no token and no marker at all. Two followers whose resolved leader addresses name each other - A believes the
 * leader is B, B believes it is A - then forwarded the same request back and forth. The dial-side
 * {@code isOwnHttpAddress} check does not see it: each node is dialling the <em>other</em> node.
 * <p>
 * The fix separates the two things the cluster token was doing at once. It proves the <b>hop</b> - so the
 * marker can now travel beside a relayed {@code Authorization} header - while identity substitution stays
 * tied to {@code X-ArcadeDB-Forwarded-User}, which a Basic/API-token forward deliberately does not send: an
 * API token carries scopes that resolving the user by name on the leader would silently discard.
 * <p>
 * The ambiguity is injected the way {@code Issue6191FollowerForwardLoopIT} injects it - by rewriting the
 * live resolved-HTTP-address map of the two followers for the duration of one test - because that reproduces
 * the production condition at the point where it matters without making cluster startup part of what is
 * under test.
 */
@Tag("slow")
class Issue7516BasicAuthForwardLoopIT extends BaseRaftHATest {

  /**
   * Short on purpose. A build without the fix ping-pongs the request between the two followers forever, so
   * this timeout - not an assertion - is what ends the first test, and it has to end it long before the
   * {@link Timeout} that fails the method.
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
   * The reported defect. Both followers are told the leader lives where the other one does, so every hop
   * lands on a node that is not the leader and resolves the same wrong address back. The request has to come
   * to rest in one hop with the typed refusal, and nothing may have been written on the way.
   */
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
   * The trust gate, which the fix must not widen: the marker is honoured only when a valid cluster token
   * travels beside it. A client that copies the header onto its own Basic-authenticated write - or a proxy
   * that relays unknown {@code X-ArcadeDB-*} headers through - must not be able to turn its own transparent
   * forward-to-leader into a refusal.
   */
  @Test
  @Timeout(180)
  void aMarkerWithoutTheClusterTokenDoesNotSuppressForwarding() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);

    final String name = "issue7516forgedmarker";
    final HttpResponse<String> response = send(follower, "POST", "/api/v1/server/users",
        new JSONObject().put("name", name).put("password", PASSWORD).toString(),
        b -> b.header("Authorization", basic("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS))
            .header(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true"));

    assertThat(response.statusCode())
        .as("a client's own marker must not stop this follower forwarding, body: %s", response.body())
        .isEqualTo(201);
    awaitUserOnEveryServer(name);
  }

  /**
   * The branch that always carried the marker, driven end to end for the first time. It authenticates to the
   * leader with the cluster token, which {@code LeaderCommandForwarder} read from the raw
   * {@code arcadedb.ha.clusterToken} setting - empty on every cluster that did not declare one explicitly,
   * because {@code ClusterTokenProvider} derives the effective token at startup and never writes it back into
   * the configuration. The forward then went out with no credentials at all and was answered 401.
   */
  @Test
  @Timeout(180)
  void aSessionTokenWriteOnAFollowerIsForwardedAndExecutedOnTheLeader() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);

    assertThat(getRaftPlugin(follower).getRaftHAServer().getClusterToken())
        .as("this cluster declares no explicit cluster token, so the effective one is derived at startup")
        .isNotBlank();
    assertThat(getServer(follower).getConfiguration()
        .getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN))
        .as("and the derived token is NOT written back into the configuration - reading it there is the defect")
        .isNullOrEmpty();

    final HttpResponse<String> login = send(follower, "POST", "/api/v1/login", null,
        b -> b.header("Authorization", basic("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)));
    assertThat(login.statusCode()).as("login on a follower, body: %s", login.body()).isEqualTo(200);
    final String sessionToken = new JSONObject(login.body()).getString("token");
    assertThat(sessionToken).startsWith("AU-");

    final String name = "issue7516sessiontoken";
    final HttpResponse<String> created = send(follower, "POST", "/api/v1/server/users",
        new JSONObject().put("name", name).put("password", PASSWORD).toString(),
        b -> b.header("Authorization", "Bearer " + sessionToken));

    assertThat(created.statusCode())
        .as("a session-token write forwarded to the leader must be authenticated there, body: %s", created.body())
        .isEqualTo(201);
    awaitUserOnEveryServer(name);
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

  private void awaitUserOnEveryServer(final String name) {
    await().atMost(30, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < getServerCount(); i++)
        if (!getServer(i).getSecurity().existsUser(name))
          return false;
      return true;
    });
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
