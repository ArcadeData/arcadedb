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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.LeaderForwardContext;
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
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #7380: {@code POST}, {@code PUT} and {@code DELETE /api/v1/server/users} called
 * {@code ServerSecurity.createUserClusterWide} / {@code updateUserClusterWide} /
 * {@code dropUserClusterWide} on whichever node served the request, which on an HA cluster submits a Raft
 * entry from a follower. The same three operations were already gated on the two other ways in: the
 * {@code POST /api/v1/server} command path forwards to the leader, and the gRPC admin service refuses
 * (issues #7304, #7309). The REST routes now forward, like the rest of the HTTP API.
 * <p>
 * The distinguishing assertion is the <b>one-hop refusal</b>, not the happy path: a request that arrives on
 * a follower already marked as forwarded by a peer has to be refused there. A route that ignores the leader
 * gate altogether - the state this issue reports - never looks at the marker and answers 201/200 instead,
 * whatever Ratis would have done with the entry it submitted. The happy-path method is the control that
 * keeps the refusals honest.
 */
@Tag("slow")
class Issue7380RestUserRoutesLeaderGateIT extends BaseRaftHATest {

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  private static final String PASSWORD     = "issue7380password";
  private static final String NEW_PASSWORD = "issue7380password2";

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * Every write route that reaches a cluster-wide security mutator has to refuse a request a peer already
   * forwarded, rather than run the mutation where the request landed. The marker is sent the way a peer
   * sends it - with the cluster token, the only form in which it is trusted (see {@link LeaderForwardContext}).
   * <p>
   * {@code POST /api/v1/server} is asserted alongside the three REST routes because the forwarding moved
   * into a shared collaborator: without it, a refactor that broke the command path would still be green.
   */
  @Test
  @Timeout(180)
  void theThreeRestUserRoutesRefuseARequestAPeerAlreadyForwarded() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);

    final String clusterToken = getRaftPlugin(follower).getRaftHAServer().getClusterToken();
    assertThat(clusterToken).as("peers authenticate to each other with a cluster token").isNotBlank();

    // An existing user for PUT and DELETE to aim at, created the normal way (on the leader) so it is
    // present on every node before the refusals are driven.
    final String existing = "issue7380marked";
    assertThat(asRoot(leader, "POST", "/api/v1/server/users",
        new JSONObject().put("name", existing).put("password", PASSWORD).toString()).statusCode())
        .isEqualTo(201);
    awaitUserOnEveryServer(existing, true);

    final String created = "issue7380nevercreated";

    assertRefusedAsAlreadyForwarded(
        asPeer(follower, clusterToken, "POST", "/api/v1/server/users",
            new JSONObject().put("name", created).put("password", PASSWORD).toString()),
        "POST /server/users");

    assertRefusedAsAlreadyForwarded(
        asPeer(follower, clusterToken, "PUT", "/api/v1/server/users?name=" + existing,
            new JSONObject().put("password", NEW_PASSWORD).toString()),
        "PUT /server/users");

    assertRefusedAsAlreadyForwarded(
        asPeer(follower, clusterToken, "DELETE", "/api/v1/server/users?name=" + existing, null),
        "DELETE /server/users");

    assertRefusedAsAlreadyForwarded(
        asPeer(follower, clusterToken, "POST", "/api/v1/server",
            new JSONObject().put("command", "create user { \"name\": \"" + created + "\", \"password\": \"" + PASSWORD
                + "\", \"databases\": {} }").toString()),
        "POST /server create user");

    // Nothing may have been written on the way to any of those refusals.
    for (int i = 0; i < getServerCount(); i++) {
      assertThat(getServer(i).getSecurity().existsUser(created))
          .as("server %d must not hold a user every route refused to create", i).isFalse();
      assertThat(getServer(i).getSecurity().existsUser(existing))
          .as("server %d must still hold the user DELETE refused to drop", i).isTrue();
    }
    // One wrong-password attempt, deliberately: the brute-force guard locks a principal out after five
    // within 30 seconds, which would then make the positive check below fail for the wrong reason.
    assertThat(canLogIn(leader, existing, NEW_PASSWORD))
        .as("the password PUT refused to change must not have changed").isFalse();
    assertThat(canLogIn(leader, existing, PASSWORD))
        .as("the original password must still work").isTrue();
  }

  /**
   * The control the refusals need: with the cluster's real addresses, each of the three routes called on a
   * follower is forwarded to the leader, executed there, and replicated back. Without it a build that
   * refused every write on a follower would satisfy the test above.
   */
  @Test
  @Timeout(180)
  void theThreeRestUserRoutesCalledOnAFollowerAreExecutedOnTheLeader() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);

    final String name = "issue7380forwarded";

    final HttpResponse<String> created = asRoot(follower, "POST", "/api/v1/server/users",
        new JSONObject().put("name", name).put("password", PASSWORD).toString());
    assertThat(created.statusCode()).as("POST on a follower, body: %s", created.body()).isEqualTo(201);
    awaitUserOnEveryServer(name, true);

    // Read after awaitUserOnEveryServer above, so every node already holds this same hash.
    final String originalHash = passwordHashOn(leader, name);

    final HttpResponse<String> updated = asRoot(follower, "PUT", "/api/v1/server/users?name=" + name,
        new JSONObject().put("password", NEW_PASSWORD).toString());
    assertThat(updated.statusCode()).as("PUT on a follower, body: %s", updated.body()).isEqualTo(200);
    // The stored hash rather than a login attempt: five failed logins in 30 seconds lock a principal out, so
    // polling for the new password to take effect would be a test that sabotages itself.
    //
    // On EVERY node, not just the leader. The claim being tested is that the forwarded update replicates,
    // and a version that only reached the node it executed on would satisfy a leader-only wait and then be
    // swept away by the DELETE below before anything noticed.
    await().atMost(30, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < getServerCount(); i++)
        if (originalHash.equals(passwordHashOn(i, name)))
          return false;
      return true;
    });
    assertThat(canLogIn(leader, name, NEW_PASSWORD))
        .as("the password the forwarded PUT set must authenticate on the leader").isTrue();

    final HttpResponse<String> dropped = asRoot(follower, "DELETE", "/api/v1/server/users?name=" + name, null);
    assertThat(dropped.statusCode()).as("DELETE on a follower, body: %s", dropped.body()).isEqualTo(200);
    awaitUserOnEveryServer(name, false);

    // The command path through the same collaborator. It dials the path of the request being served rather
    // than a hard-coded one now, so a build where that resolved to something undialable would answer here
    // and nowhere else - the refusals above never get as far as building a URL.
    final String viaCommand = "issue7380viacommand";
    final HttpResponse<String> command = asRoot(follower, "POST", "/api/v1/server",
        new JSONObject().put("command", "create user { \"name\": \"" + viaCommand + "\", \"password\": \""
            + PASSWORD + "\", \"databases\": {} }").toString());
    assertThat(command.statusCode())
        .as("POST /server 'create user' on a follower, body: %s", command.body()).isEqualTo(200);
    awaitUserOnEveryServer(viaCommand, true);
  }

  // ---------------------------------------------------------------------------------------------

  private void assertRefusedAsAlreadyForwarded(final HttpResponse<String> response, final String route) {
    assertThat(response.statusCode())
        .as("%s arriving already forwarded must be refused on this follower, body: %s", route, response.body())
        .isEqualTo(400);
    assertThat(response.body()).as("%s refusal must say why", route).contains("already forwarded");
  }

  private void awaitUserOnEveryServer(final String name, final boolean present) {
    await().atMost(30, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < getServerCount(); i++)
        if (getServer(i).getSecurity().existsUser(name) != present)
          return false;
      return true;
    });
  }

  /**
   * Whether these credentials authenticate on that node. {@code POST /api/v1/login} is the cheapest
   * question that actually checks the password: {@code existsUser} would answer true for a user whose
   * password never changed.
   */
  private boolean canLogIn(final int serverIndex, final String user, final String password) throws Exception {
    return send(serverIndex, "POST", "/api/v1/login", null,
        b -> b.header("Authorization", basic(user, password))).statusCode() == 200;
  }

  private String passwordHashOn(final int serverIndex, final String name) {
    return getServer(serverIndex).getSecurity().getUser(name).getPassword();
  }

  private HttpResponse<String> asRoot(final int serverIndex, final String method, final String path,
      final String body) throws Exception {
    return send(serverIndex, method, path, body,
        b -> b.header("Authorization", basic("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)));
  }

  /**
   * A request shaped the way a cluster peer sends a forwarded one: cluster-token authentication plus the
   * marker that says a peer already relayed it.
   */
  private HttpResponse<String> asPeer(final int serverIndex, final String clusterToken, final String method,
      final String path, final String body) throws Exception {
    return send(serverIndex, method, path, body, b -> b
        .header("X-ArcadeDB-Cluster-Token", clusterToken)
        .header("X-ArcadeDB-Forwarded-User", "root")
        .header(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true"));
  }

  private HttpResponse<String> send(final int serverIndex, final String method, final String path,
      final String body, final UnaryOperator<HttpRequest.Builder> decorate) throws Exception {
    final int port = getServer(serverIndex).getHttpServer().getPort();
    HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + port + path))
        .timeout(Duration.ofSeconds(60));
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
}
