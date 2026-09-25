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
import com.arcadedb.server.security.ServerSecurity;
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
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #8109 (absorbing #7826): the REST group and API-token routes ran their cluster-wide security mutation on
 * whichever node served the request, while the user routes had forwarded to the leader since issue #7380. With
 * every security mutation on the leader, the {@code ServerSecurity} monitor serialises the cluster rather than one
 * node, and the compare-and-set of issue #7509 is a backstop rather than the only defence.
 * <p>
 * The distinguishing assertion is the <b>one-hop refusal</b>, as in {@link Issue7380RestUserRoutesLeaderGateIT}: a
 * request that arrives on a follower already marked as forwarded by a peer must be refused there. A route that does
 * not forward never looks at the marker and answers 200/201 instead. The happy-path method is the control.
 * <p>
 * openCypher {@code CREATE USER} / {@code ALTER USER} / {@code DROP USER}, which #7826 also listed, needed no change:
 * the statement is not idempotent, so {@code RaftReplicatedDatabase.command} forwards it to the leader before the
 * engine ever reaches {@code SecurityManager}. The same one-hop refusal proves that here, so a change to that routing
 * cannot silently put those commands back on the follower.
 */
@Tag("slow")
class Issue8109SecurityMutationsReachTheLeaderIT extends BaseRaftHATest {

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  private static final String PASSWORD = "issue8109password";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(180)
  void everySecurityMutationRouteRefusesARequestAPeerAlreadyForwarded() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);

    final String clusterToken = getRaftPlugin(follower).getRaftHAServer().getClusterToken();
    assertThat(clusterToken).as("peers authenticate to each other with a cluster token").isNotBlank();

    // Existing state for the DELETE routes to aim at, created on the leader so it is on every node first.
    final String existingGroup = "issue8109existinggroup";
    assertThat(asRoot(leader, "POST", "/api/v1/server/groups",
        new JSONObject().put("database", "*").put("name", existingGroup).toString()).statusCode()).isEqualTo(200);
    awaitOnEveryServer(s -> hasGroup(s, existingGroup), "group " + existingGroup + " present");

    final String existingToken = "issue8109existingtoken";
    assertThat(asRoot(leader, "POST", "/api/v1/server/api-tokens",
        new JSONObject().put("name", existingToken).toString()).statusCode()).isEqualTo(201);
    awaitOnEveryServer(s -> tokenHashOf(s, existingToken) != null, "token " + existingToken + " present");
    final String existingTokenHash = tokenHashOf(getServer(leader).getSecurity(), existingToken);

    final String newGroup = "issue8109nevercreatedgroup";
    final String newToken = "issue8109nevercreatedtoken";
    final String newUser = "issue8109nevercreateduser";

    assertRefusedAsAlreadyForwarded(asPeer(follower, clusterToken, "POST", "/api/v1/server/groups",
        new JSONObject().put("database", "*").put("name", newGroup).toString()), "POST /server/groups");
    assertRefusedAsAlreadyForwarded(asPeer(follower, clusterToken, "DELETE",
        "/api/v1/server/groups?database=*&name=" + existingGroup, null), "DELETE /server/groups");
    assertRefusedAsAlreadyForwarded(asPeer(follower, clusterToken, "POST", "/api/v1/server/api-tokens",
        new JSONObject().put("name", newToken).toString()), "POST /server/api-tokens");
    assertRefusedAsAlreadyForwarded(asPeer(follower, clusterToken, "DELETE",
        "/api/v1/server/api-tokens?token=" + existingTokenHash, null), "DELETE /server/api-tokens");
    assertRefusedAsAlreadyForwarded(asPeer(follower, clusterToken, "POST", "/api/v1/command/" + getDatabaseName(),
            cypher("CREATE USER " + newUser + " SET PASSWORD '" + PASSWORD + "'")),
        "openCypher CREATE USER over /api/v1/command");

    for (int i = 0; i < getServerCount(); i++) {
      final ServerSecurity security = getServer(i).getSecurity();
      assertThat(hasGroup(security, newGroup)).as("server %d must not hold the refused group", i).isFalse();
      assertThat(hasGroup(security, existingGroup)).as("server %d must still hold the group DELETE refused", i)
          .isTrue();
      assertThat(tokenHashOf(security, newToken)).as("server %d must not hold the refused token", i).isNull();
      assertThat(tokenHashOf(security, existingToken)).as("server %d must still hold the token DELETE refused", i)
          .isNotNull();
      assertThat(security.existsUser(newUser)).as("server %d must not hold the refused user", i).isFalse();
    }
  }

  /**
   * A plaintext token handed to {@code DELETE /server/api-tokens} is refused on the node that received it, before
   * the forward: relaying it would copy the live token into a second node's request path. Sent marked as already
   * forwarded, so a build that forwarded first would answer with the one-hop refusal instead.
   */
  @Test
  @Timeout(180)
  void aPlaintextTokenIsRefusedOnTheFollowerBeforeTheForward() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);
    final String clusterToken = getRaftPlugin(follower).getRaftHAServer().getClusterToken();

    final HttpResponse<String> minted = asRoot(leader, "POST", "/api/v1/server/api-tokens",
        new JSONObject().put("name", "issue8109plaintext").toString());
    assertThat(minted.statusCode()).as("mint on the leader, body: %s", minted.body()).isEqualTo(201);
    final String plaintext = new JSONObject(minted.body()).getJSONObject("result").getString("token");

    final HttpResponse<String> refused = asPeer(follower, clusterToken, "DELETE",
        "/api/v1/server/api-tokens?token=" + plaintext, null);
    assertThat(refused.statusCode()).as("body: %s", refused.body()).isEqualTo(400);
    assertThat(refused.body()).contains("token hash").doesNotContain("already forwarded");
  }

  /**
   * The control: with the cluster's real addresses, each route called on a follower is executed on the leader and
   * replicated back. Without it a build that refused every write on a follower would satisfy the test above.
   */
  @Test
  @Timeout(180)
  void everySecurityMutationRouteCalledOnAFollowerIsExecutedOnTheLeader() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);

    final String group = "issue8109forwardedgroup";
    final HttpResponse<String> savedGroup = asRoot(follower, "POST", "/api/v1/server/groups",
        new JSONObject().put("database", "*").put("name", group).toString());
    assertThat(savedGroup.statusCode()).as("POST /server/groups on a follower, body: %s", savedGroup.body())
        .isEqualTo(200);
    awaitOnEveryServer(s -> hasGroup(s, group), "group " + group + " present");

    final HttpResponse<String> deletedGroup = asRoot(follower, "DELETE",
        "/api/v1/server/groups?database=*&name=" + group, null);
    assertThat(deletedGroup.statusCode()).as("DELETE /server/groups on a follower, body: %s", deletedGroup.body())
        .isEqualTo(200);
    awaitOnEveryServer(s -> !hasGroup(s, group), "group " + group + " gone");

    final String token = "issue8109forwardedtoken";
    final HttpResponse<String> minted = asRoot(follower, "POST", "/api/v1/server/api-tokens",
        new JSONObject().put("name", token).toString());
    assertThat(minted.statusCode()).as("POST /server/api-tokens on a follower, body: %s", minted.body())
        .isEqualTo(201);
    assertThat(new JSONObject(minted.body()).getJSONObject("result").getString("token", ""))
        .as("the plaintext token the leader minted must be relayed to the client").isNotBlank();
    awaitOnEveryServer(s -> tokenHashOf(s, token) != null, "token " + token + " present");

    final String tokenHash = tokenHashOf(getServer(leader).getSecurity(), token);
    final HttpResponse<String> revoked = asRoot(follower, "DELETE", "/api/v1/server/api-tokens?token=" + tokenHash,
        null);
    assertThat(revoked.statusCode()).as("DELETE /server/api-tokens on a follower, body: %s", revoked.body())
        .isEqualTo(200);
    awaitOnEveryServer(s -> tokenHashOf(s, token) == null, "token " + token + " gone");

    // openCypher user administration through the database command route of a follower.
    final String user = "issue8109cypheruser";
    final HttpResponse<String> created = asRoot(follower, "POST", "/api/v1/command/" + getDatabaseName(),
        cypher("CREATE USER " + user + " SET PASSWORD '" + PASSWORD + "'"));
    assertThat(created.statusCode()).as("CREATE USER on a follower, body: %s", created.body()).isEqualTo(200);
    awaitOnEveryServer(s -> s.existsUser(user), "user " + user + " present");

    final String originalHash = getServer(leader).getSecurity().getUser(user).getPassword();
    final HttpResponse<String> altered = asRoot(follower, "POST", "/api/v1/command/" + getDatabaseName(),
        cypher("ALTER USER " + user + " SET PASSWORD '" + PASSWORD + "2'"));
    assertThat(altered.statusCode()).as("ALTER USER on a follower, body: %s", altered.body()).isEqualTo(200);
    awaitOnEveryServer(s -> !originalHash.equals(s.getUser(user).getPassword()), "user " + user + " password changed");

    final HttpResponse<String> dropped = asRoot(follower, "POST", "/api/v1/command/" + getDatabaseName(),
        cypher("DROP USER " + user));
    assertThat(dropped.statusCode()).as("DROP USER on a follower, body: %s", dropped.body()).isEqualTo(200);
    awaitOnEveryServer(s -> !s.existsUser(user), "user " + user + " gone");
  }

  // ---------------------------------------------------------------------------------------------

  private static String cypher(final String command) {
    return new JSONObject().put("language", "opencypher").put("command", command).toString();
  }

  private static boolean hasGroup(final ServerSecurity security, final String name) {
    return security.groupsToJSON().getJSONObject("databases").getJSONObject("*", new JSONObject())
        .getJSONObject("groups", new JSONObject()).has(name);
  }

  private static String tokenHashOf(final ServerSecurity security, final String name) {
    for (final JSONObject token : security.getApiTokenConfiguration().listTokens())
      if (name.equals(token.getString("name", null)))
        return token.getString("tokenHash");
    return null;
  }

  private void assertRefusedAsAlreadyForwarded(final HttpResponse<String> response, final String route) {
    assertThat(response.statusCode())
        .as("%s arriving already forwarded must be refused on this follower, body: %s", route, response.body())
        .isEqualTo(400);
    assertThat(response.body()).as("%s refusal must say why", route).contains("already forwarded");
  }

  private void awaitOnEveryServer(final Predicate<ServerSecurity> condition, final String what) {
    await().alias(what).atMost(30, TimeUnit.SECONDS).until(() -> {
      for (int i = 0; i < getServerCount(); i++)
        if (!condition.test(getServer(i).getSecurity()))
          return false;
      return true;
    });
  }

  private HttpResponse<String> asRoot(final int serverIndex, final String method, final String path,
      final String body) throws Exception {
    return send(serverIndex, method, path, body,
        b -> b.header("Authorization", basic("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)));
  }

  /**
   * A request shaped the way a cluster peer sends a forwarded one: cluster-token authentication plus the marker that
   * says a peer already relayed it.
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
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  private int firstFollower(final int leader) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader)
        return i;
    throw new IllegalStateException("no follower in a " + getServerCount() + "-node cluster");
  }
}
