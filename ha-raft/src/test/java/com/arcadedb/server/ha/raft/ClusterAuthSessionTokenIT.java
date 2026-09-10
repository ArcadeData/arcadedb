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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpAuthSessionManager;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A login token issued by one node is honoured by every node of the cluster (issue #7424): the token names its
 * issuer, a node that has never seen it asks that issuer once and keeps a copy, a logout anywhere drops every
 * copy, and a copy dies when the issuer no longer holds the session. This is the Studio-behind-a-load-balancer
 * scenario: without it two requests out of three landed on a node that answered 401.
 */
class ClusterAuthSessionTokenIT extends BaseRaftHATest {
  /** Short, so the lease renewal (a third of it) is observable; every test finishes well inside it. */
  private static final long AUTH_SESSION_TIMEOUT_SECONDS = 6;

  private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.SERVER_HTTP_AUTH_SESSION_EXPIRE_TIMEOUT, AUTH_SESSION_TIMEOUT_SECONDS);
  }

  @Test
  void tokenIssuedByOneNodeIsHonouredByTheOthers() throws Exception {
    final String token = loginAndGetToken(0);
    assertThat(HttpAuthSessionManager.issuerOf(token)).isEqualTo(getServer(0).getServerName());

    for (int node = 1; node < getServerCount(); node++) {
      final HttpResponse<String> response = query(node, token);
      assertThat(response.statusCode()).as("node %d must honour a token node 0 issued", node).isEqualTo(200);
      assertThat(response.body()).contains("result");

      final HttpAuthSession copy = authSessions(node).getSessionByToken(token);
      assertThat(copy).as("node %d keeps a copy for the next request", node).isNotNull();
      assertThat(copy.isRemote()).isTrue();
      assertThat(copy.getIssuer()).isEqualTo(getServer(0).getServerName());
      assertThat(copy.getUser().getName()).isEqualTo("root");
    }
    assertThat(authSessions(0).getSessionByToken(token).isRemote()).as("the issuer holds the original").isFalse();
    // Second round: served from the copies, still 200.
    for (int node = 1; node < getServerCount(); node++)
      assertThat(query(node, token).statusCode()).isEqualTo(200);
  }

  @Test
  void logoutOnAnyNodeRevokesTheTokenEverywhere() throws Exception {
    final String token = loginAndGetToken(0);
    for (int node = 1; node < getServerCount(); node++)
      assertThat(query(node, token).statusCode()).isEqualTo(200);

    assertThat(logout(2, token).statusCode()).isEqualTo(204);

    for (int node = 0; node < getServerCount(); node++) {
      assertThat(authSessions(node).getSessionByToken(token)).as("node %d dropped the session", node).isNull();
      assertThat(query(node, token).statusCode()).as("node %d after logout", node).isEqualTo(401);
    }
  }

  @Test
  void copyDiesWhenTheIssuerNoLongerHoldsTheSession() throws Exception {
    final String token = loginAndGetToken(0);
    assertThat(query(1, token).statusCode()).isEqualTo(200);

    // The issuer forgets the session on its own (an idle expiry, a restart) - nobody tells node 1.
    authSessions(0).removeSession(token);
    assertThat(query(1, token).statusCode()).as("the lease is still current").isEqualTo(200);

    Thread.sleep(authSessions(1).getRemoteRenewalIntervalMs() + 200);
    assertThat(query(1, token).statusCode()).as("renewal with the issuer fails, the copy is dropped").isEqualTo(401);
    assertThat(authSessions(1).getSessionByToken(token)).isNull();
  }

  @Test
  void copyInUseKeepsTheSessionAliveOnTheIssuer() throws Exception {
    final String token = loginAndGetToken(0);
    assertThat(query(1, token).statusCode()).isEqualTo(200);
    final long lastUpdateOnIssuer = authSessions(0).getSessionByToken(token).getLastUpdate();

    Thread.sleep(authSessions(1).getRemoteRenewalIntervalMs() + 200);
    assertThat(query(1, token).statusCode()).isEqualTo(200);

    assertThat(authSessions(0).getSessionByToken(token).getLastUpdate())
        .as("the renewal counted as activity on the issuer, so the session does not idle out there")
        .isGreaterThan(lastUpdateOnIssuer);
  }

  @Test
  void tokenNamingNoMemberOrNoIssuerIsRefusedWithoutAskingAnyone() throws Exception {
    final String uuid = "2af64e60-8455-423a-bc64-ed0e19729f04";
    assertThat(query(1, "AU-no-such-node-" + uuid).statusCode()).isEqualTo(401);
    assertThat(query(1, "AU-" + uuid).statusCode()).as("legacy form").isEqualTo(401);
    assertThat(query(1, "AU-" + getServer(1).getServerName() + "-" + uuid).statusCode())
        .as("names this very node, which does not hold it").isEqualTo(401);
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  private HttpAuthSessionManager authSessions(final int node) {
    return getServer(node).getHttpServer().getAuthSessionManager();
  }

  private String loginAndGetToken(final int node) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl(node) + "/api/v1/login"))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", basicRoot())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("{}", StandardCharsets.UTF_8))
        .build();
    final HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as("login on node %d", node).isEqualTo(200);
    final String token = new JSONObject(response.body()).getString("token");
    assertThat(token).startsWith("AU-");
    return token;
  }

  private HttpResponse<String> query(final int node, final String token) throws Exception {
    final String body = new JSONObject().put("language", "sql").put("command", "SELECT FROM V1 LIMIT 1").toString();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl(node) + "/api/v1/query/" + getDatabaseName()))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", "Bearer " + token)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
        .build();
    return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
  }

  private HttpResponse<String> logout(final int node, final String token) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl(node) + "/api/v1/logout"))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", "Bearer " + token)
        .POST(HttpRequest.BodyPublishers.noBody())
        .build();
    return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private String baseUrl(final int node) {
    return "http://127.0.0.1:" + getServer(node).getHttpServer().getPort();
  }

  private static String basicRoot() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
