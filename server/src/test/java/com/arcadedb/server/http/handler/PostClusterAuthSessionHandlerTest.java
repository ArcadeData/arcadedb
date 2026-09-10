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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The issuer's side of a cross-node token lookup (issue #7424): what {@code POST /api/v1/cluster/auth-session}
 * answers a peer, and that it answers nobody else.
 */
class PostClusterAuthSessionHandlerTest extends BaseGraphServerTest {
  private static final String     CLUSTER_TOKEN = "test-cluster-secret-token";
  private static final HttpClient HTTP          = HttpClient.newHttpClient();

  @BeforeEach
  void setClusterToken() {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, CLUSTER_TOKEN);
  }

  @AfterEach
  void clearClusterToken() {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "");
  }

  @Test
  void tokenIsNamedAfterTheServer() throws Exception {
    final String token = login();
    assertThat(token).startsWith("AU-" + getServer(0).getServerName() + "-");
  }

  @Test
  void peerCanValidateASessionThisNodeIssued() throws Exception {
    final String token = login();
    final HttpAuthSession before = getServer(0).getHttpServer().getAuthSessionManager().getSessionByToken(token);

    final HttpResponse<String> response = asPeer(new JSONObject().put("action", "validate").put("token", token));

    assertThat(response.statusCode()).isEqualTo(200);
    final JSONObject body = new JSONObject(response.body());
    assertThat(body.getString("user")).isEqualTo("root");
    assertThat(body.getLong("createdAt")).isEqualTo(before.getCreatedAt());
  }

  @Test
  void validateIsTheDefaultAction() throws Exception {
    final HttpResponse<String> response = asPeer(new JSONObject().put("token", login()));
    assertThat(response.statusCode()).isEqualTo(200);
  }

  @Test
  void unknownTokenAnswers404() throws Exception {
    final HttpResponse<String> response = asPeer(new JSONObject().put("action", "validate")
        .put("token", "AU-" + getServer(0).getServerName() + "-2af64e60-8455-423a-bc64-ed0e19729f04"));
    assertThat(response.statusCode()).isEqualTo(404);
  }

  @Test
  void aCopyHeldForAnotherIssuerNeverVouches() throws Exception {
    final ServerSecurityUser root = getServer(0).getSecurity().getUser("root");
    final String token = "AU-other-node-2af64e60-8455-423a-bc64-ed0e19729f04";
    getServer(0).getHttpServer().getAuthSessionManager().addRemoteSession(token, root, 0L, "other-node");
    try {
      final HttpResponse<String> response = asPeer(new JSONObject().put("action", "validate").put("token", token));
      assertThat(response.statusCode()).isEqualTo(404);
    } finally {
      getServer(0).getHttpServer().getAuthSessionManager().removeSession(token);
    }
  }

  @Test
  void peerCanRevokeASession() throws Exception {
    final String token = login();

    final HttpResponse<String> response = asPeer(new JSONObject().put("action", "revoke").put("token", token));

    assertThat(response.statusCode()).isEqualTo(204);
    assertThat(getServer(0).getHttpServer().getAuthSessionManager().getSessionByToken(token)).isNull();
    assertThat(asPeer(new JSONObject().put("action", "validate").put("token", token)).statusCode()).isEqualTo(404);
  }

  @Test
  void missingTokenAndUnknownActionAnswer400() throws Exception {
    assertThat(asPeer(new JSONObject().put("action", "validate")).statusCode()).isEqualTo(400);
    assertThat(asPeer(new JSONObject().put("action", "wipe").put("token", login())).statusCode()).isEqualTo(400);
  }

  @Test
  void rootWithCredentialsIsRefused() throws Exception {
    final String token = login();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:2480/api/v1/cluster/auth-session"))
        .header("Content-Type", "application/json")
        .header("Authorization", basicRoot())
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("token", token).toString()))
        .build();

    final HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());

    assertThat(response.statusCode()).as("the answer names a principal; only nodes may ask").isEqualTo(403);
  }

  @Test
  void wrongClusterTokenIsRefused() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:2480/api/v1/cluster/auth-session"))
        .header("Content-Type", "application/json")
        .header("X-ArcadeDB-Cluster-Token", "not-the-token")
        .header("X-ArcadeDB-Forwarded-User", "root")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("token", login()).toString()))
        .build();
    assertThat(HTTP.send(request, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(401);
  }

  private HttpResponse<String> asPeer(final JSONObject payload) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:2480/api/v1/cluster/auth-session"))
        .header("Content-Type", "application/json")
        .header("X-ArcadeDB-Cluster-Token", CLUSTER_TOKEN)
        .header("X-ArcadeDB-Forwarded-User", "root")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private String login() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:2480/api/v1/login"))
        .header("Authorization", basicRoot())
        .POST(HttpRequest.BodyPublishers.noBody())
        .build();
    final HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
    return new JSONObject(response.body()).getString("token");
  }

  private static String basicRoot() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
