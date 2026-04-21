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
import com.arcadedb.server.BaseGraphServerTest;

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
 * Regression test for the Studio scenario where a user authenticated on a follower with
 * a per-node session token (Bearer AU-...) receives 401 for read queries.
 *
 * <p>Two behaviours are covered:
 * <ul>
 *   <li>A SELECT sent to a follower with an AU- session token must run locally on the
 *       follower and return 200. It must NOT be forwarded to the leader (that would fail
 *       since the session lives only on the follower that issued it).</li>
 *   <li>A write command sent to a follower with an AU- session token must be forwarded
 *       to the leader at the engine level and succeed. The follower substitutes the
 *       session token with X-ArcadeDB-Cluster-Token plus X-ArcadeDB-Forwarded-User; the
 *       leader must accept those cluster-internal headers.</li>
 * </ul>
 */
class FollowerSessionTokenQueryIT extends BaseRaftHATest {

  private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void followerSelectWithSessionTokenReturns200() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = firstFollowerIndex(leaderIndex);
    final int followerPort = getServer(followerIndex).getHttpServer().getPort();

    final String token = loginAndGetToken(followerPort);

    final HttpResponse<String> response = postQueryWithToken(followerPort, getDatabaseName(),
        "SELECT FROM V1 LIMIT 1", token);

    assertThat(response.statusCode())
        .as("Follower must execute the read locally and return 200")
        .isEqualTo(200);
    assertThat(response.body()).contains("result");
  }

  @Test
  void followerWriteWithSessionTokenReturns200ViaEngineForwarding() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = firstFollowerIndex(leaderIndex);
    final int followerPort = getServer(followerIndex).getHttpServer().getPort();

    final String token = loginAndGetToken(followerPort);

    final HttpResponse<String> response = postCommandWithToken(followerPort, getDatabaseName(),
        "INSERT INTO V1 SET name = 'session-token-write'", token);

    assertThat(response.statusCode())
        .as("Follower engine must forward the write to leader with cluster-internal auth and succeed")
        .isEqualTo(200);
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  private int firstFollowerIndex(final int leaderIndex) {
    for (int i = 0; i < getServerCount(); i++) {
      if (i != leaderIndex)
        return i;
    }
    throw new IllegalStateException("No follower found in " + getServerCount() + "-node cluster");
  }

  private String loginAndGetToken(final int port) throws Exception {
    final String basicAuth = "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + port + "/api/v1/login"))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", basicAuth)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("{}", StandardCharsets.UTF_8))
        .build();

    final HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as("Login must succeed on follower").isEqualTo(200);

    final String token = new JSONObject(response.body()).getString("token");
    assertThat(token).as("Session token must be issued").startsWith("AU-");
    return token;
  }

  private HttpResponse<String> postQueryWithToken(final int port, final String dbName,
      final String sql, final String token) throws Exception {
    return postJson(port, "/api/v1/query/" + dbName, sql, token);
  }

  private HttpResponse<String> postCommandWithToken(final int port, final String dbName,
      final String sql, final String token) throws Exception {
    return postJson(port, "/api/v1/command/" + dbName, sql, token);
  }

  private HttpResponse<String> postJson(final int port, final String path, final String sql,
      final String token) throws Exception {
    final String body = new JSONObject()
        .put("language", "sql")
        .put("command", sql)
        .toString();

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + port + path))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", "Bearer " + token)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
        .build();

    return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
  }
}
