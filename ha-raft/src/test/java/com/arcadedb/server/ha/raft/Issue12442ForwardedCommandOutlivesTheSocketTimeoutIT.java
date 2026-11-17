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
import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12442: a command a follower forwards to the leader failed with {@code EOFException} one socket timeout into the statement
 * ({@code arcadedb.network.socketTimeout}, 30 s by default), while the leader kept running it. The follower's forward client used the
 * JDK's default version, which upgrades a plain-HTTP connection to HTTP/2, and the leader lifts its read timeout for the time a
 * statement runs only on HTTP/1.1 connections ({@code HttpClientDisconnectWatch}, #9953): an HTTP/2 connection keeps reading frames
 * while it waits, and the client sends nothing, so the leader's read timed out and closed the connection.
 * <p>
 * The socket timeout is one second here and the statement sleeps three: a forward that works answers 200 after the sleep.
 */
class Issue12442ForwardedCommandOutlivesTheSocketTimeoutIT extends SharedClusterRaftHATest {

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.NETWORK_SOCKET_TIMEOUT, 1000);
  }

  @Tag("slow")
  @Test
  @Timeout(120)
  void aCommandForwardedToTheLeaderIsAnsweredWhateverTheSocketTimeout() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = leader == 0 ? 1 : 0;
    final String type = uniqueName("Issue12442Doc");

    final String payload = new JSONObject().put("language", "sqlscript")
        .put("command", "SLEEP 3000; CREATE DOCUMENT TYPE " + type + ";").toString();

    final long begin = System.currentTimeMillis();
    final HttpResponse<String> response = postToFollower(follower, payload);
    final long elapsed = System.currentTimeMillis() - begin;

    assertThat(response.statusCode()).as("the answer of the follower: %s", response.body()).isEqualTo(200);
    assertThat(elapsed).as("the follower waited for the leader's statement").isGreaterThanOrEqualTo(2900L);

    final Database leaderDatabase = getServerDatabase(leader, getDatabaseName());
    assertThat(leaderDatabase.getSchema().existsType(type)).as("the statement ran on the leader").isTrue();
  }

  private HttpResponse<String> postToFollower(final int follower, final String body) throws Exception {
    final String credentials = Base64.getEncoder()
        .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:" + getServer(follower).getHttpServer().getPort() + "/api/v1/command/" + getDatabaseName()))
        .timeout(Duration.ofSeconds(90))
        .header("Content-Type", "application/json")
        .header("Authorization", "Basic " + credentials)
        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
        .build();
    return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
