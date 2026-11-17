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
 * Issue #12442. A command sent to a follower and forwarded to the leader runs longer than
 * {@code arcadedb.network.socketTimeout} there. The forward went over the JDK client's HTTP/2 (an h2c upgrade of the
 * plain-HTTP connection), whose streams the leader's client-disconnect watch does not cover, so the leader's read timeout
 * closed the connection one socket timeout into the statement and the caller got "Error forwarding command to leader ...
 * EOFException": every forwarded command longer than 30 s failed - a {@code CHECK DATABASE} of a large database, a schema
 * script waiting on the async workers.
 */
// A hang detector, not a latency bound
@Timeout(300)
class HaForwardedLongCommandIT extends BaseRaftHATest {
  private static final int SOCKET_TIMEOUT_MS = 1_000;

  private static final HttpClient HTTP = HttpClient.newBuilder()
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
    config.setValue(GlobalConfiguration.NETWORK_SOCKET_TIMEOUT, SOCKET_TIMEOUT_MS);
  }

  @Test
  void aForwardedCommandLongerThanTheLeadersSocketTimeoutIsAnswered() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("precondition: a leader is elected").isGreaterThanOrEqualTo(0);
    final int follower = (leader + 1) % getServerCount();

    // Twice: the first forward opens the follower's connection to the leader, the second one runs on the connection a
    // client keeps for every later forward
    for (int i = 0; i < 2; i++) {
      final String script = "SLEEP 3000; CREATE VERTEX TYPE LongForward" + i;
      final HttpResponse<String> response = postScript(follower, script);
      assertThat(response.statusCode()).as("forward %d, three socket timeouts long: %s", i, response.body()).isEqualTo(200);
      assertThat(getServer(leader).getDatabase(getDatabaseName()).getSchema().existsType("LongForward" + i)).isTrue();
    }
  }

  private HttpResponse<String> postScript(final int serverIndex, final String script) throws Exception {
    final String body = new JSONObject().put("language", "sqlscript").put("command", script).toString();
    return HTTP.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl(serverIndex, "/api/v1/command/" + getDatabaseName())))
            .timeout(Duration.ofSeconds(60))
            .header("Content-Type", "application/json")
            .header("Authorization", "Basic " + Base64.getEncoder()
                .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }
}
