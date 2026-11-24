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
import com.arcadedb.server.http.handler.LeaderDial;
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
 * Issue #12442, over TLS: the HTTPS half of {@link HaForwardedLongCommandIT}. A command sent to a follower and forwarded to
 * the leader's HTTPS endpoint runs longer than {@code arcadedb.network.socketTimeout} there. The forward client used to
 * negotiate {@code h2} by ALPN, and the leader's read timeout closed that connection one socket timeout into the statement;
 * over HTTP/1.1 Undertow does not read a TLS connection while the request runs, so no read timer fires and the forward is
 * answered however long the statement takes.
 * <p>
 * The client of the test talks plain HTTP to the follower; the follower's forward is the hop under test, and the first
 * assertion fixes that it goes to the leader's HTTPS endpoint, so the test cannot pass by quietly taking the plain listener.
 * <p>
 * Tagged {@code slow}: it generates a certificate authority with {@code keytool} and starts a cluster with two listeners per
 * node.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
@Tag("slow")
// A hang detector, not a latency bound
@Timeout(300)
class HaForwardedLongCommandTlsIT extends BaseRaftHASslTest {
  private static final int SOCKET_TIMEOUT_MS = 1_000;

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .build();

  @Override
  protected String pkiDirectoryName() {
    return "issue12442-forwarded-long-command-pki";
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.NETWORK_SOCKET_TIMEOUT, SOCKET_TIMEOUT_MS);
  }

  @Test
  void aForwardedCommandLongerThanTheLeadersSocketTimeoutIsAnsweredOverTls() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("precondition: a leader is elected").isGreaterThanOrEqualTo(0);
    final int follower = leader == 0 ? 1 : 0;

    try (final HttpClient plain = HttpClient.newHttpClient()) {
      final LeaderDial dial = LeaderDial.resolve(getServer(follower).getHA(), plain);
      assertThat(dial.refused()).as("the forward must not be refused: %s", dial.refusal()).isFalse();
      assertThat(dial.https()).as("precondition: the follower forwards to the leader's HTTPS endpoint").isTrue();
    }

    // Twice: the first forward opens the follower's connection to the leader, the second one runs on the connection a
    // client keeps for every later forward
    for (int i = 0; i < 2; i++) {
      final String script = "SLEEP 3000; CREATE VERTEX TYPE LongTlsForward" + i;
      final HttpResponse<String> response = postScript(follower, script);
      assertThat(response.statusCode()).as("forward %d, three socket timeouts long, over TLS: %s", i, response.body())
          .isEqualTo(200);
      assertThat(getServerDatabase(leader, getDatabaseName()).getSchema().existsType("LongTlsForward" + i)).isTrue();
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
