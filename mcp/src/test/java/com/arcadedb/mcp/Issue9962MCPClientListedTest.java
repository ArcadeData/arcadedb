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
package com.arcadedb.mcp;

import com.arcadedb.query.RunningQuery;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9962: an MCP tool call was listed by {@code list queries} with no client at all, so on an endpoint every agent
 * reaches as the same service principal, nothing told two agents' work apart. The entry now names the peer address on
 * the HTTP transport and {@code stdio} on the stdio one, from the moment it is listed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
// A hang detector, not a latency bound: a terminate that regressed would leave the test running its 16 s statement
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class Issue9962MCPClientListedTest extends BaseGraphServerTest {
  /** About 16 s on one core when left alone, all of it inside one aggregation. */
  private static final String LONG_CYPHER =
      "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j RETURN sum(sin(toFloat(i * j))) AS total";

  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private MCPConfiguration mcp;
  private boolean          savedEnabled;
  private List<String>     savedUsers;

  @BeforeEach
  void enableMCP() {
    mcp = MCPPlugin.of(getServer(0)).getConfiguration();
    savedEnabled = mcp.isEnabled();
    savedUsers = mcp.getAllowedUsers();
    mcp.setEnabled(true);
    mcp.setAllowedUsers(List.of("root"));
  }

  @AfterEach
  void restoreMCP() {
    mcp.setEnabled(savedEnabled);
    mcp.setAllowedUsers(savedUsers);
  }

  @Test
  void anHttpToolCallIsListedWithItsPeerAddress() throws Exception {
    final CompletableFuture<JSONObject> call = CompletableFuture.supplyAsync(this::callOverHttp);

    final RunningQuery entry = awaitRunning();
    try {
      assertThat(entry.getProtocol()).isEqualTo("mcp");
      assertThat(entry.getClientAddress()).as("the address of the agent that asked for the work").contains("127.0.0.1");
      assertThat(entry.toJSON().getString("clientAddress")).isEqualTo(entry.getClientAddress());
    } finally {
      entry.terminate("root");
    }
    call.get(60, TimeUnit.SECONDS);
    assertThat(entry.awaitEnd(30_000)).isTrue();
  }

  @Test
  void aStdioToolCallIsListedAsStdio() throws Exception {
    final ServerSecurityUser user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
    final String line = toolCall().toString() + "\n";
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final MCPStdioServer stdio = new MCPStdioServer(getServer(0), mcp, user,
        new ByteArrayInputStream(line.getBytes(StandardCharsets.UTF_8)), new PrintStream(out, true, StandardCharsets.UTF_8));
    final CompletableFuture<Boolean> loop = CompletableFuture.supplyAsync(stdio::run);

    final RunningQuery entry = awaitRunning();
    try {
      assertThat(entry.getProtocol()).isEqualTo("mcp");
      assertThat(entry.getClientAddress()).isEqualTo("stdio");
    } finally {
      entry.terminate("root");
    }
    assertThat(loop.get(60, TimeUnit.SECONDS)).as("the loop ends on EOF").isTrue();
    assertThat(entry.awaitEnd(30_000)).isTrue();
  }

  private RunningQuery awaitRunning() {
    final RunningQuery[] found = new RunningQuery[1];
    await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).until(() -> {
      for (final RunningQuery q : getServer(0).getRunningQueries().getRunning())
        if (LONG_CYPHER.equals(q.getText())) {
          found[0] = q;
          return true;
        }
      return false;
    });
    return found[0];
  }

  private JSONObject toolCall() {
    return new JSONObject().put("jsonrpc", "2.0").put("id", 1).put("method", "tools/call").put("params",
        new JSONObject().put("name", "query").put("arguments",
            new JSONObject().put("database", getDatabaseName()).put("language", "opencypher").put("query", LONG_CYPHER)));
  }

  private JSONObject callOverHttp() {
    try {
      final HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/mcp")))
          .header("Content-Type", "application/json")
          .header("Authorization",
              "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
          .POST(HttpRequest.BodyPublishers.ofString(toolCall().toString())).build(), HttpResponse.BodyHandlers.ofString());
      return new JSONObject(response.body());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
