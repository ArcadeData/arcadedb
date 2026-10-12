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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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
 * Issue #9774 over MCP: a tool call that takes a query admission slot is listed by {@code list queries} for as long as it
 * holds it, as the work of every other protocol is, and a {@code terminate query} stops it. It used to take the slot
 * without opening an entry, so an agent's runaway query could be neither seen nor stopped short of a restart.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
// A hang detector, not a latency bound: a terminate that regressed would leave the test running its 16 s statement
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class Issue9774MCPRunningQueryTest extends BaseGraphServerTest {
  /** About 16 s on one core when left alone, all of it inside one aggregation. */
  private static final String LONG_CYPHER =
      "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j RETURN sum(sin(toFloat(i * j))) AS total";

  private static final HttpClient HTTP = HttpClient.newHttpClient();

  @Test
  void aQueryToolCallIsListedWhileItRunsAndCanBeTerminated() throws Exception {
    final MCPConfiguration mcp = MCPPlugin.of(getServer(0)).getConfiguration();
    final boolean savedEnabled = mcp.isEnabled();
    final List<String> savedUsers = mcp.getAllowedUsers();
    try {
      mcp.setEnabled(true);
      mcp.setAllowedUsers(List.of("root"));

      final CompletableFuture<JSONObject> call = CompletableFuture.supplyAsync(() -> call("query",
          new JSONObject().put("database", getDatabaseName()).put("language", "opencypher").put("query", LONG_CYPHER)));

      final RunningQuery entry = awaitRunning();
      assertThat(entry.getProtocol()).isEqualTo("mcp");
      assertThat(entry.getDatabase()).isEqualTo(getDatabaseName());
      assertThat(entry.getUser()).isEqualTo("root");
      assertThat(entry.getLanguage()).isEqualTo("opencypher");

      entry.terminate("root");
      final JSONObject response = call.get(60, TimeUnit.SECONDS);
      assertThat(isError(response)).as("a terminated tool call is answered as an error").isTrue();
      assertThat(entry.awaitEnd(30_000)).isTrue();
      assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);

      // the entry leaves the list with the call, and a tool that does no work in a database never enters it
      assertThat(isError(call("list_databases", new JSONObject()))).isFalse();
      assertThat(getServer(0).getRunningQueries().getRunning()).noneMatch(q -> "mcp".equals(q.getProtocol()));
    } finally {
      mcp.setEnabled(savedEnabled);
      mcp.setAllowedUsers(savedUsers);
    }
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

  private JSONObject call(final String tool, final JSONObject arguments) {
    try {
      final JSONObject body = new JSONObject().put("jsonrpc", "2.0").put("id", 1).put("method", "tools/call")
          .put("params", new JSONObject().put("name", tool).put("arguments", arguments));
      final HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/mcp")))
          .header("Content-Type", "application/json")
          .header("Authorization",
              "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
          .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build(), HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      return new JSONObject(response.body());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean isError(final JSONObject response) {
    return response.getJSONObject("result").getBoolean("isError", false);
  }
}
