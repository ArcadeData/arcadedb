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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9518 over MCP: a tool that runs work in a database waits for the query admission gate, and one the gate does
 * not start is answered as a tool error that says so; the tools that only report on the server are never held behind
 * the queries; and every tool call gives its slot back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class QueryAdmissionGateMcpIssue9518IT extends BaseGraphServerTest {
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();

  @AfterEach
  void resetGate() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
  }

  @Test
  void aDataToolTheGateDoesNotStartIsAToolErrorAndEveryCallGivesItsSlotBack() throws Exception {
    final MCPConfiguration mcp = MCPPlugin.of(getServer(0)).getConfiguration();
    final boolean savedEnabled = mcp.isEnabled();
    final List<String> savedUsers = mcp.getAllowedUsers();
    try {
      mcp.setEnabled(true);
      mcp.setAllowedUsers(List.of("root"));
      GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
      GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

      try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
        final JSONObject refused = call("query", new JSONObject().put("database", getDatabaseName()).put("language", "sql")
            .put("query", "SELECT 1 AS one"));
        assertThat(isError(refused)).isTrue();
        assertThat(text(refused)).contains("Query not started");

        assertThat(isError(call("list_databases", new JSONObject()))).as("never held behind the queries").isFalse();
      }

      // EVERY CALL GIVES ITS SLOT BACK: WITH ONE SLOT AND NO WAITING, A LEAKED ONE WOULD REFUSE THE SECOND
      for (int i = 0; i < 2; i++)
        assertThat(isError(call("query", new JSONObject().put("database", getDatabaseName()).put("language", "sql")
            .put("query", "SELECT 1 AS one")))).isFalse();
      assertThat(gate.getRunning()).isZero();
    } finally {
      mcp.setEnabled(savedEnabled);
      mcp.setAllowedUsers(savedUsers);
    }
  }

  private JSONObject call(final String tool, final JSONObject arguments) throws Exception {
    final JSONObject body = new JSONObject().put("jsonrpc", "2.0").put("id", 1).put("method", "tools/call")
        .put("params", new JSONObject().put("name", tool).put("arguments", arguments));
    final HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/mcp")))
        .header("Content-Type", "application/json")
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build(), HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    return new JSONObject(response.body());
  }

  private static boolean isError(final JSONObject response) {
    return response.getJSONObject("result").getBoolean("isError", false);
  }

  private static String text(final JSONObject response) {
    return response.getJSONObject("result").getJSONArray("content").getJSONObject(0).getString("text");
  }
}
