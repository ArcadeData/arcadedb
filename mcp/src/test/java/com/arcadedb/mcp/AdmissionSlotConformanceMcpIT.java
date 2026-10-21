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

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.AdmissionSlotConformance;
import com.arcadedb.server.BaseGraphServerTest;
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
 * Issue #10235 over MCP: a statement a tool runs, which gives its admission slot back before a wait, finds the slot on the
 * thread that runs it. See {@link AdmissionSlotConformance}.
 */
class AdmissionSlotConformanceMcpIT extends BaseGraphServerTest {
  private static final HttpClient HTTP = HttpClient.newHttpClient();
  private static final String     TYPE = "Conformance10235";

  @Test
  void aToolCommandGivesItsSlotBackFromTheThreadThatRunsIt() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateDocumentType(TYPE);

    final MCPConfiguration mcp = MCPPlugin.of(getServer(0)).getConfiguration();
    final boolean savedEnabled = mcp.isEnabled();
    final boolean savedInsert = mcp.isAllowInsert();
    final List<String> savedUsers = mcp.getAllowedUsers();
    try {
      mcp.setEnabled(true);
      mcp.setAllowInsert(true);
      mcp.setAllowedUsers(List.of("root"));

      AdmissionSlotConformance.assertGivesItsSlotBack(database, TYPE, "MCP execute_command", () -> {
        final JSONObject answer = call("execute_command", new JSONObject().put("database", getDatabaseName()).put("language", "sql")
            .put("command", "INSERT INTO " + TYPE + " SET id = 1"));
        assertThat(answer.getJSONObject("result").getBoolean("isError", false)).as(answer.toString()).isFalse();
        return null;
      });
      assertThat(database.countType(TYPE, false)).isEqualTo(1);
    } finally {
      mcp.setEnabled(savedEnabled);
      mcp.setAllowInsert(savedInsert);
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
}
