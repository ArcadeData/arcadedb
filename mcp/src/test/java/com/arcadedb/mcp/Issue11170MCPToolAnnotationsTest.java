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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11170: every tool {@code tools/list} returns carries the MCP {@code annotations} a client reads to decide when
 * to ask the user before a call. Without them a client has to treat {@code query} and {@code execute_command} alike.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11170MCPToolAnnotationsTest extends BaseGraphServerTest {
  private record Hints(boolean readOnly, boolean destructive, boolean idempotent, boolean openWorld) {
  }

  private static final Hints READ             = new Hints(true, false, true, false);
  // A read-only Cypher LOAD CSV can fetch a remote URL
  private static final Hints READ_OPEN_WORLD  = new Hints(true, false, true, true);
  // Overwrites the properties of a record it matched, and may rewrite the match key itself: neither additive nor idempotent
  private static final Hints UPSERT           = new Hints(false, true, false, false);
  private static final Hints STATEFUL         = new Hints(false, false, false, false);
  private static final Hints DESTRUCTIVE      = new Hints(false, true, false, false);
  // LOAD CSV and IMPORT DATABASE can fetch a remote URL
  private static final Hints COMMAND          = new Hints(false, true, false, true);

  private static final Map<String, Hints> EXPECTED = Map.ofEntries(
      Map.entry("list_databases", READ),
      Map.entry("get_schema", READ),
      Map.entry("query", READ_OPEN_WORLD),
      Map.entry("sample_records", READ),
      Map.entry("vector_search", READ),
      Map.entry("full_text_search", READ),
      Map.entry("hybrid_search", READ),
      Map.entry("server_status", READ),
      Map.entry("profiler_status", READ),
      Map.entry("get_server_settings", READ),
      Map.entry("upsert_entity", UPSERT),
      Map.entry("upsert_relationship", UPSERT),
      // Starting a session discards the results of the previous one
      Map.entry("profiler_start", DESTRUCTIVE),
      Map.entry("profiler_stop", STATEFUL),
      Map.entry("execute_command", COMMAND),
      Map.entry("set_server_setting", DESTRUCTIVE));

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
  void everyListedToolDeclaresItsAnnotations() throws Exception {
    final JSONArray tools = listTools();

    final Set<String> listed = new HashSet<>();
    for (int i = 0; i < tools.length(); i++) {
      final JSONObject tool = tools.getJSONObject(i);
      final String name = tool.getString("name");
      listed.add(name);

      // A tool added later must be classified here too, or a client would treat it as an unknown, possibly destructive one
      assertThat(EXPECTED).as("tool '%s' has no expected annotations in this test", name).containsKey(name);
      final Hints expected = EXPECTED.get(name);

      assertThat(tool.has("annotations")).as("tool '%s' declares annotations", name).isTrue();
      final JSONObject annotations = tool.getJSONObject("annotations");
      assertThat(annotations.getString("title", "")).as("tool '%s' title", name).isNotBlank();
      assertThat(annotations.getBoolean("readOnlyHint")).as("tool '%s' readOnlyHint", name).isEqualTo(expected.readOnly());
      assertThat(annotations.getBoolean("destructiveHint")).as("tool '%s' destructiveHint", name).isEqualTo(expected.destructive());
      assertThat(annotations.getBoolean("idempotentHint")).as("tool '%s' idempotentHint", name).isEqualTo(expected.idempotent());
      assertThat(annotations.getBoolean("openWorldHint")).as("tool '%s' openWorldHint", name).isEqualTo(expected.openWorld());
    }

    // Root sees the full catalog, so every registered tool was checked above
    assertThat(listed).isEqualTo(MCPDispatcher.REGISTERED_TOOL_NAMES);
  }

  /**
   * A scripting language evaluates its input inside {@code analyze()}, which both tools call to decide whether the input is
   * read-only or allowed by the MCP configuration: the script would have run before either check could refuse it. Root holds
   * the scripting permission, so only the tool itself stands between the script and the database here.
   */
  @Test
  void queryToolRefusesAScriptBeforeItRuns() throws Exception {
    assertScriptRefusedAndNotRun("query", "query");
  }

  @Test
  void executeCommandToolRefusesAScriptBeforeItRuns() throws Exception {
    assertScriptRefusedAndNotRun("execute_command", "command");
  }

  private void assertScriptRefusedAndNotRun(final String tool, final String textArgument) throws Exception {
    final boolean savedInsert = mcp.isAllowInsert();
    mcp.setAllowInsert(false);
    try {
      final JSONObject scripted = callTool(tool, new JSONObject().put("database", getDatabaseName()).put("language", "js")
          .put(textArgument, "database.command('sql', 'CREATE VERTEX " + VERTEX1_TYPE_NAME + " SET scripted = true'); 1"));
      assertThat(scripted.getJSONObject("result").getBoolean("isError", false)).as("%s must refuse a script", tool).isTrue();

      final JSONObject check = callTool("query", new JSONObject().put("database", getDatabaseName()).put("language", "sql")
          .put("query", "SELECT count(@rid) AS c FROM " + VERTEX1_TYPE_NAME + " WHERE scripted = true"));
      final String text = check.getJSONObject("result").getJSONArray("content").getJSONObject(0).getString("text");
      assertThat(new JSONObject(text).getJSONArray("records").getJSONObject(0).getLong("c"))
          .as("the script refused by %s must not have run", tool).isZero();
    } finally {
      mcp.setAllowInsert(savedInsert);
    }
  }

  private JSONArray listTools() throws Exception {
    return post(new JSONObject().put("jsonrpc", "2.0").put("id", 1).put("method", "tools/list"))
        .getJSONObject("result").getJSONArray("tools");
  }

  private JSONObject callTool(final String tool, final JSONObject arguments) throws Exception {
    return post(new JSONObject().put("jsonrpc", "2.0").put("id", 1).put("method", "tools/call")
        .put("params", new JSONObject().put("name", tool).put("arguments", arguments)));
  }

  private JSONObject post(final JSONObject message) throws Exception {
    final String auth = "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    final HttpRequest request = HttpRequest.newBuilder(new URI(getServerHttpUrl(0, "/api/v1/mcp")))
        .header("Authorization", auth)
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(message.toString(), StandardCharsets.UTF_8))
        .build();

    final HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    assertThat(response.statusCode()).isEqualTo(200);
    return new JSONObject(response.body());
  }
}
