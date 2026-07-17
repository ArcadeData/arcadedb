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
package com.arcadedb.server.security;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.mcp.MCPConfiguration;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GHSA-6x73-v3rc-f57c: the MCP HTTP transport never bound the authenticated principal onto the
 * request thread's DatabaseContext, so the engine's per-user permission gates (which are deliberately no-ops when no
 * user is bound) silently passed for every MCP caller. A non-root MCP-allowed reader could therefore perform arbitrary
 * writes, and - through the {@code language: "js"} query sub-case, whose script is eagerly evaluated during analyze()
 * before the idempotency check - execute arbitrary in-JVM JavaScript.
 * <p>
 * With the principal bound, a read-only MCP user is denied on both the direct write path (execute_command) and the
 * scripting escalation path (query + js), while ordinary read-only queries and an authorized user's writes still work.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MCPAuthorizationBindingIT extends BaseGraphServerTest {

  private static final String READER_USER = "mcp-reader";
  private static final String READER_PWD  = "mcpreaderpass1";

  @Test
  void readerCannotEscalateThroughMcp() throws Exception {
    testEachServer((serverIndex) -> {
      final MCPConfiguration mcp = getServer(serverIndex).getMCPConfiguration();
      final boolean savedEnabled = mcp.isEnabled();
      final boolean savedInsert = mcp.isAllowInsert();
      final List<String> savedUsers = mcp.getAllowedUsers();
      try {
        // MCP is off and root-only by default. Turn it on for a non-root reader, mirroring the agent-delegation
        // deployment the advisory targets. allowInsert=true so the direct-write test exercises the ENGINE per-user
        // gate (createRecord) rather than the coarse MCP-config gate.
        mcp.setEnabled(true);
        mcp.setAllowedUsers(List.of("*"));
        mcp.setAllowInsert(true);

        createReaderUser(serverIndex);
        final String readerAuth = basicAuth(READER_USER, READER_PWD);
        final String rootAuth = basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS);

        // Control: the reader is live and can run an ordinary read-only query through MCP.
        final JSONObject read = mcp(serverIndex, readerAuth,
            toolCall("query", new JSONObject().put("database", getDatabaseName()).put("language", "sql")
                .put("query", "SELECT FROM " + VERTEX1_TYPE_NAME)));
        assertThat(toolIsError(read)).as("reader must be able to run a read-only query").isFalse();

        // Escalation path #1 - direct write via execute_command. The reader has readRecord only, so the engine
        // createRecord gate must reject it even though the MCP config allows inserts.
        final JSONObject write = mcp(serverIndex, readerAuth,
            toolCall("execute_command", new JSONObject().put("database", getDatabaseName()).put("language", "sql")
                .put("command", "CREATE VERTEX " + VERTEX1_TYPE_NAME + " SET id = 888888, pwned = true")));
        assertThat(toolIsError(write)).as("reader must be denied a direct write through MCP").isTrue();

        // Escalation path #2 - the js sub-case from the advisory: analyze() eagerly evaluates the script, so the
        // scripting gate (UPDATE_SECURITY) is the only thing standing between a reader and arbitrary in-JVM code.
        final JSONObject js = mcp(serverIndex, readerAuth,
            toolCall("query", new JSONObject().put("database", getDatabaseName()).put("language", "js")
                .put("query", "database.command('sql','CREATE VERTEX " + VERTEX1_TYPE_NAME
                    + " SET id = 999999, pwned = true'); 1")));
        assertThat(toolIsError(js)).as("reader must be denied js execution through MCP").isTrue();

        // The decisive check: neither escalation attempt may have mutated the database. Verified as root so the read
        // itself is authorized. Both rows would exist on the vulnerable build (writes ran under a null principal).
        final JSONObject check = mcp(serverIndex, rootAuth,
            toolCall("query", new JSONObject().put("database", getDatabaseName()).put("language", "sql")
                .put("query", "SELECT count(*) AS c FROM " + VERTEX1_TYPE_NAME + " WHERE pwned = true")));
        assertThat(toolIsError(check)).isFalse();
        assertThat(pwnedCount(check)).as("no escalation write may have taken effect").isZero();

        // Positive control: an authorized user (root) still writes through MCP with the principal bound.
        final JSONObject rootWrite = mcp(serverIndex, rootAuth,
            toolCall("execute_command", new JSONObject().put("database", getDatabaseName()).put("language", "sql")
                .put("command", "CREATE VERTEX " + VERTEX1_TYPE_NAME + " SET id = 777777")));
        assertThat(toolIsError(rootWrite)).as("root must still write through MCP").isFalse();
      } finally {
        deleteUser(serverIndex, READER_USER);
        mcp.setEnabled(savedEnabled);
        mcp.setAllowInsert(savedInsert);
        mcp.setAllowedUsers(savedUsers);
      }
    });
  }

  private void createReaderUser(final int serverIndex) throws Exception {
    final ServerSecurity security = getServer(serverIndex).getSecurity();

    // A group that grants record reads but no database-level (admin) permission and no createRecord.
    security.getDatabaseGroupsConfiguration(getDatabaseName()).put("mcpReader",
        new JSONObject().put("access", new JSONArray()).put("types",
            new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));
    security.saveGroups();

    if (security.existsUser(READER_USER))
      security.dropUser(READER_USER);

    final JSONObject payload = new JSONObject()
        .put("name", READER_USER)
        .put("password", READER_PWD)
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("mcpReader")));

    final HttpURLConnection connection = open(serverIndex, "/api/v1/server/users", basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
    } finally {
      connection.disconnect();
    }
  }

  private void deleteUser(final int serverIndex, final String name) throws Exception {
    final HttpURLConnection connection = open(serverIndex,
        "/api/v1/server/users?name=" + URLEncoder.encode(name, StandardCharsets.UTF_8), basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
    connection.setRequestMethod("DELETE");
    connection.connect();
    try {
      connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private static JSONObject toolCall(final String toolName, final JSONObject arguments) {
    return new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 1)
        .put("method", "tools/call")
        .put("params", new JSONObject().put("name", toolName).put("arguments", arguments));
  }

  /**
   * Reads {@code result.isError}. A tool that raises a SecurityException is reported as a JSON-RPC result with
   * {@code isError:true} (not a protocol-level error), so a denial surfaces here rather than as an HTTP status.
   */
  private static boolean toolIsError(final JSONObject response) {
    return response.getJSONObject("result").getBoolean("isError", false);
  }

  private static long pwnedCount(final JSONObject response) {
    final String text = response.getJSONObject("result").getJSONArray("content").getJSONObject(0).getString("text");
    final JSONArray records = new JSONObject(text).getJSONArray("records");
    return records.isEmpty() ? 0 : records.getJSONObject(0).getLong("c");
  }

  private JSONObject mcp(final int serverIndex, final String auth, final JSONObject body) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "/api/v1/mcp", auth);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(body.toString().getBytes(StandardCharsets.UTF_8));
    connection.connect();
    try {
      final int code = connection.getResponseCode();
      final InputStream in = code >= 200 && code < 400 ? connection.getInputStream() : connection.getErrorStream();
      final String payload = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      return new JSONObject(payload);
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection open(final int serverIndex, final String path, final String auth) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + path).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    return connection;
  }

  private String basicAuth(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes());
  }
}
