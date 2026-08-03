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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityTestAccess;
import com.arcadedb.server.security.ServerSecurityUser;
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
 * Regression test for GHSA-pff6-hp53-pj54: the SERVER-level MCP tools gated only on the global
 * {@code allowAdmin} MCP configuration flag and ignored the {@link ServerSecurityUser} they were handed.
 * <p>
 * The GHSA-6x73-v3rc-f57c hardening bound the authenticated principal so DATABASE-level MCP tools enforce per-user
 * database permissions (covered by {@link MCPAuthorizationBindingIT}). Server-scoped tools act on the whole server, so
 * no per-database permission can reach them and that binding does not help. In the documented agent-delegation
 * deployment ({@code allowAdmin=true} with {@code allowedUsers=["*"]}), any authenticated read-only user could
 * therefore invoke {@code set_server_setting} and the profiler controls.
 * <p>
 * The reported tool was {@code set_server_setting}, but the defect is shared by every server-administration tool:
 * {@code profiler_start}, {@code profiler_stop} and {@code profiler_status} have the identical shape. All four are
 * asserted here. They now require root, matching {@code POST /api/v1/server}, where
 * {@code AbstractServerHttpHandler.checkRootUser} makes every server command except {@code list databases} root-only.
 * <p>
 * The read-only server tools ({@code server_status}, {@code get_server_settings}) deliberately remain available to any
 * MCP-allowed user, matching {@code GET /api/v1/server}; they are asserted here so that posture stays intentional
 * rather than accidental.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MCPServerAdminAuthorizationIT extends BaseGraphServerTest {

  private static final String READER_USER  = "mcp-server-admin-reader";
  private static final String READER_PWD   = "mcpreaderpass1";
  private static final String TAMPER_VALUE = "PWNED_BY_READER";

  @Test
  void readerCannotInvokeServerAdminTools() throws Exception {
    testEachServer((serverIndex) -> {
      final MCPConfiguration mcp = MCPPlugin.of(getServer(serverIndex)).getConfiguration();
      final boolean savedEnabled = mcp.isEnabled();
      final boolean savedAdmin = mcp.isAllowAdmin();
      final List<String> savedUsers = mcp.getAllowedUsers();
      final ContextConfiguration serverConfig = getServer(serverIndex).getConfiguration();
      final String savedServerName = serverConfig.getValueAsString(GlobalConfiguration.SERVER_NAME);
      try {
        // The exact deployment the advisory targets: MCP on, admin operations permitted by configuration, and a
        // wildcard allowedUsers so a non-root agent identity can drive the transport.
        mcp.setEnabled(true);
        mcp.setAllowAdmin(true);
        mcp.setAllowedUsers(List.of("*"));

        createReaderUser(serverIndex);
        final String readerAuth = basicAuth(READER_USER, READER_PWD);

        // The published proof-of-concept.
        final JSONObject setSetting = mcp(serverIndex, readerAuth,
            toolCall("set_server_setting", new JSONObject()
                .put("key", GlobalConfiguration.SERVER_NAME.getKey())
                .put("value", TAMPER_VALUE)));
        assertThat(toolIsError(setSetting)).as("reader must be denied set_server_setting").isTrue();
        // Assert on the SERVER's ContextConfiguration - that is what SetServerSettingTool writes to, so this is the
        // state that actually changes on the vulnerable build.
        assertThat(serverConfig.getValueAsString(GlobalConfiguration.SERVER_NAME))
            .as("the server setting must not have been modified").isNotEqualTo(TAMPER_VALUE);

        // Same defect, same shape: the profiler controls read and mutate server-wide state.
        assertThat(toolIsError(mcp(serverIndex, readerAuth,
            toolCall("profiler_start", new JSONObject().put("timeoutSeconds", 5)))))
            .as("reader must be denied profiler_start").isTrue();
        assertThat(toolIsError(mcp(serverIndex, readerAuth, toolCall("profiler_status", new JSONObject()))))
            .as("reader must be denied profiler_status").isTrue();
        assertThat(toolIsError(mcp(serverIndex, readerAuth, toolCall("profiler_stop", new JSONObject()))))
            .as("reader must be denied profiler_stop").isTrue();

        // The read-only server tools stay open to any MCP-allowed user, mirroring GET /api/v1/server. Asserting this
        // keeps the fix scoped: it must close the administration surface without breaking the read surface.
        assertThat(toolIsError(mcp(serverIndex, readerAuth, toolCall("server_status", new JSONObject()))))
            .as("reader may still read server_status").isFalse();
        assertThat(toolIsError(mcp(serverIndex, readerAuth, toolCall("get_server_settings", new JSONObject()))))
            .as("reader may still read get_server_settings").isFalse();
      } finally {
        deleteUser(serverIndex, READER_USER);
        serverConfig.setValue(GlobalConfiguration.SERVER_NAME, savedServerName);
        mcp.setEnabled(savedEnabled);
        mcp.setAllowAdmin(savedAdmin);
        mcp.setAllowedUsers(savedUsers);
      }
    });
  }

  @Test
  void rootRetainsServerAdminTools() throws Exception {
    testEachServer((serverIndex) -> {
      final MCPConfiguration mcp = MCPPlugin.of(getServer(serverIndex)).getConfiguration();
      final boolean savedEnabled = mcp.isEnabled();
      final boolean savedAdmin = mcp.isAllowAdmin();
      final List<String> savedUsers = mcp.getAllowedUsers();
      final ContextConfiguration serverConfig = getServer(serverIndex).getConfiguration();
      final String savedServerName = serverConfig.getValueAsString(GlobalConfiguration.SERVER_NAME);
      try {
        mcp.setEnabled(true);
        mcp.setAllowAdmin(true);
        mcp.setAllowedUsers(List.of("*"));

        final String rootAuth = basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS);

        // Positive control: the tools are live and reachable, so the denials above come from the authorization check
        // and not from a broken code path.
        final JSONObject setSetting = mcp(serverIndex, rootAuth,
            toolCall("set_server_setting", new JSONObject()
                .put("key", GlobalConfiguration.SERVER_NAME.getKey())
                .put("value", "root-set-name")));
        assertThat(toolIsError(setSetting)).as("root must still be able to set a server setting").isFalse();
        assertThat(serverConfig.getValueAsString(GlobalConfiguration.SERVER_NAME)).isEqualTo("root-set-name");

        assertThat(toolIsError(mcp(serverIndex, rootAuth,
            toolCall("profiler_start", new JSONObject().put("timeoutSeconds", 5)))))
            .as("root must still be able to start the profiler").isFalse();
        assertThat(toolIsError(mcp(serverIndex, rootAuth, toolCall("profiler_stop", new JSONObject()))))
            .as("root must still be able to stop the profiler").isFalse();
      } finally {
        serverConfig.setValue(GlobalConfiguration.SERVER_NAME, savedServerName);
        mcp.setEnabled(savedEnabled);
        mcp.setAllowAdmin(savedAdmin);
        mcp.setAllowedUsers(savedUsers);
      }
    });
  }

  private void createReaderUser(final int serverIndex) throws Exception {
    final ServerSecurity security = getServer(serverIndex).getSecurity();

    // A group that grants record reads but no database-level (admin) permission.
    ServerSecurityTestAccess.databaseGroups(security, getDatabaseName()).put("mcpServerAdminReader",
        new JSONObject().put("access", new JSONArray()).put("types",
            new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));
    security.saveGroups();

    if (security.existsUser(READER_USER))
      security.dropUser(READER_USER);

    final JSONObject payload = new JSONObject()
        .put("name", READER_USER)
        .put("password", READER_PWD)
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("mcpServerAdminReader")));

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
        "/api/v1/server/users?name=" + URLEncoder.encode(name, StandardCharsets.UTF_8),
        basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
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
