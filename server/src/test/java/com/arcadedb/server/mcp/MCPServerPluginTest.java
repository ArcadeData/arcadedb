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
package com.arcadedb.server.mcp;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class MCPServerPluginTest extends BaseGraphServerTest {

  private String getMcpUrl() {
    return "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/mcp";
  }

  private String getMcpConfigUrl() {
    return "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/mcp/config";
  }

  @BeforeEach
  void enableMCP() throws Exception {
    // MCP is disabled by default, enable it for tests
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowedUsers", new JSONArray().put("root")));
  }

  @Test
  void initialize() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 1)
        .put("method", "initialize")
        .put("params", new JSONObject()));

    assertThat(response.has("result")).isTrue();
    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getString("protocolVersion")).isNotEmpty();
    assertThat(result.getJSONObject("serverInfo").getString("name")).isEqualTo("arcadedb");
    assertThat(result.getJSONObject("capabilities").has("tools")).isTrue();
  }

  @Test
  void toolsList() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 2)
        .put("method", "tools/list")
        .put("params", new JSONObject()));

    assertThat(response.has("result")).isTrue();
    final JSONArray tools = response.getJSONObject("result").getJSONArray("tools");
    assertThat(tools.length()).isEqualTo(5);

    // Verify tool names
    boolean hasListDatabases = false;
    boolean hasGetSchema = false;
    boolean hasQuery = false;
    boolean hasExecuteCommand = false;
    boolean hasServerStatus = false;

    for (int i = 0; i < tools.length(); i++) {
      final String name = tools.getJSONObject(i).getString("name");
      switch (name) {
      case "list_databases" -> hasListDatabases = true;
      case "get_schema" -> hasGetSchema = true;
      case "query" -> hasQuery = true;
      case "execute_command" -> hasExecuteCommand = true;
      case "server_status" -> hasServerStatus = true;
      }
    }
    assertThat(hasListDatabases).isTrue();
    assertThat(hasGetSchema).isTrue();
    assertThat(hasQuery).isTrue();
    assertThat(hasExecuteCommand).isTrue();
    assertThat(hasServerStatus).isTrue();
  }

  @Test
  void listDatabases() throws Exception {
    final JSONObject response = callTool("list_databases", new JSONObject());

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.has("databases")).isTrue();
    final JSONArray databases = result.getJSONArray("databases");
    // The test creates a "graph" database
    boolean foundGraph = false;
    for (int i = 0; i < databases.length(); i++)
      if ("graph".equals(databases.getString(i)))
        foundGraph = true;
    assertThat(foundGraph).isTrue();
  }

  @Test
  void getSchema() throws Exception {
    final JSONObject response = callTool("get_schema", new JSONObject().put("database", "graph"));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.getString("database")).isEqualTo("graph");
    assertThat(result.has("types")).isTrue();

    // Should have V1, V2, E1, E2, Person types from BaseGraphServerTest
    final JSONArray types = result.getJSONArray("types");
    assertThat(types.length()).isGreaterThanOrEqualTo(5);

    boolean foundV1 = false;
    for (int i = 0; i < types.length(); i++) {
      final JSONObject type = types.getJSONObject(i);
      if ("V1".equals(type.getString("name"))) {
        foundV1 = true;
        assertThat(type.getString("category")).isEqualTo("vertex");
        // V1 has an "id" property and an index
        assertThat(type.has("properties")).isTrue();
        assertThat(type.has("indexes")).isTrue();
      }
    }
    assertThat(foundV1).isTrue();
  }

  @Test
  void query() throws Exception {
    final JSONObject response = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "sql")
        .put("query", "SELECT FROM V1"));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.has("records")).isTrue();
    assertThat(result.getInt("count")).isGreaterThan(0);
  }

  @Test
  void executeCommand() throws Exception {
    // Enable insert permission
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("execute_command", new JSONObject()
        .put("database", "graph")
        .put("language", "sql")
        .put("command", "INSERT INTO V1 SET id = 999, name = 'mcpTest'"));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.getInt("count")).isGreaterThan(0);
  }

  @Test
  void executeCommandDeniedByPermission() throws Exception {
    // Ensure insert is disabled
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("execute_command", new JSONObject()
        .put("database", "graph")
        .put("language", "sql")
        .put("command", "INSERT INTO V1 SET id = 998, name = 'shouldFail'"));

    assertThat(response.getBoolean("isError")).isTrue();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("not allowed");
  }

  @Test
  void serverStatus() throws Exception {
    final JSONObject response = callTool("server_status", new JSONObject());

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.has("version")).isTrue();
    assertThat(result.has("serverName")).isTrue();
    assertThat(result.has("databases")).isTrue();
  }

  @Test
  void ping() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 99)
        .put("method", "ping")
        .put("params", new JSONObject()));

    assertThat(response.has("result")).isTrue();
  }

  @Test
  void methodNotFound() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 100)
        .put("method", "nonexistent/method")
        .put("params", new JSONObject()));

    assertThat(response.has("error")).isTrue();
    assertThat(response.getJSONObject("error").getInt("code")).isEqualTo(-32601);
  }

  @Test
  void disabledMCP() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 101)
        .put("method", "initialize")
        .put("params", new JSONObject());
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(503);
    } finally {
      connection.disconnect();
    }

    // Re-enable for other tests
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowedUsers", new JSONArray().put("root")));
  }

  @Test
  void getConfig() throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpConfigUrl()).toURL().openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      final String body = FileUtils.readStreamAsString(connection.getInputStream(), "utf8");
      final JSONObject config = new JSONObject(body);
      assertThat(config.has("enabled")).isTrue();
      assertThat(config.has("allowReads")).isTrue();
      assertThat(config.has("allowedUsers")).isTrue();
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void unknownTool() throws Exception {
    final JSONObject response = callTool("nonexistent_tool", new JSONObject());
    assertThat(response.getBoolean("isError")).isTrue();
  }

  @Test
  void notificationReturns204() throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("method", "notifications/initialized")
        .put("params", new JSONObject());
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(204);
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void queryToolRejectsWriteQuery() throws Exception {
    final JSONObject response = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "sql")
        .put("query", "INSERT INTO V1 SET id = 9999, name = 'shouldFail'"));

    assertThat(response.getBoolean("isError")).isTrue();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("write operations");
  }

  @Test
  void unauthorizedUserDenied() throws Exception {
    // Configure only "root" as allowed user
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowedUsers", new JSONArray().put("root")));

    // Create a non-root user
    if (!getServer(0).getSecurity().existsUser("mcpuser"))
      getServer(0).getSecurity().createUser("mcpuser", "mcppassword");

    final String nonRootAuth = "Basic " + Base64.getEncoder()
        .encodeToString("mcpuser:mcppassword".getBytes(StandardCharsets.UTF_8));

    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", nonRootAuth);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 200)
        .put("method", "tools/list")
        .put("params", new JSONObject());
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(403);
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void queryWithLimit() throws Exception {
    final JSONObject response = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "sql")
        .put("query", "SELECT FROM V1")
        .put("limit", 1));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.getInt("count")).isEqualTo(1);
  }

  @Test
  void databaseAuthorizationDenied() throws Exception {
    // Configure MCP to allow "restricteduser"
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowedUsers", new JSONArray().put("root").put("restricteduser")));

    // Create a user with access only to a non-existent database "otherdb"
    if (!getServer(0).getSecurity().existsUser("restricteduser"))
      getServer(0).getSecurity().createUser(new JSONObject()
          .put("name", "restricteduser")
          .put("password", getServer(0).getSecurity().encodePassword("restrictedpass"))
          .put("databases", new JSONObject()
              .put("otherdb", new JSONArray().put("admin"))));

    final String restrictedAuth = "Basic " + Base64.getEncoder()
        .encodeToString("restricteduser:restrictedpass".getBytes(StandardCharsets.UTF_8));

    // Try to query "graph" database — user should be denied
    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", restrictedAuth);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 300)
        .put("method", "tools/call")
        .put("params", new JSONObject()
            .put("name", "query")
            .put("arguments", new JSONObject()
                .put("database", "graph")
                .put("language", "sql")
                .put("query", "SELECT FROM V1")));
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      final String body = FileUtils.readStreamAsString(connection.getInputStream(), "utf8");
      final JSONObject response = new JSONObject(body);
      assertThat(response.has("result")).isTrue();
      final JSONObject result = response.getJSONObject("result");
      assertThat(result.getBoolean("isError")).isTrue();
      final String text = result.getJSONArray("content").getJSONObject(0).getString("text");
      assertThat(text).contains("not authorized");
    } finally {
      connection.disconnect();
    }
  }

  // ---- Helper methods ----

  private JSONObject mcpRequest(final JSONObject request) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final byte[] data = request.toString().getBytes(StandardCharsets.UTF_8);
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(data);
    }

    connection.connect();

    try {
      final String body = FileUtils.readStreamAsString(connection.getInputStream(), "utf8");
      return new JSONObject(body);
    } finally {
      connection.disconnect();
    }
  }

  private JSONObject callTool(final String toolName, final JSONObject arguments) throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 10)
        .put("method", "tools/call")
        .put("params", new JSONObject()
            .put("name", toolName)
            .put("arguments", arguments)));

    assertThat(response.has("result")).isTrue();
    return response.getJSONObject("result");
  }

  private void saveMCPConfig(final JSONObject config) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpConfigUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final byte[] data = config.toString().getBytes(StandardCharsets.UTF_8);
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(data);
    }

    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }

  private static String getBasicAuth() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
