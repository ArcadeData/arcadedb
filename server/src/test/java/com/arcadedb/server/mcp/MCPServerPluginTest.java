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

import com.arcadedb.database.Database;
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
import java.util.HashSet;
import java.util.Set;

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
    seedFullTextIndex();
  }

  private void seedFullTextIndex() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (db.getSchema().existsType("Article"))
      return;

    db.transaction(() -> {
      // A single bucket keeps BM25 statistics (document frequency, average document length) computed over one
      // consistent corpus; the index scores per bucket, so a multi-bucket type could otherwise let term frequency
      // lose to a per-bucket IDF difference and make the ranking assertions flaky.
      db.command("sql", "CREATE DOCUMENT TYPE Article BUCKETS 1");
      db.command("sql", "CREATE PROPERTY Article.title STRING");
      db.command("sql", "CREATE PROPERTY Article.content STRING");
      db.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");
      db.command("sql", "CREATE INDEX ON Article (title) UNIQUE");

      db.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      db.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'python scripting language'");
      // All three documents tokenize to exactly three terms, so BM25 length normalization is identical across them.
      // Doc3 therefore outranks Doc1 and Doc2 purely on term frequency for 'language' (3 occurrences against 1),
      // which gives the ranking test an unambiguous top hit. Keep the three lengths equal when editing this seed.
      db.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'language language language'");

      // A second full-text index on Article, so 'typeName: Article' alone is ambiguous between the two.
      db.command("sql", "CREATE INDEX ON Article (title, content) FULL_TEXT");

      // The schema strips spaces when deriving an index name, so a property named 'my prop' yields Spaced[myprop].
      db.command("sql", "CREATE DOCUMENT TYPE Spaced BUCKETS 1");
      db.command("sql", "CREATE PROPERTY Spaced.`my prop` STRING");
      db.command("sql", "CREATE INDEX ON Spaced (`my prop`) FULL_TEXT");
      db.command("sql", "INSERT INTO Spaced SET `my prop` = 'java tooling'");

      // A full-text index declared on a supertype is named for the supertype and still returns subtype records.
      db.command("sql", "CREATE DOCUMENT TYPE Searchable");
      db.command("sql", "CREATE PROPERTY Searchable.text STRING");
      db.command("sql", "CREATE INDEX ON Searchable (text) FULL_TEXT");
      db.command("sql", "CREATE DOCUMENT TYPE Decision EXTENDS Searchable");
      db.command("sql", "INSERT INTO Decision SET text = 'approved the java migration'");
    });
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
    assertThat(tools.length()).isEqualTo(13);

    // Verify tool names
    boolean hasListDatabases = false;
    boolean hasGetSchema = false;
    boolean hasQuery = false;
    boolean hasExecuteCommand = false;
    boolean hasServerStatus = false;
    boolean hasProfilerStart = false;
    boolean hasProfilerStop = false;
    boolean hasProfilerStatus = false;
    boolean hasGetServerSettings = false;
    boolean hasSetServerSetting = false;
    boolean hasFullTextSearch = false;
    boolean hasUpsertEntity = false;
    boolean hasUpsertRelationship = false;

    for (int i = 0; i < tools.length(); i++) {
      final String name = tools.getJSONObject(i).getString("name");
      switch (name) {
      case "list_databases" -> hasListDatabases = true;
      case "get_schema" -> hasGetSchema = true;
      case "query" -> hasQuery = true;
      case "execute_command" -> hasExecuteCommand = true;
      case "server_status" -> hasServerStatus = true;
      case "profiler_start" -> hasProfilerStart = true;
      case "profiler_stop" -> hasProfilerStop = true;
      case "profiler_status" -> hasProfilerStatus = true;
      case "get_server_settings" -> hasGetServerSettings = true;
      case "set_server_setting" -> hasSetServerSetting = true;
      case "full_text_search" -> hasFullTextSearch = true;
      case "upsert_entity" -> hasUpsertEntity = true;
      case "upsert_relationship" -> hasUpsertRelationship = true;
      }
    }
    assertThat(hasListDatabases).isTrue();
    assertThat(hasGetSchema).isTrue();
    assertThat(hasQuery).isTrue();
    assertThat(hasExecuteCommand).isTrue();
    assertThat(hasServerStatus).isTrue();
    assertThat(hasProfilerStart).isTrue();
    assertThat(hasProfilerStop).isTrue();
    assertThat(hasProfilerStatus).isTrue();
    assertThat(hasGetServerSettings).isTrue();
    assertThat(hasSetServerSetting).isTrue();
    assertThat(hasFullTextSearch).isTrue();
    assertThat(hasUpsertEntity).isTrue();
    assertThat(hasUpsertRelationship).isTrue();
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

  @Test
  void profilerStartStopCycle() throws Exception {
    // Enable admin permission for profiler
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowAdmin", true)
        .put("allowedUsers", new JSONArray().put("root")));

    // Start profiler
    final JSONObject startResponse = callTool("profiler_start", new JSONObject().put("timeoutSeconds", 30));
    assertThat(startResponse.getBoolean("isError", true)).isFalse();
    final JSONObject startResult = new JSONObject(startResponse.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(startResult.getString("status")).isEqualTo("started");
    assertThat(startResult.getInt("timeoutSeconds")).isEqualTo(30);

    // Run a query through the server database so the profiler captures it
    getServer(0).getDatabase("graph").query("sql", "SELECT FROM V1 LIMIT 1").close();

    // Check status while recording
    final JSONObject statusResponse = callTool("profiler_status", new JSONObject());
    assertThat(statusResponse.getBoolean("isError", true)).isFalse();
    final JSONObject statusResult = new JSONObject(statusResponse.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(statusResult.getBoolean("recording")).isTrue();
    assertThat(statusResult.getInt("totalQueries")).isGreaterThan(0);

    // Stop profiler
    final JSONObject stopResponse = callTool("profiler_stop", new JSONObject());
    assertThat(stopResponse.getBoolean("isError", true)).isFalse();
    final JSONObject stopResult = new JSONObject(stopResponse.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(stopResult.getBoolean("recording")).isFalse();
    assertThat(stopResult.has("queries")).isTrue();
    assertThat(stopResult.getInt("totalQueries")).isGreaterThan(0);
  }

  @Test
  void profilerStartAlreadyRecording() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowAdmin", true)
        .put("allowedUsers", new JSONArray().put("root")));

    // Start profiler
    callTool("profiler_start", new JSONObject());

    // Try to start again
    final JSONObject response = callTool("profiler_start", new JSONObject());
    assertThat(response.getBoolean("isError", true)).isFalse();
    final JSONObject result = new JSONObject(response.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(result.getString("status")).isEqualTo("already_recording");

    // Cleanup: stop the profiler
    callTool("profiler_stop", new JSONObject());
  }

  @Test
  void profilerDeniedWithoutAdminPermission() throws Exception {
    // Ensure admin is disabled
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowAdmin", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("profiler_start", new JSONObject());
    assertThat(response.getBoolean("isError")).isTrue();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("not allowed");
  }

  @Test
  void profilerStatusWhenNeverStarted() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowAdmin", true)
        .put("allowedUsers", new JSONArray().put("root")));

    // Reset profiler to ensure clean state
    getServer(0).getQueryProfiler().reset();

    final JSONObject response = callTool("profiler_status", new JSONObject());
    assertThat(response.getBoolean("isError", true)).isFalse();
    final JSONObject result = new JSONObject(response.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(result.getBoolean("recording")).isFalse();
    assertThat(result.getInt("totalQueries")).isEqualTo(0);
  }

  @Test
  void getServerSettings() throws Exception {
    final JSONObject response = callTool("get_server_settings", new JSONObject());

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.has("settings")).isTrue();
    final JSONArray settings = result.getJSONArray("settings");
    assertThat(settings.length()).isGreaterThan(0);

    // Verify each setting has required fields
    final JSONObject first = settings.getJSONObject(0);
    assertThat(first.has("key")).isTrue();
    assertThat(first.has("value")).isTrue();
    assertThat(first.has("description")).isTrue();

    // Verify passwords are masked
    boolean foundPassword = false;
    for (int i = 0; i < settings.length(); i++) {
      final JSONObject setting = settings.getJSONObject(i);
      if (setting.getString("key").toLowerCase().contains("password")) {
        assertThat(setting.getString("value")).isEqualTo("*****");
        foundPassword = true;
      }
    }
    assertThat(foundPassword).isTrue();
  }

  @Test
  void getServerSettingsDeniedWithoutReadPermission() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("get_server_settings", new JSONObject());
    assertThat(response.getBoolean("isError")).isTrue();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("not allowed");
  }

  @Test
  void setServerSetting() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowAdmin", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("set_server_setting", new JSONObject()
        .put("key", "arcadedb.sqlStatementCache")
        .put("value", "500"));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.getString("key")).isEqualTo("arcadedb.sqlStatementCache");
    assertThat(result.getString("newValue")).isEqualTo("500");

    // Restore default
    callTool("set_server_setting", new JSONObject()
        .put("key", "arcadedb.sqlStatementCache")
        .put("value", "300"));
  }

  @Test
  void setServerSettingDeniedWithoutAdminPermission() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowAdmin", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("set_server_setting", new JSONObject()
        .put("key", "arcadedb.sqlStatementCache")
        .put("value", "500"));

    assertThat(response.getBoolean("isError")).isTrue();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("not allowed");
  }

  @Test
  void setServerSettingUnknownKey() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowAdmin", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("set_server_setting", new JSONObject()
        .put("key", "arcadedb.nonExistentSetting")
        .put("value", "foo"));

    assertThat(response.getBoolean("isError")).isTrue();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("Unknown server setting");
  }

  @Test
  void apiTokenUserAllowedByTokenName() throws Exception {
    // Create an API token
    final JSONObject permissions = new JSONObject()
        .put("types", new JSONObject()
            .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))))
        .put("database", new JSONArray());

    final JSONObject tokenResult = getServer(0).getSecurity().getApiTokenConfiguration()
        .createToken("mcptoken", "graph", 0, permissions);
    final String tokenValue = tokenResult.getString("token");

    try {
      // Configure MCP to allow "mcptoken" (the bare token name, not "apitoken:mcptoken")
      saveMCPConfig(new JSONObject()
          .put("enabled", true)
          .put("allowReads", true)
          .put("allowedUsers", new JSONArray().put("root").put("mcptoken")));

      // Use the API token to call MCP initialize
      final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Bearer " + tokenValue);
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);

      final JSONObject request = new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 500)
          .put("method", "initialize")
          .put("params", new JSONObject());
      try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
        out.write(request.toString().getBytes(StandardCharsets.UTF_8));
      }
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(200);
        final String body = FileUtils.readStreamAsString(connection.getInputStream(), "utf8");
        final JSONObject response = new JSONObject(body);
        assertThat(response.has("result")).isTrue();
        assertThat(response.getJSONObject("result").has("protocolVersion")).isTrue();
      } finally {
        connection.disconnect();
      }
    } finally {
      // Cleanup: delete the token
      getServer(0).getSecurity().getApiTokenConfiguration()
          .deleteToken(tokenResult.getString("tokenHash"));
    }
  }

  @Test
  void apiTokenUserDeniedWhenNotInAllowedUsers() throws Exception {
    // Create an API token
    final JSONObject permissions = new JSONObject()
        .put("types", new JSONObject()
            .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))))
        .put("database", new JSONArray());

    final JSONObject tokenResult = getServer(0).getSecurity().getApiTokenConfiguration()
        .createToken("deniedtoken", "graph", 0, permissions);
    final String tokenValue = tokenResult.getString("token");

    try {
      // Configure MCP with only "root" — token name "deniedtoken" is NOT in the list
      saveMCPConfig(new JSONObject()
          .put("enabled", true)
          .put("allowReads", true)
          .put("allowedUsers", new JSONArray().put("root")));

      final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Bearer " + tokenValue);
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);

      final JSONObject request = new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 501)
          .put("method", "initialize")
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
    } finally {
      getServer(0).getSecurity().getApiTokenConfiguration()
          .deleteToken(tokenResult.getString("tokenHash"));
    }
  }

  @Test
  void apiTokenUserAllowedByWildcard() throws Exception {
    // Create an API token
    final JSONObject permissions = new JSONObject()
        .put("types", new JSONObject()
            .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))))
        .put("database", new JSONArray());

    final JSONObject tokenResult = getServer(0).getSecurity().getApiTokenConfiguration()
        .createToken("wildcardtoken", "graph", 0, permissions);
    final String tokenValue = tokenResult.getString("token");

    try {
      // Configure MCP with wildcard "*"
      saveMCPConfig(new JSONObject()
          .put("enabled", true)
          .put("allowReads", true)
          .put("allowedUsers", new JSONArray().put("*")));

      final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Bearer " + tokenValue);
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);

      final JSONObject request = new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 502)
          .put("method", "initialize")
          .put("params", new JSONObject());
      try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
        out.write(request.toString().getBytes(StandardCharsets.UTF_8));
      }
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(200);
        final String body = FileUtils.readStreamAsString(connection.getInputStream(), "utf8");
        final JSONObject response = new JSONObject(body);
        assertThat(response.has("result")).isTrue();
      } finally {
        connection.disconnect();
      }
    } finally {
      getServer(0).getSecurity().getApiTokenConfiguration()
          .deleteToken(tokenResult.getString("tokenHash"));
    }
  }

  @Test
  void queryUnknownDatabaseReturnsAvailableList() throws Exception {
    final JSONObject response = callTool("query", new JSONObject()
        .put("database", "nonexistent_db")
        .put("language", "cypher")
        .put("query", "RETURN 1"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("nonexistent_db");
    assertThat(errorText).containsIgnoringCase("available databases");
    assertThat(errorText).contains("graph");
  }

  @Test
  void executeCommandUnknownDatabaseReturnsAvailableList() throws Exception {
    final JSONObject response = callTool("execute_command", new JSONObject()
        .put("database", "nonexistent_db")
        .put("language", "cypher")
        .put("command", "CREATE (n:Test) RETURN n"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("nonexistent_db");
    assertThat(errorText).containsIgnoringCase("available databases");
    assertThat(errorText).contains("graph");
  }

  @Test
  void getSchemaUnknownDatabaseReturnsAvailableList() throws Exception {
    final JSONObject response = callTool("get_schema", new JSONObject()
        .put("database", "nonexistent_db"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("nonexistent_db");
    assertThat(errorText).containsIgnoringCase("available databases");
    assertThat(errorText).contains("graph");
  }

  @Test
  void fullTextSearchByIndexName() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isFalse();

    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    assertThat(payload.getString("indexName")).isEqualTo("Article[content]");
    assertThat(payload.getString("similarity")).isEqualTo("BM25");
    assertThat(payload.getInt("count")).isEqualTo(1);

    final JSONObject hit = payload.getJSONArray("results").getJSONObject(0);
    assertThat(hit.getString("rid")).startsWith("#");
    assertThat(hit.getFloat("score")).isGreaterThan(0f);
    assertThat(hit.getJSONObject("properties").getString("title")).isEqualTo("Doc1");
  }

  @Test
  void fullTextSearchRanksAndLimits() throws Exception {
    final JSONObject unlimitedResponse = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("queryText", "language"));

    final JSONObject unlimitedPayload = new JSONObject(
        unlimitedResponse.getJSONArray("content").getJSONObject(0).getString("text"));

    // Doc1, Doc2 and Doc3 all contain "language".
    assertThat(unlimitedPayload.getInt("count")).isEqualTo(3);

    final JSONArray results = unlimitedPayload.getJSONArray("results");
    final JSONObject first = results.getJSONObject(0);
    final JSONObject second = results.getJSONObject(1);
    assertThat(first.getFloat("score")).isGreaterThanOrEqualTo(second.getFloat("score"));
    // Doc3 repeats "language" three times where Doc1 and Doc2 mention it once, and all three are the same length,
    // so term frequency alone must put Doc3 on top. This assertion fails if the score-descending sort is dropped.
    assertThat(first.getJSONObject("properties").getString("title")).isEqualTo("Doc3");

    final JSONObject limitedResponse = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("queryText", "language")
        .put("limit", 1));

    final JSONObject limitedPayload = new JSONObject(
        limitedResponse.getJSONArray("content").getJSONObject(0).getString("text"));

    // The limit must cut the result set down to the single best-scoring hit, not an arbitrary one.
    assertThat(limitedPayload.getInt("count")).isEqualTo(1);
    final JSONObject limitedHit = limitedPayload.getJSONArray("results").getJSONObject(0);
    assertThat(limitedHit.getJSONObject("properties").getString("title")).isEqualTo("Doc3");
  }

  @Test
  void fullTextSearchRejectsNonPositiveLimit() throws Exception {
    final JSONObject zeroResponse = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("queryText", "java")
        .put("limit", 0));

    assertThat(zeroResponse.getBoolean("isError", false)).isTrue();
    final String zeroErrorText = zeroResponse.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(zeroErrorText).containsIgnoringCase("limit");

    final JSONObject negativeResponse = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("queryText", "java")
        .put("limit", -1));

    assertThat(negativeResponse.getBoolean("isError", false)).isTrue();
    final String negativeErrorText = negativeResponse.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(negativeErrorText).containsIgnoringCase("limit");
  }

  @Test
  void fullTextSearchDerivesIndexNameWithSpacesStripped() throws Exception {
    // The schema registered this index as Spaced[myprop]; deriving 'Spaced[my prop]' verbatim would never match it.
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "Spaced")
        .put("properties", new JSONArray().put("my prop"))
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isFalse();

    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    assertThat(payload.getString("indexName")).isEqualTo("Spaced[myprop]");
    assertThat(payload.getInt("count")).isEqualTo(1);
  }

  @Test
  void fullTextSearchRejectsBlankQueryText() throws Exception {
    // The Lucene parser turns a blank query into IndexException("Invalid search query: "), which names no cause. The
    // tool must reject it with a message that says which argument is wrong.
    for (final String blank : new String[] { "", "   " }) {
      final JSONObject response = callTool("full_text_search", new JSONObject()
          .put("database", getDatabaseName())
          .put("indexName", "Article[content]")
          .put("queryText", blank));

      assertThat(response.getBoolean("isError", false)).isTrue();
      final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
      assertThat(errorText).contains("queryText");
      assertThat(errorText).doesNotContain("Invalid search query");
    }
  }

  @Test
  void fullTextSearchDeniedWhenReadsDisabled() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("not allowed");
  }

  @Test
  void fullTextSearchIndexNameWinsOverTypeName() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("typeName", "Searchable")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isFalse();

    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    // 'typeName' addresses an unrelated index (Searchable[text]); 'indexName' must win and be used as-is.
    assertThat(payload.getString("indexName")).isEqualTo("Article[content]");
  }

  @Test
  void fullTextSearchByTypeNameAndProperties() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "Article")
        .put("properties", new JSONArray().put("content"))
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isFalse();

    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    assertThat(payload.getString("indexName")).isEqualTo("Article[content]");
    assertThat(payload.getInt("count")).isEqualTo(1);
  }

  @Test
  void fullTextSearchByTypeNameAloneWhenUnambiguous() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "Searchable")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isFalse();

    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    // The index lives on the supertype; the hit is a Decision, a subtype record.
    assertThat(payload.getString("indexName")).isEqualTo("Searchable[text]");
    assertThat(payload.getInt("count")).isEqualTo(1);
    assertThat(payload.getJSONArray("results").getJSONObject(0)
        .getJSONObject("properties").getString("@type")).isEqualTo("Decision");
  }

  @Test
  void fullTextSearchAmbiguousTypeNameListsCandidates() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "Article")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("Article[content]");
    assertThat(errorText).contains("Article[title,content]");
  }

  @Test
  void fullTextSearchOnSubtypeNameGuidesToSupertypeIndex() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "Decision")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("Decision");
    assertThat(errorText).contains("Searchable[text]");
    assertThat(errorText).containsIgnoringCase("supertype");
  }

  @Test
  void fullTextSearchUnknownIndexListsAvailable() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Artcle[content]")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("Artcle[content]");
    assertThat(errorText).containsIgnoringCase("available full-text indexes");
    assertThat(errorText).contains("Article[content]");
  }

  @Test
  void fullTextSearchOnNonFullTextIndexIsRejected() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[title]")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("Article[title]");
    assertThat(errorText).contains("is not a full-text index");
    assertThat(errorText).containsIgnoringCase("available full-text indexes");
    assertThat(errorText).contains("Article[content]");
  }

  @Test
  void fullTextSearchWithoutAddressingIsRejected() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("indexName");
    assertThat(errorText).contains("typeName");
  }

  @Test
  void initializeAdvertisesResourcesCapability() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 400)
        .put("method", "initialize")
        .put("params", new JSONObject()));

    final JSONObject capabilities = response.getJSONObject("result").getJSONObject("capabilities");
    assertThat(capabilities.has("resources")).isTrue();
    final JSONObject resources = capabilities.getJSONObject("resources");
    assertThat(resources.getBoolean("listChanged")).isFalse();
    assertThat(resources.getBoolean("subscribe")).isFalse();
  }

  @Test
  void resourcesList() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 401)
        .put("method", "resources/list")
        .put("params", new JSONObject()));

    final JSONArray resources = response.getJSONObject("result").getJSONArray("resources");

    JSONObject graphResource = null;
    for (int i = 0; i < resources.length(); i++)
      if ("arcadedb://graph/schema".equals(resources.getJSONObject(i).getString("uri")))
        graphResource = resources.getJSONObject(i);

    assertThat(graphResource).isNotNull();
    assertThat(graphResource.getString("name")).isEqualTo("graph schema");
    assertThat(graphResource.getString("mimeType")).isEqualTo("application/json");
  }

  @Test
  void resourcesListMatchesListDatabases() throws Exception {
    final JSONObject listResponse = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 402)
        .put("method", "resources/list")
        .put("params", new JSONObject()));

    final Set<String> fromResources = new HashSet<>();
    final JSONArray resources = listResponse.getJSONObject("result").getJSONArray("resources");
    for (int i = 0; i < resources.length(); i++) {
      final String uri = resources.getJSONObject(i).getString("uri");
      fromResources.add(uri.substring("arcadedb://".length(), uri.length() - "/schema".length()));
    }

    final JSONObject toolResponse = callTool("list_databases", new JSONObject());
    final JSONArray databases = new JSONObject(
        toolResponse.getJSONArray("content").getJSONObject(0).getString("text")).getJSONArray("databases");

    final Set<String> fromTool = new HashSet<>();
    for (int i = 0; i < databases.length(); i++)
      fromTool.add(databases.getString(i));

    assertThat(fromResources).isEqualTo(fromTool);
  }

  @Test
  void resourcesReadMatchesGetSchemaTool() throws Exception {
    final JSONObject readResponse = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 403)
        .put("method", "resources/read")
        .put("params", new JSONObject().put("uri", "arcadedb://graph/schema")));

    final JSONArray contents = readResponse.getJSONObject("result").getJSONArray("contents");
    assertThat(contents.length()).isEqualTo(1);
    assertThat(contents.getJSONObject(0).getString("uri")).isEqualTo("arcadedb://graph/schema");
    assertThat(contents.getJSONObject(0).getString("mimeType")).isEqualTo("application/json");

    final JSONObject toolResponse = callTool("get_schema", new JSONObject().put("database", "graph"));
    final String toolText = toolResponse.getJSONArray("content").getJSONObject(0).getString("text");

    assertThat(contents.getJSONObject(0).getString("text")).isEqualTo(toolText);
  }

  @Test
  void resourcesReadUnknownDatabaseReturnsNotFound() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 404)
        .put("method", "resources/read")
        .put("params", new JSONObject().put("uri", "arcadedb://nosuchdb/schema")));

    assertThat(response.has("error")).isTrue();
    assertThat(response.getJSONObject("error").getInt("code")).isEqualTo(-32002);
  }

  @Test
  void resourcesReadMalformedUriReturnsNotFound() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 405)
        .put("method", "resources/read")
        .put("params", new JSONObject().put("uri", "arcadedb://graph/tables")));

    assertThat(response.has("error")).isTrue();
    assertThat(response.getJSONObject("error").getInt("code")).isEqualTo(-32002);
  }

  @Test
  void resourcesDeniedWhenReadsDisabled() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", false)
        .put("allowedUsers", new JSONArray().put("root")));

    try {
      final JSONObject listResponse = mcpRequest(new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 406)
          .put("method", "resources/list")
          .put("params", new JSONObject()));

      // A discovery call stays quiet: nothing is readable, so nothing is listed.
      assertThat(listResponse.getJSONObject("result").getJSONArray("resources").length()).isZero();

      final JSONObject readResponse = mcpRequest(new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 407)
          .put("method", "resources/read")
          .put("params", new JSONObject().put("uri", "arcadedb://graph/schema")));

      assertThat(readResponse.has("error")).isTrue();
      assertThat(readResponse.getJSONObject("error").getInt("code")).isEqualTo(-32600);
      assertThat(readResponse.getJSONObject("error").getString("message")).contains("not allowed");
    } finally {
      // Restore for the other tests in this class.
      saveMCPConfig(new JSONObject()
          .put("enabled", true)
          .put("allowReads", true)
          .put("allowedUsers", new JSONArray().put("root")));
    }
  }

  @Test
  void resourcesListOmitsUnauthorizedDatabases() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowedUsers", new JSONArray().put("root").put("restricteduser")));

    // A user authorized only for a database that does not exist here, so "graph" must not appear in its resource list.
    if (!getServer(0).getSecurity().existsUser("restricteduser"))
      getServer(0).getSecurity().createUser(new JSONObject()
          .put("name", "restricteduser")
          .put("password", getServer(0).getSecurity().encodePassword("restrictedpass"))
          .put("databases", new JSONObject()
              .put("otherdb", new JSONArray().put("admin"))));

    final String restrictedAuth = "Basic " + Base64.getEncoder()
        .encodeToString("restricteduser:restrictedpass".getBytes(StandardCharsets.UTF_8));

    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", restrictedAuth);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 408)
        .put("method", "resources/list")
        .put("params", new JSONObject());
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      final String body = FileUtils.readStreamAsString(connection.getInputStream(), "utf8");
      final JSONArray resources = new JSONObject(body).getJSONObject("result").getJSONArray("resources");

      for (int i = 0; i < resources.length(); i++)
        assertThat(resources.getJSONObject(i).getString("uri")).isNotEqualTo("arcadedb://graph/schema");
    } finally {
      connection.disconnect();
      saveMCPConfig(new JSONObject()
          .put("enabled", true)
          .put("allowReads", true)
          .put("allowedUsers", new JSONArray().put("root")));
    }
  }

  @Test
  void disabledServerErrorEchoesRequestId() throws Exception {
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
        .put("id", 409)
        .put("method", "initialize")
        .put("params", new JSONObject());
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(503);
      // A 503 body arrives on the error stream; it must echo the request id per JSON-RPC 2.0.
      final String body = FileUtils.readStreamAsString(connection.getErrorStream(), "utf8");
      final JSONObject response = new JSONObject(body);
      assertThat(response.getInt("id")).isEqualTo(409);
      assertThat(response.getJSONObject("error").getInt("code")).isEqualTo(-32600);
    } finally {
      connection.disconnect();
      saveMCPConfig(new JSONObject()
          .put("enabled", true)
          .put("allowReads", true)
          .put("allowedUsers", new JSONArray().put("root")));
    }
  }

  @Test
  void upsertEntityIsIdempotent() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject args = new JSONObject()
        .put("database", "graph")
        .put("typeName", "UpsertPerson")
        .put("matchKeys", new JSONObject().put("email", "ada@x.com"))
        .put("setProperties", new JSONObject().put("name", "Ada"));

    final JSONObject first = callTool("upsert_entity", args);
    assertThat(first.getBoolean("isError", true)).isFalse();

    // Second call with identical matchKeys must not create a second node.
    callTool("upsert_entity", new JSONObject(args.toString())
        .put("setProperties", new JSONObject().put("name", "Ada Lovelace")));

    final JSONObject countResp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (p:UpsertPerson {email: 'ada@x.com'}) RETURN count(p) AS c"));
    final JSONObject countPayload = new JSONObject(
        countResp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(countPayload.getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(1);
  }

  @Test
  void upsertEntityWithoutSetPropertiesCreatesNode() throws Exception {
    // A bare MERGE (no SET) analyzes to {CREATE, UPDATE}, so both flags are required. This test covers the
    // no-SET execution path: the node is created and a repeated call matches rather than duplicating it.
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject args = new JSONObject()
        .put("database", "graph")
        .put("typeName", "NoSetPerson")
        .put("matchKeys", new JSONObject().put("name", "Solo"));

    final JSONObject first = callTool("upsert_entity", args);
    assertThat(first.getBoolean("isError", true)).isFalse();

    final JSONObject firstCountResp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (n:NoSetPerson {name:'Solo'}) RETURN count(n) AS c"));
    final JSONObject firstCountPayload = new JSONObject(
        firstCountResp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(firstCountPayload.getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(1);

    // Repeat with identical args (still no setProperties): the MERGE must match, not duplicate.
    callTool("upsert_entity", new JSONObject(args.toString()));

    final JSONObject secondCountResp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (n:NoSetPerson {name:'Solo'}) RETURN count(n) AS c"));
    final JSONObject secondCountPayload = new JSONObject(
        secondCountResp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(secondCountPayload.getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(1);
  }

  @Test
  void upsertEntityBindsValuesSoInjectionIsInert() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final String malicious = "x'}) DETACH DELETE n //";
    final JSONObject args = new JSONObject()
        .put("database", "graph")
        .put("typeName", "InjTest")
        .put("matchKeys", new JSONObject().put("k", malicious));

    callTool("upsert_entity", args);
    callTool("upsert_entity", new JSONObject(args.toString())); // repeat: still one node

    final JSONObject resp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (n:InjTest) RETURN count(n) AS c"));
    final JSONObject payload = new JSONObject(resp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(payload.getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(1);
  }

  @Test
  void upsertEntityRequiresBothInsertAndUpdate() throws Exception {
    // allowUpdate off: a MERGE...SET needs UPDATE, so it must be denied.
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject resp = callTool("upsert_entity", new JSONObject()
        .put("database", "graph")
        .put("typeName", "UpsertPerson")
        .put("matchKeys", new JSONObject().put("email", "denied@x.com"))
        .put("setProperties", new JSONObject().put("name", "Nope")));

    assertThat(resp.getBoolean("isError")).isTrue();
    final String text = resp.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("not allowed");
  }

  @Test
  void upsertEntityRejectsEmptyMatchKeys() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject resp = callTool("upsert_entity", new JSONObject()
        .put("database", "graph")
        .put("typeName", "UpsertPerson")
        .put("matchKeys", new JSONObject()));

    assertThat(resp.getBoolean("isError")).isTrue();
    final String text = resp.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("matchKeys");
  }

  @Test
  void upsertRelationshipDoesNotDuplicateEdge() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject args = new JSONObject()
        .put("database", "graph")
        .put("fromType", "Author")
        .put("fromMatchKeys", new JSONObject().put("name", "Ada"))
        .put("toType", "Book")
        .put("toMatchKeys", new JSONObject().put("isbn", "111"))
        .put("relType", "WROTE")
        .put("relProperties", new JSONObject().put("year", 1843));

    final JSONObject first = callTool("upsert_relationship", args);
    assertThat(first.getBoolean("isError", true)).isFalse();

    // Repeat with a different property value: the edge must be updated, not duplicated.
    callTool("upsert_relationship", new JSONObject(args.toString())
        .put("relProperties", new JSONObject().put("year", 1844)));

    final JSONObject resp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (:Author {name:'Ada'})-[r:WROTE]->(:Book {isbn:'111'}) RETURN count(r) AS c"));
    final JSONObject payload = new JSONObject(resp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(payload.getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(1);

    // The second upsert_relationship call must have updated the existing edge's property.
    final JSONObject yearResp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (:Author {name:'Ada'})-[r:WROTE]->(:Book {isbn:'111'}) RETURN r.year AS y"));
    final JSONObject yearPayload = new JSONObject(yearResp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(yearPayload.getJSONArray("records").getJSONObject(0).getInt("y")).isEqualTo(1844);
  }

  @Test
  void upsertRelationshipAutoCreatesEndpoints() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    callTool("upsert_relationship", new JSONObject()
        .put("database", "graph")
        .put("fromType", "City")
        .put("fromMatchKeys", new JSONObject().put("name", "Turin"))
        .put("toType", "Country")
        .put("toMatchKeys", new JSONObject().put("name", "Italy"))
        .put("relType", "IN_COUNTRY"));

    final JSONObject resp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (c:City {name:'Turin'})-[:IN_COUNTRY]->(n:Country {name:'Italy'}) RETURN count(*) AS c"));
    final JSONObject payload = new JSONObject(resp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(payload.getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(1);
  }

  @Test
  void upsertRelationshipDeniedWithoutInsert() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", false)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject resp = callTool("upsert_relationship", new JSONObject()
        .put("database", "graph")
        .put("fromType", "Author")
        .put("fromMatchKeys", new JSONObject().put("name", "X"))
        .put("toType", "Book")
        .put("toMatchKeys", new JSONObject().put("isbn", "999"))
        .put("relType", "WROTE"));

    assertThat(resp.getBoolean("isError")).isTrue();
    assertThat(resp.getJSONArray("content").getJSONObject(0).getString("text")).contains("not allowed");
  }

  @Test
  void upsertRelationshipRejectsEmptyMatchKeys() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject resp = callTool("upsert_relationship", new JSONObject()
        .put("database", "graph")
        .put("fromType", "Author")
        .put("fromMatchKeys", new JSONObject())
        .put("toType", "Book")
        .put("toMatchKeys", new JSONObject().put("isbn", "111"))
        .put("relType", "WROTE"));

    assertThat(resp.getBoolean("isError")).isTrue();
    assertThat(resp.getJSONArray("content").getJSONObject(0).getString("text")).contains("fromMatchKeys");
  }

  @Test
  void upsertEntityRejectsBacktickIdentifier() throws Exception {
    // The backtick guard lives in quoteIdentifier; this asserts it is actually wired into the tool path.
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject resp = callTool("upsert_entity", new JSONObject()
        .put("database", "graph")
        .put("typeName", "Bad`Type")
        .put("matchKeys", new JSONObject().put("id", "1")));

    assertThat(resp.getBoolean("isError")).isTrue();
    assertThat(resp.getJSONArray("content").getJSONObject(0).getString("text")).contains("backtick");
  }

  @Test
  void upsertEntityCompositeMatchKeysIsIdempotent() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject args = new JSONObject()
        .put("database", "graph")
        .put("typeName", "CompositePerson")
        .put("matchKeys", new JSONObject().put("firstName", "Ada").put("lastName", "Lovelace"))
        .put("setProperties", new JSONObject().put("role", "mathematician"));

    callTool("upsert_entity", args);
    // Repeat with the same two-key match: must resolve to the same node, not create a second.
    callTool("upsert_entity", new JSONObject(args.toString())
        .put("setProperties", new JSONObject().put("role", "pioneer")));

    final JSONObject resp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (p:CompositePerson {firstName:'Ada', lastName:'Lovelace'}) RETURN count(p) AS c"));
    final JSONObject payload = new JSONObject(resp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(payload.getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(1);
  }

  @Test
  void upsertEntityBindsSetPropertyValuesSoInjectionIsInert() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final String malicious = "'}) DETACH DELETE n //";
    callTool("upsert_entity", new JSONObject()
        .put("database", "graph")
        .put("typeName", "SetInjTest")
        .put("matchKeys", new JSONObject().put("id", "1"))
        .put("setProperties", new JSONObject().put("note", malicious)));

    // The node still exists and stores the payload verbatim, proving the SET value was bound, not executed.
    final JSONObject resp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (n:SetInjTest {id:'1'}) RETURN n.note AS note"));
    final JSONObject payload = new JSONObject(resp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(payload.getJSONArray("records").length()).isEqualTo(1);
    assertThat(payload.getJSONArray("records").getJSONObject(0).getString("note")).isEqualTo(malicious);
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
