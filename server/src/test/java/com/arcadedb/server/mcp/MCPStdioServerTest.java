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
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class MCPStdioServerTest extends BaseGraphServerTest {

  private MCPConfiguration config;
  private ServerSecurityUser user;

  @BeforeEach
  void setupMCP() {
    config = getServer(0).getMCPConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @Test
  void initialize() throws Exception {
    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 1)
        .put("method", "initialize")
        .put("params", new JSONObject());

    final JSONObject response = sendSingleRequest(request);

    assertThat(response.has("result")).isTrue();
    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getString("protocolVersion")).isNotEmpty();
    assertThat(result.getJSONObject("serverInfo").getString("name")).isEqualTo("arcadedb");
    assertThat(result.getJSONObject("capabilities").has("tools")).isTrue();
  }

  @Test
  void toolsList() throws Exception {
    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 2)
        .put("method", "tools/list")
        .put("params", new JSONObject());

    final JSONObject response = sendSingleRequest(request);

    assertThat(response.has("result")).isTrue();
    final JSONArray tools = response.getJSONObject("result").getJSONArray("tools");
    assertThat(tools.length()).isEqualTo(10);
  }

  @Test
  void listDatabases() throws Exception {
    final JSONObject response = callTool("list_databases", new JSONObject());

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.has("databases")).isTrue();
    final JSONArray databases = result.getJSONArray("databases");
    boolean foundGraph = false;
    for (int i = 0; i < databases.length(); i++)
      if ("graph".equals(databases.getString(i)))
        foundGraph = true;
    assertThat(foundGraph).isTrue();
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
  void ping() throws Exception {
    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 99)
        .put("method", "ping")
        .put("params", new JSONObject());

    final JSONObject response = sendSingleRequest(request);

    assertThat(response.has("result")).isTrue();
  }

  @Test
  void notificationGetsNoResponse() throws Exception {
    final JSONObject notification = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("method", "notifications/initialized")
        .put("params", new JSONObject());

    // Send notification followed by a ping so we can verify the notification produced no output
    final JSONObject ping = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 42)
        .put("method", "ping")
        .put("params", new JSONObject());

    final String inputData = notification + "\n" + ping + "\n";
    final ByteArrayInputStream in = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    final MCPStdioServer stdioServer = new MCPStdioServer(getServer(0), config, user, in, new PrintStream(out));
    stdioServer.run();

    // Should only have one response line (the ping), not two
    final String[] lines = out.toString(StandardCharsets.UTF_8).trim().split("\n");
    assertThat(lines).hasSize(1);
    final JSONObject response = new JSONObject(lines[0]);
    assertThat(response.getInt("id")).isEqualTo(42);
  }

  @Test
  void methodNotFound() throws Exception {
    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 100)
        .put("method", "nonexistent/method")
        .put("params", new JSONObject());

    final JSONObject response = sendSingleRequest(request);

    assertThat(response.has("error")).isTrue();
    assertThat(response.getJSONObject("error").getInt("code")).isEqualTo(-32601);
  }

  @Test
  void malformedJson() throws Exception {
    final String inputData = "this is not json\n";
    final ByteArrayInputStream in = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    final MCPStdioServer stdioServer = new MCPStdioServer(getServer(0), config, user, in, new PrintStream(out));
    stdioServer.run();

    final String outputStr = out.toString(StandardCharsets.UTF_8).trim();
    final JSONObject response = new JSONObject(outputStr);
    assertThat(response.has("error")).isTrue();
    assertThat(response.getJSONObject("error").getInt("code")).isEqualTo(-32700);
  }

  @Test
  void unknownTool() throws Exception {
    final JSONObject response = callTool("nonexistent_tool", new JSONObject());
    assertThat(response.getBoolean("isError")).isTrue();
  }

  @Test
  void multipleRequestsInSequence() throws Exception {
    final JSONObject init = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 1)
        .put("method", "initialize")
        .put("params", new JSONObject());
    final JSONObject list = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 2)
        .put("method", "tools/list")
        .put("params", new JSONObject());
    final JSONObject ping = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 3)
        .put("method", "ping")
        .put("params", new JSONObject());

    final String inputData = init + "\n" + list + "\n" + ping + "\n";
    final ByteArrayInputStream in = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    final MCPStdioServer stdioServer = new MCPStdioServer(getServer(0), config, user, in, new PrintStream(out));
    stdioServer.run();

    final String[] lines = out.toString(StandardCharsets.UTF_8).trim().split("\n");
    assertThat(lines).hasSize(3);

    final JSONObject resp1 = new JSONObject(lines[0]);
    assertThat(resp1.getInt("id")).isEqualTo(1);
    assertThat(resp1.has("result")).isTrue();

    final JSONObject resp2 = new JSONObject(lines[1]);
    assertThat(resp2.getInt("id")).isEqualTo(2);
    assertThat(resp2.getJSONObject("result").has("tools")).isTrue();

    final JSONObject resp3 = new JSONObject(lines[2]);
    assertThat(resp3.getInt("id")).isEqualTo(3);
    assertThat(resp3.has("result")).isTrue();
  }

  // ---- Helpers ----

  private JSONObject sendSingleRequest(final JSONObject request) {
    final String inputData = request.toString() + "\n";
    final ByteArrayInputStream in = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    final MCPStdioServer stdioServer = new MCPStdioServer(getServer(0), config, user, in, new PrintStream(out));
    stdioServer.run();

    return new JSONObject(out.toString(StandardCharsets.UTF_8).trim());
  }

  private JSONObject callTool(final String toolName, final JSONObject arguments) {
    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 10)
        .put("method", "tools/call")
        .put("params", new JSONObject()
            .put("name", toolName)
            .put("arguments", arguments));

    final JSONObject response = sendSingleRequest(request);
    assertThat(response.has("result")).isTrue();
    return response.getJSONObject("result");
  }
}
