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

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.tools.ExecuteCommandTool;
import com.arcadedb.server.mcp.tools.GetSchemaTool;
import com.arcadedb.server.mcp.tools.GetServerSettingsTool;
import com.arcadedb.server.mcp.tools.ListDatabasesTool;
import com.arcadedb.server.mcp.tools.ProfilerStartTool;
import com.arcadedb.server.mcp.tools.ProfilerStatusTool;
import com.arcadedb.server.mcp.tools.ProfilerStopTool;
import com.arcadedb.server.mcp.tools.QueryTool;
import com.arcadedb.server.mcp.tools.ServerStatusTool;
import com.arcadedb.server.mcp.tools.SetServerSettingTool;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurityUser;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;

/**
 * MCP server using stdio transport (JSON-RPC 2.0 over stdin/stdout, newline-delimited).
 * This allows ArcadeDB to work natively with mcp-proxy, Claude Desktop, Cursor, etc.
 */
public class MCPStdioServer {
  private static final String    MCP_PROTOCOL_VERSION = "2025-03-26";
  private static final JSONArray TOOLS_LIST;

  static {
    TOOLS_LIST = new JSONArray();
    TOOLS_LIST.put(ListDatabasesTool.getDefinition());
    TOOLS_LIST.put(GetSchemaTool.getDefinition());
    TOOLS_LIST.put(QueryTool.getDefinition());
    TOOLS_LIST.put(ExecuteCommandTool.getDefinition());
    TOOLS_LIST.put(ServerStatusTool.getDefinition());
    TOOLS_LIST.put(ProfilerStartTool.getDefinition());
    TOOLS_LIST.put(ProfilerStopTool.getDefinition());
    TOOLS_LIST.put(ProfilerStatusTool.getDefinition());
    TOOLS_LIST.put(GetServerSettingsTool.getDefinition());
    TOOLS_LIST.put(SetServerSettingTool.getDefinition());
  }

  private final ArcadeDBServer   server;
  private final MCPConfiguration config;
  private final ServerSecurityUser user;
  private final InputStream      input;
  private final PrintStream      output;

  public MCPStdioServer(final ArcadeDBServer server, final MCPConfiguration config, final ServerSecurityUser user,
      final InputStream input, final PrintStream output) {
    this.server = server;
    this.config = config;
    this.user = user;
    this.input = input;
    this.output = output;
  }

  public static void main(final String[] args) {
    // Save the real stdout for MCP JSON-RPC before any server code can write to it
    final PrintStream mcpOut = System.out;
    System.setOut(System.err);

    final String rootPassword = GlobalConfiguration.SERVER_ROOT_PASSWORD.getValueAsString();
    if (rootPassword == null || rootPassword.isEmpty()) {
      System.err.println("ERROR: arcadedb.server.rootPassword must be set for MCP stdio mode");
      System.exit(1);
    }

    final ArcadeDBServer server = new ArcadeDBServer(new ContextConfiguration());
    try {
      server.start();

      final MCPConfiguration config = server.getMCPConfiguration();
      config.setEnabled(true);

      final ServerSecurityUser user = server.getSecurity().authenticate("root", rootPassword, null);

      final MCPStdioServer stdioServer = new MCPStdioServer(server, config, user, System.in, mcpOut);
      stdioServer.run();
    } catch (final Exception e) {
      System.err.println("ERROR: " + e.getMessage());
      e.printStackTrace(System.err);
    } finally {
      server.stop();
    }
  }

  public void run() {
    final BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank())
          continue;

        try {
          final JSONObject request = new JSONObject(line);
          final String response = dispatch(request);
          if (response != null) {
            output.println(response);
            output.flush();
          }
        } catch (final Exception e) {
          // JSON parse error
          final String errorResponse = jsonRpcError(null, -32700, "Parse error: " + e.getMessage());
          output.println(errorResponse);
          output.flush();
        }
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "MCP stdio error: %s", e.getMessage());
    }
  }

  private String dispatch(final JSONObject request) {
    final String method = request.getString("method", "");
    final JSONObject params = request.getJSONObject("params", new JSONObject());
    final Object id = request.opt("id");

    LogManager.instance().log(this, Level.INFO, "MCP stdio %s (user=%s)", method, user.getName());

    return switch (method) {
      case "initialize" -> handleInitialize(id);
      case "notifications/initialized" -> null; // notifications get no response
      case "tools/list" -> handleToolsList(id);
      case "tools/call" -> handleToolsCall(id, params);
      case "ping" -> jsonRpcResult(id, new JSONObject());
      default -> jsonRpcError(id, -32601, "Method not found: " + method);
    };
  }

  private String handleInitialize(final Object id) {
    final JSONObject result = new JSONObject();
    result.put("protocolVersion", MCP_PROTOCOL_VERSION);

    final JSONObject serverInfo = new JSONObject();
    serverInfo.put("name", "arcadedb");
    serverInfo.put("version", Constants.getVersion());
    result.put("serverInfo", serverInfo);

    final JSONObject capabilities = new JSONObject();
    capabilities.put("tools", new JSONObject().put("listChanged", false));
    result.put("capabilities", capabilities);

    return jsonRpcResult(id, result);
  }

  private String handleToolsList(final Object id) {
    final JSONObject result = new JSONObject();
    result.put("tools", TOOLS_LIST);
    return jsonRpcResult(id, result);
  }

  private String handleToolsCall(final Object id, final JSONObject params) {
    final String toolName = params.getString("name", "");
    final JSONObject args = params.getJSONObject("arguments", new JSONObject());

    LogManager.instance().log(this, Level.INFO, "MCP stdio tools/call '%s' (user=%s)", toolName, user.getName());

    try {
      final JSONObject toolResult = switch (toolName) {
        case "list_databases" -> ListDatabasesTool.execute(server, user, args, config);
        case "get_schema" -> GetSchemaTool.execute(server, user, args, config);
        case "query" -> QueryTool.execute(server, user, args, config);
        case "execute_command" -> ExecuteCommandTool.execute(server, user, args, config);
        case "server_status" -> ServerStatusTool.execute(server, user, args, config);
        case "profiler_start" -> ProfilerStartTool.execute(server, user, args, config);
        case "profiler_stop" -> ProfilerStopTool.execute(server, user, args, config);
        case "profiler_status" -> ProfilerStatusTool.execute(server, user, args, config);
        case "get_server_settings" -> GetServerSettingsTool.execute(server, user, args, config);
        case "set_server_setting" -> SetServerSettingTool.execute(server, user, args, config);
        default -> throw new IllegalArgumentException("Unknown tool: " + toolName);
      };

      final JSONObject result = new JSONObject();
      final JSONArray content = new JSONArray();
      content.put(new JSONObject()
          .put("type", "text")
          .put("text", toolResult.toString()));
      result.put("content", content);
      result.put("isError", false);
      return jsonRpcResult(id, result);

    } catch (final SecurityException e) {
      LogManager.instance().log(this, Level.INFO, "MCP stdio tools/call '%s' -> permission denied: %s", toolName, e.getMessage());
      return toolError(id, e.getMessage());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "MCP stdio tools/call '%s' -> error: %s", toolName, e.getMessage());
      return toolError(id, e.getMessage());
    }
  }

  private static String toolError(final Object id, final String message) {
    final JSONObject result = new JSONObject();
    final JSONArray content = new JSONArray();
    content.put(new JSONObject()
        .put("type", "text")
        .put("text", message));
    result.put("content", content);
    result.put("isError", true);
    return jsonRpcResult(id, result);
  }

  private static String jsonRpcResult(final Object id, final JSONObject result) {
    final JSONObject response = new JSONObject();
    response.put("jsonrpc", "2.0");
    response.put("id", id);
    response.put("result", result);
    return response.toString();
  }

  private static String jsonRpcError(final Object id, final int code, final String message) {
    final JSONObject response = new JSONObject();
    response.put("jsonrpc", "2.0");
    response.put("id", id);
    response.put("error", new JSONObject().put("code", code).put("message", message));
    return response.toString();
  }
}
