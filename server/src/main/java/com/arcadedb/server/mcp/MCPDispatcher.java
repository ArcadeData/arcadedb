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
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.tools.ExecuteCommandTool;
import com.arcadedb.server.mcp.tools.FullTextSearchTool;
import com.arcadedb.server.mcp.tools.GetSchemaTool;
import com.arcadedb.server.mcp.tools.GetServerSettingsTool;
import com.arcadedb.server.mcp.tools.ListDatabasesTool;
import com.arcadedb.server.mcp.tools.ProfilerStartTool;
import com.arcadedb.server.mcp.tools.ProfilerStatusTool;
import com.arcadedb.server.mcp.tools.ProfilerStopTool;
import com.arcadedb.server.mcp.tools.QueryTool;
import com.arcadedb.server.mcp.tools.ServerStatusTool;
import com.arcadedb.server.mcp.tools.SetServerSettingTool;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.logging.Level;

/**
 * Transport-neutral MCP JSON-RPC dispatcher. Owns the protocol surface (version, tool list, instructions, gating,
 * method routing, envelope shaping); the HTTP and stdio transports own only their I/O and their own parse errors.
 */
public class MCPDispatcher {
  public static final  String    MCP_PROTOCOL_VERSION = "2025-03-26";
  private static final JSONArray TOOLS_LIST;

  private static final String INSTRUCTIONS =
      """
      You are connected to an ArcadeDB multi-model database server. Follow these rules:
      1. ALWAYS call list_databases first when you do not know the target database name. Never guess it.
      2. Prefer Cypher (language: 'cypher') for graph queries unless SQL is explicitly requested.
      3. Use the 'query' tool for read-only operations (SELECT, MATCH, RETURN) and 'execute_command' for writes (CREATE, INSERT, UPDATE, DELETE, MERGE).
      4. Call get_schema before writing queries against an unfamiliar database to understand its types and properties. If your client supports MCP Resources, prefer reading arcadedb://{database}/schema instead: it carries the same content without spending a tool call.
      5. If a query returns no results, verify the type/property names with get_schema before concluding the data does not exist.""";

  static {
    TOOLS_LIST = new JSONArray();
    TOOLS_LIST.put(ListDatabasesTool.getDefinition());
    TOOLS_LIST.put(GetSchemaTool.getDefinition());
    TOOLS_LIST.put(QueryTool.getDefinition());
    TOOLS_LIST.put(ExecuteCommandTool.getDefinition());
    TOOLS_LIST.put(FullTextSearchTool.getDefinition());
    TOOLS_LIST.put(ServerStatusTool.getDefinition());
    TOOLS_LIST.put(ProfilerStartTool.getDefinition());
    TOOLS_LIST.put(ProfilerStopTool.getDefinition());
    TOOLS_LIST.put(ProfilerStatusTool.getDefinition());
    TOOLS_LIST.put(GetServerSettingsTool.getDefinition());
    TOOLS_LIST.put(SetServerSettingTool.getDefinition());
  }

  /**
   * A transport-neutral reply. A null json means a JSON-RPC notification, for which no reply is sent at all;
   * the httpStatus is meaningful only to the HTTP transport and is ignored by stdio.
   */
  public record MCPResponse(int httpStatus, JSONObject json) {
  }

  private final ArcadeDBServer   server;
  private final MCPConfiguration config;
  private final String           transport;

  public MCPDispatcher(final ArcadeDBServer server, final MCPConfiguration config, final String transport) {
    this.server = server;
    this.config = config;
    this.transport = transport;
  }

  /**
   * Routes one parsed JSON-RPC request. A null request means an empty body. Authentication is checked before
   * anything else, so that server state is not disclosed to unauthenticated callers.
   */
  public MCPResponse dispatch(final JSONObject request, final ServerSecurityUser user) {
    if (user == null)
      return error(null, -32600, "Authentication required", 401);

    if (!config.isEnabled())
      return error(null, -32600, "MCP server is disabled", 503);

    if (request == null)
      return error(null, -32700, "Parse error: empty request body", 200);

    final Object id = request.opt("id");

    if (!config.isUserAllowed(user.getName()))
      return error(id, -32600, "User not authorized for MCP access", 403);

    final String method = request.getString("method", "");
    final JSONObject params = request.getJSONObject("params", new JSONObject());

    LogManager.instance().log(this, Level.INFO, "MCP[%s] %s (user=%s)", transport, method, user.getName());

    return switch (method) {
      case "initialize" -> result(id, initialize());
      case "notifications/initialized" -> new MCPResponse(204, null);
      case "tools/list" -> result(id, new JSONObject().put("tools", TOOLS_LIST));
      case "tools/call" -> toolsCall(id, params, user);
      case "resources/list" -> resourcesList(id, user);
      case "resources/read" -> resourcesRead(id, params, user);
      case "ping" -> result(id, new JSONObject());
      default -> error(id, -32601, "Method not found: " + method, 200);
    };
  }

  private JSONObject initialize() {
    final JSONObject result = new JSONObject();
    result.put("protocolVersion", MCP_PROTOCOL_VERSION);

    final JSONObject serverInfo = new JSONObject();
    serverInfo.put("name", "arcadedb");
    serverInfo.put("version", Constants.getVersion());
    result.put("serverInfo", serverInfo);

    final JSONObject capabilities = new JSONObject();
    capabilities.put("tools", new JSONObject().put("listChanged", false));
    capabilities.put("resources", new JSONObject().put("listChanged", false).put("subscribe", false));
    result.put("capabilities", capabilities);

    result.put("instructions", INSTRUCTIONS);

    return result;
  }

  private MCPResponse resourcesList(final Object id, final ServerSecurityUser user) {
    try {
      return result(id, MCPResources.list(server, user, config));
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "MCP[%s] resources/list -> error: %s", transport, e.getMessage());
      return error(id, -32603, "Internal error: " + e.getMessage(), 200);
    }
  }

  private MCPResponse resourcesRead(final Object id, final JSONObject params, final ServerSecurityUser user) {
    final String uri = params.getString("uri", "");

    LogManager.instance().log(this, Level.INFO, "MCP[%s] resources/read '%s' (user=%s)", transport, uri, user.getName());

    try {
      return result(id, MCPResources.read(server, user, config, uri));
    } catch (final SecurityException e) {
      LogManager.instance().log(this, Level.INFO, "MCP[%s] resources/read -> permission denied: %s", transport, e.getMessage());
      return error(id, -32600, e.getMessage(), 200);
    } catch (final MCPResourceNotFoundException e) {
      return error(id, -32002, e.getMessage(), 200);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "MCP[%s] resources/read -> error: %s", transport, e.getMessage());
      return error(id, -32603, "Internal error: " + e.getMessage(), 200);
    }
  }

  private MCPResponse toolsCall(final Object id, final JSONObject params, final ServerSecurityUser user) {
    final String toolName = params.getString("name", "");
    final JSONObject args = params.getJSONObject("arguments", new JSONObject());

    LogManager.instance()
        .log(this, Level.INFO, "MCP[%s] tools/call '%s' %s (user=%s)", transport, toolName, formatArgs(args), user.getName());

    try {
      final JSONObject toolResult = switch (toolName) {
        case "list_databases" -> ListDatabasesTool.execute(server, user, args, config);
        case "get_schema" -> GetSchemaTool.execute(server, user, args, config);
        case "query" -> QueryTool.execute(server, user, args, config);
        case "execute_command" -> ExecuteCommandTool.execute(server, user, args, config);
        case "full_text_search" -> FullTextSearchTool.execute(server, user, args, config);
        case "server_status" -> ServerStatusTool.execute(server, user, args, config);
        case "profiler_start" -> ProfilerStartTool.execute(server, user, args, config);
        case "profiler_stop" -> ProfilerStopTool.execute(server, user, args, config);
        case "profiler_status" -> ProfilerStatusTool.execute(server, user, args, config);
        case "get_server_settings" -> GetServerSettingsTool.execute(server, user, args, config);
        case "set_server_setting" -> SetServerSettingTool.execute(server, user, args, config);
        default -> throw new IllegalArgumentException("Unknown tool: " + toolName);
      };

      LogManager.instance()
          .log(this, Level.INFO, "MCP[%s] tools/call '%s' -> %s", transport, toolName, formatResult(toolName, toolResult));

      final JSONObject result = new JSONObject();
      final JSONArray content = new JSONArray();
      content.put(new JSONObject()
          .put("type", "text")
          .put("text", toolResult.toString()));
      result.put("content", content);
      result.put("isError", false);
      return result(id, result);

    } catch (final SecurityException e) {
      LogManager.instance()
          .log(this, Level.INFO, "MCP[%s] tools/call '%s' -> permission denied: %s", transport, toolName, e.getMessage());
      return toolError(id, e.getMessage());
    } catch (final Exception e) {
      LogManager.instance()
          .log(this, Level.WARNING, "MCP[%s] tools/call '%s' -> error: %s", transport, toolName, e.getMessage());
      return toolError(id, e.getMessage());
    }
  }

  private static String formatArgs(final JSONObject args) {
    if (args.length() == 0)
      return "{}";
    final StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (final String key : args.keySet()) {
      if (!first)
        sb.append(", ");
      first = false;
      final Object value = args.get(key);
      if (value instanceof String s) {
        final String sanitized = sanitizeForLog(s);
        if (sanitized.length() > 100)
          sb.append(key).append("=\"").append(sanitized, 0, 100).append("...\"");
        else
          sb.append(key).append("=\"").append(sanitized).append("\"");
      } else
        sb.append(key).append("=").append(value);
    }
    return sb.append("}").toString();
  }

  private static String sanitizeForLog(final String value) {
    return value.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
  }

  private static String formatResult(final String toolName, final JSONObject result) {
    return switch (toolName) {
      case "list_databases" -> result.getJSONArray("databases", new JSONArray()).length() + " database(s)";
      case "get_schema" -> result.getJSONArray("types", new JSONArray()).length() + " type(s)";
      case "query", "execute_command" -> result.getInt("count", 0) + " record(s)";
      case "full_text_search" -> result.getInt("count", 0) + " hit(s)";
      case "server_status" -> "ok";
      case "profiler_start" -> result.getString("status", "ok");
      case "profiler_stop" -> result.getInt("totalQueries", 0) + " queries captured";
      case "profiler_status" -> result.getBoolean("recording", false) ? "recording" : "idle";
      case "get_server_settings" -> result.getJSONArray("settings", new JSONArray()).length() + " setting(s)";
      case "set_server_setting" -> result.getString("key", "") + " updated";
      default -> "ok";
    };
  }

  private static MCPResponse toolError(final Object id, final String message) {
    final JSONObject result = new JSONObject();
    final JSONArray content = new JSONArray();
    content.put(new JSONObject()
        .put("type", "text")
        .put("text", message));
    result.put("content", content);
    result.put("isError", true);
    return result(id, result);
  }

  private static MCPResponse result(final Object id, final JSONObject result) {
    final JSONObject response = new JSONObject();
    response.put("jsonrpc", "2.0");
    response.put("id", id);
    response.put("result", result);
    return new MCPResponse(200, response);
  }

  private static MCPResponse error(final Object id, final int code, final String message, final int httpStatus) {
    final JSONObject response = new JSONObject();
    response.put("jsonrpc", "2.0");
    response.put("id", id);
    response.put("error", new JSONObject().put("code", code).put("message", message));
    return new MCPResponse(httpStatus, response);
  }
}
