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
import com.arcadedb.server.mcp.tools.ExecuteCommandTool;
import com.arcadedb.server.mcp.tools.GetSchemaTool;
import com.arcadedb.server.mcp.tools.ListDatabasesTool;
import com.arcadedb.server.mcp.tools.QueryTool;
import com.arcadedb.server.mcp.tools.ServerStatusTool;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.logging.Level;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MCPHttpHandler extends AbstractServerHttpHandler {
  private static final String    MCP_PROTOCOL_VERSION = "2025-03-26";
  private static final JSONArray TOOLS_LIST;

  static {
    TOOLS_LIST = new JSONArray();
    TOOLS_LIST.put(ListDatabasesTool.getDefinition());
    TOOLS_LIST.put(GetSchemaTool.getDefinition());
    TOOLS_LIST.put(QueryTool.getDefinition());
    TOOLS_LIST.put(ExecuteCommandTool.getDefinition());
    TOOLS_LIST.put(ServerStatusTool.getDefinition());
  }

  private final ArcadeDBServer  server;
  private final MCPConfiguration config;

  public MCPHttpHandler(final HttpServer httpServer, final ArcadeDBServer server, final MCPConfiguration config) {
    super(httpServer);
    this.server = server;
    this.config = config;
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  public boolean isRequireAuthentication() {
    return true;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    // Auth check first to avoid leaking server state to unauthenticated requests
    if (user == null)
      return jsonRpcError(null, -32600, "Authentication required", 401);

    if (!config.isEnabled())
      return jsonRpcError(null, -32600, "MCP server is disabled", 503);

    if (payload == null)
      return jsonRpcError(null, -32700, "Parse error: empty request body");

    if (!config.isUserAllowed(user.getName()))
      return jsonRpcError(payload.opt("id"), -32600, "User not authorized for MCP access", 403);

    final String method = payload.getString("method", "");
    final JSONObject params = payload.getJSONObject("params", new JSONObject());
    final Object id = payload.opt("id");

    LogManager.instance().log(this, Level.INFO, "MCP %s (user=%s)", method, user.getName());

    return switch (method) {
      case "initialize" -> handleInitialize(id);
      case "notifications/initialized" -> handleNotification();
      case "tools/list" -> handleToolsList(id);
      case "tools/call" -> handleToolsCall(id, params, user);
      case "ping" -> jsonRpcResult(id, new JSONObject());
      default -> jsonRpcError(id, -32601, "Method not found: " + method);
    };
  }

  private ExecutionResponse handleInitialize(final Object id) {
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

  private ExecutionResponse handleNotification() {
    // Notifications don't get a response in JSON-RPC; return 204 No Content
    return new ExecutionResponse(204, "");
  }

  private ExecutionResponse handleToolsList(final Object id) {
    final JSONObject result = new JSONObject();
    result.put("tools", TOOLS_LIST);
    return jsonRpcResult(id, result);
  }

  private ExecutionResponse handleToolsCall(final Object id, final JSONObject params, final ServerSecurityUser user) {
    final String toolName = params.getString("name", "");
    final JSONObject args = params.getJSONObject("arguments", new JSONObject());

    LogManager.instance().log(this, Level.INFO, "MCP tools/call '%s' %s (user=%s)", toolName, formatArgs(args), user.getName());

    try {
      final JSONObject toolResult = switch (toolName) {
        case "list_databases" -> ListDatabasesTool.execute(server, user, args, config);
        case "get_schema" -> GetSchemaTool.execute(server, user, args, config);
        case "query" -> QueryTool.execute(server, user, args, config);
        case "execute_command" -> ExecuteCommandTool.execute(server, user, args, config);
        case "server_status" -> ServerStatusTool.execute(server, user, args, config);
        default -> throw new IllegalArgumentException("Unknown tool: " + toolName);
      };

      LogManager.instance().log(this, Level.INFO, "MCP tools/call '%s' -> %s", toolName, formatResult(toolName, toolResult));

      final JSONObject result = new JSONObject();
      final JSONArray content = new JSONArray();
      content.put(new JSONObject()
          .put("type", "text")
          .put("text", toolResult.toString()));
      result.put("content", content);
      result.put("isError", false);
      return jsonRpcResult(id, result);

    } catch (final SecurityException e) {
      LogManager.instance().log(this, Level.INFO, "MCP tools/call '%s' -> permission denied: %s", toolName, e.getMessage());
      return toolError(id, e.getMessage());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "MCP tools/call '%s' -> error: %s", toolName, e.getMessage());
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
      case "server_status" -> "ok";
      default -> "ok";
    };
  }

  private ExecutionResponse toolError(final Object id, final String message) {
    final JSONObject result = new JSONObject();
    final JSONArray content = new JSONArray();
    content.put(new JSONObject()
        .put("type", "text")
        .put("text", message));
    result.put("content", content);
    result.put("isError", true);
    return jsonRpcResult(id, result);
  }

  private ExecutionResponse jsonRpcResult(final Object id, final JSONObject result) {
    final JSONObject response = new JSONObject();
    response.put("jsonrpc", "2.0");
    response.put("id", id);
    response.put("result", result);
    return new ExecutionResponse(200, response.toString());
  }

  private ExecutionResponse jsonRpcError(final Object id, final int code, final String message) {
    return jsonRpcError(id, code, message, 200);
  }

  private ExecutionResponse jsonRpcError(final Object id, final int code, final String message, final int httpStatus) {
    final JSONObject response = new JSONObject();
    response.put("jsonrpc", "2.0");
    response.put("id", id);
    response.put("error", new JSONObject().put("code", code).put("message", message));
    return new ExecutionResponse(httpStatus, response.toString());
  }
}
