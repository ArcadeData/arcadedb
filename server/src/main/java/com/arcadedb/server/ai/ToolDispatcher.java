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
package com.arcadedb.server.ai;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.server.mcp.tools.GetSchemaTool;
import com.arcadedb.server.mcp.tools.ServerStatusTool;
import com.arcadedb.server.security.ServerSecurityUser;

/**
 * Executes the gateway's tool_call requests locally inside the ArcadeDB server,
 * so the AI gateway never has to make an inbound HTTP callback to the user's
 * network. Each tool is a thin wrapper over an existing in-process facility:
 * <ul>
 *   <li>{@code query_database} - read-only SQL/Cypher/Gremlin via {@code database.query()}</li>
 *   <li>{@code get_schema} - delegates to {@link GetSchemaTool}</li>
 *   <li>{@code get_server_info} - delegates to {@link ServerStatusTool}</li>
 * </ul>
 * The returned String is the JSON the LLM sees; the wire shape mirrors what the
 * gateway used to fetch back over HTTP from {@code /api/v1/query/{db}} so we can
 * keep the prompt and tool definitions unchanged.
 */
public class ToolDispatcher {
  private static final int          MAX_RESULT_BYTES = 50_000;
  private final ArcadeDBServer      server;
  private final ServerSecurityUser  user;
  private final String              defaultDatabase;

  public ToolDispatcher(final ArcadeDBServer server, final ServerSecurityUser user, final String defaultDatabase) {
    this.server = server;
    this.user = user;
    this.defaultDatabase = defaultDatabase;
  }

  /**
   * Executes the named tool and returns the JSON string to send back to the gateway.
   * Failures are returned as {@code {"error":"..."}} so the LLM can recover or wrap up
   * (matching the existing gateway-side error convention in services/llmService.js).
   */
  public String execute(final String toolName, final JSONObject args) {
    try {
      return switch (toolName) {
        case "query_database" -> executeQuery(args);
        case "get_schema" -> executeGetSchema(args);
        case "get_server_info", "server_status" -> executeServerInfo();
        default -> errorJson("Unknown tool: " + toolName);
      };
    } catch (final SecurityException e) {
      return errorJson(e.getMessage());
    } catch (final IllegalArgumentException e) {
      return errorJson(e.getMessage());
    } catch (final Exception e) {
      return errorJson(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
    }
  }

  private String executeQuery(final JSONObject args) {
    final String language = args.getString("language", "sql");
    final String command = args.getString("command", null);
    if (command == null || command.isEmpty())
      return errorJson("query_database requires a 'command' argument");

    final String databaseName = args.getString("database", defaultDatabase);
    if (databaseName == null || databaseName.isEmpty())
      return errorJson("query_database requires a 'database' argument");

    if (!user.canAccessToDatabase(databaseName))
      return errorJson("User '" + user.getName() + "' is not authorized to access database '" + databaseName + "'");

    final Database database = server.getDatabase(databaseName);

    // Read-only: use query() (not command()). Writes will surface as the engine's
    // "not idempotent" exception, which the LLM is prompted to recover by returning
    // the command as a fenced block for the user to review.
try (ResultSet rs = database.query(language, command)) {
      final JSONArray records = new JSONArray();
      long approxBytes = 0;
      boolean truncated = false;
      while (rs.hasNext()) {
        final Result row = rs.next();
        final JSONObject rowJson = new JSONObject(row.toJSON());
        final int sz = rowJson.toString().length();
        // Always allow the first row through so the LLM gets at least one record
        // even when a single row is bigger than the budget.
        if (approxBytes + sz > MAX_RESULT_BYTES && records.length() > 0) {
          truncated = true;
          break;
        }
        records.put(rowJson);
        approxBytes += sz;
      }
      final JSONObject envelope = new JSONObject();
      envelope.put("user", user.getName());
      envelope.put("result", records);
      if (truncated)
        envelope.put("truncated", true);
      return envelope.toString();
    }
  }

  private String executeGetSchema(final JSONObject args) {
    final String databaseName = args.getString("database", defaultDatabase);
    if (databaseName == null || databaseName.isEmpty())
      return errorJson("get_schema requires a 'database' argument");

    final MCPConfiguration config = effectiveMcpConfig();
    final JSONObject result = GetSchemaTool.execute(server, user, new JSONObject().put("database", databaseName), config);
    return result.toString();
  }

  private String executeServerInfo() {
    final MCPConfiguration config = effectiveMcpConfig();
    final JSONObject result = ServerStatusTool.execute(server, user, new JSONObject(), config);
    return result.toString();
  }

  // The AI assistant has its own enablement gate via ai.json; once enabled, all three
  // tools above are read-only and safe regardless of whether MCP itself is configured
  // to allow reads. We synthesize a minimal MCPConfiguration that just enables reads
  // so we don't accidentally inherit a stricter MCP policy that was meant for the
  // separate MCP HTTP/stdio endpoints.
  private MCPConfiguration effectiveMcpConfig() {
    final MCPConfiguration cfg = new MCPConfiguration(null);
    cfg.setAllowReads(true);
    return cfg;
  }

  private static String errorJson(final String message) {
    return new JSONObject().put("error", message == null ? "unknown error" : message).toString();
  }
}
