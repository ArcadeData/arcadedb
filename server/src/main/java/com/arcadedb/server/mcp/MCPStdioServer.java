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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.MCPDispatcher.MCPResponse;
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
 * Owns the stdio envelope only; all protocol routing lives in {@link MCPDispatcher}.
 */
public class MCPStdioServer {
  private final MCPDispatcher      dispatcher;
  private final ServerSecurityUser user;
  private final InputStream        input;
  private final PrintStream        output;

  public MCPStdioServer(final ArcadeDBServer server, final MCPConfiguration config, final ServerSecurityUser user,
      final InputStream input, final PrintStream output) {
    this.dispatcher = new MCPDispatcher(server, config, "stdio");
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
          final String response = dispatch(line.trim());
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

  /**
   * Dispatches one newline-delimited message, which MCP 2025-03-26 allows to be either a single JSON-RPC
   * object or a batch array. Returns the line to write back, or null when there is nothing to answer.
   */
  private String dispatch(final String line) {
    if (line.charAt(0) == '[') {
      final JSONArray batch = new JSONArray(line);
      if (batch.isEmpty())
        return jsonRpcError(null, -32600, "Invalid Request: empty batch");

      final JSONArray responses = dispatcher.dispatchBatch(batch, user);
      // A batch containing only notifications and/or responses produces no output.
      return responses.isEmpty() ? null : responses.toString();
    }

    final MCPResponse response = dispatcher.dispatch(new JSONObject(line), user);

    // A null body is a one-way notification or response, which is written back as nothing at all.
    return response.json() == null ? null : response.json().toString();
  }

  private static String jsonRpcError(final Object id, final int code, final String message) {
    final JSONObject response = new JSONObject();
    response.put("jsonrpc", "2.0");
    response.put("id", id);
    response.put("error", new JSONObject().put("code", code).put("message", message));
    return response.toString();
  }
}
