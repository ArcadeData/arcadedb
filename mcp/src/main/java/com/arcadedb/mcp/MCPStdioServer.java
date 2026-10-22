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
import com.arcadedb.log.LogManager;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.mcp.MCPDispatcher.MCPResponse;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityException;
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
  // What "list queries" names as the client of a tool call: the process at the other end of the pipe has no address (issue #9962)
  static final RunningQuery.Client STDIO_CLIENT = new RunningQuery.Client(null, "stdio");

  static final String MISSING_ROOT_PASSWORD_MESSAGE =
      GlobalConfiguration.SERVER_ROOT_PASSWORD.getKey() + " or " + GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH.getKey()
          + " must be set for MCP stdio mode";

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

    final ContextConfiguration configuration = new ContextConfiguration();
    final String rootPassword;
    try {
      rootPassword = resolveRootPassword(configuration);
    } catch (final ServerSecurityException e) {
      System.err.println("ERROR: " + e.getMessage());
      System.exit(1);
      return;
    }
    if (rootPassword == null) {
      System.err.println("ERROR: " + MISSING_ROOT_PASSWORD_MESSAGE);
      System.exit(1);
      return;
    }

    final int status = startup(new ArcadeDBServer(configuration), rootPassword, System.in, mcpOut);
    if (status != 0)
      System.exit(status);
  }

  /**
   * The root password this process authenticates with: {@code arcadedb.server.rootPassword}, or - when that is not set - the content of
   * the file {@code arcadedb.server.rootPasswordPath} names, the way {@code ServerSecurity} reads it to create the root user (issue
   * #10478). A server configured the recommended way keeps the password in a file (a mounted Secret) so that it never appears on a
   * command line, and that server must be able to run in this mode too.
   *
   * @return the password, or {@code null} when neither setting holds one
   *
   * @throws ServerSecurityException if the file is named but cannot be read, or is empty
   */
  static String resolveRootPassword(final ContextConfiguration configuration) {
    final String rootPassword = configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PASSWORD);
    if (rootPassword != null && !rootPassword.isEmpty())
      return rootPassword;

    // An empty path is "not set" here, where the server takes any non-null path as authoritative and fails on an empty one: a launcher
    // that always passes -Darcadedb.server.rootPasswordPath= (a container entrypoint, a client config) gets the same message as one
    // that sets neither setting, which names both, instead of an error about a file called ''
    final String rootPasswordPath = configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH);
    return rootPasswordPath != null && !rootPasswordPath.isEmpty() ? ServerSecurity.readRootPasswordFile(rootPasswordPath) : null;
  }

  /**
   * Starts {@code server}, runs the stdio loop on it and returns the status the JVM must exit with: {@code 0}
   * only when the loop ended the way it is meant to (stdin closed), {@code 1} for every fatal startup condition.
   * <p>
   * This process is always launched by a supervisor - mcp-proxy, Claude Desktop, Cursor, a systemd unit, a
   * container runtime - and all of them read the exit status. The catch-all arm used to print the stack trace and
   * let {@code main} return, so a wrong root password, a port already bound or an unreadable database directory
   * exited <b>0</b>: the supervisor read "this finished its work", and there was no restart, no crash-loop
   * backoff and no alert for a server that is simply not there (issue #7798).
   * <p>
   * The two early failures that already exited non-zero did it with {@code System.exit(1)} from INSIDE the
   * {@code try}, which skips the {@code finally} - so the "MCP plugin is not installed" path left the server it
   * had just started running while the JVM tore down. Returning a status instead means every path, failure
   * included, goes through {@link ArcadeDBServer#stop()} exactly once before the status is acted on.
   * <p>
   * Package-private, and taking the server rather than building it, so the status contract can be tested without
   * booting a real one.
   */
  static int startup(final ArcadeDBServer server, final String rootPassword, final InputStream in,
      final PrintStream out) {
    try {
      server.start();

      final MCPPlugin plugin = MCPPlugin.of(server);
      if (plugin == null) {
        System.err.println("ERROR: the MCP plugin is not installed on this server");
        return 1;
      }
      final MCPConfiguration config = plugin.getConfiguration();
      config.setEnabled(true);

      final ServerSecurityUser user = server.getSecurity().authenticate("root", rootPassword, null);

      return new MCPStdioServer(server, config, user, in, out).run() ? 0 : 1;

    } catch (final Exception e) {
      System.err.println("ERROR: " + e.getMessage());
      e.printStackTrace(System.err);
      return 1;
    } finally {
      server.stop();
    }
  }

  /**
   * @return {@code true} when the loop ended on EOF (stdin closed), {@code false} when it ended on an I/O failure, so
   * the caller can report a transport that died mid-session differently from a clean shutdown (issue #8039).
   */
  public boolean run() {
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
      return false;
    }
    return true;
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

      final JSONArray responses = dispatcher.dispatchBatch(batch, user, STDIO_CLIENT);
      // A batch containing only notifications and/or responses produces no output.
      return responses.isEmpty() ? null : responses.toString();
    }

    final MCPResponse response = dispatcher.dispatch(new JSONObject(line), user, STDIO_CLIENT);

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
