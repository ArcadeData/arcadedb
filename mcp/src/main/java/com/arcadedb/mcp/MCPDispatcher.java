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

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.mcp.tools.ExecuteCommandTool;
import com.arcadedb.mcp.tools.FullTextSearchTool;
import com.arcadedb.mcp.tools.GetSchemaTool;
import com.arcadedb.mcp.tools.GetServerSettingsTool;
import com.arcadedb.mcp.tools.HybridSearchTool;
import com.arcadedb.mcp.tools.ListDatabasesTool;
import com.arcadedb.mcp.tools.ProfilerStartTool;
import com.arcadedb.mcp.tools.ProfilerStatusTool;
import com.arcadedb.mcp.tools.ProfilerStopTool;
import com.arcadedb.mcp.tools.QueryTool;
import com.arcadedb.mcp.tools.SampleRecordsTool;
import com.arcadedb.mcp.tools.ServerStatusTool;
import com.arcadedb.mcp.tools.SetServerSettingTool;
import com.arcadedb.mcp.tools.UpsertEntityTool;
import com.arcadedb.mcp.tools.UpsertRelationshipTool;
import com.arcadedb.mcp.tools.VectorSearchTool;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.logging.Level;

/**
 * Transport-neutral MCP JSON-RPC dispatcher. Owns the protocol surface (version, tool list, instructions, gating,
 * method routing, envelope shaping); the HTTP and stdio transports own only their I/O and their own parse errors.
 */
public class MCPDispatcher {
  public static final  String    MCP_PROTOCOL_VERSION = "2025-03-26";
  private static final JSONArray TOOLS_LIST;
  // Package-private, not private: MCPPromptsTest asserts that every tool a prompt's text names is a tool
  // this server actually registers, which is what makes a rename or a removal fail a test instead of
  // silently shipping a prompt that instructs the model to call something that no longer exists.
  static final Set<String> REGISTERED_TOOL_NAMES;

  private static final String INSTRUCTIONS =
      """
      You are connected to an ArcadeDB multi-model database server. Follow these rules:
      1. ALWAYS call list_databases first when you do not know the target database name. Never guess it.
      2. Prefer Cypher (language: 'cypher') for graph queries unless SQL is explicitly requested.
      3. Use the 'query' tool for read-only operations (SELECT, MATCH, RETURN) and 'execute_command' for writes (CREATE, INSERT, UPDATE, DELETE, MERGE).
      4. Call get_schema before writing queries against an unfamiliar database to understand its types and properties. If your client supports MCP Resources, prefer reading arcadedb://{database}/schema instead: it carries the same content without spending a tool call.
      5. If a query returns no results, verify the type/property names with get_schema before concluding the data does not exist.
      6. Guided prompt templates may be available for retrieval and knowledge-graph construction: call prompts/list to see which ones your profile exposes.""";

  private static final String RAG_INSTRUCTIONS =
      """
      You are connected to an ArcadeDB multi-model database server for retrieval and agent memory. Follow these rules:
      1. Call list_databases when you do not know the target database name.
      2. Call get_schema before searching an unfamiliar database. If your client supports MCP Resources, prefer reading arcadedb://{database}/schema instead.
      3. Use query for custom read-only SQL or Cypher retrieval.
      4. Prefer the dedicated vector, hybrid, full-text, or sampling tools shown by tools/list when they match the task.
      5. Use upsert_entity and upsert_relationship to maintain agent memory when the corresponding write permissions are enabled.
      6. ArcadeDB does not generate embeddings; supply vectors produced by your embedding model.
      7. Call prompts/list for guided templates: graphrag_query for retrieval, build_knowledge_graph for writing extracted entities and relationships.""";

  private static final String RESTRICTED_INSTRUCTIONS =
      """
      You are connected to an ArcadeDB multi-model database server with a restricted tool surface. Follow these rules:
      1. Use only tools shown by tools/list; hidden tools cannot be invoked directly by name.
      2. Call list_databases when you do not know the target database name.
      3. Call get_schema before querying an unfamiliar database. If your client supports MCP Resources, prefer reading arcadedb://{database}/schema instead.
      4. Use query for read-only SQL or Cypher retrieval.""";

  private static final Set<String> RAG_TOOL_NAMES = Set.of(
      "list_databases",
      "get_schema",
      "query",
      "sample_records",
      "vector_search",
      "hybrid_search",
      "full_text_search",
      "upsert_entity",
      "upsert_relationship");

  private static final Set<String> ADMIN_TOOL_NAMES = Set.of(
      "list_databases",
      "get_schema",
      "query",
      "execute_command",
      "server_status",
      "profiler_start",
      "profiler_stop",
      "profiler_status",
      "get_server_settings",
      "set_server_setting");

  static {
    TOOLS_LIST = new JSONArray();
    TOOLS_LIST.put(ListDatabasesTool.getDefinition());
    TOOLS_LIST.put(GetSchemaTool.getDefinition());
    TOOLS_LIST.put(QueryTool.getDefinition());
    TOOLS_LIST.put(ExecuteCommandTool.getDefinition());
    TOOLS_LIST.put(SampleRecordsTool.getDefinition());
    TOOLS_LIST.put(VectorSearchTool.getDefinition());
    TOOLS_LIST.put(FullTextSearchTool.getDefinition());
    TOOLS_LIST.put(HybridSearchTool.getDefinition());
    TOOLS_LIST.put(UpsertEntityTool.getDefinition());
    TOOLS_LIST.put(UpsertRelationshipTool.getDefinition());
    TOOLS_LIST.put(ServerStatusTool.getDefinition());
    TOOLS_LIST.put(ProfilerStartTool.getDefinition());
    TOOLS_LIST.put(ProfilerStopTool.getDefinition());
    TOOLS_LIST.put(ProfilerStatusTool.getDefinition());
    TOOLS_LIST.put(GetServerSettingsTool.getDefinition());
    TOOLS_LIST.put(SetServerSettingTool.getDefinition());

    final Set<String> registered = new HashSet<>();
    for (int i = 0; i < TOOLS_LIST.length(); i++)
      registered.add(TOOLS_LIST.getJSONObject(i).getString("name"));
    REGISTERED_TOOL_NAMES = Set.copyOf(registered);
  }

  /**
   * A transport-neutral reply. A null json means a one-way JSON-RPC notification or response, for which no
   * reply is sent at all; the httpStatus is meaningful only to the HTTP transport and is ignored by stdio.
   */
  public record MCPResponse(int httpStatus, JSONObject json) {
  }

  /**
   * The transport result for any one-way JSON-RPC message: no body at all, and - for the HTTP transport -
   * the {@code 202 Accepted} mandated for a POST that carried only notifications or responses.
   */
  private static final MCPResponse NO_RESPONSE = new MCPResponse(202, null);

  private final ArcadeDBServer   server;
  private final MCPConfiguration config;
  private final String           transport;

  public MCPDispatcher(final ArcadeDBServer server, final MCPConfiguration config, final String transport) {
    this.server = server;
    this.config = config;
    this.transport = transport;
  }

  /**
   * Routes one parsed JSON-RPC message. A null message means an empty body. One-way notifications and responses
   * are discarded without consulting server state; messages that require a reply are authenticated before any
   * server state is disclosed.
   */
  public MCPResponse dispatch(final JSONObject request, final ServerSecurityUser user) {
    // A response acknowledges a server-to-client request and must never receive another response. ArcadeDB
    // does not currently issue such requests, so valid responses are accepted and discarded.
    if (isResponse(request))
      return NO_RESPONSE;

    // A JSON-RPC notification is a request object carrying a method and NO id member, and the receiver must
    // not answer it at all (MCP 2025-03-26 base protocol). Detect it by the absent id rather than by method
    // name: keying on the name only suppressed 'notifications/initialized' and let every other notification
    // fall through to a response with a null id. Nothing is executed for a notification, so dropping it here
    // - ahead of the authentication and authorization gates, which have no way to report a failure back -
    // discloses nothing.
    if (request != null && request.has("method") && !request.has("id"))
      return NO_RESPONSE;

    // Echo the JSON-RPC id on every error response when the request carries one, per JSON-RPC 2.0.
    final Object id = request != null ? request.opt("id") : null;

    if (user == null)
      return error(id, -32600, "Authentication required", 401);

    if (!config.isEnabled())
      return error(id, -32600, "MCP server is disabled", 503);

    if (request == null)
      return error(id, -32700, "Parse error: empty request body", 200);

    if (!config.isUserAllowed(user.getName()))
      return error(id, -32600, "User not authorized for MCP access", 403);

    // Anything that reaches this point expects a response, so it must be a well-formed request: MCP requires
    // a string or integer id, explicitly excluding null. The id is echoed as null because an invalid one
    // cannot be correlated by the client.
    if (!isValidRequestId(id))
      return error(null, -32600, "Invalid Request: 'id' must be a string or an integer", 200);

    // Read both members under a guard rather than inline. The defaulting accessors substitute the default only for
    // an absent or null member, so a member that is present but of another JSON type raises out of the read
    // instead. These two reads sit above the try below, whose only block is a finally, so an unguarded raise here
    // escapes dispatch entirely and reaches the transport as a bodiless HTTP 500 rather than a JSON-RPC envelope.
    // Both members belong to the request object itself, which makes a wrong shape an invalid request rather than
    // invalid params.
    // The two members are read differently on purpose. 'method' goes through stringMember, which decides on the
    // value's own JSON type and so raises IllegalArgumentException for every non-string shape alike; the accessor
    // it replaces would have unwrapped a one-element array instead. 'params' keeps the accessor, whose
    // getAsJsonObject raises for an array of any size, so arity never decided anything there.
    final String method;
    final JSONObject params;
    try {
      method = stringMember(request, "method", "");
      params = request.getJSONObject("params", new JSONObject());
    } catch (final IllegalArgumentException | IllegalStateException | UnsupportedOperationException e) {
      return error(id, -32600, "Invalid Request: 'method' must be a string and 'params' an object", 200);
    }

    LogManager.instance().log(this, Level.INFO, "MCP[%s] %s (user=%s)", transport, method, user.getName());

    try {
      return switch (method) {
        case "initialize" -> result(id, initialize(effectiveProfile(user)));
        case "tools/list" -> result(id, new JSONObject().put("tools", toolsForProfile(effectiveProfile(user))));
        case "tools/call" -> toolsCall(id, params, user, effectiveProfile(user));
        case "resources/list" -> resourcesList(id, user);
        case "resources/read" -> resourcesRead(id, params, user);
        case "prompts/list" -> promptsList(id, effectiveProfile(user)::allows);
        case "prompts/get" -> promptsGet(id, params, user, effectiveProfile(user)::allows);
        case "ping" -> result(id, new JSONObject());
        default -> error(id, -32601, "Method not found: " + method, 200);
      };
    } finally {
      // A tool or resource read binds the authenticated principal onto this thread's DatabaseContext (so the engine
      // permission gates enforce, see MCPToolUtils.bindCurrentUser / GHSA-6x73-v3rc-f57c). This transport runs on a
      // pooled worker thread, so the binding MUST be dropped here or it would leak onto the next request served by
      // the same thread. A no-op when nothing was bound (initialize/ping/tools-list). A one-way message returns
      // before this block but binds nothing, so it has nothing to drop.
      DatabaseContext.INSTANCE.removeCurrentThreadContexts();
    }
  }

  private JSONObject initialize(final EffectiveToolProfile profile) {
    final JSONObject result = new JSONObject();
    result.put("protocolVersion", MCP_PROTOCOL_VERSION);

    final JSONObject serverInfo = new JSONObject();
    serverInfo.put("name", "arcadedb");
    serverInfo.put("version", Constants.getVersion());
    result.put("serverInfo", serverInfo);

    final JSONObject capabilities = new JSONObject();
    capabilities.put("tools", new JSONObject().put("listChanged", false));
    capabilities.put("resources", new JSONObject().put("listChanged", false).put("subscribe", false));
    capabilities.put("prompts", new JSONObject().put("listChanged", false));
    result.put("capabilities", capabilities);

    result.put("instructions", instructionsForProfile(profile));

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
    // Guarded for the same reason as the request members in dispatch: a 'uri' present but of another JSON type
    // raises out of the read, which sits above the try below. A wrong shape is invalid params, distinct from the
    // resource-not-found the try answers with for a URI that is well formed but names nothing readable.
    // stringMember raises IllegalArgumentException for every non-string shape, including the one-element array the
    // accessor it replaces would have unwrapped into a readable URI.
    final String uri;
    try {
      uri = stringMember(params, "uri", "");
    } catch (final IllegalArgumentException e) {
      return error(id, -32602, "Invalid params: 'uri' must be a string", 200);
    }

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

  private MCPResponse promptsList(final Object id, final Predicate<String> toolAllowed) {
    try {
      return result(id, MCPPrompts.list(config, toolAllowed));
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "MCP[%s] prompts/list -> error: %s", transport, e.getMessage());
      return error(id, -32603, "Internal error: " + e.getMessage(), 200);
    }
  }

  private MCPResponse promptsGet(final Object id, final JSONObject params, final ServerSecurityUser user,
      final Predicate<String> toolAllowed) {
    // Both members are read inside the try because reading them can itself fail: the defaulting accessors fall back
    // only for an absent or null member, so a member of the wrong JSON shape raises instead. That is a malformed
    // request rather than a server fault, and answering it needs the -32602 mapping below.
    try {
      final String name = stringMember(params, "name", "");
      final JSONObject args = params.getJSONObject("arguments", new JSONObject());

      LogManager.instance().log(this, Level.INFO, "MCP[%s] prompts/get '%s' (user=%s)", transport, name, user.getName());

      return result(id, MCPPrompts.get(config, toolAllowed, name, args));
    } catch (final SecurityException e) {
      LogManager.instance().log(this, Level.INFO, "MCP[%s] prompts/get -> permission denied: %s", transport, e.getMessage());
      return error(id, -32600, e.getMessage(), 200);
    } catch (final IllegalArgumentException | IllegalStateException | UnsupportedOperationException e) {
      return error(id, -32602, "Invalid params: " + e.getMessage(), 200);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "MCP[%s] prompts/get -> error: %s", transport, e.getMessage());
      return error(id, -32603, "Internal error: " + e.getMessage(), 200);
    }
  }

  private MCPResponse toolsCall(final Object id, final JSONObject params, final ServerSecurityUser user,
      final EffectiveToolProfile profile) {
    // Guarded for the same reason as the request members in dispatch: a member present but of the wrong JSON type
    // raises out of the read, and these reads sit above the try below. A malformed member here is invalid params
    // rather than a failed tool call, so it answers with a JSON-RPC error rather than an isError tool envelope.
    // 'name' goes through stringMember so that a one-element array naming a real tool is refused here instead of
    // being unwrapped and executed; 'arguments' keeps the accessor, which already refuses an array of any size.
    // Only the name is narrowed. The members inside 'arguments' are left to the accessor on purpose: formatArgs
    // reads 'arguments.key' to decide whether to mask a secret value in the request log while SetServerSettingTool
    // reads the same member to decide what to write, and two readers of one member must agree on its coercion or
    // the masking can be bypassed. 'name' has no second reader - formatArgs receives the value resolved here.
    final String toolName;
    final JSONObject args;
    try {
      toolName = stringMember(params, "name", "");
      args = params.getJSONObject("arguments", new JSONObject());
    } catch (final IllegalArgumentException | IllegalStateException | UnsupportedOperationException e) {
      return error(id, -32602, "Invalid params: 'name' must be a string and 'arguments' an object", 200);
    }

    LogManager.instance()
        .log(this, Level.INFO, "MCP[%s] tools/call '%s' %s (user=%s)", transport, toolName, formatArgs(toolName, args), user.getName());

    try {
      if (!REGISTERED_TOOL_NAMES.contains(toolName))
        throw new IllegalArgumentException("Unknown tool: " + toolName);
      if (!profile.allows(toolName))
        throw new SecurityException(
            "Tool '" + toolName + "' is not available in " + profile.description());

      final JSONObject toolResult = switch (toolName) {
        case "list_databases" -> ListDatabasesTool.execute(server, user, args, config);
        case "get_schema" -> GetSchemaTool.execute(server, user, args, config);
        case "query" -> QueryTool.execute(server, user, args, config);
        case "execute_command" -> ExecuteCommandTool.execute(server, user, args, config);
        case "sample_records" -> SampleRecordsTool.execute(server, user, args, config);
        case "vector_search" -> VectorSearchTool.execute(server, user, args, config);
        case "full_text_search" -> FullTextSearchTool.execute(server, user, args, config);
        case "hybrid_search" -> HybridSearchTool.execute(server, user, args, config);
        case "upsert_entity" -> UpsertEntityTool.execute(server, user, args, config);
        case "upsert_relationship" -> UpsertRelationshipTool.execute(server, user, args, config);
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

  private EffectiveToolProfile effectiveProfile(final ServerSecurityUser user) {
    return new EffectiveToolProfile(
        config.getToolProfile(),
        config.getPrincipalToolProfile(user.getName()));
  }

  private static JSONArray toolsForProfile(final EffectiveToolProfile profile) {
    final JSONArray filtered = new JSONArray();
    for (int i = 0; i < TOOLS_LIST.length(); i++) {
      final JSONObject definition = TOOLS_LIST.getJSONObject(i);
      if (profile.allows(definition.getString("name")))
        filtered.put(definition);
    }
    return filtered;
  }

  static boolean isToolAllowed(final MCPConfiguration.ToolProfile profile, final String toolName) {
    if (!REGISTERED_TOOL_NAMES.contains(toolName))
      return false;
    return switch (profile) {
      case ALL -> true;
      case RAG -> RAG_TOOL_NAMES.contains(toolName);
      case ADMIN -> ADMIN_TOOL_NAMES.contains(toolName);
    };
  }

  private static String instructionsForProfile(final EffectiveToolProfile profile) {
    if (profile.matches(MCPConfiguration.ToolProfile.RAG))
      return RAG_INSTRUCTIONS;
    if (profile.matches(MCPConfiguration.ToolProfile.ALL)
        || profile.matches(MCPConfiguration.ToolProfile.ADMIN))
      return INSTRUCTIONS;
    return RESTRICTED_INSTRUCTIONS;
  }

  private record EffectiveToolProfile(
      MCPConfiguration.ToolProfile global,
      MCPConfiguration.ToolProfile principal) {

    private boolean allows(final String toolName) {
      return isToolAllowed(global, toolName)
          && (principal == null || isToolAllowed(principal, toolName));
    }

    private boolean matches(final MCPConfiguration.ToolProfile profile) {
      for (final String toolName : REGISTERED_TOOL_NAMES)
        if (allows(toolName) != isToolAllowed(profile, toolName))
          return false;
      return true;
    }

    private String description() {
      if (principal == null)
        return "MCP profile '" + global.configName() + "'";
      return "global MCP profile '" + global.configName()
          + "' intersected with principal profile '" + principal.configName() + "'";
    }
  }

  /**
   * Renders a tool's arguments for the request log. The value a caller writes to a secret setting is masked here,
   * because this line is emitted before the tool runs and would otherwise place the secret in the log regardless of
   * how the tool's own response is redacted. Masking is deliberately limited to the one argument that is a secret by
   * definition: arguments elsewhere carry caller data, which cannot be told apart from a secret without guessing, and
   * blanking it would leave the log unable to explain what a call did.
   */
  static String formatArgs(final String toolName, final JSONObject args) {
    if (args.length() == 0)
      return "{}";
    final boolean maskValue = "set_server_setting".equals(toolName) && isHiddenSetting(settingKey(args));
    final StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (final String key : args.keySet()) {
      if (!first)
        sb.append(", ");
      first = false;
      if (maskValue && "value".equals(key)) {
        sb.append(key).append("=\"*****\"");
        continue;
      }
      final Object value = args.get(key);
      if (value instanceof String s) {
        final String sanitized = sanitizeForLog(s);
        if (sanitized.length() > 100)
          sb.append(key).append("=\"").append(sanitized, 0, 100).append("...\"");
        else
          sb.append(key).append("=\"").append(sanitized).append("\"");
      } else if (value instanceof JSONArray array)
        sb.append(key).append("=[").append(array.length()).append(" item(s)]");
      else
        sb.append(key).append("=").append(value);
    }
    return sb.append("}").toString();
  }

  /**
   * Reads the setting key for the mask decision without letting a malformed member raise. This runs while the log
   * line is built, which is above the handler's try, so a raise here would escape the dispatcher and reach the
   * transport as a bodiless HTTP 500. The read deliberately keeps the coercion the tool itself applies, so a key the
   * tool will resolve is still recognised as secret; narrowing it to a plain string would leave the value unmasked
   * for a key shape the tool nevertheless writes.
   */
  private static String settingKey(final JSONObject args) {
    try {
      return args.getString("key", null);
    } catch (final IllegalStateException | UnsupportedOperationException e) {
      return null;
    }
  }

  /**
   * Reports whether a configuration key names a setting the server treats as secret, using the same
   * {@link GlobalConfiguration#isHidden()} rule the settings tools apply. An unresolvable key is not secret: the
   * tool rejects it before it can change anything.
   */
  private static boolean isHiddenSetting(final String key) {
    if (key == null || key.isEmpty())
      return false;
    final GlobalConfiguration cfg = GlobalConfiguration.findByKey(key);
    return cfg != null && cfg.isHidden();
  }

  private static String sanitizeForLog(final String value) {
    return value.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
  }

  private static String formatResult(final String toolName, final JSONObject result) {
    return switch (toolName) {
      case "list_databases" -> result.getJSONArray("databases", new JSONArray()).length() + " database(s)";
      case "get_schema" -> result.getJSONArray("types", new JSONArray()).length() + " type(s)";
      case "query", "execute_command" -> result.getInt("count", 0) + " record(s)";
      case "sample_records" -> result.getInt("recordsReturned", 0) + " record(s) across "
          + result.getInt("sampledTypes", 0) + " type(s)";
      case "vector_search" -> result.getInt("count", 0) + " neighbor(s)";
      case "full_text_search" -> result.getInt("count", 0) + " hit(s)";
      case "hybrid_search" -> result.getInt("count", 0) + " fused hit(s)";
      case "upsert_entity", "upsert_relationship" -> result.getInt("count", 0) + " record(s)";
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

  /**
   * Reads a member that the protocol types as a string, deciding on the value's own JSON type rather than on what
   * a converting accessor makes of it. {@link JSONObject#getString(String, String)} delegates to Gson, which
   * unwraps a one-element array to that element and stringifies a bare number or boolean, raising only for any
   * other shape. Array arity would then decide whether a request is executed or refused. {@link JSONObject#opt}
   * maps each JSON type to its Java counterpart without converting between them, so comparing the result against
   * String admits the JSON strings and nothing else.
   *
   * @return the member's value, or defaultValue when the member is absent or JSON null.
   *
   * @throws IllegalArgumentException when the member is present with any other JSON type. Callers translate this
   *                                  into the JSON-RPC error that fits where the member sits.
   */
  private static String stringMember(final JSONObject json, final String name, final String defaultValue) {
    final Object value = json.opt(name);
    if (value == null)
      return defaultValue;
    if (value instanceof String string)
      return string;
    throw new IllegalArgumentException("'" + name + "' must be a string");
  }

  /**
   * A JSON-RPC id that a response can be correlated by. MCP 2025-03-26 narrows JSON-RPC 2.0 to a string or an
   * integer and forbids null, so fractional numbers, booleans and structured values are rejected too. Note
   * that {@link JSONObject#opt} maps a JSON null to a Java null, which this correctly refuses.
   */
  private static boolean isValidRequestId(final Object id) {
    return id instanceof String || id instanceof Integer || id instanceof Long || id instanceof Short
        || id instanceof Byte || id instanceof java.math.BigInteger;
  }

  private static boolean isResponse(final JSONObject message) {
    if (message == null || message.has("method") || !message.has("id") || !isValidRequestId(message.opt("id")))
      return false;

    // Compared through opt rather than read as a string. This probe runs before anything else in dispatch, so it
    // is the one member read that cannot sit under a guard: it decides whether a reply is owed at all, and a raise
    // here would escape as a transport failure with no envelope. opt maps any JSON shape to an object instead of
    // demanding one, so a 'jsonrpc' that is not the string "2.0" simply means this payload is not a response.
    return "2.0".equals(message.opt("jsonrpc"))
        && message.has("result") != message.has("error");
  }

  private static MCPResponse result(final Object id, final JSONObject result) {
    final JSONObject response = new JSONObject();
    response.put("jsonrpc", "2.0");
    response.put("id", id);
    response.put("result", result);
    return new MCPResponse(200, response);
  }

  private static MCPResponse error(final Object id, final int code, final String message, final int httpStatus) {
    return new MCPResponse(httpStatus, errorObject(id, code, message));
  }

  /**
   * Builds a bare JSON-RPC error object. Shared with the transports, which have to emit errors of their own
   * (an unparseable payload, or a batch element that is not an object) outside of any dispatch.
   */
  public static JSONObject errorObject(final Object id, final int code, final String message) {
    final JSONObject response = new JSONObject();
    response.put("jsonrpc", "2.0");
    response.put("id", id);
    response.put("error", new JSONObject().put("code", code).put("message", message));
    return response;
  }

  /**
   * Routes a JSON-RPC batch, which MCP 2025-03-26 requires every receiver to support. Each element is
   * dispatched independently and only the elements that are requests contribute a response, so a batch made
   * only of notifications and/or responses yields an empty array and the transport answers with no body.
   */
  public JSONArray dispatchBatch(final JSONArray batch, final ServerSecurityUser user) {
    final JSONArray responses = new JSONArray();
    for (int i = 0; i < batch.length(); i++) {
      final Object element = batch.get(i);
      if (element instanceof JSONObject request) {
        final MCPResponse response = dispatch(request, user);
        if (response.json() != null)
          responses.put(response.json());
      } else
        // A non-object element cannot carry an id, so the error is reported with a null one, per JSON-RPC 2.0.
        responses.put(errorObject(null, -32600, "Invalid Request: batch element is not a JSON object"));
    }
    return responses;
  }
}
