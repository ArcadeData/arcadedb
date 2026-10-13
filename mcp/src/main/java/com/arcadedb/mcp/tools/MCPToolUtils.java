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
package com.arcadedb.mcp.tools;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.mcp.MCPToolArgumentException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.mcp.MCPConfiguration;
import com.arcadedb.mcp.MCPPermissions;
import com.arcadedb.server.security.DatabaseUserContext;
import com.arcadedb.server.security.ServerSecurityUser;

public class MCPToolUtils {

  public enum RequiredAccess {
    ACCESS,
    READ
  }

  public record DatabaseAccess(ServerDatabase database, MCPPermissions permissions) {
  }

  private MCPToolUtils() {
  }

  /**
   * Resolves a database by name, throwing an {@link IllegalArgumentException} with the list of databases
   * accessible to the user when the requested database does not exist, so the LLM can self-correct
   * without a separate list_databases round-trip.
   */
  public static DatabaseAccess resolveDatabase(final ArcadeDBServer server, final ServerSecurityUser user,
      final String databaseName, final MCPConfiguration config, final RequiredAccess requiredAccess) {
    final boolean requireRead = requiredAccess == RequiredAccess.READ;
    final MCPPermissions permissions = config.getPermissionsForDatabase(databaseName);
    if (!permissions.isUserAllowed(user.getName()))
      throw new SecurityException(
          "User '" + user.getName() + "' is not authorized for MCP access to database '" + databaseName + "'");
    if (requireRead && !permissions.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    if (!server.existsDatabase(databaseName)) {
      final Set<String> installed = new TreeSet<>(server.getDatabaseNames());
      installed.removeIf(db -> requireRead
          ? !canReadDatabase(user, config, db)
          : !canAccessDatabase(user, config, db));
      throw new MCPToolArgumentException(
          "Database '" + databaseName + "' does not exist. Available databases: " + installed
              + ". Use one of these names or call list_databases to refresh the list.");
    }
    if (!user.canAccessToDatabase(databaseName))
      throw new SecurityException("User '" + user.getName() + "' is not authorized to access database '" + databaseName + "'");

    final ServerDatabase database = server.getDatabase(databaseName);
    bindCurrentUser(database, user);
    return new DatabaseAccess(database, permissions);
  }

  public static boolean canAccessDatabase(final ServerSecurityUser user, final MCPConfiguration config,
      final String databaseName) {
    return user.canAccessToDatabase(databaseName)
        && config.getPermissionsForDatabase(databaseName).isUserAllowed(user.getName());
  }

  public static boolean canReadDatabase(final ServerSecurityUser user, final MCPConfiguration config,
      final String databaseName) {
    final MCPPermissions permissions = config.getPermissionsForDatabase(databaseName);
    return user.canAccessToDatabase(databaseName)
        && permissions.isUserAllowed(user.getName())
        && permissions.isAllowReads();
  }

  /**
   * Ensures the caller is authorized to invoke a server-administration tool.
   * <p>
   * Server-scoped tools ({@code set_server_setting}, the profiler controls) act on the whole server, not on a database,
   * so no per-database permission can cover them and the principal binding done by {@link #resolveDatabase} never
   * reaches them. They previously gated only on the global {@code allowAdmin} MCP flag and ignored the
   * {@link ServerSecurityUser} they were handed, so in the documented agent-delegation deployment
   * ({@code allowAdmin=true} with a non-root or {@code "*"} entry in {@code allowedUsers}) ANY allowed user - including
   * a read-only one - reached the entire server-administration tool surface (GHSA-pff6-hp53-pj54).
   * <p>
   * The rule enforced here is the same one the HTTP transport applies to {@code POST /api/v1/server}
   * ({@code AbstractServerHttpHandler.checkRootUser}): every server command except {@code list databases} is root-only.
   * The two read-only server tools ({@code server_status}, {@code get_server_settings}) intentionally stay on
   * {@code allowReads}, matching {@code GET /api/v1/server}, which is likewise open to any authenticated user and
   * relies on secret redaction (GHSA-46hj-24h4-j8gf / GHSA-p9wc-4fhr-78wm) rather than a root gate.
   *
   * @param user      the authenticated MCP caller
   * @param operation the tool name, used only in the rejection message
   */
  public static void checkServerAdmin(final ServerSecurityUser user, final String operation) {
    if (user == null || !"root".equals(user.getName()))
      throw new SecurityException("Only the root user is authorized to execute the server administration operation '"
          + operation + "'");
  }

  /**
   * Binds the authenticated MCP principal onto the current thread's {@link com.arcadedb.database.DatabaseContext} so
   * the engine's per-user permission gates ({@code LocalDatabase.checkPermissionsOnDatabase} /
   * {@code checkPermissionsOnFile}) actually enforce for MCP callers, exactly as the HTTP, Bolt, Postgres and gRPC
   * transports do. Those gates are deliberately no-ops when no user is bound (the mechanism embedded and HA-apply
   * contexts use to skip checks), so without this binding every gate silently passes and a non-root MCP user escalates
   * to arbitrary writes, DDL, security mutation, and (via a {@code js} query) in-JVM script execution
   * (GHSA-6x73-v3rc-f57c).
   * <p>
   * The binding lives on the request thread and MUST be cleared once the tool completes; {@link com.arcadedb.mcp.MCPDispatcher}
   * does this in a finally via {@link com.arcadedb.database.DatabaseContext#removeCurrentThreadContexts()} so the
   * principal never leaks onto the pooled worker thread.
   */
  public static void bindCurrentUser(final DatabaseInternal database, final ServerSecurityUser user) {
    DatabaseUserContext.bind(database, user);
  }

  /**
   * Quotes an identifier (type name, relationship type, or property key) for safe inclusion in a Cypher or
   * SQL statement. Neither dialect can bind identifiers as parameters, so they are backtick-quoted here. An
   * identifier that is null, blank, or itself contains a backtick is rejected, which guarantees the quoting
   * cannot be escaped and no clause can be injected through an identifier. Values are never routed through
   * this method; they are always bound as parameters.
   * <p>
   * A backslash is rejected as well, even though it is not special to the Cypher lexer. The SQL lexer
   * ({@code QUOTED_IDENTIFIER}) treats a backslash as an escape character inside a backtick-quoted token, so
   * an identifier ending in {@code \} would escape the closing backtick and let the lexer run past the
   * intended end of the token, absorbing whatever SQL follows it into the identifier (issue #5849). This
   * helper is shared by both dialects and cannot tell which one a given call site targets, so it rejects the
   * character unconditionally rather than escaping it per dialect.
   *
   * @param kind human-readable label for the identifier, used only in the rejection message
   * @param raw  the identifier as supplied by the caller
   * @return the identifier wrapped in backticks
   */
  public static String quoteIdentifier(final String kind, final String raw) {
    if (raw == null || raw.isBlank())
      throw new MCPToolArgumentException("The " + kind + " must not be null or blank");
    if (raw.indexOf('`') >= 0 || raw.indexOf('\\') >= 0)
      throw new MCPToolArgumentException("The " + kind + " contains a backtick or backslash, which is not supported");
    return "`" + raw + "`";
  }

  /**
   * Returns a required non-blank string argument, throwing an {@link MCPToolArgumentException} that names the field when it
   * is missing, blank or of another JSON shape. Reading with a {@code null} default avoids the raw parser exception a bare
   * lookup of an absent key would throw, and {@link #optionalString} the one a member of the wrong shape would, so the caller
   * gets a clean, self-correcting error that every server mode keeps (issue #9772).
   */
  public static String requireString(final JSONObject args, final String field) {
    final String value = optionalString(args, field, null);
    if (value == null || value.isBlank())
      throw new MCPToolArgumentException("'" + field + "' is required");
    return value;
  }

  /**
   * Returns an optional string argument, {@code defaultValue} when it is absent or null. A member of another JSON shape (an
   * array, an object) is a refusal about the request and raises an {@link MCPToolArgumentException}, not the
   * {@link JSONException} the accessor raises, which no catch arm of the dispatcher keeps in production mode.
   */
  public static String optionalString(final JSONObject args, final String field, final String defaultValue) {
    try {
      return args.getString(field, defaultValue);
    } catch (final JSONException e) {
      throw new MCPToolArgumentException("'" + field + "' must be a string");
    }
  }

  /**
   * Returns an optional integer argument, {@code defaultValue} when it is absent or null. A member that is not a number
   * raises an {@link MCPToolArgumentException} naming it (see {@link #optionalString}).
   */
  public static int intArgument(final JSONObject args, final String field, final int defaultValue) {
    try {
      return args.getInt(field, defaultValue);
    } catch (final JSONException e) {
      throw new MCPToolArgumentException("'" + field + "' must be an integer");
    }
  }

  /**
   * Returns an optional object argument, {@code null} when it is absent or null. A member that is not a JSON object raises an
   * {@link MCPToolArgumentException} naming it (see {@link #optionalString}).
   */
  public static JSONObject optionalObject(final JSONObject args, final String field) {
    try {
      return args.getJSONObject(field, null);
    } catch (final JSONException e) {
      throw new MCPToolArgumentException("'" + field + "' must be an object");
    }
  }

  /**
   * Returns an optional array argument, {@code null} when it is absent or null. A member that is not a JSON array raises an
   * {@link MCPToolArgumentException} naming it (see {@link #optionalString}).
   */
  public static JSONArray optionalArray(final JSONObject args, final String field) {
    try {
      return args.getJSONArray(field, null);
    } catch (final JSONException e) {
      throw new MCPToolArgumentException("'" + field + "' must be an array");
    }
  }

  /**
   * Returns a required object argument that must contain at least one property, throwing an
   * {@link MCPToolArgumentException} that names the field otherwise.
   */
  public static JSONObject requireNonEmptyObject(final JSONObject args, final String field) {
    final JSONObject value = optionalObject(args, field);
    if (value == null || value.length() == 0)
      throw new MCPToolArgumentException("'" + field + "' is required and must contain at least one property");
    return value;
  }

  /**
   * Runs a validator the engine shares with the other protocol surfaces ({@code FullTextQuery}, {@code VectorSearch},
   * {@code HybridSearch}), re-wording what it refuses as an {@link MCPToolArgumentException}. Those validators are documented
   * as touching no schema and doing no I/O, so everything they can raise - an {@link IllegalArgumentException} for a bound or
   * a combination, a {@link JSONException} for a member of the wrong shape - is a statement about the request and not engine
   * text. The wrapper sits at the one place that calls them, so the shared code does not have to know about MCP. Do NOT wrap
   * a call that reads the schema or runs a query: the text that raises is the engine's, which production mode conceals (issue
   * #8749).
   */
  public static void validateArguments(final Runnable validator) {
    try {
      validator.run();
    } catch (final IllegalArgumentException | JSONException e) {
      throw new MCPToolArgumentException(e.getMessage() != null ? e.getMessage() : "Invalid arguments");
    }
  }

  /**
   * Appends a {@code MERGE (<variable>:`<typeName>` {`k`: $prefix0, ...})} node pattern to {@code cypher}, binding
   * each match-key value into {@code params} under {@code <prefix>0}, {@code <prefix>1}, ... Identifiers (type name
   * and keys) are backtick-quoted via {@link #quoteIdentifier}; values are always bound as parameters. The prefix
   * keeps this node's parameters from colliding with other nodes/clauses in the same statement.
   */
  public static void appendNodeMerge(final StringBuilder cypher, final Map<String, Object> params,
      final String variable, final String typeName, final JSONObject matchKeys, final String paramPrefix) {
    cypher.append("MERGE (").append(variable).append(':')
        .append(quoteIdentifier("type name", typeName)).append(" {");
    int i = 0;
    for (final String key : matchKeys.keySet()) {
      if (i > 0)
        cypher.append(", ");
      final String p = paramPrefix + i;
      cypher.append(quoteIdentifier("match key", key)).append(": $").append(p);
      params.put(p, matchKeys.get(key));
      i++;
    }
    cypher.append("})");
  }

  /**
   * Appends a {@code SET <variable>.`k` = $prefix0, ...} clause (including the leading {@code " SET "}) to
   * {@code cypher} for every entry in {@code properties}, binding each value into {@code params} under
   * {@code <prefix>0}, {@code <prefix>1}, ... Property keys are backtick-quoted; values are always bound. Call only
   * when {@code properties} is non-empty.
   */
  public static void appendSetClause(final StringBuilder cypher, final Map<String, Object> params,
      final String variable, final JSONObject properties, final String paramPrefix) {
    cypher.append(" SET ");
    int i = 0;
    for (final String key : properties.keySet()) {
      if (i > 0)
        cypher.append(", ");
      final String p = paramPrefix + i;
      cypher.append(variable).append('.').append(quoteIdentifier("property key", key)).append(" = $").append(p);
      params.put(p, properties.get(key));
      i++;
    }
  }

  /**
   * Executes a parameterized Cypher write and returns its records. The statement is analyzed to determine its
   * operation types, which are gated through the same permission path as {@code execute_command}; a
   * {@code MERGE ... SET} yields {@code {CREATE, UPDATE}}, so both insert and update must be allowed. Values are
   * supplied through {@code params} as bound parameters; identifiers must already be quoted by the caller. The
   * command runs inside a transaction and each result row is serialized with the same configuration the other
   * write/read tools use.
   */
  public static JSONObject executeParameterizedWrite(final Database database, final String cypher,
      final Map<String, Object> params, final MCPPermissions permissions) {
    final QueryEngine engine = database.getQueryEngine("cypher");
    final QueryEngine.AnalyzedQuery analyzed = engine.analyze(cypher);
    ExecuteCommandTool.checkPermission(analyzed.getOperationTypes(), permissions);

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final JSONArray records = collectInTransaction(database, attemptRecords -> {
      try (final ResultSet resultSet = database.command("cypher", cypher, params)) {
        while (resultSet.hasNext())
          attemptRecords.put(serializer.serializeResult(database, resultSet.next()));
      }
    });

    return new JSONObject().put("records", records).put("count", records.length());
  }

  /**
   * Runs {@code collector} inside a transaction of its own and returns ONLY the rows the attempt that COMMITTED
   * produced.
   * <p>
   * {@link Database#transaction(Database.TransactionScope)} re-runs its block up to {@code arcadedb.txRetries}
   * times when an attempt loses an MVCC race or hits a duplicated key, rolling that attempt back first - and the
   * upsert tools ask the operator for a UNIQUE index on the match keys, which is exactly what turns a concurrent
   * duplicate into the second of those. An accumulator declared OUTSIDE the block keeps what the rolled-back
   * attempt put in it and the next attempt appends to it, so the reply names the same record once per attempt,
   * {@code count} is the inflated length, and the surplus rows carry values that were rolled back and are in no
   * database - the one error an agent reading the reply has no way to detect (issue #7904). A FRESH array per
   * attempt, published only once the block has run to its end, makes the reply the committed attempt's and
   * nothing else's; the abandoned attempts' rows are dropped with the transaction that produced them, the way
   * {@code WebSocketInsertSession.inOwnTransaction} drops their tallies.
   * <p>
   * Any per-attempt bound - {@code execute_command}'s {@code limit} - must be counted on the array handed in
   * here, not on a local of the block, for the same reason: a local restarts at 0 on every attempt while the
   * rows it was meant to cap do not.
   */
  public static JSONArray collectInTransaction(final Database database, final Consumer<JSONArray> collector) {
    final JSONArray[] committed = new JSONArray[1];
    database.transaction(() -> {
      final JSONArray attemptRecords = new JSONArray();
      collector.accept(attemptRecords);
      committed[0] = attemptRecords;
    });
    return committed[0];
  }
}
