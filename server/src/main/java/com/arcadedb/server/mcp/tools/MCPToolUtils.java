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
package com.arcadedb.server.mcp.tools;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.arcadedb.database.Database;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.server.security.ServerSecurityUser;

public class MCPToolUtils {

  private MCPToolUtils() {
  }

  /**
   * Resolves a database by name, throwing an {@link IllegalArgumentException} with the list of databases
   * accessible to the user when the requested database does not exist — so the LLM can self-correct
   * without a separate list_databases round-trip.
   */
  public static ServerDatabase resolveDatabase(final ArcadeDBServer server, final ServerSecurityUser user,
      final String databaseName) {
    if (!server.existsDatabase(databaseName)) {
      final Set<String> installed = new TreeSet<>(server.getDatabaseNames());
      installed.removeIf(db -> !user.canAccessToDatabase(db));
      throw new IllegalArgumentException(
          "Database '" + databaseName + "' does not exist. Available databases: " + installed
              + ". Use one of these names or call list_databases to refresh the list.");
    }
    if (!user.canAccessToDatabase(databaseName))
      throw new SecurityException("User '" + user.getName() + "' is not authorized to access database '" + databaseName + "'");
    return server.getDatabase(databaseName);
  }

  /**
   * Quotes an identifier (type name, relationship type, or property key) for safe inclusion in a Cypher
   * statement. Cypher cannot bind identifiers as parameters, so they are backtick-quoted here. An identifier
   * that is null, blank, or itself contains a backtick is rejected, which guarantees the quoting cannot be
   * escaped and no clause can be injected through an identifier. Values are never routed through this method;
   * they are always bound as parameters.
   *
   * @param kind human-readable label for the identifier, used only in the rejection message
   * @param raw  the identifier as supplied by the caller
   * @return the identifier wrapped in backticks
   */
  public static String quoteIdentifier(final String kind, final String raw) {
    if (raw == null || raw.isBlank())
      throw new IllegalArgumentException("The " + kind + " must not be null or blank");
    if (raw.indexOf('`') >= 0)
      throw new IllegalArgumentException("The " + kind + " contains a backtick, which is not supported");
    return "`" + raw + "`";
  }

  /**
   * Returns a required non-blank string argument, throwing an {@link IllegalArgumentException} that names the field
   * when it is missing or blank. Reading with a {@code null} default avoids the raw parser exception a bare lookup
   * of an absent key would throw, so the caller gets a clean, self-correcting error.
   */
  public static String requireString(final JSONObject args, final String field) {
    final String value = args.getString(field, null);
    if (value == null || value.isBlank())
      throw new IllegalArgumentException("'" + field + "' is required");
    return value;
  }

  /**
   * Returns a required object argument that must contain at least one property, throwing an
   * {@link IllegalArgumentException} that names the field otherwise.
   */
  public static JSONObject requireNonEmptyObject(final JSONObject args, final String field) {
    final JSONObject value = args.getJSONObject(field, null);
    if (value == null || value.length() == 0)
      throw new IllegalArgumentException("'" + field + "' is required and must contain at least one property");
    return value;
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
      final Map<String, Object> params, final MCPConfiguration config) {
    final QueryEngine engine = database.getQueryEngine("cypher");
    final QueryEngine.AnalyzedQuery analyzed = engine.analyze(cypher);
    ExecuteCommandTool.checkPermission(analyzed.getOperationTypes(), config);

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final JSONArray records = new JSONArray();
    database.transaction(() -> {
      try (final ResultSet resultSet = database.command("cypher", cypher, params)) {
        while (resultSet.hasNext())
          records.put(serializer.serializeResult(database, resultSet.next()));
      }
    });

    return new JSONObject().put("records", records).put("count", records.length());
  }
}
