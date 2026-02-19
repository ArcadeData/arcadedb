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

import com.arcadedb.database.Database;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.Locale;
import java.util.regex.Pattern;

public class ExecuteCommandTool {

  private static final Pattern CYPHER_CREATE_INDEX      = Pattern.compile("\\bCREATE\\s+INDEX\\b");
  private static final Pattern CYPHER_CREATE_CONSTRAINT = Pattern.compile("\\bCREATE\\s+CONSTRAINT\\b");
  private static final Pattern CYPHER_DROP_INDEX        = Pattern.compile("\\bDROP\\s+INDEX\\b");
  private static final Pattern CYPHER_DROP_CONSTRAINT   = Pattern.compile("\\bDROP\\s+CONSTRAINT\\b");
  private static final Pattern CYPHER_CREATE            = Pattern.compile("\\bCREATE\\b");
  private static final Pattern CYPHER_MERGE             = Pattern.compile("\\bMERGE\\b");
  private static final Pattern CYPHER_SET               = Pattern.compile("\\bSET\\b");
  private static final Pattern CYPHER_DELETE            = Pattern.compile("\\bDELETE\\b");
  private static final Pattern CYPHER_REMOVE            = Pattern.compile("\\bREMOVE\\b");

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "execute_command")
        .put("description",
            "Execute a non-idempotent command against an ArcadeDB database. Use this for INSERT, UPDATE, DELETE, CREATE TYPE, and other write operations. "
                + "Prefer OpenCypher (language: 'cypher') unless SQL is explicitly requested.")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database"))
                .put("language", new JSONObject()
                    .put("type", "string")
                    .put("description", "Command language: 'sql', 'cypher', 'gremlin', 'graphql', 'mongo'")
                    .put("default", "cypher"))
                .put("command", new JSONObject()
                    .put("type", "string")
                    .put("description", "The command to execute")))
            .put("required", new JSONArray().put("database").put("command")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    final String databaseName = args.getString("database");
    final String language = args.getString("language", "cypher");
    final String command = args.getString("command");

    checkPermission(command, language, config);

    final Database database = server.getDatabase(databaseName);

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final JSONArray records = new JSONArray();
    try (final ResultSet resultSet = database.command(language, command)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        records.put(serializer.serializeResult(database, row));
      }
    }

    final JSONObject result = new JSONObject();
    result.put("records", records);
    result.put("count", records.length());
    return result;
  }

  public static void checkPermission(final String command, final String language, final MCPConfiguration config) {
    final OperationType opType = detectOperationType(command, language);

    switch (opType) {
    case INSERT:
      if (!config.isAllowInsert())
        throw new SecurityException("Insert operations are not allowed by MCP configuration");
      break;
    case UPDATE:
      if (!config.isAllowUpdate())
        throw new SecurityException("Update operations are not allowed by MCP configuration");
      break;
    case DELETE:
      if (!config.isAllowDelete())
        throw new SecurityException("Delete operations are not allowed by MCP configuration");
      break;
    case SCHEMA:
      if (!config.isAllowSchemaChange())
        throw new SecurityException("Schema change operations are not allowed by MCP configuration");
      break;
    case UNKNOWN:
      // If we can't classify it, require all write permissions
      if (!config.isAllowInsert() || !config.isAllowUpdate() || !config.isAllowDelete() || !config.isAllowSchemaChange())
        throw new SecurityException("Unclassified command requires all write permissions to be enabled in MCP configuration");
      break;
    }
  }

  public enum OperationType {
    INSERT, UPDATE, DELETE, SCHEMA, UNKNOWN
  }

  public static OperationType detectOperationType(final String command, final String language) {
    final String trimmed = command.trim();
    final String upper = trimmed.toUpperCase(Locale.ENGLISH);

    if ("sql".equalsIgnoreCase(language) || "sqlScript".equalsIgnoreCase(language))
      return detectSqlOperation(upper);
    else if ("cypher".equalsIgnoreCase(language))
      return detectCypherOperation(upper);

    return OperationType.UNKNOWN;
  }

  private static OperationType detectSqlOperation(final String upper) {
    // Schema operations
    if (upper.startsWith("CREATE TYPE") || upper.startsWith("CREATE VERTEX TYPE") || upper.startsWith("CREATE EDGE TYPE")
        || upper.startsWith("CREATE DOCUMENT TYPE") || upper.startsWith("ALTER TYPE") || upper.startsWith("DROP TYPE")
        || upper.startsWith("CREATE INDEX") || upper.startsWith("DROP INDEX") || upper.startsWith("CREATE PROPERTY")
        || upper.startsWith("ALTER PROPERTY") || upper.startsWith("DROP PROPERTY") || upper.startsWith("CREATE BUCKET")
        || upper.startsWith("ALTER BUCKET") || upper.startsWith("DROP BUCKET"))
      return OperationType.SCHEMA;

    if (upper.startsWith("INSERT"))
      return OperationType.INSERT;
    if (upper.startsWith("UPDATE"))
      return OperationType.UPDATE;
    if (upper.startsWith("DELETE"))
      return OperationType.DELETE;

    return OperationType.UNKNOWN;
  }

  private static OperationType detectCypherOperation(final String upper) {
    // Schema operations in Cypher
    if (CYPHER_CREATE_INDEX.matcher(upper).find() || CYPHER_CREATE_CONSTRAINT.matcher(upper).find()
        || CYPHER_DROP_INDEX.matcher(upper).find() || CYPHER_DROP_CONSTRAINT.matcher(upper).find())
      return OperationType.SCHEMA;

    // Check for write clauses using word boundaries to avoid matching keywords inside string literals
    final boolean hasCreate = CYPHER_CREATE.matcher(upper).find()
        && !CYPHER_CREATE_INDEX.matcher(upper).find() && !CYPHER_CREATE_CONSTRAINT.matcher(upper).find();
    final boolean hasMerge = CYPHER_MERGE.matcher(upper).find();
    final boolean hasSet = CYPHER_SET.matcher(upper).find();
    final boolean hasDelete = CYPHER_DELETE.matcher(upper).find();
    final boolean hasRemove = CYPHER_REMOVE.matcher(upper).find();

    if (hasDelete)
      return OperationType.DELETE;
    if (hasSet || hasMerge || hasRemove)
      return OperationType.UPDATE;
    if (hasCreate)
      return OperationType.INSERT;

    return OperationType.UNKNOWN;
  }
}
