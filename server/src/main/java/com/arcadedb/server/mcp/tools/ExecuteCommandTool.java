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
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.Collections;
import java.util.Set;

public class ExecuteCommandTool {

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

    if (!user.canAccessToDatabase(databaseName))
      throw new SecurityException("User '" + user.getName() + "' is not authorized to access database '" + databaseName + "'");

    final Database database = server.getDatabase(databaseName);

    // Analyze once for both permission checking and execution (avoids double parsing)
    final QueryEngine engine = database.getQueryEngine(language);
    final QueryEngine.AnalyzedQuery analyzed = engine.analyze(command);
    checkPermission(analyzed.getOperationTypes(), config);

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final JSONArray records = new JSONArray();
    database.transaction(() -> {
      final ResultSet analyzedResultSet = analyzed.execute(Collections.emptyMap());
      try (final ResultSet resultSet = analyzedResultSet != null ? analyzedResultSet : database.command(language, command)) {
        while (resultSet.hasNext()) {
          final Result row = resultSet.next();
          records.put(serializer.serializeResult(database, row));
        }
      }
    });

    final JSONObject result = new JSONObject();
    result.put("records", records);
    result.put("count", records.length());
    return result;
  }

  /**
   * Checks MCP permissions using semantic, parser-based operation type detection.
   * Uses the query engine's analyze() method to determine the actual operation types
   * from the parsed AST, avoiding text-based pattern matching vulnerabilities.
   *
   * @param database the database to use for query analysis
   * @param command  the command text
   * @param language the query language
   * @param config   MCP configuration with permission settings
   */
  public static void checkPermission(final Database database, final String command, final String language,
      final MCPConfiguration config) {
    final Set<OperationType> operationTypes = getOperationTypes(database, command, language);
    checkPermission(operationTypes, config);
  }

  /**
   * Checks MCP permissions against a set of operation types.
   */
  public static void checkPermission(final Set<OperationType> operationTypes, final MCPConfiguration config) {
    for (final OperationType op : operationTypes) {
      switch (op) {
      case CREATE:
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
      case ADMIN:
        if (!config.isAllowAdmin())
          throw new SecurityException("Admin operations are not allowed by MCP configuration");
        break;
      case READ:
        if (!config.isAllowReads())
          throw new SecurityException("Read operations are not allowed by MCP configuration");
        break;
      }
    }
  }

  /**
   * Returns the set of operation types for a command using semantic analysis.
   * Delegates to the query engine's parser/AST for accurate classification.
   *
   * @param database the database to use for query analysis
   * @param command  the command text
   * @param language the query language
   * @return the set of operation types
   */
  public static Set<OperationType> getOperationTypes(final Database database, final String command, final String language) {
    final QueryEngine engine = database.getQueryEngine(language);
    final QueryEngine.AnalyzedQuery analyzed = engine.analyze(command);
    return analyzed.getOperationTypes();
  }
}
