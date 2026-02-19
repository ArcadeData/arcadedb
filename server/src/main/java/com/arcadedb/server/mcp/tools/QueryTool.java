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

public class QueryTool {

  private static final int DEFAULT_LIMIT = 1000;

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "query")
        .put("description",
            "Execute a read-only (idempotent) query against an ArcadeDB database. Use this for SELECT, MATCH, and other read operations. "
                + "Prefer OpenCypher (language: 'cypher') unless SQL is explicitly requested.")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database to query"))
                .put("language", new JSONObject()
                    .put("type", "string")
                    .put("description", "Query language: 'sql', 'cypher', 'gremlin', 'graphql', 'mongo'")
                    .put("default", "cypher"))
                .put("query", new JSONObject()
                    .put("type", "string")
                    .put("description", "The query to execute"))
                .put("limit", new JSONObject()
                    .put("type", "integer")
                    .put("description", "Maximum number of results to return (default: 1000)")))
            .put("required", new JSONArray().put("database").put("query")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final String databaseName = args.getString("database");
    final String language = args.getString("language", "cypher");
    final String query = args.getString("query");
    final int limit = args.getInt("limit", DEFAULT_LIMIT);

    if (!user.canAccessToDatabase(databaseName))
      throw new SecurityException("User '" + user.getName() + "' is not authorized to access database '" + databaseName + "'");

    final Database database = server.getDatabase(databaseName);

    // Verify the query is actually read-only using semantic analysis
    final QueryEngine engine = database.getQueryEngine(language);
    final QueryEngine.AnalyzedQuery analyzed = engine.analyze(query);
    if (!analyzed.isIdempotent())
      throw new SecurityException(
          "Query contains write operations. Use execute_command tool instead of query tool");

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    // Reuse the already-parsed statement when possible to avoid double parsing
    final JSONArray records = new JSONArray();
    final ResultSet analyzedResultSet = analyzed.execute(Collections.emptyMap());
    try (final ResultSet resultSet = analyzedResultSet != null ? analyzedResultSet : database.query(language, query)) {
      int count = 0;
      while (resultSet.hasNext() && count < limit) {
        final Result row = resultSet.next();
        records.put(serializer.serializeResult(database, row));
        count++;
      }
    }

    final JSONObject result = new JSONObject();
    result.put("records", records);
    result.put("count", records.length());
    return result;
  }
}
