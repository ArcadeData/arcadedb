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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * @author Justin Blethrow
 */
public class SampleRecordsTool {
  private static final int DEFAULT_LIMIT = 3;
  private static final int MAX_LIMIT     = 20;

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "sample_records")
        .put("description",
            "Return a small sample of records from selected database types so an agent can inspect actual value shapes.")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database to sample"))
                .put("types", new JSONObject()
                    .put("type", "array")
                    .put("items", new JSONObject().put("type", "string"))
                    .put("description", "Type names to sample; omit to sample every type"))
                .put("limit", new JSONObject()
                    .put("type", "integer")
                    .put("minimum", 1)
                    .put("maximum", MAX_LIMIT)
                    .put("default", DEFAULT_LIMIT)
                    .put("description", "Maximum records returned per type")))
            .put("required", new JSONArray().put("database")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final String databaseName = MCPToolUtils.requireString(args, "database");
    final int limit = args.getInt("limit", DEFAULT_LIMIT);
    if (limit < 1 || limit > MAX_LIMIT)
      throw new IllegalArgumentException("'limit' must be between 1 and " + MAX_LIMIT);

    final Database database = MCPToolUtils.resolveDatabase(server, user, databaseName);
    final List<String> typeNames = resolveTypeNames(database, databaseName, args);

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final JSONObject samples = new JSONObject();
    for (final String typeName : typeNames)
      samples.put(typeName, sampleType(database, typeName, limit, serializer));

    return new JSONObject().put("samples", samples);
  }

  private static List<String> resolveTypeNames(final Database database, final String databaseName,
      final JSONObject args) {
    final JSONArray requestedTypes = args.getJSONArray("types", null);
    if (requestedTypes == null)
      return database.getSchema().getTypes().stream()
          .map(DocumentType::getName)
          .sorted()
          .toList();

    final LinkedHashSet<String> typeNames = new LinkedHashSet<>();
    for (int i = 0; i < requestedTypes.length(); i++) {
      final String typeName = requestedTypes.getString(i);
      if (typeName == null || typeName.isBlank())
        throw new IllegalArgumentException("'types' must contain only non-blank type names");
      if (!database.getSchema().existsType(typeName))
        throw new IllegalArgumentException(
            "Type '" + typeName + "' does not exist in database '" + databaseName + "'");
      typeNames.add(typeName);
    }
    return new ArrayList<>(typeNames);
  }

  private static JSONArray sampleType(final Database database, final String typeName, final int limit,
      final JsonSerializer serializer) {
    final String query = "SELECT FROM " + MCPToolUtils.quoteIdentifier("type name", typeName) + " LIMIT " + limit;
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze(query);
    if (!analyzed.isIdempotent())
      throw new SecurityException("Generated sample query is not read-only");

    final JSONArray records = new JSONArray();
    final ResultSet analyzedResultSet = analyzed.execute(Collections.emptyMap());
    try (final ResultSet resultSet = analyzedResultSet != null ? analyzedResultSet : database.query("sql", query)) {
      while (resultSet.hasNext())
        records.put(serializer.serializeResult(database, resultSet.next()));
    }
    return records;
  }
}
