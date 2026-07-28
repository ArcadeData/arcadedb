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
import com.arcadedb.schema.EdgeType;
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
  private static final int MAX_TYPES     = 20;

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "sample_records")
        .put("description",
            "Return the first records in storage order from at most 20 selected database types so an agent can inspect "
                + "actual value shapes. This is not a random or representative sample. When types is omitted, edge types "
                + "are excluded and truncated reports whether more eligible types existed.")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database to sample"))
                .put("types", new JSONObject()
                    .put("type", "array")
                    .put("items", new JSONObject().put("type", "string"))
                    .put("maxItems", MAX_TYPES)
                    .put("description",
                        "Type names to sample, including edge types when explicitly requested; omit to sample up to "
                            + MAX_TYPES + " vertex and document types"))
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

    final MCPToolUtils.DatabaseAccess access = MCPToolUtils.resolveDatabase(
        server, user, databaseName, config, MCPToolUtils.RequiredAccess.READ);
    final Database database = access.database();
    final ResolvedTypes resolvedTypes = resolveTypeNames(database, databaseName, args);

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final JSONObject samples = new JSONObject();
    int recordsReturned = 0;
    for (final String typeName : resolvedTypes.names()) {
      final JSONArray records = sampleType(database, typeName, limit, serializer);
      samples.put(typeName, records);
      recordsReturned += records.length();
    }

    return new JSONObject()
        .put("samples", samples)
        .put("sampledTypes", samples.length())
        .put("availableTypes", resolvedTypes.availableTypeCount())
        .put("recordsReturned", recordsReturned)
        .put("truncated", resolvedTypes.truncated());
  }

  private static ResolvedTypes resolveTypeNames(final Database database, final String databaseName,
      final JSONObject args) {
    final List<String> eligibleTypes = database.getSchema().getTypes().stream()
        .filter(type -> !(type instanceof EdgeType))
        .map(DocumentType::getName)
        .sorted()
        .toList();
    final JSONArray requestedTypes = args.getJSONArray("types", null);
    if (requestedTypes == null) {
      return new ResolvedTypes(
          eligibleTypes.stream().limit(MAX_TYPES).toList(),
          eligibleTypes.size(),
          eligibleTypes.size() > MAX_TYPES);
    }
    if (requestedTypes.length() > MAX_TYPES)
      throw new IllegalArgumentException("'types' must contain at most " + MAX_TYPES + " entries");

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
    final List<String> names = new ArrayList<>(typeNames);
    return new ResolvedTypes(names, eligibleTypes.size(), false);
  }

  private static JSONArray sampleType(final Database database, final String typeName, final int limit,
      final JsonSerializer serializer) {
    final String query = "SELECT FROM " + MCPToolUtils.quoteIdentifier("type name", typeName) + " LIMIT " + limit;
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze(query);
    if (!analyzed.isIdempotent())
      throw new SecurityException("Generated sample query is not read-only");

    // Reuse the already-parsed statement when possible (SQL). For other engines analyzed.execute()
    // returns null and the query is re-parsed by database.query().
    final JSONArray records = new JSONArray();
    final ResultSet analyzedResultSet = analyzed.execute(Collections.emptyMap());
    try (final ResultSet resultSet = analyzedResultSet != null ? analyzedResultSet : database.query("sql", query)) {
      while (resultSet.hasNext())
        records.put(serializer.serializeResult(database, resultSet.next()));
    }
    return records;
  }

  private record ResolvedTypes(List<String> names, int availableTypeCount, boolean truncated) {
  }
}
