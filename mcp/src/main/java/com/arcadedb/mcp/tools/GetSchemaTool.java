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

import com.arcadedb.database.Database;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.mcp.MCPConfiguration;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityUser;

public class GetSchemaTool {

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "get_schema")
        .put("description",
            "Get the full schema of a database including types (vertex, edge, document), their properties, indexes, and inheritance hierarchy.")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database")))
            .put("required", new JSONArray().put("database")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final String databaseName = args.getString("database");
    final Database database = server.getDatabase(databaseName);

    final Schema schema = database.getSchema();
    final JSONArray types = new JSONArray();

    for (final DocumentType type : schema.getTypes()) {
      final JSONObject typeJson = new JSONObject();
      typeJson.put("name", type.getName());

      if (type instanceof VertexType)
        typeJson.put("category", "vertex");
      else if (type instanceof EdgeType)
        typeJson.put("category", "edge");
      else
        typeJson.put("category", "document");

      // Parent types
      final JSONArray parents = new JSONArray();
      for (final DocumentType superType : type.getSuperTypes())
        parents.put(superType.getName());
      if (parents.length() > 0)
        typeJson.put("parentTypes", parents);

      // Properties
      final JSONArray properties = new JSONArray();
      for (final com.arcadedb.schema.Property prop : type.getProperties()) {
        final JSONObject propJson = new JSONObject();
        propJson.put("name", prop.getName());
        propJson.put("type", prop.getType().name());
        if (prop.isMandatory())
          propJson.put("mandatory", true);
        if (prop.isReadonly())
          propJson.put("readonly", true);
        if (prop.isNotNull())
          propJson.put("notNull", true);
        if (prop.getDefaultValue() != null)
          propJson.put("default", prop.getDefaultValue());
        if (prop.getMin() != null)
          propJson.put("min", prop.getMin());
        if (prop.getMax() != null)
          propJson.put("max", prop.getMax());
        if (prop.getOfType() != null)
          propJson.put("ofType", prop.getOfType());
        properties.put(propJson);
      }
      if (properties.length() > 0)
        typeJson.put("properties", properties);

      // Indexes
      final JSONArray indexes = new JSONArray();
      for (final TypeIndex index : type.getAllIndexes(false)) {
        final JSONObject indexJson = new JSONObject();
        indexJson.put("name", index.getName());
        indexJson.put("type", index.getType().name());
        indexJson.put("properties", new JSONArray(index.getPropertyNames()));
        indexJson.put("unique", index.isUnique());
        indexes.put(indexJson);
      }
      if (indexes.length() > 0)
        typeJson.put("indexes", indexes);

      types.put(typeJson);
    }

    final JSONObject result = new JSONObject();
    result.put("database", databaseName);
    result.put("types", types);
    return result;
  }
}
