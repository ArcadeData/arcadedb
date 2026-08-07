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
package com.arcadedb.server.info;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.security.DatabaseUserContext;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.Set;
import java.util.TreeSet;

/**
 * Renders a database schema as JSON. The single source of truth for schema shaping: the MCP get_schema tool,
 * the arcadedb://{database}/schema MCP resource, and the Studio AI assistant all render from here, so their
 * content cannot drift apart.
 */
public class SchemaInfo {

  private SchemaInfo() {
  }

  /**
   * Builds the JSON schema document for a database. Performs no permission or authorization check; the caller
   * is responsible for both.
   */
  public static JSONObject toJSON(final Database database, final String databaseName) {
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
      for (final Property prop : type.getProperties()) {
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

  /**
   * Resolves a database on behalf of an authenticated user and renders its schema with the principal bound for
   * the duration of the read, so the engine's per-user gates enforce and nothing is left bound on the calling
   * thread afterwards.
   */
  public static JSONObject forUser(final ArcadeDBServer server, final ServerSecurityUser user,
      final String databaseName) {
    if (!server.existsDatabase(databaseName)) {
      final Set<String> installed = new TreeSet<>(server.getDatabaseNames());
      installed.removeIf(db -> !user.canAccessToDatabase(db));
      throw new IllegalArgumentException(
          "Database '" + databaseName + "' does not exist. Available databases: " + installed);
    }
    if (!user.canAccessToDatabase(databaseName))
      throw new SecurityException("User '" + user.getName() + "' is not authorized to access database '"
          + databaseName + "'");

    final ServerDatabase database = server.getDatabase(databaseName);
    return DatabaseUserContext.runAs((DatabaseInternal) database, user, () -> toJSON(database, databaseName));
  }
}
