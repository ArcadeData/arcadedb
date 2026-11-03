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
import com.arcadedb.query.RunningQuery;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.VertexType;
import com.arcadedb.security.SecurityHelper;
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
   * Builds the JSON schema document for a database. Authorizing the database itself is the caller's job; the types are
   * the ones the principal bound to the database's context may read ({@link SecurityHelper#readableTypes}), so a user
   * denied a type by its per-type permissions is not handed that type's definition, as {@code SELECT FROM schema:types}
   * does not hand it either.
   */
  public static JSONObject toJSON(final Database database, final String databaseName) {
    final JSONArray types = new JSONArray();

    // The entry a caller listed this read under (MCP get_schema, issue #10928): a terminate stops the walk between two types
    final RunningQuery runningQuery = RunningQuery.current();
    for (final DocumentType type : SecurityHelper.readableTypes(database)) {
      if (runningQuery != null)
        runningQuery.checkNotTerminated("the schema read");
      types.put(typeToJSON(database, type));
    }

    final JSONObject result = new JSONObject();
    result.put("database", databaseName);
    result.put("types", types);
    return result;
  }

  /**
   * The JSON of one type: category, parents, properties and indexes. Whether the caller may read {@code type} is the
   * caller's check; a parent type the principal bound to {@code database} may not read is left out of the parents, as
   * {@code SELECT FROM schema:types} leaves it out.
   */
  public static JSONObject typeToJSON(final Database database, final DocumentType type) {
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
      if (SecurityHelper.isReadable(database, superType))
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
      // Issue #6134: the definition, not the evaluated value - this is schema metadata, not a record being written.
      if (prop.getDefaultValueDefinition() != null)
        propJson.put("default", prop.getDefaultValueDefinition());
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

    return typeJson;
  }

  /**
   * Resolves a database on behalf of an authenticated user and renders its schema with the principal bound for
   * the duration of the read, so the engine's per-user gates enforce and nothing is left bound on the calling
   * thread afterwards.
   */
  public static JSONObject forUser(final ArcadeDBServer server, final ServerSecurityUser user,
      final String databaseName) {
    // Open or not (issue #11068): one closed for idleness, or not opened at startup, is opened below. A database the user
    // is not granted is answered exactly as one that does not exist: two different refusals would tell the caller which
    // names exist on the server, one guess at a time
    if (!user.canAccessToDatabase(databaseName) || !server.isDatabaseAvailable(databaseName)) {
      final Set<String> installed = new TreeSet<>(server.getAvailableDatabaseNames());
      installed.removeIf(db -> !user.canAccessToDatabase(db));
      throw new IllegalArgumentException(
          "Database '" + databaseName + "' does not exist. Available databases: " + installed);
    }

    // A client surface (MCP, the AI assistant): resolved like every other one, so a database an operator closed or a restore holds
    // since the existsDatabase() check above is refused rather than reopened (issue #10690)
    final ServerDatabase database = server.getDatabaseForClient(databaseName);
    return DatabaseUserContext.runAs((DatabaseInternal) database, user, () -> toJSON(database, databaseName));
  }
}
