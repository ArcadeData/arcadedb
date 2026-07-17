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
package com.arcadedb.server.mcp;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.mcp.tools.GetSchemaTool;
import com.arcadedb.server.mcp.tools.MCPToolUtils;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.TreeSet;

/**
 * MCP Resources surface. Exposes each database's schema at arcadedb://{database}/schema.
 */
public class MCPResources {
  public static final String URI_SCHEME    = "arcadedb://";
  public static final String SCHEMA_SUFFIX = "/schema";
  public static final String MIME_TYPE     = "application/json";

  private MCPResources() {
  }

  /**
   * Enumerates the schema resources the user may read. A discovery call issued unprompted by most clients at session
   * start, so a denial is expressed as an empty list rather than an error, and databases the user cannot access are
   * omitted rather than reported.
   */
  public static JSONObject list(final ArcadeDBServer server, final ServerSecurityUser user, final MCPConfiguration config) {
    final JSONArray resources = new JSONArray();

    if (config.isAllowReads())
      for (final String databaseName : new TreeSet<>(server.getDatabaseNames())) {
        if (!user.canAccessToDatabase(databaseName))
          continue;

        resources.put(new JSONObject()
            .put("uri", schemaURI(databaseName))
            .put("name", databaseName + " schema")
            .put("description", "Schema of the ArcadeDB database '" + databaseName
                + "': types (vertex, edge, document), properties, indexes, inheritance.")
            .put("mimeType", MIME_TYPE));
      }

    return new JSONObject().put("resources", resources);
  }

  /**
   * Reads a schema resource. An unknown database, an unauthorized database, and a malformed URI are indistinguishable
   * to the caller, so that this method cannot be used to probe which databases exist.
   */
  public static JSONObject read(final ArcadeDBServer server, final ServerSecurityUser user, final MCPConfiguration config,
      final String uri) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final String databaseName = parseSchemaURI(uri);
    if (databaseName == null || !server.existsDatabase(databaseName) || !user.canAccessToDatabase(databaseName))
      throw new MCPResourceNotFoundException("Resource not found: " + uri);

    final ServerDatabase database = server.getDatabase(databaseName);
    // Bind the principal so engine permission gates enforce for MCP callers (GHSA-6x73-v3rc-f57c); cleared by
    // MCPDispatcher in a finally.
    MCPToolUtils.bindCurrentUser(database, user);
    final JSONObject schema = GetSchemaTool.buildSchema(database, databaseName);

    final JSONArray contents = new JSONArray();
    contents.put(new JSONObject()
        .put("uri", uri)
        .put("mimeType", MIME_TYPE)
        .put("text", schema.toString()));

    return new JSONObject().put("contents", contents);
  }

  public static String schemaURI(final String databaseName) {
    return URI_SCHEME + databaseName + SCHEMA_SUFFIX;
  }

  /**
   * Returns the database name in arcadedb://{database}/schema, or null when the URI does not match that shape.
   * Parsed with string operations rather than java.net.URI on purpose: URI applies hostname rules to the authority,
   * lowercasing it and rejecting the underscores that an ArcadeDB database name may legally contain, so
   * arcadedb://My_DB/schema would otherwise resolve to nothing. A database name cannot contain '/', because it is a
   * directory name on disk, so this parse round-trips exactly against what list() emits.
   */
  public static String parseSchemaURI(final String uri) {
    if (uri == null || !uri.startsWith(URI_SCHEME) || !uri.endsWith(SCHEMA_SUFFIX))
      return null;
    if (uri.length() <= URI_SCHEME.length() + SCHEMA_SUFFIX.length())
      return null;

    final String databaseName = uri.substring(URI_SCHEME.length(), uri.length() - SCHEMA_SUFFIX.length());
    if (databaseName.indexOf('/') >= 0)
      return null;

    return databaseName;
  }
}
