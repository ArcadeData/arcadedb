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
import com.arcadedb.mcp.MCPConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.server.vector.FullTextQuery;

/**
 * MCP {@code full_text_search} tool: the MCP-shaped wrapper - tool schema, database resolution - around
 * {@link FullTextQuery}, which the HTTP and gRPC full-text surfaces call too.
 */
public class FullTextSearchTool {

  private static final int DEFAULT_LIMIT = FullTextQuery.DEFAULT_LIMIT;
  /**
   * Upper bound on the requested window (issue #6837), re-exported from {@link FullTextQuery#MAX_LIMIT} so the
   * tool schema advertises exactly the bound the search enforces.
   */
  public static final int MAX_LIMIT = FullTextQuery.MAX_LIMIT;

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "full_text_search")
        .put("description",
            """
            Search a full-text index and return the matching records ranked by relevance score. \
            Address the index either by 'indexName' (e.g. 'Article[content]') or by 'typeName' plus optional 'properties'. \
            If both are given, 'indexName' wins. Query syntax: '+a +b' requires both terms, 'a -b' excludes b, 'a b' matches \
            either, '"exact phrase"' requires all terms in the same record (term order is NOT enforced), 'pre*' matches a \
            prefix, 'term~' is a fuzzy match, 'field:term' restricts to one property of a multi-property index, and 'term^2' \
            boosts a term. The returned 'similarity' says whether scores are BM25 or legacy CLASSIC coordination counts.""")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database to search"))
                .put("indexName", new JSONObject()
                    .put("type", "string")
                    .put("description", "Name of the full-text index, e.g. 'Article[content]' or 'Article[title,body]'"))
                .put("typeName", new JSONObject()
                    .put("type", "string")
                    .put("description",
                        "Type carrying the full-text index, e.g. 'Article'. An index declared on a supertype is named for the supertype"))
                .put("properties", new JSONObject()
                    .put("type", "array")
                    .put("items", new JSONObject().put("type", "string"))
                    .put("description",
                        "Indexed properties, used with 'typeName' to identify the index, e.g. ['content']. Must be given "
                            + "in the same order the index declares them, e.g. ['title','content'] for an index declared as "
                            + "Article[title,content]"))
                .put("queryText", new JSONObject()
                    .put("type", "string")
                    .put("description", "The full-text query"))
                .put("limit", new JSONObject()
                    .put("type", "integer")
                    .put("default", DEFAULT_LIMIT)
                    .put("minimum", 1)
                    .put("maximum", MAX_LIMIT)
                    .put("description",
                        "Maximum number of results to return (default: " + DEFAULT_LIMIT + ", maximum: " + MAX_LIMIT
                            + ")")))
            .put("required", new JSONArray().put("database").put("queryText")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    final String databaseName = args.getString("database");
    // Argument faults are reported before the database is resolved, so a malformed request reads the same
    // whether or not the database also resolves, and a rejected request does no I/O.
    FullTextQuery.validateArguments(args);

    final MCPToolUtils.DatabaseAccess access = MCPToolUtils.resolveDatabase(
        server, user, databaseName, config, MCPToolUtils.RequiredAccess.READ);
    final Database database = access.database();

    return FullTextQuery.search(database, args);
  }
}
