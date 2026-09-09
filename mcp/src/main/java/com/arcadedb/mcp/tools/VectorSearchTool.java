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

import com.arcadedb.query.search.VectorSearchLeg;
import com.arcadedb.query.search.VectorSearchOperation;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.mcp.MCPConfiguration;
import com.arcadedb.server.security.ServerSecurityUser;

/**
 * @author Justin Blethrow
 */
public class VectorSearchTool {
  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "vector_search")
        .put("description",
            """
            Search a dense LSM_VECTOR or sparse LSM_SPARSE_VECTOR index using a pre-computed query vector. \
            Dense results expose a distance (lower is better); sparse results expose a score (higher is better). \
            Set sparse=true for sparse indexes and pass queryIndices for a compact sparse representation. \
            Filtered searches inspect a bounded candidate window whose size is reported as candidateLimit. \
            The truncated flag means the result window was filled, so more matches may exist: raise k to see them. \
            When fewer than k results come back, truncated is false and the search already returned every match \
            it could find within candidateLimit. \
            Embedding generation is not performed by ArcadeDB.""")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database to search"))
                .put("indexName", new JSONObject()
                    .put("type", "string")
                    .put("description", "Name of an LSM_VECTOR or LSM_SPARSE_VECTOR index"))
                .put("queryVector", new JSONObject()
                    .put("type", "array")
                    .put("items", new JSONObject().put("type", "number"))
                    .put("description",
                        "Dense query vector, or sparse weights corresponding to queryIndices when sparse=true"))
                .put("queryIndices", new JSONObject()
                    .put("type", "array")
                    .put("items", new JSONObject().put("type", "integer").put("minimum", 0))
                    .put("description",
                        "Sparse dimension ids corresponding to queryVector weights; omit to use queryVector positions"))
                .put("k", new JSONObject()
                    .put("type", "integer")
                    .put("minimum", 1)
                    .put("maximum", MCPVectorLeg.MAX_K)
                    .put("default", MCPVectorLeg.DEFAULT_K)
                    .put("description", "Maximum number of results to return"))
                .put("efSearch", new JSONObject()
                    .put("type", "integer")
                    .put("minimum", 1)
                    .put("maximum", MCPVectorLeg.MAX_EF_SEARCH)
                    .put("description",
                        "Dense-index search beam width; higher values improve recall at higher cost (maximum: "
                            + MCPVectorLeg.MAX_EF_SEARCH + ")"))
                .put("filter", new JSONObject()
                    .put("type", "string")
                    .put("description",
                        "Optional read-only SQL WHERE predicate applied to a bounded candidate set. It is evaluated "
                            + "against each expanded neighbor row, where record properties are flattened and @rid, "
                            + "@type, record, plus distance (dense) or score (sparse) are available."))
                .put("sparse", new JSONObject()
                    .put("type", "boolean")
                    .put("default", false)
                    .put("description", "Use vector.sparseNeighbors against an LSM_SPARSE_VECTOR index")))
            .put("required", new JSONArray().put("database").put("indexName").put("queryVector").put("k")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final String databaseName = MCPToolUtils.requireString(args, "database");
    // Argument faults are reported before the database is resolved, so a malformed request reads the same
    // whether or not the database also resolves, and a rejected request does no I/O.
    VectorSearchOperation.requireK(args);
    VectorSearchLeg.validateArguments(args, "indexName");

    final MCPToolUtils.DatabaseAccess access = MCPToolUtils.resolveDatabase(
        server, user, databaseName, config, MCPToolUtils.RequiredAccess.READ);

    return VectorSearchOperation.execute(access.database(), args);
  }
}
