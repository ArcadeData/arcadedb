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
import com.arcadedb.query.search.HybridSearch;
import com.arcadedb.query.search.VectorLeg;

/**
 * MCP {@code hybrid_search} tool: the MCP-shaped wrapper - tool schema, read permission, database resolution -
 * around {@link HybridSearch}, which the HTTP and gRPC hybrid surfaces call too.
 */
public class HybridSearchTool {
  public static final int LEG_OVERFETCH      = HybridSearch.LEG_OVERFETCH;
  public static final int MAX_LEG_CANDIDATES = HybridSearch.MAX_LEG_CANDIDATES;
  public static final int MAX_SEEDS          = HybridSearch.MAX_SEEDS;
  public static final int MAX_EXPANSION      = HybridSearch.MAX_EXPANSION;
  public static final int MAX_DEPTH          = HybridSearch.MAX_DEPTH;

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "hybrid_search")
        .put("description",
            """
            Retrieve records by fusing a vector search, an optional full-text search, and an optional \
            graph expansion into one ranked list. The expansion walks outward from the records the other \
            legs found, so a neighbor of a strong match can itself rank. Supply 'fulltextIndexName' with \
            'fulltextQuery' to add the full-text leg, and 'expand' to add the graph leg; with neither, \
            this returns a plain vector search and reports fused=false. Graph expansion is rank-only, so \
            it requires fusionStrategy RRF. Depth is capped at 3 hops server-side. Each result names the \
            legs it came from in 'sources', and expansion-sourced results carry 'depth' and the 'path' \
            back to their seed. Embedding generation is not performed by ArcadeDB.""")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database to search"))
                .put("vectorIndexName", new JSONObject()
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
                    .put("maximum", VectorLeg.MAX_K)
                    .put("default", VectorLeg.DEFAULT_K)
                    .put("description", "Maximum number of results to return after fusion"))
                .put("efSearch", new JSONObject()
                    .put("type", "integer")
                    .put("minimum", 1)
                    .put("maximum", VectorLeg.MAX_EF_SEARCH)
                    .put("description",
                        "Dense-index search beam width; higher values improve recall at higher cost (maximum: "
                            + VectorLeg.MAX_EF_SEARCH + ")"))
                .put("filter", new JSONObject()
                    .put("type", "string")
                    .put("description",
                        "Optional read-only SQL WHERE predicate applied to the vector leg's bounded candidate set"))
                .put("sparse", new JSONObject()
                    .put("type", "boolean")
                    .put("default", false)
                    .put("description", "Use vector.sparseNeighbors against an LSM_SPARSE_VECTOR index"))
                .put("fulltextIndexName", new JSONObject()
                    .put("type", "string")
                    .put("description",
                        "Name of the full-text index, e.g. 'Article[content]'. Must be given together with fulltextQuery"))
                .put("fulltextQuery", new JSONObject()
                    .put("type", "string")
                    .put("description",
                        "Full-text query for the second leg. Must be given together with fulltextIndexName"))
                .put("expand", new JSONObject()
                    .put("type", "object")
                    .put("description", "Graph expansion leg, seeded from the records the other legs found")
                    .put("properties", new JSONObject()
                        .put("edgeTypes", new JSONObject()
                            .put("type", "array")
                            .put("items", new JSONObject().put("type", "string"))
                            .put("description", "Edge types to traverse; omit to traverse every edge type"))
                        .put("direction", new JSONObject()
                            .put("type", "string")
                            .put("enum", new JSONArray().put("out").put("in").put("both"))
                            .put("default", "out")
                            .put("description", "Traversal direction"))
                        .put("maxDepth", new JSONObject()
                            .put("type", "integer")
                            .put("minimum", 1)
                            .put("maximum", MAX_DEPTH)
                            .put("default", 1)
                            .put("description", "Hops to walk; capped at " + MAX_DEPTH + " server-side"))))
                .put("fusionStrategy", new JSONObject()
                    .put("type", "string")
                    .put("enum", new JSONArray().put("RRF").put("DBSF").put("LINEAR"))
                    .put("default", "RRF")
                    .put("description",
                        "Fusion strategy. DBSF and LINEAR need a score on every row, so neither can be combined "
                            + "with the rank-only expansion leg"))
                .put("weights", new JSONObject()
                    .put("type", "object")
                    .put("description",
                        "Per-leg fusion weights. The expansion leg defaults to 0.5 because an arbitrary neighbor "
                            + "should not outrank a direct match. A weight naming a leg the request does not ask for "
                            + "is rejected; a weight for a leg that runs but matches nothing has no effect, and that "
                            + "leg's zero row count is reported under 'legs'.")
                    .put("properties", new JSONObject()
                        .put("vector", new JSONObject().put("type", "number").put("default", 1.0))
                        .put("fulltext", new JSONObject().put("type", "number").put("default", 1.0))
                        .put("expand", new JSONObject().put("type", "number").put("default", 0.5)))))
            .put("required", new JSONArray().put("database").put("vectorIndexName").put("queryVector").put("k")));
  }

  /**
   * Rows each retrieval leg fetches before fusion, delegated to {@link HybridSearch#legLimit(int)} so the tool
   * and the shared implementation cannot disagree about the over-fetch.
   */
  public static int legLimit(final int k) {
    return HybridSearch.legLimit(k);
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final String databaseName = MCPToolUtils.requireString(args, "database");
    // Argument faults are reported before the database is resolved, so a malformed request reads the same
    // whether or not the database also resolves, and a rejected request does no I/O.
    HybridSearch.validateArguments(args);

    final MCPToolUtils.DatabaseAccess access = MCPToolUtils.resolveDatabase(
        server, user, databaseName, config, MCPToolUtils.RequiredAccess.READ);
    final Database database = access.database();

    return HybridSearch.search(database, args);
  }
}
