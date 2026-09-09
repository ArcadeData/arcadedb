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

import com.arcadedb.database.RID;
import com.arcadedb.query.search.HybridSearchOperation;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.mcp.MCPConfiguration;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.ArrayList;
import java.util.List;

/**
 * MCP {@code hybrid_search} tool: publishes the JSON-Schema the protocol needs and delegates the search itself to
 * {@link HybridSearchOperation}, which the HTTP {@code /api/v1/vector/{database}/hybrid} route and the gRPC
 * {@code HybridSearch} RPC also call (issue #7306).
 * <p>
 * The caps below are read from that operation rather than restated, so the schema this tool advertises cannot
 * drift from the one the search enforces.
 */
public class HybridSearchTool {
  public static final int LEG_OVERFETCH      = HybridSearchOperation.LEG_OVERFETCH;
  public static final int MAX_LEG_CANDIDATES = HybridSearchOperation.MAX_LEG_CANDIDATES;
  public static final int MAX_SEEDS          = HybridSearchOperation.MAX_SEEDS;
  public static final int MAX_EXPANSION      = HybridSearchOperation.MAX_EXPANSION;
  public static final int MAX_DEPTH          = HybridSearchOperation.MAX_DEPTH;

  /**
   * One row of one retrieval leg. A null score marks a rank-only leg, whose position in the list is
   * its only ranking signal.
   */
  public record LegRow(RID rid, Double score) {
  }

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
                    .put("maximum", MCPVectorLeg.MAX_K)
                    .put("default", MCPVectorLeg.DEFAULT_K)
                    .put("description", "Maximum number of results to return after fusion"))
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

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final String databaseName = MCPToolUtils.requireString(args, "database");
    // Argument faults are reported before the database is resolved, so a malformed request reads the same
    // whether or not the database also resolves, and a rejected request does no I/O.
    HybridSearchOperation.validateArguments(args);

    final MCPToolUtils.DatabaseAccess access = MCPToolUtils.resolveDatabase(
        server, user, databaseName, config, MCPToolUtils.RequiredAccess.READ);

    return HybridSearchOperation.execute(access.database(), args);
  }

  /**
   * Rows each retrieval leg fetches before fusion. Fusing lists that are only as long as the requested
   * result count would leave fusion nothing to reorder, so each leg over-fetches.
   */
  public static int legLimit(final int k) {
    return HybridSearchOperation.legLimit(k);
  }

  /**
   * Picks the records the expansion walks out from. Delegates to {@link HybridSearchOperation#collectSeeds} so the
   * interleaving policy has one implementation; this overload exists because the tool's own {@link LegRow} is the
   * shape the MCP-side tests build.
   */
  static List<RID> collectSeeds(final List<LegRow> vectorLeg, final List<LegRow> fullTextLeg) {
    return HybridSearchOperation.collectSeeds(toEngineRows(vectorLeg), toEngineRows(fullTextLeg));
  }

  private static List<HybridSearchOperation.LegRow> toEngineRows(final List<LegRow> rows) {
    final List<HybridSearchOperation.LegRow> converted = new ArrayList<>(rows.size());
    for (final LegRow row : rows)
      converted.add(new HybridSearchOperation.LegRow(row.rid(), row.score()));
    return converted;
  }
}
