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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Fuses a vector retrieval leg, a full-text retrieval leg, and a depth-limited graph expansion leg
 * into one ranked result list.
 * <p>
 * The expansion leg is seeded from the union of the retrieval legs and carries no score of its own:
 * its breadth-first discovery order is its rank. Fusion scoring is performed by the engine's
 * {@code vector.fuse}, never re-implemented here.
 * <p>
 * A request naming only the vector leg cannot be fused, because fusion needs at least two sources.
 * Such a response reports {@code fused=false} and returns the leg's native distance or score rather
 * than a fabricated fused score.
 */
public class HybridSearchTool {
  public static final int LEG_OVERFETCH      = 4;
  public static final int MAX_LEG_CANDIDATES = 1_000;
  public static final int MAX_SEEDS          = 256;
  public static final int MAX_EXPANSION      = 2_000;
  public static final int MAX_DEPTH          = 3;

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
                    .put("description", "Dense-index search beam width; higher values improve recall at higher cost"))
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
                            + "should not outrank a direct match")
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
    final int k = args.getInt("k", MCPVectorLeg.DEFAULT_K);
    if (k < 1 || k > MCPVectorLeg.MAX_K)
      throw new IllegalArgumentException("'k' must be between 1 and " + MCPVectorLeg.MAX_K);

    // Argument faults are reported before the database is resolved, so a malformed request reads the
    // same whether or not the database also resolves.
    MCPVectorLeg.validateArguments(args, "vectorIndexName");

    final MCPToolUtils.DatabaseAccess access = MCPToolUtils.resolveDatabase(
        server, user, databaseName, config, MCPToolUtils.RequiredAccess.READ);
    final Database database = access.database();

    final MCPVectorLeg.VectorLegQuery vectorQuery = MCPVectorLeg.build(database, args, "vectorIndexName", legLimit(k));
    final List<LegRow> vectorLeg = runVectorLeg(database, vectorQuery);

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final JSONObject legs = new JSONObject()
        .put("vector", new JSONObject().put("count", vectorLeg.size()));

    final JSONArray results = new JSONArray();
    for (final LegRow row : vectorLeg) {
      if (results.length() >= k)
        break;
      final Document document = lookup(database, row.rid());
      if (document == null)
        continue;
      results.put(new JSONObject()
          .put("rid", row.rid().toString())
          .put(vectorQuery.sparse() ? "score" : "distance", row.score())
          .put("sources", new JSONArray().put("vector"))
          .put("properties", serializer.serializeDocument(document)));
    }

    return new JSONObject()
        .put("vectorIndexName", vectorQuery.index().typeIndex().getName())
        .put("sparse", vectorQuery.sparse())
        .put("scoring", vectorQuery.index().scoring())
        .put("fused", false)
        .put("legs", legs)
        .put("truncated", results.length() >= k)
        .put("count", results.length())
        .put("results", results);
  }

  /**
   * Rows each retrieval leg fetches before fusion. Fusing lists that are only as long as the requested
   * result count would leave fusion nothing to reorder, so each leg over-fetches.
   */
  public static int legLimit(final int k) {
    return (int) Math.min((long) k * LEG_OVERFETCH, MAX_LEG_CANDIDATES);
  }

  private static List<LegRow> runVectorLeg(final Database database, final MCPVectorLeg.VectorLegQuery query) {
    final QueryEngine.AnalyzedQuery analyzed;
    try {
      analyzed = database.getQueryEngine("sql").analyze(query.sql());
    } catch (final RuntimeException e) {
      throw invalidExpression("vector leg", e);
    }
    if (!analyzed.isIdempotent())
      throw new SecurityException("Generated vector leg is not read-only");

    final List<LegRow> rows = new ArrayList<>();
    final Set<RID> seen = new LinkedHashSet<>();
    try {
      final ResultSet analyzedResultSet = analyzed.execute(query.parameters());
      try (final ResultSet resultSet = analyzedResultSet != null
          ? analyzedResultSet
          : database.query("sql", query.sql(), query.parameters())) {
        while (resultSet.hasNext()) {
          final Result row = resultSet.next();
          RID rid = MCPVectorLeg.toRID(row.getProperty("@rid"));
          if (rid == null)
            rid = row.getIdentity().orElse(null);
          if (rid == null || !seen.add(rid))
            continue;
          final Object raw = row.getProperty(query.sparse() ? "score" : "distance");
          if (!(raw instanceof final Number score))
            continue;
          rows.add(new LegRow(rid, score.doubleValue()));
        }
      }
    } catch (final SecurityException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw invalidExpression("vector leg", e);
    }
    return rows;
  }

  private static Document lookup(final Database database, final RID rid) {
    try {
      return database.lookupByRID(rid, true) instanceof final Document document ? document : null;
    } catch (final RecordNotFoundException e) {
      return null;
    }
  }

  private static IllegalArgumentException invalidExpression(final String stage, final RuntimeException cause) {
    final String detail = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
    return new IllegalArgumentException("Invalid hybrid search " + stage + ": " + detail, cause);
  }
}
