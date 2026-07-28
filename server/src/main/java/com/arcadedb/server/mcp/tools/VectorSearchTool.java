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
                    .put("description", "Dense-index search beam width; higher values improve recall at higher cost"))
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
    final int k = args.getInt("k", MCPVectorLeg.DEFAULT_K);
    if (k < 1 || k > MCPVectorLeg.MAX_K)
      throw new IllegalArgumentException("'k' must be between 1 and " + MCPVectorLeg.MAX_K);
    MCPVectorLeg.validateArguments(args, "indexName");

    final MCPToolUtils.DatabaseAccess access = MCPToolUtils.resolveDatabase(
        server, user, databaseName, config, MCPToolUtils.RequiredAccess.READ);
    final Database database = access.database();

    final MCPVectorLeg.VectorLegQuery leg = MCPVectorLeg.build(database, args, "indexName", k);

    final QueryEngine.AnalyzedQuery analyzed;
    try {
      analyzed = database.getQueryEngine("sql").analyze(leg.sql());
    } catch (final RuntimeException e) {
      throw invalidExpression(e);
    }
    if (!analyzed.isIdempotent())
      throw new SecurityException("Generated vector search is not read-only");

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final JSONArray results = new JSONArray();
    try {
      final ResultSet analyzedResultSet = analyzed.execute(leg.parameters());
      try (final ResultSet resultSet = analyzedResultSet != null
          ? analyzedResultSet
          : database.query("sql", leg.sql(), leg.parameters())) {
        // A stale/deleted hit or malformed row is skipped and cannot be backfilled because the vector candidate
        // window is already fixed. The response therefore reports possible truncation for filtered short results.
        while (resultSet.hasNext() && results.length() < k)
          appendResult(database, resultSet.next(), leg.sparse(), serializer, results);
      }
    } catch (final SecurityException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw invalidExpression(e);
    }

    // Truncation describes the result window, not the index. A filled window is the only state in which further
    // matches may exist; a short result set means the search ran out of candidates that satisfy the request, so
    // reporting truncation there would tell the caller to widen a search that cannot yield more. Index cardinality
    // is deliberately not consulted: it is almost always larger than the window, which would pin the flag to true
    // and strip it of meaning, and reading it costs a full scan of the index locations on the dense path.
    return new JSONObject()
        .put("indexName", leg.index().typeIndex().getName())
        .put("sparse", leg.sparse())
        .put("scoring", leg.index().scoring())
        .put("candidateLimit", leg.candidateLimit())
        .put("truncated", results.length() >= k)
        .put("count", results.length())
        .put("results", results);
  }

  private static void appendResult(final Database database, final Result row, final boolean sparse,
      final JsonSerializer serializer, final JSONArray results) {
    RID rid = MCPVectorLeg.toRID(row.getProperty("@rid"));
    if (rid == null)
      rid = row.getIdentity().orElse(null);
    if (rid == null)
      return;

    final Object rawScore = row.getProperty(sparse ? "score" : "distance");
    if (!(rawScore instanceof final Number score))
      return;

    final Object embeddedRecord = row.getProperty("record");
    final Document document;
    if (embeddedRecord instanceof final Document candidate) {
      document = candidate;
    } else {
      try {
        final Object loaded = database.lookupByRID(rid, true);
        if (!(loaded instanceof final Document candidate))
          return;
        document = candidate;
      } catch (final RecordNotFoundException e) {
        return;
      }
    }

    final JSONObject result = new JSONObject()
        .put("rid", rid.toString())
        .put("properties", serializer.serializeDocument(document));
    if (sparse)
      result.put("score", score);
    else
      result.put("distance", score);
    results.put(result);
  }

  private static IllegalArgumentException invalidExpression(final RuntimeException cause) {
    final String detail = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
    return new IllegalArgumentException("Invalid vector search or filter expression: " + detail, cause);
  }
}
