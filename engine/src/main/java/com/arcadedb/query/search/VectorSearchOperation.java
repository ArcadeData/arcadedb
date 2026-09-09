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
package com.arcadedb.query.search;

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

/**
 * kNN search over a dense {@code LSM_VECTOR} or sparse {@code LSM_SPARSE_VECTOR} index, expressed as a
 * JSON request and a JSON response so every wire protocol can serve it from one implementation.
 * <p>
 * This is the engine-side half of the MCP {@code vector_search} tool, of {@code POST /api/v1/vector/{database}/search}
 * and of the gRPC {@code VectorSearch} RPC (issue #7306). Whatever authenticated the caller and resolved the
 * database belongs to the surface; everything from argument validation down belongs here, so the three cannot
 * disagree about what a legal request is or about what the answer looks like.
 *
 * @author Justin Blethrow (original MCP tool)
 */
public final class VectorSearchOperation {

  private VectorSearchOperation() {
  }

  /**
   * Runs the search described by {@code args} against {@code database}.
   *
   * @param args {@code indexName}, {@code queryVector} and {@code k} are required; {@code queryIndices},
   *             {@code efSearch}, {@code filter} and {@code sparse} are optional. The bounds are
   *             {@link VectorSearchLeg#MAX_K} and {@link VectorSearchLeg#MAX_EF_SEARCH}.
   *
   * @return {@code indexName}, {@code sparse}, {@code scoring}, {@code candidateLimit}, {@code truncated},
   * {@code count} and the {@code results} array, each entry carrying {@code rid}, {@code properties} and either
   * {@code distance} (dense) or {@code score} (sparse).
   *
   * @throws IllegalArgumentException for any malformed argument, an unknown index, or an unparseable filter
   * @throws SecurityException        if the generated statement is not read-only
   */
  public static JSONObject execute(final Database database, final JSONObject args) {
    return execute(database, args, "indexName");
  }

  /**
   * Same as {@link #execute(Database, JSONObject)}, but reads the index name from {@code indexNameField}. The
   * hybrid search tool names the same argument {@code vectorIndexName}, and a shared error message must name the
   * field the caller actually sent.
   */
  public static JSONObject execute(final Database database, final JSONObject args, final String indexNameField) {
    final int k = requireK(args);
    VectorSearchLeg.validateArguments(args, indexNameField);

    final VectorSearchLeg.VectorLegQuery leg = VectorSearchLeg.build(database, args, indexNameField, k);

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

  /**
   * Reads and bounds {@code k}. Validated before anything else so a request that was going to be rejected for its
   * result-window size never resolves an index first and reports the index fault instead.
   */
  public static int requireK(final JSONObject args) {
    final int k = args.getInt("k", VectorSearchLeg.DEFAULT_K);
    if (k < 1 || k > VectorSearchLeg.MAX_K)
      throw new IllegalArgumentException("'k' must be between 1 and " + VectorSearchLeg.MAX_K);
    return k;
  }

  private static void appendResult(final Database database, final Result row, final boolean sparse,
      final JsonSerializer serializer, final JSONArray results) {
    RID rid = VectorSearchLeg.toRID(row.getProperty("@rid"));
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
