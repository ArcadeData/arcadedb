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
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LSMSparseVectorIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author Justin Blethrow
 */
public class VectorSearchTool {
  private static final int DEFAULT_K             = 10;
  private static final int MAX_K                 = 1_000;
  private static final int FILTER_OVERFETCH      = 8;
  private static final int MAX_FILTER_EXPRESSION = 4_096;

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "vector_search")
        .put("description",
            """
            Search a dense LSM_VECTOR or sparse LSM_SPARSE_VECTOR index using a pre-computed query vector. \
            Dense results expose a distance (lower is better); sparse results expose a score (higher is better). \
            Set sparse=true for sparse indexes and pass queryIndices for a compact sparse representation. \
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
                    .put("maximum", MAX_K)
                    .put("default", DEFAULT_K)
                    .put("description", "Maximum number of results to return"))
                .put("efSearch", new JSONObject()
                    .put("type", "integer")
                    .put("minimum", 1)
                    .put("description", "Dense-index search beam width; higher values improve recall at higher cost"))
                .put("filter", new JSONObject()
                    .put("type", "string")
                    .put("description",
                        "Optional read-only SQL WHERE predicate applied to a bounded candidate set"))
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
    final String indexName = MCPToolUtils.requireString(args, "indexName");
    final int k = args.getInt("k", DEFAULT_K);
    if (k < 1 || k > MAX_K)
      throw new IllegalArgumentException("'k' must be between 1 and " + MAX_K);

    final boolean sparse = args.getBoolean("sparse", false);
    final Integer efSearch = args.has("efSearch") ? args.getInt("efSearch") : null;
    if (efSearch != null && efSearch < 1)
      throw new IllegalArgumentException("'efSearch' must be at least 1");
    if (sparse && efSearch != null)
      throw new IllegalArgumentException("'efSearch' applies only to dense LSM_VECTOR indexes");
    if (!sparse && args.has("queryIndices"))
      throw new IllegalArgumentException("'queryIndices' requires sparse=true");

    final String filter = normalizeFilter(args.getString("filter", null));
    final Database database = MCPToolUtils.resolveDatabase(server, user, databaseName);
    final ResolvedVectorIndex resolved = resolveIndex(database, indexName, sparse);
    final float[] queryVector = readFloatArray(args.getJSONArray("queryVector", null), "queryVector");

    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("indexName", resolved.typeIndex().getName());
    parameters.put("k", k);

    final Map<String, Object> options = new LinkedHashMap<>();
    if (efSearch != null)
      options.put("efSearch", efSearch);
    parameters.put("options", options);

    final String functionCall;
    if (sparse) {
      final SparseQuery sparseQuery = buildSparseQuery(args, queryVector, resolved.dimensions());
      parameters.put("queryIndices", sparseQuery.indices());
      parameters.put("queryVector", sparseQuery.values());
      functionCall = "`vector.sparseNeighbors`(:indexName, :queryIndices, :queryVector, :candidateLimit, :options)";
    } else {
      if (queryVector.length != resolved.dimensions())
        throw new IllegalArgumentException(
            "'queryVector' has " + queryVector.length + " dimensions, but index '" + indexName + "' requires "
                + resolved.dimensions());
      parameters.put("queryVector", queryVector);
      functionCall = "`vector.neighbors`(:indexName, :queryVector, :candidateLimit, :options)";
    }

    final int candidateLimit = filter == null ? k : (int) Math.min((long) k * FILTER_OVERFETCH, MAX_K);
    parameters.put("candidateLimit", candidateLimit);

    final StringBuilder sql = new StringBuilder("SELECT FROM (SELECT expand(").append(functionCall).append("))");
    if (filter != null)
      sql.append(" WHERE (").append(filter).append(')');
    sql.append(" LIMIT :k");

    final QueryEngine.AnalyzedQuery analyzed;
    try {
      analyzed = database.getQueryEngine("sql").analyze(sql.toString());
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
      final ResultSet analyzedResultSet = analyzed.execute(parameters);
      try (final ResultSet resultSet =
          analyzedResultSet != null ? analyzedResultSet : database.query("sql", sql.toString(), parameters)) {
        while (resultSet.hasNext() && results.length() < k)
          appendResult(database, resultSet.next(), sparse, serializer, results);
      }
    } catch (final SecurityException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw invalidExpression(e);
    }

    return new JSONObject()
        .put("indexName", resolved.typeIndex().getName())
        .put("sparse", sparse)
        .put("scoring", resolved.scoring())
        .put("count", results.length())
        .put("results", results);
  }

  private static String normalizeFilter(final String raw) {
    if (raw == null)
      return null;
    final String filter = raw.trim();
    if (filter.isEmpty())
      return null;
    if (filter.length() > MAX_FILTER_EXPRESSION)
      throw new IllegalArgumentException(
          "'filter' must not exceed " + MAX_FILTER_EXPRESSION + " characters");
    return filter;
  }

  private static ResolvedVectorIndex resolveIndex(final Database database, final String indexName,
      final boolean sparse) {
    final Index rawIndex;
    try {
      rawIndex = database.getSchema().getIndexByName(indexName);
    } catch (final SchemaException e) {
      throw new IllegalArgumentException(
          "Vector index '" + indexName + "' does not exist. " + describeAvailableIndexes(database, sparse), e);
    }

    if (!(rawIndex instanceof final TypeIndex typeIndex))
      throw new IllegalArgumentException(
          "Index '" + indexName + "' is not a type index. " + describeAvailableIndexes(database, sparse));

    final Schema.INDEX_TYPE expectedType =
        sparse ? Schema.INDEX_TYPE.LSM_SPARSE_VECTOR : Schema.INDEX_TYPE.LSM_VECTOR;
    final Schema.INDEX_TYPE actualType = typeIndex.getType();
    if (actualType != expectedType) {
      final String hint = actualType == Schema.INDEX_TYPE.LSM_SPARSE_VECTOR
          ? " Set sparse=true for this index."
          : actualType == Schema.INDEX_TYPE.LSM_VECTOR ? " Set sparse=false for this index." : "";
      throw new IllegalArgumentException(
          "Index '" + indexName + "' is " + actualType + ", not " + expectedType + "." + hint + " "
              + describeAvailableIndexes(database, sparse));
    }

    for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets()) {
      if (bucketIndex instanceof final LSMVectorIndex denseIndex)
        return new ResolvedVectorIndex(typeIndex, denseIndex.getDimensions(),
            "distance_lower_is_better:" + denseIndex.getSimilarityFunction().name());

      if (bucketIndex instanceof final LSMSparseVectorIndex sparseIndex) {
        final LSMSparseVectorIndexMetadata metadata = sparseIndex.getSparseMetadata();
        final int dimensions = metadata != null ? metadata.dimensions : 0;
        final String modifier = metadata != null ? metadata.modifier : LSMSparseVectorIndexMetadata.MODIFIER_NONE;
        final String scoring = LSMSparseVectorIndexMetadata.MODIFIER_IDF.equals(modifier)
            ? "score_higher_is_better:idf_weighted_dot_product"
            : "score_higher_is_better:dot_product";
        return new ResolvedVectorIndex(typeIndex, dimensions, scoring);
      }
    }

    throw new IllegalArgumentException("Vector index '" + indexName + "' has no searchable bucket indexes");
  }

  private static String describeAvailableIndexes(final Database database, final boolean sparse) {
    final Schema.INDEX_TYPE expectedType =
        sparse ? Schema.INDEX_TYPE.LSM_SPARSE_VECTOR : Schema.INDEX_TYPE.LSM_VECTOR;
    final Set<String> names = new TreeSet<>();
    for (final Index index : database.getSchema().getIndexes())
      if (index instanceof TypeIndex && index.getType() == expectedType)
        names.add(index.getName());
    return "Available " + expectedType + " indexes: " + names;
  }

  private static float[] readFloatArray(final JSONArray array, final String field) {
    if (array == null || array.length() == 0)
      throw new IllegalArgumentException("'" + field + "' is required and must not be empty");

    final float[] result = new float[array.length()];
    for (int i = 0; i < array.length(); i++) {
      final Object value = array.get(i);
      if (!(value instanceof final Number number))
        throw new IllegalArgumentException("'" + field + "' must contain only numbers");
      final double asDouble = number.doubleValue();
      if (!Double.isFinite(asDouble) || Math.abs(asDouble) > Float.MAX_VALUE)
        throw new IllegalArgumentException("'" + field + "' must contain only finite float values");
      result[i] = (float) asDouble;
    }
    return result;
  }

  private static SparseQuery buildSparseQuery(final JSONObject args, final float[] queryVector,
      final int dimensions) {
    final JSONArray queryIndices = args.getJSONArray("queryIndices", null);
    if (queryIndices == null) {
      if (dimensions > 0 && queryVector.length != dimensions)
        throw new IllegalArgumentException(
            "Sparse 'queryVector' has " + queryVector.length + " dimensions, but the index requires " + dimensions
                + ". Pass queryIndices with compact sparse weights instead.");

      final List<Integer> indices = new ArrayList<>();
      final List<Float> values = new ArrayList<>();
      for (int i = 0; i < queryVector.length; i++)
        if (queryVector[i] != 0.0f) {
          indices.add(i);
          values.add(queryVector[i]);
        }
      if (indices.isEmpty())
        throw new IllegalArgumentException("Sparse 'queryVector' must contain at least one non-zero weight");

      final int[] compactIndices = new int[indices.size()];
      final float[] compactValues = new float[values.size()];
      for (int i = 0; i < indices.size(); i++) {
        compactIndices[i] = indices.get(i);
        compactValues[i] = values.get(i);
      }
      return new SparseQuery(compactIndices, compactValues);
    }

    if (queryIndices.length() != queryVector.length)
      throw new IllegalArgumentException(
          "'queryIndices' and 'queryVector' must have the same length (got " + queryIndices.length() + " and "
              + queryVector.length + ")");

    final int[] indices = new int[queryIndices.length()];
    final Set<Integer> seen = new HashSet<>();
    boolean hasNonZeroWeight = false;
    for (int i = 0; i < queryIndices.length(); i++) {
      final Object raw = queryIndices.get(i);
      if (!(raw instanceof final Number number) || !Double.isFinite(number.doubleValue())
          || number.doubleValue() != Math.rint(number.doubleValue())
          || number.doubleValue() < 0 || number.doubleValue() > Integer.MAX_VALUE)
        throw new IllegalArgumentException("'queryIndices' must contain only non-negative integers");

      final int index = number.intValue();
      if (!seen.add(index))
        throw new IllegalArgumentException("'queryIndices' contains duplicate dimension " + index);
      if (dimensions > 0 && index >= dimensions)
        throw new IllegalArgumentException(
            "Sparse query dimension " + index + " is outside index dimensions 0-" + (dimensions - 1));
      indices[i] = index;
      hasNonZeroWeight |= queryVector[i] != 0.0f;
    }
    if (!hasNonZeroWeight)
      throw new IllegalArgumentException("Sparse 'queryVector' must contain at least one non-zero weight");

    return new SparseQuery(indices, queryVector);
  }

  private static void appendResult(final Database database, final Result row, final boolean sparse,
      final JsonSerializer serializer, final JSONArray results) {
    final RID rid = resolveRID(row);
    if (rid == null)
      return;

    final Object rawScore = row.getProperty(sparse ? "score" : "distance");
    if (!(rawScore instanceof final Number score))
      return;

    final Record record;
    try {
      record = database.lookupByRID(rid, true);
    } catch (final RecordNotFoundException e) {
      return;
    }
    if (!(record instanceof final Document document))
      return;

    final JSONObject result = new JSONObject()
        .put("rid", rid.toString())
        .put("score", score)
        .put("properties", serializer.serializeDocument(document));
    if (!sparse)
      result.put("distance", score);
    results.put(result);
  }

  private static RID resolveRID(final Result row) {
    final Object raw = row.getProperty("@rid");
    if (raw instanceof final RID rid)
      return rid;
    if (raw instanceof final String value && RID.is(value))
      return new RID(value);
    return row.getIdentity().orElse(null);
  }

  private static IllegalArgumentException invalidExpression(final RuntimeException cause) {
    final String detail = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
    return new IllegalArgumentException("Invalid vector search or filter expression: " + detail, cause);
  }

  private record ResolvedVectorIndex(TypeIndex typeIndex, int dimensions, String scoring) {
  }

  private record SparseQuery(int[] indices, float[] values) {
  }
}
