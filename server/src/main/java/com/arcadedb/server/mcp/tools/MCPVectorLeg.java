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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.schema.LSMSparseVectorIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Shared vector-retrieval leg. Resolves and validates a dense or sparse vector index against
 * caller-supplied arguments and produces the read-only SQL statement plus bound parameters that
 * fetch its ranked neighbors. Both the standalone vector search tool and the hybrid search tool
 * build their vector leg here, so a given malformed argument produces one error message rather
 * than two divergent ones.
 */
public final class MCPVectorLeg {
  public static final int DEFAULT_K             = 10;
  public static final int MAX_K                 = 1_000;
  public static final int FILTER_OVERFETCH      = 8;
  public static final int MAX_FILTER_CANDIDATES = 8_000;
  public static final int MAX_FILTER_EXPRESSION = 4_096;

  public record ResolvedVectorIndex(TypeIndex typeIndex, int dimensions, String scoring) {
  }

  public record VectorLegQuery(ResolvedVectorIndex index, boolean sparse, int candidateLimit, String sql,
      Map<String, Object> parameters) {
  }

  private record SparseQuery(int[] indices, float[] values) {
  }

  private MCPVectorLeg() {
  }

  /**
   * Validates the arguments that need no schema access. Kept separate from {@link #build} so callers
   * can run it before resolving the database, which is the order the tools use: an argument fault is
   * reported as an argument fault regardless of whether the database also resolves.
   */
  public static void validateArguments(final JSONObject args, final String indexNameField) {
    MCPToolUtils.requireString(args, indexNameField);
    final boolean sparse = args.getBoolean("sparse", false);
    final Integer efSearch = args.has("efSearch") ? args.getInt("efSearch") : null;
    if (efSearch != null && efSearch < 1)
      throw new IllegalArgumentException("'efSearch' must be at least 1");
    if (sparse && efSearch != null)
      throw new IllegalArgumentException("'efSearch' applies only to dense LSM_VECTOR indexes");
    if (!sparse && args.has("queryIndices"))
      throw new IllegalArgumentException("'queryIndices' requires sparse=true");
    normalizeFilter(args.getString("filter", null));
  }

  /**
   * Validates every vector argument and assembles the leg statement. Validation order is deliberate:
   * argument-shape faults are reported before any schema lookup, so a caller that got both the flags
   * and the index name wrong learns about the flags first and does not chase a schema error.
   *
   * @param indexNameField the argument key holding the index name, which differs between tools
   * @param limit          rows the leg should return, before any fusion
   */
  public static VectorLegQuery build(final Database database, final JSONObject args, final String indexNameField,
      final int limit) {
    validateArguments(args, indexNameField);
    final String indexName = MCPToolUtils.requireString(args, indexNameField);
    final boolean sparse = args.getBoolean("sparse", false);
    final Integer efSearch = args.has("efSearch") ? args.getInt("efSearch") : null;

    final String filter = normalizeFilter(args.getString("filter", null));
    final ResolvedVectorIndex resolved = resolveIndex(database, indexName, sparse);
    final float[] queryVector = readFloatArray(args.getJSONArray("queryVector", null), "queryVector");

    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("indexName", resolved.typeIndex().getName());
    parameters.put("legLimit", limit);

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
      requireNonZeroDenseVector(queryVector);
      parameters.put("queryVector", queryVector);
      functionCall = "`vector.neighbors`(:indexName, :queryVector, :candidateLimit, :options)";
    }

    final int candidateLimit =
        filter == null ? limit : (int) Math.min((long) limit * FILTER_OVERFETCH, MAX_FILTER_CANDIDATES);
    parameters.put("candidateLimit", candidateLimit);

    final StringBuilder sql = new StringBuilder("SELECT FROM (SELECT expand(").append(functionCall).append("))");
    if (filter != null)
      sql.append(" WHERE (").append(filter).append(')');
    sql.append(" LIMIT :legLimit");

    return new VectorLegQuery(resolved, sparse, candidateLimit, sql.toString(), parameters);
  }

  /**
   * Coerces the several shapes a RID reaches a projection in - a RID, a record reference, or the
   * string form a projected {@code @rid} can carry - into a RID. Returns null for anything else,
   * which callers treat as a row to skip.
   */
  public static RID toRID(final Object raw) {
    if (raw instanceof final RID rid)
      return rid;
    if (raw instanceof final Identifiable identifiable)
      return identifiable.getIdentity();
    if (raw instanceof final String value && RID.is(value))
      return new RID(value);
    return null;
  }

  private static String normalizeFilter(final String raw) {
    if (raw == null)
      return null;
    final String filter = raw.trim();
    if (filter.isEmpty())
      return null;
    if (filter.length() > MAX_FILTER_EXPRESSION)
      throw new IllegalArgumentException("'filter' must not exceed " + MAX_FILTER_EXPRESSION + " characters");
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

  private static void requireNonZeroDenseVector(final float[] queryVector) {
    for (final float value : queryVector)
      if (value != 0.0f)
        return;
    throw new IllegalArgumentException("Dense 'queryVector' must contain at least one non-zero value");
  }

  private static SparseQuery buildSparseQuery(final JSONObject args, final float[] queryVector, final int dimensions) {
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
}
