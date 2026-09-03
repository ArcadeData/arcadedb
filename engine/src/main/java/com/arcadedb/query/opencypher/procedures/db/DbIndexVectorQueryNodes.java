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
package com.arcadedb.query.opencypher.procedures.db;

import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.NumberUtils;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.RidHashSet;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Neo4j-compatible procedure: db.index.vector.queryNodes(indexName, k, vector)
 * <p>
 * Performs an approximate nearest neighbor search using a vector index.
 * This provides compatibility with Neo4j's vector search API.
 * </p>
 * <p>
 * The index can be specified as:
 * <ul>
 *   <li>ArcadeDB format: {@code 'Type[property]'} (e.g., {@code 'Document[embedding]'})</li>
 *   <li>Index name: the name of the vector index as returned by the schema</li>
 * </ul>
 * </p>
 * <p>
 * Example (Neo4j-compatible):
 * <pre>
 * CALL db.index.vector.queryNodes('Document[embedding]', 10, $vec)
 * YIELD node, score
 * RETURN node.title AS title, score
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DbIndexVectorQueryNodes implements CypherProcedure {
  public static final String NAME = "db.index.vector.querynodes";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Performs approximate nearest neighbor vector search using an HNSW index (Neo4j-compatible)";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final String indexSpec = args[0].toString();
    final int limit = extractLimit(args[1]);

    Object key = args[2];
    if (key == null)
      throw new CommandSQLParsingException("Vector parameter is null");
    if (key instanceof List<?> list)
      key = list.toArray();

    // Resolve the vector index
    final List<LSMVectorIndex> vectorIndexes = resolveVectorIndexes(indexSpec, context);

    // Get the query vector
    final float[] queryVector = extractQueryVector(key, vectorIndexes.get(0), context);

    // Search across all indexes and collect results
    final List<Pair<RID, Float>> allNeighbors = new ArrayList<>();
    for (final LSMVectorIndex lsmIndex : vectorIndexes) {
      final List<Pair<RID, Float>> neighbors = lsmIndex.findNeighborsFromVector(queryVector, limit);
      allNeighbors.addAll(neighbors);
    }

    // Sort by distance (ascending - closer is better)
    allNeighbors.sort(Comparator.comparing(Pair::getSecond));

    // Take top k results, converting distance to score (similarity).
    // EUCLIDEAN distance is L2² in [0, ∞); map it back to a (0, 1] similarity so callers receive a comparable score.
    final VectorSimilarityFunction similarityFunction = vectorIndexes.get(0).getSimilarityFunction();
    final int resultCount = Math.min(limit, allNeighbors.size());
    final List<Result> results = new ArrayList<>(resultCount);

    // One node, one row: the Neo4j procedure this mirrors promises k rows carrying k DISTINCT nodes, and every
    // consumer taking the top k relies on it. Nothing upstream can promise it on its own - this list is the
    // concatenation of one search per resolved index, and a single record can be offered by more than one of them
    // (issue #7057). Deduplicating here, after the sort, keeps each node at its NEAREST distance and lets the walk
    // continue past a repeat, so a k that was silently halved comes back full instead of merely unduplicated.
    final RidHashSet emitted = new RidHashSet(resultCount);

    for (final Pair<RID, Float> neighbor : allNeighbors) {
      if (results.size() == resultCount)
        break;
      if (!emitted.add(neighbor.getFirst()))
        continue;

      final Document record = neighbor.getFirst().asDocument();
      final float distance = neighbor.getSecond();

      final float score = similarityFunction == VectorSimilarityFunction.EUCLIDEAN
          ? 1.0f / (1.0f + distance)
          : 1.0f - distance;

      final ResultInternal r = new ResultInternal();
      r.setProperty("node", record);
      r.setProperty("score", score);
      results.add(r);
    }

    return results.stream();
  }

  /**
   * Narrows the {@code k} argument to an {@code int}, saturating at the int bounds instead of
   * failing, whether it arrives as a {@link Number} or as a numeric string.
   * <p>
   * A result-count bound has a sensible "as many as exist" reading, so a value above
   * {@link Integer#MAX_VALUE} saturates rather than being rejected. The string fallback used to go
   * through {@link Integer#parseInt}, which threw a bare {@code NumberFormatException} for exactly
   * the same magnitudes the {@link Number} branch next to it accepted - the two branches now agree.
   * </p>
   * <p>
   * A negative {@code k} has no such reading and is rejected by name, mirroring
   * {@code AbstractAlgoProcedure.extractCount()}. {@code LSMVectorIndex.findNeighborsFromVector}
   * clamps with {@code Math.min(k, candidateCount)}, which leaves a negative value negative, so
   * without this check it reaches {@code new ArrayList<>(k)} and surfaces as a bare
   * {@code IllegalArgumentException: Illegal Capacity} that never mentions {@code k}.
   * </p>
   */
  private static int extractLimit(final Object arg) {
    final int limit;
    if (arg instanceof Number n)
      limit = NumberUtils.saturateToInt(n);
    else {
      final String text = arg.toString().trim();
      try {
        limit = NumberUtils.saturateToInt(new BigDecimal(text));
      } catch (final NumberFormatException e) {
        throw new CommandSQLParsingException("db.index.vector.queryNodes(): k must be a number, got '" + text + "'", e);
      }
    }

    if (limit < 0)
      throw new CommandSQLParsingException("db.index.vector.queryNodes(): k must not be negative, got " + limit);

    return limit;
  }

  /**
   * Resolves the index specification to a list of LSMVectorIndex instances.
   * Supports both ArcadeDB format (Type[property]) and direct index name.
   */
  private List<LSMVectorIndex> resolveVectorIndexes(final String indexSpec, final CommandContext context) {
    final int bracketStart = indexSpec.indexOf('[');
    if (bracketStart > 0 && indexSpec.endsWith("]")) {
      // ArcadeDB format: Type[property]
      final String typeName = indexSpec.substring(0, bracketStart);
      final String propertyName = indexSpec.substring(bracketStart + 1, indexSpec.length() - 1);

      final DocumentType type = context.getDatabase().getSchema().getType(typeName);
      final TypeIndex typeIndex = type.getPolymorphicIndexByProperties(propertyName);

      if (typeIndex == null)
        throw new CommandSQLParsingException(
            "No vector index found on property '" + propertyName + "' for type '" + typeName + "'");

      final Set<Integer> allowedBucketIds = new HashSet<>();
      for (final Bucket bucket : type.getBuckets(false))
        allowedBucketIds.add(bucket.getFileId());

      return filterVectorIndexes(typeIndex, allowedBucketIds);
    }

    // Direct index name lookup
    final Index directIndex = context.getDatabase().getSchema().getIndexByName(indexSpec);
    if (directIndex instanceof TypeIndex typeIndex)
      return filterVectorIndexes(typeIndex, null);

    throw new CommandSQLParsingException(
        "Index '" + indexSpec + "' is not a vector index (found: " +
            (directIndex != null ? directIndex.getClass().getSimpleName() : "null") + ")");
  }

  /**
   * Resolves the vector sub-indexes to search. See
   * {@link VectorUtils#collectSubIndexesOnce(IndexInternal[], Class, java.util.function.IntPredicate)} for why the same
   * sub-index can be listed twice and why the guard is on identity rather than on the bucket id (issue #7057).
   */
  private List<LSMVectorIndex> filterVectorIndexes(final TypeIndex typeIndex, final Set<Integer> allowedBucketIds) {
    final IndexInternal[] bucketIndexes = typeIndex.getIndexesOnBuckets();
    if (bucketIndexes == null || bucketIndexes.length == 0)
      throw new CommandSQLParsingException("Index '" + typeIndex.getName() + "' has no bucket indexes");

    final List<LSMVectorIndex> vectorIndexes = VectorUtils.collectSubIndexesOnce(bucketIndexes, LSMVectorIndex.class,
        allowedBucketIds == null ? null : allowedBucketIds::contains);

    if (vectorIndexes.isEmpty())
      throw new CommandSQLParsingException("Index '" + typeIndex.getName() + "' is not a vector index");

    return vectorIndexes;
  }

  /**
   * Extracts a float[] query vector from the key parameter.
   */
  private float[] extractQueryVector(final Object key, final LSMVectorIndex lsmIndex, final CommandContext context) {
    if (key instanceof float[] floatArray)
      return floatArray;

    if (key instanceof long[] src) {
      // Issue #4148: integer-only JSON arrays now arrive as long[] from the HTTP path.
      final float[] queryVector = new float[src.length];
      for (int i = 0; i < src.length; i++)
        queryVector[i] = src[i];
      return queryVector;
    }

    if (key instanceof double[] src) {
      final float[] queryVector = new float[src.length];
      for (int i = 0; i < src.length; i++)
        queryVector[i] = (float) src[i];
      return queryVector;
    }

    if (key instanceof Object[] objArray && objArray.length > 0 && objArray[0] instanceof Number) {
      final float[] queryVector = new float[objArray.length];
      for (int i = 0; i < objArray.length; i++)
        queryVector[i] = ((Number) objArray[i]).floatValue();
      return queryVector;
    }

    // Key is a vertex identifier - fetch the vertex and get its vector
    final String keyStr = key.toString();
    final String typeName = lsmIndex.getTypeName();
    final String vectorProperty = lsmIndex.getPropertyNames().get(0);
    final String idProperty = lsmIndex.getIdPropertyName();

    try (final var rs = context.getDatabase().query("sql",
        "SELECT " + vectorProperty + " FROM " + typeName + " WHERE " + idProperty + " = ? LIMIT 1", keyStr)) {
      if (rs.hasNext()) {
        final float[] queryVector = rs.next().getProperty(vectorProperty);
        if (queryVector != null)
          return queryVector;
      }
    }

    throw new CommandSQLParsingException("Could not find vertex with key '" + keyStr + "' or extract vector");
  }
}
