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
package com.arcadedb.query.sql.function.vector;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.Pair;

import java.util.*;

/**
 * Returns the K neighbors from a vertex. This function requires a vector index has been created beforehand.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorNeighbors extends SQLFunctionVectorAbstract {
  public static final String NAME = "vectorNeighbors";

  public SQLFunctionVectorNeighbors() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 3)
      throw new CommandSQLParsingException(getSyntax());

    final String indexSpec = params[0].toString();

    Object key = params[1];
    if (key == null)
      throw new CommandSQLParsingException("key is null");

    if (key instanceof List<?> list)
      key = list.toArray();

    final int limit = params[2] instanceof Number n ? n.intValue() : Integer.parseInt(params[2].toString());

    // Parse the index specification: TYPE[property] or just index name
    final String specifiedTypeName;
    final String propertyName;
    final int bracketStart = indexSpec.indexOf('[');
    if (bracketStart > 0 && indexSpec.endsWith("]")) {
      specifiedTypeName = indexSpec.substring(0, bracketStart);
      propertyName = indexSpec.substring(bracketStart + 1, indexSpec.length() - 1);
    } else {
      // Assume it's just an index name
      final Index directIndex = context.getDatabase().getSchema().getIndexByName(indexSpec);
      if (directIndex instanceof TypeIndex typeIndex) {
        return executeWithTypeIndex(typeIndex, null, key, limit, context);
      }
      throw new CommandSQLParsingException(
          "Index '" + indexSpec + "' is not a vector index (found: " + (directIndex != null ? directIndex.getClass().getSimpleName() : "null") + ")");
    }

    // Get the specified type
    final DocumentType specifiedType = context.getDatabase().getSchema().getType(specifiedTypeName);

    // Try to find index directly on the specified type first
    TypeIndex typeIndex = specifiedType.getPolymorphicIndexByProperties(propertyName);

    if (typeIndex == null) {
      throw new CommandSQLParsingException(
          "No vector index found on property '" + propertyName + "' for type '" + specifiedTypeName + "' or its parent types");
    }

    // Get the bucket IDs that belong to the specified type (not polymorphic - just this type's own buckets)
    final Set<Integer> allowedBucketIds = new HashSet<>();
    for (final Bucket bucket : specifiedType.getBuckets(false)) {
      allowedBucketIds.add(bucket.getFileId());
    }

    return executeWithTypeIndex(typeIndex, allowedBucketIds, key, limit, context);
  }

  private Object executeWithTypeIndex(final TypeIndex typeIndex, final Set<Integer> allowedBucketIds, final Object key,
      final int limit, final CommandContext context) {
    final var bucketIndexes = typeIndex.getIndexesOnBuckets();
    if (bucketIndexes == null || bucketIndexes.length == 0) {
      throw new CommandSQLParsingException("Index '" + typeIndex.getName() + "' has no bucket indexes");
    }

    // Filter bucket indexes if a specific type was requested
    final List<LSMVectorIndex> vectorIndexes = new ArrayList<>();
    for (final IndexInternal bucketIndex : bucketIndexes) {
      if (bucketIndex instanceof LSMVectorIndex lsmIndex) {
        if (allowedBucketIds == null || allowedBucketIds.contains(bucketIndex.getAssociatedBucketId())) {
          vectorIndexes.add(lsmIndex);
        }
      }
    }

    if (vectorIndexes.isEmpty()) {
      if (allowedBucketIds != null) {
        throw new CommandSQLParsingException(
            "No vector index buckets found for the specified type. The index may have been created on a parent type.");
      }
      throw new CommandSQLParsingException(
          "Index '" + typeIndex.getName() + "' is not a vector index");
    }

    // Search across all matching vector indexes and merge results
    return executeWithLSMVectorIndexes(vectorIndexes, key, limit, context);
  }

  /**
   * Search across multiple vector indexes and merge the results.
   * This is used when searching within a specific type that may have multiple buckets.
   */
  private Object executeWithLSMVectorIndexes(final List<LSMVectorIndex> vectorIndexes, final Object key, final int limit,
      final CommandContext context) {
    // Get the query vector
    final float[] queryVector = extractQueryVector(key, vectorIndexes.getFirst(), context);

    // Search across all indexes and collect results
    final List<Pair<RID, Float>> allNeighbors = new ArrayList<>();

    for (final LSMVectorIndex lsmIndex : vectorIndexes) {
      // Request more results from each index to ensure we have enough after merging
      final List<Pair<RID, Float>> neighbors = lsmIndex.findNeighborsFromVector(queryVector, limit);
      allNeighbors.addAll(neighbors);
    }

    // Sort by distance (ascending - closer is better for similarity search)
    allNeighbors.sort(Comparator.comparing(Pair::getSecond));

    // Take top k results
    final int resultCount = Math.min(limit, allNeighbors.size());
    final ArrayList<Object> result = new ArrayList<>(resultCount);

    for (int i = 0; i < resultCount; i++) {
      final Pair<RID, Float> neighbor = allNeighbors.get(i);
      final RID rid = neighbor.getFirst();
      final Document record = rid.asDocument();
      final float distance = neighbor.getSecond();
      result.add(Map.of("record", record, "distance", distance));
    }

    return result;
  }

  /**
   * Extract query vector from the key parameter.
   */
  private float[] extractQueryVector(final Object key, final LSMVectorIndex lsmIndex, final CommandContext context) {
    // If key is already a vector, use it directly
    if (key instanceof float[] floatArray) {
      return floatArray;
    } else if (key instanceof Object[] objArray && objArray.length > 0 && objArray[0] instanceof Number) {
      // Convert array of numbers to float array
      final float[] queryVector = new float[objArray.length];
      for (int i = 0; i < objArray.length; i++) {
        queryVector[i] = ((Number) objArray[i]).floatValue();
      }
      return queryVector;
    } else {
      // Key is a vertex identifier - fetch the vertex and get its vector
      final String keyStr = key.toString();
      final String typeName = lsmIndex.getTypeName();
      final String vectorProperty = lsmIndex.getPropertyNames().getFirst();
      final String idProperty = lsmIndex.getIdPropertyName();

      // Query for the vertex by the configured ID property
      final ResultSet rs = context.getDatabase().query("sql",
          "SELECT " + vectorProperty + " FROM " + typeName + " WHERE " + idProperty + " = ? LIMIT 1", keyStr);

      float[] queryVector = null;
      if (rs.hasNext()) {
        final var result = rs.next();
        queryVector = result.getProperty(vectorProperty);
      }
      rs.close();

      if (queryVector == null) {
        throw new CommandSQLParsingException("Could not find vertex with key '" + keyStr + "' or extract vector");
      }
      return queryVector;
    }
  }

  public String getSyntax() {
    return "vectorNeighbors(<index-name>, <key-or-vector>, <k>)";
  }
}
