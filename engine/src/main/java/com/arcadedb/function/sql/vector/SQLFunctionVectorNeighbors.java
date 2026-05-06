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
package com.arcadedb.function.sql.vector;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.function.sql.FunctionOptions;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.IntHashSet;
import com.arcadedb.utility.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Returns the K neighbors from a vertex. This function requires a vector index has been created beforehand.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorNeighbors extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.neighbors";

  private static final Set<String> OPTIONS = Set.of("efSearch", "filter", "groupBy", "groupSize");

  // Hard cap on the candidate pool the index is asked to materialize when grouping is enabled.
  // Prevents memory exhaustion on pathological combinations of `k` and `groupSize` (e.g. k=1000,
  // groupSize=1000 would otherwise request 5,000,000 candidates). Picked conservatively: ~100k
  // RID-distance pairs is a few MB of heap, well under the budget for any realistic query.
  private static final int MAX_FETCH_CANDIDATES = 100_000;

  public SQLFunctionVectorNeighbors() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 3 || params.length > 4)
      throw new CommandSQLParsingException(getSyntax());

    final String indexSpec = params[0].toString();

    Object key = params[1];
    if (key == null)
      throw new CommandSQLParsingException("key is null");

    if (key instanceof List<?> list)
      key = list.toArray();

    final int limit = params[2] instanceof Number n ? n.intValue() : Integer.parseInt(params[2].toString());

    // Optional 4th parameter. Either:
    //   - a number: efSearch (backward-compatible positional form)
    //   - a map: options {efSearch, filter, groupBy, groupSize}
    int efSearch = -1;
    Set<RID> allowedRIDs = null;
    String groupBy = null;
    int groupSize = 1;

    if (params.length >= 4 && params[3] != null) {
      if (params[3] instanceof Map<?, ?> rawMap) {
        final FunctionOptions opts = new FunctionOptions(NAME, rawMap, OPTIONS);
        efSearch = opts.getInt("efSearch", -1);
        allowedRIDs = parseRidFilter(opts.getList("filter"), NAME, context);
        groupBy = opts.getString("groupBy", null);
        groupSize = opts.getInt("groupSize", 1);
        if (groupSize < 1)
          throw new CommandSQLParsingException(NAME + " groupSize must be >= 1, got " + groupSize);
      } else if (params[3] instanceof Number n) {
        efSearch = n.intValue();
      } else {
        efSearch = Integer.parseInt(params[3].toString());
      }
    }

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
        return executeWithTypeIndex(typeIndex, null, key, limit, efSearch, allowedRIDs, groupBy, groupSize, context);
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

    // Get the bucket IDs that belong to the specified type (not polymorphic - just this type's own buckets).
    // IntHashSet is zero-boxing - the contains() in executeWithTypeIndex runs once per bucket index per
    // vector query, and avoiding Integer boxing on every probe matters under sustained ANN workloads.
    IntHashSet allowedBucketIds = new IntHashSet();
    for (final Bucket bucket : specifiedType.getBuckets(false)) {
      allowedBucketIds.add(bucket.getFileId());
    }

    // Narrow the per-type bucket allow-list to the partition-pruned subset when the outer SELECT
    // already restricted to specific buckets via a WHERE on the partition key (issue #4087
    // follow-up). The function would otherwise enumerate every bucket sub-index even though the
    // outer query is logically constrained to one bucket; the intersection skips work AND keeps
    // results consistent with the WHERE.
    allowedBucketIds = narrowAllowedBucketIdsByPartitionHint(allowedBucketIds, specifiedTypeName, context);

    return executeWithTypeIndex(typeIndex, allowedBucketIds, key, limit, efSearch, allowedRIDs, groupBy, groupSize, context);
  }

  private Object executeWithTypeIndex(final TypeIndex typeIndex, final IntHashSet allowedBucketIds, final Object key,
      final int limit, final int efSearch, final Set<RID> allowedRIDs, final String groupBy, final int groupSize,
      final CommandContext context) {
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
    return executeWithLSMVectorIndexes(vectorIndexes, key, limit, efSearch, allowedRIDs, groupBy, groupSize, context);
  }

  /**
   * Search across multiple vector indexes and merge the results.
   * This is used when searching within a specific type that may have multiple buckets.
   * <p>
   * When {@code groupBy} is set, the index is asked for an over-fetched candidate pool and the
   * top-{@code limit} <em>groups</em> are returned, each capped at {@code groupSize} rows. Best-effort:
   * fewer groups may be returned if the candidate pool runs out before {@code limit} groups are filled,
   * in which case raising the index's {@code efSearch} option improves coverage.
   */
  private Object executeWithLSMVectorIndexes(final List<LSMVectorIndex> vectorIndexes, final Object key, final int limit,
      final int efSearch, final Set<RID> allowedRIDs, final String groupBy, final int groupSize, final CommandContext context) {
    // Get the query vector
    final float[] queryVector = extractQueryVector(key, vectorIndexes.getFirst(), context);

    // When grouping, over-fetch candidates so the post-traversal filter has enough material to
    // fill limit groups at groupSize each. The 5x cushion is empirical; highly skewed group
    // distributions may need more, in which case users can lift recall via efSearch.
    //
    // Hard-cap the fetch at MAX_FETCH_CANDIDATES so a pathological combination of `limit` and
    // `groupSize` cannot blow up the candidate pool. The product is computed in long arithmetic
    // first to detect overflow before truncating to int.
    final int fetchPerIndex;
    if (groupBy == null) {
      fetchPerIndex = limit;
    } else {
      final long requested = Math.max((long) limit * groupSize * 5L, (long) limit);
      if (requested > MAX_FETCH_CANDIDATES)
        throw new CommandSQLParsingException(NAME + " over-fetch budget exceeded: limit=" + limit
            + ", groupSize=" + groupSize + " would require " + requested
            + " candidates (cap " + MAX_FETCH_CANDIDATES + "). Reduce limit or groupSize.");
      fetchPerIndex = (int) requested;
    }

    // Search across all indexes and collect results
    final List<Pair<RID, Float>> allNeighbors = new ArrayList<>();

    for (final LSMVectorIndex lsmIndex : vectorIndexes) {
      final List<Pair<RID, Float>> neighbors = lsmIndex.findNeighborsFromVector(queryVector, fetchPerIndex, efSearch, allowedRIDs);
      allNeighbors.addAll(neighbors);
    }

    // Sort by distance (ascending - closer is better for similarity search)
    allNeighbors.sort(Comparator.comparing(Pair::getSecond));

    final BasicDatabase db = context.getDatabase();
    final ArrayList<Object> result = new ArrayList<>();
    final GroupAdmissionState groups = groupBy != null ? new GroupAdmissionState(limit, groupSize) : null;

    for (final Pair<RID, Float> neighbor : allNeighbors) {
      // Stop conditions:
      //   - groupBy unset: stop once we collected `limit` rows
      //   - groupBy set: stop once `limit` groups have been filled to groupSize
      if (groupBy == null) {
        if (result.size() >= limit)
          break;
      } else if (groups.isFull()) {
        break;
      }

      final RID rid = neighbor.getFirst();
      final Document record;
      try {
        record = (Document) db.lookupByRID(rid, true);
      } catch (final RecordNotFoundException e) {
        // Skip records that no longer exist in the bucket (issue #3717).
        // This can happen when the vector index has stale entries pointing to deleted records,
        // e.g., after crash recovery, backup restore, or index/storage inconsistencies.
        continue;
      }

      if (groupBy != null && !groups.admit(readNestedField(record, groupBy)))
        continue;

      final float distance = neighbor.getSecond();

      // Flatten document properties into the result map so they are accessible
      // in outer queries after expand() (e.g., SELECT name, distance FROM (SELECT expand(...)))
      final LinkedHashMap<String, Object> entry = new LinkedHashMap<>();
      entry.put("record", record); // backward compatibility
      for (final String prop : record.getPropertyNames())
        entry.put(prop, record.get(prop));
      entry.put("@rid", record.getIdentity());
      entry.put("@type", record.getTypeName());
      entry.put("distance", distance);
      result.add(entry);
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
    return NAME + "(<index-name>, <key-or-vector>, <k>[, <efSearch> | "
        + "{ efSearch: <int>, filter: [<rid>, ...], groupBy: <field>, groupSize: <int> }])";
  }
}
