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
import com.arcadedb.index.vector.GroupAdmissionState;
import com.arcadedb.index.vector.GroupedTopUpPlanner;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.IntHashSet;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayList;
import java.util.Comparator;
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

  private static final Set<String> OPTIONS = Set.of("efSearch", "filter", "where", "groupBy", "groupSize", "maxDistance");

  // Hard cap on the candidate pool the index is asked to materialize when grouping is enabled.
  // Prevents memory exhaustion on pathological combinations of `k` and `groupSize` (e.g. k=1000,
  // groupSize=1000 would otherwise request 5,000,000 candidates). Shared with the stale top-up of every k-NN entry point.
  private static final int MAX_FETCH_CANDIDATES = VectorUtils.MAX_FETCH_CANDIDATES;

  public SQLFunctionVectorNeighbors() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 3 || params.length > 4)
      throw new CommandSQLParsingException(getSyntax());

    if (params[0] == null)
      throw new CommandSQLParsingException("index name is null");

    final String indexSpec = params[0].toString();

    Object key = params[1];
    if (key == null)
      throw new CommandSQLParsingException("key is null");

    if (key instanceof List<?> list)
      key = list.toArray();

    final int limit = params[2] instanceof Number n ? n.intValue() : Integer.parseInt(params[2].toString());
    if (limit <= 0)
      return new ArrayList<>(0);

    // Optional 4th parameter. Either:
    //   - a number: efSearch (backward-compatible positional form)
    //   - a map: options {efSearch, filter, where, groupBy, groupSize, maxDistance}
    int efSearch = -1;
    Set<RID> allowedRIDs = null;
    String groupBy = null;
    int groupSize = 1;
    float maxDistance = Float.POSITIVE_INFINITY;

    if (params.length >= 4 && params[3] != null) {
      if (params[3] instanceof Map<?, ?> rawMap) {
        final FunctionOptions opts = new FunctionOptions(NAME, rawMap, OPTIONS);
        efSearch = opts.getInt("efSearch", -1);
        // `where` restricts the candidates by a predicate on their record, tested as the search meets them instead of
        // listing every RID in scope up front as a `filter` subquery has to (issue #10169)
        allowedRIDs = RecordPredicateRidSet.of(opts.get("where"), parseRidFilter(opts.getList("filter"), NAME, context), NAME,
            context);
        groupBy = opts.getString("groupBy", null);
        groupSize = opts.getInt("groupSize", 1);
        if (groupSize < 1)
          throw new CommandSQLParsingException(NAME + " groupSize must be >= 1, got " + groupSize);
        // Range-search gate (Tier 4 follow-up). Drop neighbors whose distance is strictly greater
        // than {@code maxDistance}; the result set may legitimately be smaller than {@code limit}
        // when the threshold is tight, which is the documented contract of a range-style query.
        // The check is a post-filter on top of the regular HNSW top-K - JVector's HNSW does not
        // expose a native range mode, so we still pay for the K candidates the index returns;
        // setting a generous {@code limit} alongside a tight {@code maxDistance} matches the
        // intent (radius search bounded by an outer cap).
        maxDistance = (float) opts.getDouble("maxDistance", Double.POSITIVE_INFINITY);
        if (Float.isNaN(maxDistance))
          throw new CommandSQLParsingException(NAME + " maxDistance must be a number, not NaN");
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
        return executeWithTypeIndex(typeIndex, null, key, limit, efSearch, allowedRIDs, groupBy, groupSize, maxDistance, context);
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

    // Get the bucket IDs that belong to the specified type and of its sub-types (issue #8958): the parent spec is the cross-type search.
    // IntHashSet is zero-boxing - the contains() in executeWithTypeIndex runs once per bucket index per
    // vector query, and avoiding Integer boxing on every probe matters under sustained ANN workloads.
    IntHashSet allowedBucketIds = new IntHashSet();
    for (final Bucket bucket : specifiedType.getBuckets(true)) {
      allowedBucketIds.add(bucket.getFileId());
    }

    // Narrow the per-type bucket allow-list to the partition-pruned subset when the outer SELECT
    // already restricted to specific buckets via a WHERE on the partition key (issue #4087
    // follow-up). The function would otherwise enumerate every bucket sub-index even though the
    // outer query is logically constrained to one bucket; the intersection skips work AND keeps
    // results consistent with the WHERE.
    allowedBucketIds = narrowAllowedBucketIdsByPartitionHint(allowedBucketIds, specifiedTypeName, context);

    return executeWithTypeIndex(typeIndex, allowedBucketIds, key, limit, efSearch, allowedRIDs, groupBy, groupSize, maxDistance, context);
  }

  private Object executeWithTypeIndex(final TypeIndex typeIndex, final IntHashSet allowedBucketIds, final Object key,
      final int limit, final int efSearch, final Set<RID> allowedRIDs, final String groupBy, final int groupSize,
      final float maxDistance, final CommandContext context) {
    final var bucketIndexes = typeIndex.getIndexesOnBuckets();
    if (bucketIndexes == null || bucketIndexes.length == 0) {
      throw new CommandSQLParsingException("Index '" + typeIndex.getName() + "' has no bucket indexes");
    }

    // Filter bucket indexes if a specific type was requested, and visit each sub-index at most once. See
    // VectorUtils.collectSubIndexesOnce for why the same sub-index can be listed twice (issue #7057).
    final List<LSMVectorIndex> vectorIndexes = VectorUtils.collectSubIndexesOnce(bucketIndexes, LSMVectorIndex.class,
        allowedBucketIds == null ? null : allowedBucketIds::contains);

    if (vectorIndexes.isEmpty()) {
      if (allowedBucketIds != null) {
        throw new CommandSQLParsingException(
            "No vector index buckets found for the specified type. The index may have been created on a parent type.");
      }
      throw new CommandSQLParsingException(
          "Index '" + typeIndex.getName() + "' is not a vector index");
    }

    // Checked after the index is resolved, so a bad index spec stays loud. A filter that matched nothing is not "no filter":
    // no record can satisfy it (issue #8959)
    if (allowedRIDs != null && allowedRIDs.isEmpty())
      return new ArrayList<>(0);

    // Search across all matching vector indexes and merge results
    return executeWithLSMVectorIndexes(vectorIndexes, key, limit, efSearch, allowedRIDs, groupBy, groupSize, maxDistance, context);
  }

  /**
   * Search across multiple vector indexes and merge the results.
   * This is used when searching within a specific type that may have multiple buckets.
   * <p>
   * When {@code groupBy} is set, each index applies the {@code limit} / {@code groupSize} cap to its own
   * score-ordered search output, resuming the graph walk until it has {@code limit} distinct groups or runs
   * out of candidate budget (#5761), and the {@link GroupAdmissionState} below re-applies the same cap once
   * across every index so the caller never sees a {@code (limit + 1)}-th group. In between, a {@link GroupedTopUpPlanner}
   * asks an index that ranked an overall winner outside its own top {@code limit} for that winner's members, which it
   * would otherwise never have returned (issue #8002). Still best-effort: an index
   * whose nearest group is denser than its candidate budget returns fewer groups and counts the query in
   * {@code groupedSearchesShortOfLimit}, which is the signal to raise the index's {@code efSearch}. A short answer
   * for the other reason - the corpus or the allow-list simply has fewer than {@code limit} distinct groups to give,
   * which no beam width can change - counts in {@code groupedSearchesGroupsUnavailable} instead (issue #6559), so
   * the {@code efSearch} signal stays actionable rather than firing on every query over low-cardinality data.
   * <p>
   * Adding {@code groupBy} to a query changes how its rows are capped and nothing else about what the index can see:
   * both branches below are answered from the graph <em>and</em> the delta buffer of vectors written since it was
   * built. That is worth stating because it was not true until issue #6501 - the grouped branch skipped the buffer,
   * so the same query, index and instant returned recent writes without {@code groupBy} and hid them with it. Any
   * future divergence between the two branches on what is visible is a defect, not a tradeoff of grouping.
   * <p>
   * <b>Parameter sprawl</b> (8 positional args). Each new option that lands here adds another
   * parameter. The next addition should refactor to a {@code VectorSearchOptions} record - keeping
   * it positional for now to minimise the blast radius of the maxDistance / Tier 4 follow-ups.
   */
  private Object executeWithLSMVectorIndexes(final List<LSMVectorIndex> vectorIndexes, final Object key, final int limit,
      final int efSearch, final Set<RID> allowedRIDs, final String groupBy, final int groupSize,
      final float maxDistance, final CommandContext context) {
    // Get the query vector
    final float[] queryVector = extractQueryVector(key, vectorIndexes.getFirst(), context);

    // One bound for every pass and every index searched: the command's deadline and the termination of its statement,
    // honoured inside each search and while it waits for a graph build (issue #9896)
    final WorkGuard guard = WorkGuard.forCommand(context, NAME + "()");

    // Memory-budget guard for the grouped path (issue #4071): the per-group accounting is O(limit * groupSize)
    // per bucket, but we still reject pathological combinations (e.g. limit=10000, groupSize=10000 = 100M
    // candidates) so the search loop does not allocate arrays in the GiB range. Same shape (and exception text)
    // as the MVP cap so existing tests probing the boundary remain valid.
    if (groupBy != null && groupedFetchBudget(limit, groupSize) > MAX_FETCH_CANDIDATES)
      throw new CommandSQLParsingException(NAME + " over-fetch budget exceeded: limit=" + limit
          + ", groupSize=" + groupSize + " would require " + groupedFetchBudget(limit, groupSize)
          + " candidates (cap " + MAX_FETCH_CANDIDATES + "). Reduce limit or groupSize.");

    // A record deleted by a commit that overlaps the search can still be in the index's answer for a short while. The
    // non-grouped path asks for exactly `limit` candidates, so each such record would cost the caller a row (#9368).
    // When a stale candidate was skipped, the answer is short, and the index could have given more (a search that
    // returned fewer candidates than asked has nothing left), ask again with a wider fetch so a stale entry costs a
    // candidate and not an answer row. Bounded by MAX_FETCH_CANDIDATES.
    //
    // The grouped path has the same hole one level up (issue #9697), where `limit` is a group budget: each pass re-runs
    // the per-index grouped search and the #8002 second phase - see searchGroupedWithStaleTopUp.
    // The grouped top-up reads the stale RIDs of a pass, not their count, so the pass's counter is a scratch one
    if (groupBy != null)
      return searchGroupedWithStaleTopUp(limit, groupSize, MAX_FETCH_CANDIDATES,
          (fetchGroups, fetchGroupSize, staleRids) -> searchAndEmit(vectorIndexes, queryVector, limit, fetchGroups, efSearch,
              allowedRIDs, groupBy, fetchGroupSize, groupSize, maxDistance, context, guard, new int[1], staleRids, null));

    return VectorUtils.searchWithStaleTopUp(limit,
        (fetchK, staleSkipped, saturated) -> searchAndEmit(vectorIndexes, queryVector, limit, fetchK, efSearch, allowedRIDs,
            null, groupSize, groupSize, maxDistance, context, guard, staleSkipped, null, saturated));
  }

  /**
   * One search and emit pass. {@code fetchK} and {@code fetchGroupSize} size what the indexes are asked for: candidates
   * on the plain path, the group budget and the per-group cap on the grouped one. The answer is always capped at the
   * caller's own {@code limit} rows, or {@code limit} groups of {@code groupSize}. {@code saturated} is only tracked,
   * and only non-null, on the plain path.
   * {@code staleRids} is only non-null on the grouped path, where it receives every stale candidate the emit skips, so
   * the top-up can tell a candidate deleted since the last pass from one it already met (issue #10002).
   */
  private ArrayList<Object> searchAndEmit(final List<LSMVectorIndex> vectorIndexes, final float[] queryVector, final int limit,
      final int fetchK, final int efSearch, final Set<RID> allowedRIDs, final String groupBy, final int fetchGroupSize,
      final int groupSize, final float maxDistance, final CommandContext context, final WorkGuard guard,
      final int[] staleSkipped, final Set<RID> staleRids, final boolean[] saturated) {

    final BasicDatabase db = context.getDatabase();

    // One resolver for every search of this pass and for the emit below, so they all group a candidate the same way
    // and a record that changed under the search is caught at emit (issue #10002) - see GroupKeyMemo.
    final GroupKeyMemo groupKeyResolver = groupBy == null ? null : new GroupKeyMemo(db, groupBy);

    // Per-bucket fetch cap. The non-grouped path asks each bucket for {@code fetchK} candidates; the grouped path uses
    // {@code fetchK} as the distinct-group budget on each bucket (with {@code fetchGroupSize} members each) and relies on
    // the SQL-layer global GroupAdmissionState to reduce cross-bucket overlap.
    final List<Pair<RID, Float>> allNeighbors = new ArrayList<>();

    final GroupedTopUpPlanner planner =
        groupBy != null && vectorIndexes.size() > 1 ? new GroupedTopUpPlanner(fetchK, fetchGroupSize) : null;
    for (final LSMVectorIndex lsmIndex : vectorIndexes) {
      final List<Pair<RID, Float>> neighbors;
      if (groupBy == null)
        neighbors = lsmIndex.findNeighborsFromVector(queryVector, fetchK, efSearch, allowedRIDs, guard);
      else
        neighbors = lsmIndex.findNeighborsFromVectorGrouped(queryVector, fetchK, fetchGroupSize, efSearch, allowedRIDs,
            groupKeyResolver, guard);
      allNeighbors.addAll(neighbors);
      if (saturated != null && neighbors.size() >= fetchK)
        saturated[0] = true;
      if (planner != null) {
        final List<RID> rids = new ArrayList<>(neighbors.size());
        final float[] values = new float[neighbors.size()];
        final List<Object> keys = new ArrayList<>(neighbors.size());
        for (int i = 0; i < neighbors.size(); i++) {
          final RID rid = neighbors.get(i).getFirst();
          rids.add(rid);
          values[i] = -neighbors.get(i).getSecond(); // the planner ranks higher-is-better
          keys.add(groupKeyResolver.apply(rid));
        }
        planner.addSource(rids, values, keys, true);
      }
    }

    // Second phase over several indexes (issue #8002). Each index answered with ITS nearest `limit` groups, which
    // settles the winners overall but not their members: an index that ranked a winner outside its own top `limit` may
    // still hold members of it, and never returned them. The planner names the indexes that may, for which winners,
    // and beyond what distance nothing they hold could matter; it asks none in the common case.
    if (planner != null)
      for (final GroupedTopUpPlanner.TopUp topUp : planner.plan())
        allNeighbors.addAll(vectorIndexes.get(topUp.source())
            .findNeighborsFromVectorForGroups(queryVector, topUp.groupKeys(), fetchGroupSize, efSearch, allowedRIDs,
                groupKeyResolver, -topUp.floor(), guard));

    // Sort by distance (ascending - closer is better for similarity search)
    allNeighbors.sort(Comparator.comparing(Pair::getSecond));

    final ArrayList<Object> result = new ArrayList<>();
    final GroupAdmissionState groups = groupBy != null ? new GroupAdmissionState(limit, groupSize) : null;
    // One record, one row. This list is the concatenation of one search per index, so a record offered by more than
    // one of them would otherwise take two of the caller's `limit` slots - and, under groupBy, two slots of its own
    // group's cap (issue #7057). The list is already sorted, so the sighting kept is the nearest one.
    //
    // Sized from the candidates actually fetched, never from `limit`. On the non-grouped path `limit` is the raw
    // caller-supplied k, checked only for `<= 0` - the MAX_FETCH_CANDIDATES budget above guards the grouped path
    // alone. RidHashSet allocates an int[] and a long[] of the next power of two up front, so sizing off k would
    // let `vector.neighbors(idx, v, 100000000)` claim ~1.5GB before looking at a single record, and a k near
    // Integer.MAX_VALUE would overflow the rounding to a negative capacity and throw NegativeArraySizeException.
    // That is the hazard issue #5924 already closed one layer down, where findNeighborsFromVector clamps k against
    // the real candidate count; allNeighbors.size() is that clamp having already happened, per index and summed.
    final RidHashSet emitted = new RidHashSet(Math.min(limit, allNeighbors.size()));

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

      // Range gate. allNeighbors is already sorted by distance ascending, so once we observe the
      // first neighbor whose distance exceeds maxDistance, every following one will too - break
      // the whole loop instead of just continuing.
      if (neighbor.getSecond() > maxDistance)
        break;

      final RID rid = neighbor.getFirst();
      if (!emitted.add(rid))
        continue;

      final Document record;
      try {
        record = (Document) db.lookupByRID(rid, true);
      } catch (final RecordNotFoundException e) {
        // Skip records that no longer exist in the bucket (issue #3717).
        // This can happen when the vector index has stale entries pointing to deleted records,
        // e.g., after crash recovery, backup restore, or index/storage inconsistencies.
        staleSkipped[0]++;
        if (staleRids != null)
          staleRids.add(rid);
        continue;
      }

      if (groupBy != null) {
        final Object groupKey = readNestedField(record, groupBy);
        // A commit since the search ranked it updated the field, or gave the RID to a record it inserted (issue
        // #10002): the row would open a group the searches never filled, so it costs a candidate, as a deleted one does
        if (!groupKeyResolver.rankedUnder(rid, groupKey)) {
          staleSkipped[0]++;
          if (staleRids != null)
            staleRids.add(rid);
          continue;
        }
        if (!groups.admit(groupKey))
          continue;
      }

      final float distance = neighbor.getSecond();

      // The row exposes the record's properties so they are accessible in outer queries after expand() (e.g., SELECT
      // name, distance FROM (SELECT expand(...))), reading them off the record on demand (issue #10129)
      result.add(new ScoredRecordRow(record, "distance", distance));
    }

    return result;
  }

  /**
   * Extract query vector from the key parameter. Accepts {@code float[]}, {@code byte[]}
   * (INT8-encoded, dequantized via {@link VectorUtils#dequantizeInt8ToFloat}), {@code double[]},
   * arrays/lists of numbers, or a vertex-id string whose stored vector property is read back and
   * converted - the property may itself be {@code byte[]} when the index uses INT8 encoding.
   */
  private float[] extractQueryVector(final Object key, final LSMVectorIndex lsmIndex, final CommandContext context) {
    // Vertex-id lookup path: if the key is a string and not a literal vector, fetch the stored vector property.
    if (key instanceof String keyStr) {
      final String typeName = lsmIndex.getTypeName();
      final String vectorProperty = lsmIndex.getPropertyNames().getFirst();
      final String idProperty = lsmIndex.getIdPropertyName();

      Object stored = null;
      // Shared with db.index.vector.queryNodes, the Cypher entry point onto this same lookup (issue #8097).
      try (final ResultSet rs = context.getDatabase().query("sql",
          buildVectorByIdLookup(typeName, vectorProperty, idProperty), keyStr)) {
        if (rs.hasNext()) {
          final var result = rs.next();
          stored = result.getProperty(vectorProperty);
        }
      }

      if (stored == null)
        throw new CommandSQLParsingException("Could not find vertex with key '" + keyStr + "' or extract vector");

      // Encoding-aware: a byte[] property is only valid when the index is INT8-encoded.
      try {
        return VectorUtils.toFloatArray(stored, lsmIndex.getMetadata().encoding);
      } catch (final IllegalArgumentException e) {
        throw new CommandSQLParsingException("Could not extract vector for key '" + keyStr + "': " + e.getMessage());
      }
    }

    // Direct query vector. Allow byte[] as a query when the target index uses INT8 encoding.
    try {
      return VectorUtils.toFloatArray(key, lsmIndex.getMetadata().encoding);
    } catch (final IllegalArgumentException e) {
      throw new CommandSQLParsingException("Unsupported query vector type: " + key.getClass().getSimpleName());
    }
  }

  public String getSyntax() {
    return NAME + "(<index-name>, <key-or-vector>, <k>[, <efSearch> | "
        + "{ efSearch: <int>, filter: [<rid>, ...], where: { <property>: <value>, ... } | '<condition>', groupBy: <field>, "
        + "groupSize: <int>, maxDistance: <float> }])";
  }
}
