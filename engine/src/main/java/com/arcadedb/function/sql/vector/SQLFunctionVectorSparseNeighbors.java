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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.function.sql.FunctionOptions;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.index.sparsevector.RidScore;
import com.arcadedb.index.sparsevector.SparseVectorScoringPool;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.IntHashSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Returns the top-K nearest records by sparse-vector dot product against a {@code LSM_SPARSE_VECTOR}
 * index.
 * <p>
 * Usage: {@code vector.sparseNeighbors(indexSpec, queryIndices, queryValues, k[, options])}
 * <p>
 * {@code indexSpec} is either a fully qualified index name or a {@code Type[property1,property2]}
 * string. {@code queryIndices} is an {@code int[]} of non-negative dimension ids and
 * {@code queryValues} is the matching {@code float[]}.
 * <p>
 * Optional 5th argument: a map with keys {@code filter} (List of allowed RIDs).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionVectorSparseNeighbors extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.sparseNeighbors";

  private static final Set<String> OPTIONS = Set.of("filter", "groupBy", "groupSize", "minScore");

  // Hard cap on the candidate pool when grouping is enabled. Same rationale as
  // SQLFunctionVectorNeighbors.MAX_FETCH_CANDIDATES: bounds memory at a few MB on pathological
  // (k, groupSize) combinations.
  private static final int MAX_FETCH_CANDIDATES = 100_000;

  public SQLFunctionVectorSparseNeighbors() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 4 || params.length > 5)
      throw new CommandSQLParsingException(getSyntax());

    final String indexSpec = params[0].toString();

    final int[]   queryIndices = sparseToIntArray(params[1]);
    final float[] queryValues  = sparseToFloatArray(params[2]);
    if (queryIndices == null || queryValues == null)
      throw new CommandSQLParsingException("Query indices and values must not be null");
    if (queryIndices.length != queryValues.length)
      throw new CommandSQLParsingException(
          "Query indices and values must have the same length (got " + queryIndices.length + " and " + queryValues.length + ")");

    final int k = params[3] instanceof Number n ? n.intValue() : Integer.parseInt(params[3].toString());

    Set<RID> allowedRIDs = null;
    String groupBy = null;
    int groupSize = 1;
    float minScore = Float.NEGATIVE_INFINITY;
    if (params.length >= 5 && params[4] != null) {
      if (params[4] instanceof Map<?, ?> rawMap) {
        final FunctionOptions opts = new FunctionOptions(NAME, rawMap, OPTIONS);
        allowedRIDs = parseRidFilter(opts.getList("filter"), NAME, context);
        groupBy = opts.getString("groupBy", null);
        groupSize = opts.getInt("groupSize", 1);
        if (groupSize < 1)
          throw new CommandSQLParsingException(NAME + " groupSize must be >= 1, got " + groupSize);
        // Range gate (Tier 4 follow-up). Drop neighbors whose score (BM25 / dot product / IDF
        // weighted sum, higher is better) falls below {@code minScore}. The result set may be
        // smaller than {@code k} when the threshold is tight; that is the documented contract of
        // a range query. Post-filter on the merged BMW DAAT result.
        minScore = (float) opts.getDouble("minScore", Double.NEGATIVE_INFINITY);
        if (Float.isNaN(minScore))
          throw new CommandSQLParsingException(NAME + " minScore must be a number, not NaN");
      } else {
        throw new CommandSQLParsingException(NAME + " 5th parameter must be an options map, got: " + params[4]);
      }
    }

    final TypeIndex typeIndex = resolveTypeIndex(indexSpec, context);
    final List<LSMSparseVectorIndex> sparseIndexes = collectSparseIndexes(indexSpec, typeIndex, context);

    if (sparseIndexes.isEmpty())
      throw new CommandSQLParsingException(
          "Index '" + indexSpec + "' is not a sparse vector index");

    return executeWithIndexes(sparseIndexes, queryIndices, queryValues, k, allowedRIDs, groupBy, groupSize, minScore, context);
  }

  private TypeIndex resolveTypeIndex(final String indexSpec, final CommandContext context) {
    final int bracketStart = indexSpec.indexOf('[');
    if (bracketStart > 0 && indexSpec.endsWith("]")) {
      final String specifiedTypeName = indexSpec.substring(0, bracketStart);
      final String propertySpec = indexSpec.substring(bracketStart + 1, indexSpec.length() - 1);
      final String[] properties = splitPropertyList(propertySpec);
      final DocumentType specifiedType = context.getDatabase().getSchema().getType(specifiedTypeName);
      final TypeIndex idx = specifiedType.getPolymorphicIndexByProperties(properties);
      if (idx == null)
        throw new CommandSQLParsingException(
            "No sparse vector index found on properties '" + propertySpec + "' for type '" + specifiedTypeName + "'");
      return idx;
    }

    final var direct = context.getDatabase().getSchema().getIndexByName(indexSpec);
    if (direct instanceof TypeIndex typeIndex)
      return typeIndex;

    throw new CommandSQLParsingException(
        "Index '" + indexSpec + "' is not a sparse vector index (found: " + (direct != null ? direct.getClass().getSimpleName() : "null") + ")");
  }

  private static String[] splitPropertyList(final String propertySpec) {
    final String[] raw = propertySpec.split(",");
    final String[] out = new String[raw.length];
    for (int i = 0; i < raw.length; i++)
      out[i] = raw[i].trim();
    return out;
  }

  private List<LSMSparseVectorIndex> collectSparseIndexes(final String indexSpec, final TypeIndex typeIndex,
      final CommandContext context) {
    IntHashSet allowedBucketIds = new IntHashSet();
    String specifiedTypeName = null;
    final int bracketStart = indexSpec.indexOf('[');
    if (bracketStart > 0 && indexSpec.endsWith("]")) {
      specifiedTypeName = indexSpec.substring(0, bracketStart);
      final DocumentType specifiedType = context.getDatabase().getSchema().getType(specifiedTypeName);
      for (final Bucket bucket : specifiedType.getBuckets(false))
        allowedBucketIds.add(bucket.getFileId());
    }

    // Narrow to the partition-pruned subset when the outer SELECT bound every partition property
    // (issue #4087 follow-up). Same rationale as {@link SQLFunctionVectorNeighbors}: skips per
    // bucket sparse indexes outside the partition AND keeps results consistent with the WHERE.
    if (specifiedTypeName != null)
      allowedBucketIds = narrowAllowedBucketIdsByPartitionHint(allowedBucketIds, specifiedTypeName, context);

    final var bucketIndexes = typeIndex.getIndexesOnBuckets();
    final List<LSMSparseVectorIndex> result = new ArrayList<>();
    if (bucketIndexes == null)
      return result;

    for (final IndexInternal bucketIndex : bucketIndexes) {
      if (bucketIndex instanceof LSMSparseVectorIndex sparseIndex) {
        if (allowedBucketIds.isEmpty() || allowedBucketIds.contains(bucketIndex.getAssociatedBucketId()))
          result.add(sparseIndex);
      }
    }
    return result;
  }

  private Object executeWithIndexes(final List<LSMSparseVectorIndex> indexes, final int[] queryIndices,
      final float[] queryValues, final int k, final Set<RID> allowedRIDs, final String groupBy, final int groupSize,
      final float minScore, final CommandContext context) {

    // Over-fetch when grouping so the post-traversal filter has enough material to fill k groups
    // at groupSize each. Same 5x cushion and MAX_FETCH_CANDIDATES cap as `vector.neighbors`.
    final int fetchK;
    if (groupBy == null) {
      fetchK = k;
    } else {
      final long requested = Math.max((long) k * groupSize * 5L, (long) k);
      if (requested > MAX_FETCH_CANDIDATES)
        throw new CommandSQLParsingException(NAME + " over-fetch budget exceeded: k=" + k
            + ", groupSize=" + groupSize + " would require " + requested
            + " candidates (cap " + MAX_FETCH_CANDIDATES + "). Reduce k or groupSize.");
      fetchK = (int) requested;
    }

    final ArrayList<RidScore> merged = new ArrayList<>();
    if (indexes.size() <= 1) {
      // Single bucket: no parallelism opportunity. Skip the pool dispatch overhead and run the
      // topK on the calling thread.
      for (final LSMSparseVectorIndex idx : indexes)
        merged.addAll(idx.topK(queryIndices, queryValues, fetchK, allowedRIDs));
    } else {
      // Multi-bucket fan-out (#4085). Per-bucket sub-indexes are independent: different buckets
      // contain disjoint RID ranges, so per-bucket top-K calls have no shared mutable state and
      // need no coordination. Submit each call to the dedicated SparseVectorScoringPool and gather
      // results. The pool's CallerRuns rejection policy guarantees the call always completes -
      // worst case it runs inline on the submitter thread, which is exactly the serial fallback.
      final ExecutorService pool = SparseVectorScoringPool.getInstance().getExecutorService();
      final List<Future<List<RidScore>>> futures = new ArrayList<>(indexes.size());
      for (final LSMSparseVectorIndex idx : indexes)
        futures.add(pool.submit(() -> idx.topK(queryIndices, queryValues, fetchK, allowedRIDs)));
      // Drain ALL futures even when one fails: a partial drain leaves the still-running tasks
      // contending for index I/O after the caller has moved on. Collect the per-future errors,
      // attach the rest as suppressed, then throw the first. On interrupt, cancel outstanding work
      // so we do not pay for compute we will never observe.
      // <p>
      // Single deadline across the whole fan-out: caps the total wall-clock at
      // SPARSE_VECTOR_SCORING_TIMEOUT_SECONDS regardless of bucket count. A per-future timeout
      // would let N wedged buckets accumulate up to N * timeoutSeconds before the caller sees an
      // error (e.g. 16 buckets * 30s = 8-minute hang for a deadlocked compaction). The deadline
      // approach keeps the worst case at a single timeoutSeconds. timeoutSeconds <= 0 disables
      // the deadline entirely (untimed gets); not recommended in production.
      final int timeoutSeconds = GlobalConfiguration.SPARSE_VECTOR_SCORING_TIMEOUT_SECONDS.getValueAsInteger();
      final long deadlineNs = timeoutSeconds > 0
          ? System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds)
          : Long.MAX_VALUE;
      final List<Throwable> errors = new ArrayList<>();
      for (final Future<List<RidScore>> f : futures) {
        try {
          final List<RidScore> partial;
          if (timeoutSeconds <= 0) {
            partial = f.get();
          } else {
            // Compute the remaining budget for this future from the shared deadline; if zero or
            // negative the deadline has already passed and we treat this as a timeout without
            // even attempting to await.
            final long remainingNs = deadlineNs - System.nanoTime();
            if (remainingNs <= 0L)
              throw new TimeoutException("deadline elapsed before draining future");
            partial = f.get(remainingNs, TimeUnit.NANOSECONDS);
          }
          merged.addAll(partial);
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          for (final Future<?> other : futures)
            other.cancel(true);
          throw new RuntimeException("Interrupted during sparse-vector top-K fan-out", ie);
        } catch (final TimeoutException te) {
          // Cancel every still-pending future so the pool stops working on results we will not
          // observe. The cancelled-from-the-pool task may still throw an InterruptedException
          // inside its IO; we don't await those (cancel(true) returns immediately).
          for (final Future<?> other : futures)
            other.cancel(true);
          throw new RuntimeException("Sparse-vector top-K fan-out timed out after "
              + timeoutSeconds + "s (configurable via "
              + GlobalConfiguration.SPARSE_VECTOR_SCORING_TIMEOUT_SECONDS.getKey() + ")", te);
        } catch (final ExecutionException ee) {
          errors.add(ee.getCause() != null ? ee.getCause() : ee);
        }
      }
      if (!errors.isEmpty()) {
        final Throwable first = errors.getFirst();
        final RuntimeException toThrow = first instanceof RuntimeException re ? re
            : new RuntimeException("Sparse-vector top-K fan-out failed", first);
        for (int i = 1; i < errors.size(); i++)
          toThrow.addSuppressed(errors.get(i));
        throw toThrow;
      }
    }

    merged.sort((a, b) -> Float.compare(b.score(), a.score()));

    final BasicDatabase db = context.getDatabase();
    final ArrayList<Object> result = new ArrayList<>();
    final GroupAdmissionState groups = groupBy != null ? new GroupAdmissionState(k, groupSize) : null;

    for (final RidScore neighbor : merged) {
      if (groupBy == null) {
        if (result.size() >= k)
          break;
      } else if (groups.isFull()) {
        break;
      }

      // Range gate. merged is sorted by score descending, so once we observe the first neighbor
      // whose score is below minScore, every subsequent one will too - break out of the loop.
      if (neighbor.score() < minScore)
        break;

      final Document record;
      try {
        record = (Document) db.lookupByRID(neighbor.rid(), true);
      } catch (final RecordNotFoundException e) {
        continue;
      }

      if (groupBy != null && !groups.admit(readNestedField(record, groupBy)))
        continue;

      final LinkedHashMap<String, Object> entry = new LinkedHashMap<>();
      entry.put("record", record);
      for (final String prop : record.getPropertyNames())
        entry.put(prop, record.get(prop));
      entry.put("@rid", record.getIdentity());
      entry.put("@type", record.getTypeName());
      entry.put("score", neighbor.score());
      result.add(entry);
    }

    return result;
  }

  private static int[] sparseToIntArray(final Object o) {
    if (o == null)
      return null;
    if (o instanceof int[] arr)
      return arr;
    if (o instanceof Integer[] arr) {
      final int[] out = new int[arr.length];
      for (int i = 0; i < arr.length; i++)
        out[i] = arr[i];
      return out;
    }
    if (o instanceof List<?> list) {
      final int[] out = new int[list.size()];
      for (int i = 0; i < out.length; i++) {
        final Object e = list.get(i);
        if (!(e instanceof Number n))
          throw new CommandSQLParsingException("Sparse query indices must be numbers, got: " + e);
        out[i] = n.intValue();
      }
      return out;
    }
    throw new CommandSQLParsingException(
        "Sparse query indices must be int[], Integer[] or List<Number>, got: " + o.getClass().getName());
  }

  private static float[] sparseToFloatArray(final Object o) {
    if (o == null)
      return null;
    if (o instanceof float[] arr)
      return arr;
    if (o instanceof Float[] arr) {
      final float[] out = new float[arr.length];
      for (int i = 0; i < arr.length; i++)
        out[i] = arr[i];
      return out;
    }
    if (o instanceof List<?> list) {
      final float[] out = new float[list.size()];
      for (int i = 0; i < out.length; i++) {
        final Object e = list.get(i);
        if (!(e instanceof Number n))
          throw new CommandSQLParsingException("Sparse query values must be numbers, got: " + e);
        out[i] = n.floatValue();
      }
      return out;
    }
    throw new CommandSQLParsingException(
        "Sparse query values must be float[], Float[] or List<Number>, got: " + o.getClass().getName());
  }

  @Override
  public String getSyntax() {
    return NAME + "(<index-name>, <indices>, <values>, <k>[, { filter: [<rid>, ...], "
        + "groupBy: <field>, groupSize: <int> }])";
  }
}
