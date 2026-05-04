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
package com.arcadedb.index.sparsevector;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexFactoryHandler;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.LSMSparseVectorIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Sparse vector index implementation backed by an LSM-Tree as a posting-list inverted index.
 * <p>
 * Storage model: the underlying LSM-Tree uses a composite key {@code (int dim_id, RID rid, float weight)}
 * with the record RID as the value. For each non-zero dimension of an inserted sparse vector, one posting
 * is added to the underlying index. The composite key sorts postings within a given dim by RID ascending,
 * which is the order required by WAND-style dynamic pruning (#4068). Top-K dot-product retrieval is a
 * document-at-a-time merge across per-dim cursors driven by a fixed-size min-heap; when a global per-dim
 * upper bound is available the algorithm prunes candidates that cannot enter the top-K.
 * <p>
 * The wrapper requires two parallel array properties on the indexed type:
 * <ul>
 *   <li>An {@link Type#ARRAY_OF_INTEGERS} property holding the non-zero dimension ids</li>
 *   <li>An {@link Type#ARRAY_OF_FLOATS} property holding the corresponding weights</li>
 * </ul>
 * Both arrays must have the same length and dimension ids must be non-negative.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMSparseVectorIndex implements Index, IndexInternal {

  private final LSMTreeIndex                 underlyingIndex;
  private       LSMSparseVectorIndexMetadata sparseMetadata;
  private       TypeIndex                    typeIndex;

  // Per-dim upper bound on the weight of any single posting under that dim. Maintained as a
  // monotonically non-decreasing approximation: `put` raises it, `remove` is a no-op (acceptable
  // because WAND only requires `maxWeight` to be a valid upper bound, not the exact maximum).
  //
  // Concurrency: ConcurrentHashMap.merge(...) is atomic and lock-free under contention, so writers
  // can update the per-dim bound from any thread without serializing on a shared monitor. The
  // lazy-init flag and its companion lock protect the one-shot index scan that populates the cache
  // on the first WAND query against a freshly-opened index; subsequent put()s keep it accurate
  // without further synchronization.
  private final ConcurrentHashMap<Integer, Float> dimMaxWeight            = new ConcurrentHashMap<>();
  private volatile boolean                        dimMaxWeightInitialized = false;
  // Dedicated monitor so the init double-check does not block ConcurrentHashMap operations.
  private final Object                            dimMaxWeightInitLock    = new Object();

  /**
   * Factory handler used by the schema to instantiate sparse vector indexes.
   */
  public static class LSMSparseVectorIndexFactoryHandler implements IndexFactoryHandler {
    @Override
    public IndexInternal create(final IndexBuilder builder) {
      if (builder.isUnique())
        throw new IllegalArgumentException("Sparse vector index cannot be unique");

      final Type[] keyTypes = builder.getKeyTypes();
      if (keyTypes == null || keyTypes.length != 2)
        throw new IllegalArgumentException(
            "Sparse vector index requires 2 properties: an indices array (ARRAY_OF_INTEGERS) and a weights array (ARRAY_OF_FLOATS)");
      if (keyTypes[0] != Type.ARRAY_OF_INTEGERS)
        throw new IllegalArgumentException(
            "Sparse vector index 1st property must be ARRAY_OF_INTEGERS, found: " + keyTypes[0]);
      if (keyTypes[1] != Type.ARRAY_OF_FLOATS)
        throw new IllegalArgumentException(
            "Sparse vector index 2nd property must be ARRAY_OF_FLOATS, found: " + keyTypes[1]);

      LSMSparseVectorIndexMetadata sparseMetadata = null;
      if (builder.getMetadata() instanceof LSMSparseVectorIndexMetadata m)
        sparseMetadata = m;

      return new LSMSparseVectorIndex(builder.getDatabase(), builder.getIndexName(), builder.getFilePath(),
          ComponentFile.MODE.READ_WRITE, builder.getPageSize(), builder.getNullStrategy(), sparseMetadata);
    }
  }

  /**
   * Loading time: wrap an already-loaded LSMTreeIndex.
   */
  public LSMSparseVectorIndex(final LSMTreeIndex index) {
    this(index, null);
  }

  /**
   * Loading time with optional metadata.
   */
  public LSMSparseVectorIndex(final LSMTreeIndex index, final LSMSparseVectorIndexMetadata metadata) {
    this.underlyingIndex = index;
    this.sparseMetadata = metadata;
  }

  /**
   * Creation time.
   * <p>
   * Storage layout (chosen to enable WAND-style document-at-a-time pruning, see #4068):
   * <pre>
   *   key:   (int dim_id, RID rid, float weight)
   *   value: rid
   * </pre>
   * Postings under a given dim are returned by the cursor in {@code RID} ascending order, which
   * is the order WAND requires to advance per-dim cursors in lockstep. The weight is carried in
   * the third key column so each posting is self-describing during the scan.
   */
  public LSMSparseVectorIndex(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final LSMSparseVectorIndexMetadata metadata) {
    this.sparseMetadata = metadata;
    this.underlyingIndex = new LSMTreeIndex(database, name, false, filePath, mode,
        new Type[] { Type.INTEGER, Type.LINK, Type.FLOAT }, pageSize, nullStrategy);
  }

  @Override
  public Schema.INDEX_TYPE getType() {
    return Schema.INDEX_TYPE.LSM_SPARSE_VECTOR;
  }

  /**
   * Inserts a sparse vector for a record.
   * <p>
   * Original call (from {@code DocumentIndexer}): {@code keys[0]} is an {@code int[]} of dimension ids
   * and {@code keys[1]} is a {@code float[]} of weights. Each non-zero dimension is expanded to a
   * separate {@code (dim_id, rid, weight)} posting on the underlying LSM-Tree.
   * <p>
   * Replay call (from {@code TransactionIndexContext} at commit time): the keys are already a single
   * scalar posting {@code [Integer dim_id, RID rid, Float weight]} and are forwarded unchanged.
   */
  @Override
  public void put(final Object[] keys, final RID[] rids) {
    if (!isOriginalCall(keys)) {
      underlyingIndex.put(keys, rids);
      return;
    }

    final int[]   indices = toIntArray(keys[0]);
    final float[] values  = toFloatArray(keys[1]);
    if (indices == null || values == null)
      return;
    if (indices.length != values.length)
      throw new IndexException(
          "Sparse vector indices and weights must have the same length (got " + indices.length + " and " + values.length + ")");
    if (rids == null || rids.length == 0)
      return;

    // When the index was created with an explicit `dimensions` cap (> 0), reject out-of-range
    // dimension ids at write time so a misconfigured client doesn't silently store entries that
    // can never be retrieved within the declared vocabulary. dimensions=0 leaves the index open
    // ended (informational metadata only).
    final int declaredDimensions = sparseMetadata != null ? sparseMetadata.dimensions : 0;

    for (int i = 0; i < indices.length; i++) {
      final int dim = indices[i];
      if (dim < 0)
        throw new IndexException("Sparse vector dimension must be >= 0, found: " + dim);
      if (declaredDimensions > 0 && dim >= declaredDimensions)
        throw new IndexException(
            "Sparse vector dimension " + dim + " is out of range for index '" + getName()
                + "' with declared dimensions=" + declaredDimensions);
      final float w = values[i];
      if (w == 0.0f)
        continue;
      // Negative weights are rejected: WAND's per-dim `max_weight` upper bound is built via
      // `Math.max(...)` on inserted weights and assumes non-negative values. A mix of negative
      // and positive entries would yield a max that is no longer an upper bound on any single
      // posting's contribution, and pruning would silently drop candidates that should rank.
      // All standard sparse-retrieval models (BM25, SPLADE, BGE-M3, Cohere sparse) emit
      // non-negative weights, so this is not a real constraint in practice.
      if (w < 0.0f || Float.isNaN(w))
        throw new IndexException(
            "Sparse vector weight must be a non-negative finite number, found: " + w
                + " at dimension " + dim);
      for (final RID rid : rids)
        underlyingIndex.put(new Object[] { dim, rid, w }, new RID[] { rid });
      // ConcurrentHashMap.merge is atomic; no external synchronization needed.
      dimMaxWeight.merge(dim, w, Math::max);
    }
  }

  @Override
  public void remove(final Object[] keys) {
    if (!isOriginalCall(keys)) {
      underlyingIndex.remove(keys);
      return;
    }
    // The composite-key layout `(dim_id, rid, weight)` makes it impossible to delete the right
    // postings without the owning RID, so the no-RID overload cannot do useful work. Document
    // deletion in ArcadeDB always goes through DocumentIndexer.removeFromIndex(record), which
    // invokes the (rid)-aware overload below; we don't expect this path to fire in practice.
    // Logging at WARNING so an unexpected caller (e.g. a future type-drop refactor) does not
    // silently lose entries.
    LogManager.instance().log(this, Level.WARNING,
        "%s.remove(keys) called without a RID; sparse vector index needs the per-RID variant. "
            + "No postings were removed. Caller should switch to remove(keys, rid).", null, getName());
  }

  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    if (!isOriginalCall(keys)) {
      underlyingIndex.remove(keys, rid);
      return;
    }

    final int[]   indices = toIntArray(keys[0]);
    final float[] values  = toFloatArray(keys[1]);
    if (indices == null || values == null || rid == null)
      return;
    // Mirror put()'s length check: silently truncating on mismatch would leak postings on the
    // longer side and is impossible to diagnose after the fact.
    if (indices.length != values.length)
      throw new IndexException(
          "Sparse vector indices and weights must have the same length on remove (got "
              + indices.length + " and " + values.length + ")");
    final RID actualRid = rid.getIdentity();

    for (int i = 0; i < indices.length; i++) {
      if (values[i] == 0.0f)
        continue;
      underlyingIndex.remove(new Object[] { indices[i], actualRid, values[i] }, actualRid);
    }
  }

  /**
   * Computes the top-K records by sparse dot product against the supplied query.
   * <p>
   * If the index was created with {@code modifier = "IDF"}, each query weight is scaled by the
   * inverse document frequency of its dimension before scoring (Robertson-Sparck-Jones BM25-style
   * IDF). Otherwise the score is the plain dot product.
   *
   * @param queryIndices non-negative dimension ids of the query
   * @param queryValues  weights matching {@code queryIndices}
   * @param k            number of neighbors to return; must be > 0
   * @param allowedRIDs  optional whitelist; null means no restriction
   *
   * @return ordered list of (RID, score) pairs from highest to lowest score, capped at {@code k}
   */
  public List<RidScore> topK(final int[] queryIndices, final float[] queryValues, final int k, final Set<RID> allowedRIDs) {
    if (queryIndices == null || queryValues == null)
      throw new IndexException("Query indices and values must not be null");
    if (queryIndices.length != queryValues.length)
      throw new IndexException(
          "Query indices and values must have the same length (got " + queryIndices.length + " and " + queryValues.length + ")");
    if (k <= 0)
      throw new IndexException("k must be > 0");

    final boolean useIDF = sparseMetadata != null
        && LSMSparseVectorIndexMetadata.MODIFIER_IDF.equals(sparseMetadata.modifier);

    // Pre-compute effective query weights. Under IDF, this folds the per-dimension IDF into the
    // query weight so the scoring inner loop stays a plain dot product. df is counted via a
    // dedicated scan per query dim (acceptable for the MVP - the WAND follow-up #4068 maintains
    // df incrementally to avoid the extra pass). Within a single topK call we cache df per dim so
    // duplicate dims in the query don't re-scan, and so future code paths that re-derive idf
    // mid-query don't multiply the cost.
    final float[] effectiveWeights = new float[queryValues.length];
    if (useIDF) {
      final long n = totalDocuments();
      final HashMap<Integer, Long> dfCache = new HashMap<>();
      for (int i = 0; i < queryIndices.length; i++) {
        final int qDim = queryIndices[i];
        if (qDim < 0)
          throw new IndexException("Query dimension must be >= 0, found: " + qDim);
        if (queryValues[i] == 0.0f) {
          effectiveWeights[i] = 0.0f;
          continue;
        }
        Long df = dfCache.get(qDim);
        if (df == null) {
          df = countPostings(qDim);
          dfCache.put(qDim, df);
        }
        effectiveWeights[i] = queryValues[i] * idf(n, df);
      }
    } else {
      System.arraycopy(queryValues, 0, effectiveWeights, 0, queryValues.length);
    }

    return wandTopK(queryIndices, effectiveWeights, k, allowedRIDs);
  }

  /**
   * Document-at-a-time top-K with WAND pivot-based skipping.
   * <p>
   * Per query dim {@code d} an upper bound {@code u[d] = effW[d] * dimMaxWeight[d]} is precomputed.
   * Each iteration sorts the live cursors by current RID ascending, finds the pivot (smallest cursor
   * index whose prefix sum of {@code u[]} reaches the current top-K threshold), and either scores the
   * pivot's RID (when cursors[0] is already aligned with the pivot) or skips cursors before the pivot
   * forward to the pivot's RID. When no pivot exists, no remaining document can enter the top-K and
   * the loop exits early.
   */
  private List<RidScore> wandTopK(final int[] queryIndices, final float[] effW, final int k, final Set<RID> allowedRIDs) {
    final int n = queryIndices.length;

    final DimCursor[] cursors = new DimCursor[n];
    int liveCount = 0;

    // Allocation phase: a DimCursor that exhausts immediately still gets closed before we drop
    // the reference, so an empty-dim doesn't leak its initial range cursor.
    try {
      for (int i = 0; i < n; i++) {
        final int qDim = queryIndices[i];
        if (qDim < 0)
          throw new IndexException("Query dimension must be >= 0, found: " + qDim);
        if (effW[i] == 0.0f)
          continue;
        final float dimMax = getMaxWeight(qDim);
        if (dimMax == 0.0f)
          continue;
        final DimCursor c = new DimCursor(underlyingIndex, qDim, effW[i], dimMax);
        if (c.exhausted)
          c.close();
        else
          cursors[liveCount++] = c;
      }

      if (liveCount == 0)
        return new ArrayList<>(0);

      return runWandLoop(cursors, liveCount, k, allowedRIDs);
    } finally {
      // Close every DimCursor that was kept alive (active or exhausted but not yet compacted out)
      // so the underlying LSM page cursors release their references regardless of how the loop
      // terminates: normal exit, threshold reached, or exception thrown mid-scoring.
      for (int i = 0; i < cursors.length; i++) {
        final DimCursor c = cursors[i];
        if (c != null) {
          try {
            c.close();
          } catch (final RuntimeException ignored) {
            // best-effort cleanup; never let a close exception mask the real error path
          }
          cursors[i] = null;
        }
      }
    }
  }

  /**
   * The WAND scoring loop, factored out of {@link #wandTopK} so the surrounding
   * {@code try / finally} can guarantee cursor cleanup.
   */
  private List<RidScore> runWandLoop(final DimCursor[] cursors, int liveCount, final int k, final Set<RID> allowedRIDs) {
    // Min-heap of (rid, score) keyed by score ASC, capped at K. The minimum element is always at
    // the head, so the K-th best score (= threshold) is heap.peek() once heap is full.
    final PriorityQueue<RidScore> heap = new PriorityQueue<>(k, Comparator.comparingDouble(r -> r.score));
    float threshold = Float.NEGATIVE_INFINITY;

    while (liveCount > 0) {
      // Cursors are mostly already sorted between iterations: at most a few cursors moved during
      // the previous step, so insertion sort runs in roughly O(n) on near-sorted input. With
      // typical query nnz around 10-30, this beats Arrays.sort's O(n log n) constant factor.
      // BlockMax-WAND in #4068 will replace this loop with a heap-keyed-by-current-RID, removing
      // the resort altogether.
      sortCursorsByCurrentRid(cursors, liveCount);

      // Find pivot index: smallest i where prefix sum of upperBounds[0..i] reaches threshold.
      float prefix = 0;
      int pivot = -1;
      for (int i = 0; i < liveCount; i++) {
        prefix += cursors[i].upperBound;
        if (prefix > threshold) {
          pivot = i;
          break;
        }
      }
      if (pivot == -1)
        break;

      final RID pivotRid = cursors[pivot].currentRid;

      if (cursors[0].currentRid.compareTo(pivotRid) == 0) {
        // Score: sum contributions from every cursor whose currentRid == pivotRid.
        float score = 0.0f;
        for (int i = 0; i < liveCount; i++) {
          if (cursors[i].currentRid.compareTo(pivotRid) == 0)
            score += cursors[i].queryWeight * cursors[i].currentWeight;
          else
            break; // sorted, so once we miss alignment we stop
        }

        if (allowedRIDs == null || allowedRIDs.contains(pivotRid)) {
          if (heap.size() < k) {
            heap.offer(new RidScore(pivotRid, score));
            if (heap.size() == k)
              threshold = heap.peek().score;
          } else if (score > threshold) {
            heap.poll();
            heap.offer(new RidScore(pivotRid, score));
            threshold = heap.peek().score;
          }
        }

        // Advance every cursor that was on pivotRid.
        for (int i = 0; i < liveCount; i++) {
          if (cursors[i].currentRid.compareTo(pivotRid) == 0)
            cursors[i].advance();
          else
            break;
        }
      } else {
        // Skip cursors strictly below pivotRid forward to pivotRid; they can never reach threshold
        // alone before then.
        for (int i = 0; i < pivot; i++) {
          if (cursors[i].currentRid.compareTo(pivotRid) < 0)
            cursors[i].seekTo(pivotRid);
        }
      }

      // Compact: drop exhausted cursors. Close them eagerly so their underlying page-cursor
      // resources release immediately rather than waiting for the outer finally block.
      int newLive = 0;
      for (int i = 0; i < liveCount; i++) {
        if (!cursors[i].exhausted)
          cursors[newLive++] = cursors[i];
        else
          cursors[i].close();
      }
      // Null the trailing slots so the outer finally block does not double-close.
      for (int i = newLive; i < liveCount; i++)
        cursors[i] = null;
      liveCount = newLive;
    }

    final List<RidScore> result = new ArrayList<>(heap);
    result.sort((a, b) -> Float.compare(b.score, a.score));
    return result;
  }

  /**
   * Returns the per-dim upper bound used by WAND. Lazily initializes the cache on first call by
   * scanning the underlying index once. Subsequent inserts keep it accurate via {@link #put};
   * deletes leave the bound conservatively high (still a valid upper bound).
   * <p>
   * Uses double-checked locking on a dedicated monitor: the volatile {@code dimMaxWeightInitialized}
   * flag is read on the fast path with no lock, and only the first thread reaching a fresh index
   * pays the monitor cost. Writers concurrent with the init scan are still safe because
   * {@link ConcurrentHashMap#merge} is monotone and atomic; the worst case is the init scan and a
   * concurrent {@code put} both updating the same key, in which case {@link Math#max} ensures the
   * later read sees the higher of the two values.
   */
  private float getMaxWeight(final int dim) {
    if (!dimMaxWeightInitialized) {
      synchronized (dimMaxWeightInitLock) {
        if (!dimMaxWeightInitialized) {
          initializeMaxWeights();
          dimMaxWeightInitialized = true;
        }
      }
    }
    return dimMaxWeight.getOrDefault(dim, 0.0f);
  }

  private void initializeMaxWeights() {
    final IndexCursor cursor = underlyingIndex.iterator(true);
    try {
      while (cursor.hasNext()) {
        cursor.next();
        final Object[] keys = cursor.getKeys();
        if (keys == null || keys.length < 3)
          continue;
        if (!(keys[0] instanceof Number nDim) || !(keys[2] instanceof Number nW))
          continue;
        dimMaxWeight.merge(nDim.intValue(), nW.floatValue(), Math::max);
      }
    } finally {
      cursor.close();
    }
  }

  /**
   * Per-dim cursor used by the WAND scoring loop. Holds the latest {@code (rid, weight)} of an
   * underlying range scan within a single dimension and exposes {@code advance()} and
   * {@code seekTo(rid)} primitives.
   * <p>
   * Resource management: every cursor allocated via {@link LSMTreeIndex#range} may hold page
   * references inside the LSM-Tree (see {@code LSMTreeIndexCursor.close()} which releases page
   * cursor handles). {@link #close()} closes the active cursor; {@link #seekTo} closes the
   * previous cursor before allocating a new one; the WAND loop closes every {@link DimCursor}
   * before returning.
   */
  private static final class DimCursor implements AutoCloseable {
    final LSMTreeIndex underlying;
    final int          dim;
    final float        queryWeight;
    final float        upperBound;

    IndexCursor cursor;
    RID         currentRid;
    float       currentWeight;
    boolean     exhausted;

    DimCursor(final LSMTreeIndex underlying, final int dim, final float queryWeight, final float dimMax) {
      this.underlying = underlying;
      this.dim = dim;
      this.queryWeight = queryWeight;
      this.upperBound = queryWeight * dimMax;
      this.cursor = underlying.range(true, new Object[] { dim }, true, new Object[] { dim }, true);
      advance();
    }

    void advance() {
      while (cursor.hasNext()) {
        final Identifiable next = cursor.next();
        if (next == null)
          continue;
        final Object[] keys = cursor.getKeys();
        if (keys == null || keys.length < 3)
          continue;
        if (!(keys[0] instanceof Number nDim) || nDim.intValue() != dim)
          continue;
        if (!(keys[2] instanceof Number nW))
          continue;
        currentRid = next.getIdentity();
        currentWeight = nW.floatValue();
        exhausted = false;
        return;
      }
      exhausted = true;
      currentRid = null;
      currentWeight = 0.0f;
    }

    void seekTo(final RID target) {
      // Release the previous cursor's resources (page handles in LSMTreeIndexCursor) before
      // replacing it; otherwise every WAND skip leaks one page-cursor's worth of state.
      if (cursor != null)
        cursor.close();
      // Upper bound { dim } is intentionally a prefix: LSMTreeIndexAbstract.compareKeys() compares
      // only up to min(key1.length, key2.length) columns, so a posting whose key starts with
      // `dim` compares equal to `[dim]` and is included by an inclusive upper bound. The effect
      // is equivalent to "to the end of dim". A regression test in LSMSparseVectorIndexSeekTest
      // pins this behaviour so a future comparator change cannot silently drop postings.
      cursor = underlying.range(true, new Object[] { dim, target }, true, new Object[] { dim }, true);
      advance();
    }

    @Override
    public void close() {
      if (cursor != null) {
        cursor.close();
        cursor = null;
      }
    }
  }

  /**
   * In-place insertion sort of the live prefix of {@code cursors} by {@code currentRid} ascending.
   * Cheaper than {@link Arrays#sort} for the small, mostly-sorted arrays the WAND loop produces.
   */
  private static void sortCursorsByCurrentRid(final DimCursor[] cursors, final int liveCount) {
    for (int i = 1; i < liveCount; i++) {
      final DimCursor key = cursors[i];
      int j = i - 1;
      while (j >= 0 && cursors[j].currentRid.compareTo(key.currentRid) > 0) {
        cursors[j + 1] = cursors[j];
        j--;
      }
      cursors[j + 1] = key;
    }
  }

  /**
   * Robertson-Sparck-Jones IDF as used by Qdrant's IDF modifier and BM25 scoring.
   * Returns {@code ln((N - df + 0.5) / (df + 0.5) + 1)}, which is non-negative for any
   * non-negative {@code N} and {@code df}.
   */
  private static float idf(final long n, final long df) {
    final double numerator = (double) (n - df) + 0.5;
    final double denominator = (double) df + 0.5;
    return (float) Math.log((numerator / denominator) + 1.0);
  }

  /**
   * Counts the postings under a single dimension by iterating the underlying LSM-Tree range.
   * O(df) per call: scans every posting for {@code qDim} once. Bounded by the size of one posting
   * list, but for high-frequency dims (corpus stopwords) this dominates the IDF preprocessing
   * step. The {@link #topK} caller caches the result per (dim, query) so duplicate dims do not
   * re-scan, and incremental df maintenance is tracked as part of the WAND scaling work in
   * #4068, which removes this scan entirely.
   */
  private long countPostings(final int qDim) {
    long df = 0;
    final IndexCursor cursor = underlyingIndex.range(true,
        new Object[] { qDim }, true,
        new Object[] { qDim }, true);
    try {
      while (cursor.hasNext()) {
        cursor.next();
        df++;
      }
    } finally {
      cursor.close();
    }
    return df;
  }

  /**
   * Total number of documents in the indexed type, used as {@code N} in the IDF formula.
   * <p>
   * Called once per {@code topK} in IDF mode (not once per query dim), so the overhead is bounded.
   * {@code countType} sums each non-polymorphic bucket's cached entry count under a read lock; in
   * the typical few-buckets case this is on the order of a handful of long reads plus the lock
   * round-trip. If profiling later shows this is hot, the count can be cached on the wrapper with
   * invalidation on put / remove.
   */
  private long totalDocuments() {
    final String typeName = getTypeName();
    if (typeName == null)
      return 0;
    return underlyingIndex.getMutableIndex().getDatabase().countType(typeName, false);
  }

  /**
   * Read-only handle to the wrapped LSM-Tree, useful for low-level inspection and testing.
   */
  public LSMTreeIndex getUnderlyingIndex() {
    return underlyingIndex;
  }

  public LSMSparseVectorIndexMetadata getSparseMetadata() {
    return sparseMetadata;
  }

  // --------------------------- pure delegation below ---------------------------

  @Override
  public IndexCursor get(final Object[] keys) {
    return get(keys, -1);
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    // Direct exact lookup is rarely meaningful for sparse vector retrieval; surface only
    // raw posting-list lookup for diagnostic purposes.
    return underlyingIndex.get(keys, limit);
  }

  @Override
  public long countEntries() {
    return underlyingIndex.countEntries();
  }

  @Override
  public boolean compact() throws IOException, InterruptedException {
    return underlyingIndex.compact();
  }

  @Override
  public IndexMetadata getMetadata() {
    return underlyingIndex.getMetadata();
  }

  @Override
  public boolean isCompacting() {
    return underlyingIndex.isCompacting();
  }

  @Override
  public boolean scheduleCompaction() {
    return underlyingIndex.scheduleCompaction();
  }

  @Override
  public String getMostRecentFileName() {
    return underlyingIndex.getMostRecentFileName();
  }

  @Override
  public void setMetadata(final IndexMetadata metadata) {
    underlyingIndex.setMetadata(metadata);
  }

  @Override
  public void setMetadata(final JSONObject indexJSON) {
    underlyingIndex.setMetadata(indexJSON);

    final LSMSparseVectorIndexMetadata m = new LSMSparseVectorIndexMetadata(
        underlyingIndex.getMetadata().typeName,
        underlyingIndex.getPropertyNames() != null ?
            underlyingIndex.getPropertyNames().toArray(new String[0]) : new String[0],
        underlyingIndex.getMetadata().associatedBucketId);
    m.fromJSON(indexJSON);
    this.sparseMetadata = m;
  }

  @Override
  public boolean setStatus(final INDEX_STATUS[] expectedStatuses, final INDEX_STATUS newStatus) {
    return underlyingIndex.setStatus(expectedStatuses, newStatus);
  }

  @Override
  public IndexInternal getAssociatedIndex() {
    return null;
  }

  @Override
  public String getTypeName() {
    return underlyingIndex.getTypeName();
  }

  @Override
  public List<String> getPropertyNames() {
    return underlyingIndex.getPropertyNames();
  }

  @Override
  public void close() {
    underlyingIndex.close();
  }

  @Override
  public void drop() {
    underlyingIndex.drop();
  }

  @Override
  public String getName() {
    return underlyingIndex.getName();
  }

  @Override
  public Map<String, Long> getStats() {
    return underlyingIndex.getStats();
  }

  @Override
  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    return underlyingIndex.getNullStrategy();
  }

  @Override
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    underlyingIndex.setNullStrategy(nullStrategy);
  }

  @Override
  public int getFileId() {
    return underlyingIndex.getFileId();
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  @Override
  public PaginatedComponent getComponent() {
    return underlyingIndex.getComponent();
  }

  @Override
  public Type[] getKeyTypes() {
    return underlyingIndex.getKeyTypes();
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    return underlyingIndex.getBinaryKeyTypes();
  }

  @Override
  public int getAssociatedBucketId() {
    return underlyingIndex.getAssociatedBucketId();
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public boolean isAutomatic() {
    return underlyingIndex.getPropertyNames() != null;
  }

  @Override
  public int getPageSize() {
    return underlyingIndex.getPageSize();
  }

  @Override
  public List<Integer> getFileIds() {
    return underlyingIndex.getFileIds();
  }

  @Override
  public void setTypeIndex(final TypeIndex typeIndex) {
    this.typeIndex = typeIndex;
  }

  @Override
  public TypeIndex getTypeIndex() {
    return typeIndex;
  }

  @Override
  public long build(final int buildIndexBatchSize, final BuildIndexCallback callback) {
    return underlyingIndex.build(buildIndexBatchSize, callback);
  }

  @Override
  public void updateTypeName(final String newTypeName) {
    underlyingIndex.updateTypeName(newTypeName);
  }

  @Override
  public boolean isValid() {
    return underlyingIndex.isValid();
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("type", getType());
    json.put("bucket",
        underlyingIndex.getMutableIndex().getDatabase().getSchema().getBucketById(getAssociatedBucketId()).getName());
    json.put("properties", getPropertyNames());
    json.put("nullStrategy", getNullStrategy());
    json.put("unique", isUnique());
    if (sparseMetadata != null) {
      json.put("dimensions", sparseMetadata.dimensions);
      json.put("modifier", sparseMetadata.modifier);
    }
    return json;
  }

  // --------------------------- helpers ---------------------------

  /**
   * An "original" call is one made by {@code DocumentIndexer} with the raw property values: a pair
   * of parallel arrays (indices, weights) per record. Anything else (already-expanded scalar postings
   * coming from the transaction commit replay path, or null/empty arrays) is forwarded to the
   * underlying LSM-Tree unchanged.
   */
  private static boolean isOriginalCall(final Object[] keys) {
    if (keys == null || keys.length != 2)
      return false;
    final boolean firstIsArray = keys[0] instanceof int[] || keys[0] instanceof Integer[] || keys[0] instanceof List<?>;
    final boolean secondIsArray = keys[1] instanceof float[] || keys[1] instanceof Float[] || keys[1] instanceof List<?>;
    return firstIsArray && secondIsArray;
  }

  private static int[] toIntArray(final Object o) {
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
          throw new IndexException("Sparse vector indices must be numbers, found: " + e);
        out[i] = n.intValue();
      }
      return out;
    }
    throw new IndexException("Sparse vector indices must be int[], Integer[] or List<Number>, found: " + o.getClass().getName());
  }

  private static float[] toFloatArray(final Object o) {
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
          throw new IndexException("Sparse vector weights must be numbers, found: " + e);
        out[i] = n.floatValue();
      }
      return out;
    }
    throw new IndexException("Sparse vector weights must be float[], Float[] or List<Number>, found: " + o.getClass().getName());
  }

  /**
   * Pair of (RID, dot-product score) returned by {@link #topK}.
   */
  public static final class RidScore {
    public final RID   rid;
    public final float score;

    public RidScore(final RID rid, final float score) {
      this.rid = rid;
      this.score = score;
    }
  }
}
