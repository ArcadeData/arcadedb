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
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.TransactionIndexContext;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * Sparse vector index backed by the v2 {@link SparseVectorEngine} storage backend.
 * <p>
 * Storage model: an in-memory memtable + sealed {@code .sparseseg} segment files. Writes hit
 * the memtable first; once it crosses a flush threshold (or the database is closed), the
 * memtable is serialized as a new sealed segment. Background compaction merges small segments
 * into larger ones via N-way merge with newest-source-wins precedence. Top-K retrieval is
 * BlockMax-WAND DAAT (see {@link BmwScorer}); per-segment block-max metadata + skip lists
 * make selective queries skip whole posting-list regions without decompressing them.
 * <p>
 * The wrapper requires two parallel array properties on the indexed type:
 * <ul>
 *   <li>An {@link Type#ARRAY_OF_INTEGERS} property holding the non-zero dimension ids.</li>
 *   <li>An {@link Type#ARRAY_OF_FLOATS} property holding the corresponding weights.</li>
 * </ul>
 * Both arrays must have the same length and dimension ids must be non-negative.
 * <p>
 * <b>Persistence layout.</b> The wrapper retains a thin {@link LSMTreeIndex} shell purely for
 * IndexInternal compliance (file id, schema integration, lifecycle hooks). The shell never
 * receives postings; all data lives in the engine's {@code .sparse-engine/} sibling directory.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMSparseVectorIndex implements Index, IndexInternal {

  private final LSMTreeIndex                 underlyingIndex;
  private       LSMSparseVectorIndexMetadata sparseMetadata;
  private       TypeIndex                    typeIndex;
  private final PaginatedSparseVectorEngine  engine;

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
    this.engine = openEngine(index);
  }

  /**
   * Creation time. Allocates the LSM-Tree shell that holds the index's IndexInternal scaffolding
   * and opens (or creates) the v2 sparse-vector engine in a sibling {@code .sparse-engine/}
   * directory.
   */
  public LSMSparseVectorIndex(final DatabaseInternal database, final String name, final String filePath,
      final ComponentFile.MODE mode, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final LSMSparseVectorIndexMetadata metadata) {
    this.sparseMetadata = metadata;
    // The underlying LSM-Tree is kept solely as a registration shell. Postings never enter it; the
    // composite key types match the legacy MVP layout so prior databases that still hold an empty
    // shell from before the v2 swap remain readable.
    this.underlyingIndex = new LSMTreeIndex(database, name, false, filePath, mode,
        new Type[] { Type.INTEGER, Type.LINK, Type.FLOAT }, pageSize, nullStrategy);
    this.engine = openEngine(this.underlyingIndex);
  }

  @Override
  public Schema.INDEX_TYPE getType() {
    return Schema.INDEX_TYPE.LSM_SPARSE_VECTOR;
  }

  /**
   * Inserts a sparse vector for a record.
   * <p>
   * Original call (from {@code DocumentIndexer}): {@code keys[0]} is an {@code int[]} of dimension
   * ids and {@code keys[1]} is a {@code float[]} of weights. Each non-zero dim is queued for the
   * transaction or applied directly to the engine memtable depending on transaction status.
   * <p>
   * Replay call (from {@code TransactionIndexContext} at commit time): {@code keys[0]} is a
   * {@link SparsePostingReplayKey} produced by {@link #queueOrApply}. The typed marker (issue
   * #4073) replaces the prior {@code [Integer, RID, Float]} 3-tuple shape that required
   * {@code instanceof} chains to disambiguate from the original call shape.
   */
  @Override
  public void put(final Object[] keys, final RID[] rids) {
    if (rids == null || rids.length == 0)
      return;

    if (isReplayKey(keys)) {
      applyReplayPosting((SparsePostingReplayKey) keys[0], true);
      return;
    }

    final int[]   indices = toIntArray(keys[0]);
    final float[] values  = toFloatArray(keys[1]);
    if (indices == null || values == null)
      return;
    if (indices.length != values.length)
      throw new IndexException(
          "Sparse vector indices and weights must have the same length (got " + indices.length + " and " + values.length + ")");

    // dimensions=0 leaves the index open ended (informational metadata only).
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
      // BMW scoring assumes weights are non-negative finite numbers (the per-dim block-max upper
      // bound is the maximum stored weight, used as a pruning ceiling). Reject anything else at
      // write time so a misconfigured client cannot silently corrupt scoring.
      if (w < 0.0f || Float.isNaN(w) || Float.isInfinite(w))
        throw new IndexException(
            "Sparse vector weight must be a non-negative finite number, found: " + w + " at dimension " + dim);

      for (final RID rid : rids)
        queueOrApply(true, dim, rid, w);
    }
  }

  @Override
  public void remove(final Object[] keys) {
    if (isReplayKey(keys)) {
      applyReplayPosting((SparsePostingReplayKey) keys[0], false);
      return;
    }
    // The wrapper's mandatory shape `(int[] indices, float[] values)` carries no RID, so we have
    // no way to identify which postings to retract. Document deletion in ArcadeDB always goes
    // through DocumentIndexer.removeFromIndex(record), which invokes the (rid)-aware overload
    // below; this branch is logged at WARNING so an unexpected caller (a future refactor of the
    // type-drop path, e.g.) does not silently leave stale entries behind.
    LogManager.instance().log(this, Level.WARNING,
        "%s.remove(keys) called without a RID; sparse vector index needs the per-RID variant. "
            + "No postings were removed. Caller should switch to remove(keys, rid).", null, getName());
  }

  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    if (rid == null)
      return;
    if (isReplayKey(keys)) {
      // Replay path. The marker carries the per-posting RID (the same value Raft serialized into
      // the WAL); the {@code rid} parameter is intentionally unused on this branch since the
      // underlying LSM-Tree shell never sees these postings.
      applyReplayPosting((SparsePostingReplayKey) keys[0], false);
      return;
    }

    final int[]   indices = toIntArray(keys[0]);
    final float[] values  = toFloatArray(keys[1]);
    if (indices == null || values == null)
      return;
    if (indices.length != values.length)
      throw new IndexException(
          "Sparse vector indices and weights must have the same length on remove (got "
              + indices.length + " and " + values.length + ")");
    final RID actualRid = rid.getIdentity();

    for (int i = 0; i < indices.length; i++) {
      if (values[i] == 0.0f)
        continue;
      queueOrApply(false, indices[i], actualRid, values[i]);
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
   * @param allowedRIDs  optional whitelist; null means no restriction. Applied as a post-filter,
   *                     so over-fetching by a small factor mitigates fewer-than-K results when
   *                     the whitelist is selective.
   *
   * @return ordered list of (RID, score) pairs from highest to lowest score, capped at {@code k}.
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

    final float[] effectiveWeights = new float[queryValues.length];
    if (useIDF) {
      final long n = totalDocuments();
      // df is computed against engine.totalPostings under each dim. Cached per dim within this
      // call so duplicate query dims don't recompute, and so future code paths that re-derive idf
      // mid-query don't multiply the cost.
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

    // Over-fetch when an allowedRIDs whitelist is in play to reduce the chance of returning
    // fewer than K items because the top-scored RIDs were filtered out. The fixed cap keeps
    // worst-case work bounded even when the filter is very selective.
    final int fetchK = (allowedRIDs == null || allowedRIDs.isEmpty()) ? k : Math.min(k * 8, 100_000);

    final List<RidScore> raw;
    try {
      raw = engine.topK(queryIndices, effectiveWeights, fetchK);
    } catch (final IOException e) {
      throw new IndexException("Sparse vector top-K failed", e);
    }

    if (allowedRIDs == null || allowedRIDs.isEmpty())
      return raw.size() <= k ? raw : raw.subList(0, k);

    final List<RidScore> out = new ArrayList<>(Math.min(k, raw.size()));
    for (final RidScore r : raw) {
      if (!allowedRIDs.contains(r.rid()))
        continue;
      out.add(r);
      if (out.size() == k)
        break;
    }
    return out;
  }

  /**
   * Top-K with traversal-integrated {@code groupBy} (issue #4071). Equivalent shape to
   * {@link #topK} but pushes the grouping into the BMW DAAT loop: a per-group min-heap replaces
   * the global K-heap, the {@code allowedRIDs} filter is applied inline (no over-fetch), and the
   * pruning threshold tightens to the global per-group worst once every group has reached
   * {@code groupSize}.
   * <p>
   * Picks up the same IDF weighting as {@link #topK} when the index was created with
   * {@code modifier = "IDF"} so callers can swap the two methods without changing the scoring
   * model.
   *
   * @param queryIndices     non-negative dimension ids of the query
   * @param queryValues      weights matching {@code queryIndices}
   * @param limit            max number of distinct groups to return; must be {@code > 0}
   * @param groupSize        max records per group; must be {@code > 0}
   * @param allowedRIDs      optional whitelist; applied inside the BMW loop, no over-fetch needed
   * @param groupKeyResolver maps a candidate RID to its group key; {@code null} group keys are
   *                         allowed (single "null" bucket)
   *
   * @return ordered list of (RID, score) pairs from highest to lowest score; capped at
   *         {@code limit * groupSize}, with each distinct group key represented at most
   *         {@code groupSize} times.
   */
  public List<RidScore> topKGrouped(final int[] queryIndices, final float[] queryValues, final int limit, final int groupSize,
      final Set<RID> allowedRIDs, final Function<RID, Object> groupKeyResolver) {
    if (queryIndices == null || queryValues == null)
      throw new IndexException("Query indices and values must not be null");
    if (queryIndices.length != queryValues.length)
      throw new IndexException(
          "Query indices and values must have the same length (got " + queryIndices.length + " and " + queryValues.length + ")");
    if (limit <= 0)
      throw new IndexException("limit must be > 0");
    if (groupSize <= 0)
      throw new IndexException("groupSize must be > 0");
    if (groupKeyResolver == null)
      throw new IndexException("groupKeyResolver must not be null");

    final boolean useIDF = sparseMetadata != null
        && LSMSparseVectorIndexMetadata.MODIFIER_IDF.equals(sparseMetadata.modifier);

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

    try {
      return engine.topKGrouped(queryIndices, effectiveWeights, limit, groupSize, groupKeyResolver, allowedRIDs);
    } catch (final IOException e) {
      throw new IndexException("Sparse vector grouped top-K failed", e);
    }
  }

  /** Counts live postings under one dimension via the engine's merged cursor. O(df). */
  private long countPostings(final int dim) {
    try {
      return engine.countDim(dim);
    } catch (final IOException e) {
      throw new IndexException("Failed to count postings for dim " + dim, e);
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
   * Total number of documents in the indexed type, used as {@code N} in the IDF formula.
   */
  private long totalDocuments() {
    final String typeName = getTypeName();
    if (typeName == null)
      return 0;
    return underlyingIndex.getMutableIndex().getDatabase().countType(typeName, false);
  }

  /**
   * Read-only handle to the wrapped LSM-Tree shell. The shell is a registration scaffolding only;
   * it does not contain postings - those live in {@link SparseSegmentComponent} files owned by
   * the engine.
   */
  public LSMTreeIndex getUnderlyingIndex() {
    return underlyingIndex;
  }

  public LSMSparseVectorIndexMetadata getSparseMetadata() {
    return sparseMetadata;
  }

  public PaginatedSparseVectorEngine getEngine() {
    return engine;
  }

  // --------------------------- internals ---------------------------

  private static PaginatedSparseVectorEngine openEngine(final LSMTreeIndex shell) {
    return new PaginatedSparseVectorEngine(shell.getMutableIndex().getDatabase(), shell.getName(), SegmentParameters.defaults());
  }

  /**
   * Either queues the operation onto the active transaction (so it is applied at commit time
   * along with all other index changes that participate in lock ordering and recovery), or
   * applies it directly when no transaction is in flight.
   * <p>
   * <b>Memtable bounding.</b> The post-commit callback registered via
   * {@link TransactionContext#addAfterCommitCallbackIfAbsent} fires once per transaction (keyed
   * by index name) and asks the engine to flush iff the memtable has accumulated at least
   * {@link PaginatedSparseVectorEngine#DEFAULT_MEMTABLE_FLUSH_THRESHOLD} postings. Without this
   * hook a long bulk-load grows the memtable unbounded toward OOM; the threshold lets small
   * individual commits coalesce into a single segment instead of producing one segment per
   * commit. A clean shutdown still flushes via {@link #flush()} (called from
   * {@code LocalDatabase.closeInternal}) regardless of the memtable size.
   */
  private void queueOrApply(final boolean add, final int dim, final RID rid, final float weight) {
    final TransactionContext tx = underlyingIndex.getMutableIndex().getDatabase().getTransaction();
    if (tx != null && tx.getStatus() == TransactionContext.STATUS.BEGUN) {
      tx.addIndexOperation(this,
          add ? TransactionIndexContext.IndexKey.IndexKeyOperation.ADD
              : TransactionIndexContext.IndexKey.IndexKeyOperation.REMOVE,
          new Object[] { new SparsePostingReplayKey(dim, rid, weight) }, rid);
      tx.addAfterCommitCallbackIfAbsent("sparse-flush:" + getName(), engine::maybeFlush);
      return;
    }
    if (add)
      engine.put(dim, rid, weight);
    else
      engine.remove(dim, rid);
  }

  /** Apply a single posting carried through commit replay via {@link SparsePostingReplayKey}. */
  private void applyReplayPosting(final SparsePostingReplayKey key, final boolean add) {
    if (add)
      engine.put(key.dim(), key.rid(), key.weight());
    else
      engine.remove(key.dim(), key.rid());
  }

  // --------------------------- pure delegation below ---------------------------

  @Override
  public IndexCursor get(final Object[] keys) {
    return get(keys, -1);
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    // Direct exact lookup is meaningless for sparse-vector retrieval: the shell LSMTreeIndex
    // never receives any postings (the engine owns them in `.sparseseg` files), so a delegation
    // would silently return an empty cursor and a caller mistaking that for "no matches" would
    // get a wrong answer. Fail loudly instead - callers should use the `vector.sparseNeighbors`
    // SQL function (or {@link PaginatedSparseVectorEngine#topK}) for retrieval.
    throw new UnsupportedOperationException(
        "Direct posting lookup is not supported on LSM_SPARSE_VECTOR indexes; "
            + "use vector.sparseNeighbors(...) for top-K retrieval");
  }

  @Override
  public long countEntries() {
    return engine.totalPostings();
  }

  @Override
  public boolean compact() throws IOException, InterruptedException {
    return engine.compactAll() != -1L;
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
  public void flush() {
    // LocalDatabase.closeInternal calls flush() on every IndexInternal before close so any
    // memtable-resident postings land in a sealed component while the database transaction
    // pipeline is still wired up. Page WAL durability for in-flight transactions is already in
    // place; this is the "graceful shutdown" entry point.
    engine.flush();
  }

  @Override
  public void close() {
    engine.close();
    underlyingIndex.close();
  }

  @Override
  public void drop() {
    // Reclaim all .sparseseg component files this index owns. engine.close() alone would seal
    // the memtable into a *new* segment and leave every existing one (plus the just-flushed one)
    // registered with the FileManager - the files would survive after this drop() returns,
    // leaking disk and confusing the next reopen.
    engine.dropAll();
    underlyingIndex.drop();
  }

  @Override
  public String getName() {
    return underlyingIndex.getName();
  }

  @Override
  public Map<String, Long> getStats() {
    final Map<String, Long> stats = new HashMap<>();
    stats.put("memtablePostings", engine.memtablePostings());
    stats.put("totalPostings", engine.totalPostings());
    stats.put("segmentCount", (long) engine.segmentCount());
    return stats;
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
   * Detects a transaction-commit replay frame: a single-element {@code Object[]} whose only
   * element is a {@link SparsePostingReplayKey}. Anything else is the original DocumentIndexer
   * call shape (parallel arrays of indices and weights).
   */
  private static boolean isReplayKey(final Object[] keys) {
    return keys != null && keys.length == 1 && keys[0] instanceof SparsePostingReplayKey;
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
}
