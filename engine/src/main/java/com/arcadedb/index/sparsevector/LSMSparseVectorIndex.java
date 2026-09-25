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
import com.arcadedb.index.IndexReplayConclusion;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.index.vector.GroupAdmissionState;
import com.arcadedb.index.vector.GroupedTopUpPlanner;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.LSMSparseVectorIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
 * Block-Max MaxScore DAAT (see {@link BmwScorer}): terms whose combined maximum contribution
 * cannot reach the top-K watermark leave the traversal entirely and are only point-probed, and
 * per-segment block-max metadata + skip lists make selective queries skip whole posting-list
 * regions without decompressing them.
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
   * Pre-built key for the post-commit memtable-flush callback. Built once per index instance
   * because {@link #queueOrApply} registers the callback for every queued posting - hundreds per
   * record on learned-sparse corpora - and rebuilding the string there showed up as pure garbage
   * on bulk-load profiles (issue #5411).
   */
  private final String                       afterCommitFlushKey;

  /**
   * Ceiling on the rows a search may materialise BEYOND what the caller asked for: the ungrouped path's over-fetch
   * multiplier, and the grouped path's {@code limit * groupSize} product, neither of which may be turned into an
   * allocation by a caller passing two large numbers. Never applied to {@code k} itself - a caller that asks for
   * more rows than this and narrows nothing still gets them (PR #8001 review).
   */
  private static final int MAX_OVERFETCH_ROWS = 100_000;

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
    this.engine = openEngine(index, metadata);
    this.afterCommitFlushKey = "sparse-flush:" + index.getName();
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
    this.engine = openEngine(this.underlyingIndex, metadata);
    this.afterCommitFlushKey = "sparse-flush:" + name;
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
        """
        %s.remove(keys) called without a RID; sparse vector index needs the per-RID variant. \
        No postings were removed. Caller should switch to remove(keys, rid).""", null, getName());
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
    return topK(queryIndices, queryValues, k, allowedRIDs, transactionOverlay());
  }

  /**
   * {@link #topK(int[], float[], int, Set)} against an overlay the CALLER resolved (issue #7966).
   * <p>
   * The no-overlay overload resolves one from whatever transaction is bound to the thread it runs on, which is
   * right for a caller that searches on its own thread and wrong for one that fans out: on a
   * {@code SparseVectorScoringPool} worker there is no transaction to find, so the same query would read its own
   * writes on the serial plan and miss them on the parallel one. A fan-out caller resolves the overlay up front and
   * passes it here.
   *
   * @param overlay what this transaction has queued for this index, or {@code null} for the committed state alone
   */
  public List<RidScore> topK(final int[] queryIndices, final float[] queryValues, final int k, final Set<RID> allowedRIDs,
      final SparseTransactionOverlay overlay) {
    if (queryIndices == null || queryValues == null)
      throw new IndexException("Query indices and values must not be null");
    if (queryIndices.length != queryValues.length)
      throw new IndexException(
          "Query indices and values must have the same length (got " + queryIndices.length + " and " + queryValues.length + ")");
    if (k <= 0)
      throw new IndexException("k must be > 0");

    final float[] effectiveWeights = effectiveWeights(queryIndices, queryValues);

    // Over-fetch when an allowedRIDs whitelist is in play to reduce the chance of returning
    // fewer than K items because the top-scored RIDs were filtered out. The fixed cap keeps
    // worst-case work bounded even when the filter is very selective.
    //
    // The overlay widens it by exactly the number of RIDs the transaction has touched (issue #7966): those are
    // dropped from the committed answer below - their committed vector is stale, and a deleted record must not
    // come back - so fetching k of them and discarding some would under-fill. Fetching k + touched cannot.
    final Set<RID> pendingRIDs = overlay != null ? overlay.touchedRIDs() : null;
    final long widened = (long) k + (overlay != null ? overlay.touchedCount() : 0);
    // The unfiltered branch stays UNCAPPED, as it was before the overflow guard went in (PR #8001 review). The
    // 100_000 ceiling belongs to the over-fetch: it bounds the multiplier applied to compensate for a selective
    // filter, not the caller's own k. Capping the unfiltered branch made this method quietly return at most
    // 100_000 rows to a caller that asked for more and would previously have got them all - a truncation with no
    // error to notice it by. The SQL function refuses a k that large long before it reaches here, but this method
    // is public and an embedded caller is not going through it.
    final int fetchK = allowedRIDs == null || allowedRIDs.isEmpty() ?
        (int) Math.min(widened, Integer.MAX_VALUE) :
        (int) Math.min(widened * 8, MAX_OVERFETCH_ROWS);

    final List<RidScore> raw;
    try {
      raw = engine.topK(queryIndices, effectiveWeights, fetchK);
    } catch (final IOException e) {
      throw new IndexException("Sparse vector top-K failed", e);
    }

    final boolean filtered = allowedRIDs != null && !allowedRIDs.isEmpty();
    if (!filtered && pendingRIDs == null)
      return raw.size() <= k ? raw : raw.subList(0, k);

    final List<RidScore> out = new ArrayList<>(Math.min(k, raw.size()));
    for (final RidScore r : raw) {
      if (filtered && !allowedRIDs.contains(r.rid()))
        continue;
      if (pendingRIDs != null && pendingRIDs.contains(r.rid()))
        continue;
      out.add(r);
      if (out.size() == k)
        break;
    }

    if (overlay == null)
      return out;

    // The transaction's own rows, scored from what it has queued, merged into the committed ones by score. Both
    // sides are exact top-k of their own population, so the merge of the two is the exact top-k of the union.
    return mergeByScore(out, overlay.topK(queryIndices, effectiveWeights, allowedRIDs, k), k);
  }

  /**
   * The best {@code k} of two already-sorted, disjoint score lists, highest first. Disjoint by construction: the
   * committed side skips every RID the overlay holds.
   */
  private static List<RidScore> mergeByScore(final List<RidScore> committed, final List<RidScore> pending, final int k) {
    // Asserted rather than only documented (PR #8001 review): a caller that handed this an unsorted or overlapping
    // pair would not fail here, it would return a ranking that is quietly wrong - and Surefire runs this repository
    // with -ea, so a caller added without those properties trips this before it can ship. Not a hard check: the two
    // lists are the whole result set of a query, and re-validating them per query would pay O(n) on the hot path to
    // guard against a mistake only a code change can introduce.
    assert isDescending(committed) && isDescending(pending) : "mergeByScore() inputs must be sorted by score, highest first";
    assert disjoint(committed, pending) : "mergeByScore() inputs must be disjoint: the committed side skips every RID the overlay holds";

    if (pending.isEmpty())
      return committed;
    if (committed.isEmpty())
      return pending.size() <= k ? pending : pending.subList(0, k);

    final List<RidScore> merged = new ArrayList<>(Math.min(k, committed.size() + pending.size()));
    int c = 0;
    int p = 0;
    while (merged.size() < k && (c < committed.size() || p < pending.size())) {
      if (p == pending.size() || (c < committed.size() && committed.get(c).score() >= pending.get(p).score()))
        merged.add(committed.get(c++));
      else
        merged.add(pending.get(p++));
    }
    return merged;
  }

  private static boolean isDescending(final List<RidScore> scores) {
    for (int i = 1; i < scores.size(); i++)
      if (scores.get(i - 1).score() < scores.get(i).score())
        return false;
    return true;
  }

  private static boolean disjoint(final List<RidScore> a, final List<RidScore> b) {
    if (a.isEmpty() || b.isEmpty())
      return true;
    final Set<RID> seen = new HashSet<>(a.size() * 4 / 3 + 1);
    for (final RidScore r : a)
      seen.add(r.rid());
    for (final RidScore r : b)
      if (seen.contains(r.rid()))
        return false;
    return true;
  }

  /**
   * What the transaction bound to THIS thread has queued for this index, or {@code null} when it has queued nothing
   * - which is every read-only query, and costs one map lookup (issue #7966).
   * <p>
   * Public because a caller that fans its per-bucket searches out to a pool has to resolve it here, on its own
   * thread, and hand it to the workers: see {@link #topK(int[], float[], int, Set, SparseTransactionOverlay)}.
   */
  public SparseTransactionOverlay transactionOverlay() {
    final TransactionContext tx = underlyingIndex.getMutableIndex().getDatabase().getTransactionIfExists();
    if (tx == null || tx.getStatus() != TransactionContext.STATUS.BEGUN)
      return null;

    // Cached on the transaction under the lane version, exactly as the dense overlay is (issue #7967, and the gap
    // named in the PR #8001 review). The overlay is a pure function of the lanes, and a sparse one is dearer to
    // build than a dense one: a learned-sparse record queues one entry per non-zero dimension, hundreds of them,
    // so a transaction that ingests and searches in turn was replaying its whole posting set per search.
    final TransactionIndexContext changes = tx.getIndexChanges();
    final Object cached = changes.cachedIndexView(this);
    if (cached != null)
      return cached == TransactionIndexContext.NO_VIEW ? null : (SparseTransactionOverlay) cached;

    final SparseTransactionOverlay built = SparseTransactionOverlay.of(changes.getUnorderedIndexKeys(getName()));
    changes.cacheIndexView(this, built);
    return built;
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
    return topKGrouped(queryIndices, queryValues, limit, groupSize, allowedRIDs, groupKeyResolver, transactionOverlay());
  }

  /**
   * {@link #topKGrouped(int[], float[], int, int, Set, Function)} against an overlay the CALLER resolved, for the
   * same reason the ungrouped overload has one (issue #7966).
   */
  public List<RidScore> topKGrouped(final int[] queryIndices, final float[] queryValues, final int limit, final int groupSize,
      final Set<RID> allowedRIDs, final Function<RID, Object> groupKeyResolver, final SparseTransactionOverlay overlay) {
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

    final float[] effectiveWeights = effectiveWeights(queryIndices, queryValues);

    final List<RidScore> committed;
    try {
      // The excluded set is applied INSIDE the DAAT loop rather than by filtering the result: a grouped search
      // counts admissions per group as it goes, so dropping rows afterwards would leave groups short of their cap
      // with candidates still available (issue #7966).
      committed = engine.topKGrouped(queryIndices, effectiveWeights, limit, groupSize, groupKeyResolver, allowedRIDs,
          overlay != null ? overlay.touchedRIDs() : null);
    } catch (final IOException e) {
      throw new IndexException("Sparse vector grouped top-K failed", e);
    }

    if (overlay == null)
      return committed;

    // EVERY pending row, not the best limit * groupSize of them (PR #8001 review). A global-score cut here happens
    // BEFORE the per-group caps are applied, so it can spend the whole budget on the surplus of one good group and
    // starve another of its only candidate: with limit 2, groupSize 1 and pending A:0.99, A:0.98, B:0.50, a budget of
    // two returns both A rows, admission keeps one and rejects the other, and B is never offered - while the same
    // search after the commit returns A and B. Only the admission pass knows which rows a cap can still take, so it
    // is the only thing allowed to drop one. The overlay already scores its whole pending set to sort it, so this
    // costs list length rather than work, and the admission loop stops as soon as the groups are full.
    final List<RidScore> pending = overlay.topK(queryIndices, effectiveWeights, allowedRIDs, Integer.MAX_VALUE);
    if (pending.isEmpty())
      return committed;

    // The committed pass chose ITS best `limit` groups, and a pending row can promote a group it ranked out, whose
    // committed members that pass therefore never returned. Those groups are asked for again (issue #8002), which is
    // what makes this answer the one the same search gives after the commit - the contract of issue #7966.
    final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(limit, groupSize);
    addSource(planner, committed, groupKeyResolver, true);
    addSource(planner, pending, groupKeyResolver, false);
    List<RidScore> committedRows = committed;
    for (final GroupedTopUpPlanner.TopUp topUp : planner.plan()) {
      final List<RidScore> topUpRows;
      try {
        topUpRows = engine.topKForGroups(queryIndices, effectiveWeights, topUp.groupKeys(), groupSize, topUp.floor(),
            groupKeyResolver, allowedRIDs, overlay.touchedRIDs());
      } catch (final IOException e) {
        throw new IndexException("Sparse vector grouped top-K failed", e);
      }
      // Disjoint from `committed`: a restricted search returns only groups the first pass left out.
      committedRows = new ArrayList<>(committed.size() + topUpRows.size());
      committedRows.addAll(committed);
      committedRows.addAll(topUpRows);
      committedRows.sort(BmwScorer.BY_SCORE_DESC);
    }

    final List<RidScore> merged = mergeByScore(committedRows, pending, Integer.MAX_VALUE);

    // The transaction's rows carry no group accounting of their own, so the caps are re-applied over the union. The
    // engine already enforced them on the committed half, which makes this pass idempotent there - the same
    // relationship the SQL layer's own re-application has with a single-bucket result.
    //
    // long, not int (PR #8001 review): limit and groupSize are validated as positive and nothing bounds them from
    // above, so their product overflows to a negative int at around 46341 each. It is a CAPACITY HINT for the list
    // below and nothing more: what bounds the admitted rows is the GroupAdmissionState, at most limit groups of
    // groupSize each, and an ArrayList given a small hint grows, it does not truncate.
    final int rowBudget = (int) Math.min((long) limit * groupSize, MAX_OVERFETCH_ROWS);
    final GroupAdmissionState groups = new GroupAdmissionState(limit, groupSize);
    final List<RidScore> out = new ArrayList<>(Math.min(merged.size(), rowBudget));
    for (final RidScore candidate : merged) {
      if (groups.isFull())
        break;
      if (groups.admit(groupKeyResolver.apply(candidate.rid())))
        out.add(candidate);
    }
    return out;
  }

  /**
   * The best {@code groupSize} members of each of a fixed set of groups, committed rows plus what the calling
   * transaction has queued (issue #8002). The second phase of a grouped search merged across several indexes: an
   * index that ranked a group winning overall out of its own top {@code limit} may still hold members that group
   * needs.
   *
   * @param groupKeys the groups to fill; no other group is ever returned
   * @param floor     only scores strictly above this matter to the caller; {@link Float#NEGATIVE_INFINITY} for none
   * @param overlay   what this transaction has queued for this index, or {@code null} for the committed state alone
   *
   * @return (RID, score) pairs sorted by score descending, at most {@code groupSize} per group, every one above
   *         {@code floor}
   */
  public List<RidScore> topKForGroups(final int[] queryIndices, final float[] queryValues, final Set<Object> groupKeys,
      final int groupSize, final float floor, final Set<RID> allowedRIDs, final Function<RID, Object> groupKeyResolver,
      final SparseTransactionOverlay overlay) {
    if (queryIndices == null || queryValues == null)
      throw new IndexException("Query indices and values must not be null");
    if (queryIndices.length != queryValues.length)
      throw new IndexException(
          "Query indices and values must have the same length (got " + queryIndices.length + " and " + queryValues.length + ")");
    if (groupSize <= 0)
      throw new IndexException("groupSize must be > 0");
    if (groupKeyResolver == null)
      throw new IndexException("groupKeyResolver must not be null");
    if (groupKeys == null || groupKeys.isEmpty())
      return List.of();

    final float[] effectiveWeights = effectiveWeights(queryIndices, queryValues);

    final List<RidScore> committed;
    try {
      committed = engine.topKForGroups(queryIndices, effectiveWeights, groupKeys, groupSize, floor, groupKeyResolver,
          allowedRIDs, overlay != null ? overlay.touchedRIDs() : null);
    } catch (final IOException e) {
      throw new IndexException("Sparse vector grouped top-K failed", e);
    }
    if (overlay == null)
      return committed;

    final List<RidScore> merged = mergeByScore(committed,
        overlay.topK(queryIndices, effectiveWeights, allowedRIDs, Integer.MAX_VALUE), Integer.MAX_VALUE);
    final HashMap<Object, Integer> perGroup = new HashMap<>();
    final List<RidScore> out = new ArrayList<>();
    for (final RidScore candidate : merged) {
      if (candidate.score() <= floor)
        break;
      final Object key = groupKeyResolver.apply(candidate.rid());
      if (groupKeys.contains(key) && perGroup.merge(key, 1, Integer::sum) <= groupSize)
        out.add(candidate);
    }
    return out;
  }

  private static void addSource(final GroupedTopUpPlanner planner, final List<RidScore> rows,
      final Function<RID, Object> groupKeyResolver, final boolean capped) {
    final List<RID> rids = new ArrayList<>(rows.size());
    final float[] scores = new float[rows.size()];
    final List<Object> keys = new ArrayList<>(rows.size());
    for (int i = 0; i < rows.size(); i++) {
      final RidScore row = rows.get(i);
      rids.add(row.rid());
      scores[i] = row.score();
      keys.add(groupKeyResolver.apply(row.rid()));
    }
    planner.addSource(rids, scores, keys, capped);
  }

  /**
   * The query weights actually scored: the caller's, times each dim's IDF when the index was created with
   * {@code modifier = "IDF"}.
   */
  private float[] effectiveWeights(final int[] queryIndices, final float[] queryValues) {
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
    return effectiveWeights;
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

  private static PaginatedSparseVectorEngine openEngine(final LSMTreeIndex shell,
      final LSMSparseVectorIndexMetadata metadata) {
    // Already-written segments are self-describing (each header stores its own quantization code), so
    // the metadata quantization only governs how NEW segments (memtable flushes and compactions) are
    // encoded. A null metadata (legacy load path) falls back to the INT8 default.
    final SegmentParameters params = metadata == null ?
        SegmentParameters.defaults() :
        SegmentParameters.builder().weightQuantization(metadata.weightQuantization).build();
    return new PaginatedSparseVectorEngine(shell.getMutableIndex().getDatabase(), shell.getName(), params);
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
    final TransactionContext tx = underlyingIndex.getMutableIndex().getDatabase().getTransactionIfExists();
    if (tx != null && tx.getStatus() == TransactionContext.STATUS.BEGUN) {
      tx.addIndexOperation(this,
          add ? TransactionIndexContext.IndexKey.IndexKeyOperation.ADD
              : TransactionIndexContext.IndexKey.IndexKeyOperation.REMOVE,
          new Object[] { new SparsePostingReplayKey(dim, rid, weight) }, rid);
      // Pre-built callback key: this runs once per POSTING (hundreds per record on learned-sparse
      // corpora), so building the string here would concatenate millions of times per bulk load.
      tx.addAfterCommitCallbackIfAbsent(afterCommitFlushKey, engine::maybeFlush);
      return;
    }
    // #7933: no open transaction to queue onto, but the commit of one may still be in flight. An UPDATE indexes at
    // commit time, not at save() time - TransactionContext.commit1stPhase() drains its deferred writes through
    // updateRecordNoLock, which re-runs DocumentIndexer and lands right here, with the status no longer BEGUN - and
    // that is BEFORE the page versions are validated, so applying straight through would leak exactly what the
    // deferral below exists to prevent. This path is what a conflicted vector REWRITE travels.
    // record() carries a tripwire for a posting arriving after its transaction concluded. Should it ever fire, it
    // surfaces from inside commit1stPhase() - here, or through indexChanges.commit() below - not from wherever the
    // conclusion ran, which is where a reader would go looking first.
    final SparseVectorReplayBuffer buffer = replayBuffer(tx);
    if (buffer != null) {
      buffer.record(dim, rid, weight, !add);
      return;
    }

    if (add)
      engine.put(dim, rid, weight);
    else
      engine.remove(dim, rid);
  }

  /**
   * Takes delivery of a single posting carried through commit replay via {@link SparsePostingReplayKey}.
   * <p>
   * <b>Deferred, not applied (issue #7933).</b> The replay runs inside {@code commit1stPhase()} BEFORE the page
   * versions are validated, so a transaction that then loses the MVCC check has already replayed every one of its
   * postings - and this index's replay writes into the engine's process-wide {@link Memtable}, which no rollback
   * can reach. Buffering here and publishing from {@link SparseVectorReplayBuffer#publishIndexReplay()} - which
   * {@code TransactionContext.reset()} calls, and which a rolled back transaction never reaches - is what keeps an
   * aborted transaction's postings and tombstones out of the index. See {@link SparseVectorReplayBuffer} for why
   * deferring rather than journalling-and-undoing is the only correct answer for this index.
   * <p>
   * The direct branch is for a replay with no transaction driving it. Nothing in the engine does that today -
   * {@code TransactionIndexContext.commit()} is the only caller of {@code putReplay}/{@code removeReplay} - but a
   * marker reaching {@link #put} outside a commit must still land somewhere rather than be silently dropped.
   */
  private void applyReplayPosting(final SparsePostingReplayKey key, final boolean add) {
    // See the note in queueOrApply on where record()'s tripwire surfaces if it ever fires.
    final SparseVectorReplayBuffer buffer = replayBuffer();
    if (buffer != null) {
      buffer.record(key.dim(), key.rid(), key.weight(), !add);
      return;
    }

    if (add)
      engine.put(key.dim(), key.rid(), key.weight());
    else
      engine.remove(key.dim(), key.rid());
  }

  /**
   * This transaction's deferred-posting buffer, created and registered on first use, or null when no transaction is
   * replaying - in which case the caller applies straight through.
   * <p>
   * Gated on the commit being IN FLIGHT rather than merely on a transaction being present: that is what tells a
   * write which a conclusion is about to be applied to apart from one issued in an ordinary open transaction, whose
   * posting would be buffered against a conclusion that is not coming. {@code LSMVectorIndex.replayableTransaction()}
   * makes the same test for the same reason.
   * <p>
   * {@code getTransactionIfExists()} rather than {@code getTransaction()}: the latter THROWS
   * {@code TransactionException} on a thread with no database context rather than answering null, which would make
   * the null branch dead code and raise on a caller that legitimately has no transaction instead of letting it
   * write straight through (#7934 review). Same correction applied to {@link #queueOrApply}, whose own null check
   * had been dead for the same reason since it was written.
   */
  private SparseVectorReplayBuffer replayBuffer() {
    return replayBuffer(underlyingIndex.getMutableIndex().getDatabase().getTransactionIfExists());
  }

  /**
   * As {@link #replayBuffer()}, for a caller that has already resolved the thread's transaction. Resolving it costs
   * a thread-local lookup plus the database-identity checks {@code getTransactionIfExists()} makes, and this runs
   * once per POSTING - hundreds per record on a learned-sparse corpus - so the one caller that has the answer in
   * hand passes it rather than asking again (#7934 review).
   *
   * @param tx the thread's transaction, or null if it has none
   */
  private SparseVectorReplayBuffer replayBuffer(final TransactionContext tx) {
    if (tx == null || !isCommitInFlight(tx.getStatus()))
      return null;

    final IndexReplayConclusion registered = tx.getIndexReplayConclusion(this);
    if (registered != null)
      return (SparseVectorReplayBuffer) registered;

    final SparseVectorReplayBuffer created = new SparseVectorReplayBuffer(engine);
    tx.addIndexReplayConclusion(this, created);
    // Registered here as well as in queueOrApply, and keyed so the two never register it twice: a transaction whose
    // ONLY sparse writes are deferred updates (which never pass through the BEGUN branch) would otherwise leave the
    // memtable to be bounded by some later transaction's commit.
    tx.addAfterCommitCallbackIfAbsent(afterCommitFlushKey, engine::maybeFlush);
    return created;
  }

  /**
   * Whether a commit is under way on {@code status}, and so will conclude and apply that conclusion to this index.
   * <p>
   * Only {@code COMMIT_1ST_PHASE} is reachable today: both routes into the buffer run there, and nothing in
   * {@code commit2ndPhase()}/{@code publishCommittedPages()} writes to an index. The 2nd phase is admitted anyway
   * because the cost of being wrong is asymmetric (#7934 review): a status this test does not recognise falls
   * through to the direct-apply branch, which publishes into the shared memtable ahead of the conclusion and is
   * precisely the defect issue #7933 fixes, while a status it recognises too eagerly merely defers a write to the
   * end of the very commit that issued it. So the question asked is "is a commit in flight", not "is this the one
   * phase that writes indexes today".
   * <p>
   * Admitting the 2nd phase cannot strand a buffer that nothing will conclude, which is the one way the eager
   * reading would be the safer of the two: {@code reset()} publishes and clears the registrations and then, two
   * lines later and before it releases anything, sets the status to {@code INACTIVE}. There is no instant at which
   * a write sees {@code COMMIT_2ND_PHASE} on a transaction whose conclusion has already run.
   */
  private static boolean isCommitInFlight(final TransactionContext.STATUS status) {
    return status == TransactionContext.STATUS.COMMIT_1ST_PHASE || status == TransactionContext.STATUS.COMMIT_2ND_PHASE;
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
        """
        Direct posting lookup is not supported on LSM_SPARSE_VECTOR indexes; \
        use vector.sparseNeighbors(...) for top-K retrieval""");
  }

  /**
   * {@inheritDoc}
   * <p>
   * Delegates to {@link PaginatedSparseVectorEngine#livePostings()}, not to {@code totalPostings()}: the latter is
   * the sizing metric and counts tombstones, which is precisely the one thing {@link Index#countEntries()} says the
   * answer must not do, so the count settled on a residual after deletions instead of dropping (issue #7140).
   * As that contract warns, this walks the whole structure - never call it on a query path.
   */
  @Override
  public long countEntries() {
    try {
      return engine.livePostings();
    } catch (final IOException e) {
      throw new IndexException("Error on counting the live entries of index '" + getName() + "'", e);
    }
  }

  @Override
  public boolean compact() throws IOException, InterruptedException {
    return engine.compactAll() != -1L;
  }

  @Override
  public IndexMetadata getMetadata() {
    return underlyingIndex.getMetadata();
  }

  /**
   * The dimensionality, the scoring modifier and the weight quantization live here, not on the underlying LSM-Tree, so
   * a site carrying this definition into a new index file has to read them from this instance (issue #5723).
   */
  @Override
  public IndexMetadata getMetadataForNewFile() {
    return sparseMetadata != null ? sparseMetadata : underlyingIndex.getMetadata();
  }

  @Override
  public boolean isCompacting() {
    return underlyingIndex.isCompacting();
  }

  /**
   * Delegated rather than left at the interface default: a sparse-vector posting key is a dimension identifier, so
   * the key-order mismatch of #5802 should never arise here - but that is an invariant about the keys, not a property
   * of this class, and answering {@code null} unconditionally would hide the mismatch if it ever did.
   * <p>
   * Also folds in {@link PaginatedSparseVectorEngine#segmentTrust()} (issue #6566): a segment merged before the
   * recency-epoch fix of #6379 can still return a document that was deleted, and until now the only way to learn
   * that was the once-per-instance WARNING log line at open/refresh time - unqueryable once the moment passed, and
   * with no way to confirm a {@code REBUILD INDEX} actually cleared it. This makes the same condition answer through
   * the surface every other index upgrade warning already uses: {@code schema:indexes}, {@code schema:index:<name>},
   * Studio, and the HTTP admin API, all of which read {@link IndexInternal#getUpgradeWarning()} today.
   * <p>
   * Deliberately says nothing about WHAT to run: {@link #getName()} on this class is the physical per-bucket
   * sub-index name, not the logical index {@code REBUILD INDEX} takes, and the contract on
   * {@link IndexInternal#getUpgradeWarning()} is explicit that implementations say what is lost and why, not what to
   * type - the caller ({@code LocalSchema.reportUpgradeWarning()}) resolves the logical name itself and appends the
   * actual command, the same way {@link com.arcadedb.index.geospatial.LSMTreeGeoIndex#getUpgradeWarning()} leaves it
   * (PR #6720 review: embedding a second, physical-named "Run 'REBUILD INDEX ...'" here would have shown an operator
   * two different targets in the same message).
   */
  @Override
  public String getUpgradeWarning() {
    final String delegated = underlyingIndex.getUpgradeWarning();
    final PaginatedSparseVectorEngine.SegmentTrustSnapshot trust = engine.segmentTrust();
    if (trust.untrusted() == 0)
      return delegated;
    final String own = "%d of %d segment(s) carry a precedence that predates the recency-epoch fix (issue #6379); a "
        + "document deleted before those segments were merged may still be returned by queries.".formatted(
        trust.untrusted(), trust.total());
    return delegated == null ? own : delegated + " " + own;
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
    // One snapshot for both, so the two numbers can never straddle a compaction landing in between (PR #6720 review).
    final PaginatedSparseVectorEngine.SegmentTrustSnapshot trust = engine.segmentTrust();
    stats.put("segmentCount", (long) trust.total());
    // Issue #6566: a queryable counterpart to the once-per-instance WARNING log line, so a monitoring system (or an
    // operator re-checking after a REBUILD INDEX) can ask "is this index still affected" rather than grep for it.
    stats.put("untrustedSegments", (long) trust.untrusted());
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

  /**
   * Sparse postings ride the transaction's append-only lane (issue #5411). Both preconditions
   * hold: the index is never unique (see {@link LSMSparseVectorIndexFactoryHandler}), and it has
   * no in-transaction read path - {@link #get(Object[])} rejects direct lookups outright, and
   * top-K retrieval reads the engine's memtable + segments, never the transaction overlay.
   * <p>
   * The win is structural rather than constant-factor: one record contributes one entry per
   * non-zero dimension, so the ordered lane's {@code O(log n)} key comparison plus per-key
   * {@code HashMap} were paid hundreds of times per record. On a 100k x 130-nnz load that put
   * ~87% of build wall-clock inside {@code TransactionIndexContext} against ~14% in the memtable
   * doing the actual work. Insertion-order replay also expresses the intended semantics directly
   * ("the last operation on a posting wins") instead of relying on dedup-map overwrite plus the
   * ordered lane's two-phase REMOVE-then-ADD split.
   */
  @Override
  public boolean isTransactionKeyOrderRequired() {
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
    // Delegated rather than re-derived from getPropertyNames(): the list is never null (IndexMetadata coerces a
    // missing one to empty), so the local test was the same always-true answer issue #5780 removed.
    return underlyingIndex.isAutomatic();
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
  public long build(final int buildIndexBatchSize, final boolean sharesCallerTransaction,
      final BuildIndexCallback callback) {
    return underlyingIndex.build(buildIndexBatchSize, sharesCallerTransaction, callback);
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
      json.put("weightQuantization", sparseMetadata.weightQuantization.name());
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
