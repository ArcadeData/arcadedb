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

import com.arcadedb.database.RID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Top-K scoring with Block-Max MaxScore DAAT (document-at-a-time) over merged dim cursors.
 * <p>
 * <b>Why MaxScore and not WAND.</b> The first implementation used pivot-based (Block-Max) WAND.
 * WAND keeps every query term in the traversal and advances a pivot document; on learned-sparse
 * vectors (SPLADE and friends) that degenerates. Such queries carry 30-120 active terms with
 * relatively flat weights, and a handful of very high document-frequency expansion terms carry
 * almost all of the posting mass. Those dense terms stay inside the pivot prefix forever and get
 * skip-seeked a few documents at a time; because their lists are dense, each seek lands in the next
 * block, so effectively every block of every term is decoded. That is the "latency is a near-pure
 * function of the summed posting length" behaviour reported in issue #5388 (Spearman 0.95), and
 * per-block maxima cannot rescue it: real SPLADE weights are near-uniform <i>within</i> a block, so
 * a block bound is no tighter than the term's global bound and the block-skip never fires.
 * <p>
 * MaxScore (Turtle &amp; Flynn) attacks the same problem from the term side instead of the document
 * side, which is the dimension that actually discriminates here.
 * <p>
 * Measured on a 200k-document corpus with this shape (48 query terms, flat query weights, Zipf-like
 * document frequencies, top-10): 26.3 ms/query and 3006 of 3007 blocks decoded before, 7.6 ms/query
 * and 1538 of 3007 blocks decoded after, with byte-identical results. See
 * {@code MaxScorePruningTest#spladeShapedQueryCost} to reproduce.
 * <p>
 * Algorithm overview:
 * <ol>
 *   <li>Open one {@link DimCursor} per query dim. Each cursor merges across all sources. Compute
 *       each term's maximum possible contribution {@code sigma = queryWeight * globalMaxWeight} and
 *       order the terms by how much traversal work each would remove per unit of pruning budget it
 *       consumes (see {@link DimEntry#BY_PRUNING_VALUE}).</li>
 *   <li>Split the terms into a <b>non-essential</b> prefix {@code [0, split)} and an
 *       <b>essential</b> suffix {@code [split, n)}, where {@code split} is the largest index whose
 *       prefix-sum of {@code sigma} still fits under the current top-K threshold. A document that
 *       matches non-essential terms only cannot possibly reach the threshold, so non-essential
 *       terms stop generating candidates entirely - their posting lists are never traversed, only
 *       point-probed. This is where the posting mass of the head terms disappears.</li>
 *   <li>The next candidate is the smallest current RID across the essential cursors only.</li>
 *   <li><b>Block-max shallow advance:</b> sum the <i>tight</i> per-block maxima of the essential
 *       cursors aligned at the candidate plus the non-essential ceiling. If that cannot beat the
 *       threshold, no document up to the limiting block boundary can, so skip the whole range
 *       reading in-memory block headers alone - no payload is decoded. (This is the Block-Max half
 *       of Block-Max MaxScore; it is what keeps the tail-spike corpora of the first round pruned.)</li>
 *   <li>Otherwise score the essential terms aligned at the candidate, then walk the non-essential
 *       terms back towards the head of the order, point-probing each with a forward seek, and
 *       <b>abandon the document</b> as soon as the partial score plus the remaining non-essential
 *       ceiling drops to the threshold. On a flat-weight query this abandons after one or two
 *       probes, so the head terms are touched for a negligible number of documents.</li>
 *   <li>Advance the essential cursors aligned at the candidate and repeat. The split is recomputed
 *       whenever the threshold rises; it moves in one direction only, so a term never re-enters the
 *       traversal.</li>
 * </ol>
 * <p>
 * <b>Tombstone semantics.</b> A tombstone observed on any one of the cursors visited at the
 * candidate RID skips the whole document - the loop drops the candidate from this query without
 * scoring even the dims that have live postings under the same RID. This is the
 * whole-document-delete contract documented on
 * {@link PaginatedSparseVectorEngine#put(int, com.arcadedb.database.RID, float)} and
 * {@link PaginatedSparseVectorEngine#remove(int, com.arcadedb.database.RID)}: the engine treats
 * a tombstone as "this RID is gone", not "this one dim of this RID is gone". Partial-dim updates
 * are not supported; rewrite the document's full posting set instead. A tombstone that sits on a
 * term the traversal abandoned early is immaterial: the abandon already proved the document cannot
 * enter the result set.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class BmwScorer {

  private BmwScorer() {
    // utility class
  }

  /**
   * Top-K Block-Max MaxScore DAAT over the merged sources for each query dim. Sources for each dim
   * are passed implicitly via {@link DimCursor}: the caller assembles those (typically once per
   * query) by constructing a {@link DimCursor} with the per-dim {@link SourceCursor} list.
   * <p>
   * The caller passes parallel arrays {@code queryDims}, {@code queryWeights} of identical length;
   * each dim must be unique. The {@code cursors} array is parallel to those (one cursor per dim),
   * with {@code null} for dims absent from every source.
   *
   * @return list of up to {@code k} (RID, score) pairs sorted by score descending.
   * @throws IllegalArgumentException if the three input arrays have mismatched lengths, or any
   *                                  query weight is NaN, infinite, or negative. Dynamic pruning
   *                                  requires the per-dim contribution upper bound to be
   *                                  monotonically non-decreasing in the accumulated sum, which a
   *                                  negative weight would break (the essential/non-essential split
   *                                  would no longer be a valid bound and the result set would
   *                                  silently be wrong).
   * @throws IOException              if a {@link DimCursor#start} / {@link DimCursor#advance} /
   *                                  {@link DimCursor#seekTo} fails to read its underlying source.
   */
  public static List<RidScore> topK(final int[] queryDims, final float[] queryWeights, final DimCursor[] cursors, final int k)
      throws IOException {
    validate(queryDims, queryWeights, cursors);
    if (k <= 0)
      return List.of();

    final DimEntry[] terms = openTerms(queryWeights, cursors);
    if (terms.length == 0)
      return List.of();

    final TopKCollector collector = new TopKCollector(k);
    scan(terms, collector);
    return collector.drain();
  }

  /**
   * Top-K with traversal-integrated {@code groupBy} / {@code groupSize} (issue #4071). Replaces the
   * global K-heap with a per-group min-heap so the post-traversal filter that the MVP applied on top
   * of {@link #topK} no longer needs an over-fetched candidate pool. The {@code groupKeyResolver} is
   * consulted once per scored document; the resolver typically reads the group field off the
   * materialised record, so callers should keep it cheap.
   * <p>
   * <b>Threshold semantics with per-group state.</b> The pruning threshold is a lower bound on any
   * score that could still enter the result set. For non-grouped top-K that is the K-th best score
   * so far; for grouped top-K the analogue is "the lowest score that could replace any group's
   * worst member". Until {@code limit} groups have all reached {@code groupSize} (so any candidate
   * could open a new group or fill an empty slot), the threshold stays at
   * {@link Float#NEGATIVE_INFINITY}, which keeps every term essential and the traversal exhaustive -
   * exactly the behaviour correctness requires. Once globally full, the threshold is the minimum
   * across per-group worst scores - any score below it cannot beat any group's worst, so the
   * essential/non-essential split and the block-max skip can prune against it. The threshold is
   * conservative (a candidate above it may still be rejected because its specific group has a higher
   * worst), which is fine: pruning is correct, just slightly less aggressive than non-grouped top-K.
   * <p>
   * <b>{@code allowedRIDs} filter.</b> Applied inline in the scoring branch: a candidate RID outside
   * the whitelist is dropped before the non-essential probe walk even starts (cursors still advance
   * so the loop progresses). This removes the over-fetch + post-filter pattern that
   * {@link LSMSparseVectorIndex#topK} used to compensate for highly selective filters.
   *
   * @param queryDims        query dim ids
   * @param queryWeights     query weights, parallel to {@code queryDims}; must be non-negative
   * @param cursors          per-dim cursors, parallel to {@code queryDims}; nulls allowed for dims
   *                         absent from every source
   * @param limit            max number of distinct groups to return
   * @param groupSize        max records per group
   * @param groupKeyResolver maps a candidate RID to its group key; {@code null} group keys are
   *                         allowed (treated as the "null" group), matching the MVP's HashMap
   *                         null-key handling
   * @param allowedRIDs      optional RID whitelist; {@code null} or empty means no restriction
   *
   * @return at most {@code limit * groupSize} (RID, score) pairs sorted by score descending. Each
   *         distinct group key in the result has at most {@code groupSize} entries and the result
   *         covers at most {@code limit} distinct groups.
   *
   * @throws IllegalArgumentException if input arrays mismatch length, query weights are NaN /
   *                                  infinite / negative, or {@code groupKeyResolver} is null.
   * @throws IOException              propagated from the underlying cursor reads.
   */
  public static List<RidScore> topKGrouped(final int[] queryDims, final float[] queryWeights, final DimCursor[] cursors,
      final int limit, final int groupSize, final Function<RID, Object> groupKeyResolver, final Set<RID> allowedRIDs)
      throws IOException {
    validate(queryDims, queryWeights, cursors);
    if (groupKeyResolver == null)
      throw new IllegalArgumentException("groupKeyResolver must not be null");
    if (limit <= 0 || groupSize <= 0)
      return List.of();

    final DimEntry[] terms = openTerms(queryWeights, cursors);
    if (terms.length == 0)
      return List.of();

    final GroupedCollector collector = new GroupedCollector(limit, groupSize, groupKeyResolver, allowedRIDs);
    scan(terms, collector);
    return collector.drain();
  }

  // ---------- traversal ----------

  /**
   * Block-Max MaxScore traversal. {@code terms} is in promotion order (see
   * {@link DimEntry#BY_PRUNING_VALUE}); the collector owns the result set and publishes the current
   * pruning threshold.
   * <p>
   * The essential terms are kept in a binary min-heap keyed by current RID rather than rescanned
   * linearly for every candidate. On a learned-sparse query the essential set still holds dozens of
   * terms while only one or two of them align on any given document, so a linear rescan costs
   * O(terms) per document three times over (find the minimum, score the aligned run, advance it) and
   * dominates everything else once the posting mass has been pruned away. The heap turns that into
   * O(aligned * log terms).
   */
  private static void scan(final DimEntry[] terms, final Collector collector) throws IOException {
    final int n = terms.length;
    // prefix[i] == sum of sigma over terms [0, i). prefix[n] is the whole query's ceiling.
    final float[] prefix = new float[n + 1];
    recomputePrefix(terms, prefix);

    final int[] heap = new int[n];     // essential term indices, min-heap by current RID
    final int[] aligned = new int[n];  // scratch: the term indices sitting on the current candidate
    // Cursor positions, mirrored into flat arrays indexed by term. The heap reads a position far
    // more often than a cursor moves - about a dozen comparisons per posting consumed - and reading
    // it off the cursor costs two dependent loads through scattered objects. Mirroring turns each
    // comparison into two loads from a pair of arrays that stay in L1 for any realistic term count,
    // and the mirrors are refreshed only where a cursor actually moves (issue #5467).
    final int[] keyBucketIds = new int[n];
    final long[] keyPositions = new long[n];
    for (int i = 0; i < n; i++)
      syncKey(terms, keyBucketIds, keyPositions, i);
    int heapSize = 0;
    int split = 0;
    float lastThreshold = Float.NEGATIVE_INFINITY;
    boolean splitDirty = true;
    boolean heapDirty = true;

    while (true) {
      final float threshold = collector.threshold();
      if (splitDirty || threshold > lastThreshold) {
        lastThreshold = threshold;
        splitDirty = false;
        // The threshold only ever rises and the prefix bounds only ever shrink, so the split only
        // ever moves right: a term that left the traversal never re-enters it.
        final int before = split;
        while (split < n && prefix[split + 1] <= threshold)
          split++;
        if (split >= n)
          return;  // even every term at its maximum cannot beat the threshold: nothing left to find.
        if (split != before)
          heapDirty = true;
      }
      if (heapDirty) {
        heapSize = buildHeap(terms, keyBucketIds, keyPositions, split, n, heap);
        heapDirty = false;
      }
      if (heapSize == 0)
        return;  // every essential term is exhausted.

      // Next candidate: the smallest current RID across the essential terms. Non-essential terms do
      // not generate candidates - a document they alone match is bounded by prefix[split], which is
      // at or below the threshold by construction of the split.
      final int candidateBucketId = keyBucketIds[heap[0]];
      final long candidatePosition = keyPositions[heap[0]];

      // Detach the whole aligned run so the cursors can be moved without corrupting the heap order.
      int alignedCount = 0;
      while (heapSize > 0) {
        final int top = heap[0];
        if (keyPositions[top] != candidatePosition || keyBucketIds[top] != candidateBucketId)
          break;
        aligned[alignedCount++] = heap[0];
        heapSize = popHeap(keyBucketIds, keyPositions, heap, heapSize);
      }
      final int nextEssentialBucketId = heapSize > 0 ? keyBucketIds[heap[0]] : -1;
      final long nextEssentialPosition = heapSize > 0 ? keyPositions[heap[0]] : -1L;

      if (!tryBlockMaxSkip(terms, keyBucketIds, keyPositions, aligned, alignedCount, prefix[split], candidateBucketId,
          candidatePosition, threshold, nextEssentialBucketId, nextEssentialPosition))
        scoreCandidate(terms, keyBucketIds, keyPositions, aligned, alignedCount, split, prefix, candidateBucketId,
            candidatePosition, threshold, collector);

      // Re-attach whatever is still live. An exhausted term contributes nothing from here on:
      // dropping its ceiling tightens every prefix bound, which can only push the split further
      // right on the next pass.
      boolean exhaustedAny = false;
      for (int j = 0; j < alignedCount; j++) {
        final int idx = aligned[j];
        if (terms[idx].cursor.isExhausted())
          exhaustedAny |= terms[idx].clearSigma();
        else
          heapSize = pushHeap(keyBucketIds, keyPositions, heap, heapSize, idx);
      }
      if (exhaustedAny) {
        recomputePrefix(terms, prefix);
        splitDirty = true;
      }
    }
  }

  /**
   * Score {@code candidate}: sum the essential terms aligned on it, then probe the non-essential
   * terms from the highest {@code sigma} down, abandoning the document as soon as the partial score
   * plus the remaining non-essential ceiling can no longer beat {@code threshold}. Finally advance
   * every aligned cursor so the traversal makes progress.
   */
  private static void scoreCandidate(final DimEntry[] terms, final int[] keyBucketIds, final long[] keyPositions,
      final int[] aligned, final int alignedCount, final int split, final float[] prefix, final int candidateBucketId,
      final long candidatePosition, final float threshold, final Collector collector) throws IOException {
    boolean alive = true;
    float score = 0.0f;
    for (int j = 0; j < alignedCount; j++) {
      final DimEntry t = terms[aligned[j]];
      if (t.cursor.isTombstone()) {
        alive = false;
        break;
      }
      score += t.queryWeight * t.cursor.currentWeight();
    }

    // The RID object is built once, and only for a candidate that survived the aligned-run scan -
    // the traversal itself never materialises one (issue #5467).
    RID candidate = null;
    if (alive) {
      candidate = new RID(candidateBucketId, candidatePosition);
      alive = collector.accepts(candidate);
    }
    if (alive) {
      for (int i = split - 1; i >= 0; i--) {
        // Early abandon: even taking the maximum of every non-essential term still on the walk, the
        // document cannot reach the threshold. Nothing below i needs to be touched - which is what
        // keeps the fat posting lists out of the query on flat weight distributions.
        if (score + prefix[i + 1] <= threshold) {
          alive = false;
          break;
        }
        final DimCursor c = terms[i].cursor;
        if (c.isExhausted())
          continue;
        c.seekTo(candidateBucketId, candidatePosition);
        if (c.isExhausted() || c.currentPosition() != candidatePosition || c.currentBucketId() != candidateBucketId)
          continue;
        if (c.isTombstone()) {
          alive = false;
          break;
        }
        score += terms[i].queryWeight * c.currentWeight();
      }
      if (alive)
        collector.collect(candidate, score);
    }

    for (int j = 0; j < alignedCount; j++) {
      final int idx = aligned[j];
      terms[idx].cursor.advance();
      syncKey(terms, keyBucketIds, keyPositions, idx);
    }
  }

  /**
   * Block-max shallow advance, the Block-Max half of Block-Max MaxScore. Sums the <i>tight</i>
   * per-block maxima of the essential cursors aligned at {@code candidate} on top of
   * {@code nonEssentialCeiling} (the maximum the whole non-essential prefix could ever add) and
   * compares against {@code threshold}:
   * <ul>
   *   <li>If the bound exceeds the threshold, {@code candidate} might still make the result set;
   *       return {@code false} so the caller scores it normally.</li>
   *   <li>Otherwise no document in {@code [candidate, minBlockEnd]} can beat the threshold, so seek
   *       the aligned cursors past the limiting block boundary - bounded by {@code nextEssential},
   *       the first essential cursor sitting strictly after {@code candidate}, since that one could
   *       align a document inside the range. Return {@code true}.</li>
   * </ul>
   * The skip target is always strictly greater than {@code candidate}, so the traversal cannot
   * stall. Block maxima are read from in-memory headers only - no posting payload is decoded.
   * <p>
   * This is the step that carries corpora whose weights <i>do</i> vary within a block. On real
   * learned-sparse data they barely do, which is why the term-level split above has to do the work.
   *
   * @return {@code true} if a block range was skipped, {@code false} if the caller must score.
   */
  private static boolean tryBlockMaxSkip(final DimEntry[] terms, final int[] keyBucketIds, final long[] keyPositions,
      final int[] aligned, final int alignedCount, final float nonEssentialCeiling, final int candidateBucketId,
      final long candidatePosition, final float threshold, final int nextEssentialBucketId, final long nextEssentialPosition)
      throws IOException {
    float bound = nonEssentialCeiling;
    if (bound > threshold)
      return false;  // no headroom at all (threshold still NEGATIVE_INFINITY, or an all-essential query).

    RID minBlockEnd = null;
    for (int j = 0; j < alignedCount; j++) {
      final DimEntry t = terms[aligned[j]];
      bound += t.queryWeight * t.cursor.blockMaxAt(candidateBucketId, candidatePosition);
      if (bound > threshold)
        return false;
      final RID be = t.cursor.blockEndAt(candidateBucketId, candidatePosition);
      if (be != null && (minBlockEnd == null || SparseSegmentBuilder.compareRid(be, minBlockEnd) < 0))
        minBlockEnd = be;
    }

    if (minBlockEnd == null)
      return false;  // no finite block boundary (only loose/memtable sources) - cannot block-skip.

    // RID successor is (bucket, position + 1); seekTo lands on the first real posting >= that, i.e.
    // the first strictly greater than minBlockEnd.
    int targetBucketId = minBlockEnd.getBucketId();
    long targetPosition = minBlockEnd.getPosition() + 1;
    if (nextEssentialBucketId >= 0
        && SparseSegmentBuilder.compareRid(nextEssentialBucketId, nextEssentialPosition, targetBucketId, targetPosition) < 0) {
      targetBucketId = nextEssentialBucketId;
      targetPosition = nextEssentialPosition;
    }

    for (int j = 0; j < alignedCount; j++) {
      final int idx = aligned[j];
      terms[idx].cursor.seekTo(targetBucketId, targetPosition);
      syncKey(terms, keyBucketIds, keyPositions, idx);
    }
    return true;
  }

  // ---------- essential-term min-heap (indices into {@code terms}, ordered by current RID) ----------

  private static int buildHeap(final DimEntry[] terms, final int[] keyBucketIds, final long[] keyPositions, final int split,
      final int n, final int[] heap) {
    int size = 0;
    for (int i = split; i < n; i++)
      if (!terms[i].cursor.isExhausted())
        heap[size++] = i;
    for (int i = (size >> 1) - 1; i >= 0; i--)
      siftDown(keyBucketIds, keyPositions, heap, size, i);
    return size;
  }

  /**
   * Removes the minimum. Returns the new size.
   * <p>
   * Uses the bottom-up (Floyd) variant: descend to a leaf always following the smaller child, then
   * sift the promoted element back up from there. A textbook sift-down spends two comparisons per
   * level - one to pick the smaller child, one to test it against the element being pushed down -
   * whereas this spends one per level going down plus, in the common case, one or two coming back
   * up. The element promoted here is the heap's last, which tends to be large and therefore belongs
   * deep, so the return trip is usually trivial. On a learned-sparse query this comparison is run
   * millions of times per query (issue #5467), and it is the traversal's single hottest operation.
   */
  private static int popHeap(final int[] keyBucketIds, final long[] keyPositions, final int[] heap, final int size) {
    final int newSize = size - 1;
    final int promoted = heap[newSize];
    if (newSize > 1) {
      int i = 0;
      int left = 1;
      while (left < newSize) {
        final int right = left + 1;
        final int child = (right < newSize && compareByRid(keyBucketIds, keyPositions, heap[right], heap[left]) < 0) ? right : left;
        heap[i] = heap[child];
        i = child;
        left = (i << 1) + 1;
      }
      heap[i] = promoted;
      while (i > 0) {
        final int parent = (i - 1) >>> 1;
        if (compareByRid(keyBucketIds, keyPositions, heap[i], heap[parent]) >= 0)
          break;
        final int tmp = heap[i];
        heap[i] = heap[parent];
        heap[parent] = tmp;
        i = parent;
      }
    } else if (newSize == 1) {
      heap[0] = promoted;
    }
    return newSize;
  }

  /** Inserts a term index. Returns the new size. */
  private static int pushHeap(final int[] keyBucketIds, final long[] keyPositions, final int[] heap, final int size,
      final int termIdx) {
    int i = size;
    heap[i] = termIdx;
    while (i > 0) {
      final int parent = (i - 1) >>> 1;
      if (compareByRid(keyBucketIds, keyPositions, heap[i], heap[parent]) >= 0)
        break;
      final int tmp = heap[i];
      heap[i] = heap[parent];
      heap[parent] = tmp;
      i = parent;
    }
    return size + 1;
  }

  private static void siftDown(final int[] keyBucketIds, final long[] keyPositions, final int[] heap, final int size,
      final int from) {
    int i = from;
    while (true) {
      final int left = (i << 1) + 1;
      if (left >= size)
        return;
      int smallest = left;
      final int right = left + 1;
      if (right < size && compareByRid(keyBucketIds, keyPositions, heap[right], heap[left]) < 0)
        smallest = right;
      if (compareByRid(keyBucketIds, keyPositions, heap[i], heap[smallest]) <= 0)
        return;
      final int tmp = heap[i];
      heap[i] = heap[smallest];
      heap[smallest] = tmp;
      i = smallest;
    }
  }

  private static int compareByRid(final int[] keyBucketIds, final long[] keyPositions, final int a, final int b) {
    return SparseSegmentBuilder.compareRid(keyBucketIds[a], keyPositions[a], keyBucketIds[b], keyPositions[b]);
  }

  /** Refresh term {@code i}'s mirrored position after its cursor moved. */
  private static void syncKey(final DimEntry[] terms, final int[] keyBucketIds, final long[] keyPositions, final int i) {
    final DimCursor c = terms[i].cursor;
    keyBucketIds[i] = c.currentBucketId();
    keyPositions[i] = c.currentPosition();
  }

  // ---------- setup ----------

  private static void validate(final int[] queryDims, final float[] queryWeights, final DimCursor[] cursors) {
    if (queryDims.length != queryWeights.length || queryWeights.length != cursors.length)
      throw new IllegalArgumentException("queryDims, queryWeights, cursors must have the same length");
    // Dynamic pruning relies on the accumulated {@code queryWeight * upperBound} being monotonically
    // non-decreasing per added dim, so a negative query weight would let the running sum drop, the
    // essential/non-essential split would stop being a valid bound, and the result set would
    // silently be wrong. Match the non-negativity contract enforced by
    // {@link com.arcadedb.index.sparsevector.LSMSparseVectorIndex#put} on the document side.
    for (final float w : queryWeights) {
      if (Float.isNaN(w) || Float.isInfinite(w))
        throw new IllegalArgumentException("query weights must be finite numbers; got " + w);
      if (w < 0.0f)
        throw new IllegalArgumentException("query weights must be non-negative; got " + w);
    }
  }

  /**
   * Start every non-null cursor, drop the ones with nothing to iterate, and order the survivors by
   * {@link DimEntry#BY_PRUNING_VALUE promotion order}, i.e. by how much traversal work each term
   * would remove per unit of pruning budget it consumes.
   */
  private static DimEntry[] openTerms(final float[] queryWeights, final DimCursor[] cursors) throws IOException {
    final List<DimEntry> live = new ArrayList<>(cursors.length);
    for (int i = 0; i < cursors.length; i++) {
      if (cursors[i] == null)
        continue;
      cursors[i].start();
      if (cursors[i].isExhausted())
        continue;
      live.add(new DimEntry(cursors[i], queryWeights[i]));
    }
    final DimEntry[] terms = live.toArray(new DimEntry[0]);
    Arrays.sort(terms, DimEntry.BY_PRUNING_VALUE);
    return terms;
  }

  private static void recomputePrefix(final DimEntry[] terms, final float[] prefix) {
    prefix[0] = 0.0f;
    for (int i = 0; i < terms.length; i++)
      prefix[i + 1] = prefix[i] + terms[i].sigma;
  }

  // ---------- collectors ----------

  /**
   * Result accumulator, and the owner of the pruning threshold the traversal prunes against. The
   * threshold must be monotonically non-decreasing: the split point that drops terms out of the
   * traversal is derived from it and is never walked back.
   */
  private interface Collector {
    /** Lower bound on any score that can still enter the result set. Never decreases. */
    float threshold();

    /** Pre-scoring admission test (RID whitelist); a rejected candidate skips the probe walk. */
    boolean accepts(RID rid);

    void collect(RID rid, float score);
  }

  /** Result ordering handed back to the caller: best score first. */
  private static final Comparator<RidScore> BY_SCORE_DESC = (a, b) -> Float.compare(b.score(), a.score());

  /** Plain top-K: a K-sized min-heap whose head is the threshold once full. */
  private static final class TopKCollector implements Collector {
    private final RidScoreMinHeap heap;
    private float                 threshold = Float.NEGATIVE_INFINITY;

    TopKCollector(final int k) {
      this.heap = new RidScoreMinHeap(k);
    }

    @Override
    public float threshold() {
      return threshold;
    }

    @Override
    public boolean accepts(final RID rid) {
      return true;
    }

    @Override
    public void collect(final RID rid, final float score) {
      // Below capacity the candidate is always retained and the threshold stays at
      // NEGATIVE_INFINITY (anything can still enter). Once full, the heap's minimum *is* the
      // threshold, and offer() admits exactly the candidates that beat it.
      if (heap.offer(rid, score) && heap.isFull())
        threshold = heap.minScore();
    }

    List<RidScore> drain() {
      final List<RidScore> out = new ArrayList<>(heap.size());
      heap.drainInto(out);
      out.sort(BY_SCORE_DESC);
      return out;
    }
  }

  /** Grouped top-K: one min-heap per group key, threshold = min across per-group worst scores. */
  private static final class GroupedCollector implements Collector {
    private final int                                        limit;
    private final int                                        groupSize;
    private final Function<RID, Object>                      groupKeyResolver;
    private final Set<RID>                                   allowedRIDs;
    private final boolean                                    filterActive;
    private final HashMap<Object, RidScoreMinHeap>           groups;
    private int                                              filledGroups;
    private float                                            threshold = Float.NEGATIVE_INFINITY;

    GroupedCollector(final int limit, final int groupSize, final Function<RID, Object> groupKeyResolver,
        final Set<RID> allowedRIDs) {
      this.limit = limit;
      this.groupSize = groupSize;
      this.groupKeyResolver = groupKeyResolver;
      this.allowedRIDs = allowedRIDs;
      this.filterActive = allowedRIDs != null && !allowedRIDs.isEmpty();
      this.groups = new HashMap<>(limit);
    }

    @Override
    public float threshold() {
      return threshold;
    }

    @Override
    public boolean accepts(final RID rid) {
      return !filterActive || allowedRIDs.contains(rid);
    }

    @Override
    public void collect(final RID rid, final float score) {
      final Object groupKey = groupKeyResolver.apply(rid);
      final RidScoreMinHeap group = groups.get(groupKey);
      boolean stateChanged = false;
      if (group == null) {
        if (groups.size() < limit) {
          final RidScoreMinHeap opened = new RidScoreMinHeap(groupSize);
          opened.offer(rid, score);
          groups.put(groupKey, opened);
          if (opened.isFull())  // groupSize == 1: the group is already at capacity.
            filledGroups++;
          stateChanged = true;
        }
        // else: limit groups already open and this one is a new key - reject.
      } else {
        final boolean wasFull = group.isFull();
        stateChanged = group.offer(rid, score);
        if (!wasFull && group.isFull())
          filledGroups++;
      }
      // Recompute the global threshold once every group has reached capacity. Until then it stays
      // at NEGATIVE_INFINITY: a candidate could still open a new group or fill an empty slot inside
      // an existing one, so pruning against a per-group watermark would be incorrect.
      if (stateChanged && filledGroups == limit && groups.size() == limit) {
        float min = Float.POSITIVE_INFINITY;
        for (final RidScoreMinHeap pq : groups.values()) {
          if (!pq.isEmpty() && pq.minScore() < min)
            min = pq.minScore();
        }
        if (min != Float.POSITIVE_INFINITY && min > threshold)
          threshold = min;
      }
    }

    List<RidScore> drain() {
      int total = 0;
      for (final RidScoreMinHeap pq : groups.values())
        total += pq.size();
      final List<RidScore> out = new ArrayList<>(total);
      for (final RidScoreMinHeap pq : groups.values())
        pq.drainInto(out);
      out.sort(BY_SCORE_DESC);
      return out;
    }
  }

  /**
   * Per-term entry. {@code sigma} is the term's maximum possible contribution to any document's
   * score, captured once at traversal start from {@link DimCursor#upperBoundRemaining()} (which at
   * that point is the dim's global maximum across every source). The cursor's own
   * {@link DimCursor#isExhausted} stays the source of truth on exhaustion; we do not duplicate that
   * flag here.
   */
  private static final class DimEntry {
    /**
     * Promotion order: the order in which terms are allowed to leave the traversal. Correctness
     * only constrains the <i>sum</i> of the non-essential ceilings, never which terms make up the
     * set, so the order is free to be chosen for cost.
     * <p>
     * Textbook MaxScore sorts by {@code sigma} ascending, on the reasoning that the terms that
     * contribute least should be the first to go. On learned-sparse data that ordering is actively
     * counter-productive, and measurably so: the maximum weight of a term is the maximum over its
     * whole posting list, so the terms with millions of postings almost always hold the highest
     * maximum too. Sorting by {@code sigma} ascending therefore keeps exactly the fattest posting
     * lists inside the traversal and drops the cheap ones - the traversal cost barely moves, which
     * is what issue #5388 measured.
     * <p>
     * This orders by <i>skipped postings per unit of ceiling spent</i> instead - the classic greedy
     * for "maximise the work removed under a budget". A term with no measurable ceiling
     * ({@code sigma == 0}, e.g. a zero query weight) sorts first: it can leave the traversal for
     * free. Ties fall back to the textbook {@code sigma} ascending.
     */
    static final Comparator<DimEntry> BY_PRUNING_VALUE = Comparator.<DimEntry>comparingDouble(e -> -e.pruningValue())
        .thenComparingDouble(e -> e.sigma);

    final DimCursor cursor;
    final float     queryWeight;
    final long      df;
    float           sigma;

    DimEntry(final DimCursor cursor, final float queryWeight) {
      this.cursor = cursor;
      this.queryWeight = queryWeight;
      this.sigma = queryWeight * cursor.upperBoundRemaining();
      this.df = cursor.documentFrequency();
    }

    /**
     * Postings this term would stop traversing per unit of pruning budget it consumes. Sources that
     * cannot report a posting count in O(1) (the memtable) report 0, which floors the estimate at
     * one posting so such a term is promoted last rather than first - conservative, and memtables
     * are small by construction.
     */
    private double pruningValue() {
      final long cost = Math.max(df, 1L);
      return sigma <= 0.0f ? Double.POSITIVE_INFINITY : cost / (double) sigma;
    }

    /** Zeroes an exhausted term's ceiling. Returns true if this changed anything. */
    boolean clearSigma() {
      if (sigma == 0.0f)
        return false;
      sigma = 0.0f;
      return true;
    }
  }
}
