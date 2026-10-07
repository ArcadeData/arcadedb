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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.RID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
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
 * <b>Tombstone semantics.</b> A tombstone on a cursor means that one dim of that RID is gone, never the
 * whole document (issue #9343): it adds nothing to the score and does not by itself make the RID a candidate,
 * while the live postings the same RID holds under other dims are still scored. A whole-document delete
 * ({@code remove(keys, rid)}) tombstones every dim of the document, so none of them scores; an UPDATE that
 * replaces the dims of a document ({@code remove(oldKeys)} then {@code put(newKeys)} for the same RID) leaves
 * the old dims tombstoned and the new ones live, and the document answers on the new ones only.
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
    return topK(queryDims, queryWeights, cursors, k, null, null);
  }

  /**
   * {@link #topK(int[], float[], DimCursor[], int)} restricted to the RID range
   * {@code [startInclusive, endExclusive)}. This is the unit of work of the parallel fan-out in
   * {@link PaginatedSparseVectorEngine#topK}: each worker takes one range, and because the ranges
   * partition the RID space, concatenating their results and keeping the best {@code k} yields
   * exactly the serial top-K.
   * <p>
   * <b>A range costs more than its share.</b> The pruning threshold is per-traversal, so a worker
   * that sees 1/P of the corpus reaches a lower watermark than the serial scan would at the same
   * point and therefore prunes less. The partitioned traversal does strictly more total work than
   * the serial one - measured at about 1.9x total CPU for an 8-way split on a learned-sparse corpus.
   * It buys latency with CPU, which is why the caller gates the fan-out rather than always taking it.
   * <p>
   * Cursors are seeked to {@code startInclusive} <i>before</i> each term's ceiling is captured, so a
   * worker prunes against the maximum weight remaining in <i>its</i> range rather than the whole
   * dim's - which claws back part of that amplification.
   *
   * @param startInclusive first RID to consider, or {@code null} to start at the beginning
   * @param endExclusive   first RID <i>not</i> to consider, or {@code null} to run to the end
   */
  public static List<RidScore> topK(final int[] queryDims, final float[] queryWeights, final DimCursor[] cursors, final int k,
      final RID startInclusive, final RID endExclusive) throws IOException {
    validate(queryDims, queryWeights, cursors);
    if (k <= 0)
      return List.of();

    final DimEntry[] terms = openTerms(queryWeights, cursors, startInclusive);
    if (terms.length == 0)
      return List.of();

    final TopKCollector collector = new TopKCollector(k);
    scan(terms, collector, endExclusive);
    return collector.drain();
  }

  /**
   * Opens the per-dim cursors of one traversal. A grouped search can need two traversals over the same snapshot (issue
   * #8002) and a {@link DimCursor} only moves forward, so the grouped entry points take this rather than an array of
   * already-open cursors.
   */
  @FunctionalInterface
  public interface CursorSource {
    /**
     * Returns one fresh cursor per query dim, parallel to {@code queryDims}, with {@code null} for a dim absent from
     * every source. Every call must read the same snapshot. The scorer closes what it gets.
     */
    DimCursor[] open() throws IOException;
  }

  /**
   * Top-K with traversal-integrated {@code groupBy} / {@code groupSize} (issue #4071). Replaces the
   * global K-heap with a per-group min-heap so the post-traversal filter that the MVP applied on top
   * of {@link #topK} no longer needs an over-fetched candidate pool. The {@code groupKeyResolver} is
   * consulted once per scored document; the resolver typically reads the group field off the
   * materialised record, so callers should keep it cheap.
   * <p>
   * <b>What the answer is.</b> The rule the caller asked for is the score-ordered, first-come-first-served
   * {@link com.arcadedb.index.vector.GroupAdmissionState}: at most {@code limit} distinct group keys, at most
   * {@code groupSize} rows each. Walked in score order that is exactly "the {@code limit} groups with the highest
   * peaks, each with its {@code groupSize} best members", and that is what this method returns.
   * <p>
   * <b>Threshold semantics with per-group state.</b> The pruning threshold is a lower bound on any
   * score that could still enter the result set. For non-grouped top-K that is the K-th best score
   * so far; for grouped top-K the analogue is "the lowest score that could replace any group's
   * worst member". Until {@code limit} groups have all reached {@code groupSize} (so any candidate
   * could open a new group or fill an empty slot), the threshold stays at
   * {@link Float#NEGATIVE_INFINITY}, which keeps every term essential and the traversal exhaustive.
   * Once globally full, the threshold is the minimum across per-group worst scores, and the
   * essential/non-essential split and the block-max skip can prune against it.
   * <p>
   * <b>Which groups win (issue #6936).</b> Candidates reach this traversal in ascending RID order, not
   * score order, so the first {@code limit} distinct group keys encountered are not necessarily the
   * highest-peaking ones. Once all {@code limit} slots are taken, a genuinely new group key is admitted
   * anyway if its score beats the weakest currently open group's <i>peak</i>, evicting that group
   * entirely. The pruning above never hides a group that should win: every open group's peak is at
   * least the threshold, so a document at or below it cannot beat any of them.
   * <p>
   * <b>What each winning group holds (issue #8002).</b> Picking the right groups is not the same as
   * filling them. A group that entered by eviction may have had members earlier in RID order, which
   * were rejected while it was not open, or skipped by a threshold raised on the assumption that the
   * group set was settled - and the threshold cannot be walked back. The first traversal therefore marks
   * every group it opens after a rejection, an eviction, or the first rise of the threshold as
   * possibly short; a group open from before any of those saw every document that could enter it.
   * When a winner is marked, a second traversal restricted to the marked keys (see
   * {@link #topKForGroups}) refills them, seeded with the members the first traversal already holds so
   * it can prune from the start. The common case - no winner marked - costs one traversal as before.
   * <p>
   * <b>{@code allowedRIDs} filter.</b> Applied inline in the scoring branch: a candidate RID outside
   * the whitelist is dropped before the non-essential probe walk even starts (cursors still advance
   * so the loop progresses). This removes the over-fetch + post-filter pattern that
   * {@link LSMSparseVectorIndex#topK} used to compensate for highly selective filters.
   *
   * @param queryDims        query dim ids
   * @param queryWeights     query weights, parallel to {@code queryDims}; must be non-negative
   * @param cursors          opens the per-dim cursors, once per traversal
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
  public static List<RidScore> topKGrouped(final int[] queryDims, final float[] queryWeights, final CursorSource cursors,
      final int limit, final int groupSize, final Function<RID, Object> groupKeyResolver, final Set<RID> allowedRIDs)
      throws IOException {
    return topKGrouped(queryDims, queryWeights, cursors, limit, groupSize, groupKeyResolver, allowedRIDs, null);
  }

  /**
   * {@link #topKGrouped(int[], float[], CursorSource, int, int, Function, Set)} with a set of RIDs the traversal
   * must skip (issue #7966).
   * <p>
   * A caller reading its own uncommitted writes scores those records itself, from what its transaction has queued,
   * and needs the committed - and therefore stale - copy of each of them left out. Applied here rather than by
   * filtering the result because a grouped traversal counts admissions per group as it goes: a row removed
   * afterwards leaves its group one short of a cap that another candidate could have filled.
   *
   * @param excludedRIDs RIDs to skip, or {@code null}/empty for none
   */
  public static List<RidScore> topKGrouped(final int[] queryDims, final float[] queryWeights, final CursorSource cursors,
      final int limit, final int groupSize, final Function<RID, Object> groupKeyResolver, final Set<RID> allowedRIDs,
      final Set<RID> excludedRIDs) throws IOException {
    if (groupKeyResolver == null)
      throw new IllegalArgumentException("groupKeyResolver must not be null");
    if (limit <= 0 || groupSize <= 0) {
      validate(queryDims, queryWeights);
      return List.of();
    }

    final GroupedCollector collector = new GroupedCollector(limit, groupSize, groupKeyResolver, allowedRIDs, excludedRIDs);
    scan(queryDims, queryWeights, cursors, collector);

    final HashMap<Object, GroupState> shortGroups = collector.possiblyShortGroups();
    if (!shortGroups.isEmpty()) {
      final FixedGroupsCollector refill = new FixedGroupsCollector(shortGroups.keySet(), groupSize, Float.NEGATIVE_INFINITY,
          groupKeyResolver, allowedRIDs, excludedRIDs);
      for (final Map.Entry<Object, GroupState> e : shortGroups.entrySet())
        refill.seed(e.getKey(), e.getValue().heap);
      scan(queryDims, queryWeights, cursors, refill);
      for (final Map.Entry<Object, RidScoreMinHeap> e : refill.groups.entrySet())
        shortGroups.get(e.getKey()).heap = e.getValue();
    }
    return collector.drain();
  }

  /**
   * The best {@code groupSize} members of each of a FIXED set of groups (issue #8002): no group outside
   * {@code groupKeys} is ever admitted, and no limit on distinct groups applies because the caller already chose them.
   * <p>
   * This is the second half of a grouped search whose group choice is settled but whose members may not all have been
   * seen: {@link #topKGrouped} uses it on its own snapshot, and a caller merging the grouped answers of several indexes
   * (one per bucket, or committed rows plus a transaction's own) uses it on an index that ranked a winning group out
   * of its local top {@code limit}.
   *
   * @param groupKeys the groups to fill; a key with no member here simply comes back absent
   * @param floor     only scores strictly above this can matter to the caller - typically the worst member it already
   *                  holds for the weakest of {@code groupKeys} - so the traversal prunes against it from the start;
   *                  {@link Float#NEGATIVE_INFINITY} for none
   *
   * @return at most {@code groupKeys.size() * groupSize} (RID, score) pairs sorted by score descending, every one above
   *         {@code floor}
   */
  public static List<RidScore> topKForGroups(final int[] queryDims, final float[] queryWeights, final CursorSource cursors,
      final Set<Object> groupKeys, final int groupSize, final float floor, final Function<RID, Object> groupKeyResolver,
      final Set<RID> allowedRIDs, final Set<RID> excludedRIDs) throws IOException {
    if (groupKeyResolver == null)
      throw new IllegalArgumentException("groupKeyResolver must not be null");
    if (Float.isNaN(floor))
      throw new IllegalArgumentException("floor must not be NaN");
    if (groupKeys == null || groupKeys.isEmpty() || groupSize <= 0) {
      validate(queryDims, queryWeights);
      return List.of();
    }

    final FixedGroupsCollector collector = new FixedGroupsCollector(groupKeys, groupSize, floor, groupKeyResolver,
        allowedRIDs, excludedRIDs);
    scan(queryDims, queryWeights, cursors, collector);
    return collector.drain();
  }

  /** One full traversal: opens the cursors, scans them into {@code collector}, and closes them whatever happens. */
  private static void scan(final int[] queryDims, final float[] queryWeights, final CursorSource source,
      final Collector collector) throws IOException {
    validate(queryDims, queryWeights);
    final DimCursor[] cursors = source.open();
    try {
      if (cursors.length != queryDims.length)
        throw new IllegalArgumentException("queryDims, queryWeights, cursors must have the same length");
      final DimEntry[] terms = openTerms(queryWeights, cursors, null);
      if (terms.length > 0)
        scan(terms, collector, null);
    } finally {
      for (final DimCursor c : cursors)
        if (c != null)
          c.close();
    }
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
  private static void scan(final DimEntry[] terms, final Collector collector, final RID endExclusive) throws IOException {
    final int window = GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.getValueAsInteger();
    if (window > 0) {
      scanWindowed(terms, collector, endExclusive, (Math.min(window, MAX_WINDOW) + 63) & ~63);
      return;
    }
    scanDocumentAtATime(terms, collector, endExclusive);
  }

  private static void scanDocumentAtATime(final DimEntry[] terms, final Collector collector, final RID endExclusive)
      throws IOException {
    final int n = terms.length;
    // Keys are packed relative to the smallest bucket id any cursor starts on: cursors only move
    // forward, so no key this traversal will ever see has a smaller one (issue #8553).
    int base = Integer.MAX_VALUE;
    for (final DimEntry t : terms)
      base = Math.min(base, t.cursor.currentBucketId());
    final KeyMirror mirror = new KeyMirror(n, base);
    final boolean bounded = endExclusive != null;
    final long endKey = bounded ?
        SparseSegmentBuilder.packRidCeiling(endExclusive.getBucketId(), endExclusive.getPosition(), base) :
        Long.MAX_VALUE;
    // prefix[i] == sum of sigma over terms [0, i). prefix[n] is the whole query's ceiling.
    final float[] prefix = new float[n + 1];
    recomputePrefix(terms, prefix);

    final int[] heap = new int[n];     // essential term indices, min-heap by current RID
    final int[] aligned = new int[n];  // scratch: the term indices sitting on the current candidate
    final int[] alignedSlots = new int[n];  // scratch: the heap slots those term indices occupy
    final RID[] blockEndOut = new RID[1];   // scratch: the block end paired with each block-max probe
    // Cursor positions, mirrored into a flat array indexed by term. The heap reads a position far
    // more often than a cursor moves - about a dozen comparisons per posting consumed - and reading
    // it off the cursor costs two dependent loads through scattered objects (issue #5467). Each
    // position is folded into one packed key whose natural order is the RID order (issue #8553), so a
    // heap comparison is a single load and a single long compare rather than a bucket compare, a branch
    // and a position compare over two arrays. The mirrors are refreshed only where a cursor actually
    // moves; an exhausted cursor mirrors as -1.
    final long[] keys = mirror.keys;
    for (int i = 0; i < n; i++)
      mirror.sync(terms, i);
    int heapSize = 0;
    int split = 0;
    float lastThreshold = Float.NEGATIVE_INFINITY;
    boolean splitDirty = true;
    boolean heapDirty = true;

    while (true) {
      // MUST stay the first statement of the loop: nothing may read the heap for candidate selection between the
      // mirror sync that sets overflow and this check.
      if (mirror.overflow) {
        // A cursor reached a RID the packed order cannot hold. Every mirror was valid at the end of the
        // previous iteration, and this one completed on comparisons that stay exact against the
        // UNPACKABLE marker, so the collector and the cursors are exactly where a traversal on RIDs
        // would have them: it carries on from here on RID comparisons (issue #8553).
        WIDE_FALLBACKS.incrementAndGet();
        scanWide(terms, collector, endExclusive);
        return;
      }
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
        heapSize = buildHeap(terms, keys, split, n, heap);
        heapDirty = false;
      }
      if (heapSize == 0)
        return;  // every essential term is exhausted.

      // Next candidate: the smallest current RID across the essential terms. Non-essential terms do
      // not generate candidates - a document they alone match is bounded by prefix[split], which is
      // at or below the threshold by construction of the split.
      final long candidateKey = keys[heap[0]];

      // Range bound: candidates are produced in ascending RID order, so the first one at or past the
      // end of this worker's range means the range is done - everything left belongs to a sibling.
      if (bounded && candidateKey >= endKey)
        return;

      // Locate the aligned run without touching the heap.
      final int alignedCount = collectAlignedRun(keys, heap, heapSize, candidateKey, aligned, alignedSlots);

      if (!tryBlockMaxSkip(terms, mirror, aligned, alignedSlots, alignedCount, prefix[split], candidateKey, threshold, heap,
          heapSize, blockEndOut))
        scoreCandidate(terms, mirror, aligned, alignedCount, split, prefix, candidateKey, threshold, collector);

      // The moved cursors are still in their slots holding keys that only ever grew, so the heap is
      // repaired in place, deepest slot first: by then every slot below it is already a valid heap.
      // An exhausted term has to leave the heap entirely, which a repair cannot express - the heap
      // would have to shrink - so that (rare: at most once per term per query) case rebuilds instead.
      // An exhausted term contributes nothing from here on, and dropping its ceiling tightens every
      // prefix bound, which can only push the split further right on the next pass.
      boolean exhaustedAny = false;
      boolean anyCursorExhausted = false;
      for (int j = 0; j < alignedCount; j++) {
        final int idx = aligned[j];
        // The mirror holds -1 for an exhausted cursor, so asking it costs one array load instead of
        // three dependent dereferences through the term and its cursor.
        if (keys[idx] < 0) {
          anyCursorExhausted = true;
          exhaustedAny |= terms[idx].clearSigma();
        }
      }
      if (anyCursorExhausted)
        heapDirty = true;
      else
        // A cursor that moved onto an unpackable RID this iteration leaves the heap ordered on UNPACKABLE: tolerated,
        // because nothing reads the heap before the next iteration's overflow check hands the query to scanWide.
        for (int j = alignedCount - 1; j >= 0; j--)
          siftDownFromFloyd(keys, heap, heapSize, alignedSlots[j]);
      if (exhaustedAny) {
        recomputePrefix(terms, prefix);
        splitDirty = true;
      }
    }
  }

  // ---------- window traversal (issue #9200) ----------

  /** Upper cap of {@link GlobalConfiguration#SPARSE_VECTOR_SCORING_WINDOW}: the window array is 4 bytes per position. */
  private static final int MAX_WINDOW = 1 << 20;

  /** Size of the first window of a traversal; each window after it doubles, up to the configured size. */
  private static final int INITIAL_WINDOW = 128;

  /**
   * Window MaxScore: the shape of Lucene's {@code MaxScoreBulkScorer}. The terms are split into essential and
   * non-essential exactly as in {@link #scanDocumentAtATime}, but instead of keeping the essential cursors ordered by
   * current RID, the traversal takes the smallest RID any essential cursor sits on as the start of a window of
   * {@code windowSize} RID positions and, one term at a time, adds every posting of that term inside the window into a
   * score array. No per-posting heap is kept: the cost of ordering the cursors, which dominated the document-at-a-time
   * traversal on corpora where the essential set stays large, is gone. Each document that received a score then probes
   * the non-essential terms from the highest ceiling down and is abandoned as soon as it cannot beat the threshold.
   * <p>
   * A tombstone adds nothing to its document's slot and does not mark it as touched (issue #9343). Documents are drained in ascending RID order, so the collector sees the same sequence as in
   * the document-at-a-time traversal. The threshold is re-read per document and the split per window, never walked back.
   * Falls to {@link #scanWide} on a RID the packed order cannot hold, like the other traversal.
   */
  private static void scanWindowed(final DimEntry[] terms, final Collector collector, final RID endExclusive,
      final int windowSize) throws IOException {
    final int n = terms.length;
    int base = Integer.MAX_VALUE;
    for (final DimEntry t : terms)
      base = Math.min(base, t.cursor.currentBucketId());
    final KeyMirror mirror = new KeyMirror(n, base);
    final long endKey = endExclusive != null ?
        SparseSegmentBuilder.packRidCeiling(endExclusive.getBucketId(), endExclusive.getPosition(), base) :
        Long.MAX_VALUE;
    final float[] prefix = new float[n + 1];
    recomputePrefix(terms, prefix);
    final long[] keys = mirror.keys;
    for (int i = 0; i < n; i++)
      mirror.sync(terms, i);

    // Allocated lazily: a query that prunes everything away never pays for the arrays.
    float[] scores = null;
    long[] touched = null;
    float[] blockMax = null;
    RID[] blockEndOut = null;
    // Windows start small and double up to windowSize: the threshold only exists once the first top-K documents have
    // been collected, and an opening window as wide as the whole corpus would score everything before pruning could
    // start (the same reason the classic traversal fills its heap from the first documents).
    int currentWindow = Math.min(windowSize, INITIAL_WINDOW);
    int split = 0;
    boolean splitDirty = true;
    float lastThreshold = Float.NEGATIVE_INFINITY;

    while (true) {
      if (mirror.overflow) {
        WIDE_FALLBACKS.incrementAndGet();
        scanWide(terms, collector, endExclusive);
        return;
      }
      final float threshold = collector.threshold();
      if (splitDirty || threshold > lastThreshold) {
        lastThreshold = threshold;
        splitDirty = false;
        while (split < n && prefix[split + 1] <= threshold)
          split++;
        if (split >= n)
          return;
      }

      long windowStart = Long.MAX_VALUE;
      for (int i = split; i < n; i++) {
        final long key = keys[i];
        if (key >= 0 && key < windowStart)
          windowStart = key;
      }
      if (windowStart == Long.MAX_VALUE || windowStart >= endKey)
        return;  // every essential term is exhausted, or the range is done.
      final long windowEnd = Math.min(endKey,
          windowStart > Long.MAX_VALUE - currentWindow ? Long.MAX_VALUE : windowStart + currentWindow);

      // Block-Max: when even the best block of every essential term cannot lift a document of this stretch over the
      // threshold, move the cursors past it without decoding a weight. Only worth asking once a threshold exists.
      if (threshold > Float.NEGATIVE_INFINITY && prefix[split] <= threshold) {
        if (blockMax == null) {
          blockMax = new float[n];
          blockEndOut = new RID[1];
        }
        if (trySkipWindow(terms, mirror, split, windowStart, windowEnd, prefix[split], threshold, blockMax, blockEndOut))
          continue;
      }

      // Sized to the window in use and grown with it, so a query that ends after a few small windows never pays for the
      // full-size array. Every slot is zeroed on drain, so a larger array starts clean.
      if (scores == null || scores.length < currentWindow) {
        scores = new float[currentWindow];
        touched = new long[currentWindow >>> 6];
      }

      // Add the essential terms' postings of this window, one term at a time.
      boolean exhaustedAny = false;
      for (int i = split; i < n; i++) {
        long key = keys[i];
        if (key < 0 || key >= windowEnd)
          continue;
        final DimEntry t = terms[i];
        final DimCursor c = t.cursor;
        final float w = t.queryWeight;
        while (true) {
          final int offset = (int) (key - windowStart);
          final int word = offset >>> 6;
          final long bit = 1L << offset;
          // A tombstone says only that THIS dim of this document is gone: it adds nothing and does not make the
          // document a candidate, so a document whose other dims are live is still scored on them (issue #9343).
          if (!c.isTombstone()) {
            if ((touched[word] & bit) == 0L)
              scores[offset] = w * c.currentWeight();
            else
              scores[offset] += w * c.currentWeight();
            touched[word] |= bit;
          }
          c.advance();
          mirror.sync(terms, i);
          key = keys[i];
          if (key < 0 || key >= windowEnd)
            break;
        }
        if (key < 0)
          exhaustedAny |= t.clearSigma();
      }

      // Drain in ascending RID order; every slot is zeroed on the way out so the arrays are reusable.
      final float nonEssentialCeiling = prefix[split];
      final int words = (currentWindow + 63) >>> 6;
      if (currentWindow < windowSize)
        currentWindow = Math.min(windowSize, currentWindow << 1);
      for (int word = 0; word < words; word++) {
        long bits = touched[word];
        if (bits == 0L)
          continue;
        touched[word] = 0L;
        while (bits != 0L) {
          final int offset = (word << 6) + Long.numberOfTrailingZeros(bits);
          bits &= bits - 1;
          final float essentialScore = scores[offset];
          scores[offset] = 0.0f;
          scoreWindowDocument(terms, mirror, split, prefix, nonEssentialCeiling, windowStart + offset, essentialScore,
              collector);
        }
      }

      if (exhaustedAny) {
        recomputePrefix(terms, prefix);
        splitDirty = true;
      }
    }
  }

  /**
   * The Block-Max half of the window traversal. Takes, for every essential term with a posting inside the window, the
   * maximum of the block its cursor sits in, and shortens the stretch to the earliest block end so that every term's
   * bound covers all of it. If the ceiling of the non-essential terms plus those maxima cannot beat {@code threshold},
   * no document of {@code [windowStart, skipEnd)} can: the essential cursors are sought to {@code skipEnd} and the
   * stretch is never decoded.
   *
   * @return true if a stretch was skipped (progress is guaranteed: {@code skipEnd} is past {@code windowStart}).
   */
  private static boolean trySkipWindow(final DimEntry[] terms, final KeyMirror mirror, final int split,
      final long windowStart, final long windowEnd, final float nonEssentialCeiling, final float threshold,
      final float[] blockMax, final RID[] blockEndOut) throws IOException {
    final long[] keys = mirror.keys;
    final int base = mirror.baseBucketId;
    long skipEnd = windowEnd;
    for (int i = split; i < terms.length; i++) {
      final long key = keys[i];
      if (key < 0 || key >= windowEnd)
        continue;
      blockMax[i] = terms[i].cursor.blockBoundsAt(SparseSegmentBuilder.unpackBucketId(key, base),
          SparseSegmentBuilder.unpackPosition(key), blockEndOut);
      final RID be = blockEndOut[0];
      if (be == null)
        continue;  // no finite block boundary (memtable or loose source): its bound holds for the whole stretch.
      final long endKey = SparseSegmentBuilder.packRidCeiling(be.getBucketId(), be.getPosition(), base);
      if (endKey == SparseSegmentBuilder.UNPACKABLE)
        return false;
      if (endKey + 1 < skipEnd)
        skipEnd = endKey + 1;  // the key after the block's last RID
    }

    float bound = nonEssentialCeiling;
    for (int i = split; i < terms.length; i++) {
      final long key = keys[i];
      if (key < 0 || key >= skipEnd)
        continue;
      bound += terms[i].queryWeight * blockMax[i];
      if (bound > threshold)
        return false;
    }

    final int targetBucketId = SparseSegmentBuilder.unpackBucketId(skipEnd, base);
    final long targetPosition = SparseSegmentBuilder.unpackPosition(skipEnd);
    for (int i = split; i < terms.length; i++) {
      final long key = keys[i];
      if (key < 0 || key >= skipEnd)
        continue;
      terms[i].cursor.seekTo(targetBucketId, targetPosition);
      mirror.sync(terms, i);
    }
    return true;
  }

  /** Probes the non-essential terms for one document of the window and collects it if it still beats the threshold. */
  private static void scoreWindowDocument(final DimEntry[] terms, final KeyMirror mirror, final int split,
      final float[] prefix, final float nonEssentialCeiling, final long candidateKey, final float essentialScore,
      final Collector collector) throws IOException {
    final float threshold = collector.threshold();
    if (essentialScore + nonEssentialCeiling <= threshold)
      return;
    final int candidateBucketId = SparseSegmentBuilder.unpackBucketId(candidateKey, mirror.baseBucketId);
    final long candidatePosition = SparseSegmentBuilder.unpackPosition(candidateKey);
    final RID candidate = new RID(candidateBucketId, candidatePosition);
    if (!collector.accepts(candidate))
      return;
    final long[] keys = mirror.keys;
    float score = essentialScore;
    for (int i = split - 1; i >= 0; i--) {
      if (score + prefix[i + 1] <= threshold)
        return;
      long key = keys[i];
      if (key < 0)
        continue;
      if (key < candidateKey) {
        terms[i].cursor.seekTo(candidateBucketId, candidatePosition);
        mirror.sync(terms, i);
        key = keys[i];
      }
      if (key != candidateKey)
        continue;
      final DimCursor c = terms[i].cursor;
      if (c.isTombstone())
        continue;  // this dim of the document is gone, its other dims still count (issue #9343)
      score += terms[i].queryWeight * c.currentWeight();
    }
    collector.collect(candidate, score);
  }

  /**
   * Collect the heap slots whose key equals {@code candidateKey} - the traversal's aligned run -
   * <b>without modifying the heap</b>.
   * <p>
   * Every slot holding the heap's minimum forms a connected subtree rooted at slot 0: if a slot holds
   * the minimum then so does its parent, since a parent's key is at most its child's and at least the
   * global minimum. A breadth-first walk that stops descending at the first non-matching child
   * therefore visits exactly the run, and it visits it in ascending slot order because a binary heap
   * array <i>is</i> its own level order - which is what lets the caller repair deepest-slot-first by
   * walking the result backwards.
   * <p>
   * <b>Why not pop and push.</b> Detaching the run with one pop per term and re-attaching
   * it with one push per term costs, per term, a descent to a leaf plus a climb from a
   * leaf back to wherever the advanced cursor now belongs - and in a wide merge a just-advanced cursor
   * usually still belongs near the top, so that climb is nearly the full height of the heap. Repairing
   * in place skips it: the element starts where it already is, and one Floyd sift puts it back. On a
   * SPLADE-shaped 100k corpus that is 695k key comparisons per query instead of 913k, a quarter of
   * them gone (issue #5467).
   * <p>
   * An earlier attempt at the same idea repaired with a textbook top-down sift-down and measured
   * <i>slower</i>, which is recorded on the issue. The reason is the sift, not the collection: a
   * top-down sift spends two comparisons per level (pick the smaller child, then test it against the
   * element), while Floyd's spends one per level going down and pays for the climb only when the
   * element really does belong high up. See {@link #siftDownFromFloyd}.
   *
   * @param keys         packed RID key per term (see {@link SparseSegmentBuilder#packRid(int, long, int)})
   * @param aligned      out: term indices of the run, in ascending heap-slot order
   * @param alignedSlots out: the heap slots those terms occupy, parallel to {@code aligned}
   *
   * @return the number of terms in the run, always at least 1
   */
  // Package-private rather than private so BmwScorerHeapRepairTest can pin these invariants against a
  // hand-built heap, which fails with a small counterexample instead of only as a top-K mismatch.
  static int collectAlignedRun(final long[] keys, final int[] heap, final int heapSize, final long candidateKey,
      final int[] aligned, final int[] alignedSlots) {
    aligned[0] = heap[0];
    alignedSlots[0] = 0;
    int count = 1;
    for (int q = 0; q < count; q++) {
      final int left = (alignedSlots[q] << 1) + 1;
      if (left >= heapSize)
        continue;  // a leaf slot, and a heap array is left-packed, so there is no right child either.
      int term = heap[left];
      if (keys[term] == candidateKey) {
        aligned[count] = term;
        alignedSlots[count] = left;
        count++;
      }
      final int right = left + 1;
      if (right < heapSize) {
        term = heap[right];
        if (keys[term] == candidateKey) {
          aligned[count] = term;
          alignedSlots[count] = right;
          count++;
        }
      }
    }
    return count;
  }

  /**
   * The smallest key in the heap that is not part of the aligned run, or {@code -1} when the run is
   * the whole heap.
   * <p>
   * The candidates for that minimum are exactly the children of run slots that are not themselves in
   * the run: any slot outside the run reaches the root through one of them, and each of those is the
   * minimum of its own subtree.
   * <p>
   * This is computed on demand rather than alongside {@link #collectAlignedRun} because only the
   * block-max skip needs it, and only once it has already decided to skip. On a learned-sparse query
   * the skip fires for a couple of percent of candidates - block maxima are no tighter than global
   * maxima when weights are near-uniform - so folding it into the run walk paid a three-way
   * comparison per heap child on every candidate to answer a question almost none of them asked
   * (issue #5467).
   */
  static long nextEssentialKey(final long[] keys, final int[] heap, final int heapSize, final int[] alignedSlots,
      final int alignedCount, final long candidateKey) {
    // Every key in the heap is at least the candidate, so a child that is not ON the candidate is
    // strictly after it and Long.MAX_VALUE is a safe "none yet" marker: no valid key reaches it.
    long best = Long.MAX_VALUE;
    for (int q = 0; q < alignedCount; q++) {
      final int left = (alignedSlots[q] << 1) + 1;
      if (left >= heapSize)
        continue;  // a leaf slot, and a heap array is left-packed, so there is no right child either.
      // Skip children that are themselves in the run. A run slot's child can be another run slot, and
      // the run's key IS the candidate, so without this the walk would return the candidate as the
      // next key after itself and the block-max skip would target where it already is. Testing the
      // key rather than the slot is what keeps it O(1): no cursor has moved yet, so a child still
      // sitting on the candidate is exactly a run member (BmwScorerHeapRepairTest pins it).
      long key = keys[heap[left]];
      if (key != candidateKey && key < best)
        best = key;
      final int right = left + 1;
      if (right < heapSize) {
        key = keys[heap[right]];
        if (key != candidateKey && key < best)
          best = key;
      }
    }
    return best == Long.MAX_VALUE ? -1L : best;
  }

  /**
   * Score {@code candidate}: sum the essential terms aligned on it, then probe the non-essential
   * terms from the highest {@code sigma} down, abandoning the document as soon as the partial score
   * plus the remaining non-essential ceiling can no longer beat {@code threshold}. Finally advance
   * every aligned cursor so the traversal makes progress.
   */
  private static void scoreCandidate(final DimEntry[] terms, final KeyMirror mirror, final int[] aligned,
      final int alignedCount, final int split, final float[] prefix, final long candidateKey, final float threshold,
      final Collector collector) throws IOException {
    final long[] keys = mirror.keys;
    // A tombstone removes one dim of one document, not the document (issue #9343): it adds nothing to the score, and
    // a candidate with no live essential posting is dropped, since its non-essential terms alone cannot beat the threshold.
    boolean alive = false;
    float score = 0.0f;
    for (int j = 0; j < alignedCount; j++) {
      final DimEntry t = terms[aligned[j]];
      if (t.cursor.isTombstone())
        continue;
      alive = true;
      score += t.queryWeight * t.cursor.currentWeight();
    }

    // The RID object is built once, and only for a candidate that survived the aligned-run scan -
    // the traversal itself never materialises one (issue #5467).
    final int candidateBucketId = SparseSegmentBuilder.unpackBucketId(candidateKey, mirror.baseBucketId);
    final long candidatePosition = SparseSegmentBuilder.unpackPosition(candidateKey);
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
        // The mirrored key answers the probe outright whenever the term's cursor already sits past
        // this document, which on a dense non-essential list is most of the time: only a cursor that
        // is still behind the candidate has to be moved. Reading it here instead of calling into the
        // cursor turns the common case into one load from an array that stays in L1 (issue #5467).
        long key = keys[i];
        if (key < 0)
          continue;  // exhausted; the mirror holds -1 for that.
        if (key < candidateKey) {
          terms[i].cursor.seekTo(candidateBucketId, candidatePosition);
          mirror.sync(terms, i);
          key = keys[i];
        }
        // An UNPACKABLE key is still exact here: the cursor sits at or after the candidate, which is
        // packable, so a RID that is not can only be strictly after it - no posting for this document.
        if (key != candidateKey)
          continue;  // this term holds no posting for this document (or just ran out).
        final DimCursor c = terms[i].cursor;
        if (c.isTombstone())
          continue;  // this dim of the document is gone, its other dims still count (issue #9343)
        score += terms[i].queryWeight * c.currentWeight();
      }
      if (alive)
        collector.collect(candidate, score);
    }

    for (int j = 0; j < alignedCount; j++) {
      final int idx = aligned[j];
      terms[idx].cursor.advance();
      mirror.sync(terms, idx);
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
  private static boolean tryBlockMaxSkip(final DimEntry[] terms, final KeyMirror mirror, final int[] aligned,
      final int[] alignedSlots, final int alignedCount, final float nonEssentialCeiling, final long candidateKey,
      final float threshold, final int[] heap, final int heapSize, final RID[] blockEndOut) throws IOException {
    float bound = nonEssentialCeiling;
    if (bound > threshold)
      return false;  // no headroom at all (threshold still NEGATIVE_INFINITY, or an all-essential query).

    final int base = mirror.baseBucketId;
    final int candidateBucketId = SparseSegmentBuilder.unpackBucketId(candidateKey, base);
    final long candidatePosition = SparseSegmentBuilder.unpackPosition(candidateKey);
    RID minBlockEnd = null;
    for (int j = 0; j < alignedCount; j++) {
      final DimEntry t = terms[aligned[j]];
      // One call for both edges of this term's bound: the memo is consulted once, and the block end
      // cannot belong to a different probe than the max it is paired with.
      bound += t.queryWeight * t.cursor.blockBoundsAt(candidateBucketId, candidatePosition, blockEndOut);
      if (bound > threshold)
        return false;
      final RID be = blockEndOut[0];
      if (be != null && (minBlockEnd == null || SparseSegmentBuilder.compareRid(be, minBlockEnd) < 0))
        minBlockEnd = be;
    }

    if (minBlockEnd == null)
      return false;  // no finite block boundary (only loose/memtable sources) - cannot block-skip.

    // The skip target is the RID successor of the block end, (bucket, position + 1), kept as a RID
    // pair because the block end need not be packable. At the very top of the position space the
    // successor is the next bucket's first position; with nowhere left to go, score normally.
    int targetBucketId = minBlockEnd.getBucketId();
    long targetPosition = minBlockEnd.getPosition();
    if (targetPosition == Long.MAX_VALUE) {
      if (targetBucketId == Integer.MAX_VALUE)
        return false;
      targetBucketId++;
      targetPosition = 0L;
    } else
      targetPosition++;
    // Only now, having committed to the skip, is the first essential cursor sitting strictly after the
    // candidate worth locating: it bounds how far the skip may go, and nothing else needs it. It is a
    // valid heap key, so packable, and compared on RIDs against a target that may not be.
    final long nextEssential = nextEssentialKey(mirror.keys, heap, heapSize, alignedSlots, alignedCount, candidateKey);
    if (nextEssential >= 0) {
      final int nextBucketId = SparseSegmentBuilder.unpackBucketId(nextEssential, base);
      final long nextPosition = SparseSegmentBuilder.unpackPosition(nextEssential);
      if (SparseSegmentBuilder.compareRid(nextBucketId, nextPosition, targetBucketId, targetPosition) < 0) {
        targetBucketId = nextBucketId;
        targetPosition = nextPosition;
      }
    }

    for (int j = 0; j < alignedCount; j++) {
      final int idx = aligned[j];
      terms[idx].cursor.seekTo(targetBucketId, targetPosition);
      mirror.sync(terms, idx);
    }
    return true;
  }

  // ---------- essential-term min-heap (indices into {@code terms}, ordered by current RID) ----------

  private static int buildHeap(final DimEntry[] terms, final long[] keys, final int split, final int n, final int[] heap) {
    int size = 0;
    for (int i = split; i < n; i++)
      if (!terms[i].cursor.isExhausted())
        heap[size++] = i;
    for (int i = (size >> 1) - 1; i >= 0; i--)
      siftDown(keys, heap, size, i);
    return size;
  }

  /**
   * Repair heap slot {@code from} after its element's key grew, using Floyd's bottom-up variant:
   * descend to a leaf of {@code from}'s subtree always following the smaller child, drop the element
   * there, then climb back up - never above {@code from}, since the parent chain above it is
   * untouched and holds keys no larger than the one that was there before.
   * <p>
   * The variant matters. A textbook top-down sift-down spends <b>two</b> comparisons per level - one
   * to pick the smaller child, one to test it against the element being pushed down - so it costs
   * {@code 2 * depth} no matter where the element ends up. Floyd's spends one per level on the way
   * down and pays for the climb only in proportion to how high the element really belongs. On a
   * learned-sparse query the essential heap holds dozens of terms and this runs millions of times per
   * query, which is why the traversal's single hottest operation is worth the asymmetry
   * (issue #5467). The element's own key is read once, and each level costs one load per child it
   * compares, one packed key each (issue #8553).
   */
  static void siftDownFromFloyd(final long[] keys, final int[] heap, final int size, final int from) {
    int left = (from << 1) + 1;
    if (left >= size)
      return;  // a leaf slot: no descendant can violate the ordering.
    final int element = heap[from];
    final long elementKey = keys[element];
    int i = from;
    while (left < size) {
      final int right = left + 1;
      int child = left;
      if (right < size && keys[heap[right]] < keys[heap[left]])
        child = right;
      heap[i] = heap[child];
      i = child;
      left = (i << 1) + 1;
    }
    // Climb back: the element sits at the leaf; move it up while it is smaller than its parent, never
    // above the slot it started from.
    while (i > from) {
      final int parent = (i - 1) >>> 1;
      final int parentTerm = heap[parent];
      if (elementKey >= keys[parentTerm])
        break;
      heap[i] = parentTerm;
      i = parent;
    }
    heap[i] = element;
  }

  private static void siftDown(final long[] keys, final int[] heap, final int size, final int from) {
    int i = from;
    while (true) {
      final int left = (i << 1) + 1;
      if (left >= size)
        return;
      int smallest = left;
      final int right = left + 1;
      if (right < size && keys[heap[right]] < keys[heap[left]])
        smallest = right;
      if (keys[heap[i]] <= keys[heap[smallest]])
        return;
      final int tmp = heap[i];
      heap[i] = heap[smallest];
      heap[smallest] = tmp;
      i = smallest;
    }
  }

  /**
   * The packed-key mirror of the cursor positions (issue #8553), with the bucket id the keys are packed
   * relative to, and whether a cursor has reached a RID the packing cannot hold.
   */
  private static final class KeyMirror {
    final long[] keys;
    final int    baseBucketId;
    boolean      overflow;

    KeyMirror(final int n, final int baseBucketId) {
      this.keys = new long[n];
      this.baseBucketId = baseBucketId;
    }

    /** Refresh term {@code i}'s mirrored key after its cursor moved; {@code -1} once it is exhausted. */
    void sync(final DimEntry[] terms, final int i) {
      final DimCursor c = terms[i].cursor;
      final long key = SparseSegmentBuilder.packRid(c.currentBucketId(), c.currentPosition(), baseBucketId);
      if (key == SparseSegmentBuilder.UNPACKABLE)
        overflow = true;
      keys[i] = key;
    }
  }

  /**
   * How many traversals had to leave the packed-key path for {@link #scanWide} (issue #8553). Package-private so a test
   * can prove it forced one; it moves only on a traversal spanning more than a million bucket ids or reading a
   * position no bucket hands out.
   */
  static final AtomicLong WIDE_FALLBACKS = new AtomicLong();

  /**
   * The MaxScore traversal on RID comparisons, for the rest of a traversal whose RIDs the packed order cannot hold
   * (issue #8553). Exact and deliberately plain - a linear minimum instead of the heap, no block-max skip - because it
   * only runs on RIDs no real bucket produces: a traversal spanning more than a million bucket ids, or a position past
   * {@code 2^42}. It picks up the cursors and the collector exactly where the packed traversal left them.
   */
  private static void scanWide(final DimEntry[] terms, final Collector collector, final RID endExclusive)
      throws IOException {
    final int n = terms.length;
    final float[] prefix = new float[n + 1];
    recomputePrefix(terms, prefix);
    int split = 0;
    while (true) {
      final float threshold = collector.threshold();
      while (split < n && prefix[split + 1] <= threshold)
        split++;
      if (split >= n)
        return;

      int minBucketId = -1;
      long minPosition = -1L;
      for (int i = split; i < n; i++) {
        final DimCursor c = terms[i].cursor;
        if (c.isExhausted())
          continue;
        if (minBucketId < 0
            || SparseSegmentBuilder.compareRid(c.currentBucketId(), c.currentPosition(), minBucketId, minPosition) < 0) {
          minBucketId = c.currentBucketId();
          minPosition = c.currentPosition();
        }
      }
      if (minBucketId < 0)
        return;
      if (endExclusive != null && SparseSegmentBuilder.compareRid(minBucketId, minPosition, endExclusive) >= 0)
        return;

      boolean alive = false;
      float score = 0.0f;
      for (int i = split; i < n; i++) {
        final DimCursor c = terms[i].cursor;
        if (!c.isExhausted() && c.currentBucketId() == minBucketId && c.currentPosition() == minPosition) {
          if (c.isTombstone())
            continue;  // one dim of the document is gone, not the document (issue #9343)
          alive = true;
          score += terms[i].queryWeight * c.currentWeight();
        }
      }
      RID candidate = null;
      if (alive) {
        candidate = new RID(minBucketId, minPosition);
        alive = collector.accepts(candidate);
      }
      if (alive) {
        for (int i = split - 1; i >= 0; i--) {
          if (score + prefix[i + 1] <= threshold) {
            alive = false;
            break;
          }
          final DimCursor c = terms[i].cursor;
          if (c.isExhausted())
            continue;
          if (SparseSegmentBuilder.compareRid(c.currentBucketId(), c.currentPosition(), minBucketId, minPosition) < 0)
            c.seekTo(minBucketId, minPosition);
          if (c.isExhausted() || c.currentBucketId() != minBucketId || c.currentPosition() != minPosition)
            continue;
          if (c.isTombstone())
            continue;
          score += terms[i].queryWeight * c.currentWeight();
        }
        if (alive)
          collector.collect(candidate, score);
      }

      boolean exhaustedAny = false;
      for (int i = split; i < n; i++) {
        final DimCursor c = terms[i].cursor;
        if (!c.isExhausted() && c.currentBucketId() == minBucketId && c.currentPosition() == minPosition) {
          c.advance();
          if (c.isExhausted())
            exhaustedAny |= terms[i].clearSigma();
        }
      }
      if (exhaustedAny)
        recomputePrefix(terms, prefix);
    }
  }


  // ---------- setup ----------

  private static void validate(final int[] queryDims, final float[] queryWeights, final DimCursor[] cursors) {
    if (queryWeights.length != cursors.length)
      throw new IllegalArgumentException("queryDims, queryWeights, cursors must have the same length");
    validate(queryDims, queryWeights);
  }

  private static void validate(final int[] queryDims, final float[] queryWeights) {
    if (queryDims.length != queryWeights.length)
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
   * <p>
   * When a {@code startInclusive} is given, each cursor is seeked there <i>before</i> its
   * {@link DimEntry} is built. That ordering matters: a term's ceiling is captured from
   * {@link DimCursor#upperBoundRemaining()}, which on a sealed segment is a suffix max from the
   * cursor's current position, so seeking first gives a worker the maximum weight remaining in its
   * own range rather than the whole dim's. A tighter ceiling means a term can be proven
   * non-essential sooner, which is the one lever that offsets a partitioned traversal's weaker
   * pruning threshold.
   */
  private static DimEntry[] openTerms(final float[] queryWeights, final DimCursor[] cursors, final RID startInclusive)
      throws IOException {
    final List<DimEntry> live = new ArrayList<>(cursors.length);
    for (int i = 0; i < cursors.length; i++) {
      if (cursors[i] == null)
        continue;
      cursors[i].start();
      if (startInclusive != null)
        cursors[i].seekTo(startInclusive);
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

  /**
   * Result ordering handed back to the caller: best score first, ties broken by RID ascending.
   * <p>
   * The RID leg is what makes a partitioned traversal indistinguishable from the serial one
   * (issue #4085). It is the same total order {@link RidScoreMinHeap} retains against, so sorting
   * the concatenated per-range results and keeping the first {@code k} reproduces the serial result
   * exactly, ties included.
   */
  static final Comparator<RidScore> BY_SCORE_DESC = (a, b) -> {
    final int c = Float.compare(b.score(), a.score());
    return c != 0 ? c : SparseSegmentBuilder.compareRid(a.rid(), b.rid());
  };

  /**
   * Merge the per-range results of a partitioned {@link #topK(int[], float[], DimCursor[], int, RID, RID)}
   * back into the global top-K.
   * <p>
   * Correct because the ranges partition the RID space: every document is scored by exactly one
   * worker, against the same query, so a document's score does not depend on which range it fell in.
   * Only the <i>pruning</i> differs - a worker with a weaker local watermark keeps candidates the
   * serial scan would have discarded - and keeping the best {@code k} of the union discards those
   * again.
   */
  static List<RidScore> mergeRanges(final List<List<RidScore>> ranges, final int k) {
    int total = 0;
    for (final List<RidScore> r : ranges)
      total += r.size();
    final List<RidScore> all = new ArrayList<>(total);
    for (final List<RidScore> r : ranges)
      all.addAll(r);
    all.sort(BY_SCORE_DESC);
    return all.size() <= k ? all : new ArrayList<>(all.subList(0, k));
  }

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

  /**
   * Grouped top-K: one min-heap per group key, threshold = min across per-group worst scores.
   * <p>
   * Admission of a brand-new group key once {@code limit} slots are taken is a comparison against
   * the weakest currently open group's <i>peak</i> (its own best member) - see {@link #topKGrouped}
   * for why (issue #6936), and for what {@link GroupState#possiblyShort} records (issue #8002).
   */
  private static final class GroupedCollector implements Collector {
    private final int                                        limit;
    private final int                                        groupSize;
    private final Function<RID, Object>                      groupKeyResolver;
    private final Set<RID>                                   allowedRIDs;
    private final boolean                                    filterActive;
    /** RIDs whose committed copy the caller is superseding with its own uncommitted one (issue #7966). */
    private final Set<RID>                                   excludedRIDs;
    private final boolean                                    exclusionActive;
    private final HashMap<Object, GroupState>                groups;
    private int                                              filledGroups;
    private float                                            threshold = Float.NEGATIVE_INFINITY;
    /**
     * Whether some candidate has already been turned away for its group - a new key that lost to the weakest open
     * group, or a whole group evicted. From then on a key opened by eviction may be one whose earlier members were
     * among those turned away (issue #8002).
     */
    private boolean                                          displaced;

    GroupedCollector(final int limit, final int groupSize, final Function<RID, Object> groupKeyResolver,
        final Set<RID> allowedRIDs, final Set<RID> excludedRIDs) {
      this.limit = limit;
      this.groupSize = groupSize;
      this.groupKeyResolver = groupKeyResolver;
      this.allowedRIDs = allowedRIDs;
      this.filterActive = allowedRIDs != null && !allowedRIDs.isEmpty();
      this.excludedRIDs = excludedRIDs;
      this.exclusionActive = excludedRIDs != null && !excludedRIDs.isEmpty();
      this.groups = new HashMap<>(limit);
    }

    @Override
    public float threshold() {
      return threshold;
    }

    @Override
    public boolean accepts(final RID rid) {
      if (filterActive && !allowedRIDs.contains(rid))
        return false;
      return !exclusionActive || !excludedRIDs.contains(rid);
    }

    @Override
    public void collect(final RID rid, final float score) {
      final Object groupKey = groupKeyResolver.apply(rid);
      GroupState group = groups.get(groupKey);
      boolean stateChanged;
      if (group != null) {
        stateChanged = admit(group, rid, score);
      } else if (groups.size() < limit) {
        // A free slot exists only before the first eviction and before the threshold first rises (both need every
        // slot taken), and no key has been rejected yet, so this key has never been seen: the group is complete.
        group = new GroupState(groupSize);
        groups.put(groupKey, group);
        stateChanged = admit(group, rid, score);
      } else {
        // All limit slots are taken by other keys. Find the weakest one - the lowest peak (best
        // member) among the open groups - and only displace it if this candidate beats that peak;
        // a linear scan is cheap here since limit is small and this branch only runs for a
        // genuinely new key, far rarer than a plain collect() call.
        Object weakestKey = null;
        GroupState weakest = null;
        for (final Map.Entry<Object, GroupState> e : groups.entrySet()) {
          if (weakest == null || e.getValue().peak < weakest.peak) {
            weakest = e.getValue();
            weakestKey = e.getKey();
          }
        }
        if (weakest != null && score > weakest.peak) {
          if (weakest.heap.isFull())
            filledGroups--;
          groups.remove(weakestKey);
          group = new GroupState(groupSize);
          // Earlier members of this key may have been rejected while it was not open, or skipped by a threshold
          // raised while the group set looked settled. Neither can have happened if nothing was ever turned away
          // and nothing was ever pruned.
          group.possiblyShort = displaced || threshold != Float.NEGATIVE_INFINITY;
          groups.put(groupKey, group);
          stateChanged = admit(group, rid, score);
        } else
          stateChanged = false;
        displaced = true;
      }
      // Recompute the global threshold once every group has reached capacity. Until then it stays
      // at NEGATIVE_INFINITY: a candidate could still open a new group, evict a weaker one, or fill
      // an empty slot inside an existing one, so pruning against a per-group watermark would be
      // incorrect. Never lowered once raised: an eviction that drops filledGroups back under limit
      // simply skips this block on the next call rather than walking the threshold back, since a
      // DAAT traversal cannot un-skip documents it has already pruned past - which is why a group
      // opened after that point is marked possibly short above.
      if (stateChanged && filledGroups == limit && groups.size() == limit) {
        float min = Float.POSITIVE_INFINITY;
        for (final GroupState g : groups.values()) {
          if (!g.heap.isEmpty() && g.heap.minScore() < min)
            min = g.heap.minScore();
        }
        if (min != Float.POSITIVE_INFINITY && min > threshold)
          threshold = min;
      }
    }

    /** Offers {@code (rid, score)} into {@code group}'s heap and keeps its peak/fill bookkeeping current. */
    private boolean admit(final GroupState group, final RID rid, final float score) {
      final boolean wasFull = group.heap.isFull();
      final boolean changed = group.heap.offer(rid, score);
      if (score > group.peak)
        group.peak = score;
      if (!wasFull && group.heap.isFull())
        filledGroups++;
      return changed;
    }

    /** The winning groups whose members this traversal may not all have seen, by key. Empty in the common case. */
    HashMap<Object, GroupState> possiblyShortGroups() {
      HashMap<Object, GroupState> out = null;
      for (final Map.Entry<Object, GroupState> e : groups.entrySet()) {
        if (e.getValue().possiblyShort) {
          if (out == null)
            out = new HashMap<>();
          out.put(e.getKey(), e.getValue());
        }
      }
      return out != null ? out : new HashMap<>(0);
    }

    List<RidScore> drain() {
      int total = 0;
      for (final GroupState g : groups.values())
        total += g.heap.size();
      final List<RidScore> out = new ArrayList<>(total);
      for (final GroupState g : groups.values())
        g.heap.drainInto(out);
      out.sort(BY_SCORE_DESC);
      return out;
    }
  }

  /**
   * Per-group top-{@code groupSize} for a fixed set of group keys (issue #8002): candidates of any other key are
   * ignored, and there is no distinct-group limit to enforce because the keys were chosen beforehand. The threshold is
   * the caller's {@code floor} until every group is full, then the weakest group's worst member if that is higher.
   */
  private static final class FixedGroupsCollector implements Collector {
    private final int                                groupSize;
    private final Function<RID, Object>              groupKeyResolver;
    private final Set<RID>                           allowedRIDs;
    private final boolean                            filterActive;
    private final Set<RID>                           excludedRIDs;
    private final boolean                            exclusionActive;
    private final HashMap<Object, RidScoreMinHeap>   groups;
    /** Members handed over by a previous traversal of the same snapshot, which must not be counted twice. */
    private       HashSet<RID>                       seeded;
    private       int                                filledGroups;
    private       float                              threshold;

    FixedGroupsCollector(final Set<Object> groupKeys, final int groupSize, final float floor,
        final Function<RID, Object> groupKeyResolver, final Set<RID> allowedRIDs, final Set<RID> excludedRIDs) {
      this.groupSize = groupSize;
      this.groupKeyResolver = groupKeyResolver;
      this.allowedRIDs = allowedRIDs;
      this.filterActive = allowedRIDs != null && !allowedRIDs.isEmpty();
      this.excludedRIDs = excludedRIDs;
      this.exclusionActive = excludedRIDs != null && !excludedRIDs.isEmpty();
      this.groups = new HashMap<>(groupKeys.size() * 2);
      for (final Object key : groupKeys)
        groups.put(key, new RidScoreMinHeap(groupSize));
      this.threshold = floor;
    }

    /** Pre-loads {@code key}'s heap with members already scored on this snapshot, so pruning can start at once. */
    void seed(final Object key, final RidScoreMinHeap members) {
      final List<RidScore> list = new ArrayList<>(members.size());
      members.drainInto(list);
      if (seeded == null)
        seeded = new HashSet<>();
      final RidScoreMinHeap heap = groups.get(key);
      for (final RidScore m : list) {
        seeded.add(m.rid());
        offer(heap, m.rid(), m.score());
      }
    }

    @Override
    public float threshold() {
      return threshold;
    }

    @Override
    public boolean accepts(final RID rid) {
      if (filterActive && !allowedRIDs.contains(rid))
        return false;
      if (exclusionActive && excludedRIDs.contains(rid))
        return false;
      return seeded == null || !seeded.contains(rid);
    }

    @Override
    public void collect(final RID rid, final float score) {
      if (score <= threshold)
        return;
      final RidScoreMinHeap heap = groups.get(groupKeyResolver.apply(rid));
      if (heap != null)
        offer(heap, rid, score);
    }

    private void offer(final RidScoreMinHeap heap, final RID rid, final float score) {
      final boolean wasFull = heap.isFull();
      if (!heap.offer(rid, score))
        return;
      if (!wasFull && heap.isFull())
        filledGroups++;
      if (filledGroups == groups.size()) {
        float min = Float.POSITIVE_INFINITY;
        for (final RidScoreMinHeap g : groups.values())
          if (g.minScore() < min)
            min = g.minScore();
        if (min > threshold)
          threshold = min;
      }
    }

    List<RidScore> drain() {
      int total = 0;
      for (final RidScoreMinHeap g : groups.values())
        total += g.size();
      final List<RidScore> out = new ArrayList<>(total);
      for (final RidScoreMinHeap g : groups.values())
        g.drainInto(out);
      out.sort(BY_SCORE_DESC);
      return out;
    }
  }

  /**
   * One open group's retained members plus its peak (best member ever admitted), tracked apart from
   * the min-heap since {@link RidScoreMinHeap} exposes only the current minimum.
   */
  private static final class GroupState {
    RidScoreMinHeap heap;
    float           peak = Float.NEGATIVE_INFINITY;
    /** Opened when earlier members of its key may already have been turned away or pruned (issue #8002). */
    boolean         possiblyShort;

    GroupState(final int groupSize) {
      this.heap = new RidScoreMinHeap(groupSize);
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
