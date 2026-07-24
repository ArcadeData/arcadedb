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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;

/**
 * Top-K scoring with BlockMax-WAND DAAT (document-at-a-time) over merged dim cursors.
 * <p>
 * Algorithm overview (see {@code docs/sparse-vector-storage-design.md} for the contract):
 * <ol>
 *   <li>Open one {@link DimCursor} per query dim. Each cursor merges across all sources.</li>
 *   <li>Sort cursors by current RID ascending. The smallest RID is the next candidate.</li>
 *   <li>Compute the pivot: the smallest prefix index whose accumulated upper bound exceeds
 *       the current threshold. A cursor whose RID is strictly less than the pivot's RID can
 *       only be skipped past, never scored, because no remaining contribution from dims
 *       1..pivot can drag its score over the threshold.</li>
 *   <li>If the head cursor matches the pivot RID, score the doc and advance every aligned
 *       cursor. Otherwise skip head cursors forward to the pivot RID (block-skip when possible).</li>
 *   <li>Repeat until no live cursor remains, or the prefix-sum can no longer beat the threshold.</li>
 * </ol>
 * <p>
 * <b>Tombstone semantics.</b> A tombstone observed on any one of the aligned cursors at the
 * candidate RID skips the whole document - the loop drops the candidate from this query without
 * scoring even the dims that have live postings under the same RID. This is the
 * whole-document-delete contract documented on
 * {@link PaginatedSparseVectorEngine#put(int, com.arcadedb.database.RID, float)} and
 * {@link PaginatedSparseVectorEngine#remove(int, com.arcadedb.database.RID)}: the engine treats
 * a tombstone as "this RID is gone", not "this one dim of this RID is gone". Partial-dim updates
 * are not supported; rewrite the document's full posting set instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class BmwScorer {

  private BmwScorer() {
    // utility class
  }

  /**
   * Top-K BMW DAAT over the merged sources for each query dim. Sources for each dim are passed
   * implicitly via {@link DimCursor}: the caller assembles those (typically once per query) by
   * constructing a {@link DimCursor} with the per-dim {@link SourceCursor} list.
   * <p>
   * The caller passes parallel arrays {@code queryDims}, {@code queryWeights} of identical length;
   * each dim must be unique. The {@code cursors} array is parallel to those (one cursor per dim),
   * with {@code null} for dims absent from every source.
   *
   * @return list of up to {@code k} (RID, score) pairs sorted by score descending.
   * @throws IllegalArgumentException if the three input arrays have mismatched lengths, or any
   *                                  query weight is NaN, infinite, or negative. BMW pruning
   *                                  requires the per-dim contribution upper bound to be
   *                                  monotonically non-decreasing in the prefix sum, which a
   *                                  negative weight would break (pivot search would never
   *                                  converge and the result set would silently be wrong).
   * @throws IOException              if a {@link DimCursor#start} / {@link DimCursor#advance} /
   *                                  {@link DimCursor#seekTo} fails to read its underlying source.
   */
  public static List<RidScore> topK(final int[] queryDims, final float[] queryWeights, final DimCursor[] cursors, final int k)
      throws IOException {
    if (queryDims.length != queryWeights.length || queryWeights.length != cursors.length)
      throw new IllegalArgumentException("queryDims, queryWeights, cursors must have the same length");
    // BMW pruning relies on the prefix-sum of {@code queryWeight * upperBoundRemaining} being
    // monotonically non-decreasing per added dim, so a negative query weight would let the
    // running sum drop, the pivot search would never converge, and the result set would
    // silently be wrong. Match the non-negativity contract enforced by
    // {@link com.arcadedb.index.sparsevector.LSMSparseVectorIndex#put} on the document side.
    for (final float w : queryWeights) {
      if (Float.isNaN(w) || Float.isInfinite(w))
        throw new IllegalArgumentException("query weights must be finite numbers; got " + w);
      if (w < 0.0f)
        throw new IllegalArgumentException("query weights must be non-negative; got " + w);
    }
    if (k <= 0)
      return List.of();

    // Filter out null/exhausted cursors. Each surviving entry holds (cursor, queryWeight).
    final List<DimEntry> live = new ArrayList<>(cursors.length);
    for (int i = 0; i < cursors.length; i++) {
      if (cursors[i] == null)
        continue;
      cursors[i].start();
      if (cursors[i].isExhausted())
        continue;
      live.add(new DimEntry(cursors[i], queryWeights[i]));
    }
    if (live.isEmpty())
      return List.of();

    // Min-heap of the current top-K. Peek == K-th best score so far == threshold candidate.
    final PriorityQueue<RidScore> heap = new PriorityQueue<>(k, Comparator.comparing(RidScore::score));
    float threshold = Float.NEGATIVE_INFINITY;

    while (!live.isEmpty()) {
      // Insertion-sort live by currentRid ascending. Inputs are mostly already sorted from the previous iteration.
      sortByCurrentRid(live);

      // Pivot search.
      final int pivot = findPivot(live, threshold);
      if (pivot < 0)
        break;

      final RID pivotRid = live.get(pivot).cursor.currentRid();

      // Block-Max WAND shallow advance: before touching the pivot doc, test the *tight* per-block
      // maxima. If the sum of block maxima over every cursor that could contribute to pivotRid
      // cannot beat the threshold, no document in the covered block range can enter the top-K, so
      // skip the whole range without decoding a single posting. This is what turns the plain-WAND
      // suffix-max pivot (which barely prunes on flat SPLADE weight distributions) into real BMW.
      if (tryBlockMaxSkip(live, pivot, pivotRid, threshold)) {
        removeExhausted(live);
        continue;
      }

      if (live.get(0).cursor.currentRid().equals(pivotRid)) {
        // Score the doc.
        boolean tombstoned = false;
        float score = 0.0f;
        for (final DimEntry e : live) {
          if (!pivotRid.equals(e.cursor.currentRid()))
            break;  // sorted so the run of matching cursors is a prefix
          if (e.cursor.isTombstone()) {
            tombstoned = true;
            break;
          }
          score += e.queryWeight * e.cursor.currentWeight();
        }
        if (!tombstoned) {
          if (heap.size() < k) {
            heap.add(new RidScore(pivotRid, score));
            if (heap.size() == k)
              threshold = heap.peek().score();
          } else if (score > threshold) {
            heap.poll();
            heap.add(new RidScore(pivotRid, score));
            threshold = heap.peek().score();
          }
        }
        // Advance every aligned cursor.
        for (final DimEntry e : live) {
          if (pivotRid.equals(e.cursor.currentRid()))
            e.cursor.advance();
        }
      } else {
        // Skip the prefix [0..pivot] forward to pivotRid. Cursors past `pivot` are already at >= pivotRid (sorted).
        for (int i = 0; i <= pivot; i++) {
          final DimEntry e = live.get(i);
          if (SparseSegmentBuilder.compareRid(e.cursor.currentRid(), pivotRid) < 0)
            e.cursor.seekTo(pivotRid);
        }
      }

      removeExhausted(live);
    }

    // Convert min-heap to sorted-descending list.
    final List<RidScore> out = new ArrayList<>(heap);
    out.sort((a, b) -> Float.compare(b.score(), a.score()));
    return out;
  }

  /**
   * Top-K BMW DAAT with traversal-integrated {@code groupBy} / {@code groupSize} (issue #4071).
   * Replaces the global K-heap with a per-group min-heap so the post-traversal filter that the
   * MVP applied on top of {@link #topK} no longer needs an over-fetched candidate pool. The
   * {@code groupKeyResolver} is consulted once per scored document; the resolver typically reads
   * the group field off the materialised record, so callers should keep it cheap.
   * <p>
   * <b>Threshold semantics with per-group state.</b> The BMW pruning threshold is a lower bound on
   * any score that could still enter the result set. For non-grouped top-K that is the K-th best
   * score so far; for grouped top-K the analogue is "the lowest score that could replace any
   * group's worst member". Until {@code limit} groups have all reached {@code groupSize} (so any
   * candidate could open a new group or fill an empty slot), the threshold stays at
   * {@link Float#NEGATIVE_INFINITY} and the loop accepts every candidate that survives BMW's other
   * gates. Once globally full, the threshold is the minimum across per-group worst scores - any
   * score below it cannot beat any group's worst, so the BMW prefix-sum pivot can prune it
   * straight away. The threshold is conservative (a candidate above it may still be rejected
   * because its specific group has a higher worst), which is fine: pruning is correct, just
   * slightly less aggressive than non-grouped top-K.
   * <p>
   * <b>{@code allowedRIDs} filter.</b> Applied inline in the scoring branch: a pivot RID outside
   * the whitelist is dropped without scoring (cursors still advance so the loop progresses). This
   * removes the over-fetch + post-filter pattern that {@link LSMSparseVectorIndex#topK} used to
   * compensate for highly selective filters.
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
    if (queryDims.length != queryWeights.length || queryWeights.length != cursors.length)
      throw new IllegalArgumentException("queryDims, queryWeights, cursors must have the same length");
    for (final float w : queryWeights) {
      if (Float.isNaN(w) || Float.isInfinite(w))
        throw new IllegalArgumentException("query weights must be finite numbers; got " + w);
      if (w < 0.0f)
        throw new IllegalArgumentException("query weights must be non-negative; got " + w);
    }
    if (groupKeyResolver == null)
      throw new IllegalArgumentException("groupKeyResolver must not be null");
    if (limit <= 0 || groupSize <= 0)
      return List.of();

    final List<DimEntry> live = new ArrayList<>(cursors.length);
    for (int i = 0; i < cursors.length; i++) {
      if (cursors[i] == null)
        continue;
      cursors[i].start();
      if (cursors[i].isExhausted())
        continue;
      live.add(new DimEntry(cursors[i], queryWeights[i]));
    }
    if (live.isEmpty())
      return List.of();

    final boolean filterActive = allowedRIDs != null && !allowedRIDs.isEmpty();

    // Per-group state. Each min-heap holds at most groupSize entries; peek == that group's worst.
    final HashMap<Object, PriorityQueue<RidScore>> groups = new HashMap<>(limit);
    int filledGroups = 0;
    float threshold = Float.NEGATIVE_INFINITY;

    while (!live.isEmpty()) {
      sortByCurrentRid(live);

      final int pivot = findPivot(live, threshold);
      if (pivot < 0)
        break;

      final RID pivotRid = live.get(pivot).cursor.currentRid();

      // Block-Max WAND shallow advance (see the note in {@link #topK}). Grouped top-K keeps the
      // threshold at NEGATIVE_INFINITY until every group is full, so this gate stays inert until
      // there is a real watermark to prune against - exactly like the non-grouped path.
      if (tryBlockMaxSkip(live, pivot, pivotRid, threshold)) {
        removeExhausted(live);
        continue;
      }

      if (live.get(0).cursor.currentRid().equals(pivotRid)) {
        if (filterActive && !allowedRIDs.contains(pivotRid)) {
          // Whitelist rejected. Skip the doc; advance every aligned cursor.
          for (final DimEntry e : live) {
            if (pivotRid.equals(e.cursor.currentRid()))
              e.cursor.advance();
          }
        } else {
          boolean tombstoned = false;
          float score = 0.0f;
          for (final DimEntry e : live) {
            if (!pivotRid.equals(e.cursor.currentRid()))
              break;
            if (e.cursor.isTombstone()) {
              tombstoned = true;
              break;
            }
            score += e.queryWeight * e.cursor.currentWeight();
          }
          if (!tombstoned) {
            // Resolve the candidate's group and apply per-group admission.
            final Object groupKey = groupKeyResolver.apply(pivotRid);
            final PriorityQueue<RidScore> group = groups.get(groupKey);
            boolean stateChanged = false;
            if (group == null) {
              if (groups.size() < limit) {
                final PriorityQueue<RidScore> opened = new PriorityQueue<>(groupSize, Comparator.comparing(RidScore::score));
                opened.add(new RidScore(pivotRid, score));
                groups.put(groupKey, opened);
                if (groupSize == 1)
                  filledGroups++;
                stateChanged = true;
              }
              // else: limit groups already open and this one is a new key - reject.
            } else if (group.size() < groupSize) {
              group.add(new RidScore(pivotRid, score));
              if (group.size() == groupSize)
                filledGroups++;
              stateChanged = true;
            } else if (score > group.peek().score()) {
              group.poll();
              group.add(new RidScore(pivotRid, score));
              stateChanged = true;
            }
            // Recompute the global threshold once every group has reached capacity. Until then
            // stays at NEGATIVE_INFINITY: a candidate could still open a new group or fill an
            // empty slot inside an existing one, so BMW pruning would be incorrect.
            if (stateChanged && filledGroups == limit && groups.size() == limit)
              threshold = computeGlobalMinWorst(groups);
          }
          // Advance every aligned cursor.
          for (final DimEntry e : live) {
            if (pivotRid.equals(e.cursor.currentRid()))
              e.cursor.advance();
          }
        }
      } else {
        // Skip the prefix [0..pivot] forward to pivotRid. Cursors past pivot already at >= pivotRid.
        for (int i = 0; i <= pivot; i++) {
          final DimEntry e = live.get(i);
          if (SparseSegmentBuilder.compareRid(e.cursor.currentRid(), pivotRid) < 0)
            e.cursor.seekTo(pivotRid);
        }
      }

      removeExhausted(live);
    }

    int total = 0;
    for (final PriorityQueue<RidScore> pq : groups.values())
      total += pq.size();
    final List<RidScore> out = new ArrayList<>(total);
    for (final PriorityQueue<RidScore> pq : groups.values())
      out.addAll(pq);
    out.sort((a, b) -> Float.compare(b.score(), a.score()));
    return out;
  }

  /**
   * Minimum score across per-group worst-score watermarks. Used as the BMW pruning threshold once
   * every group has reached capacity; any candidate score at or below this value cannot beat any
   * group's worst member and so cannot enter the result set, so BMW's prefix-sum pivot can prune
   * the rest of the loop without scoring it.
   */
  private static float computeGlobalMinWorst(final HashMap<Object, PriorityQueue<RidScore>> groups) {
    float min = Float.POSITIVE_INFINITY;
    for (final PriorityQueue<RidScore> pq : groups.values()) {
      final RidScore worst = pq.peek();
      if (worst != null && worst.score() < min)
        min = worst.score();
    }
    return min == Float.POSITIVE_INFINITY ? Float.NEGATIVE_INFINITY : min;
  }

  // ---------- internals ----------

  /**
   * Block-Max WAND shallow advance. {@code live} is sorted by current RID ascending and
   * {@code pivotRid == live.get(pivot).cursor.currentRid()}. Sums the <i>tight</i> per-block
   * maxima of every cursor that could contribute to {@code pivotRid} - the pivot prefix
   * {@code [0..pivot]} plus the run of cursors tied at {@code pivotRid} immediately after it - and
   * compares against {@code threshold}:
   * <ul>
   *   <li>If the block-max sum exceeds the threshold, {@code pivotRid} might still be a candidate;
   *       return {@code false} so the caller scores or aligns it normally.</li>
   *   <li>Otherwise no document in {@code [pivotRid, minBlockEnd]} can beat the threshold (only
   *       these cursors align at {@code pivotRid}, and WAND already proved every doc &lt; pivotRid
   *       is a non-candidate). Skip: seek those cursors forward past the min block boundary,
   *       bounded by the first cursor that sits strictly after {@code pivotRid} so the recomputed
   *       pivot stays correct. Return {@code true}.</li>
   * </ul>
   * The skip is a strict advance (the new target is always &gt; {@code pivotRid}), so the outer
   * loop cannot stall. Block maxima are read from in-memory headers only - no posting is decoded.
   *
   * @return {@code true} if a block was skipped (cursors advanced), {@code false} if the caller
   *         must fall through to the score/align path.
   */
  private static boolean tryBlockMaxSkip(final List<DimEntry> live, final int pivot, final RID pivotRid, final float threshold)
      throws IOException {
    // Extend the prefix over the run of cursors tied at pivotRid: those beyond `pivot` also align
    // at pivotRid and so must be included in the tight bound, otherwise the sum would under-count
    // pivotRid's real ceiling. Cursors past this run sit strictly after pivotRid and cannot align.
    int end = pivot;
    while (end + 1 < live.size() && live.get(end + 1).cursor.currentRid().equals(pivotRid))
      end++;

    float blockMaxSum = 0.0f;
    RID minBlockEnd = null;
    for (int i = 0; i <= end; i++) {
      final DimEntry e = live.get(i);
      blockMaxSum += e.queryWeight * e.cursor.blockMaxAt(pivotRid);
      if (blockMaxSum > threshold)
        return false;  // pivotRid may still make the top-K; score/align it normally.
      final RID be = e.cursor.blockEndAt(pivotRid);
      if (be != null && (minBlockEnd == null || SparseSegmentBuilder.compareRid(be, minBlockEnd) < 0))
        minBlockEnd = be;
    }

    // blockMaxSum <= threshold: pivotRid and its block range are all non-candidates.
    if (minBlockEnd == null)
      return false;  // no finite block boundary (only loose/memtable sources) - cannot block-skip.

    // Next candidate = first RID strictly past the limiting block boundary, but never past the
    // first cursor that sits after the tied run (that cursor could align a doc the recomputed
    // pivot must see). RID successor is (bucket, position+1); seekTo lands on the first real
    // posting >= that, i.e. the first strictly greater than minBlockEnd.
    RID candidate = new RID(minBlockEnd.getBucketId(), minBlockEnd.getPosition() + 1);
    if (end + 1 < live.size()) {
      final RID nextRid = live.get(end + 1).cursor.currentRid();
      if (SparseSegmentBuilder.compareRid(nextRid, candidate) < 0)
        candidate = nextRid;
    }

    for (int i = 0; i <= end; i++) {
      final DimEntry e = live.get(i);
      if (SparseSegmentBuilder.compareRid(e.cursor.currentRid(), candidate) < 0)
        e.cursor.seekTo(candidate);
    }
    return true;
  }

  /** Returns the smallest index i such that the prefix sum of upperBound contributions exceeds threshold; -1 if none. */
  private static int findPivot(final List<DimEntry> live, final float threshold) {
    float prefix = 0.0f;
    for (int i = 0; i < live.size(); i++) {
      final DimEntry e = live.get(i);
      prefix += e.queryWeight * e.cursor.upperBoundRemaining();
      if (prefix > threshold)
        return i;
    }
    return -1;
  }

  /** In-place insertion sort (stable) by current RID ascending. Cursors past their current must be advanced first. */
  private static void sortByCurrentRid(final List<DimEntry> live) {
    for (int i = 1; i < live.size(); i++) {
      final DimEntry curr = live.get(i);
      final RID currRid = curr.cursor.currentRid();
      int j = i - 1;
      while (j >= 0 && SparseSegmentBuilder.compareRid(live.get(j).cursor.currentRid(), currRid) > 0) {
        live.set(j + 1, live.get(j));
        j--;
      }
      live.set(j + 1, curr);
    }
  }

  private static void removeExhausted(final List<DimEntry> live) {
    int w = 0;
    for (int r = 0; r < live.size(); r++) {
      final DimEntry e = live.get(r);
      if (e.cursor.isExhausted())
        continue;
      if (r != w)
        live.set(w, e);
      w++;
    }
    while (live.size() > w)
      live.remove(live.size() - 1);
  }

  /**
   * Per-cursor entry. The cursor's own {@link DimCursor#isExhausted} is the source of truth on
   * exhaustion; we do not duplicate that flag here. {@link DimCursor#advance} and
   * {@link DimCursor#seekTo} return {@code false} only after they have already set the cursor's
   * internal {@code exhausted} flag, so {@link #removeExhausted} consults
   * {@code cursor.isExhausted()} alone.
   */
  private static final class DimEntry {
    final DimCursor cursor;
    final float     queryWeight;

    DimEntry(final DimCursor cursor, final float queryWeight) {
      this.cursor = cursor;
      this.queryWeight = queryWeight;
    }
  }
}
