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
import java.util.List;
import java.util.PriorityQueue;

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

  // ---------- internals ----------

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
