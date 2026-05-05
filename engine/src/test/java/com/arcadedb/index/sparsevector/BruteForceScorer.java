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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Reference brute-force scorer used in tests to validate {@link BmwScorer} correctness.
 * <p>
 * Walks every posting in every source for every query dim, accumulates the partial dot product
 * per RID, and returns the top-K. Sources are passed oldest-to-newest; tombstones in any source
 * mask the RID across older sources.
 * <p>
 * <b>Tombstone semantics.</b> A tombstone in any one of the query-dim cursors masks the RID
 * across <i>all</i> query dims, matching the BMW pivot rule. This is the whole-document-delete
 * contract documented on
 * {@link PaginatedSparseVectorEngine#put(int, com.arcadedb.database.RID, float)} /
 * {@link PaginatedSparseVectorEngine#remove(int, com.arcadedb.database.RID)}: the engine treats a
 * tombstone as "this RID is gone", not "this one dim of this RID is gone". Partial-dim updates
 * are not supported; rewrite the document's full posting set in the same write batch instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class BruteForceScorer {

  private BruteForceScorer() {
    // utility class
  }

  public static List<RidScore> topK(final int[] queryDims, final float[] queryWeights, final PaginatedSegmentReader[] segmentsOldestFirst,
      final int k) throws IOException {
    if (k <= 0)
      return List.of();

    // For each query dim, find the newest-segment value at every RID. If the newest entry is a
    // tombstone the RID is masked across all query dims (matches BMW's "any aligned cursor at the
    // pivot is a tombstone -> drop the doc" rule). If absent from every segment, dim contributes 0.
    final Map<RID, Float> scores = new HashMap<>();
    final Set<RID> masked = new HashSet<>();

    for (int qi = 0; qi < queryDims.length; qi++) {
      final int dim = queryDims[qi];
      final float qw = queryWeights[qi];

      final Map<RID, Float> dimNewestWeight = new HashMap<>();
      final Set<RID> dimNewestSeen = new HashSet<>();

      // Iterate segments newest -> oldest; the first entry seen for a given RID wins.
      for (int s = segmentsOldestFirst.length - 1; s >= 0; s--) {
        try (final PaginatedSegmentDimCursor c = segmentsOldestFirst[s].openCursor(dim)) {
          if (c == null)
            continue;
          // Use the documented {@link SourceCursor} lifecycle: start() positions on the first
          // posting (or marks exhausted), then advance() walks the rest. The cursor impl tolerates
          // a missing start() by self-healing inside advance(), but relying on that is brittle
          // and sets a bad precedent for future SourceCursor implementors.
          c.start();
          while (!c.isExhausted()) {
            final RID r = c.currentRid();
            if (dimNewestSeen.add(r)) {
              if (c.isTombstone())
                masked.add(r);
              else
                dimNewestWeight.put(r, c.currentWeight());
            }
            if (!c.advance())
              break;
          }
        }
      }

      for (final var e : dimNewestWeight.entrySet())
        scores.merge(e.getKey(), qw * e.getValue(), Float::sum);
    }

    for (final RID r : masked)
      scores.remove(r);

    final PriorityQueue<RidScore> heap = new PriorityQueue<>(k, Comparator.comparing(RidScore::score));
    for (final var e : scores.entrySet()) {
      final RidScore rs = new RidScore(e.getKey(), e.getValue());
      if (heap.size() < k) {
        heap.add(rs);
      } else if (rs.score() > heap.peek().score()) {
        heap.poll();
        heap.add(rs);
      }
    }
    final List<RidScore> out = new ArrayList<>(heap);
    out.sort((a, b) -> Float.compare(b.score(), a.score()));
    return out;
  }
}
