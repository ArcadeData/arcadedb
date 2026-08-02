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
package com.arcadedb.index.vector;

import com.arcadedb.database.RID;
import io.github.jbellis.jvector.util.Bits;

import java.util.Set;

/**
 * Tells JVector which graph ordinals may enter a search result: the ones that still map to a live vector, optionally
 * narrowed to an allow-list of RIDs.
 * <p>
 * The liveness half is not an optimisation, it is what keeps a delete from emptying a result set (issue #5558). A
 * deleted vector stays in the HNSW graph until the next rebuild, and JVector's beam stops as soon as it holds
 * {@code rerankK} <i>acceptable</i> candidates. Accepting everything therefore lets a query aimed at a deleted
 * neighbourhood fill its beam with tombstones, declare itself finished, and hand back nodes that the caller's
 * post-filter then removes one by one - down to nothing. Rejecting them here instead makes the same beam keep walking
 * until it has found live neighbours: the tombstones are still traversed (JVector expands a rejected node, it just
 * does not collect it), so the surviving vectors behind the hole stay reachable.
 * <p>
 * The predicate is deliberately identical to the post-filter applied to the search output, so nothing that would have
 * survived it is dropped here.
 * <p>
 * <b>Deliberately not memoized</b>, unlike {@link GroupedRIDBitsFilter}, which caches its per-ordinal verdicts so
 * JVector sees a stable answer across repeated calls within one search. That filter has to: its verdict consumes a
 * group budget, so answering twice would count an ordinal twice. This one has no state to protect, so it reads the
 * location map live and a delete committed mid-traversal takes effect immediately. The freshness is worth more than
 * the stability here, and it is safe because the result loop re-checks the same predicate before emitting - an
 * ordinal admitted just before its delete lands is dropped at the output rather than returned.
 * <p>
 * <b>This rests on one JVector behaviour.</b> "The tombstones are still traversed" is true because
 * {@code GraphSearcher.searchOneLayer} consults {@code acceptOrds} only to decide whether a popped node joins the
 * result heap, and expands its neighbours either way. A JVector upgrade that started pruning the neighbours of a
 * rejected node would silently make the vectors behind a deleted region unreachable again - the same bug this class
 * exists to fix, with the filter now causing it. If you are upgrading JVector, check that first;
 * {@code Issue5558DeletedRegionSearchTest} is what will tell you.
 * <p>
 * <b>Cost.</b> This runs on every search, not only on the RID-restricted ones that used to need a filter, so it is
 * worth knowing what it costs an index with no deletions at all. JVector calls {@code acceptOrds.get()} once per
 * <i>popped</i> candidate, not once per scored neighbour, so it does not scale with beam width times graph fan-out:
 * measured at 28.7 calls per query on a 50k-vector, 128-dimension INT8 index at {@code k=10}. One call is a
 * {@code getLocation} lookup at 7.5 ns, so the whole filter adds 0.22 us to a 172 us query - 0.13%. That is why there
 * is no "no tombstones, use Bits.ALL" fast path here: it would buy nothing measurable and would cost a second, subtly
 * different definition of which nodes a search may return.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LiveVectorBitsFilter implements Bits {
  private final Set<RID>            allowedRIDs;
  private final int[]               ordinalToVectorIdSnapshot;
  // NOT a snapshot, unlike the ordinal map: this is the live location map, read at traversal time so the filter
  // answers exactly what the post-filter on the search output will.
  private final VectorLocationIndex vectorIndex;

  /**
   * @param allowedRIDs               optional RID allow-list; {@code null} or empty means "every live vector"
   * @param ordinalToVectorIdSnapshot the ordinal map captured together with the vectors snapshot by the caller
   * @param vectorIndex               the live location map that answers whether a vector id is still live
   */
  LiveVectorBitsFilter(final Set<RID> allowedRIDs, final int[] ordinalToVectorIdSnapshot,
      final VectorLocationIndex vectorIndex) {
    this.allowedRIDs = allowedRIDs != null && !allowedRIDs.isEmpty() ? allowedRIDs : null;
    this.ordinalToVectorIdSnapshot = ordinalToVectorIdSnapshot;
    this.vectorIndex = vectorIndex;
  }

  @Override
  public boolean get(final int ordinal) {
    return admissibleLocation(ordinal, ordinalToVectorIdSnapshot, vectorIndex, allowedRIDs) != null;
  }

  /**
   * The location a graph ordinal may be answered with, or {@code null} if it may not: out of the ordinal map, no
   * longer live, or outside the allow-list. Shared with {@link GroupedRIDBitsFilter}, which needs the location itself
   * to resolve a group key and would otherwise carry a second copy of this predicate for the two to drift apart on.
   *
   * @param allowedRIDs optional RID allow-list; {@code null} or empty means "every live vector"
   */
  static VectorLocationIndex.VectorLocation admissibleLocation(final int ordinal, final int[] ordinalToVectorIdSnapshot,
      final VectorLocationIndex vectorIndex, final Set<RID> allowedRIDs) {
    if (ordinal < 0 || ordinal >= ordinalToVectorIdSnapshot.length)
      return null;

    final VectorLocationIndex.VectorLocation loc = vectorIndex.getLocation(ordinalToVectorIdSnapshot[ordinal]);
    if (loc == null || loc.deleted)
      return null;

    if (allowedRIDs != null && !allowedRIDs.isEmpty() && !allowedRIDs.contains(loc.rid))
      return null;

    return loc;
  }
}
