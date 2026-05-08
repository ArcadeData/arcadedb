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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Group-aware {@link Bits} filter for {@code vector.neighbors} grouped retrieval (issue #4071,
 * option 2 from the design discussion). Rejects HNSW candidates whose group has already reached
 * {@code groupSize} during traversal so JVector returns at most {@code limit * groupSize} eligible
 * nodes spread across at most {@code limit} distinct groups.
 * <p>
 * <b>Decision caching is mandatory.</b> JVector calls {@link #get(int)} multiple times for the
 * same ordinal during navigation and scoring; the contract for {@link Bits} is that the answer is
 * stable within one search. We cache admitted and rejected ordinals so repeat calls are O(1) and
 * the per-group counters are not double-incremented.
 * <p>
 * <b>Best-per-group is approximate, not exact.</b> Unlike the sparse-side BMW DAAT integration
 * which uses a per-group min-heap (so an arriving high-score candidate evicts the group's worst
 * member), {@link Bits} cannot consult scores - the search loop calls it to gate eligibility
 * before the score function ever runs. Group admission is therefore traversal-order-based: the
 * first {@code groupSize} eligible candidates per group encountered during HNSW traversal are
 * admitted, regardless of their score relative to later candidates in the same group. HNSW
 * traverses approximately best-first from the entry node, so the first encountered candidate per
 * group is usually one of the highest-scoring members, but pathological topologies can leave
 * better members unreachable once their group has hit capacity. Callers that require exact
 * best-per-group can post-process by widening {@code efSearch} so the traversal sees more of each
 * group.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GroupedRIDBitsFilter implements Bits {
  private final Set<RID>              allowedRIDs;
  private final int[]                 ordinalToVectorIdSnapshot;
  private final VectorLocationIndex   vectorIndexSnapshot;
  private final Function<RID, Object> groupKeyResolver;
  private final int                   limit;
  private final int                   groupSize;

  // Mutable per-search state. {@code groupCount} keys on the resolved group key (may be null);
  // {@code admitted} / {@code rejected} memoize per-ordinal decisions so we do not pay the doc
  // lookup twice and so JVector sees a stable answer across repeated {@code get} calls.
  private final HashMap<Object, Integer> groupCount = new HashMap<>();
  private final HashSet<Integer>         admitted   = new HashSet<>();
  private final HashSet<Integer>         rejected   = new HashSet<>();

  public GroupedRIDBitsFilter(final Set<RID> allowedRIDs, final int[] ordinalToVectorIdSnapshot,
      final VectorLocationIndex vectorIndexSnapshot, final Function<RID, Object> groupKeyResolver,
      final int limit, final int groupSize) {
    if (groupKeyResolver == null)
      throw new IllegalArgumentException("groupKeyResolver must not be null");
    if (limit <= 0)
      throw new IllegalArgumentException("limit must be > 0");
    if (groupSize <= 0)
      throw new IllegalArgumentException("groupSize must be > 0");
    this.allowedRIDs = allowedRIDs;
    this.ordinalToVectorIdSnapshot = ordinalToVectorIdSnapshot;
    this.vectorIndexSnapshot = vectorIndexSnapshot;
    this.groupKeyResolver = groupKeyResolver;
    this.limit = limit;
    this.groupSize = groupSize;
  }

  @Override
  public boolean get(final int ordinal) {
    if (admitted.contains(ordinal))
      return true;
    if (rejected.contains(ordinal))
      return false;

    if (ordinal < 0 || ordinal >= ordinalToVectorIdSnapshot.length) {
      rejected.add(ordinal);
      return false;
    }

    final int vectorId = ordinalToVectorIdSnapshot[ordinal];
    final VectorLocationIndex.VectorLocation loc = vectorIndexSnapshot.getLocation(vectorId);
    if (loc == null || loc.deleted) {
      rejected.add(ordinal);
      return false;
    }

    if (allowedRIDs != null && !allowedRIDs.isEmpty() && !allowedRIDs.contains(loc.rid)) {
      rejected.add(ordinal);
      return false;
    }

    // Resolve the group key via the caller-supplied resolver. The resolver is responsible for
    // doc lookup and nested-field traversal; null group keys are allowed (treated as a single
    // null bucket) to match the MVP's HashMap null-key handling.
    final Object groupKey;
    try {
      groupKey = groupKeyResolver.apply(loc.rid);
    } catch (final RuntimeException e) {
      rejected.add(ordinal);
      return false;
    }

    final Integer count = groupCount.get(groupKey);
    if (count == null) {
      if (groupCount.size() >= limit) {
        rejected.add(ordinal);
        return false;
      }
      groupCount.put(groupKey, 1);
      admitted.add(ordinal);
      return true;
    }
    if (count >= groupSize) {
      rejected.add(ordinal);
      return false;
    }
    groupCount.put(groupKey, count + 1);
    admitted.add(ordinal);
    return true;
  }
}
