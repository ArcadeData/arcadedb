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

import java.util.HashMap;

/**
 * Mutable accounting for the {@code groupBy} / {@code groupSize} cap: at most {@code limit} distinct group keys, at
 * most {@code groupSize} rows in each. Candidates must be offered in descending score order, because the rule is
 * first-come-first-served and only a score-ordered walk makes "first" mean "best" (issue #5761).
 * <p>
 * One instance is shared by the index-level cap in {@code LSMVectorIndex.findNeighborsFromVectorGrouped} and by the
 * SQL-layer cap in the {@code vector.neighbors} / {@code vector.sparseNeighbors} / {@code vector.fuse} functions, so
 * the two cannot drift apart: the index applies it per sub-index and the SQL layer re-applies it across sub-indexes,
 * and a candidate the index admitted must never be one the SQL layer would have counted differently.
 * <p>
 * Lifetime is one query: instantiate, call {@link #admit(Object)} per candidate row in rank order, call
 * {@link #isFull()} to decide whether the loop can stop, discard. Not thread-safe.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GroupAdmissionState {
  private final HashMap<Object, Integer> perGroup = new HashMap<>();
  private final int                      limit;
  private final int                      groupSize;
  private       int                      filledGroups = 0;

  public GroupAdmissionState(final int limit, final int groupSize) {
    this.limit = limit;
    this.groupSize = groupSize;
  }

  /**
   * Decides whether a candidate row with the given group key should be kept. Side-effects the internal counters when
   * admitting. Returns {@code true} if admitted, {@code false} if the row must be skipped (group already full, or this
   * would open a {@code (limit + 1)}-th group).
   */
  public boolean admit(final Object groupKey) {
    final int existing = perGroup.getOrDefault(groupKey, 0);
    if (existing == 0 && perGroup.size() >= limit)
      return false;
    if (existing >= groupSize)
      return false;
    perGroup.put(groupKey, existing + 1);
    if (existing + 1 == groupSize)
      filledGroups++;
    return true;
  }

  /**
   * Returns {@code true} when {@code limit} groups have all reached {@code groupSize}, signalling the caller to break
   * out of its scoring loop. O(1).
   */
  public boolean isFull() {
    return filledGroups >= limit;
  }

  /**
   * Number of distinct group keys admitted so far. Used to report a search that ran out of candidates before it could
   * open {@code limit} groups.
   */
  public int distinctGroups() {
    return perGroup.size();
  }
}
