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
import java.util.Set;

/**
 * Mutable accounting for the {@code groupBy} / {@code groupSize} cap: at most {@code limit} distinct group keys, at
 * most {@code groupSize} rows in each. Candidates must be offered in descending score order, because the rule is
 * first-come-first-served and only a score-ordered walk makes "first" mean "best" (issue #5761).
 * <p>
 * One instance is shared by the index-level cap in {@code LSMVectorIndex.findNeighborsFromVectorGrouped} and by the
 * SQL-layer cap in the {@code vector.neighbors} / {@code vector.sparseNeighbors} / {@code vector.fuse} functions, so
 * the two cannot drift apart: the index applies it per sub-index and the SQL layer re-applies it across sub-indexes,
 * and a candidate the index admitted must never be one the SQL layer would have counted differently. Re-applying it
 * over per-index answers picks the right groups but can leave one short of members another index ranked out;
 * {@link GroupedTopUpPlanner} fetches those first (issue #8002).
 * <p>
 * Lifetime is one query: instantiate, call {@link #admit(Object)} per candidate row in rank order, call
 * {@link #isFull()} to decide whether the loop can stop, discard. Not thread-safe.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GroupAdmissionState {
  /**
   * The group key a resolver answers for a candidate whose record it cannot read right now (issue #10002): deleted by a
   * commit that overlaps the search, or not yet published by one whose index rows the search already sees, since an
   * index takes a commit's rows before the commit publishes its records. Such a candidate belongs to no group:
   * {@link #admit(Object)} refuses it without opening a group or counting it in one, and every grouped search skips it
   * the same way. That is what keeps it from costing a group its place. Filed under the {@code null} group instead, as
   * it used to be, it took one of the search's {@code limit} group slots and kept a real group out of the answer; and
   * when the record became readable before the caller emitted it, it opened a group of its own there, so the answer
   * came back short while every stale-candidate count said nothing was missing.
   * <p>
   * Compared by identity. A record that is readable but has no value for the {@code groupBy} field still resolves to
   * {@code null}, the ordinary null group.
   */
  public static final Object UNRESOLVED = new Object() {
    @Override
    public String toString() {
      return "<unresolved>";
    }
  };

  private final HashMap<Object, Integer> perGroup = new HashMap<>();
  private final int                      limit;
  private final int                      groupSize;
  /** The only group keys this state may admit, or {@code null} for any. */
  private final Set<Object>              onlyGroups;
  private       int                      filledGroups = 0;

  public GroupAdmissionState(final int limit, final int groupSize) {
    this.limit = limit;
    this.groupSize = groupSize;
    this.onlyGroups = null;
  }

  /**
   * Admission restricted to a group set chosen beforehand (issue #8002): a candidate of any other key is refused, and
   * the state is full once every one of {@code onlyGroups} holds {@code groupSize} rows. This is the second phase of a
   * grouped search merged from several indexes, asking one of them for the members of groups that won overall.
   */
  public GroupAdmissionState(final Set<Object> onlyGroups, final int groupSize) {
    this.limit = onlyGroups.size();
    this.groupSize = groupSize;
    this.onlyGroups = onlyGroups;
  }

  /**
   * Decides whether a candidate row with the given group key should be kept. Side-effects the internal counters when
   * admitting. Returns {@code true} if admitted, {@code false} if the row must be skipped (group already full, this
   * would open a {@code (limit + 1)}-th group, the key is outside a restricted group set, or the key is
   * {@link #UNRESOLVED}).
   */
  public boolean admit(final Object groupKey) {
    if (groupKey == UNRESOLVED)
      return false;
    if (onlyGroups != null && !onlyGroups.contains(groupKey))
      return false;
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
