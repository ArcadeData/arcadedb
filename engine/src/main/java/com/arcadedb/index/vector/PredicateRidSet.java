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

import java.util.Set;

/**
 * The allow-list of a vector search whose members are decided one candidate at a time, by a predicate on the candidate's
 * record, instead of being listed up front: the {@code where} option of {@code vector.neighbors} (issue #10169) and the
 * {@code WHERE} of the Cypher {@code SEARCH} clause (issue #10782).
 * <p>
 * Its {@link #size()} is no count of what it accepts, so it never qualifies for a plan that enumerates the allow-list by
 * itself, and an index asks it {@link #contains}. The filtered walk keeps expanding until it holds {@code k} members or has
 * met every vector it can reach, so a walk it leaves short of {@code k} is short because the predicate accepts fewer records,
 * not because the graph is damaged: the scan that then completes the answer is reported as the expected cost of a selective
 * predicate, not as a graph to rebuild.
 * <p>
 * That walk is the plan of a predicate that keeps a fair share of the records. One that keeps few is cheaper to answer by
 * scoring its members directly (the pre-filter plan of issue #6502), and a predicate an index can answer can list them
 * without testing every vector: {@link #listCandidates} (issue #10921).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface PredicateRidSet extends Set<RID> {

  /**
   * The records of bucket {@code bucketId} this set may accept, listed through an index rather than decided one candidate at
   * a time, when there are no more than {@code maxCandidates} of them (issue #10921). An index asks it before choosing its
   * plan: a short listing lets it score the candidates directly instead of walking its graph until the predicate has
   * accepted {@code k} of them, which for a selective predicate means walking most of the graph.
   *
   * @return the candidates, or null when they cannot be listed without testing every record (no index answers the
   *         predicate) or they are more than {@code maxCandidates}
   */
  default Candidates listCandidates(final int bucketId, final int maxCandidates) {
    return null;
  }

  /**
   * Whether {@link #listCandidates} can list the candidates of bucket {@code bucketId} at all, whatever their number: an
   * index answers the predicate there. Reads no entry: an index asks it to decide whether a walk that turns out to be long
   * is worth giving up for the listing.
   */
  default boolean listsCandidates(final int bucketId) {
    return false;
  }

  /**
   * The records of one bucket a {@link PredicateRidSet} may accept.
   *
   * @param rids  the candidates, never empty when {@code exact} is false
   * @param exact whether every candidate is a member: the index answered the whole predicate, so the candidates are the
   *              set's members in that bucket and none needs testing. When false the candidates are a superset, and only
   *              {@link #contains} tells a member, at the cost of testing its record: an index is then expected to test
   *              them in the order it ranks them, and to stop once it holds enough members
   */
  record Candidates(Set<RID> rids, boolean exact) {
  }
}
