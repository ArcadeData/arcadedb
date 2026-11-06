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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.RID;
import com.arcadedb.query.facet.MatchSet;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.function.IntPredicate;
import java.util.function.Predicate;

/**
 * A fetch step that can hand the collect step of a FACETS query every match of the WHERE as RIDs, without loading a record: the rows
 * still come from the fetch as the plan pulls them, but the matches past the rows a LIMIT keeps are never read.
 */
public interface IndexedMatchSource {
  /**
   * Adds every match whose bucket {@code bucketFilter} accepts to {@code builder}, with fail mask 0. Stops early when the builder
   * refuses a match (memory budget): the builder then reports the set as over budget.
   *
   * @return false, having added nothing, when the matches cannot be enumerated without loading the records
   */
  boolean collectAllMatches(CommandContext ctx, MatchSet.Builder builder, IntPredicate bucketFilter);

  /** Whether a filter chained after the source passes every match it collects (it re-evaluates the source's own condition). */
  default boolean isOwnCondition(final WhereClause where) {
    return false;
  }

  /**
   * Implemented by the answer of an indexed function (the {@code Iterable} of records it returns) when that answer is every match
   * and can be enumerated by RID without loading a record.
   */
  interface Rids {
    /**
     * Hands every RID of the answer to {@code action} until it returns false: exactly the records its iterator would return, so a RID
     * whose record is gone (an index entry the caller's transaction deleted the record of) is left out.
     */
    void forEachRid(Predicate<RID> action);
  }
}
