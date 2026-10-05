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

import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.Timeout;
import com.arcadedb.query.sql.parser.WhereClause;

/**
 * Plan-cache key of the read side of an UPDATE or DELETE (the synthetic {@code SELECT FROM <target> WHERE <where>}), built
 * once per parsed statement. The statement itself comes from the statement cache, so the deep copy and the text rendering
 * the key needs are paid by the first execution only and a cache hit costs one map lookup (issue #9207).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DmlSourcePlanKey {
  /** Consecutive plannings that left nothing in the cache before the statement stops trying (e.g. a parameter dependent plan) */
  private static final int MAX_UNSTORED_PLANS = 3;

  private final    Statement owner;
  private volatile boolean   resolved;
  private volatile String    key;
  // heuristics: a lost update of these counters only delays or advances the give-up by one planning
  private volatile int       unstoredPlans;
  private volatile long      giveUpEpoch;

  /**
   * @param owner the UPDATE or DELETE this key belongs to
   */
  public DmlSourcePlanKey(final Statement owner) {
    this.owner = owner;
  }

  /**
   * The key is memoized, so the target and the WHERE of the statement must not change after it is parsed (the statement
   * cache hands out the same instance to every execution).
   *
   * @return the plan-cache key, or null when the source SELECT cannot be cached or has repeatedly not been stored
   */
  String resolve(final FromClause target, final WhereClause whereClause, final Timeout timeout, final long cacheEpoch) {
    if (!resolved) {
      final SelectStatement source = newSource(target, whereClause, timeout, "");
      String text = source.executionPlanCanBeCached() ? source.getOriginalStatement() : null;
      // a positional parameter prints as '?' whatever its number, while the plan reads the value by the number fixed at parse
      // time (the WHERE of "UPDATE A SET x = ? WHERE y = ?" reads parameter 1): the text cannot tell two statements apart that
      // read the same WHERE from different positions. The numbers follow from the text of the whole statement, so that text
      // completes the key: equal statements, however often they are parsed, still share one plan (issue #9245). A '?' inside a
      // string literal only costs sharing
      if (text != null && text.indexOf('?') >= 0)
        text += " /*dml:" + owner + "*/";
      key = text;
      resolved = true;
    }
    if (unstoredPlans >= MAX_UNSTORED_PLANS) {
      if (cacheEpoch == giveUpEpoch)
        return null;
      // a schema change invalidated the cache since the statement gave up: try again
      unstoredPlans = 0;
    }
    return key;
  }

  /**
   * Called after the source plan of a cache miss was built: a statement whose plans never reach the cache stops paying the
   * lookup and the statement copy on every execution.
   */
  void planned(final boolean stored, final long epochBeforePlanning, final long epochAfterPlanning) {
    if (stored)
      unstoredPlans = 0;
    else if (epochBeforePlanning == epochAfterPlanning) {
      // a plan discarded because a DDL ran meanwhile says nothing about the statement
      unstoredPlans++;
      giveUpEpoch = epochAfterPlanning;
    }
  }

  /**
   * @param key the key returned by {@link #resolve} under which the planner stores the plan, empty to key it by the plain text
   * of the source, null for a source that is not cached
   */
  static SelectStatement newSource(final FromClause target, final WhereClause whereClause, final Timeout timeout,
      final String key) {
    final SelectStatement source = new SelectStatement();
    source.setTarget(target);
    source.setWhereClause(whereClause);
    if (timeout != null)
      source.setTimeout(timeout.copy());
    if (key != null) {
      // the planner works on this statement, the cache key is the text of an untouched copy
      source.setOriginalStatement(source.copy());
      if (!key.isEmpty())
        source.originalStatementAsString = key;
    }
    return source;
  }
}
