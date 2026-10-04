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

  private volatile boolean resolved;
  private volatile String  key;
  private volatile int     unstoredPlans;

  /**
   * The key is memoized, so the target and the WHERE of the statement must not change after it is parsed (the statement
   * cache hands out the same instance to every execution).
   *
   * @return the plan-cache key, or null when the source SELECT cannot be cached or has repeatedly not been stored
   */
  String resolve(final FromClause target, final WhereClause whereClause, final Timeout timeout) {
    if (!resolved) {
      final SelectStatement source = newSource(target, whereClause, timeout, true);
      key = source.executionPlanCanBeCached() ? source.getOriginalStatement() : null;
      resolved = true;
    }
    return unstoredPlans >= MAX_UNSTORED_PLANS ? null : key;
  }

  /**
   * Called after the source plan of a cache miss was built: a statement whose plans never reach the cache stops paying the
   * lookup and the statement copy on every execution.
   */
  void planned(final boolean stored) {
    unstoredPlans = stored ? 0 : unstoredPlans + 1;
  }

  static SelectStatement newSource(final FromClause target, final WhereClause whereClause, final Timeout timeout,
      final boolean cacheable) {
    final SelectStatement source = new SelectStatement();
    source.setTarget(target);
    source.setWhereClause(whereClause);
    if (timeout != null)
      source.setTimeout(timeout.copy());
    if (cacheable)
      // the planner works on this statement, the cache key is the text of an untouched copy
      source.setOriginalStatement(source.copy());
    return source;
  }
}
