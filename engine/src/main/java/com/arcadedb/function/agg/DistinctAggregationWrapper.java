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
package com.arcadedb.function.agg;

import com.arcadedb.database.RID;
import com.arcadedb.function.DistinctNumericKey;
import com.arcadedb.function.HeapBufferingFunction;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.HeapEstimator;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.utility.RidHashSet;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Wrapper that applies DISTINCT semantics to an aggregation function.
 * Tracks seen argument values and only delegates to the wrapped function for unique values.
 */
public class DistinctAggregationWrapper implements StatelessFunction, HeapBufferingFunction {
  private final StatelessFunction  delegate;
  private final Set<List<Object>>  seenValues = new HashSet<>();
  // THE VALUES SEEN THAT ARE ONE GRAPH ELEMENT, KEYED BY ITS ADDRESS: NULL UNTIL THE FIRST ONE
  private       RidHashSet         seenRecords;
  // THE VALUES SEEN ARE HELD UNTIL THE AGGREGATION ENDS: CHARGED TO THE HEAP BUDGET OF ALL THE QUERIES (ISSUE #8591)
  private       OperationHeapLimit limit;
  // TRUE WHEN NO STEP HANDED AN OPERATION: THE CHARGE IS THEN THIS FUNCTION'S TO GIVE BACK
  private       boolean            ownLimit;

  public DistinctAggregationWrapper(final StatelessFunction delegate) {
    this.delegate = delegate;
  }

  @Override
  public void setHeapLimit(final OperationHeapLimit owner) {
    limit = owner.child(delegate.getName() + "(DISTINCT)");
    ownLimit = false;
    HeapBufferingFunction.adopt(delegate, owner);
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  /**
   * The wrapped aggregation's contract: {@code collect(DISTINCT x)} takes the same arguments as {@code collect(x)}.
   */
  @Override
  public int getMinArgs() {
    return delegate.getMinArgs();
  }

  @Override
  public int getMaxArgs() {
    return delegate.getMaxArgs();
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    // Canonicalize cross-type numeric equality (e.g. INTEGER 1 vs FLOAT 1.0) so DISTINCT stays
    // consistent with Cypher's `=` operator (issue #5789). The delegate still executes on the
    // original, uncanonicalized arguments.
    if (args.length == 1 && DistinctNumericKey.canonicalize(args[0]) instanceof RID rid && rid.getPosition() >= 0) {
      // ONE GRAPH ELEMENT, THE count(DISTINCT x) OF A TRAVERSAL: ITS KEY IS ITS ADDRESS, KEPT AS TWO PRIMITIVES INSTEAD OF AN ARRAY, THE
      // LIST AROUND IT AND A HASH ENTRY PER VALUE. A RID KEY NEVER EQUALS A KEY OF ANOTHER KIND, SO THE TWO SETS CANNOT SPLIT ONE VALUE.
      // A NEGATIVE POSITION NAMES NO RECORD (ITS EQUALITY READS THE ENDPOINTS) AND TAKES THE GENERAL ROUTE
      if (seenRecords == null)
        seenRecords = new RidHashSet();
      if (!seenRecords.add(rid.getBucketId(), rid.getPosition()))
        return null;
      ensureLimit(context);
      limit.check(seenRecords.size() + (long) seenValues.size());
      limit.charge(RidHashSet.BYTES_PER_ENTRY);
      return delegate.execute(args, context);
    }

    final Object[] key = new Object[args.length];
    for (int i = 0; i < args.length; i++)
      key[i] = DistinctNumericKey.canonicalize(args[i]);
    if (!seenValues.add(Arrays.asList(key)))
      return null;
    ensureLimit(context);
    // THE KEY, THE LIST THAT WRAPS IT AND ITS ENTRY IN THE SET
    limit.add(seenValues.size() + (long) (seenRecords != null ? seenRecords.size() : 0), key,
        HeapEstimator.HASH_ENTRY_BYTES + HeapEstimator.OBJECT_BYTES);
    return delegate.execute(args, context);
  }

  private void ensureLimit(final CommandContext context) {
    if (limit == null) {
      limit = OperationHeapLimit.of(context, delegate.getName() + "(DISTINCT)");
      ownLimit = true;
    }
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getAggregatedResult() {
    if (ownLimit)
      limit.release();
    return delegate.getAggregatedResult();
  }
}
