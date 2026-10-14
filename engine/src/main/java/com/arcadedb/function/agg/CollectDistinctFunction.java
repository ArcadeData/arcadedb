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

import com.arcadedb.database.Identifiable;
import com.arcadedb.function.DistinctNumberWrapper;
import com.arcadedb.function.DistinctNumericKey;
import com.arcadedb.function.HeapBufferingFunction;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.HeapEstimator;
import com.arcadedb.query.sql.executor.OperationHeapLimit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * collect(DISTINCT ...) aggregation function - collects unique values into a list.
 * Uses LinkedHashSet to maintain insertion order while eliminating duplicates.
 * Example: MATCH (n:Person) RETURN collect(DISTINCT n.name)
 */
public class CollectDistinctFunction implements StatelessFunction, HeapBufferingFunction {
  // A VALUE THAT IS ITS OWN KEY (A STRING, A BOOLEAN, ANY VALUE canonicalize() LEAVES ALONE) IS HELD AS IS, EVERY OTHER
  // ONE IN A DistinctNumberWrapper: THE TWO NEVER EQUAL EACH OTHER, AND THE COMMON collect(DISTINCT n.name) ALLOCATES NO
  // WRAPPER PER ROW
  private final Set<Object> distinctValues = new LinkedHashSet<>();
  /** The set is held in heap until the aggregation ends (issues #8585, #8591). */
  private       OperationHeapLimit limit;
  // TRUE WHEN NO STEP HANDED AN OPERATION: THE CHARGE IS THEN THIS FUNCTION'S TO GIVE BACK
  private       boolean            ownLimit;

  @Override
  public void setHeapLimit(final OperationHeapLimit owner) {
    limit = owner.child("collect(DISTINCT)");
    ownLimit = false;
  }

  @Override
  public String getName() {
    return "collect";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    final Object value = args[0];
    // Only add non-null values, keyed the way every other DISTINCT keys them (DistinctNumericKey): a graph element by
    // its identity, so proxies and reloaded records collapse, and a lightweight edge by an identity that tells parallel
    // twins apart (issue #10008); a number across its Java types, so INTEGER 1 and FLOAT 1.0 are one value as under
    // Cypher's `=` (issue #5789); a list or map by its canonicalized elements
    if (value != null) {
      final boolean added = distinctValues.add(needsKey(value) ? new DistinctNumberWrapper(value) : value);
      if (limit == null) {
        limit = OperationHeapLimit.of(context, "collect(DISTINCT)");
        ownLimit = true;
      }
      if (added)
        // THE VALUE, ITS WRAPPER AND ITS ENTRY IN THE SET
        limit.add(distinctValues.size(), value, HeapEstimator.HASH_ENTRY_BYTES + HeapEstimator.OBJECT_BYTES);
    }
    return null; // Intermediate result doesn't matter
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getAggregatedResult() {
    if (ownLimit)
      // THE VALUES GO TO THE CALLER, WHICH HOLDS THEM FROM NOW ON
      limit.release();
    // Unwrap the keys back to the original objects
    final List<Object> result = new ArrayList<>(distinctValues.size());
    for (final Object value : distinctValues)
      result.add(value instanceof DistinctNumberWrapper wrapper ? wrapper.getOriginal() : value);
    return result;
  }

  /** Whether {@code value} keys by something other than itself: see {@link DistinctNumericKey#canonicalize}. */
  private static boolean needsKey(final Object value) {
    return value instanceof Number || value instanceof Identifiable || value instanceof Collection || value instanceof Map;
  }
}
