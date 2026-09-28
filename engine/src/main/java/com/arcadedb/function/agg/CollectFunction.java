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

import com.arcadedb.function.HeapBufferingFunction;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;

import java.util.ArrayList;
import java.util.List;

/**
 * collect() aggregation function - collects values into a list.
 * Example: MATCH (n:Person) RETURN collect(n.name)
 * <p>
 * The list is held in heap until the aggregation ends, so it counts against
 * {@link com.arcadedb.GlobalConfiguration#QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP} (issue #8585) and its values are
 * charged to the heap budget of all the queries, through the operation of the step that aggregates (issue #8591).
 */
public class CollectFunction implements StatelessFunction, HeapBufferingFunction {
  private final List<Object>       collectedValues = new ArrayList<>();
  private       OperationHeapLimit limit;
  // TRUE WHEN NO STEP HANDED AN OPERATION: THE CHARGE IS THEN THIS FUNCTION'S TO GIVE BACK
  private       boolean            ownLimit;

  @Override
  public void setHeapLimit(final OperationHeapLimit owner) {
    limit = owner.child("collect()");
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
    // Collect the value (skip nulls per OpenCypher spec)
    if (args[0] != null) {
      collectedValues.add(args[0]);
      if (limit == null) {
        limit = OperationHeapLimit.of(context, "collect()");
        ownLimit = true;
      }
      limit.add(collectedValues.size(), args[0]);
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
      // THE LIST GOES TO THE CALLER, WHICH HOLDS IT FROM NOW ON
      limit.release();
    return new ArrayList<>(collectedValues);
  }
}
