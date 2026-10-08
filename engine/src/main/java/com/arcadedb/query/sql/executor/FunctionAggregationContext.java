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

import com.arcadedb.function.HeapBufferingFunction;
import com.arcadedb.function.sql.SQLAggregatedFunction;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.schema.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Delegates to an aggregate function for aggregation calculation
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class FunctionAggregationContext implements AggregationContext, HeapBufferingFunction {
  // a NULL is one more distinct value: the function sees it once, and ignores it or keeps it as it always did
  private static final Object NULL_KEY = new Object();

  private final SQLFunction        aggregateFunction;
  private       List<Expression>   params;
  // WHAT A FUNCTION THAT KEEPS EVERY VALUE HOLDS (list(), percentile()...), CHARGED TO THE HEAP BUDGET OF ALL THE
  // QUERIES THROUGH THE OPERATION OF THE STEP THAT AGGREGATES (ISSUE #8591); NULL FOR A RUNNING STATE OF FIXED SIZE
  private       OperationHeapLimit heapLimit;
  // THE DISTINCT VALUES ALREADY GIVEN TO THE FUNCTION - count(DISTINCT x) - OR NULL WHEN IT SEES EVERY VALUE (ISSUE #8889)
  private final Set<Object>      seen;

  public FunctionAggregationContext(final SQLFunction function, final List<Expression> params, final boolean distinct) {
    this.seen = distinct ? new HashSet<>() : null;
    this.aggregateFunction = function;
    this.params = params;
    if (this.params == null)
      this.params = new ArrayList<>();
    // Argument count does not change across the rows apply() is called for, so the check belongs here, once,
    // rather than on every row (#5884): this is the aggregate-projection dispatch path (used for both real
    // aggregate functions and any function projected with no FROM), a second entry point FunctionCall.execute()'s
    // checkArity does not cover.
    this.aggregateFunction.checkArity(new Object[this.params.size()]);
  }

  @Override
  public void setHeapLimit(final OperationHeapLimit owner) {
    // a DISTINCT aggregate remembers every distinct value it has seen, whatever the function keeps itself
    if (seen != null || aggregateFunction instanceof SQLAggregatedFunction function && function.holdsEveryValue())
      heapLimit = owner.child(aggregateFunction.getName() + "()");
  }

  @Override
  public Object getFinalValue() {
    return aggregateFunction.getResult();
  }

  @Override
  public boolean canMerge() {
    // the distinct values seen by two partial states cannot be told apart once each has folded them into its result
    return seen == null && aggregateFunction instanceof SQLAggregatedFunction function && function.canMergePartials();
  }

  @Override
  public void merge(final AggregationContext other) {
    ((SQLAggregatedFunction) aggregateFunction).mergePartial(
        (SQLAggregatedFunction) ((FunctionAggregationContext) other).aggregateFunction);
  }

  @Override
  public void apply(final Result next, final CommandContext context) {
    if (params.size() == 1) {
      applyValue(params.getFirst().execute(next, context), next, context);
      return;
    }

    final Object[] paramValues = new Object[params.size()];
    for (int i = 0; i < paramValues.length; i++)
      paramValues[i] = params.get(i).execute(next, context);
    applyValues(paramValues, next, context);
  }

  /** The arguments of this aggregation, which {@link #apply} evaluates on every row. */
  public List<Expression> getParams() {
    return params;
  }

  /** The function this aggregation feeds. */
  public SQLFunction getFunction() {
    return aggregateFunction;
  }

  /** Whether the function is fed every distinct value of the arguments once: {@code count(DISTINCT x)}. */
  public boolean isDistinct() {
    return seen != null;
  }

  /**
   * Feeds one row to a single-argument aggregation whose argument the caller evaluated already: the aggregation of a
   * GROUP BY reads it off the projection that computed it instead of evaluating it again (issue #9496). No array, no
   * list: the function gets the value through {@link SQLAggregatedFunction#aggregate}.
   */
  public void applyValue(final Object value, final Result next, final CommandContext context) {
    if (seen != null && !remember(normalizeForKey(value)))
      return;

    if (aggregateFunction instanceof SQLAggregatedFunction function)
      function.aggregate(next, value, context);
    else
      aggregateFunction.execute(next, null, null, new Object[] { value }, context);
    // a DISTINCT call is charged through the set of distinct values it remembers, which share the function's own items
    if (heapLimit != null && seen == null)
      heapLimit.chargeElement(value, 0);
  }

  /**
   * Feeds one row to the aggregation, with the values of its arguments evaluated already. The function may keep
   * {@code paramValues}: the caller passes an array of its own for every row.
   */
  public void applyValues(final Object[] paramValues, final Result next, final CommandContext context) {
    if (paramValues.length == 1) {
      applyValue(paramValues[0], next, context);
      return;
    }

    if (seen != null && !firstTimeSeen(paramValues))
      return;

    aggregateFunction.execute(next, null, null, paramValues, context);
    // a DISTINCT call is charged through the set of distinct values it remembers, which share the function's own items
    if (heapLimit != null && seen == null)
      heapLimit.chargeElement(paramValues, 0);
  }

  /**
   * Records the argument values and tells whether they are new. Equality follows the rule of GROUP BY and SELECT
   * DISTINCT: numbers meet in a canonical form, so {@code 1} and {@code 1.0}, or {@code 19.9} and {@code 19.90}, are
   * one value. A NULL is one more value, seen once.
   */
  private boolean firstTimeSeen(final Object[] paramValues) {
    final Object[] key = new Object[paramValues.length];
    for (int i = 0; i < key.length; i++)
      key[i] = normalizeForKey(paramValues[i]);
    return remember(Arrays.asList(key));
  }

  /** Whether the normalized {@code element} is seen for the first time, remembering it if it is. */
  private boolean remember(final Object element) {
    if (!seen.add(element))
      return false;

    if (heapLimit != null)
      heapLimit.add(seen.size(), element, HeapEstimator.HASH_ENTRY_BYTES + HeapEstimator.OBJECT_BYTES);
    return true;
  }

  private static Object normalizeForKey(final Object value) {
    return value == null ? NULL_KEY : Type.normalizeForKey(value);
  }
}
