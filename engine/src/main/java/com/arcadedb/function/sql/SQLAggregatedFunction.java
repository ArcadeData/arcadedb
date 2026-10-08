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
package com.arcadedb.function.sql;

import com.arcadedb.function.AggregatedFunction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;

import java.util.function.Consumer;

/**
 * Abstract base class for SQL aggregate functions (count, sum, avg, min, max, etc.).
 * <p>
 * Aggregate functions accumulate state across multiple records and return a final
 * result via {@link #getResult()}. This class implements {@link AggregatedFunction}
 * making SQL aggregates part of the unified function system.
 * </p>
 * <p>
 * The default aggregation behavior is determined by the number of configured parameters:
 * <ul>
 *   <li>Single parameter (e.g., {@code sum(price)}) → aggregates across all rows</li>
 *   <li>Multiple parameters (e.g., {@code sum(a, b, c)}) → per-row computation</li>
 * </ul>
 * Subclasses can override {@link #aggregateResults()} for custom aggregation logic.
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 * @see AggregatedFunction
 */
public abstract class SQLAggregatedFunction extends SQLFunctionConfigurableAbstract implements AggregatedFunction {
  /**
   * Feeds one argument's worth of input to a numeric accumulator: a {@link Number} goes straight in, a collection is
   * unrolled element by element, a null is skipped, and anything else is a client-facing type error.
   * <p>
   * That triage is the same three branches in every cross-row numeric aggregate, and #6390 exists precisely because
   * it was copied per function - {@code sum()} was hardened in #5799 and the copies in {@code avg}, {@code variance}
   * and {@code percentile} drifted away from it. It lives here so the next hardening reaches all of them.
   *
   * @param value       the argument to accumulate
   * @param accumulator where each numeric (or null) value is sent
   */
  protected void accumulateNumeric(final Object value, final Consumer<Number> accumulator) {
    if (value instanceof Number number)
      accumulator.accept(number);
    else if (MultiValue.isMultiValue(value))
      for (final Object item : MultiValue.getMultiValueIterable(value))
        accumulator.accept(requireNumericOrNull(item));
    else
      // A non-numeric, non-null, non-list value is a client-facing type error rather than a silently dropped one,
      // which used to make an all-invalid input indistinguishable from an all-null one (#5799, #6390).
      accumulator.accept(requireNumericOrNull(value));
  }


  protected SQLAggregatedFunction(final String name) {
    super(name);
  }

  /**
   * Feeds the only argument of one row to the cross-row state: what {@link #execute} does for a single-argument call in
   * an aggregation, without the per-row answer it also returns, which an aggregation never reads (issue #9496). That
   * answer is the running result for some functions - a running {@code sum()} or {@code avg()} would have to box a new
   * number on every row to give it - and the argument array is one more object per row. A function on the hot path of a
   * GROUP BY overrides this; any other gets {@link #execute} as before.
   *
   * @param self  what {@link #execute} receives as its first argument: the row being aggregated
   * @param value the value of the function's argument on that row
   */
  public void aggregate(final Object self, final Object value, final CommandContext context) {
    execute(self, null, null, new Object[] { value }, context);
  }

  /**
   * Determines whether this function should aggregate results across multiple records.
   * <p>
   * Default behavior: aggregate when called with a single parameter.
   * This matches SQL semantics where {@code SELECT sum(price) FROM ...} aggregates,
   * but {@code SELECT sum(a, b, c) FROM ...} computes per-row.
   * </p>
   *
   * @return true if results should be aggregated
   */
  @Override
  public boolean aggregateResults() {
    return configuredParameters.length == 1;
  }

  /**
   * Whether two instances of this function, each fed a disjoint part of the rows, can be combined by
   * {@link #mergePartial} into the state one instance fed every row would have: the partial aggregation a parallel
   * scan runs in its workers (issue #8523). {@code false} unless a function says otherwise, and a function answers
   * {@code true} only in its cross-row form ({@link #aggregateResults()}).
   */
  public boolean canMergePartials() {
    return false;
  }

  /**
   * Whether the function keeps every value it aggregates until it hands out its result - list(), percentile(), the
   * windows of the time-series functions - rather than a running state of a fixed size. What such a function holds is
   * charged to the heap budget all the running queries share (issue #8591). {@code false} unless a function says
   * otherwise.
   */
  public boolean holdsEveryValue() {
    return false;
  }

  /**
   * Folds into this instance the state of {@code other}, an instance of the same function configured with the same
   * parameters and fed rows this one was not. Only called when {@link #canMergePartials()} is {@code true}.
   */
  public void mergePartial(final SQLAggregatedFunction other) {
    throw new UnsupportedOperationException("Function '" + getName() + "' cannot merge partial aggregations");
  }

  /**
   * Returns the aggregated result after all records have been processed.
   * <p>
   * Subclasses must implement this to return their accumulated result.
   * </p>
   *
   * @return the aggregated result
   */
  @Override
  public abstract Object getResult();
}
