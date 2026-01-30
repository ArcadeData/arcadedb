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
package com.arcadedb.function;

import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Interface for stateless functions that don't need record context.
 * <p>
 * Stateless functions are pure transformations that take input arguments and produce
 * output values without needing access to the current record being processed.
 * They are thread-safe and can be reused across queries.
 * </p>
 * <p>
 * Examples include:
 * <ul>
 *   <li>Text functions: text.indexOf, text.join, text.split</li>
 *   <li>Math functions: math.sigmoid, math.tanh</li>
 *   <li>Conversion functions: convert.toJson, convert.toMap</li>
 *   <li>Utility functions: util.md5, util.sha256</li>
 * </ul>
 * </p>
 * <p>
 * These functions are available in both Cypher and SQL query engines.
 * </p>
 * <p>
 * Stateless functions can also be aggregation functions by overriding
 * {@link #aggregateResults()} to return true and implementing {@link #getAggregatedResult()}.
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 * @see Function
 * @see RecordFunction
 */
public interface StatelessFunction extends Function {
  /**
   * Executes the function with the given arguments.
   * <p>
   * This method is called with pre-evaluated arguments and should return the
   * computed result. Implementations should be thread-safe and should not
   * maintain internal state between calls (unless this is an aggregation function).
   * </p>
   *
   * @param args    the function arguments (already evaluated)
   * @param context the command execution context (provides database access if needed)
   * @return the function result
   */
  Object execute(Object[] args, CommandContext context);

  /**
   * Returns the minimum number of arguments required.
   * Default implementation returns 0 (variable arguments).
   *
   * @return minimum argument count
   */
  @Override
  default int getMinArgs() {
    return 0;
  }

  /**
   * Returns the maximum number of arguments allowed.
   * Default implementation returns Integer.MAX_VALUE (variable arguments).
   *
   * @return maximum argument count
   */
  @Override
  default int getMaxArgs() {
    return Integer.MAX_VALUE;
  }

  /**
   * Returns a description of the function for documentation.
   * Default implementation returns an empty string.
   *
   * @return function description
   */
  @Override
  default String getDescription() {
    return "";
  }

  /**
   * Returns whether this function aggregates results across multiple rows.
   * <p>
   * Aggregation functions (like collect) accumulate state across multiple
   * calls to {@link #execute} and return the final result via {@link #getAggregatedResult()}.
   * </p>
   *
   * @return true if this function aggregates results, false by default
   */
  default boolean aggregateResults() {
    return false;
  }

  /**
   * Returns the aggregated result after all rows have been processed.
   * <p>
   * Only called for functions where {@link #aggregateResults()} returns true.
   * </p>
   *
   * @return the aggregated result
   * @throws UnsupportedOperationException if this is not an aggregation function
   */
  default Object getAggregatedResult() {
    throw new UnsupportedOperationException("Not an aggregation function");
  }
}
