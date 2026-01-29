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

/**
 * Interface for aggregate functions that accumulate state across multiple records.
 * <p>
 * Aggregate functions like count, sum, avg, min, and max process multiple records
 * and return a single aggregated result. They are stateful - each instance
 * maintains its own accumulator state.
 * </p>
 * <p>
 * Usage pattern:
 * <ol>
 *   <li>Create a new function instance for each aggregation operation</li>
 *   <li>Call {@link #execute} for each record to process</li>
 *   <li>Call {@link #getResult()} to get the final aggregated value</li>
 * </ol>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 * @see RecordFunction
 */
public interface AggregatedFunction extends RecordFunction {
  /**
   * Always returns true since aggregate functions by definition aggregate results.
   *
   * @return true
   */
  @Override
  default boolean aggregateResults() {
    return true;
  }

  /**
   * Returns the aggregated result after all records have been processed.
   * <p>
   * This method must be implemented by aggregate functions to return their
   * accumulated result.
   * </p>
   *
   * @return the aggregated result
   */
  @Override
  Object getResult();

  /**
   * Returns whether this function should merge distinct values before aggregation.
   * <p>
   * Some aggregate functions (like count distinct) need to filter duplicate
   * values before counting. This flag indicates whether the query engine
   * should handle distinct merging.
   * </p>
   *
   * @return true if distinct values should be merged
   */
  default boolean shouldMergeDistinct() {
    return false;
  }
}
