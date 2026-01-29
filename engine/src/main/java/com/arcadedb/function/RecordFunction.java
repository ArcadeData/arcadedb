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

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Interface for functions that operate on records and need current record context.
 * <p>
 * Record functions receive the current record being processed along with their
 * arguments. They can be either stateless (single execution) or aggregate
 * (accumulating results across multiple records).
 * </p>
 * <p>
 * Examples of record functions include:
 * <ul>
 *   <li>Field access functions that need the current record</li>
 *   <li>Aggregate functions (count, sum, avg) that accumulate across records</li>
 *   <li>Graph traversal functions that start from the current vertex</li>
 * </ul>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 * @see Function
 * @see StatelessFunction
 * @see AggregatedFunction
 */
public interface RecordFunction extends Function {
  /**
   * Executes the function with the given record context and arguments.
   *
   * @param self          the 'this' object for method-style calls (can be null)
   * @param currentRecord the current record being processed (can be null)
   * @param currentResult the current result being built (can be null)
   * @param params        the function parameters (already evaluated)
   * @param context       the command execution context
   * @return the function result, or null for aggregate functions that return results via {@link #getResult()}
   */
  Object execute(Object self, Identifiable currentRecord, Object currentResult, Object[] params, CommandContext context);

  /**
   * Configures the function with parameters.
   * <p>
   * This is called when the function is parsed, before execution. Some functions
   * need to store their configured parameters for use during execution.
   * </p>
   *
   * @param configuredParameters the parameters to configure
   * @return this function (for chaining)
   */
  RecordFunction config(Object[] configuredParameters);

  /**
   * Returns whether this function aggregates results across multiple records.
   * <p>
   * Aggregate functions (like count, sum, avg) accumulate state across multiple
   * calls to {@link #execute} and return the final result via {@link #getResult()}.
   * </p>
   *
   * @return true if this function aggregates results
   */
  default boolean aggregateResults() {
    return false;
  }

  /**
   * Returns the aggregated result after all records have been processed.
   * <p>
   * Only called for functions where {@link #aggregateResults()} returns true.
   * </p>
   *
   * @return the aggregated result, or null for non-aggregate functions
   */
  default Object getResult() {
    return null;
  }
}
