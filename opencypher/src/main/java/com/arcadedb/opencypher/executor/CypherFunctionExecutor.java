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
package com.arcadedb.opencypher.executor;

import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Interface for executing Cypher functions.
 * Functions can be either scalar (evaluated per row) or aggregation (accumulated across rows).
 */
public interface CypherFunctionExecutor {
  /**
   * Execute the function with the given arguments.
   *
   * @param args    The function arguments (already evaluated)
   * @param context The command execution context
   * @return The function result
   */
  Object execute(Object[] args, CommandContext context);

  /**
   * Returns true if this is an aggregation function.
   * Aggregation functions accumulate results across multiple rows.
   */
  boolean isAggregation();

  /**
   * For aggregation functions, returns the final aggregated result.
   * Called after all rows have been processed.
   */
  default Object getAggregatedResult() {
    throw new UnsupportedOperationException("Not an aggregation function");
  }
}
