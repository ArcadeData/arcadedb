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
package com.arcadedb.query.sql.function;

import com.arcadedb.function.AggregatedFunction;

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

  protected SQLAggregatedFunction(final String name) {
    super(name);
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
