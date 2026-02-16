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

/**
 * Abstract base class for SQL aggregate functions that collect values into a collection.
 * <p>
 * This class provides a typed {@link #context} field for accumulating results during
 * aggregation. Subclasses typically use List, Set, or Map as the context type.
 * </p>
 * <p>
 * Examples: {@code list()}, {@code set()}, {@code unionAll()}, {@code difference()},
 * {@code intersect()}, {@code symmetricDifference()}.
 * </p>
 *
 * @param <T> the type of the aggregation context (e.g., List, Set, Map)
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class SQLAggregatedCollectionFunction<T> extends SQLAggregatedFunction {

  /**
   * The aggregation context that accumulates values across records.
   * Subclasses should initialize this in {@link #execute} when processing begins.
   */
  protected T context;

  protected SQLAggregatedCollectionFunction(final String name) {
    super(name);
  }

  /**
   * Returns the accumulated collection context as the aggregation result.
   *
   * @return the context containing aggregated values
   */
  @Override
  public T getResult() {
    return context;
  }
}
