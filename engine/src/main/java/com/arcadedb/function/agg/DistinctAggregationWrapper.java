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

import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Wrapper that applies DISTINCT semantics to an aggregation function.
 * Tracks seen argument values and only delegates to the wrapped function for unique values.
 */
public class DistinctAggregationWrapper implements StatelessFunction {
  private final StatelessFunction delegate;
  private final Set<List<Object>> seenValues = new HashSet<>();

  public DistinctAggregationWrapper(final StatelessFunction delegate) {
    this.delegate = delegate;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (!seenValues.add(Arrays.asList(args)))
      return null;
    return delegate.execute(args, context);
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getAggregatedResult() {
    return delegate.getAggregatedResult();
  }
}
