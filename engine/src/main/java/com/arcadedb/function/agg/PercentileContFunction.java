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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.List;

/**
 * percentileCont() aggregation function - computes the continuous (interpolated) percentile.
 * Uses linear interpolation between values.
 * Example: percentileCont(n.age, 0.4) returns the 40th percentile age
 */
public class PercentileContFunction implements StatelessFunction {
  private final List<Number> values = new ArrayList<>();
  private double percentile = -1;

  @Override
  public String getName() {
    return "percentileCont";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 2)
      throw new CommandExecutionException("percentileCont() requires exactly 2 arguments: percentileCont(expr, percentile)");
    if (percentile < 0) {
      if (args[1] == null)
        throw new CommandExecutionException("percentileCont() percentile argument must not be null");
      percentile = ((Number) args[1]).doubleValue();
      if (percentile < 0.0 || percentile > 1.0)
        throw new CommandExecutionException("NumberOutOfRange: percentile must be between 0.0 and 1.0, got: " + percentile);
    }
    if (args[0] instanceof Number)
      values.add((Number) args[0]);
    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getAggregatedResult() {
    if (values.isEmpty())
      return null;
    values.sort((a, b) -> Double.compare(a.doubleValue(), b.doubleValue()));
    if (percentile == 1.0)
      return values.getLast().doubleValue();
    if (percentile == 0.0)
      return values.getFirst().doubleValue();
    final double pos = percentile * (values.size() - 1);
    final int lower = (int) Math.floor(pos);
    final int upper = (int) Math.ceil(pos);
    if (lower == upper)
      return values.get(lower).doubleValue();
    final double fraction = pos - lower;
    return values.get(lower).doubleValue() + fraction * (values.get(upper).doubleValue() - values.get(lower).doubleValue());
  }
}
