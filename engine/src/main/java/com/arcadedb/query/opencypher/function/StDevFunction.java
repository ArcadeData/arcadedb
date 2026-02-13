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
package com.arcadedb.query.opencypher.function;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * stDev() aggregation function - computes the sample standard deviation.
 * Uses Bessel's correction (divides by n-1).
 * Returns 0.0 for a single value, null for no values.
 * Example: MATCH (n:Val) RETURN stDev(n.v)
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class StDevFunction implements StatelessFunction {
  private long   n;
  private double mean;
  private double m2;

  @Override
  public String getName() {
    return "stDev";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("stDev() requires exactly one argument");
    if (args[0] instanceof Number number) {
      ++n;
      final double value = number.doubleValue();
      final double nextMean = mean + (value - mean) / n;
      m2 += (value - mean) * (value - nextMean);
      mean = nextMean;
    }
    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getAggregatedResult() {
    if (n == 0)
      return null;
    if (n == 1)
      return 0.0;
    return Math.sqrt(m2 / (n - 1));
  }
}
