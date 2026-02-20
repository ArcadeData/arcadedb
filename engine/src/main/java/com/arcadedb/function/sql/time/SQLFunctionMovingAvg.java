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
package com.arcadedb.function.sql.time;

import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.SQLAggregatedFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Computes a sliding window moving average over accumulated values.
 * Syntax: moving_avg(value, window_size)
 * Returns a list of moving averages with the same length as the input.
 */
public class SQLFunctionMovingAvg extends SQLAggregatedFunction {
  public static final String NAME = "ts.movingAvg";

  private final List<Double> values = new ArrayList<>();
  private int windowSize = -1;

  public SQLFunctionMovingAvg() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (windowSize < 0)
      windowSize = ((Number) params[1]).intValue();

    if (params[0] instanceof Number number)
      values.add(number.doubleValue());

    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getResult() {
    if (values.isEmpty())
      return new ArrayList<>();

    final int w = Math.max(1, windowSize);
    final List<Double> result = new ArrayList<>(values.size());
    double windowSum = 0;

    for (int i = 0; i < values.size(); i++) {
      windowSum += values.get(i);
      if (i >= w)
        windowSum -= values.get(i - w);
      final int count = Math.min(i + 1, w);
      result.add(windowSum / count);
    }
    return result;
  }

  @Override
  public String getSyntax() {
    return NAME + "(<value>, <window_size>)";
  }
}
