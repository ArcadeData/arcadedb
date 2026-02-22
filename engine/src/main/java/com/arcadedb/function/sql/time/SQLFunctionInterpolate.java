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
 * Fills null values in a series using the specified method.
 * Syntax: interpolate(value, method [, timestamp])
 * Methods: 'prev' (carry forward), 'zero' (replace with 0), 'none' (leave nulls),
 *          'linear' (linear interpolation between surrounding non-null values, requires timestamp parameter)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionInterpolate extends SQLAggregatedFunction {
  public static final String NAME = "ts.interpolate";

  private final List<Object> values     = new ArrayList<>();
  private final List<Long>   timestamps = new ArrayList<>();
  private       String       method;

  public SQLFunctionInterpolate() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (method == null && params.length > 1 && params[1] != null)
      method = params[1].toString();

    values.add(params[0]);

    // Capture timestamp if provided (needed for linear interpolation)
    if (params.length > 2 && params[2] != null)
      timestamps.add(SQLFunctionRate.toEpochMillis(params[2]));
    else
      timestamps.add((long) timestamps.size()); // Use index as fallback

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

    final String m = method != null ? method : "none";
    final List<Object> result = new ArrayList<>(values.size());

    switch (m) {
    case "zero":
      for (final Object v : values)
        result.add(v != null ? v : 0.0);
      break;

    case "prev":
      Object lastNonNull = null;
      for (final Object v : values) {
        if (v != null)
          lastNonNull = v;
        result.add(lastNonNull);
      }
      break;

    case "linear":
      applyLinearInterpolation(result);
      break;

    default: // "none"
      result.addAll(values);
      break;
    }
    return result;
  }

  private void applyLinearInterpolation(final List<Object> result) {
    final int size = values.size();

    // First pass: copy all values
    for (final Object v : values)
      result.add(v);

    // Second pass: interpolate nulls between non-null values
    int i = 0;
    while (i < size) {
      if (result.get(i) != null) {
        i++;
        continue;
      }

      // Find the previous non-null value
      final int prevIdx = i - 1;
      if (prevIdx < 0 || result.get(prevIdx) == null) {
        // No previous value - leave null
        i++;
        continue;
      }

      // Find the next non-null value
      int nextIdx = i + 1;
      while (nextIdx < size && values.get(nextIdx) == null)
        nextIdx++;

      if (nextIdx >= size) {
        // No next value - leave nulls
        break;
      }

      // Interpolate all nulls between prevIdx and nextIdx
      final double prevVal = ((Number) result.get(prevIdx)).doubleValue();
      final double nextVal = ((Number) values.get(nextIdx)).doubleValue();
      final long prevTs = timestamps.get(prevIdx);
      final long nextTs = timestamps.get(nextIdx);
      final long tsDelta = nextTs - prevTs;

      if (tsDelta == 0) {
        // Same timestamp - just use prev value
        for (int j = i; j < nextIdx; j++)
          result.set(j, prevVal);
      } else {
        for (int j = i; j < nextIdx; j++) {
          final long currentTs = timestamps.get(j);
          final double fraction = (double) (currentTs - prevTs) / tsDelta;
          result.set(j, prevVal + fraction * (nextVal - prevVal));
        }
      }

      i = nextIdx + 1;
    }
  }

  @Override
  public String getSyntax() {
    return NAME + "(<value>, <method> [, <timestamp>])";
  }
}
