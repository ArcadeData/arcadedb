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
 * When a timestamp is given the rows are ordered by it, and the result is listed in timestamp order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionInterpolate extends SQLAggregatedFunction {
  public static final String NAME = "ts.interpolate";

  // [value, timestamp in epoch millis or null]
  private final List<Object[]> rows = new ArrayList<>();
  private       boolean        anyTimestamp;
  private       String         method;

  public SQLFunctionInterpolate() {
    super(NAME);
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (method == null && params.length > 1 && params[1] != null)
      method = params[1].toString();

    final Long timestamp = params.length > 2 && params[2] != null ? SQLFunctionRate.toEpochMillis(params[2]) : null;
    if (timestamp != null)
      anyTimestamp = true;
    rows.add(new Object[] { params[0], timestamp });
    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getResult() {
    if (rows.isEmpty())
      return new ArrayList<>();

    // Filling a gap is defined over TIME: the previous and next samples are the neighbours in timestamp order, not in
    // arrival order, so with a timestamp the rows are ordered by it first and the result is listed in that order, like
    // ts.lag/ts.lead/ts.rank (issue #9956). Rows without a timestamp sort last and keep their arrival order; with no
    // timestamp at all, arrival order is the only order there is.
    if (anyTimestamp)
      WindowRows.sortBy(NAME, rows, 1, "<timestamp>");

    final String m = method != null ? method : "none";
    final List<Object> result = new ArrayList<>(rows.size());

    switch (m) {
    case "zero":
      for (final Object[] row : rows)
        result.add(row[0] != null ? row[0] : 0.0);
      break;

    case "prev":
      Object lastNonNull = null;
      for (final Object[] row : rows) {
        if (row[0] != null)
          lastNonNull = row[0];
        result.add(lastNonNull);
      }
      break;

    case "linear":
      applyLinearInterpolation(result);
      break;

    default: // "none"
      for (final Object[] row : rows)
        result.add(row[0]);
      break;
    }
    return result;
  }

  private void applyLinearInterpolation(final List<Object> result) {
    final int size = rows.size();

    // First pass: copy all values
    for (final Object[] row : rows)
      result.add(row[0]);

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
      while (nextIdx < size && rows.get(nextIdx)[0] == null)
        nextIdx++;

      if (nextIdx >= size) {
        // No next value - leave nulls
        break;
      }

      // Interpolate all nulls between prevIdx and nextIdx
      final double prevVal = requireNumericOrNull(result.get(prevIdx)).doubleValue();
      final double nextVal = requireNumericOrNull(rows.get(nextIdx)[0]).doubleValue();
      final Long prevTs = (Long) rows.get(prevIdx)[1];
      final Long nextTs = (Long) rows.get(nextIdx)[1];

      if (prevTs != null && nextTs != null && prevTs.equals(nextTs)) {
        // Same timestamp - just use prev value
        for (int j = i; j < nextIdx; j++)
          result.set(j, prevVal);
      } else {
        for (int j = i; j < nextIdx; j++) {
          final Long currentTs = (Long) rows.get(j)[1];
          // Without a timestamp on every row of the gap, the position stands in for time (the pre-#9956 index fallback)
          final double fraction = prevTs != null && nextTs != null && currentTs != null ?
              (double) (currentTs - prevTs) / (nextTs - prevTs) :
              (double) (j - prevIdx) / (nextIdx - prevIdx);
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

  /** Keeps every value it aggregates until the aggregation ends (issue #8591). */
  @Override
  public boolean holdsEveryValue() {
    return true;
  }

  @Override
  public long getHeldValues() {
    return rows.size();
  }
}
