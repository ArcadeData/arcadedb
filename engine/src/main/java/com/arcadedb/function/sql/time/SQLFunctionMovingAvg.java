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
import com.arcadedb.function.sql.FunctionOptions;
import com.arcadedb.function.sql.SQLAggregatedFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Computes a sliding window moving average over accumulated values.
 * Syntax: moving_avg(value, window_size)
 * Returns a list of moving averages with the same length as the input: one element per row, in row order.
 * <p>
 * A row whose value is null keeps its slot (issue #9958): it contributes nothing to the windows it belongs to, each
 * window averages the non-null values it holds, and a window holding no value at all answers null.
 * <p>
 * Each window's sum is computed from that window's own members (van Herk/Gil-Werman block decomposition: the window is
 * the suffix of one block plus the prefix of the next), never by subtracting the value that leaves a running sum, so a
 * large sample cannot leave a rounding residue in the windows after it. The cost stays O(n) whatever the window size.
 */
public class SQLFunctionMovingAvg extends SQLAggregatedFunction {
  public static final String NAME = "ts.movingAvg";

  private static final Set<String> OPTIONS = Set.of("window");

  private double[]  values     = new double[16];
  private boolean[] present    = new boolean[16];
  private int       size       = 0;
  private int       windowSize = -1;

  public SQLFunctionMovingAvg() {
    super(NAME);
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (windowSize < 0)
      windowSize = parseWindow(params[1]);

    // A NON-NUMERIC, NON-NULL VALUE IS A CLIENT-FACING TYPE ERROR, NOT A SILENTLY DROPPED SAMPLE (#6390).
    final Number value = requireNumericOrNull(params[0]);

    // ONE SLOT PER ROW, A NULL INCLUDED: DROPPING IT WOULD SHIFT EVERY LATER AVERAGE INTO AN EARLIER ROW'S SLOT (#9958)
    if (size == values.length) {
      // DOUBLE THE CAPACITY WITHOUT OVERFLOWING int NEAR THE ARRAY SIZE LIMIT
      final int newCapacity = (int) Math.min(Integer.MAX_VALUE - 8L, Math.max(size + 1L, (long) size << 1));
      values = Arrays.copyOf(values, newCapacity);
      present = Arrays.copyOf(present, newCapacity);
    }
    if (value != null) {
      values[size] = value.doubleValue();
      present[size] = true;
    }
    size++;

    return null;
  }

  private int parseWindow(final Object arg) {
    if (arg instanceof Map<?, ?> rawMap) {
      final FunctionOptions opts = new FunctionOptions(NAME, rawMap, OPTIONS);
      return opts.getInt("window", -1);
    }
    return requireIntArgument(arg, "window_size");
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getResult() {
    if (size == 0)
      return new ArrayList<>();

    final int n = size;
    final int w = Math.max(1, windowSize);
    final List<Double> result = new ArrayList<>(n);

    if (w == 1) {
      for (int i = 0; i < n; i++)
        result.add(present[i] ? values[i] : null);
      return result;
    }

    // BLOCKS OF w ROWS: prefix[i] SUMS ROW i's BLOCK FROM ITS START TO i, suffix[i] FROM i TO THE BLOCK's END. THE WINDOW
    // ENDING AT i IS EITHER A WHOLE BLOCK PREFIX (prefix[i]) OR suffix[start] + prefix[i] ACROSS TWO ADJACENT BLOCKS, SO
    // EVERY SUM ONLY EVER ADDS THE WINDOW's OWN MEMBERS (#9958).
    final double[] prefixSum = new double[n];
    final int[] prefixCount = new int[n];

    for (int i = 0; i < n; i++) {
      final boolean blockStart = i % w == 0;
      final double v = present[i] ? values[i] : 0D;
      final int c = present[i] ? 1 : 0;
      prefixSum[i] = blockStart ? v : prefixSum[i - 1] + v;
      prefixCount[i] = blockStart ? c : prefixCount[i - 1] + c;
    }
    if (n <= w) {
      // EVERY WINDOW STARTS AT ROW 0, SO EACH ONE IS A PREFIX OF THE SINGLE BLOCK: NO SUFFIX PASS NEEDED
      for (int i = 0; i < n; i++)
        result.add(prefixCount[i] == 0 ? null : prefixSum[i] / prefixCount[i]);
      return result;
    }

    final double[] suffixSum = new double[n];
    final int[] suffixCount = new int[n];
    for (int i = n - 1; i >= 0; i--) {
      final boolean blockEnd = i == n - 1 || (i + 1) % w == 0;
      final double v = present[i] ? values[i] : 0D;
      final int c = present[i] ? 1 : 0;
      suffixSum[i] = blockEnd ? v : suffixSum[i + 1] + v;
      suffixCount[i] = blockEnd ? c : suffixCount[i + 1] + c;
    }

    for (int i = 0; i < n; i++) {
      final int start = Math.max(0, i - w + 1);
      final double sum;
      final int count;
      if (start % w == 0) {
        // THE WINDOW STARTS ON A BLOCK BOUNDARY, SO IT IS THE PREFIX OF i's OWN BLOCK
        sum = prefixSum[i];
        count = prefixCount[i];
      } else {
        sum = suffixSum[start] + prefixSum[i];
        count = suffixCount[start] + prefixCount[i];
      }
      result.add(count == 0 ? null : sum / count);
    }
    return result;
  }

  @Override
  public String getSyntax() {
    return NAME + "(<value>, <window_size> | { window: <int> })";
  }

  /** Keeps every value it aggregates until the aggregation ends (issue #8591). */
  @Override
  public boolean holdsEveryValue() {
    return true;
  }

  @Override
  public long getHeldValues() {
    return size;
  }
}
