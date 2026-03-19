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
import java.util.Comparator;
import java.util.Date;
import java.util.List;

/**
 * Computes the per-second rate of change.
 * <p>
 * Syntax: ts.rate(value, timestamp [, counterResetDetection])
 * <ul>
 *   <li>Default (no 3rd param or false): simple rate = (last - first) / time_delta</li>
 *   <li>counterResetDetection = true: detects counter resets (where value decreases)
 *       and treats the post-reset value as an increment from 0, matching Prometheus rate() semantics</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionRate extends SQLAggregatedFunction {
  public static final String NAME = "ts.rate";

  private final List<long[]> samples = new ArrayList<>(); // [timestamp, Double.doubleToRawLongBits(value)]
  private       boolean      counterResetDetection;
  private       boolean      counterResetDetectionSet;

  public SQLFunctionRate() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params[0] == null || params[1] == null)
      return null;

    if (!counterResetDetectionSet && params.length > 2 && params[2] != null) {
      counterResetDetection = Boolean.TRUE.equals(params[2]) || "true".equalsIgnoreCase(params[2].toString());
      counterResetDetectionSet = true;
    }

    final double value = ((Number) params[0]).doubleValue();
    final long ts = toEpochMillis(params[1]);
    samples.add(new long[] { ts, Double.doubleToRawLongBits(value) });
    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getResult() {
    if (samples.size() < 2)
      return null;

    // Sort by timestamp
    samples.sort(Comparator.comparingLong(a -> a[0]));

    final long firstTs = samples.getFirst()[0];
    final long lastTs = samples.getLast()[0];
    if (lastTs == firstTs)
      return null;

    if (counterResetDetection) {
      // Compute total increase, accounting for counter resets
      double totalIncrease = 0.0;
      double prevValue = Double.longBitsToDouble(samples.getFirst()[1]);

      for (int i = 1; i < samples.size(); i++) {
        final double currentValue = Double.longBitsToDouble(samples.get(i)[1]);
        if (currentValue < prevValue)
          // Counter reset detected: treat the current value as the increase from 0
          totalIncrease += currentValue;
        else
          totalIncrease += (currentValue - prevValue);
        prevValue = currentValue;
      }

      return totalIncrease / ((lastTs - firstTs) / 1000.0);
    } else {
      // Simple rate: (last - first) / time_delta
      final double firstValue = Double.longBitsToDouble(samples.getFirst()[1]);
      final double lastValue = Double.longBitsToDouble(samples.getLast()[1]);
      return (lastValue - firstValue) / ((lastTs - firstTs) / 1000.0);
    }
  }

  @Override
  public String getSyntax() {
    return NAME + "(<value>, <timestamp> [, <counterResetDetection>])";
  }

  static long toEpochMillis(final Object ts) {
    if (ts instanceof Date date)
      return date.getTime();
    return ((Number) ts).longValue();
  }
}
