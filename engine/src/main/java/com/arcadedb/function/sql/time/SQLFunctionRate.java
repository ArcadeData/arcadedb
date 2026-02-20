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

import java.util.Date;

/**
 * Computes the per-second rate of change: (last_value - first_value) / (last_ts - first_ts) * 1000.
 * Syntax: rate(value, timestamp)
 */
public class SQLFunctionRate extends SQLAggregatedFunction {
  public static final String NAME = "ts.rate";

  private double firstValue;
  private long   firstTimestamp = Long.MAX_VALUE;
  private double lastValue;
  private long   lastTimestamp  = Long.MIN_VALUE;
  private int    count;

  public SQLFunctionRate() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params[0] == null || params[1] == null)
      return null;

    final double value = ((Number) params[0]).doubleValue();
    final long ts = toEpochMillis(params[1]);
    count++;

    if (ts < firstTimestamp) {
      firstTimestamp = ts;
      firstValue = value;
    }
    if (ts > lastTimestamp) {
      lastTimestamp = ts;
      lastValue = value;
    }
    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getResult() {
    if (count < 2 || lastTimestamp == firstTimestamp)
      return null;
    return (lastValue - firstValue) / ((lastTimestamp - firstTimestamp) / 1000.0);
  }

  @Override
  public String getSyntax() {
    return NAME + "(<value>, <timestamp>)";
  }

  static long toEpochMillis(final Object ts) {
    if (ts instanceof Date date)
      return date.getTime();
    return ((Number) ts).longValue();
  }
}
