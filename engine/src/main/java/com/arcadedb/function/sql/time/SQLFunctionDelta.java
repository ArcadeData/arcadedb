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

/**
 * Computes the difference between the last and first value ordered by timestamp.
 * Syntax: delta(value, timestamp)
 */
public class SQLFunctionDelta extends SQLAggregatedFunction {
  public static final String NAME = "ts.delta";

  private double firstValue;
  private long   firstTimestamp = Long.MAX_VALUE;
  private double lastValue;
  private long   lastTimestamp  = Long.MIN_VALUE;
  private int    count;

  public SQLFunctionDelta() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params[0] == null || params[1] == null)
      return null;

    final double value = ((Number) params[0]).doubleValue();
    final long ts = SQLFunctionRate.toEpochMillis(params[1]);
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
    if (count == 0)
      return null;
    if (count == 1)
      return 0.0;
    return lastValue - firstValue;
  }

  @Override
  public String getSyntax() {
    return NAME + "(<value>, <timestamp>)";
  }
}
