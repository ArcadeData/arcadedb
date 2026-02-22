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
import java.util.Collections;
import java.util.List;

/**
 * Computes the approximate percentile of a numeric series.
 * Uses exact sorting for the collected values and linear interpolation
 * between ranks.
 * <p>
 * Syntax: ts.percentile(value, percentile)
 * where percentile is 0.0..1.0 (e.g. 0.95 for p95, 0.99 for p99)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionTsPercentile extends SQLAggregatedFunction {
  public static final String NAME = "ts.percentile";

  private final List<Double> values = new ArrayList<>();
  private       double       percentile;
  private       boolean      percentileSet;

  public SQLFunctionTsPercentile() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params[0] == null)
      return null;

    if (!percentileSet && params.length > 1 && params[1] != null) {
      percentile = ((Number) params[1]).doubleValue();
      if (percentile < 0.0 || percentile > 1.0)
        throw new IllegalArgumentException("Percentile must be between 0.0 and 1.0, got: " + percentile);
      percentileSet = true;
    }

    values.add(((Number) params[0]).doubleValue());
    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getResult() {
    if (values.isEmpty())
      return null;

    Collections.sort(values);

    final int n = values.size();

    // Use the "exclusive" interpolation method (same as NumPy default)
    final double index = percentile * (n - 1);
    final int lower = (int) Math.floor(index);
    final int upper = (int) Math.ceil(index);

    if (lower == upper || upper >= n)
      return values.get(lower);

    // Linear interpolation between the two closest ranks
    final double fraction = index - lower;
    return values.get(lower) + fraction * (values.get(upper) - values.get(lower));
  }

  @Override
  public String getSyntax() {
    return NAME + "(<value>, <percentile>)";
  }
}
