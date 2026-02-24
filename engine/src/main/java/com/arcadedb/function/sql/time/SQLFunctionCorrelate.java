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
 * Computes the Pearson correlation coefficient between two series.
 * Syntax: correlate(value_a, value_b)
 * Returns a value between -1.0 and 1.0, or null if fewer than 2 samples or zero variance.
 * Uses Welford's online algorithm for numerical stability.
 */
public class SQLFunctionCorrelate extends SQLAggregatedFunction {
  public static final String NAME = "ts.correlate";

  private long   n;
  private double meanA;
  private double meanB;
  private double m2A;
  private double m2B;
  private double covAB;

  public SQLFunctionCorrelate() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params[0] == null || params[1] == null)
      return null;

    final double a = ((Number) params[0]).doubleValue();
    final double b = ((Number) params[1]).doubleValue();

    n++;
    final double dA = a - meanA;
    final double dB = b - meanB;
    meanA += dA / n;
    meanB += dB / n;
    final double dA2 = a - meanA;
    final double dB2 = b - meanB;
    m2A += dA * dA2;
    m2B += dB * dB2;
    covAB += dA * dB2;

    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getResult() {
    if (n < 2)
      return null;
    final double denom = Math.sqrt(m2A * m2B);
    if (denom == 0.0)
      return null;
    return covAB / denom;
  }

  @Override
  public String getSyntax() {
    return NAME + "(<value_a>, <value_b>)";
  }
}
