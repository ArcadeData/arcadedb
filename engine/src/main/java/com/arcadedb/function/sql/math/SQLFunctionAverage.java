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
package com.arcadedb.function.sql.math;

import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.SQLAggregatedFunction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.schema.Type;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Compute the average value for a field. Uses the context to save the last average number. When different Number class are used,
 * take the class with most precision.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionAverage extends SQLAggregatedFunction {
  public static final String NAME = "avg";

  /**
   * Scale used when averaging {@link BigDecimal} values, so the result keeps fractional digits instead of inheriting the
   * dividend scale (which would truncate to an integer).
   */
  private static final int BIG_DECIMAL_SCALE = 10;

  private Number sum;
  private int    total = 0;

  public SQLFunctionAverage() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params.length == 1) {
      if (params[0] instanceof Number number)
        sum(number);
      else if (MultiValue.isMultiValue(params[0]))
        for (final Object n : MultiValue.getMultiValueIterable(params[0]))
          sum((Number) n);

      return getResult();
    }

    // MULTI-ARG IS A PER-ROW COMPUTATION: AVERAGE THE ARGUMENTS USING LOCAL VARIABLES WITHOUT TOUCHING THE
    // CROSS-ROW ACCUMULATOR, OTHERWISE A SUBSEQUENT getResult() WOULD ONLY REFLECT THIS ROW'S CONTRIBUTION.
    Number rowSum = null;
    int rowTotal = 0;
    for (int i = 0; i < params.length; ++i) {
      final Number value = (Number) params[i];
      if (value != null) {
        rowTotal++;
        rowSum = rowSum == null ? value : Type.increment(rowSum, value);
      }
    }
    return computeAverage(rowSum, rowTotal);
  }

  protected void sum(final Number value) {
    if (value != null) {
      total++;
      if (sum == null)
        // FIRST TIME
        sum = value;
      else
        sum = Type.increment(sum, value);
    }
  }

  public String getSyntax() {
    return "avg(<field> [,<field>*])";
  }

  @Override
  public Object getResult() {
    return computeAverage(sum, total);
  }

  @Override
  public boolean aggregateResults() {
    return configuredParameters.length == 1;
  }

  private Object computeAverage(final Number iSum, final int iTotal) {
    // EMPTY GROUP: NO VALUES TO AVERAGE (ALSO AVOIDS A DIVISION BY ZERO)
    if (iSum == null || iTotal == 0)
      return null;

    // BIGDECIMAL KEEPS FULL PRECISION: USE AN EXPLICIT SCALE, OTHERWISE divide() INHERITS THE DIVIDEND SCALE AND TRUNCATES
    // (e.g. new BigDecimal("10").divide(BigDecimal(3)) -> 3 instead of 3.3333333333).
    if (iSum instanceof BigDecimal decimal)
      return decimal.divide(new BigDecimal(iTotal), BIG_DECIMAL_SCALE, RoundingMode.HALF_UP);

    // ALL OTHER NUMERIC TYPES AVERAGE AS A DOUBLE SO INTEGER/LONG SUMS ARE NOT TRUNCATED (e.g. avg(1, 2) -> 1.5).
    return iSum.doubleValue() / iTotal;
  }
}
