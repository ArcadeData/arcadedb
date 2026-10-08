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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.function.sql.SQLAggregatedFunction;
import com.arcadedb.schema.Type;

/**
 * Computes the sum of field. Uses the context to save the last sum number. When different Number class are used, take the class
 * with most precision.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionSum extends SQLFunctionRunningSumAbstract {
  public static final String NAME = "sum";

  public SQLFunctionSum() {
    super(NAME);
  }

  /**
   * Variadic in SQL: {@code sum(a, b, c)} adds the arguments of one row, which is a different computation from the
   * cross-row {@code sum(a)}. Cypher exposes only the single-argument aggregation, so its parser declaration is
   * deliberately narrower - see {@code CypherFunctionArityRegistryTest.NARROWER_IN_CYPHER}.
   */
  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return Integer.MAX_VALUE;
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params.length == 1) {
      aggregate(self, params[0], context);
      return getSum();
    }

    // MULTI-ARG IS A PER-ROW COMPUTATION: SUM THE ARGUMENTS INTO A LOCAL VARIABLE WITHOUT TOUCHING THE
    // CROSS-ROW ACCUMULATOR, OTHERWISE A SUBSEQUENT getResult() WOULD ONLY RETURN THIS ROW'S CONTRIBUTION.
    Number rowSum = null;
    for (int i = 0; i < params.length; ++i) {
      final Number value = requireNumericOrNull(params[i]);
      if (value != null)
        rowSum = rowSum == null ? value : Type.increment(rowSum, value);
    }
    return rowSum;
  }

  @Override
  public void aggregate(final Object self, final Object value, final CommandContext context) {
    accumulate(value);
  }

  @Override
  protected void accept(final Number value) {
    addToSum(value);
  }

  public String getSyntax() {
    return "sum(<field> [,<field>*])";
  }

  @Override
  public boolean canMergePartials() {
    return aggregateResults();
  }

  @Override
  public void mergePartial(final SQLAggregatedFunction other) {
    addToSum((SQLFunctionSum) other);
  }

  @Override
  public Object getResult() {
    // SQL: SUM over an empty group or an all-NULL group is NULL, not 0 (issue #9351). Same answer as avg/min/max and
    // as the time-series push-down.
    return getSum();
  }
}
