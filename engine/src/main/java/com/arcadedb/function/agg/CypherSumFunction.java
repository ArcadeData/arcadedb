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
package com.arcadedb.function.agg;

import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.math.NumericSum;
import com.arcadedb.function.sql.math.SQLFunctionSum;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * The openCypher {@code sum()}: the SQL {@code sum()} (numbers, durations, the elements of a list, the type errors, the exact
 * floating-point total), except for an integral total out of the {@code long} range. SQL widens it to a DECIMAL, a type Cypher
 * does not have, and the type of the answer then depended on the order of the rows once a FLOAT joined (issue #12993). Here the
 * total stays exact ({@link NumericSum} with {@code integralOverflowFails}): back in range it is the INTEGER, past it the
 * {@code long overflow} ArithmeticError the integer operators of the language raise, and with a FLOAT among the values it is the
 * FLOAT of the exact sum, whatever the order of the rows and however a parallel scan splits them.
 * <p>
 * Reached through {@code SQLFunctionBridge}, which keeps answering 0 for a sum over no value, merges the partials of a parallel
 * aggregation and wraps it for DISTINCT.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class CypherSumFunction extends SQLFunctionSum {
  public CypherSumFunction() {
    super(true);
  }

  /**
   * The cross-row form answers nothing per row: the aggregation reads {@link #getResult()} once at the end, and the running total
   * may be out of the {@code long} range on the way to an answer that is not.
   */
  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params.length != 1)
      return super.execute(self, currentRecord, currentResult, params, context);
    accumulate(params[0]);
    return null;
  }
}
