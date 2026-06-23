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

/**
 * Count the record that contains a field. Use * to indicate the record instead of the field. Uses the context to save the counter
 * number. When different Number class are used, take the class with most precision.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionCount extends SQLAggregatedFunction {
  public static final String NAME = "count";

  private long total = 0;

  public SQLFunctionCount() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    final boolean counted = params.length == 0 || params[0] != null;
    if (counted)
      total++;

    // Return the per-row contribution (1 for a counted row, 0 otherwise). The cross-row running total
    // is exposed only through getResult(); the aggregation pipeline ignores this return value, while a
    // non-aggregating caller (e.g. count(x) in a LET) gets a consistent per-row value instead of the
    // cumulative count.
    return counted ? 1L : 0L;
  }

  public boolean aggregateResults() {
    return true;
  }

  public String getSyntax() {
    return "count(<field>|*)";
  }

  @Override
  public Object getResult() {
    return total;
  }
}
