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
import java.util.List;

/**
 * Returns a sequential 1-based row number ordered by timestamp.
 * Syntax: ts.rowNumber(timestamp)
 */
public class SQLFunctionRowNumber extends SQLAggregatedFunction {
  public static final String NAME = "ts.rowNumber";

  private int count = 0;

  public SQLFunctionRowNumber() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    count++;
    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getResult() {
    final List<Integer> result = new ArrayList<>(count);
    for (int i = 1; i <= count; i++)
      result.add(i);
    return result;
  }

  @Override
  public String getSyntax() {
    return NAME + "(<timestamp>)";
  }
}
