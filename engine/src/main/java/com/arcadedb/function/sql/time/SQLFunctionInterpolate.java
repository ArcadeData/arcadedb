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
 * Fills null values in a series using the specified method.
 * Syntax: interpolate(value, method)
 * Methods: 'prev' (carry forward), 'zero' (replace with 0), 'none' (leave nulls)
 */
public class SQLFunctionInterpolate extends SQLAggregatedFunction {
  public static final String NAME = "ts.interpolate";

  private final List<Object> values = new ArrayList<>();
  private String method;

  public SQLFunctionInterpolate() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (method == null && params.length > 1 && params[1] != null)
      method = params[1].toString();

    values.add(params[0]);
    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getResult() {
    if (values.isEmpty())
      return new ArrayList<>();

    final String m = method != null ? method : "none";
    final List<Object> result = new ArrayList<>(values.size());

    switch (m) {
    case "zero":
      for (final Object v : values)
        result.add(v != null ? v : 0.0);
      break;

    case "prev":
      Object lastNonNull = null;
      for (final Object v : values) {
        if (v != null)
          lastNonNull = v;
        result.add(lastNonNull);
      }
      break;

    default: // "none"
      result.addAll(values);
      break;
    }
    return result;
  }

  @Override
  public String getSyntax() {
    return NAME + "(<value>, <method>)";
  }
}
