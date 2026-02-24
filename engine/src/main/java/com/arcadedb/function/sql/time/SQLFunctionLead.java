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
import java.util.Comparator;
import java.util.List;

/**
 * Returns the value from a subsequent row, ordered by timestamp.
 * Syntax: ts.lead(value, offset, timestamp [, default])
 * <ul>
 *   <li>value — the field to retrieve from the leading row</li>
 *   <li>offset — how many rows forward (default 1)</li>
 *   <li>timestamp — the ordering field</li>
 *   <li>default — optional value returned when there is no subsequent row</li>
 * </ul>
 */
public class SQLFunctionLead extends SQLAggregatedFunction {
  public static final String NAME = "ts.lead";

  private final List<Object[]> pairs = new ArrayList<>();
  private int    offset       = 1;
  private Object defaultValue = null;
  private boolean paramsRead  = false;

  public SQLFunctionLead() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (!paramsRead) {
      if (params.length >= 2)
        offset = ((Number) params[1]).intValue();
      if (params.length >= 4)
        defaultValue = params[3];
      paramsRead = true;
    }

    final Object value = params[0];
    final Object timestamp = params.length >= 3 ? params[2] : null;
    pairs.add(new Object[] { value, timestamp });
    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getResult() {
    if (pairs.isEmpty())
      return new ArrayList<>();

    pairs.sort(Comparator.comparing(p -> ((Comparable) p[1])));

    final int size = pairs.size();
    final List<Object> result = new ArrayList<>(size);
    for (int i = 0; i < size; i++)
      result.add(i + offset < size ? pairs.get(i + offset)[0] : defaultValue);

    return result;
  }

  @Override
  public String getSyntax() {
    return NAME + "(<value>, <offset>, <timestamp> [, <default>])";
  }
}
