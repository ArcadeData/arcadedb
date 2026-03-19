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
import java.util.Objects;

/**
 * Returns the rank of each row based on value, ordered by timestamp.
 * Same values receive the same rank; the next distinct value skips ranks.
 * Syntax: ts.rank(value, timestamp)
 */
public class SQLFunctionRank extends SQLAggregatedFunction {
  public static final String NAME = "ts.rank";

  private final List<Object[]> pairs = new ArrayList<>();

  public SQLFunctionRank() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    pairs.add(new Object[] { params[0], params[1] });
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

    final List<Integer> result = new ArrayList<>(pairs.size());
    result.add(1);

    for (int i = 1; i < pairs.size(); i++) {
      if (Objects.equals(pairs.get(i)[0], pairs.get(i - 1)[0]))
        result.add(result.get(i - 1));
      else
        result.add(i + 1);
    }

    return result;
  }

  @Override
  public String getSyntax() {
    return NAME + "(<value>, <timestamp>)";
  }
}
