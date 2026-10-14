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
 * Returns the rank of each row by its value (standard SQL {@code RANK() OVER (ORDER BY value)}), listed in timestamp order.
 * Same values receive the same rank wherever they sit in time; the next distinct value skips ranks. Null values rank last.
 * Syntax: ts.rank(value, timestamp)
 */
public class SQLFunctionRank extends SQLAggregatedFunction {
  public static final String NAME = "ts.rank";

  private final List<Object[]> pairs = new ArrayList<>();

  public SQLFunctionRank() {
    super(NAME);
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
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

    // Two orders, two keys (issue #9956). The OUTPUT is listed in timestamp order, like ts.lag/ts.lead/ts.rowNumber, so
    // the n-th element belongs to the n-th row in time. The RANK is defined over the value: equal values share a rank
    // however far apart in time they are, and the next distinct value skips the ranks they took.
    WindowRows.sortBy(NAME, pairs, 1, "<timestamp>");
    final int[] ranks = WindowRows.rankBy(NAME, pairs, 0, "<value>");

    final List<Integer> result = new ArrayList<>(ranks.length);
    for (final int rank : ranks)
      result.add(rank);

    return result;
  }

  @Override
  public String getSyntax() {
    return NAME + "(<value>, <timestamp>)";
  }

  /** Keeps every value it aggregates until the aggregation ends (issue #8591). */
  @Override
  public boolean holdsEveryValue() {
    return true;
  }

  @Override
  public long getHeldValues() {
    return pairs.size();
  }
}
