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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Optimized execution step for counting all records of a specific type.
 * Uses O(1) database.countType() instead of iterating through all records.
 *
 * This optimization applies to simple Cypher queries like:
 * MATCH (a:Account) RETURN COUNT(a) as count
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TypeCountStep extends AbstractExecutionStep {
  private final String typeName;
  private final String outputAlias;
  private boolean executed = false;

  public TypeCountStep(final String typeName, final String outputAlias, final CommandContext context) {
    super(context);
    this.typeName = typeName;
    this.outputAlias = outputAlias;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (executed)
      return new InternalResultSet();

    executed = true;

    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    final long count;
    try {
      if (context.isProfiling())
        rowCount++;

      // Use O(1) count operation instead of iterating through all records
      count = context.getDatabase().countType(typeName, true);
    } finally {
      if (context.isProfiling())
        cost += (System.nanoTime() - begin);
    }

    // Create result with the count
    final ResultInternal result = new ResultInternal();
    result.setProperty(outputAlias, count);

    final List<Result> results = new ArrayList<>(1);
    results.add(result);

    return new ResultSet() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < results.size();
      }

      @Override
      public Result next() {
        return results.get(index++);
      }

      @Override
      public void close() {
        TypeCountStep.this.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    final StringBuilder builder = new StringBuilder();
    builder.append(ind);
    builder.append("+ TYPE COUNT OPTIMIZATION (").append(typeName).append(")");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }
}
