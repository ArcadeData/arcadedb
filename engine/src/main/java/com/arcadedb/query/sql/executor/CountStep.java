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
package com.arcadedb.query.sql.executor;

import com.arcadedb.exception.TimeoutException;

/**
 * Counts the records from the previous steps.
 * Returns a record with a single property, called "count" containing the count of records received from pervious steps
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class CountStep extends AbstractExecutionStep {

  boolean executed = false;

  /**
   * @param context          the query context
   * @param profilingEnabled true to enable the profiling of the execution (for SQL PROFILE)
   */
  public CountStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (executed)
      return new InternalResultSet();

    final ResultInternal resultRecord = new ResultInternal(context.getDatabase());
    executed = true;
    long count = 0;
    while (true) {
      final ResultSet prevResult = getPrev().syncPull(context, nRecords);

      if (!prevResult.hasNext()) {
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          final InternalResultSet result = new InternalResultSet();
          resultRecord.setProperty("count", count);
          result.add(resultRecord);
          return result;
        } finally {
          if (context.isProfiling()) {
            cost += (System.nanoTime() - begin);
          }
        }
      }
      while (prevResult.hasNext()) {
        count++;
        prevResult.next();
      }
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COUNT");
    if (context.isProfiling()) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    return result.toString();
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new CountStep(context);
  }
}
