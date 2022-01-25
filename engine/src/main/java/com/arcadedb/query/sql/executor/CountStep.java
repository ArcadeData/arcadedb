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

  private long cost = 0;

  boolean executed = false;

  /**
   * @param ctx              the query context
   * @param profilingEnabled true to enable the profiling of the execution (for SQL PROFILE)
   */
  public CountStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    if (executed) {
      return new InternalResultSet();
    }
    ResultInternal resultRecord = new ResultInternal();
    executed = true;
    long count = 0;
    while (true) {
      ResultSet prevResult = getPrev().get().syncPull(ctx, nRecords);

      if (!prevResult.hasNext()) {
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          InternalResultSet result = new InternalResultSet();
          resultRecord.setProperty("count", count);
          result.add(resultRecord);
          return result;
        } finally {
          if (profilingEnabled) {
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
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COUNT");
    if (profilingEnabled) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public ExecutionStep copy(CommandContext ctx) {
    return new CountStep(ctx, profilingEnabled);
  }
}
