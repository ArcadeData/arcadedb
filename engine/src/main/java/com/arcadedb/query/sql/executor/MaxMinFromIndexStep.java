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

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;

import java.util.*;

/**
 * Efficiently retrieves max or min value from an index by using ordered iteration.
 * For MAX values, it iterates in descending order and returns the first entry.
 * For MIN values, it iterates in ascending order and returns the first entry.
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila - at - gmail.com)
 */
public class MaxMinFromIndexStep extends AbstractExecutionStep {
  private final TypeIndex index;
  private final boolean   isMax;
  private final String    propertyName;
  private final String    alias;

  private boolean executed = false;
  private long    cost     = -1;

  /**
   * @param index        the index to use for retrieving the max/min value
   * @param isMax        true for MAX operation, false for MIN operation
   * @param propertyName the property name to retrieve the value from
   * @param alias        the name of the property returned in the result-set
   * @param context      the query context
   */
  public MaxMinFromIndexStep(final TypeIndex index, final boolean isMax, final String propertyName, final String alias,
      final CommandContext context) {
    super(context);
    this.index = index;
    this.isMax = isMax;
    this.propertyName = propertyName;
    this.alias = alias;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    return new ResultSet() {
      private Object resultValue = null;
      private boolean hasValue = false;
      private boolean initialized = false;

      private void initialize() {
        if (initialized)
          return;
        initialized = true;

        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          // For MAX: iterate descending (ascending=false)
          // For MIN: iterate ascending (ascending=true)
          final boolean ascending = !isMax;
          final IndexCursor cursor = index.iterator(ascending);

          // Get the first entry from the ordered iteration
          if (cursor.hasNext()) {
            final Identifiable record = cursor.next();
            if (record != null) {
              // Load the record and extract the property value
              resultValue = record.asDocument().get(propertyName);
              hasValue = true;
            }
          }
        } finally {
          if (context.isProfiling())
            cost += (System.nanoTime() - begin);
        }
      }

      @Override
      public boolean hasNext() {
        initialize();
        // Only return true if we have a value and haven't been consumed yet
        return hasValue && !executed;
      }

      @Override
      public Result next() {
        initialize();
        if (executed || !hasValue)
          throw new NoSuchElementException();

        executed = true;
        final ResultInternal result = new ResultInternal(context.getDatabase());
        result.setProperty(alias, resultValue);
        return result;
      }

      @Override
      public void reset() {
        MaxMinFromIndexStep.this.reset();
      }
    };
  }

  @Override
  public void reset() {
    executed = false;
    // Note: The ResultSet's internal state (initialized, hasValue) is created fresh
    // each time syncPull is called, so we only need to reset executed here
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final String operation = isMax ? "MAX" : "MIN";
    String result = spaces + "+ CALCULATE " + operation + " FROM INDEX: " + index.getName();
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    return result;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new MaxMinFromIndexStep(index, isMax, propertyName, alias, context);
  }
}
