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
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;

import java.util.NoSuchElementException;

/**
 * Efficiently retrieves MAX or MIN value from a RangeIndex.
 * For MAX: iterates in descending order and returns the first non-null key.
 * For MIN: iterates in ascending order and returns the first non-null key.
 * <p>
 * This is an optimization for queries like "SELECT MAX(indexed_field) FROM type"
 * that avoids a full table scan when an index is available.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/3304">Issue #3304</a>
 */
public class MaxMinFromIndexStep extends AbstractExecutionStep {

  private final RangeIndex index;
  private final String alias;
  private final boolean max; // true for MAX, false for MIN

  private boolean executed = false;

  /**
   * @param index   The range index to read from
   * @param alias   The alias for the result property
   * @param max     true for MAX, false for MIN
   * @param context The command context
   */
  public MaxMinFromIndexStep(final RangeIndex index, final String alias, final boolean max, final CommandContext context) {
    super(context);
    this.index = index;
    this.alias = alias;
    this.max = max;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return !executed;
      }

      @Override
      public Result next() {
        if (executed)
          throw new NoSuchElementException();

        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          Object resultValue = null;

          // For MAX: iterate descending (false), for MIN: iterate ascending (true)
          // The first non-null key is the result
          final IndexCursor cursor = index.iterator(!max);

          while (cursor.hasNext()) {
            cursor.next(); // advance to get the key
            final Object key = cursor.getKeys();

            if (key != null) {
              // Handle single-key vs multi-key indexes
              if (key instanceof Object[]) {
                final Object[] keys = (Object[]) key;
                if (keys.length > 0 && keys[0] != null) {
                  resultValue = keys[0];
                  break;
                }
              } else {
                resultValue = key;
                break;
              }
            }
          }

          executed = true;
          final ResultInternal result = new ResultInternal(context.getDatabase());
          result.setProperty(alias, resultValue);
          return result;

        } finally {
          if (context.isProfiling())
            cost += (System.nanoTime() - begin);
        }
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
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ " + (max ? "MAX" : "MIN") + " FROM INDEX " + index.getName();
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    return result;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }
}
