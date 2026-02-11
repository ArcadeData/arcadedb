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
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.NoSuchElementException;

/**
 * Execution step for SKIP clause.
 * Skips the first N results from the previous step.
 * <p>
 * Example: SKIP 10
 */
public class SkipStep extends AbstractExecutionStep {
  private final int skipCount;

  /**
   * Creates a skip step.
   *
   * @param skipCount number of results to skip
   * @param context   command context
   */
  public SkipStep(final int skipCount, final CommandContext context) {
    super(context);
    this.skipCount = skipCount;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("SkipStep requires a previous step");

    return new ResultSet() {
      private ResultSet prevResults = null;
      private int skipped = 0;
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        if (finished) {
          return false;
        }

        // Initialize prevResults on first call
        if (prevResults == null) {
          prevResults = prev.syncPull(context, nRecords);
        }

        // Skip records if we haven't skipped enough yet
        while (skipped < skipCount && prevResults.hasNext()) {
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            prevResults.next();
            skipped++;
          } finally {
            if (context.isProfiling())
              cost += (System.nanoTime() - begin);
          }
        }

        // Check if there are more results after skipping
        if (!prevResults.hasNext()) {
          finished = true;
          return false;
        }

        return true;
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return prevResults.next();
      }

      @Override
      public void close() {
        SkipStep.this.close();
      }
    };
  }
}
