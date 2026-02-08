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
 * Execution step for LIMIT clause.
 * Limits the number of results to at most N from the previous step.
 * <p>
 * Example: LIMIT 10
 */
public class LimitStep extends AbstractExecutionStep {
  private final int limitCount;

  /**
   * Creates a limit step.
   *
   * @param limitCount maximum number of results to return
   * @param context    command context
   */
  public LimitStep(final int limitCount, final CommandContext context) {
    super(context);
    this.limitCount = limitCount;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("LimitStep requires a previous step");

    // When LIMIT is 0, still drain the previous step to ensure side effects
    // (CREATE, DELETE, SET, REMOVE) are executed, but return no results
    if (limitCount == 0) {
      final ResultSet prevResults = prev.syncPull(context, nRecords);
      while (prevResults.hasNext())
        prevResults.next();
      return new ResultSet() {
        @Override
        public boolean hasNext() { return false; }
        @Override
        public Result next() { throw new NoSuchElementException(); }
        @Override
        public void close() { LimitStep.this.close(); }
      };
    }

    return new ResultSet() {
      private ResultSet prevResults = null;
      private int returned = 0;
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        if (finished || returned >= limitCount) {
          finished = true;
          return false;
        }

        // Initialize prevResults on first call
        if (prevResults == null) {
          prevResults = prev.syncPull(context, Math.min(nRecords, limitCount));
        }

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
        returned++;
        return prevResults.next();
      }

      @Override
      public void close() {
        LimitStep.this.close();
      }
    };
  }
}
