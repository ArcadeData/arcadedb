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

import com.arcadedb.exception.CommandExecutionException;

import java.util.*;

/**
 * unwinds a result-set.
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public abstract class AbstractUnrollStep extends AbstractExecutionStep {

  ResultSet        lastResult      = null;
  Iterator<Result> nextSubsequence = null;
  Result           nextElement     = null;

  public AbstractUnrollStep(final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
  }

  @Override
  public void reset() {
    this.lastResult = null;
    this.nextSubsequence = null;
    this.nextElement = null;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) {
    if (prev == null)
      throw new CommandExecutionException("Cannot expand without a target");

    return new ResultSet() {
      long localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        if (nextElement == null) {
          fetchNext(context, nRecords);
        }
        return nextElement != null;
      }

      @Override
      public Result next() {
        if (localCount >= nRecords) {
          throw new NoSuchElementException();
        }
        if (nextElement == null) {
          fetchNext(context, nRecords);
        }
        if (nextElement == null) {
          throw new NoSuchElementException();
        }

        final Result result = nextElement;
        localCount++;
        nextElement = null;
        fetchNext(context, nRecords);
        return result;
      }
    };
  }

  private void fetchNext(final CommandContext context, final int n) {
    do {
      if (nextSubsequence != null && nextSubsequence.hasNext()) {
        nextElement = nextSubsequence.next();
        break;
      }

      if (lastResult == null || !lastResult.hasNext()) {
        lastResult = getPrev().get().syncPull(context, n);
      }
      if (!lastResult.hasNext()) {
        return;
      }

      final Result nextAggregateItem = lastResult.next();
      nextSubsequence = unroll(nextAggregateItem, context).iterator();

    } while (true);

  }

  protected abstract Collection<Result> unroll(final Result doc, final CommandContext iContext);

}
