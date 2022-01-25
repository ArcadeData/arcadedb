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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * unwinds a result-set.
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public abstract class AbstractUnrollStep extends AbstractExecutionStep {


  ResultSet        lastResult      = null;
  Iterator<Result> nextSubsequence = null;
  Result           nextElement     = null;

  public AbstractUnrollStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override public void reset() {
    this.lastResult = null;
    this.nextSubsequence = null;
    this.nextElement = null;
  }

  @Override public ResultSet syncPull(CommandContext ctx, int nRecords) {
    if (prev == null || prev.isEmpty()) {
      throw new CommandExecutionException("Cannot expand without a target");
    }
    return new ResultSet() {
      long localCount = 0;

      @Override public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        if (nextElement == null) {
          fetchNext(ctx, nRecords);
        }
        return nextElement != null;
      }

      @Override public Result next() {
        if (localCount >= nRecords) {
          throw new IllegalStateException();
        }
        if (nextElement == null) {
          fetchNext(ctx, nRecords);
        }
        if (nextElement == null) {
          throw new IllegalStateException();
        }

        Result result = nextElement;
        localCount++;
        nextElement = null;
        fetchNext(ctx, nRecords);
        return result;
      }

      @Override public void close() {

      }

      @Override public Optional<ExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private void fetchNext(CommandContext ctx, int n) {
    do {
      if (nextSubsequence != null && nextSubsequence.hasNext()) {
        nextElement = nextSubsequence.next();
        break;
      }

      if (nextSubsequence == null || !nextSubsequence.hasNext()) {
        if (lastResult == null || !lastResult.hasNext()) {
          lastResult = getPrev().get().syncPull(ctx, n);
        }
        if (!lastResult.hasNext()) {
          return;
        }
      }

      Result nextAggregateItem = lastResult.next();
      nextSubsequence = unroll(nextAggregateItem, ctx).iterator();

    } while (true);

  }

  protected abstract Collection<Result> unroll(final Result doc, final CommandContext iContext);

}
