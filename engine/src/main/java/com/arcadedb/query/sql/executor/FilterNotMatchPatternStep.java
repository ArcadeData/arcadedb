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

import java.util.*;

public class FilterNotMatchPatternStep extends AbstractExecutionStep {

  private final List<AbstractExecutionStep> subSteps;

  private ResultSet prevResult = null;

  public FilterNotMatchPatternStep(final List<AbstractExecutionStep> steps, final CommandContext context, final boolean enableProfiling) {
    super(context, enableProfiling);
    this.subSteps = steps;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ExecutionStepInternal prevStep = checkForPrevious();

    return new ResultSet() {
      public boolean finished = false;

      private Result nextItem = null;
      private int fetched = 0;

      private void fetchNextItem() {
        nextItem = null;
        if (finished) {
          return;
        }
        if (prevResult == null) {
          prevResult = prevStep.syncPull(context, nRecords);
          if (!prevResult.hasNext()) {
            finished = true;
            return;
          }
        }
        while (!finished) {
          while (!prevResult.hasNext()) {
            prevResult = prevStep.syncPull(context, nRecords);
            if (!prevResult.hasNext()) {
              finished = true;
              return;
            }
          }
          nextItem = prevResult.next();
          final long begin = profilingEnabled ? System.nanoTime() : 0;
          try {
            if (!matchesPattern(nextItem, context)) {
              break;
            }

            nextItem = null;
          } finally {
            if (profilingEnabled) {
              cost += (System.nanoTime() - begin);
            }
          }
        }
      }

      @Override
      public boolean hasNext() {

        if (fetched >= nRecords || finished) {
          return false;
        }
        if (nextItem == null) {
          fetchNextItem();
        }

        return nextItem != null;
      }

      @Override
      public Result next() {
        if (fetched >= nRecords || finished) {
          throw new NoSuchElementException();
        }
        if (nextItem == null) {
          fetchNextItem();
        }
        if (nextItem == null) {
          throw new NoSuchElementException();
        }
        final Result result = nextItem;
        nextItem = null;
        fetched++;
        return result;
      }

      @Override
      public void close() {
        FilterNotMatchPatternStep.this.close();
      }
    };
  }

  private boolean matchesPattern(final Result nextItem, final CommandContext context) {
    final SelectExecutionPlan plan = createExecutionPlan(nextItem, context);
    try (final ResultSet rs = plan.fetchNext(1)) {
      return rs.hasNext();
    }
  }

  private SelectExecutionPlan createExecutionPlan(final Result nextItem, final CommandContext context) {
    final SelectExecutionPlan plan = new SelectExecutionPlan(context);
    plan.chain(new AbstractExecutionStep(context, profilingEnabled) {
      private boolean executed = false;

      @Override
      public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
        final InternalResultSet result = new InternalResultSet();
        if (!executed) {
          result.add(copy(nextItem));
          executed = true;
        }
        return result;
      }

      private Result copy(final Result nextItem) {
        final ResultInternal result = new ResultInternal();
        for (final String prop : nextItem.getPropertyNames()) {
          result.setProperty(prop, nextItem.getProperty(prop));
        }
        for (final String md : nextItem.getMetadataKeys()) {
          result.setMetadata(md, nextItem.getMetadata(md));
        }
        return result;
      }
    });
    subSteps.forEach(step -> plan.chain(step));
    return plan;
  }

  @Override
  public List<ExecutionStep> getSubSteps() {
    return (List) subSteps;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ NOT (\n");
    this.subSteps.forEach(x -> result.append(x.prettyPrint(depth + 1, indent)).append("\n"));
    result.append(spaces);
    result.append("  )");
    return result.toString();
  }
}
