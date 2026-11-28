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
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.NoSuchElementException;

/**
 * The part of a MATCH node's {@code where:} that reads {@code $currentMatch}, applied to the records the root plan of the node
 * fetched (issue #12949). The traversers bind {@code $currentMatch} to the candidate they test, but a root, a prefetched node
 * included, is fetched by a SELECT whose steps know nothing of that variable, so its filter read {@code null} there. This step
 * binds it to each record before it evaluates the condition, as {@link MatchEdgeTraverser} does, and restores the value it found
 * afterwards. It also binds {@code $current}, as the {@link FilterStep} of the SELECT the condition comes out of does.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class MatchRootFilterStep extends AbstractExecutionStep {
  static final String CURRENT_MATCH_VARIABLE = "currentMatch";

  private final WhereClause whereClause;
  private       ResultSet   prevResult = null;

  public MatchRootFilterStep(final WhereClause whereClause, final CommandContext context) {
    super(context);
    this.whereClause = whereClause;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ExecutionStepInternal prevStep = checkForPrevious();
    // A FILTER THAT REJECTS EVERY RECORD SPENDS THE WHOLE SCAN INSIDE ONE hasNext(): THE COMMAND DEADLINE IS CHECKED HERE (#6266)
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    return new ResultSet() {
      private boolean finished = false;
      private Result  nextItem = null;
      private int     fetched  = 0;

      private void fetchNextItem() {
        nextItem = null;
        while (!finished) {
          guard.check();
          if (prevResult == null || !prevResult.hasNext()) {
            prevResult = prevStep.syncPull(context, nRecords);
            if (!prevResult.hasNext()) {
              finished = true;
              return;
            }
          }
          final Result candidate = prevResult.next();
          if (matches(candidate, context)) {
            nextItem = candidate;
            return;
          }
        }
      }

      @Override
      public boolean hasNext() {
        if (fetched >= nRecords || finished)
          return false;
        if (nextItem == null)
          fetchNextItem();
        return nextItem != null;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result result = nextItem;
        nextItem = null;
        fetched++;
        return result;
      }

      @Override
      public void close() {
        MatchRootFilterStep.this.close();
      }
    };
  }

  private boolean matches(final Result candidate, final CommandContext context) {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    final Object previousMatch = context.getVariable(CURRENT_MATCH_VARIABLE);
    context.setVariable(CURRENT_MATCH_VARIABLE, candidate.isElement() ? candidate.toElement() : candidate);
    // $current IS LEFT ON THE ROW, AS FilterStep AND THE FETCH STEPS LEAVE IT: EVERY STEP THAT READS IT SETS IT FIRST
    context.setVariable("current", candidate);
    try {
      return whereClause.matchesFilters(candidate, context);
    } finally {
      // THE FILTER IS ARBITRARY SQL: RESTORE EVEN WHEN IT THROWS, OR THE NEXT NODE WOULD READ THIS ROOT'S CANDIDATE
      context.setVariable(CURRENT_MATCH_VARIABLE, previousMatch);
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder result = new StringBuilder();
    result.append(ExecutionStepInternal.getIndent(depth, indent)).append("+ FILTER ITEMS WHERE ($currentMatch BOUND)");
    if (context.isProfiling())
      result.append(" (").append(getCostFormatted()).append(")");
    result.append("\n");
    result.append(ExecutionStepInternal.getIndent(depth, indent));
    result.append("  ");
    result.append(whereClause.toString());
    return result.toString();
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new MatchRootFilterStep(whereClause.copy(), context);
  }
}
