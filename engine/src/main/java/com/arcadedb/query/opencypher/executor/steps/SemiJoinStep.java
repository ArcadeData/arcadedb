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

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ExecutionStepInternal;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Keeps each input row for which a chain of clauses produces at least one row, stopping the chain at its first row
 * (issue #11009). Planned by {@code DistinctSemiJoin} for a {@code RETURN DISTINCT} whose items read only names bound
 * before the chain: the rows the chain produces then differ from their input row only in names nothing reads, and
 * DISTINCT folds them all into one, so one row or a thousand cannot change the result.
 * <p>
 * Two input rows with the same values for the names the RETURN reads project the same row, so once one qualified the
 * other is passed over without running the chain: DISTINCT would drop whatever it produced. When the chain reads none
 * of those names its answer is a function of the names it does read, and is remembered by their values.
 * <p>
 * The chain is the same steps the clauses would have been planned as, fed one input row at a time through
 * {@link ChainInputStep}, the way {@link OptionalMatchStep} feeds its own match chain.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SemiJoinStep extends AbstractExecutionStep {
  // Past this many remembered answers or qualified keys a new one is still computed but no longer kept, so the memory of
  // one execution stays bounded: forgetting only costs running the chain again, the downstream DISTINCT still dedups
  private static final int MAX_REMEMBERED = 100_000;

  private final ChainInputStep        chainInput;
  private final AbstractExecutionStep chainEnd;
  private final String[]              distinctKey;
  private final String[]              memoKey;
  private       boolean               chainClosed = false;
  // Profiling counters, reported by prettyPrint
  private       long                  chainRuns   = 0;
  private       long                  remembered  = 0;
  private       long                  qualified   = 0;

  /**
   * @param chainInput  the step the chain's first clause reads from
   * @param chainEnd    the chain's last step, pulled for at most one row per input row
   * @param distinctKey the names the RETURN reads: an input row whose values for them already qualified is passed over
   * @param memoKey     the names the chain's answer is a function of, or null to run it for every input row
   */
  public SemiJoinStep(final ChainInputStep chainInput, final AbstractExecutionStep chainEnd, final List<String> distinctKey,
      final List<String> memoKey, final CommandContext context) {
    super(context);
    this.chainInput = chainInput;
    this.chainEnd = chainEnd;
    this.distinctKey = distinctKey.toArray(new String[0]);
    this.memoKey = memoKey != null ? memoKey.toArray(new String[0]) : null;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    // Pulled once, like every step of this pipeline: the result set streams the whole input
    final ResultSet prevResults = checkForPrevious().syncPull(context, nRecords);
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    final Set<Object> qualifiedKeys = new HashSet<>();
    final Map<Object, Boolean> answers = memoKey != null ? new HashMap<>() : null;

    return new ResultSet() {
      private Result next = null;

      @Override
      public boolean hasNext() {
        if (next != null)
          return true;

        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          while (prevResults.hasNext()) {
            guard.check();
            final Result row = prevResults.next();
            final Object distinct = keyOf(row, distinctKey);
            if (qualifiedKeys.contains(distinct)) {
              if (context.isProfiling())
                qualified++;
              continue;
            }

            final boolean matches;
            if (answers != null) {
              final Object key = keyOf(row, memoKey);
              final Boolean known = answers.get(key);
              if (known != null) {
                if (context.isProfiling())
                  remembered++;
                matches = known;
              } else {
                matches = chainMatches(row);
                if (answers.size() < MAX_REMEMBERED)
                  answers.put(key, matches);
              }
            } else
              matches = chainMatches(row);

            if (matches) {
              if (qualifiedKeys.size() < MAX_REMEMBERED)
                qualifiedKeys.add(distinct);
              if (context.isProfiling())
                rowCount++;
              next = row;
              return true;
            }
          }
          return false;
        } finally {
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result result = next;
        next = null;
        return result;
      }

      @Override
      public void close() {
        prevResults.close();
      }
    };
  }

  // The chain is not reachable through prev, so it is closed here once; closing chainEnd stops at chainInput
  @Override
  public void close() {
    if (!chainClosed) {
      chainClosed = true;
      chainEnd.close();
    }
    super.close();
  }

  private boolean chainMatches(final Result row) {
    if (context.isProfiling())
      chainRuns++;
    chainInput.setRow(row);
    final ResultSet chainResults = chainEnd.syncPull(context, 1);
    try {
      return chainResults.hasNext();
    } finally {
      chainResults.close();
    }
  }

  // A record is keyed by its RID: the same vertex can reach two rows as two different objects
  private static Object keyOf(final Result row, final String[] names) {
    if (names.length == 1)
      return keyValue(row.getProperty(names[0]));
    final Object[] values = new Object[names.length];
    for (int i = 0; i < names.length; i++)
      values[i] = keyValue(row.getProperty(names[i]));
    return Arrays.asList(values);
  }

  private static Object keyValue(final Object value) {
    return value instanceof Identifiable identifiable ? identifiable.getIdentity() : value;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    builder.append(ExecutionStepInternal.getIndent(depth, indent)).append("+ SEMI JOIN (keeps a row once the clauses below match it, once per ")
        .append(String.join(", ", distinctKey));
    if (memoKey != null)
      builder.append(", remembered per ").append(memoKey.length == 0 ? "query" : String.join(", ", memoKey));
    builder.append(")");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(") chain runs: ").append(chainRuns).append(", remembered: ").append(remembered)
          .append(", already qualified: ").append(qualified);
    }
    final List<AbstractExecutionStep> chain = new ArrayList<>();
    for (AbstractExecutionStep step = chainEnd; step != null && step != chainInput; step = (AbstractExecutionStep) step.getPrev())
      chain.addFirst(step);
    for (final AbstractExecutionStep step : chain)
      builder.append("\n").append(step.prettyPrint(depth + 1, indent));
    return builder.toString();
  }

  /**
   * The first step of the chain: answers the pull of each test with the input row {@link SemiJoinStep} is testing,
   * once, so a step pulling again finds its input exhausted.
   */
  public static class ChainInputStep extends AbstractExecutionStep {
    private Result row;

    public ChainInputStep(final CommandContext context) {
      super(context);
    }

    void setRow(final Result row) {
      this.row = row;
    }

    @Override
    public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
      final Result current = row;
      row = null;
      return new IteratorResultSet(current != null ? List.of(current).iterator() : List.<Result>of().iterator());
    }

    @Override
    public String prettyPrint(final int depth, final int indent) {
      return ExecutionStepInternal.getIndent(depth, indent) + "+ SEMI JOIN INPUT ROW";
    }
  }
}
