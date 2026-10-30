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
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Keeps each input row for which a chain of clauses produces at least one row, and stops running the chain for that
 * input row at its first result.
 * <p>
 * Planned for a {@code RETURN DISTINCT} whose items and {@code ORDER BY} read only variables bound before the chain:
 * the rows the chain produces then differ from their input row only in variables nothing reads, and {@code DISTINCT}
 * folds all of them into one, so whether the chain produces one row or a thousand for an input row cannot change the
 * result. In {@code MATCH (vm) OPTIONAL MATCH (vm)--(rule) ... MATCH (src)--(nic) RETURN DISTINCT vm} every
 * (rule, category, source, NIC) combination of a VM was enumerated only for DISTINCT to throw all but one away.
 * <p>
 * When the chain only passes the returned variables on, its answer depends on the other variables it reads alone, and
 * is remembered by their values: there the chain runs once per rule, not once per (VM, rule) pair.
 */
public class SemiJoinStep extends AbstractExecutionStep {
  // Past this many remembered answers a new one is computed but not kept, so the memory of one execution stays bounded
  private static final int MAX_REMEMBERED = 100_000;

  private final ChainInputStep        chainInput;
  private final AbstractExecutionStep chainEnd;
  private final List<String>          memoKey;
  private       boolean               closed = false;

  /**
   * @param chainInput the step the chain's first clause reads from, fed one input row at a time
   * @param chainEnd   the chain's last step, pulled for at most one row per input row
   * @param memoKey    the variables of the input row the chain's answer is a function of, or null to run it for every row
   */
  public SemiJoinStep(final ChainInputStep chainInput, final AbstractExecutionStep chainEnd, final List<String> memoKey,
      final CommandContext context) {
    super(context);
    this.chainInput = chainInput;
    this.chainEnd = chainEnd;
    this.memoKey = memoKey;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ResultSet prevResults = checkForPrevious().syncPull(context, nRecords);
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    final Map<Object, Boolean> remembered = memoKey != null ? new HashMap<>() : null;

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
            final Object key = remembered != null ? keyOf(row) : null;
            Boolean matches = remembered != null ? remembered.get(key) : null;
            if (matches == null) {
              matches = chainMatches(row);
              if (remembered != null && remembered.size() < MAX_REMEMBERED)
                remembered.put(key, matches);
            }
            if (matches) {
              next = row;
              if (context.isProfiling())
                rowCount++;
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

  // The chain is not reachable through prev, so its steps are closed here; closing chainEnd stops at chainInput
  @Override
  public void close() {
    if (!closed) {
      closed = true;
      chainEnd.close();
    }
    super.close();
  }

  private boolean chainMatches(final Result row) {
    chainInput.setRow(row);
    final ResultSet chainResults = chainEnd.syncPull(context, 1);
    try {
      return chainResults.hasNext();
    } finally {
      chainResults.close();
    }
  }

  // A record is keyed by its RID: the same vertex can reach two rows as two different objects
  private Object keyOf(final Result row) {
    if (memoKey.size() == 1)
      return keyValue(row.getProperty(memoKey.getFirst()));
    final Object[] values = new Object[memoKey.size()];
    for (int i = 0; i < values.length; i++)
      values[i] = keyValue(row.getProperty(memoKey.get(i)));
    return Arrays.asList(values);
  }

  private static Object keyValue(final Object value) {
    return value instanceof Identifiable identifiable ? identifiable.getIdentity() : value;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    final StringBuilder builder = new StringBuilder();
    builder.append(ind).append("+ SEMI JOIN (keeps a row once the clauses below match it");
    if (memoKey != null)
      builder.append(", remembered per ").append(String.join(", ", memoKey));
    builder.append(")");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    final List<AbstractExecutionStep> chain = new ArrayList<>();
    for (AbstractExecutionStep step = chainEnd; step != null && step != chainInput;
        step = (AbstractExecutionStep) step.getPrev())
      chain.addFirst(step);
    for (final AbstractExecutionStep step : chain)
      builder.append("\n").append(step.prettyPrint(depth + 1, indent));
    return builder.toString();
  }

  /**
   * The first step of the chain: answers each pull with the input row {@link SemiJoinStep} is testing.
   */
  public static class ChainInputStep extends AbstractExecutionStep {
    private Result row;

    public ChainInputStep(final CommandContext context) {
      super(context);
    }

    void setRow(final Result row) {
      this.row = row;
    }

    // Hands the row over once: a step pulling again after its first batch must find its input exhausted
    @Override
    public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
      final Result current = row;
      row = null;
      return new IteratorResultSet(current != null ? List.of(current).iterator() : List.<Result>of().iterator());
    }

    @Override
    public String prettyPrint(final int depth, final int indent) {
      return "  ".repeat(Math.max(0, depth * indent)) + "+ SEMI JOIN INPUT ROW";
    }
  }
}
