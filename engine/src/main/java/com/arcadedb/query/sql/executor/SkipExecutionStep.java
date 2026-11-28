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
import com.arcadedb.query.sql.parser.Skip;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class SkipExecutionStep extends AbstractExecutionStep {
  private final Skip    skip;
  private       int     skipped = 0;
  private       boolean finished;
  // The rows of the SKIP the step that reads the index passed over without loading their records, see skipUnloaded(). -1 until the
  // first pull. Like skipped and finished it is not reset: a plan with a SKIP runs once, this step has no reset() and no copy(), and
  // neither has the LIMIT step that goes with it, so the plan is never cached nor executed again
  private       int     skippedUnloaded = -1;

  /**
   * Rows an upstream batch carried PAST the skip point, waiting to be handed to the caller.
   * <p>
   * {@code syncPull} asks upstream for only the rows it still owes the skip, but a step is free to answer with
   * more than it was asked for, and every {@code schema:} catalog listing answered with its whole listing until
   * issue #7898. Draining that batch here - which is what this step used to do, discarding every row it received
   * rather than the ones it owed - lost them: {@code SELECT FROM schema:types SKIP 1} answered zero rows out of
   * six. They are kept here instead and served in the requested batch size.
   */
  private final Deque<Result> overflow = new ArrayDeque<>();

  public SkipExecutionStep(final Skip skip, final CommandContext context) {
    super(context);
    this.skip = skip;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) {
    if (finished)
      return new InternalResultSet();//empty

    checkForPrevious();

    final int skipValue = skip.getValue(context);
    if (skipValue < 0)
      throw new CommandExecutionException("Cannot execute a query with a negative SKIP");
    if (skippedUnloaded < 0) {
      skippedUnloaded = skipUnloaded(context, skipValue);
      skipped = skippedUnloaded;
    }
    while (skipped < skipValue) {
      //fetch and discard
      final ResultSet rs = prev.syncPull(context, Math.min(100, skipValue - skipped));//fetch blocks of 100, at most
      if (!rs.hasNext()) {
        finished = true;
        return new InternalResultSet();//empty
      }
      // Discard only what is still owed...
      while (skipped < skipValue && rs.hasNext()) {
        rs.next();
        skipped++;
      }
      // ...and keep whatever the batch carried beyond it (issue #7898).
      while (rs.hasNext())
        overflow.add(rs.next());
    }

    if (!overflow.isEmpty()) {
      final InternalResultSet batch = new InternalResultSet();
      final int count = nRecords > 0 ? Math.min(nRecords, overflow.size()) : overflow.size();
      for (int i = 0; i < count; i++)
        batch.add(overflow.poll());
      return batch;
    }

    return prev.syncPull(context, nRecords);
  }

  /**
   * Hands the SKIP to the step that reads the index entries, which passes over the rows without loading their records (issue
   * #11954), when every step in between makes one row of each row it reads and decides nothing on the record: a projection, and the
   * type filter, whose decision the index step takes from the record's bucket. Any other step in between (a WHERE left to evaluate,
   * DISTINCT, expand(), UNWIND, LET) leaves the SKIP to discard the rows as they come.
   * <p>
   * A projection under the SKIP is not computed for the rows passed over: what it would evaluate for a row the SKIP throws away (an
   * error, a function with a side effect) is not evaluated, exactly as in a statement with no ORDER BY, where the planner chains the
   * SKIP before the projection.
   *
   * @return the rows passed over, the rest of the SKIP being discarded from the rows that follow
   */
  private int skipUnloaded(final CommandContext context, final int skipValue) {
    if (skipValue <= 0)
      return 0;

    FilterByTypeStep typeFilter = null;
    ExecutionStepInternal step = prev;
    while (step != null) {
      final Class<?> stepClass = step.getClass();
      if (stepClass == ProjectionCalculationStep.class)
        step = ((ProjectionCalculationStep) step).getPrev();
      else if (stepClass == FilterByTypeStep.class && typeFilter == null) {
        typeFilter = (FilterByTypeStep) step;
        step = typeFilter.getPrev();
        // the filter judges the stored record, which the index step knows from its bucket: not a row a projection made of it
        if (step == null || step.getClass() != GetValueFromIndexEntryStep.class && step.getClass() != ParallelExecStep.class)
          return 0;
      } else if (stepClass == GetValueFromIndexEntryStep.class)
        return ((GetValueFromIndexEntryStep) step).skipUnloaded(context, skipValue, typeFilter);
      else if (stepClass == ParallelExecStep.class)
        return ((ParallelExecStep) step).skipUnloaded(context, skipValue, typeFilter);
      else
        return 0;
    }
    return 0;
  }

  @Override
  public void sendTimeout() {
    // IGNORE THE TIMEOUT
  }

  @Override
  public void close() {
    overflow.clear();
    if (prev != null)
      prev.close();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ SKIP (" + skip.toString() + ")";
    if (context.isProfiling() && skippedUnloaded > 0)
      result += " (" + skippedUnloaded + " rows passed over in the index, records not loaded)";
    return result;
  }
}
