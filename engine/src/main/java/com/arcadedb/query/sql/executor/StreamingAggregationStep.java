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
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.GroupBy;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Streaming aggregation step that processes sorted input and emits aggregated results
 * as soon as the GROUP BY key changes, without materializing all groups in memory.
 * This is efficient when input is already sorted by the GROUP BY keys (e.g., from an index scan).
 * <p>
 * Memory usage: O(1) per group since only the current group's state is held in memory.
 */
public class StreamingAggregationStep extends AbstractExecutionStep {

  private final Projection projection;
  private final GroupBy    groupBy;

  private ExecutionStepInternal prevStep;
  private ResultSet             lastRs;
  private Result                pendingResult;
  private Object[]              currentKey;
  private ResultInternal        currentAggregation;
  private boolean               finished = false;

  public StreamingAggregationStep(final Projection projection, final GroupBy groupBy,
      final CommandContext context) {
    super(context);
    this.projection = projection;
    this.groupBy = groupBy;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (prevStep == null) {
      prevStep = checkForPrevious("Cannot execute streaming aggregation without a previous result");
      lastRs = prevStep.syncPull(context, nRecords);
    }

    return new ResultSet() {
      int fetched = 0;

      @Override
      public boolean hasNext() {
        return fetched < nRecords && !finished;
      }

      @Override
      public Result next() {
        if (fetched >= nRecords || finished)
          throw new NoSuchElementException();

        final Result result = computeNextGroup(context, nRecords);
        if (result == null)
          throw new NoSuchElementException();
        fetched++;
        return result;
      }
    };
  }

  private Result computeNextGroup(final CommandContext context, final int nRecords) {
    while (true) {
      // Get the next input row
      final Result nextRow = getNextRow(context, nRecords);

      if (nextRow == null) {
        // End of input: flush the current aggregation
        finished = true;
        if (currentAggregation != null) {
          final ResultInternal result = finalizeAggregation();
          currentAggregation = null;
          return result;
        }
        return null;
      }

      // Compute the GROUP BY key for this row
      final Object[] key = computeKey(nextRow, context);

      if (currentKey == null || Arrays.equals(currentKey, key)) {
        // Same group: accumulate
        currentKey = key;
        aggregate(nextRow, context);
      } else {
        // Group changed: finalize current group, start new one
        final ResultInternal result = finalizeAggregation();
        currentKey = key;
        currentAggregation = null;
        aggregate(nextRow, context);
        return result;
      }
    }
  }

  private Result getNextRow(final CommandContext context, final int nRecords) {
    // If we have a pending result from the previous pull, use it
    if (pendingResult != null) {
      final Result r = pendingResult;
      pendingResult = null;
      return r;
    }

    while (true) {
      if (lastRs != null && lastRs.hasNext())
        return lastRs.next();

      lastRs = prevStep.syncPull(context, nRecords);
      if (!lastRs.hasNext())
        return null;
    }
  }

  private Object[] computeKey(final Result row, final CommandContext context) {
    final Object[] keyValues = new Object[groupBy.getItems().size()];
    int idx = 0;
    for (final Expression item : groupBy.getItems())
      keyValues[idx++] = item.execute(row, context);
    return keyValues;
  }

  private void aggregate(final Result next, final CommandContext context) {
    if (currentAggregation == null) {
      currentAggregation = new ResultInternal(context.getDatabase());
      for (final ProjectionItem proj : projection.getItems()) {
        final String alias = proj.getProjectionAlias().getStringValue();
        if (!proj.isAggregate(context))
          currentAggregation.setProperty(alias, proj.execute(next, context));
      }
    }

    for (final ProjectionItem proj : projection.getItems()) {
      final String alias = proj.getProjectionAlias().getStringValue();
      if (proj.isAggregate(context)) {
        AggregationContext aggrCtx = (AggregationContext) currentAggregation.getTemporaryProperty(alias);
        if (aggrCtx == null) {
          aggrCtx = proj.getAggregationContext(context);
          currentAggregation.setTemporaryProperty(alias, aggrCtx);
        }
        aggrCtx.apply(next, context);
      }
    }
  }

  private ResultInternal finalizeAggregation() {
    for (final String name : currentAggregation.getTemporaryProperties()) {
      final Object prevVal = currentAggregation.getTemporaryProperty(name);
      if (prevVal instanceof AggregationContext aggregationContext)
        currentAggregation.setTemporaryProperty(name, aggregationContext.getFinalValue());
    }
    return currentAggregation;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ STREAMING AGGREGATE";
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    result += "\n" + spaces + "      " + projection.toString() + (groupBy == null ? "" : "\n" + spaces + "  " + groupBy);
    return result;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new StreamingAggregationStep(projection.copy(), groupBy.copy(), context);
  }
}
