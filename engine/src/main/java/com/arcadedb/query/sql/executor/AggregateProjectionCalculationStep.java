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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.GroupBy;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;

import java.util.*;

/**
 * Created by luigidellaquila on 12/07/16.
 */
public class AggregateProjectionCalculationStep extends ProjectionCalculationStep {

  /**
   * Lightweight wrapper for GROUP BY keys using Object[] instead of ArrayList.
   * This reduces memory overhead by eliminating ArrayList wrapper objects for each key.
   */
  private static class GroupByKey {
    private final Object[] values;
    private final int hashCode;

    GroupByKey(final Object[] values) {
      this.values = values;
      this.hashCode = Arrays.hashCode(values);
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj)
        return true;
      if (!(obj instanceof GroupByKey))
        return false;
      return Arrays.equals(this.values, ((GroupByKey) obj).values);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  private final GroupBy groupBy;
  private final long    timeoutMillis;
  private final long    limit;
  private final long    maxGroupsAllowed;

  //the key is the GROUP BY key, the value is the (partially) aggregated value
  private final Map<GroupByKey, ResultInternal> aggregateResults = new LinkedHashMap<>();
  private       List<ResultInternal>            finalResults     = null;

  private int nextItem = 0;

  public AggregateProjectionCalculationStep(final Projection projection, final GroupBy groupBy, final long limit,
      final CommandContext context,
      final long timeoutMillis) {
    super(projection, context);
    this.groupBy = groupBy;
    this.timeoutMillis = timeoutMillis;
    this.limit = limit;

    // Memory optimization: Enforce memory limits for GROUP BY operations
    final Database db = context == null ? null : context.getDatabase();
    this.maxGroupsAllowed = db == null ?
        GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong() :
        db.getConfiguration().getValueAsLong(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) {
    if (finalResults == null) {
      executeAggregation(context, nRecords);
    }

    return new ResultSet() {
      int localNext = 0;

      @Override
      public boolean hasNext() {
        return localNext <= nRecords && nextItem < finalResults.size();
      }

      @Override
      public Result next() {
        if (localNext > nRecords || nextItem >= finalResults.size()) {
          throw new NoSuchElementException();
        }
        final Result result = finalResults.get(nextItem);
        nextItem++;
        localNext++;
        return result;
      }
    };
  }

  private void executeAggregation(final CommandContext context, final int nRecords) {
    final long timeoutBegin = System.currentTimeMillis();

    final ExecutionStepInternal prevStep = checkForPrevious(
        "Cannot execute an aggregation or a GROUP BY without a previous result");
    ResultSet lastRs = prevStep.syncPull(context, nRecords);
    while (lastRs.hasNext()) {
      if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
        sendTimeout();
      }
      aggregate(lastRs.next(), context);
      if (!lastRs.hasNext()) {
        lastRs = prevStep.syncPull(context, nRecords);
      }
    }
    finalResults = new ArrayList<>(aggregateResults.values());
    aggregateResults.clear();
    for (final ResultInternal item : finalResults) {
      if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
        sendTimeout();
      }
      for (final String name : item.getTemporaryProperties()) {
        final Object prevVal = item.getTemporaryProperty(name);
        if (prevVal instanceof AggregationContext aggregationContext) {
          item.setTemporaryProperty(name, aggregationContext.getFinalValue());
        }
      }
    }
  }

  private void aggregate(final Result next, final CommandContext context) {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      // Memory optimization: Use Object[] instead of ArrayList to reduce object allocation overhead
      final GroupByKey key;
      if (groupBy != null) {
        final Object[] keyValues = new Object[groupBy.getItems().size()];
        int idx = 0;
        for (final Expression item : groupBy.getItems()) {
          keyValues[idx++] = item.execute(next, context);
        }
        key = new GroupByKey(keyValues);
      } else {
        // No GROUP BY means single aggregation group
        key = new GroupByKey(new Object[0]);
      }
      ResultInternal preAggr = aggregateResults.get(key);
      if (preAggr == null) {
        // Query LIMIT optimization: stop processing once we have enough groups
        if (limit > 0 && aggregateResults.size() >= limit)
          return;

        // Memory safety: enforce memory limit for GROUP BY operations
        if (maxGroupsAllowed > 0 && aggregateResults.size() >= maxGroupsAllowed) {
          aggregateResults.clear();
          throw new CommandExecutionException(
              "Limit of allowed groups for in-heap GROUP BY in a single query exceeded (" + maxGroupsAllowed
                  + "). You can set " + GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey()
                  + " to increase this limit");
        }

        preAggr = new ResultInternal(context.getDatabase());

        for (final ProjectionItem proj : this.projection.getItems()) {
          final String alias = proj.getProjectionAlias().getStringValue();
          if (!proj.isAggregate(context))
            preAggr.setProperty(alias, proj.execute(next, context));
        }
        aggregateResults.put(key, preAggr);
      }

      for (final ProjectionItem proj : this.projection.getItems()) {
        final String alias = proj.getProjectionAlias().getStringValue();
        if (proj.isAggregate(context)) {
          AggregationContext aggrCtx = (AggregationContext) preAggr.getTemporaryProperty(alias);
          if (aggrCtx == null) {
            aggrCtx = proj.getAggregationContext(context);
            preAggr.setTemporaryProperty(alias, aggrCtx);
          }
          aggrCtx.apply(next, context);
        }
      }

      // Memory optimization: Clear the element reference from the input Result after processing
      // This releases the full Document object and allows it to be garbage collected
      // We've already extracted all needed values into the key and preAggr
      if (next instanceof ResultInternal) {
        ((ResultInternal) next).setElement(null);
      }
    } finally {
      if (context.isProfiling())
        cost += (System.nanoTime() - begin);
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ CALCULATE AGGREGATE PROJECTIONS";
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";

    result += "\n" + spaces + "      " + projection.toString() + "" + (groupBy == null ? "" : (spaces + "\n  " + groupBy));
    return result;
  }
}
