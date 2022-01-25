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
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.GroupBy;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;

import java.util.*;

/**
 * Created by luigidellaquila on 12/07/16.
 */
public class AggregateProjectionCalculationStep extends ProjectionCalculationStep {

  private final GroupBy groupBy;
  private final long    timeoutMillis;
  private final long    limit;

  //the key is the GROUP BY key, the value is the (partially) aggregated value
  private final Map<List, ResultInternal> aggregateResults = new LinkedHashMap<>();
  private       List<ResultInternal>      finalResults     = null;

  private int  nextItem = 0;
  private long cost     = 0;

  public AggregateProjectionCalculationStep(Projection projection, GroupBy groupBy, long limit, CommandContext ctx, long timeoutMillis,
      boolean profilingEnabled) {
    super(projection, ctx, profilingEnabled);
    this.groupBy = groupBy;
    this.timeoutMillis = timeoutMillis;
    this.limit = limit;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) {
    if (finalResults == null) {
      executeAggregation(ctx, nRecords);
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
          throw new IllegalStateException();
        }
        Result result = finalResults.get(nextItem);
        nextItem++;
        localNext++;
        return result;
      }

      @Override
      public void close() {

      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private void executeAggregation(CommandContext ctx, int nRecords) {
    long timeoutBegin = System.currentTimeMillis();
    if (prev.isEmpty()) {
      throw new CommandExecutionException("Cannot execute an aggregation or a GROUP BY without a previous result");
    }
    ExecutionStepInternal prevStep = prev.get();
    ResultSet lastRs = prevStep.syncPull(ctx, nRecords);
    while (lastRs.hasNext()) {
      if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
        sendTimeout();
      }
      aggregate(lastRs.next(), ctx);
      if (!lastRs.hasNext()) {
        lastRs = prevStep.syncPull(ctx, nRecords);
      }
    }
    finalResults = new ArrayList<>();
    finalResults.addAll(aggregateResults.values());
    aggregateResults.clear();
    for (ResultInternal item : finalResults) {
      if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
        sendTimeout();
      }
      for (String name : item.getTemporaryProperties()) {
        Object prevVal = item.getTemporaryProperty(name);
        if (prevVal instanceof AggregationContext) {
          item.setTemporaryProperty(name, ((AggregationContext) prevVal).getFinalValue());
        }
      }
    }
  }

  private void aggregate(Result next, CommandContext ctx) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      List<Object> key = new ArrayList<>();
      if (groupBy != null) {
        for (Expression item : groupBy.getItems()) {
          Object val = item.execute(next, ctx);
          key.add(val);
        }
      }
      ResultInternal preAggr = aggregateResults.get(key);
      if (preAggr == null) {
        if (limit > 0 && aggregateResults.size() > limit) {
          return;
        }
        preAggr = new ResultInternal();

        for (ProjectionItem proj : this.projection.getItems()) {
          String alias = proj.getProjectionAlias().getStringValue();
          if (!proj.isAggregate()) {
            preAggr.setProperty(alias, proj.execute(next, ctx));
          }
        }
        aggregateResults.put(key, preAggr);
      }

      for (ProjectionItem proj : this.projection.getItems()) {
        String alias = proj.getProjectionAlias().getStringValue();
        if (proj.isAggregate()) {
          AggregationContext aggrCtx = (AggregationContext) preAggr.getTemporaryProperty(alias);
          if (aggrCtx == null) {
            aggrCtx = proj.getAggregationContext(ctx);
            preAggr.setTemporaryProperty(alias, aggrCtx);
          }
          aggrCtx.apply(next, ctx);
        }
      }
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ CALCULATE AGGREGATE PROJECTIONS";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result += "\n" + spaces + "      " + projection.toString() + "" + (groupBy == null ? "" : (spaces + "\n  " + groupBy));
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
