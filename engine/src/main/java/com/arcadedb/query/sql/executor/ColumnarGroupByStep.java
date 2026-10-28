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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.columnar.ColumnarStats;
import com.arcadedb.query.sql.executor.ColumnarGroupByPlanner.AggregateSpec;
import com.arcadedb.query.sql.executor.ColumnarGroupByPlanner.ColumnarGroupByPlan;

import java.util.List;

/**
 * Answers an eligible GROUP BY ({@link ColumnarGroupByPlanner}) from the COLUMNAR projection, or runs the normal aggregation chain it
 * carries as its fallback (pre-aggregate projection, aggregation, post-aggregate projection, exactly as the planner builds them without
 * this step). Which of the two answers is decided at each execution, so a cached plan follows the projection's state and the
 * {@link GlobalConfiguration#COLUMNAR_GROUP_BY_ACCELERATION} switch of the database at the time it runs.
 * <p>
 * This version always runs the fallback; the columnar path is not implemented yet.
 */
public class ColumnarGroupByStep extends AbstractExecutionStep {
  private final ColumnarGroupByPlan   plan;
  private final SelectExecutionPlan   fallback;
  private final ExecutionStepInternal fallbackFirst;
  private final ExecutionStepInternal fallbackLast;
  private       String                lastPath;
  private       ExecutionStepInternal source;

  ColumnarGroupByStep(final ColumnarGroupByPlan plan, final SelectExecutionPlan fallback, final CommandContext context) {
    super(context);
    this.plan = plan;
    this.fallback = fallback;
    this.fallbackFirst = (ExecutionStepInternal) fallback.getSteps().getFirst();
    this.fallbackLast = (ExecutionStepInternal) fallback.getSteps().getLast();
  }

  /** {@code "columnar"}, {@code "normal: <reason>"}, or null before the first pull of this execution. */
  String lastPath() {
    return lastPath;
  }

  @Override
  public void setPrevious(final ExecutionStepInternal step) {
    super.setPrevious(step);
    // the fallback reads the same upstream as this step
    fallbackFirst.setPrevious(step);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (source == null)
      source = decide(context);
    return source.syncPull(context, nRecords);
  }

  /** The step that answers this execution, with {@link #lastPath} and the path counters set. */
  private ExecutionStepInternal decide(final CommandContext context) {
    final DatabaseInternal db = context.getDatabase();
    final String reason = db.getConfiguration().getValueAsBoolean(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION) ?
        "the columnar path is not implemented" :
        "disabled by " + GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION.getKey();
    lastPath = "normal: " + reason;
    stats(db).groupByFallbacks();
    return fallbackLast;
  }

  private static ColumnarStats stats(final DatabaseInternal db) {
    return ((LocalDatabase) db.getEmbedded()).getColumnarIndexManager().getStats();
  }

  @Override
  public void sendTimeout() {
    fallbackLast.sendTimeout();
    super.sendTimeout();
  }

  @Override
  public void close() {
    // the fallback chain closes upstream too: closing is idempotent
    fallbackLast.close();
    super.close();
  }

  @Override
  public void reset() {
    fallback.reset(context);
    source = null;
    lastPath = null;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces).append("+ AGGREGATE FROM COLUMNAR PROJECTION (").append(plan.typeName()).append('.').append(plan.groupProperty())
        .append(')');
    if (context.isProfiling())
      result.append(" (").append(getTotalCostFormatted()).append(')');
    result.append('\n').append(spaces).append("    aggregates: ");
    boolean first = true;
    for (final AggregateSpec aggregate : plan.aggregates())
      if (aggregate != null) {
        if (!first)
          result.append(", ");
        result.append(aggregate);
        first = false;
      }
    if (first)
      result.append("none");
    result.append('\n').append(spaces).append("    match set: ").append(plan.whereless() ? "all records" : "from upstream");
    result.append('\n').append(spaces).append("    fallback: CALCULATE AGGREGATE PROJECTIONS");
    if (lastPath != null) {
      result.append('\n').append(spaces).append("    path: normal (reason: ").append(lastPath.substring("normal: ".length())).append(')');
      // the chain that ran, with its own costs
      result.append('\n').append(fallback.prettyPrint(depth + 2, indent));
    }
    return result.toString();
  }

  /** The fallback chain: the steps that answer while the path is normal, so plan JSON and cost roll-ups include them. */
  @Override
  public List<ExecutionStep> getSubSteps() {
    return fallback.getSteps();
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new ColumnarGroupByStep(plan, (SelectExecutionPlan) fallback.copy(context), context);
  }

  /** Cacheable as its fallback is: the path is decided per execution, never at planning. */
  @Override
  public boolean canBeCached() {
    return fallback.canBeCached();
  }
}
