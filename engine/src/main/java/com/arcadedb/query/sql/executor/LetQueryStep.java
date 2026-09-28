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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.LocalResultSet;
import com.arcadedb.query.sql.parser.Statement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luigidellaquila on 03/08/16.
 */
public class LetQueryStep extends AbstractExecutionStep {
  private final Identifier varName;
  private final Statement  query;

  // Built from the first record this instance processes, then reused (via #copy) for every later record instead of
  // being planned again. A LET subquery is re-evaluated once per incoming record - see #calculate - so without this,
  // query.createExecutionPlan() re-runs the full SelectExecutionPlanner (target/index selection, optimizeQuery, ...)
  // for every single row, even though the query text never changes between rows: only the $parent binding a
  // correlated subquery reads does. #copy() gives every row its own fresh step instances bound to that row's
  // subCtx - the same mechanism ExecutionPlanCache.get() already relies on for the (single) top-level statement
  // cache - so this is safe for a correlated subquery exactly as that cache already is. Left null (falling back to
  // the per-row createExecutionPlan below) when the plan reports it cannot be cached.
  private InternalExecutionPlan cachedSubPlanTemplate;

  // The statement this LET belongs to. Its own calls run between two rows' subqueries, so a side-effecting call there
  // (a user-defined function) rules the result cache out as surely as one inside the subquery does. Null when the
  // step was built without it, which only makes the check look at the subquery alone.
  private final Statement               enclosingStatement;
  // Plan reuse (above) stops re-PLANNING the subquery every row; this stops re-EXECUTING it for a binding already
  // seen, when many rows feed it the same outer values (issue #8400). Created on the first row, null when the
  // subquery or the enclosing statement is not cacheable or the cache is disabled by configuration. One instance per
  // execution: this step is never copied into another execution (it does not implement copy()), so a cached result
  // can never outlive the execution, and with it the input parameters and database state, it was computed under.
  private       CorrelatedSubQueryCache resultCache;
  private       boolean                 resultCacheInitialized;

  public LetQueryStep(final Identifier varName, final Statement query, final CommandContext context) {
    this(varName, query, null, context);
  }

  public LetQueryStep(final Identifier varName, final Statement query, final Statement enclosingStatement, final CommandContext context) {
    super(context);
    this.varName = varName;
    this.query = query;
    this.enclosingStatement = enclosingStatement;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("Cannot execute a local LET on a query without a target");

    return new ResultSet() {
      final ResultSet source = getPrev().syncPull(context, nRecords);

      @Override
      public boolean hasNext() {
        return source.hasNext();
      }

      @Override
      public Result next() {
        final Result result = source.next();
        if (result != null) {
          calculate(result, context);
        }
        return result;
      }

      private void calculate(final Result result, final CommandContext context) {
        final long beginTime = System.nanoTime();

        final DatabaseInternal database = context.getDatabase();
        final CorrelatedSubQueryCache cache = getResultCache(database);

        // NO ROW CAN HIT A DISABLED CACHE: STOP PAYING FOR THE TRACKING CONTEXT TOO
        final boolean useCache = cache != null && !cache.isDisabled();
        List<Result> value = useCache ? cache.lookup(context, database) : null;
        if (value == null) {
          final BasicCommandContext subCtx;
          if (useCache)
            subCtx = cache.newContext(context);
          else {
            subCtx = new BasicCommandContext();
            subCtx.setDatabase(database);
            subCtx.setParentWithoutOverridingChild(context);
          }

          final InternalExecutionPlan subExecutionPlan;
          if (cachedSubPlanTemplate != null) {
            subExecutionPlan = cachedSubPlanTemplate.copy(subCtx);
          } else {
            subExecutionPlan = query.createExecutionPlan(subCtx);
            if (subExecutionPlan.canBeCached())
              cachedSubPlanTemplate = subExecutionPlan;
          }

          value = toList(new LocalResultSet(subExecutionPlan));
          if (useCache)
            cache.store((CorrelatedSubQueryCache.TrackingContext) subCtx, context, database, value);
        }
        // Not every upstream Result is a ResultInternal (e.g. wrapper Results): guard the cast to avoid a
        // ClassCastException. When the row cannot carry per-row metadata, the LET value is still exposed through
        // the context variable below, so $varName keeps resolving.
        if (result instanceof ResultInternal resultInternal)
          resultInternal.setMetadata(varName.getStringValue(), value);
        context.setVariable(varName.getStringValue(), value);

        // Accumulate (+=) the elapsed time across every processed record so the reported cost reflects the
        // whole step, not just the last record; only sample nanoTime() when profiling to avoid the overhead.
        if (context.isProfiling())
          cost += System.nanoTime() - beginTime;
      }

      private List<Result> toList(final LocalResultSet oLocalResultSet) {
        final List<Result> result = new ArrayList<>();
        while (oLocalResultSet.hasNext()) {
          result.add(oLocalResultSet.next());
        }
        oLocalResultSet.close();
        return result;
      }

      @Override
      public void close() {
        source.close();
      }
    };
  }

  private CorrelatedSubQueryCache getResultCache(final DatabaseInternal database) {
    if (!resultCacheInitialized) {
      resultCacheInitialized = true;
      if (database != null)
        resultCache = CorrelatedSubQueryCache.create(query, enclosingStatement, database,
            database.getConfiguration().getValueAsInteger(GlobalConfiguration.SQL_LET_SUBQUERY_CACHE_SIZE));
    }
    return resultCache;
  }

  /** The per-execution result cache, or null when it is not in use. Exposed for tests. */
  CorrelatedSubQueryCache getResultCache() {
    return resultCache;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);

    final StringBuilder result = new StringBuilder();
    result.append(spaces).append("+ LET (for each record)\n").append(spaces).append("  ").append(varName).append(" = (")
        .append(query).append(")");
    if (context.isProfiling()) {
      result.append(" (").append(getCostFormatted()).append(")");
      if (resultCache != null)
        result.append("\n").append(spaces).append("  result cache: ").append(resultCache.getHits()).append(" hits, ")
            .append(resultCache.getMisses()).append(" misses").append(resultCache.isDisabled() ? " (disabled)" : "");
    }
    return result.toString();
  }
}
