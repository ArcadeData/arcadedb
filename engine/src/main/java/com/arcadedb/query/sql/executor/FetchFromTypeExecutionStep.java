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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BetweenCondition;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.InCondition;
import com.arcadedb.query.sql.parser.IsDefinedCondition;
import com.arcadedb.query.sql.parser.IsNotDefinedCondition;
import com.arcadedb.query.sql.parser.IsNotNullCondition;
import com.arcadedb.query.sql.parser.IsNullCondition;
import com.arcadedb.query.sql.parser.NotBlock;
import com.arcadedb.query.sql.parser.OrBlock;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.FileUtils;

import java.util.*;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class FetchFromTypeExecutionStep extends AbstractExecutionStep {
  private              String                             typeName;
  private              boolean                            orderByRidAsc  = false;
  private              boolean                            orderByRidDesc = false;
  private              List<ExecutionStep>                subSteps = new ArrayList<>();
  /** Size above which scanning a whole type is worth warning an operator about. */
  static final         long                               LARGE_TYPE_BYTES = 100_000_000L;

  ResultSet currentResultSet;
  int       currentStep = 0;

  // #8523: whether a parallel scan was decided for this execution, and the scan when it was. Decided when the step is
  // first pulled, not when it is planned: the plan is cached and copied for later executions, which can run inside a
  // transaction, or on a thread, that rules a parallel scan out (and a cached copy used to lose the decision).
  private boolean          parallelDecided;
  private ParallelTypeScan parallelScan;

  protected FetchFromTypeExecutionStep(final CommandContext context) {
    super(context);
  }

  public FetchFromTypeExecutionStep(final String typeName, final Set<String> clusters, final CommandContext context,
      final Boolean ridOrder) {
    this(typeName, clusters, null, context, ridOrder);
  }

  /**
   * iterates over a class and its subTypes
   *
   * @param typeName the class name
   * @param clusters if present (it can be null), filter by only these clusters
   * @param context  the query context
   * @param ridOrder true to sort by RID asc, false to sort by RID desc, null for no sort.
   */
  public FetchFromTypeExecutionStep(final String typeName, final Set<String> clusters, final QueryPlanningInfo planningInfo,
      final CommandContext context, final Boolean ridOrder) {
    super(context);

    this.typeName = typeName;

    if (Boolean.TRUE.equals(ridOrder))
      orderByRidAsc = true;
    else if (Boolean.FALSE.equals(ridOrder))
      orderByRidDesc = true;

    final DocumentType type = context.getDatabase().getSchema().getType(typeName);
    if (type == null)
      throw new CommandExecutionException("Type " + typeName + " not found");

    final int[] typeBuckets = type.getBuckets(true).stream().mapToInt(x -> x.getFileId()).distinct().sorted().toArray();
    final List<Integer> filteredTypeBuckets = new ArrayList<>();
    for (final int bucketId : typeBuckets) {
      final String bucketName = context.getDatabase().getSchema().getBucketById(bucketId).getName();
      if (clusters == null || clusters.contains(bucketName) || clusters.contains("*"))
        filteredTypeBuckets.add(bucketId);
    }
    final int[] bucketIds = new int[filteredTypeBuckets.size() + 1];
    for (int i = 0; i < filteredTypeBuckets.size(); i++)
      bucketIds[i] = filteredTypeBuckets.get(i);

    bucketIds[bucketIds.length - 1] = -1;//temporary bucket, data in tx

    // getFileIfExists, not getFile: the latter throws when the id is not registered, and a bucket can be dropped
    // between the schema snapshot above and this lookup. The total only decides whether to log a warning, so every
    // way of not knowing a bucket's size counts it as 0 rather than failing the query - and it is not computed at
    // all when warnings are off. Page count rather than getSize(): same number, but a field read instead of the
    // channel lock and the channel.size() syscall (#6132).
    long typeFileSize = 0;
    if (CommandWarnings.isEnabled())
      for (final int fileId : bucketIds)
        if (fileId > -1
            && context.getDatabase().getFileManager().getFileIfExists(fileId) instanceof PaginatedComponentFile f)
          typeFileSize += f.getTotalPages() * (long) f.getPageSize();

    if (typeFileSize > LARGE_TYPE_BYTES) {
      final int counter = CommandWarnings.occurrencesWhenDue((DatabaseInternal) context.getDatabase(), typeName + ".scan");
      if (counter > 0) {
        final Set<String> filteredProperties = planningInfo != null ?
            extractFilteredProperties(planningInfo.whereClause) : Collections.emptySet();
        if (filteredProperties.isEmpty())
          LogManager.instance().log(this, Level.WARNING,
              "Attempt to scan type '%s' in database '%s' of total size %s %d times. This operation is very expensive, consider using an index",
              typeName, context.getDatabase().getName(), FileUtils.getSizeAsString(typeFileSize), counter);
        else
          LogManager.instance().log(this, Level.WARNING,
              "Attempt to scan type '%s' in database '%s' of total size %s %d times, filtering on propert%s %s. This operation is very expensive, consider creating an index",
              typeName, context.getDatabase().getName(), FileUtils.getSizeAsString(typeFileSize), counter,
              filteredProperties.size() == 1 ? "y" : "ies", String.join(", ", filteredProperties));
      }
    }

    sortBuckets(bucketIds);
    for (final int bucketId : bucketIds) {
      if (bucketId > 0) {
        final FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(bucketId, planningInfo, null, context);
        if (orderByRidAsc)
          step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
        else if (orderByRidDesc)
          step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);

        getSubSteps().add(step);
      }
    }
  }

  /**
   * Best-effort extraction of the property names referenced by a WHERE clause, so the slow-scan warning
   * above can tell the operator what to index instead of just how big the type is. Walks the common
   * boolean-tree and comparison shapes (AND/OR/NOT, =/&lt;/&gt;/etc., IN, BETWEEN, IS [NOT] NULL/DEFINED);
   * anything else (function calls, CONTAINS*, nested subqueries) is silently skipped rather than guessed at.
   */
  static Set<String> extractFilteredProperties(final WhereClause whereClause) {
    if (whereClause == null || whereClause.baseExpression == null)
      return Collections.emptySet();

    final Set<String> properties = new LinkedHashSet<>();
    collectFilteredProperties(whereClause.baseExpression, properties);
    return properties;
  }

  private static void collectFilteredProperties(final BooleanExpression expression, final Set<String> properties) {
    if (expression instanceof final AndBlock block) {
      for (final BooleanExpression sub : block.getSubBlocks())
        collectFilteredProperties(sub, properties);
    } else if (expression instanceof final OrBlock block) {
      for (final BooleanExpression sub : block.getSubBlocks())
        collectFilteredProperties(sub, properties);
    } else if (expression instanceof final NotBlock block) {
      collectFilteredProperties(block.getSub(), properties);
    } else if (expression instanceof final BinaryCondition condition) {
      addIfProperty(condition.left, properties);
      addIfProperty(condition.right, properties);
    } else if (expression instanceof final InCondition condition) {
      addIfProperty(condition.getLeft(), properties);
    } else if (expression instanceof final BetweenCondition condition) {
      addIfProperty(condition.first, properties);
    } else if (expression instanceof final IsNullCondition condition) {
      addIfProperty(condition.expression, properties);
    } else if (expression instanceof final IsNotNullCondition condition) {
      addIfProperty(condition.expression, properties);
    } else if (expression instanceof final IsDefinedCondition condition) {
      addIfProperty(condition.expression, properties);
    } else if (expression instanceof final IsNotDefinedCondition condition) {
      addIfProperty(condition.expression, properties);
    }
  }

  private static void addIfProperty(final Expression expression, final Set<String> properties) {
    if (expression != null && expression.isBaseIdentifier())
      properties.add(expression.toString());
  }

  private void sortBuckets(final int[] bucketIds) {
    if (orderByRidAsc) {
      Arrays.sort(bucketIds);
    } else if (orderByRidDesc) {
      Arrays.sort(bucketIds);
      //revert order
      for (int i = 0; i < bucketIds.length / 2; i++) {
        final int old = bucketIds[i];
        bucketIds[i] = bucketIds[bucketIds.length - 1 - i];
        bucketIds[bucketIds.length - 1 - i] = old;
      }
    }
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    if (!parallelDecided) {
      parallelDecided = true;
      if (!orderByRidAsc && !orderByRidDesc)
        parallelScan = ParallelTypeScan.plan(context, typeName, getSubSteps());
    }

    if (parallelScan != null)
      return parallelScan.pull(context, nRecords);

    return syncPullSequential(context, nRecords);
  }

  /** Whether an execution starting now would scan in parallel: what an EXPLAIN shows. */
  boolean wouldRunInParallel(final CommandContext context) {
    return !parallelDecided && !orderByRidAsc && !orderByRidDesc && ParallelTypeScan.plan(context, typeName, getSubSteps()) != null;
  }

  /**
   * Plans a parallel execution of this scan for an aggregation that consumes its rows in the scan's workers (issue
   * #8523), or returns {@code null} when this execution cannot run in parallel, or has already started.
   */
  ParallelTypeScan planParallelAggregation(final CommandContext context) {
    if (parallelDecided || orderByRidAsc || orderByRidDesc)
      return null;
    pullPrevious(context, Integer.MAX_VALUE);
    parallelDecided = true;
    return ParallelTypeScan.plan(context, typeName, getSubSteps());
  }

  /** The failure of a worker of this execution's parallel scan, if any: for tests. */
  Throwable getParallelScanFailure() {
    return parallelScan != null ? parallelScan.getFailure() : null;
  }

  private ResultSet syncPullSequential(final CommandContext context, final int nRecords) {
    return new ResultSet() {
      int totDispatched = 0;

      @Override
      public boolean hasNext() {
        while (true) {
          if (totDispatched >= nRecords)
            return false;

          if (currentResultSet != null && currentResultSet.hasNext())
            return true;
          else {
            if (currentStep >= getSubSteps().size())
              return false;

            currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep)).syncPull(context, nRecords);
            if (!currentResultSet.hasNext())
              currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep++)).syncPull(context, nRecords);
          }
        }
      }

      @Override
      public Result next() {
        while (true) {
          if (totDispatched >= nRecords)
            throw new NoSuchElementException();

          if (currentResultSet != null && currentResultSet.hasNext()) {
            totDispatched++;
            final Result result = currentResultSet.next();
            context.setVariable("current", result);
            return result;
          } else {
            if (currentStep >= getSubSteps().size())
              throw new NoSuchElementException();

            currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep)).syncPull(context, nRecords);
            if (!currentResultSet.hasNext())
              currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep++)).syncPull(context, nRecords);
          }
        }
      }

      @Override
      public void close() {
        for (final ExecutionStep step : getSubSteps())
          ((AbstractExecutionStep) step).close();
      }
    };
  }

  @Override
  public void sendTimeout() {
    for (final ExecutionStep step : getSubSteps())
      ((AbstractExecutionStep) step).sendTimeout();

    if (prev != null)
      prev.sendTimeout();
  }

  @Override
  public void close() {
    if (parallelScan != null)
      parallelScan.close();

    for (final ExecutionStep step : getSubSteps())
      ((AbstractExecutionStep) step).close();

    if (prev != null)
      prev.close();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = ExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM TYPE ").append(typeName);
    appendParallelism(builder, parallelDecided, parallelScan, !orderByRidAsc && !orderByRidDesc, context, typeName, getSubSteps());
    if (context.isProfiling()) {
      // The subtree roll-up, because this step is pure dispatch: the bucket sub-steps below own every nanosecond
      // it would otherwise claim, and claiming them here as if they were its own is what made a plain type scan
      // appear twice in the server profiler's aggregated table (issue #7329).
      builder.append(" (").append(getTotalCostFormatted()).append(")");
    }
    builder.append("\n");
    for (int i = 0; i < getSubSteps().size(); i++) {
      final ExecutionStepInternal step = (ExecutionStepInternal) getSubSteps().get(i);
      builder.append(step.prettyPrint(depth + 1, indent));
      if (i < getSubSteps().size() - 1) {
        builder.append("\n");
      }
    }
    return builder.toString();
  }

  /**
   * Prints whether the scan runs in parallel: as it did, once it ran, and as it would now otherwise - which is what an
   * EXPLAIN, run in the same thread and transaction state as the statement, asks.
   */
  static void appendParallelism(final StringBuilder builder, final boolean decided, final ParallelTypeScan scan, final boolean unordered,
      final CommandContext context, final String typeName, final List<ExecutionStep> bucketSteps) {
    final ParallelTypeScan shown = decided ? scan : unordered ? ParallelTypeScan.plan(context, typeName, bucketSteps) : null;
    if (shown != null)
      builder.append(" (parallel)");
  }

  @Override
  public List<ExecutionStep> getSubSteps() {
    return subSteps;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    final FetchFromTypeExecutionStep result = new FetchFromTypeExecutionStep(context);
    result.typeName = this.typeName;
    result.orderByRidAsc = this.orderByRidAsc;
    result.orderByRidDesc = this.orderByRidDesc;
    result.subSteps = this.subSteps.stream().map(x -> ((ExecutionStepInternal) x).copy(context)).collect(Collectors.toList());
    return result;
  }
}
