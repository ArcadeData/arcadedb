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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.DocumentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Fetches records from a type (all buckets) with an integrated WHERE filter.
 * This avoids the separate FilterStep overhead by evaluating the predicate inline
 * during scanning, which is significantly faster for selective queries.
 */
public class FetchFromTypeWithFilterStep extends AbstractExecutionStep {

  private       String      typeName;
  private       WhereClause whereClause;
  private       Set<String> projectedProperties;
  private       boolean     orderByRidAsc  = false;
  private       boolean     orderByRidDesc = false;
  private       List<ExecutionStep> subSteps = new ArrayList<>();

  private ResultSet currentResultSet;
  private int       currentStep = 0;

  private FetchFromTypeWithFilterStep(final CommandContext context) {
    super(context);
  }

  public FetchFromTypeWithFilterStep(final String typeName, final Set<String> clusters,
      final WhereClause whereClause, final Set<String> projectedProperties,
      final CommandContext context, final Boolean ridOrder) {
    super(context);
    this.typeName = typeName;
    this.whereClause = whereClause;
    this.projectedProperties = projectedProperties;

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
    final int[] bucketIds = new int[filteredTypeBuckets.size()];
    for (int i = 0; i < filteredTypeBuckets.size(); i++)
      bucketIds[i] = filteredTypeBuckets.get(i);

    if (orderByRidAsc)
      Arrays.sort(bucketIds);
    else if (orderByRidDesc) {
      Arrays.sort(bucketIds);
      for (int i = 0; i < bucketIds.length / 2; i++) {
        final int old = bucketIds[i];
        bucketIds[i] = bucketIds[bucketIds.length - 1 - i];
        bucketIds[bucketIds.length - 1 - i] = old;
      }
    }

    for (final int bucketId : bucketIds) {
      if (bucketId > 0) {
        final ScanWithFilterStep step = new ScanWithFilterStep(bucketId, whereClause, projectedProperties, context);
        if (orderByRidAsc)
          step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
        else if (orderByRidDesc)
          step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
        subSteps.add(step);
      }
    }
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

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
            if (currentStep >= subSteps.size())
              return false;

            currentResultSet = ((AbstractExecutionStep) subSteps.get(currentStep)).syncPull(context, nRecords);
            if (!currentResultSet.hasNext())
              currentResultSet = ((AbstractExecutionStep) subSteps.get(currentStep++)).syncPull(context, nRecords);
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
            if (currentStep >= subSteps.size())
              throw new NoSuchElementException();

            currentResultSet = ((AbstractExecutionStep) subSteps.get(currentStep)).syncPull(context, nRecords);
            if (!currentResultSet.hasNext())
              currentResultSet = ((AbstractExecutionStep) subSteps.get(currentStep++)).syncPull(context, nRecords);
          }
        }
      }

      @Override
      public void close() {
        for (final ExecutionStep step : subSteps)
          ((AbstractExecutionStep) step).close();
      }
    };
  }

  @Override
  public void sendTimeout() {
    for (final ExecutionStep step : subSteps)
      ((AbstractExecutionStep) step).sendTimeout();
    if (prev != null)
      prev.sendTimeout();
  }

  @Override
  public void close() {
    for (final ExecutionStep step : subSteps)
      ((AbstractExecutionStep) step).close();
    if (prev != null)
      prev.close();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = ExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM TYPE ").append(typeName).append(" WITH FILTER");
    if (projectedProperties != null)
      builder.append(" [projected: ").append(String.join(", ", projectedProperties)).append("]");
    if (context.isProfiling())
      builder.append(" (").append(getCostFormatted()).append(")");
    builder.append("\n");
    for (int i = 0; i < subSteps.size(); i++) {
      final ExecutionStepInternal step = (ExecutionStepInternal) subSteps.get(i);
      builder.append(step.prettyPrint(depth + 1, indent));
      if (i < subSteps.size() - 1)
        builder.append("\n");
    }
    return builder.toString();
  }

  @Override
  public long getCost() {
    return subSteps.stream().map(ExecutionStep::getCost).reduce((a, b) -> a > 0 && b > 0 ? a + b : a > 0 ? a : b > 0 ? b : -1L)
        .orElse(-1L);
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
    final FetchFromTypeWithFilterStep result = new FetchFromTypeWithFilterStep(context);
    result.typeName = this.typeName;
    result.whereClause = this.whereClause;
    result.orderByRidAsc = this.orderByRidAsc;
    result.orderByRidDesc = this.orderByRidDesc;
    result.projectedProperties = this.projectedProperties;
    result.subSteps = this.subSteps.stream().map(x -> ((ExecutionStepInternal) x).copy(context)).collect(Collectors.toList());
    return result;
  }
}
