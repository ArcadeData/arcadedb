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

import java.util.*;

/**
 * Created by luigidellaquila on 21/07/16.
 */
public class FetchFromClustersExecutionStep extends AbstractExecutionStep {

  final   List<ExecutionStep> subSteps;
  private boolean             orderByRidAsc  = false;
  private boolean             orderByRidDesc = false;
  ResultSet currentResultSet;
  int       currentStep = 0;

  /**
   * iterates over a class and its subTypes
   *
   * @param bucketIds the clusters
   * @param context   the query context
   * @param ridOrder  true to sort by RID asc, false to sort by RID desc, null for no sort.
   */
  public FetchFromClustersExecutionStep(final int[] bucketIds, final CommandContext context, final Boolean ridOrder, final boolean profilingEnabled) {
    super(context, profilingEnabled);

    if (Boolean.TRUE.equals(ridOrder))
      orderByRidAsc = true;
    else if (Boolean.FALSE.equals(ridOrder))
      orderByRidDesc = true;

    subSteps = new ArrayList<>();
    sort(bucketIds);
    for (final int bucketId : bucketIds) {
      final FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(bucketId, context, profilingEnabled);
      if (orderByRidAsc)
        step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
      else if (orderByRidDesc)
        step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);

      subSteps.add(step);
    }
  }

  private void sort(final int[] bucketIds) {
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

    return new ResultSet() {

      int totDispatched = 0;

      @Override
      public boolean hasNext() {
        while (true) {
          if (totDispatched >= nRecords)
            return false;

          if (currentResultSet == null || !currentResultSet.hasNext()) {
            if (currentStep >= subSteps.size())
              return false;

            currentResultSet = ((AbstractExecutionStep) subSteps.get(currentStep)).syncPull(context, nRecords);
            if (!currentResultSet.hasNext()) {
              currentResultSet = ((AbstractExecutionStep) subSteps.get(currentStep++)).syncPull(context, nRecords);
            }
          }

          if (currentResultSet.hasNext())
            return true;
        }
      }

      @Override
      public Result next() {
        while (true) {
          if (totDispatched >= nRecords)
            throw new NoSuchElementException();

          if (currentResultSet == null || !currentResultSet.hasNext()) {
            if (currentStep >= subSteps.size())
              throw new NoSuchElementException();

            currentResultSet = ((AbstractExecutionStep) subSteps.get(currentStep)).syncPull(context, nRecords);
            if (!currentResultSet.hasNext())
              currentResultSet = ((AbstractExecutionStep) subSteps.get(currentStep++)).syncPull(context, nRecords);
          }
          if (!currentResultSet.hasNext())
            continue;

          totDispatched++;
          return currentResultSet.next();
        }
      }

      @Override
      public void close() {
        for (final ExecutionStep step : subSteps) {
          ((AbstractExecutionStep) step).close();
        }
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return new HashMap<>();
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
    builder.append("+ FETCH FROM BUCKETS");
    if (profilingEnabled) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    builder.append("\n");
    for (int i = 0; i < subSteps.size(); i++) {
      final ExecutionStepInternal step = (ExecutionStepInternal) subSteps.get(i);
      builder.append(step.prettyPrint(depth + 1, indent));
      if (i < subSteps.size() - 1) {
        builder.append("\n");
      }
    }
    return builder.toString();
  }

  @Override
  public List<ExecutionStep> getSubSteps() {
    return subSteps;
  }

  @Override
  public long getCost() {
    return subSteps.stream().map(ExecutionStep::getCost).reduce((a, b) -> a > 0 && b > 0 ? a + b : a > 0 ? a : b > 0 ? b : -1L).orElse(-1L);
  }

}
