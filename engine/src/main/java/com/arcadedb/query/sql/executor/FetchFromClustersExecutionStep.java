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

import java.util.*;

/**
 * Created by luigidellaquila on 21/07/16.
 */
public class FetchFromClustersExecutionStep extends AbstractExecutionStep {

  final   List<ExecutionStep> subSteps;
  private boolean             orderByRidAsc  = false;
  private boolean orderByRidDesc = false;

  ResultSet currentResultSet;
  int       currentStep = 0;

  /**
   * iterates over a class and its subTypes
   *
   * @param bucketIds the clusters
   * @param ctx        the query context
   * @param ridOrder   true to sort by RID asc, false to sort by RID desc, null for no sort.
   */
  public FetchFromClustersExecutionStep(int[] bucketIds, CommandContext ctx, Boolean ridOrder, boolean profilingEnabled) {
    super(ctx, profilingEnabled);

    if (Boolean.TRUE.equals(ridOrder)) {
      orderByRidAsc = true;
    } else if (Boolean.FALSE.equals(ridOrder)) {
      orderByRidDesc = true;
    }

    subSteps = new ArrayList<>();
    sort(bucketIds);
    for (int bucketId : bucketIds) {
      FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(bucketId, ctx, profilingEnabled);
      if (orderByRidAsc) {
        step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
      } else if (orderByRidDesc) {
        step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
      }
      subSteps.add(step);
    }
  }

  private void sort(int[] bucketIds) {
    if (orderByRidAsc) {
      Arrays.sort(bucketIds);
    } else if (orderByRidDesc) {
      Arrays.sort(bucketIds);
      //revert order
      for (int i = 0; i < bucketIds.length / 2; i++) {
        int old = bucketIds[i];
        bucketIds[i] = bucketIds[bucketIds.length - 1 - i];
        bucketIds[bucketIds.length - 1 - i] = old;
      }
    }
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new ResultSet() {

      int totDispatched = 0;

      @Override
      public boolean hasNext() {
        while (true) {
          if (totDispatched >= nRecords) {
            return false;
          }
          if (currentResultSet == null || !currentResultSet.hasNext()) {
            if (currentStep >= subSteps.size()) {
              return false;
            }
            currentResultSet = ((AbstractExecutionStep) subSteps.get(currentStep)).syncPull(ctx, nRecords);
            if (!currentResultSet.hasNext()) {
              currentResultSet = ((AbstractExecutionStep) subSteps.get(currentStep++)).syncPull(ctx, nRecords);
            }
          }
          if (!currentResultSet.hasNext()) {
            continue;
          }
          return true;
        }
      }

      @Override
      public Result next() {
        while (true) {
          if (totDispatched >= nRecords) {
            throw new IllegalStateException();
          }
          if (currentResultSet == null || !currentResultSet.hasNext()) {
            if (currentStep >= subSteps.size()) {
              throw new IllegalStateException();
            }
            currentResultSet = ((AbstractExecutionStep) subSteps.get(currentStep)).syncPull(ctx, nRecords);
            if (!currentResultSet.hasNext()) {
              currentResultSet = ((AbstractExecutionStep) subSteps.get(currentStep++)).syncPull(ctx, nRecords);
            }
          }
          if (!currentResultSet.hasNext()) {
            continue;
          }
          totDispatched++;
          return currentResultSet.next();
        }
      }

      @Override
      public void close() {
        for (ExecutionStep step : subSteps) {
          ((AbstractExecutionStep) step).close();
        }
      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return new HashMap<>();
      }
    };

  }

  @Override
  public void sendTimeout() {
    for (ExecutionStep step : subSteps) {
      ((AbstractExecutionStep) step).sendTimeout();
    }
    prev.ifPresent(p -> p.sendTimeout());
  }

  @Override
  public void close() {
    for (ExecutionStep step : subSteps) {
      ((AbstractExecutionStep) step).close();
    }
    prev.ifPresent(p -> p.close());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder builder = new StringBuilder();
    String ind = ExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM BUCKETSS");
    if (profilingEnabled) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    builder.append("\n");
    for (int i = 0; i < subSteps.size(); i++) {
      ExecutionStepInternal step = (ExecutionStepInternal) subSteps.get(i);
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
    return subSteps.stream().map(x -> x.getCost()).reduce((a, b) -> a + b).orElse(-1L);
  }

  @Override
  public Result serialize() {
    ResultInternal result = ExecutionStepInternal.basicSerialize(this);
    result.setProperty("orderByRidAsc", orderByRidAsc);
    result.setProperty("orderByRidDesc", orderByRidDesc);
    return result;
  }

  @Override
  public void deserialize(Result fromResult) {
    try {
      ExecutionStepInternal.basicDeserialize(fromResult, this);
      this.orderByRidAsc = fromResult.getProperty("orderByRidAsc");
      this.orderByRidDesc = fromResult.getProperty("orderByRidDesc");
    } catch (Exception e) {
      throw new CommandExecutionException(e);
    }
  }
}
