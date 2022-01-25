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

import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.FileUtils;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class FetchFromClassExecutionStep extends AbstractExecutionStep {

  private String              className;
  private boolean             orderByRidAsc  = false;
  private boolean             orderByRidDesc = false;
  private List<ExecutionStep> subSteps       = new ArrayList<>();

  ResultSet currentResultSet;
  int       currentStep = 0;

  protected FetchFromClassExecutionStep(final CommandContext ctx, final boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  public FetchFromClassExecutionStep(final String className, final Set<String> clusters, final CommandContext ctx, final Boolean ridOrder,
      final boolean profilingEnabled) {
    this(className, clusters, null, ctx, ridOrder, profilingEnabled);
  }

  /**
   * iterates over a class and its subTypes
   *
   * @param className the class name
   * @param clusters  if present (it can be null), filter by only these clusters
   * @param ctx       the query context
   * @param ridOrder  true to sort by RID asc, false to sort by RID desc, null for no sort.
   */
  public FetchFromClassExecutionStep(final String className, final Set<String> clusters, final QueryPlanningInfo planningInfo, final CommandContext ctx,
      final Boolean ridOrder, final boolean profilingEnabled) {
    super(ctx, profilingEnabled);

    this.className = className;

    if (Boolean.TRUE.equals(ridOrder)) {
      orderByRidAsc = true;
    } else if (Boolean.FALSE.equals(ridOrder)) {
      orderByRidDesc = true;
    }
    final DocumentType type = ctx.getDatabase().getSchema().getType(className);
    if (type == null) {
      throw new CommandExecutionException("Type " + className + " not found");
    }

    final int[] typeBuckets = type.getBuckets(true).stream().mapToInt(x -> x.getId()).distinct().sorted().toArray();
    List<Integer> filteredTypeBuckets = new ArrayList<>();
    for (int bucketId : typeBuckets) {
      String bucketName = ctx.getDatabase().getSchema().getBucketById(bucketId).getName();
      if (clusters == null || clusters.contains(bucketName) || clusters.contains("*")) {
        filteredTypeBuckets.add(bucketId);
      }
    }
    int[] bucketIds = new int[filteredTypeBuckets.size() + 1];
    for (int i = 0; i < filteredTypeBuckets.size(); i++) {
      bucketIds[i] = filteredTypeBuckets.get(i);
    }
    bucketIds[bucketIds.length - 1] = -1;//temporary bucket, data in tx

    long typeFileSize = 0;
    for (final int fileId : bucketIds) {
      if (fileId > -1) {
        final PaginatedFile f = ctx.getDatabase().getFileManager().getFile(fileId);
        if (f != null) {
          try {
            typeFileSize += f.getSize();
          } catch (IOException e) {
            // IGNORE IT
          }
        }
      }
    }

    if (typeFileSize > 100_000_000) {
      LogManager.instance()
          .log(this, Level.WARNING, "Attempt to scan type '%s' of total size %s. This operation is very expensive, consider using an index", className,
              FileUtils.getSizeAsString(typeFileSize));
    }

    sortBuckets(bucketIds);
    for (int bucketId : bucketIds) {
      if (bucketId > 0) {
        FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(bucketId, planningInfo, ctx, profilingEnabled);
        if (orderByRidAsc) {
          step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
        } else if (orderByRidDesc) {
          step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
        }
        getSubSteps().add(step);
      }
    }
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
  public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new ResultSet() {

      int totDispatched = 0;

      @Override
      public boolean hasNext() {
        while (true) {
          if (totDispatched >= nRecords) {
            return false;
          }
          if (currentResultSet != null && currentResultSet.hasNext()) {
            return true;
          } else {
            if (currentStep >= getSubSteps().size()) {
              return false;
            }
            currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep)).syncPull(ctx, nRecords);
            if (!currentResultSet.hasNext()) {
              currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep++)).syncPull(ctx, nRecords);
            }
          }
        }
      }

      @Override
      public Result next() {
        while (true) {
          if (totDispatched >= nRecords) {
            throw new IllegalStateException();
          }
          if (currentResultSet != null && currentResultSet.hasNext()) {
            totDispatched++;
            Result result = currentResultSet.next();
            ctx.setVariable("$current", result);
            return result;
          } else {
            if (currentStep >= getSubSteps().size()) {
              throw new IllegalStateException();
            }
            currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep)).syncPull(ctx, nRecords);
            if (!currentResultSet.hasNext()) {
              currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep++)).syncPull(ctx, nRecords);
            }
          }
        }
      }

      @Override
      public void close() {
        for (ExecutionStep step : getSubSteps()) {
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
    for (ExecutionStep step : getSubSteps()) {
      ((AbstractExecutionStep) step).sendTimeout();
    }
    prev.ifPresent(p -> p.sendTimeout());
  }

  @Override
  public void close() {
    for (ExecutionStep step : getSubSteps()) {
      ((AbstractExecutionStep) step).close();
    }
    prev.ifPresent(p -> p.close());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder builder = new StringBuilder();
    String ind = ExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM TYPE ").append(className);
    if (profilingEnabled) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    builder.append("\n");
    for (int i = 0; i < getSubSteps().size(); i++) {
      ExecutionStepInternal step = (ExecutionStepInternal) getSubSteps().get(i);
      builder.append(step.prettyPrint(depth + 1, indent));
      if (i < getSubSteps().size() - 1) {
        builder.append("\n");
      }
    }
    return builder.toString();
  }

  @Override
  public long getCost() {
    return getSubSteps().stream().map(x -> x.getCost()).reduce((a, b) -> a + b).orElse(0L);
  }

  @Override
  public Result serialize() {
    ResultInternal result = ExecutionStepInternal.basicSerialize(this);
    result.setProperty("typeName", className);
    result.setProperty("orderByRidAsc", orderByRidAsc);
    result.setProperty("orderByRidDesc", orderByRidDesc);
    return result;
  }

  @Override
  public void deserialize(Result fromResult) {
    try {
      ExecutionStepInternal.basicDeserialize(fromResult, this);
      this.className = fromResult.getProperty("typeName");
      this.orderByRidAsc = fromResult.getProperty("orderByRidAsc");
      this.orderByRidDesc = fromResult.getProperty("orderByRidDesc");
    } catch (Exception e) {
      throw new CommandExecutionException("", e);
    }
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
  public ExecutionStep copy(CommandContext ctx) {
    FetchFromClassExecutionStep result = new FetchFromClassExecutionStep(ctx, profilingEnabled);
    result.className = this.className;
    result.orderByRidAsc = this.orderByRidAsc;
    result.orderByRidDesc = this.orderByRidDesc;
    result.subSteps = this.subSteps.stream().map(x -> ((ExecutionStepInternal) x).copy(ctx)).collect(Collectors.toList());
    return result;
  }
}
