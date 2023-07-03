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

import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.FileUtils;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.util.stream.*;

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

  protected FetchFromClassExecutionStep(final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
  }

  public FetchFromClassExecutionStep(final String className, final Set<String> clusters, final CommandContext context, final Boolean ridOrder,
      final boolean profilingEnabled) {
    this(className, clusters, null, context, ridOrder, profilingEnabled);
  }

  /**
   * iterates over a class and its subTypes
   *
   * @param className the class name
   * @param clusters  if present (it can be null), filter by only these clusters
   * @param context   the query context
   * @param ridOrder  true to sort by RID asc, false to sort by RID desc, null for no sort.
   */
  public FetchFromClassExecutionStep(final String className, final Set<String> clusters, final QueryPlanningInfo planningInfo, final CommandContext context,
      final Boolean ridOrder, final boolean profilingEnabled) {
    super(context, profilingEnabled);

    this.className = className;

    if (Boolean.TRUE.equals(ridOrder)) {
      orderByRidAsc = true;
    } else if (Boolean.FALSE.equals(ridOrder)) {
      orderByRidDesc = true;
    }
    final DocumentType type = context.getDatabase().getSchema().getType(className);
    if (type == null) {
      throw new CommandExecutionException("Type " + className + " not found");
    }

    final int[] typeBuckets = type.getBuckets(true).stream().mapToInt(x -> x.getFileId()).distinct().sorted().toArray();
    final List<Integer> filteredTypeBuckets = new ArrayList<>();
    for (final int bucketId : typeBuckets) {
      final String bucketName = context.getDatabase().getSchema().getBucketById(bucketId).getName();
      if (clusters == null || clusters.contains(bucketName) || clusters.contains("*")) {
        filteredTypeBuckets.add(bucketId);
      }
    }
    final int[] bucketIds = new int[filteredTypeBuckets.size() + 1];
    for (int i = 0; i < filteredTypeBuckets.size(); i++) {
      bucketIds[i] = filteredTypeBuckets.get(i);
    }
    bucketIds[bucketIds.length - 1] = -1;//temporary bucket, data in tx

    long typeFileSize = 0;
    for (final int fileId : bucketIds) {
      if (fileId > -1) {
        final PaginatedComponentFile f = (PaginatedComponentFile) context.getDatabase().getFileManager().getFile(fileId);
        if (f != null) {
          try {
            typeFileSize += f.getSize();
          } catch (final IOException e) {
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
    for (final int bucketId : bucketIds) {
      if (bucketId > 0) {
        final FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(bucketId, planningInfo, context, profilingEnabled);
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
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

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
            currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep)).syncPull(context, nRecords);
            if (!currentResultSet.hasNext()) {
              currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep++)).syncPull(context, nRecords);
            }
          }
        }
      }

      @Override
      public Result next() {
        while (true) {
          if (totDispatched >= nRecords) {
            throw new NoSuchElementException();
          }
          if (currentResultSet != null && currentResultSet.hasNext()) {
            totDispatched++;
            final Result result = currentResultSet.next();
            context.setVariable("current", result);
            return result;
          } else {
            if (currentStep >= getSubSteps().size()) {
              throw new NoSuchElementException();
            }
            currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep)).syncPull(context, nRecords);
            if (!currentResultSet.hasNext()) {
              currentResultSet = ((AbstractExecutionStep) getSubSteps().get(currentStep++)).syncPull(context, nRecords);
            }
          }
        }
      }

      @Override
      public void close() {
        for (final ExecutionStep step : getSubSteps()) {
          ((AbstractExecutionStep) step).close();
        }
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
    for (final ExecutionStep step : getSubSteps()) {
      ((AbstractExecutionStep) step).close();
    }

    if (prev != null)
      prev.close();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = ExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM TYPE ").append(className);
    if (profilingEnabled) {
      builder.append(" (").append(getCostFormatted()).append(")");
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

  @Override
  public long getCost() {
    return subSteps.stream().map(ExecutionStep::getCost).reduce((a, b) -> a > 0 && b > 0 ? a + b : a > 0 ? a : b > 0 ? b : -1L).orElse(-1L);
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
    final FetchFromClassExecutionStep result = new FetchFromClassExecutionStep(context, profilingEnabled);
    result.className = this.className;
    result.orderByRidAsc = this.orderByRidAsc;
    result.orderByRidDesc = this.orderByRidDesc;
    result.subSteps = this.subSteps.stream().map(x -> ((ExecutionStepInternal) x).copy(context)).collect(Collectors.toList());
    return result;
  }
}
