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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.*;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class FetchFromClusterExecutionStep extends AbstractExecutionStep {

  public static final Object            ORDER_ASC  = "ASC";
  public static final Object            ORDER_DESC = "DESC";
  private final       QueryPlanningInfo queryPlanning;

  private int    bucketId;
  private Object order;

  private Iterator<Record> iterator;
  private long             cost = 0;

  public FetchFromClusterExecutionStep(int bucketId, CommandContext ctx, boolean profilingEnabled) {
    this(bucketId, null, ctx, profilingEnabled);
  }

  public FetchFromClusterExecutionStep(int bucketId, QueryPlanningInfo queryPlanning, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.bucketId = bucketId;
    this.queryPlanning = queryPlanning;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (iterator == null) {
        iterator = ctx.getDatabase().getSchema().getBucketById(bucketId).iterator();

        //TODO check how to support ranges and DESC
//        long minClusterPosition = calculateMinClusterPosition();
//        long maxClusterPosition = calculateMaxClusterPosition();
//            new ORecordIteratorCluster((ODatabaseDocumentInternal) ctx.getDatabase(),
//            (ODatabaseDocumentInternal) ctx.getDatabase(), bucketId, minClusterPosition, maxClusterPosition);
//        if (ORDER_DESC == order) {
//          iterator.last();
//        }
      }
      return new ResultSet() {

        int nFetched = 0;

        @Override
        public boolean hasNext() {
          long begin1 = profilingEnabled ? System.nanoTime() : 0;
          try {
            if (nFetched >= nRecords) {
              return false;
            }
            //TODO
//            if (ORDER_DESC == order) {
//              return iterator.hasPrevious();
//            } else {
            return iterator.hasNext();
//            }
          } finally {
            if (profilingEnabled) {
              cost += (System.nanoTime() - begin1);
            }
          }
        }

        @Override
        public Result next() {
          long begin1 = profilingEnabled ? System.nanoTime() : 0;
          try {
            if (nFetched >= nRecords) {
              throw new IllegalStateException();
            }
//            if (ORDER_DESC == order && !iterator.hasPrevious()) {
//              throw new IllegalStateException();
//            } else
            if (!iterator.hasNext()) {
              throw new IllegalStateException();
            }

            Record record = null;
//            if (ORDER_DESC == order) {
//              record = iterator.previous();
//            } else {
            record = iterator.next();
//            }
            nFetched++;
            ResultInternal result = new ResultInternal();
            result.element = (Document) record;
            ctx.setVariable("$current", result);
            return result;
          } finally {
            if (profilingEnabled) {
              cost += (System.nanoTime() - begin1);
            }
          }
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
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }

  }

  private long calculateMinClusterPosition() {
    if (queryPlanning == null || queryPlanning.ridRangeConditions == null || queryPlanning.ridRangeConditions.isEmpty()) {
      return -1;
    }

    long maxValue = -1;

    for (BooleanExpression ridRangeCondition : queryPlanning.ridRangeConditions.getSubBlocks()) {
      if (ridRangeCondition instanceof BinaryCondition) {
        BinaryCondition cond = (BinaryCondition) ridRangeCondition;
        Rid condRid = cond.getRight().getRid();
        BinaryCompareOperator operator = cond.getOperator();
        if (condRid != null) {
          if (condRid.getBucket().getValue().intValue() != this.bucketId) {
            continue;
          }
          if (operator instanceof GtOperator || operator instanceof GeOperator) {
            maxValue = Math.max(maxValue, condRid.getPosition().getValue().longValue());
          }
        }
      }
    }

    return maxValue;
  }

  private long calculateMaxClusterPosition() {
    if (queryPlanning == null || queryPlanning.ridRangeConditions == null || queryPlanning.ridRangeConditions.isEmpty()) {
      return -1;
    }
    long minValue = Long.MAX_VALUE;

    for (BooleanExpression ridRangeCondition : queryPlanning.ridRangeConditions.getSubBlocks()) {
      if (ridRangeCondition instanceof BinaryCondition) {
        BinaryCondition cond = (BinaryCondition) ridRangeCondition;
        RID conditionRid;

        Object obj;
        if (((BinaryCondition) ridRangeCondition).getRight().getRid() != null) {
          obj = ((BinaryCondition) ridRangeCondition).getRight().getRid().toRecordId((Result) null, ctx);
        } else {
          obj = ((BinaryCondition) ridRangeCondition).getRight().execute((Result) null, ctx);
        }

        conditionRid = ((Identifiable) obj).getIdentity();
        BinaryCompareOperator operator = cond.getOperator();
        if (conditionRid != null) {
          if (conditionRid.getBucketId() != this.bucketId) {
            continue;
          }
          if (operator instanceof LtOperator || operator instanceof LeOperator) {
            minValue = Math.min(minValue, conditionRid.getPosition());
          }
        }
      }
    }

    return minValue == Long.MAX_VALUE ? -1 : minValue;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result =
        ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM BUCKET " + bucketId + " (" + ctx.getDatabase().getSchema().getBucketById(bucketId)
            .getName() + ") " + (ORDER_DESC.equals(order) ? "DESC" : "ASC");
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  public void setOrder(Object order) {
    this.order = order;
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public Result serialize() {
    ResultInternal result = ExecutionStepInternal.basicSerialize(this);
    result.setProperty("bucketId", bucketId);
    result.setProperty("order", order);
    return result;
  }

  @Override
  public void deserialize(Result fromResult) {
    try {
      ExecutionStepInternal.basicDeserialize(fromResult, this);
      this.bucketId = fromResult.getProperty("bucketId");
      Object orderProp = fromResult.getProperty("order");
      if (orderProp != null) {
        this.order = ORDER_ASC.equals(fromResult.getProperty("order")) ? ORDER_ASC : ORDER_DESC;
      }
    } catch (Exception e) {
      throw new CommandExecutionException(e);
    }
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(CommandContext ctx) {
    return new FetchFromClusterExecutionStep(this.bucketId, this.queryPlanning == null ? null : this.queryPlanning.copy(), ctx,
        profilingEnabled);
  }
}
