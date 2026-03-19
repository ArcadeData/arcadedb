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

import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;

import java.util.*;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class FetchFromClusterExecutionStep extends AbstractExecutionStep {

  public static final Object            ORDER_ASC    = "ASC";
  public static final Object            ORDER_DESC   = "DESC";
  private final       QueryPlanningInfo queryPlanning;
  private final       int               bucketId;
  private final       Set<String>       projectedProperties;
  private             Object            order;
  private             long              totalFetched = 0L;

  private Iterator<Record> iterator;

  public FetchFromClusterExecutionStep(final int bucketId, final CommandContext context) {
    this(bucketId, null, null, context);
  }

  public FetchFromClusterExecutionStep(final int bucketId, final QueryPlanningInfo queryPlanning, final Object order,
      final CommandContext context) {
    super(context);
    this.bucketId = bucketId;
    this.queryPlanning = queryPlanning;
    this.projectedProperties = queryPlanning != null ? queryPlanning.projectedProperties : null;
    this.order = order;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      if (iterator == null) {
        if (order == ORDER_DESC)
          iterator = context.getDatabase().getSchema().getBucketById(bucketId).inverseIterator();
        else
          iterator = context.getDatabase().getSchema().getBucketById(bucketId).iterator();

        //TODO check how to support ranges
//        long minClusterPosition = calculateMinClusterPosition();
//        long maxClusterPosition = calculateMaxClusterPosition();
//            new ORecordIteratorCluster((ODatabaseDocumentInternal) context.getDatabase(),
//            (ODatabaseDocumentInternal) context.getDatabase(), bucketId, minClusterPosition, maxClusterPosition);
      }
      return new ResultSet() {
        int nFetched = 0;

        @Override
        public boolean hasNext() {
          final long begin1 = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (nFetched >= nRecords)
              return false;

            //TODO
//            if (ORDER_DESC == order) {
//              return iterator.hasPrevious();
//            } else {
            return iterator.hasNext();
//            }
          } finally {
            if (context.isProfiling()) {
              cost += (System.nanoTime() - begin1);
            }
          }
        }

        @Override
        public Result next() {
          final long begin1 = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (nFetched >= nRecords)
              throw new NoSuchElementException();

//            if (ORDER_DESC == order && !iterator.hasPrevious()) {
//              throw new NoSuchElementException();
//            } else
            if (!iterator.hasNext()) {
              throw new NoSuchElementException();
            }

            Record record = null;
//            if (ORDER_DESC == order) {
//              record = iterator.previous();
//            } else {
            record = iterator.next();
//            }
            ++nFetched;
            ++totalFetched;

            final ResultInternal result = new ResultInternal(record);
            context.setVariable("current", result);

            return result;

          } finally {
            if (context.isProfiling()) {
              cost += (System.nanoTime() - begin1);
            }
          }
        }
      };
    } finally {
      if (context.isProfiling()) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

//  private long calculateMinClusterPosition() {
//    if (queryPlanning == null || queryPlanning.ridRangeConditions == null || queryPlanning.ridRangeConditions.isEmpty()) {
//      return -1;
//    }
//
//    long maxValue = -1;
//
//    for (final BooleanExpression ridRangeCondition : queryPlanning.ridRangeConditions.getSubBlocks()) {
//      if (ridRangeCondition instanceof BinaryCondition) {
//        final BinaryCondition cond = (BinaryCondition) ridRangeCondition;
//        final Rid condRid = cond.getRight().getRid();
//        final BinaryCompareOperator operator = cond.getOperator();
//        if (condRid != null) {
//          if (condRid.getBucket().getValue().intValue() != this.bucketId) {
//            continue;
//          }
//          if (operator instanceof GtOperator || operator instanceof GeOperator) {
//            maxValue = Math.max(maxValue, condRid.getPosition().getValue().longValue());
//          }
//        }
//      }
//    }
//
//    return maxValue;
//  }
//
//  private long calculateMaxClusterPosition() {
//    if (queryPlanning == null || queryPlanning.ridRangeConditions == null || queryPlanning.ridRangeConditions.isEmpty()) {
//      return -1;
//    }
//    long minValue = Long.MAX_VALUE;
//
//    for (final BooleanExpression ridRangeCondition : queryPlanning.ridRangeConditions.getSubBlocks()) {
//      if (ridRangeCondition instanceof BinaryCondition) {
//        final BinaryCondition cond = (BinaryCondition) ridRangeCondition;
//        final RID conditionRid;
//
//        final Object obj;
//        if (((BinaryCondition) ridRangeCondition).getRight().getRid() != null) {
//          obj = ((BinaryCondition) ridRangeCondition).getRight().getRid().toRecordId((Result) null, context);
//        } else {
//          obj = ((BinaryCondition) ridRangeCondition).getRight().execute((Result) null, context);
//        }
//
//        conditionRid = ((Identifiable) obj).getIdentity();
//        final BinaryCompareOperator operator = cond.getOperator();
//        if (conditionRid != null) {
//          if (conditionRid.getBucketId() != this.bucketId) {
//            continue;
//          }
//          if (operator instanceof LtOperator || operator instanceof LeOperator) {
//            minValue = Math.min(minValue, conditionRid.getPosition());
//          }
//        }
//      }
//    }
//
//    return minValue == Long.MAX_VALUE ? -1 : minValue;
//  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result =
        ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM BUCKET " + bucketId + " (" + context.getDatabase()
            .getSchema().getBucketById(bucketId)
            .getName() + ") " + (ORDER_DESC.equals(order) ? "DESC" : "ASC" + " = " + totalFetched + " RECORDS");
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    return result;
  }

  public void setOrder(final Object order) {
    this.order = order;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new FetchFromClusterExecutionStep(this.bucketId, this.queryPlanning == null ? null : this.queryPlanning.copy(),
        this.order, context);
  }
}
