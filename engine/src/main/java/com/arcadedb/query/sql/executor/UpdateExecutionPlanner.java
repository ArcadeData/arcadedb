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
import com.arcadedb.query.sql.parser.Bucket;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.Limit;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Timeout;
import com.arcadedb.query.sql.parser.UpdateEdgeStatement;
import com.arcadedb.query.sql.parser.UpdateOperations;
import com.arcadedb.query.sql.parser.UpdateStatement;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.*;
import java.util.stream.*;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class UpdateExecutionPlanner {
  private final   FromClause             target;
  public final    WhereClause            whereClause;
  protected final boolean                upsert;
  protected final List<UpdateOperations> operations;
  protected final boolean                returnBefore;
  protected final boolean                returnAfter;
  protected final boolean                returnCount;
  protected       boolean                updateEdge = false;
  protected final Projection             returnProjection;
  public final    Limit                  limit;
  public final    Timeout                timeout;

  public UpdateExecutionPlanner(final UpdateStatement oUpdateStatement) {
    if (oUpdateStatement instanceof UpdateEdgeStatement) {
      updateEdge = true;
    }
    this.target = oUpdateStatement.getTarget().copy();
    this.whereClause = oUpdateStatement.getWhereClause() == null ? null : oUpdateStatement.getWhereClause().copy();
    this.operations =
        oUpdateStatement.getOperations() == null ?
            null :
            oUpdateStatement.getOperations().stream().map(x -> x.copy()).collect(Collectors.toList());
    this.upsert = oUpdateStatement.isUpsert();

    this.returnBefore = oUpdateStatement.isReturnBefore();
    this.returnAfter = oUpdateStatement.isReturnAfter();
    this.returnCount = !(returnAfter || returnBefore);
    this.returnProjection = oUpdateStatement.getReturnProjection() == null ? null : oUpdateStatement.getReturnProjection().copy();
    this.limit = oUpdateStatement.getLimit() == null ? null : oUpdateStatement.getLimit().copy();
    this.timeout = oUpdateStatement.getTimeout() == null ? null : oUpdateStatement.getTimeout().copy();
  }

  public UpdateExecutionPlan createExecutionPlan(final CommandContext context, final boolean enableProfiling) {
    final UpdateExecutionPlan result = new UpdateExecutionPlan(context);

    handleTarget(result, context, this.target, this.whereClause, this.timeout, enableProfiling);
    if (updateEdge) {
      result.chain(new CheckRecordTypeStep(context, "E", enableProfiling));
    }
    handleUpsert(result, context, this.target, this.whereClause, this.upsert, enableProfiling);
    handleTimeout(result, context, this.timeout, enableProfiling);
    convertToModifiableResult(result, context, enableProfiling);
    handleLimit(result, context, this.limit, enableProfiling);
    handleReturnBefore(result, context, this.returnBefore, enableProfiling);
    handleOperations(result, context, this.operations, enableProfiling);
    handleSave(result, target.getItem().getBucket(), context, enableProfiling);
    handleResultForReturnBefore(result, context, this.returnBefore, returnProjection, enableProfiling);
    handleResultForReturnAfter(result, context, this.returnAfter, returnProjection, enableProfiling);
    handleResultForReturnCount(result, context, this.returnCount, enableProfiling);
    return result;
  }

  /**
   * add a step that transforms a normal OResult in a specific object that under setProperty() updates the actual PIdentifiable
   *
   * @param plan    the execution plan
   * @param context the execution context
   */
  private void convertToModifiableResult(final UpdateExecutionPlan plan, final CommandContext context,
      final boolean profilingEnabled) {
    plan.chain(new ConvertToUpdatableResultStep(context, profilingEnabled));
  }

  private void handleResultForReturnCount(final UpdateExecutionPlan result, final CommandContext context, final boolean returnCount,
      final boolean profilingEnabled) {
    if (returnCount) {
      result.chain(new CountStep(context, profilingEnabled));
    }
  }

  private void handleResultForReturnAfter(final UpdateExecutionPlan result, final CommandContext context, final boolean returnAfter,
      final Projection returnProjection, final boolean profilingEnabled) {
    if (returnAfter) {
      //re-convert to normal step
      result.chain(new ConvertToResultInternalStep(context, profilingEnabled));
      if (returnProjection != null) {
        result.chain(new ProjectionCalculationStep(returnProjection, context, profilingEnabled));
      }
    }
  }

  private void handleResultForReturnBefore(final UpdateExecutionPlan result, final CommandContext context,
      final boolean returnBefore,
      final Projection returnProjection, final boolean profilingEnabled) {
    if (returnBefore) {
      result.chain(new UnwrapPreviousValueStep(context, profilingEnabled));
      if (returnProjection != null) {
        result.chain(new ProjectionCalculationStep(returnProjection, context, profilingEnabled));
      }
    }
  }

  private void handleSave(final UpdateExecutionPlan result, final Bucket bucket, final CommandContext context,
      final boolean profilingEnabled) {
    if (bucket != null) {
      final String bucketName =
          bucket.getBucketName() != null ?
              bucket.getBucketName() :
              context.getDatabase().getSchema().getBucketById(bucket.getBucketNumber()).getName();
      result.chain(new SaveElementStep(context, new Identifier(bucketName), profilingEnabled));
    } else
      result.chain(new SaveElementStep(context, profilingEnabled));
  }

  private void handleTimeout(final UpdateExecutionPlan result, final CommandContext context, final Timeout timeout,
      final boolean profilingEnabled) {
    if (timeout != null && timeout.getVal().longValue() > 0) {
      result.chain(new TimeoutStep(timeout, context, profilingEnabled));
    }
  }

  private void handleReturnBefore(final UpdateExecutionPlan result, final CommandContext context, final boolean returnBefore,
      final boolean profilingEnabled) {
    if (returnBefore) {
      result.chain(new CopyRecordContentBeforeUpdateStep(context, profilingEnabled));
    }
  }

  private void handleLimit(final UpdateExecutionPlan plan, final CommandContext context, final Limit limit,
      final boolean profilingEnabled) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit, context, profilingEnabled));
    }
  }

  private void handleUpsert(final UpdateExecutionPlan plan, final CommandContext context, final FromClause target,
      final WhereClause where,
      final boolean upsert, final boolean profilingEnabled) {
    if (upsert) {
      plan.chain(new UpsertStep(target, where, context, profilingEnabled));
    }
  }

  private void handleOperations(final UpdateExecutionPlan plan, final CommandContext context, final List<UpdateOperations> ops,
      final boolean profilingEnabled) {
    if (ops != null) {
      for (final UpdateOperations op : ops) {
        switch (op.getType()) {
        case UpdateOperations.TYPE_SET:
          plan.chain(new UpdateSetStep(op.getUpdateItems(), context, profilingEnabled));
          //TODO: ARCADEDB MANAGES EDGES IN DIFFERENT WAY. DO WE NEED THIS?
          //if(updateEdge){
          //plan.chain(new UpdateEdgePointersStep( context, profilingEnabled));
          //}
          break;
        case UpdateOperations.TYPE_REMOVE:
          plan.chain(new UpdateRemoveStep(op.getUpdateRemoveItems(), context, profilingEnabled));
          break;
        case UpdateOperations.TYPE_MERGE:
          plan.chain(new UpdateMergeStep(op.getJson(), context, profilingEnabled));
          break;
        case UpdateOperations.TYPE_CONTENT:
          plan.chain(new UpdateContentStep(op.getJson(), context, profilingEnabled));
          break;
        case UpdateOperations.TYPE_PUT:
        case UpdateOperations.TYPE_INCREMENT:
        case UpdateOperations.TYPE_ADD:
          throw new CommandExecutionException("Cannot execute with UPDATE PUT/ADD/INCREMENT new executor: " + op);
        }
      }
    }
  }

  private void handleTarget(final UpdateExecutionPlan result, final CommandContext context, final FromClause target,
      final WhereClause whereClause,
      final Timeout timeout, final boolean profilingEnabled) {
    final SelectStatement sourceStatement = new SelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    if (timeout != null) {
      sourceStatement.setTimeout(this.timeout.copy());
    }
    final SelectExecutionPlanner planner = new SelectExecutionPlanner(sourceStatement);
    result.chain(
        new SubQueryStep(planner.createExecutionPlan(context, profilingEnabled, false), context, context, profilingEnabled));
  }
}
