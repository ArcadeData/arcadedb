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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.parser.Batch;
import com.arcadedb.query.sql.parser.Bucket;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.Limit;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.Timeout;
import com.arcadedb.query.sql.parser.UpdateItem;
import com.arcadedb.query.sql.parser.UpdateOperations;
import com.arcadedb.query.sql.parser.UpdateStatement;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.LocalTimeSeriesType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class UpdateExecutionPlanner {
  private final   FromClause             target;
  public final    WhereClause            whereClause;
  protected final boolean                upsert;
  protected final boolean                applyDefaults;
  protected final List<UpdateOperations> operations;
  protected final boolean                returnBefore;
  protected final boolean                returnAfter;
  protected final boolean                returnCount;
  protected final Projection             returnProjection;
  public final    Limit                  limit;
  public final    Batch                  batch;
  public final    Timeout                timeout;
  private final   DmlSourcePlanKey       sourcePlanKey;

  public UpdateExecutionPlanner(final UpdateStatement oUpdateStatement) {

    this.sourcePlanKey = oUpdateStatement.getSourcePlanKey();
    this.target = oUpdateStatement.getTarget().copy();
    this.whereClause = oUpdateStatement.getWhereClause() == null ? null : oUpdateStatement.getWhereClause().copy();
    this.operations =
        oUpdateStatement.getOperations() == null ?
            null :
            oUpdateStatement.getOperations().stream().map(x -> x.copy()).collect(Collectors.toList());
    this.upsert = oUpdateStatement.isUpsert();
    this.applyDefaults = oUpdateStatement.isApplyDefaults();

    this.returnBefore = oUpdateStatement.isReturnBefore();
    this.returnAfter = oUpdateStatement.isReturnAfter();
    this.returnCount = !(returnAfter || returnBefore);
    this.returnProjection = oUpdateStatement.getReturnProjection() == null ? null : oUpdateStatement.getReturnProjection().copy();
    this.limit = oUpdateStatement.getLimit() == null ? null : oUpdateStatement.getLimit().copy();
    this.batch = oUpdateStatement.getBatch();
    this.timeout = oUpdateStatement.getTimeout() == null ? null : oUpdateStatement.getTimeout().copy();
  }

  public UpdateExecutionPlan createExecutionPlan(final CommandContext context) {
    refuseTimeSeriesTarget(target, context, "UPDATE");
    if (upsert)
      UpsertStep.refuseEdgeType(target, context);
    final int limitValue = limit != null ? limit.getValue(context) : 0;
    final UpdateExecutionPlan result = new UpdateExecutionPlan(context, limitValue);
    // Taken from the statement as written, before the source plan is built from it (issue #12593)
    final SubquerySourceSelection sourceSelection = SubquerySourceSelection.of(target, whereClause, context);

    handleTarget(result, context, this.target, this.whereClause, this.timeout, limitValue);

    handleUpsert(result, context, this.target, this.whereClause, this.upsert);
    handleTimeout(result, context, this.timeout);
    convertToModifiableResult(result, context, sourceSelection);
    handleLimit(result, context, this.limit);
    handleReturnBefore(result, context, this.returnBefore);
    handleOperations(result, context, this.operations);
    handleApplyDefaults(result, context, this.applyDefaults);
    handleSave(result, target.getItem().getBucket(), context);
    handleBatch(result, context, this.batch);
    handleResultForReturnBefore(result, context, this.returnBefore, returnProjection);
    handleResultForReturnAfter(result, context, this.returnAfter, returnProjection);
    handleResultForReturnCount(result, context, this.returnCount);
    return result;
  }

  /**
   * add a step that transforms a normal OResult in a specific object that under setProperty() updates the actual PIdentifiable
   * <p>
   * The step evaluates the WHERE again on a record a concurrent commit changed after the source read it (issue #11818). It gets its
   * own copy: the source plan is built from this one.
   *
   * @param plan            the execution plan
   * @param context         the execution context
   * @param sourceSelection the predicates of a SELECT source, evaluated again on the record a row of it names (issue #12593), or
   *                        null
   */
  private void convertToModifiableResult(final UpdateExecutionPlan plan, final CommandContext context,
      final SubquerySourceSelection sourceSelection) {
    plan.chain(new ConvertToUpdatableResultStep(context, whereClause != null ? whereClause.copy() : null, upsert, sourceSelection));
  }

  private void handleResultForReturnCount(final UpdateExecutionPlan result, final CommandContext context,
      final boolean returnCount) {
    if (returnCount) {
      result.chain(new CountStep(context));
    }
  }

  private void handleResultForReturnAfter(final UpdateExecutionPlan result, final CommandContext context, final boolean returnAfter,
      final Projection returnProjection) {
    if (returnAfter) {
      //re-convert to normal step
      result.chain(new ConvertToResultInternalStep(context));
      if (returnProjection != null) {
        result.chain(new ProjectionCalculationStep(returnProjection, context));
      }
    }
  }

  private void handleResultForReturnBefore(final UpdateExecutionPlan result, final CommandContext context,
      final boolean returnBefore,
      final Projection returnProjection) {
    if (returnBefore) {
      result.chain(new UnwrapPreviousValueStep(context));
      if (returnProjection != null) {
        result.chain(new ProjectionCalculationStep(returnProjection, context));
      }
    }
  }

  private void handleSave(final UpdateExecutionPlan result, final Bucket bucket, final CommandContext context) {
    if (bucket != null) {
      // a name, an id or an input parameter (bucket::name, bucket:?) is resolved as the read side of the statement resolves it (#12480)
      String bucketName = bucket.resolveName(context.getInputParameters());
      if (bucketName == null && bucket.getBucketNumber() != null)
        bucketName = context.getDatabase().getSchema().getBucketById(bucket.getBucketNumber()).getName();
      if (bucketName == null)
        throw new SchemaException("Bucket '" + bucket + "' does not exist");
      result.chain(new SaveElementStep(context, new Identifier(bucketName), false));
    } else
      result.chain(new SaveElementStep(context, null, false));
  }

  private void handleTimeout(final UpdateExecutionPlan result, final CommandContext context, final Timeout timeout) {
    final TimeoutStep step = StatementTimeouts.stepFor(timeout, context);
    if (step != null)
      result.chain(step);
  }

  private void handleReturnBefore(final UpdateExecutionPlan result, final CommandContext context, final boolean returnBefore) {
    if (returnBefore)
      result.chain(new CopyRecordContentBeforeUpdateStep(context));
  }

  private void handleLimit(final UpdateExecutionPlan plan, final CommandContext context, final Limit limit) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit, context));
    }
  }

  private void handleBatch(final UpdateExecutionPlan plan, final CommandContext context, final Batch batch) {
    if (batch != null)
      plan.chain(new BatchStep(batch, context));
  }

  private void handleUpsert(final UpdateExecutionPlan plan, final CommandContext context, final FromClause target,
      final WhereClause where,
      final boolean upsert) {
    if (upsert) {
      plan.chain(new UpsertStep(target, where, context));
    }
  }

  private void handleApplyDefaults(final UpdateExecutionPlan plan, final CommandContext context,
      final boolean applyDefaults) {
    if (applyDefaults) {
      plan.chain(ApplyDefaultsStep.forUpdate(context));
    }
  }

  private void handleOperations(final UpdateExecutionPlan plan, final CommandContext context, final List<UpdateOperations> ops) {
    if (ops != null) {
      for (final UpdateOperations op : ops) {
        switch (op.getType()) {
        case UpdateOperations.TYPE_SET:
          plan.chain(UpdateSetStep.forExistingRecord(op.getUpdateItems(), context));
          break;
        case UpdateOperations.TYPE_REMOVE:
          plan.chain(new UpdateRemoveStep(op.getUpdateRemoveItems(), context));
          break;
        case UpdateOperations.TYPE_MERGE:
          if (op.getJson() != null)
            plan.chain(new UpdateMergeStep(op.getJson(), context));
          else if (op.getExpression() != null)
            plan.chain(new UpdateMergeStep(op.getExpression(), context));
          else
            throw new CommandExecutionException("Missing payload for UPDATE MERGE: " + op);
          break;
        case UpdateOperations.TYPE_CONTENT:
          plan.chain(UpdateContentStep.of(op, "UPDATE", context));
          break;
        case UpdateOperations.TYPE_PUT:
        case UpdateOperations.TYPE_INCREMENT:
        case UpdateOperations.TYPE_ADD:
          throw new CommandExecutionException("Cannot execute with UPDATE PUT/ADD/INCREMENT new executor: " + op);
        }
      }
    }
  }

  /**
   * Refuses a {@code DELETE} or {@code UPDATE} that names a TIMESERIES type (issue #10336). Its rows are samples read from the
   * engine, not records, so the statement found nothing it could delete or save: DELETE counted every matching row as deleted, UPDATE
   * reported 0, and both kept every sample.
   */
  static void refuseTimeSeriesTarget(final FromClause target, final CommandContext context, final String statement) {
    final Identifier identifier = target != null && target.getItem() != null ? target.getItem().getIdentifier() : null;
    if (identifier != null && context.getDatabase().getSchema()
        .getTypeOrNull(identifier.getStringValue()) instanceof LocalTimeSeriesType tsType)
      throw new CommandExecutionException(tsType.immutableSamplesRefusal(statement));
  }

  private void handleTarget(final UpdateExecutionPlan result, final CommandContext context, final FromClause target,
      final WhereClause whereClause, final Timeout timeout, final int limitValue) {
    final InternalExecutionPlan sourcePlan = SelectExecutionPlanner.createSourcePlan(target, whereClause, timeout, sourcePlanKey, context);
    result.chain(new SubQueryStep(sourcePlan, context, context));

    // #8814 (Halloween problem): a statement that rewrites a key it walks must not read its own output, so the addresses
    // are read first. An UPSERT is an equality lookup on a unique index, which cannot walk a range, and UpsertStep looks
    // for that index in the step right before it.
    if (!upsert && operations != null && !operations.isEmpty() && readsIndex(sourcePlan, modifiedProperties()))
      result.chain(new MaterializeRecordsStep(context, limitValue));
  }

  /**
   * @return the properties the operations set, or null when they can touch any (REMOVE, CONTENT, MERGE, SET on a path)
   */
  private Set<String> modifiedProperties() {
    final Set<String> properties = new HashSet<>();
    for (final UpdateOperations op : operations) {
      if (op.getType() != UpdateOperations.TYPE_SET || op.getUpdateItems() == null)
        return null;
      for (final UpdateItem item : op.getUpdateItems()) {
        if (item.getLeft() == null || item.getLeftModifier() != null)
          return null;
        properties.add(item.getLeft().getStringValue());
      }
    }
    return properties;
  }

  private static boolean readsIndex(final ExecutionPlan plan, final Set<String> modified) {
    for (final ExecutionStep step : plan.getSteps())
      if (readsIndex(step, modified))
        return true;
    return false;
  }

  private static boolean readsIndex(final ExecutionStep step, final Set<String> modified) {
    if (step instanceof final FetchFromIndexStep indexStep) {
      if (modified == null)
        return true;
      for (final String property : indexStep.index.getPropertyNames())
        if (modified.contains(Index.basePropertyName(property)))
          return true;
    } else if (step instanceof FetchFromIndexedFunctionStep)
      return true;

    for (final ExecutionStep subStep : step.getSubSteps())
      if (readsIndex(subStep, modified))
        return true;
    if (step instanceof final ExecutionStepInternal internal)
      for (final ExecutionPlan subPlan : internal.getSubExecutionPlans())
        if (readsIndex(subPlan, modified))
          return true;
    return false;
  }
}
