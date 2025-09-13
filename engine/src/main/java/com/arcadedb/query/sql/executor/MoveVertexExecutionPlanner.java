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
import com.arcadedb.query.sql.parser.Batch;
import com.arcadedb.query.sql.parser.Bucket;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.MoveVertexStatement;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.UpdateOperations;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class MoveVertexExecutionPlanner {
  private final FromItem         source;
  private final Identifier       targetType;
  private final Bucket           targetBucket;
  private final UpdateOperations updateOperations;
  private final Batch            batch;

  public MoveVertexExecutionPlanner(MoveVertexStatement oStatement) {
    this.source = oStatement.getSource();
    this.targetType = oStatement.getTargetType();
    this.targetBucket = oStatement.getTargetBucket();
    this.updateOperations = oStatement.getUpdateOperations();
    this.batch = oStatement.getBatch();
  }

  public UpdateExecutionPlan createExecutionPlan(final CommandContext ctx) {
    UpdateExecutionPlan result = new UpdateExecutionPlan(ctx);

    handleSource(result, ctx, this.source);
    convertToModifiableResult(result, ctx);
    handleTarget(result, targetType, targetBucket, ctx);
    handleOperations(result, ctx, this.updateOperations);
    handleBatch(result, ctx, this.batch);
    handleSave(result, ctx);
    return result;
  }

  private void handleTarget(final UpdateExecutionPlan result, final Identifier targetType,
      final com.arcadedb.query.sql.parser.Bucket targetBucket,
      final CommandContext ctx) {
    result.chain(new MoveVertexStep(targetType, targetBucket, ctx));
  }

  private void handleBatch(final UpdateExecutionPlan result, final CommandContext ctx, final Batch batch) {
    if (batch != null)
      result.chain(new BatchStep(batch, ctx));
  }

  /**
   * add a step that transforms a normal OResult in a specific object that under setProperty()
   * updates the actual OIdentifiable
   *
   * @param plan the execution plan
   * @param ctx  the executino context
   */
  private void convertToModifiableResult(final UpdateExecutionPlan plan, final CommandContext ctx) {
    plan.chain(new ConvertToUpdatableResultStep(ctx));
  }

  private void handleSave(final UpdateExecutionPlan result, final CommandContext ctx) {
    result.chain(new SaveElementStep(ctx, null, true));
  }

  private void handleOperations(final UpdateExecutionPlan plan, final CommandContext ctx, final UpdateOperations op) {
    if (op != null) {
      switch (op.getType()) {
      case UpdateOperations.TYPE_SET:
        plan.chain(new UpdateSetStep(op.getUpdateItems(), ctx));
        break;
      case UpdateOperations.TYPE_REMOVE:
        plan.chain(new UpdateRemoveStep(op.getUpdateRemoveItems(), ctx));
        break;
      case UpdateOperations.TYPE_MERGE:
        plan.chain(new UpdateMergeStep(op.getJson(), ctx));
        break;
      case UpdateOperations.TYPE_CONTENT:
        plan.chain(new UpdateContentStep(op.getJson(), ctx));
        break;
      case UpdateOperations.TYPE_PUT:
      case UpdateOperations.TYPE_INCREMENT:
      case UpdateOperations.TYPE_ADD:
        throw new CommandExecutionException(
            "Cannot execute with UPDATE PUT/ADD/INCREMENT new executor: " + op);
      }
    }
  }

  private void handleSource(final UpdateExecutionPlan result, final CommandContext ctx, final FromItem source) {
    final SelectStatement sourceStatement = new SelectStatement(-1);
    sourceStatement.setTarget(new FromClause(-1));
    sourceStatement.getTarget().setItem(source);
    final SelectExecutionPlanner planner = new SelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(ctx, false), ctx, ctx));
  }
}
