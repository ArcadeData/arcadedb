/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.query.sql.executor;

import com.arcadedb.query.sql.parser.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class OInsertExecutionPlanner {

  protected Identifier      targetType;
  protected Identifier      targetBucketName;
  protected Bucket          targetBucket;
  protected IndexIdentifier targetIndex;
  protected InsertBody      insertBody;
  protected Projection      returnStatement;
  protected SelectStatement selectStatement;

  public OInsertExecutionPlanner() {

  }

  public OInsertExecutionPlanner(InsertStatement statement) {
    this.targetType = statement.getTargetType() == null ? null : statement.getTargetType().copy();
    this.targetBucketName = statement.getTargetBucketName() == null ? null : statement.getTargetBucketName().copy();
    this.targetBucket = statement.getTargetBucket() == null ? null : statement.getTargetBucket().copy();
    this.targetIndex = statement.getTargetIndex() == null ? null : statement.getTargetIndex().copy();
    this.insertBody = statement.getInsertBody() == null ? null : statement.getInsertBody().copy();
    this.returnStatement = statement.getReturnStatement() == null ? null : statement.getReturnStatement().copy();
    this.selectStatement = statement.getSelectStatement() == null ? null : statement.getSelectStatement().copy();
  }

  public InsertExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    InsertExecutionPlan result = new InsertExecutionPlan(ctx);

    if (targetIndex != null) {
      result.chain(new InsertIntoIndexStep(targetIndex, insertBody, ctx, enableProfiling));
    } else {
      if (selectStatement != null) {
        handleInsertSelect(result, this.selectStatement, ctx, enableProfiling);
      } else {
        handleCreateRecord(result, this.insertBody, ctx, enableProfiling);
      }
      handleTargetClass(result, targetType, ctx, enableProfiling);
      handleSetFields(result, insertBody, ctx, enableProfiling);
      if (targetBucket != null) {
        String name = targetBucket.getBucketName();
        if (name == null) {
          name = ctx.getDatabase().getSchema().getBucketById(targetBucket.getBucketNumber()).getName();
        }
        handleSave(result, new Identifier(name), ctx, enableProfiling);
      } else {
        handleSave(result, targetBucketName, ctx, enableProfiling);
      }
      handleReturn(result, returnStatement, ctx, enableProfiling);
    }
    return result;
  }

  private void handleSave(InsertExecutionPlan result, Identifier targetClusterName, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new SaveElementStep(ctx, targetClusterName, profilingEnabled));
  }

  private void handleReturn(InsertExecutionPlan result, Projection returnStatement, CommandContext ctx, boolean profilingEnabled) {
    if (returnStatement != null) {
      result.chain(new ProjectionCalculationStep(returnStatement, ctx, profilingEnabled));
    }
  }

  private void handleSetFields(InsertExecutionPlan result, InsertBody insertBody, CommandContext ctx, boolean profilingEnabled) {
    if (insertBody == null) {
      return;
    }
    if (insertBody.getIdentifierList() != null) {
      result.chain(new InsertValuesStep(insertBody.getIdentifierList(), insertBody.getValueExpressions(), ctx, profilingEnabled));
    } else if (insertBody.getContent() != null) {
      result.chain(new UpdateContentStep(insertBody.getContent(), ctx, profilingEnabled));
    } else if (insertBody.getContentInputParam() != null) {
      result.chain(new UpdateContentStep(insertBody.getContentInputParam(), ctx, profilingEnabled));
    } else if (insertBody.getSetExpressions() != null) {
      List<UpdateItem> items = new ArrayList<>();
      for (InsertSetExpression exp : insertBody.getSetExpressions()) {
        UpdateItem item = new UpdateItem(-1);
        item.setOperator(UpdateItem.OPERATOR_EQ);
        item.setLeft(exp.getLeft().copy());
        item.setRight(exp.getRight().copy());
        items.add(item);
      }
      result.chain(new UpdateSetStep(items, ctx, profilingEnabled));
    }
  }

  private void handleTargetClass(InsertExecutionPlan result, Identifier targetClass, CommandContext ctx, boolean profilingEnabled) {
    if (targetClass != null) {
      result.chain(new SetDocumentClassStep(targetClass, ctx, profilingEnabled));
    }
  }

  private void handleCreateRecord(InsertExecutionPlan result, InsertBody body, CommandContext ctx, boolean profilingEnabled) {
    int tot = 1;

    if (body != null && body.getValueExpressions() != null && body.getValueExpressions().size() > 0) {
      tot = body.getValueExpressions().size();
    }
    result.chain(new CreateRecordStep(targetType.getStringValue(), ctx, tot, profilingEnabled));
  }

  private void handleInsertSelect(InsertExecutionPlan result, SelectStatement selectStatement, CommandContext ctx, boolean profilingEnabled) {
    InternalExecutionPlan subPlan = selectStatement.createExecutionPlan(ctx, profilingEnabled);
    result.chain(new SubQueryStep(subPlan, ctx, ctx, profilingEnabled));
    if(targetType!=null) {
      result.chain(new CopyDocumentStep(ctx, targetType.getStringValue(), profilingEnabled));
    }
    result.chain(new RemoveEdgePointersStep(ctx, profilingEnabled));
  }
}
