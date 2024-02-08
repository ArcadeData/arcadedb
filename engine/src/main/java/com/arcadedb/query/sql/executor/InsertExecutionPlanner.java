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

import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.parser.Bucket;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.InsertBody;
import com.arcadedb.query.sql.parser.InsertSetExpression;
import com.arcadedb.query.sql.parser.InsertStatement;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.UpdateItem;

import java.util.*;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class InsertExecutionPlanner {

  protected Identifier      targetType;
  protected Identifier      targetBucketName;
  protected Bucket          targetBucket;
  protected InsertBody      insertBody;
  protected Projection      returnStatement;
  protected SelectStatement selectStatement;

  public InsertExecutionPlanner() {
  }

  public InsertExecutionPlanner(final InsertStatement statement) {
    this.targetType = statement.getTargetType() == null ? null : statement.getTargetType().copy();
    this.targetBucketName = statement.getTargetBucketName() == null ? null : statement.getTargetBucketName().copy();
    this.targetBucket = statement.getTargetBucket() == null ? null : statement.getTargetBucket().copy();
    this.insertBody = statement.getInsertBody() == null ? null : statement.getInsertBody().copy();
    this.returnStatement = statement.getReturnStatement() == null ? null : statement.getReturnStatement().copy();
    this.selectStatement = statement.getSelectStatement() == null ? null : statement.getSelectStatement().copy();
  }

  public InsertExecutionPlan createExecutionPlan(final CommandContext context, final boolean enableProfiling) {
    final InsertExecutionPlan result = new InsertExecutionPlan(context);

    if (selectStatement != null) {
      handleInsertSelect(result, this.selectStatement, context, enableProfiling);
    } else {
      handleCreateRecord(result, this.insertBody, context, enableProfiling);
    }
    handleTargetClass(result, targetType, context, enableProfiling);
    handleSetFields(result, insertBody, context, enableProfiling);
    if (targetBucket != null) {
      String name = targetBucket.getBucketName();
      if (name == null) {
        name = context.getDatabase().getSchema().getBucketById(targetBucket.getBucketNumber()).getName();
      }
      handleSave(result, new Identifier(name), context, enableProfiling);
    } else {
      handleSave(result, targetBucketName, context, enableProfiling);
    }
    handleReturn(result, returnStatement, context, enableProfiling);

    return result;
  }

  private void handleSave(final InsertExecutionPlan result, final Identifier targetClusterName, final CommandContext context,
      final boolean profilingEnabled) {
    result.chain(new SaveElementStep(context, targetClusterName, profilingEnabled));
  }

  private void handleReturn(final InsertExecutionPlan result, final Projection returnStatement, final CommandContext context,
      final boolean profilingEnabled) {
    if (returnStatement != null)
      result.chain(new ProjectionCalculationStep(returnStatement, context, profilingEnabled));
  }

  private void handleSetFields(final InsertExecutionPlan result, final InsertBody insertBody, final CommandContext context,
      final boolean profilingEnabled) {
    if (insertBody == null)
      return;

    if (insertBody.getIdentifierList() != null) {
      result.chain(
          new InsertValuesStep(insertBody.getIdentifierList(), insertBody.getValueExpressions(), context, profilingEnabled));
    } else if (insertBody.getJsonContent() != null) {
      result.chain(new UpdateContentStep(insertBody.getJsonContent(), context, profilingEnabled));
    } else if (insertBody.getJsonArrayContent() != null) {
      result.chain(new UpdateContentStep(insertBody.getJsonArrayContent(), context, profilingEnabled));
    } else if (insertBody.getContentInputParam() != null) {
      result.chain(new UpdateContentStep(insertBody.getContentInputParam(), context, profilingEnabled));
    } else if (insertBody.getSetExpressions() != null) {
      final List<UpdateItem> items = new ArrayList<>();
      for (final InsertSetExpression exp : insertBody.getSetExpressions()) {
        final UpdateItem item = new UpdateItem(-1);
        item.setOperator(UpdateItem.OPERATOR_EQ);
        item.setLeft(exp.getLeft().copy());
        item.setRight(exp.getRight().copy());
        items.add(item);
      }
      result.chain(new UpdateSetStep(items, context, profilingEnabled));
    }
  }

  private void handleTargetClass(final InsertExecutionPlan result, final Identifier targetClass, final CommandContext context,
      final boolean profilingEnabled) {
    if (targetClass != null)
      result.chain(new SetDocumentStepStep(targetClass, context, profilingEnabled));
  }

  private void handleCreateRecord(final InsertExecutionPlan result, final InsertBody body, final CommandContext context,
      final boolean profilingEnabled) {
    int tot = 1;
    if (body != null && body.getValueExpressions() != null && body.getValueExpressions().size() > 0)
      tot = body.getValueExpressions().size();
    else if (body != null && body.getJsonArrayContent() != null && body.getJsonArrayContent().items.size() > 0)
      tot = body.getJsonArrayContent().items.size();

    if (targetType == null && targetBucket != null) {
      final com.arcadedb.engine.Bucket bucket;
      if (targetBucket.getBucketName() != null)
        bucket = context.getDatabase().getSchema().getBucketByName(targetBucket.getBucketName());
      else
        bucket = context.getDatabase().getSchema().getBucketById(targetBucket.getBucketNumber());

      if (bucket == null)
        throw new CommandSQLParsingException("Target not specified");

      targetType = new Identifier(context.getDatabase().getSchema().getTypeNameByBucketId(bucket.getFileId()));
    }

    result.chain(new CreateRecordStep(targetType.getStringValue(), context, tot, profilingEnabled));
  }

  private void handleInsertSelect(final InsertExecutionPlan result, final SelectStatement selectStatement,
      final CommandContext context, final boolean profilingEnabled) {
    final InternalExecutionPlan subPlan = selectStatement.createExecutionPlan(context, profilingEnabled);
    result.chain(new SubQueryStep(subPlan, context, context, profilingEnabled));
    if (targetType != null)
      result.chain(new CopyDocumentStep(context, targetType.getStringValue(), profilingEnabled));

    result.chain(new RemoveEdgePointersStep(context, profilingEnabled));
  }
}
