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

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.parser.CreateEdgeStatement;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.InsertBody;
import com.arcadedb.query.sql.parser.InsertSetExpression;
import com.arcadedb.query.sql.parser.UpdateItem;
import com.arcadedb.schema.DocumentType;

import java.util.*;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class CreateEdgeExecutionPlanner {

  protected       Identifier targetClass;
  protected final Identifier targetClusterName;
  protected final Expression leftExpression;
  protected final Expression rightExpression;
  protected final boolean    ifNotExists;
  protected final InsertBody body;
  protected final Number     retry;
  protected final Number     wait;

  public CreateEdgeExecutionPlanner(final CreateEdgeStatement statement) {
    this.targetClass = statement.getTargetType() == null ? null : statement.getTargetType().copy();
    this.targetClusterName = statement.getTargetBucketName() == null ? null : statement.getTargetBucketName().copy();
    this.leftExpression = statement.getLeftExpression() == null ? null : statement.getLeftExpression().copy();
    this.rightExpression = statement.getRightExpression() == null ? null : statement.getRightExpression().copy();
    this.ifNotExists = statement.ifNotExists();
    this.body = statement.getBody() == null ? null : statement.getBody().copy();
    this.retry = statement.getRetry();
    this.wait = statement.getWait();
  }

  public InsertExecutionPlan createExecutionPlan(final CommandContext ctx, final boolean enableProfiling) {

    if (targetClass == null) {
      if (targetClusterName == null) {
        throw new CommandSQLParsingException("Missing target");
      } else {
        final Database db = ctx.getDatabase();
        final DocumentType typez = db.getSchema().getTypeByBucketId((db.getSchema().getBucketByName(targetClusterName.getStringValue()).getId()));
        if (typez != null) {
          targetClass = new Identifier(typez.getName());
        } else {
          throw new CommandSQLParsingException("Missing target");
        }
      }
    }

    final InsertExecutionPlan result = new InsertExecutionPlan(ctx);

    handleCheckType(result, ctx, enableProfiling);

    handleGlobalLet(result, new Identifier("$__ARCADEDB_CREATE_EDGE_fromV"), leftExpression, ctx, enableProfiling);
    handleGlobalLet(result, new Identifier("$__ARCADEDB_CREATE_EDGE_toV"), rightExpression, ctx, enableProfiling);

    final String uniqueIndexName;
//    if (ctx.getDatabase().getSchema().existsType(targetClass.getStringValue())) {
//      final EdgeType clazz = (EdgeType) ctx.getDatabase().getSchema().getType(targetClass.getStringValue());
//      uniqueIndexName = clazz.getAllIndexes(true).stream().filter(x -> x.isUnique())
//          .filter(x -> x.getPropertyNames().size() == 2 && x.has("@out") && x.has("@in")).map(x -> x.getName())
//          .findFirst().orElse(null);
//    } else
    uniqueIndexName = null;

    result.chain(new CreateEdgesStep(targetClass, targetClusterName, uniqueIndexName, new Identifier("$__ARCADEDB_CREATE_EDGE_fromV"),
        new Identifier("$__ARCADEDB_CREATE_EDGE_toV"), ifNotExists, wait, retry, ctx, enableProfiling));

    handleSetFields(result, body, ctx, enableProfiling);
    handleSave(result, targetClusterName, ctx, enableProfiling);
    //TODO implement batch, wait and retry
    return result;
  }

  private void handleGlobalLet(final InsertExecutionPlan result, final Identifier name, final Expression expression, final CommandContext ctx,
      final boolean profilingEnabled) {
    result.chain(new GlobalLetExpressionStep(name, expression, ctx, profilingEnabled));
  }

  private void handleCheckType(final InsertExecutionPlan result, final CommandContext ctx, final boolean profilingEnabled) {
    if (targetClass != null) {
      result.chain(new CheckIsEdgeTypeStep(targetClass.getStringValue(), ctx, profilingEnabled));
    }
  }

  private void handleSave(final InsertExecutionPlan result, final Identifier targetClusterName, final CommandContext ctx, final boolean profilingEnabled) {
    result.chain(new SaveElementStep(ctx, targetClusterName, profilingEnabled));
  }

  private void handleSetFields(final InsertExecutionPlan result, final InsertBody insertBody, final CommandContext ctx, final boolean profilingEnabled) {
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
      final List<UpdateItem> items = new ArrayList<>();
      for (final InsertSetExpression exp : insertBody.getSetExpressions()) {
        final UpdateItem item = new UpdateItem(-1);
        item.setOperator(UpdateItem.OPERATOR_EQ);
        item.setLeft(exp.getLeft().copy());
        item.setRight(exp.getRight().copy());
        items.add(item);
      }
      result.chain(new UpdateSetStep(items, ctx, profilingEnabled));
    }
  }

}
