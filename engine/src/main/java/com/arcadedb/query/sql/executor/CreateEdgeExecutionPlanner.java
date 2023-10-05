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
import com.arcadedb.query.sql.parser.JsonArray;
import com.arcadedb.query.sql.parser.UpdateItem;
import com.arcadedb.schema.DocumentType;

import java.util.*;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class CreateEdgeExecutionPlanner {

  protected       Identifier targetClass;
  protected final Identifier targetBucketName;
  protected final Expression leftExpression;
  protected final Expression rightExpression;
  protected final boolean    unidirectional;
  protected final boolean    ifNotExists;
  protected final InsertBody body;

  public CreateEdgeExecutionPlanner(final CreateEdgeStatement statement) {
    this.targetClass = statement.getTargetType() == null ? null : statement.getTargetType().copy();
    this.targetBucketName = statement.getTargetBucketName() == null ? null : statement.getTargetBucketName().copy();
    this.leftExpression = statement.getLeftExpression() == null ? null : statement.getLeftExpression().copy();
    this.rightExpression = statement.getRightExpression() == null ? null : statement.getRightExpression().copy();
    this.unidirectional = statement.isUnidirectional();
    this.ifNotExists = statement.ifNotExists();
    this.body = statement.getBody() == null ? null : statement.getBody().copy();

    if (statement.getBody() != null) {
      final JsonArray jsonArray = statement.getBody().getJsonArrayContent();
      if (jsonArray != null && jsonArray.items.size() != 1)
        throw new CommandSQLParsingException("Expected one entry in the json array as content");
    }
  }

  public InsertExecutionPlan createExecutionPlan(final CommandContext context, final boolean enableProfiling) {

    if (targetClass == null) {
      if (targetBucketName == null) {
        throw new CommandSQLParsingException("Missing target");
      } else {
        final Database db = context.getDatabase();
        final DocumentType typez = db.getSchema()
            .getTypeByBucketId((db.getSchema().getBucketByName(targetBucketName.getStringValue()).getFileId()));
        if (typez != null) {
          targetClass = new Identifier(typez.getName());
        } else {
          throw new CommandSQLParsingException("Missing target");
        }
      }
    }

    final InsertExecutionPlan result = new InsertExecutionPlan(context);

    handleCheckType(result, context, enableProfiling);

    handleGlobalLet(result, new Identifier("$__ARCADEDB_CREATE_EDGE_fromV"), leftExpression, context, enableProfiling);
    handleGlobalLet(result, new Identifier("$__ARCADEDB_CREATE_EDGE_toV"), rightExpression, context, enableProfiling);

    final String uniqueIndexName;
//    if (context.getDatabase().getSchema().existsType(targetClass.getStringValue())) {
//      final EdgeType clazz = (EdgeType) context.getDatabase().getSchema().getType(targetClass.getStringValue());
//      uniqueIndexName = clazz.getAllIndexes(true).stream().filter(x -> x.isUnique())
//          .filter(x -> x.getPropertyNames().size() == 2 && x.has("@out") && x.has("@in")).map(x -> x.getName())
//          .findFirst().orElse(null);
//    } else
    uniqueIndexName = null;

    result.chain(
        new CreateEdgesStep(targetClass, targetBucketName, uniqueIndexName, new Identifier("$__ARCADEDB_CREATE_EDGE_fromV"),
            new Identifier("$__ARCADEDB_CREATE_EDGE_toV"), unidirectional, ifNotExists, context, enableProfiling));

    handleSetFields(result, body, context, enableProfiling);
    handleSave(result, targetBucketName, context, enableProfiling);
    //TODO implement batch, wait and retry
    return result;
  }

  private void handleGlobalLet(final InsertExecutionPlan result, final Identifier name, final Expression expression,
      final CommandContext context, final boolean profilingEnabled) {
    result.chain(new GlobalLetExpressionStep(name, expression, context, profilingEnabled));
  }

  private void handleCheckType(final InsertExecutionPlan result, final CommandContext context, final boolean profilingEnabled) {
    if (targetClass != null) {
      result.chain(new CheckIsEdgeTypeStep(targetClass.getStringValue(), context, profilingEnabled));
    }
  }

  private void handleSave(final InsertExecutionPlan result, final Identifier targetClusterName, final CommandContext context,
      final boolean profilingEnabled) {
    result.chain(new SaveElementStep(context, targetClusterName, profilingEnabled));
  }

  private void handleSetFields(final InsertExecutionPlan result, final InsertBody insertBody, final CommandContext context,
      final boolean profilingEnabled) {
    if (insertBody == null) {
      return;
    }
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

}
