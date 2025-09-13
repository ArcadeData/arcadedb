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
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.CreateVertexExecutionPlanner;
import com.arcadedb.query.sql.executor.InsertExecutionPlan;
import com.arcadedb.query.sql.executor.InternalExecutionPlan;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.*;

public class CreateVertexStatement extends Statement {
  Identifier targetType;
  Identifier targetBucketName;
  Bucket     targetBucket;
  Projection returnStatement;
  InsertBody insertBody;

  public CreateVertexStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet execute(final Database database, final Map params, final CommandContext parentcontext,
      final boolean usePlanCache) {
    final BasicCommandContext context = new BasicCommandContext();
    if (parentcontext != null)
      context.setParentWithoutOverridingChild(parentcontext);

    context.setDatabase(database);
    context.setInputParameters(params);

    final boolean implicitTransaction = ((DatabaseInternal) database).checkTransactionIsActive(database.isAutoTransaction());
    try {
      final InsertExecutionPlan executionPlan = (InsertExecutionPlan) createExecutionPlan(context);
      executionPlan.executeInternal();
      return new LocalResultSet(executionPlan);
    } finally {
      if (implicitTransaction)
        database.commit();
    }
  }

  @Override
  public ResultSet execute(final Database database, final Object[] args, final CommandContext parentcontext,
      final boolean usePlanCache) {
    final BasicCommandContext context = new BasicCommandContext();
    if (parentcontext != null) {
      context.setParentWithoutOverridingChild(parentcontext);
    }
    context.setDatabase(database);
    context.setInputParameters(args);

    final boolean implicitTransaction = ((DatabaseInternal) database).checkTransactionIsActive(database.isAutoTransaction());
    try {

      final InsertExecutionPlan executionPlan = (InsertExecutionPlan) createExecutionPlan(context);
      executionPlan.executeInternal();
      return new LocalResultSet(executionPlan);
    } finally {
      if (implicitTransaction)
        database.commit();
    }
  }

  @Override
  public InternalExecutionPlan createExecutionPlan(final CommandContext context) {
    final CreateVertexExecutionPlanner planner = new CreateVertexExecutionPlanner(this);
    return planner.createExecutionPlan(context);
  }

  public void toString(final Map<String, Object> params, final StringBuilder builder) {

    builder.append("CREATE VERTEX ");
    if (targetType != null) {
      targetType.toString(params, builder);
      if (targetBucketName != null) {
        builder.append(" BUCKET ");
        targetBucketName.toString(params, builder);
      }
    }
    if (targetBucket != null) {
      targetBucket.toString(params, builder);
    }
    if (returnStatement != null) {
      builder.append(" RETURN ");
      returnStatement.toString(params, builder);
    }
    if (insertBody != null) {
      if (targetType != null || targetBucket != null || returnStatement != null) {
        builder.append(" ");
      }
      insertBody.toString(params, builder);
    }
  }

  @Override
  public CreateVertexStatement copy() {
    CreateVertexStatement result = null;
    try {
      result = getClass().getConstructor(Integer.TYPE).newInstance(-1);
    } catch (final Exception e) {
      throw new ArcadeDBException(e);
    }
    result.targetType = targetType == null ? null : targetType.copy();
    result.targetBucketName = targetBucketName == null ? null : targetBucketName.copy();
    result.targetBucket = targetBucket == null ? null : targetBucket.copy();
    result.returnStatement = returnStatement == null ? null : returnStatement.copy();
    result.insertBody = insertBody == null ? null : insertBody.copy();
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final CreateVertexStatement that = (CreateVertexStatement) o;

    if (!Objects.equals(targetType, that.targetType))
      return false;
    if (!Objects.equals(targetBucketName, that.targetBucketName))
      return false;
    if (!Objects.equals(targetBucket, that.targetBucket))
      return false;
    if (!Objects.equals(returnStatement, that.returnStatement))
      return false;
    return Objects.equals(insertBody, that.insertBody);
  }

  @Override
  public int hashCode() {
    int result = targetType != null ? targetType.hashCode() : 0;
    result = 31 * result + (targetBucketName != null ? targetBucketName.hashCode() : 0);
    result = 31 * result + (targetBucket != null ? targetBucket.hashCode() : 0);
    result = 31 * result + (returnStatement != null ? returnStatement.hashCode() : 0);
    result = 31 * result + (insertBody != null ? insertBody.hashCode() : 0);
    return result;
  }

  public Identifier getTargetType() {
    return targetType;
  }

  public Identifier getTargetBucketName() {
    return targetBucketName;
  }

  public Bucket getTargetBucket() {
    return targetBucket;
  }

  public Projection getReturnStatement() {
    return returnStatement;
  }

  public InsertBody getInsertBody() {
    return insertBody;
  }
}
/* JavaCC - OriginalChecksum=0ac3d3f09a76b9924a17fd05bc293863 (do not edit this line) */
