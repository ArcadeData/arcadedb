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
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.DeleteExecutionPlan;
import com.arcadedb.query.sql.executor.DeleteExecutionPlanner;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.*;

public class DeleteStatement extends Statement {

  public    FromClause  fromClause;
  protected WhereClause whereClause;
  protected boolean     returnBefore = false;
  protected boolean     unsafe       = false;

  public DeleteStatement(final int id) {
    super(id);
  }

  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("DELETE FROM ");
    fromClause.toString(params, builder);
    if (returnBefore) {
      builder.append(" RETURN BEFORE");
    }
    if (whereClause != null) {
      builder.append(" WHERE ");
      whereClause.toString(params, builder);
    }
    if (limit != null) {
      limit.toString(params, builder);
    }
    if (unsafe) {
      builder.append(" UNSAFE");
    }
  }

  @Override
  public DeleteStatement copy() {
    final DeleteStatement result = new DeleteStatement(-1);
    result.fromClause = fromClause == null ? null : fromClause.copy();
    result.whereClause = whereClause == null ? null : whereClause.copy();
    result.returnBefore = returnBefore;
    result.limit = limit == null ? null : limit.copy();
    result.unsafe = unsafe;
    return result;
  }

  @Override
  protected Object[] getIdentityElements() {
    return new Object[] { fromClause, whereClause, returnBefore, limit, unsafe };
  }

  @Override
  public ResultSet execute(final Database db, final Map params, final CommandContext parentcontext, final boolean usePlanCache) {
    final BasicCommandContext context = new BasicCommandContext();
    if (parentcontext != null) {
      context.setParentWithoutOverridingChild(parentcontext);
    }
    context.setDatabase(db);
    context.setInputParameters(params);
    final DeleteExecutionPlan executionPlan = createExecutionPlan(context);
    executionPlan.executeInternal();
    return new LocalResultSet(executionPlan);
  }

  @Override
  public ResultSet execute(final Database db, final Object[] args, final CommandContext parentcontext, final boolean usePlanCache) {
    final BasicCommandContext context = new BasicCommandContext();
    if (parentcontext != null) {
      context.setParentWithoutOverridingChild(parentcontext);
    }
    context.setDatabase(db);
    context.setInputParameters(args);
    final DeleteExecutionPlan executionPlan = createExecutionPlan(context);
    executionPlan.executeInternal();
    return new LocalResultSet(executionPlan);
  }

  public DeleteExecutionPlan createExecutionPlan(final CommandContext context) {
    final DeleteExecutionPlanner planner = new DeleteExecutionPlanner(this);
    return planner.createExecutionPlan(context);
  }

  public FromClause getFromClause() {
    return fromClause;
  }

  public WhereClause getWhereClause() {
    return whereClause;
  }

  public boolean isReturnBefore() {
    return returnBefore;
  }

  public boolean isUnsafe() {
    return unsafe;
  }
}
/* JavaCC - OriginalChecksum=5fb4ca5ba648e6c9110f41d806206a6f (do not edit this line) */
