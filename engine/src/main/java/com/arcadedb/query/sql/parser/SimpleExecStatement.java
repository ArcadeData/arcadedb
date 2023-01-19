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
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalExecutionPlan;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SingleOpExecutionPlan;

import java.util.*;

/**
 * Superclass for SQL statements that are too simple to deserve an execution planner.
 * All the execution is delegated to the statement itself, with the execute(ctx) method.
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public abstract class SimpleExecStatement extends Statement {

  public SimpleExecStatement(final int id) {
    super(id);
  }

  public abstract ResultSet executeSimple(CommandContext ctx);

  public ResultSet execute(final Database db, final Object[] args, final CommandContext parentContext, final boolean usePlanCache) {
    final BasicCommandContext ctx = new BasicCommandContext();
    if (parentContext != null) {
      ctx.setParentWithoutOverridingChild(parentContext);
    }
    ctx.setDatabase(db);
    ctx.setInputParameters(args);
    final SingleOpExecutionPlan executionPlan = (SingleOpExecutionPlan) createExecutionPlan(ctx, false);
    return executionPlan.executeInternal();
  }

  public ResultSet execute(final Database db, final Map<String, Object> params, final CommandContext parentContext, final boolean usePlanCache) {
    final BasicCommandContext ctx = new BasicCommandContext();
    if (parentContext != null) {
      ctx.setParentWithoutOverridingChild(parentContext);
    }
    ctx.setDatabase(db);
    ctx.setInputParameters(params);
    final SingleOpExecutionPlan executionPlan = (SingleOpExecutionPlan) createExecutionPlan(ctx, false);
    return executionPlan.executeInternal();
  }

  public InternalExecutionPlan createExecutionPlan(final CommandContext ctx, final boolean enableProfiling) {
    return new SingleOpExecutionPlan(ctx, this);
  }

}
