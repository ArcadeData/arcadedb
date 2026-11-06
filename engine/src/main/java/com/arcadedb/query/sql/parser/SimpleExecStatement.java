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
import com.arcadedb.security.SecurityDatabaseUser.DATABASE_ACCESS;

import java.util.Map;

/**
 * Superclass for SQL statements that are too simple to deserve an execution planner.
 * All the execution is delegated to the statement itself, with the execute(context) method.
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public abstract class SimpleExecStatement extends Statement {

  public SimpleExecStatement() {
  }

  public abstract ResultSet executeSimple(CommandContext context);

  /**
   * Refuses a whole-database statement ({@code BACKUP}, {@code EXPORT}, {@code IMPORT DATABASE}) to a user that does not hold
   * {@link DATABASE_ACCESS#UPDATE_SECURITY}. That grant is the privilege these statements ask for, not what they do, so the
   * refusal says which statement was refused and which permission it takes: the plain check answered "is not allowed to update
   * security" to a user who had asked for a backup (issue #11334).
   */
  protected static void checkWholeDatabaseStatementAllowed(final CommandContext context, final String statement) {
    try {
      context.getDatabase().checkPermissionsOnDatabase(DATABASE_ACCESS.UPDATE_SECURITY);
    } catch (final SecurityException ignored) {
      // NOT CHAINED: THE HTTP LAYER APPENDS THE CAUSE'S MESSAGE TO THE DETAIL IT SENDS, WHICH WOULD PUT THE WORDING BACK
      throw new SecurityException("User '" + context.getDatabase().getCurrentUserName() + "' is not allowed to run " + statement
          + ": it reaches the whole database whatever the user's per-type grants, so it requires the '"
          + DATABASE_ACCESS.UPDATE_SECURITY.name + "' database permission");
    }
  }

  public ResultSet execute(final Database db, final Object[] args, final CommandContext parentContext, final boolean usePlanCache) {
    final BasicCommandContext context = new BasicCommandContext();
    if (parentContext != null) {
      context.setParentWithoutOverridingChild(parentContext);
    }
    context.setDatabase(db);
    context.setInputParameters(args);

    if (parentContext != null)
      context.setConfiguration(parentContext.getConfiguration());

    final SingleOpExecutionPlan executionPlan = (SingleOpExecutionPlan) createExecutionPlan(context);
    return executionPlan.executeInternal();
  }

  public ResultSet execute(final Database db, final Map<String, Object> params, final CommandContext parentContext,
      final boolean usePlanCache) {
    final BasicCommandContext context = new BasicCommandContext();
    if (parentContext != null)
      context.setParentWithoutOverridingChild(parentContext);

    context.setDatabase(db);
    context.setInputParameters(params);

    if (parentContext != null)
      context.setConfiguration(parentContext.getConfiguration());

    final SingleOpExecutionPlan executionPlan = (SingleOpExecutionPlan) createExecutionPlan(context);
    return executionPlan.executeInternal();
  }

  public InternalExecutionPlan createExecutionPlan(final CommandContext context) {
    return new SingleOpExecutionPlan(context, this);
  }
}
