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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.DDLExecutionPlan;
import com.arcadedb.query.sql.executor.InternalExecutionPlan;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.*;

/**
 * Created by luigidellaquila on 12/08/16.
 */
public abstract class DDLStatement extends Statement {

  public DDLStatement(final int id) {
    super(id);
  }

  public abstract ResultSet executeDDL(CommandContext context);

  public ResultSet execute(final Database db, final Object[] args, final CommandContext parentcontext, final boolean usePlanCache) {
    final BasicCommandContext context = new BasicCommandContext();
    if (parentcontext != null)
      context.setParentWithoutOverridingChild(parentcontext);

    context.setDatabase(db);
    context.setInputParameters(args);
    final DDLExecutionPlan executionPlan = (DDLExecutionPlan) createExecutionPlan(context);
    return executionPlan.executeInternal();
  }

  public ResultSet execute(final Database db, final Map params, final CommandContext parentcontext, final boolean usePlanCache) {
    final BasicCommandContext context = new BasicCommandContext();
    if (parentcontext != null) {
      context.setParentWithoutOverridingChild(parentcontext);
    }
    context.setDatabase(db);
    context.setInputParameters(params);
    final DDLExecutionPlan executionPlan = (DDLExecutionPlan) createExecutionPlan(context);
    return executionPlan.executeInternal();
  }

  public InternalExecutionPlan createExecutionPlan(final CommandContext context) {
    return new DDLExecutionPlan(context, this);
  }

  /**
   * Parses a {@code WITH} clause boolean setting. Accepts a {@link Boolean} directly, the
   * strings {@code "true"} / {@code "false"} (case-insensitive), and any equivalent that
   * {@link String#valueOf} produces from a literal expression. Throws
   * {@link CommandSQLParsingException} with a locatable message on anything else.
   * <p>
   * Shared between DDL statements that accept boolean settings (e.g.
   * {@code REBUILD TYPE ... WITH repartition = true} and
   * {@code ALTER TYPE ... WITH repartition = true}) so the parsing semantics stay identical
   * across statement types and a future addition picks up the same rules without re-deriving
   * them.
   *
   * @param statementContext display label for the statement, used in the error message (e.g.
   *                         {@code "REBUILD TYPE"}).
   * @param settingName      the setting key, used in the error message.
   * @param raw              the evaluated value of the setting expression (typically the
   *                         result of {@code expression.execute(null, context)}).
   */
  protected static boolean parseBooleanSetting(final String statementContext, final String settingName, final Object raw) {
    if (raw instanceof Boolean b)
      return b;
    final String text = String.valueOf(raw);
    if ("true".equalsIgnoreCase(text))
      return true;
    if ("false".equalsIgnoreCase(text))
      return false;
    throw new CommandSQLParsingException(
        statementContext + " setting '" + settingName + "' must be true or false, got: " + raw);
  }
}
