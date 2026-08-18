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

import java.util.Map;

/**
 * Created by luigidellaquila on 12/08/16.
 */
public abstract class DDLStatement extends Statement {

  public DDLStatement() {
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

  /**
   * Parses a {@code WITH} clause setting that has to be a whole number of at least one, reporting EVERY way it can
   * fail as a parsing error that names the statement, the setting and the value.
   * <p>
   * Takes the EVALUATED value, exactly like {@link #parseBooleanSetting}, so callers pass
   * {@code expression.execute((Result) null, context)}. Reading the expression any other way does not work: the
   * {@link SimpleNode#value} field is null for every numeric literal the parser builds, so a statement consulting it
   * refuses every value it is given including the legal ones - that is what {@code REBUILD TYPE ... WITH batchSize =
   * 1000} did, failing with "got: null" - and RENDERING the expression instead answers with the placeholder text for
   * a bound parameter, so {@code WITH batchSize = :size} could never resolve to the number the caller bound. Only
   * evaluation covers literals and parameters alike (issue #6359, item 2).
   * <p>
   * A value below one is refused rather than coerced: it is not a smaller batch, it is a request that cannot be
   * honoured, and each caller would read it differently - a scan-based index build as "never chunk", a rebuild loop
   * whose commit cadence is {@code count % batchSize} as a modulo by zero. A fractional or out-of-int-range number is
   * refused for the same reason: truncating one would honour a request nobody made.
   * <p>
   * Shares {@link #parseBooleanSetting}'s reasoning: one place for the rules so a statement added later picks them up
   * rather than re-deriving them, and so a raw {@link NumberFormatException} - which names neither the setting nor the
   * problem - cannot escape from any of them.
   *
   * @param raw the evaluated value of the setting expression
   */
  protected static int parsePositiveIntSetting(final String statementContext, final String settingName,
      final Object raw) {
    Double exact = null;
    if (raw instanceof final Number number)
      exact = number.doubleValue();
    else if (raw != null)
      try {
        // Read as a double rather than as an int, so the same value written as TEXT is read the same way: the branch
        // above already accepts a Double of 5.0, and refusing the string "5.0" while accepting "5" would be an
        // accident of which branch the value happened to arrive on.
        exact = Double.valueOf(raw.toString().trim());
      } catch (final NumberFormatException notANumber) {
        // Reported below together with every other refusal, so they all read the same and all name the value.
      }

    // One test for every way this can be wrong - absent, not a number, fractional, out of int range, below one -
    // because they are one answer: this is not a value the setting can take. Truncating a fractional one would
    // honour a request nobody made, and NaN fails the whole-number test as surely as 1.5 does.
    if (exact == null || exact != Math.rint(exact) || exact < 1 || exact > Integer.MAX_VALUE)
      throw new CommandSQLParsingException(
          statementContext + " setting '" + settingName + "' must be a whole number of at least 1, got: " + raw);

    return exact.intValue();
  }
}
