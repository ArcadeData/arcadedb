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
package com.arcadedb.schema.trigger;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

/**
 * Executor for SQL-based trigger actions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLTriggerExecutor implements TriggerExecutor {
  private final String triggerName;
  private final String sql;

  public SQLTriggerExecutor(final String triggerName, final String sql) {
    this.triggerName = triggerName;
    this.sql = sql;
  }

  @Override
  public boolean execute(final Database database, final Record record, final Record oldRecord) {
    try {
      // Bound both ways: as $record/$oldRecord (a CommandContext variable, so $record.field navigation works -
      // see #command's javadoc) and as record/oldRecord (an input parameter, so :record/? substitution works
      // too, as a whole value).
      final Map<String, Object> bindings = new HashMap<>();
      bindings.put("record", record);
      bindings.put("$record", record);
      if (oldRecord != null) {
        bindings.put("oldRecord", oldRecord);
        bindings.put("$oldRecord", oldRecord);
      }

      return consume(database, sqlEngine(database).command(sql, new ContextConfiguration(), bindings, bindings));
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error executing SQL trigger '%s': %s", e, triggerName, e.getMessage());
      throw new TriggerExecutionException("SQL trigger '" + triggerName + "' failed: " + e.getMessage(), e);
    }
  }

  /**
   * A BEFORE READ trigger has no record to bind - see {@link TriggerExecutor#executeBeforeRead}. The identity is
   * bound instead, so the body can still address WHICH record is being read; {@code record}/{@code $record} are
   * deliberately absent rather than null, so a body that expects them fails visibly instead of silently seeing
   * nothing.
   */
  @Override
  public boolean executeBeforeRead(final Database database, final RID rid) {
    try {
      final Map<String, Object> bindings = new HashMap<>();
      bindings.put("rid", rid);
      bindings.put("$rid", rid);

      return consume(database, sqlEngine(database).command(sql, new ContextConfiguration(), bindings, bindings));
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error executing SQL trigger '%s': %s", e, triggerName, e.getMessage());
      throw new TriggerExecutionException("SQL trigger '" + triggerName + "' failed: " + e.getMessage(), e);
    }
  }

  /**
   * {@code database.command("sql", ...)} only ever fills {@link com.arcadedb.query.sql.executor.CommandContext}'s
   * input parameters, so a body that reads {@code $record}/{@code $rid} - the documented binding - saw nothing:
   * {@code SuffixIdentifier} resolves a {@code $name} exclusively through {@code CommandContext.getVariable},
   * a different store input parameters never populate. Going to {@link SQLQueryEngine} directly reaches the
   * overload that seeds both.
   */
  private static SQLQueryEngine sqlEngine(final Database database) {
    return (SQLQueryEngine) database.getQueryEngine("sql");
  }

  /**
   * Drains the body's result set to exhaustion before closing it. A {@code SELECT} result set in ArcadeDB is
   * pull-based: nothing runs until it is iterated, so a body that reads without ever being consumed executes
   * nothing at all - a silent no-op rather than a slow or partial one. DML bodies (INSERT/UPDATE/DELETE) already
   * execute eagerly when the command is built, so draining them here only walks their already-buffered rows.
   * <p>
   * The drained result also decides whether the operation continues, giving a SQL body the same veto contract as
   * {@link ScriptTriggerExecutor#execute}: a body that evaluates to a single scalar {@code false} - one row, one
   * property - aborts the operation, exactly what {@code SELECT false} or {@code SELECT <condition> AS ok} looks
   * like it should do.
   * <p>
   * The shape alone is not enough to tell a veto from an accident, though: {@code UPDATE ... RETURN AFTER <field>}
   * (or {@code RETURN BEFORE}) with a single non-{@code @this} projection item produces that exact same one-row,
   * one-property {@link Result} - see {@code Projection.calculateSingle} - so a bookkeeping trigger like
   * {@code UPDATE Flag SET active = false RETURN AFTER active} would trip the same check on an unrelated boolean
   * field and silently abort the CREATE/UPDATE/DELETE that fired it, something the pre-fix code never did because
   * it never looked at the result at all. {@link com.arcadedb.query.sql.parser.Statement#isIdempotent()} is exactly
   * the read/write classification the engine already uses elsewhere (e.g. {@code SQLQueryEngine.query()} rejects a
   * non-idempotent statement), and only a {@code SELECT} answers true - never {@code INSERT}/{@code UPDATE}/
   * {@code DELETE}, {@code RETURN} clause or not. Gating on it keeps the veto contract scoped to bodies that look
   * like {@code SELECT false}, so an existing DML trigger cannot regress: it never produced a single-row,
   * single-property result before, and now that it might (via {@code RETURN}), that shape is simply not idempotent
   * and is drained for its effects like any other DML body. The check runs only once a candidate row is already in
   * hand, so it costs nothing on the far more common multi-row, multi-column or non-projection results.
   * <p>
   * {@code $score}/{@code $similarity} are excluded from the property count: {@code ResultInternal.getPropertyNames()}
   * adds them automatically whenever a full-text or vector search set a score, so a scored validation body like
   * {@code SELECT ok FROM ... WHERE MATCH(...) ...} would otherwise report two property names instead of one and
   * silently skip a veto its single real column asked for. They are metadata about how the row was found, not a
   * column the body projected, so they take no part in deciding whether the row is a single-value answer.
   */
  private boolean consume(final Database database, final ResultSet result) {
    try (result) {
      Result first = null;
      long count = 0;
      while (result.hasNext()) {
        final Result row = result.next();
        if (count == 0)
          first = row;
        count++;
      }

      if (count == 1 && first.isProjection()) {
        final String property = soleRealProperty(first);
        if (property != null && Boolean.FALSE.equals(first.getProperty(property))
            && ((DatabaseInternal) database).getStatementCache().isIdempotent(sql))
          return false;
      }
      return true;
    }
  }

  /**
   * The single property name of {@code result}, ignoring the {@code $score}/{@code $similarity} scoring metadata -
   * see {@link #consume} - or {@code null} if zero or more than one real property remains.
   */
  private static String soleRealProperty(final Result result) {
    String sole = null;
    for (final String property : result.getPropertyNames()) {
      if ("$score".equals(property) || "$similarity".equals(property))
        continue;
      if (sole != null)
        return null;
      sole = property;
    }
    return sole;
  }

  @Override
  public void close() {
    // No resources to cleanup for SQL executor
  }
}
