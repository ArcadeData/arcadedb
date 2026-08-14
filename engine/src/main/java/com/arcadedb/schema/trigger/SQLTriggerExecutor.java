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

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
      // Prepare context variables for SQL execution
      final Map<String, Object> params = new HashMap<>();
      params.put("record", record);
      params.put("$record", record);
      if (oldRecord != null) {
        params.put("oldRecord", oldRecord);
        params.put("$oldRecord", oldRecord);
      }

      return consume(database.command("sql", sql, params));
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
      final Map<String, Object> params = new HashMap<>();
      params.put("rid", rid);
      params.put("$rid", rid);

      return consume(database.command("sql", sql, params));
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error executing SQL trigger '%s': %s", e, triggerName, e.getMessage());
      throw new TriggerExecutionException("SQL trigger '" + triggerName + "' failed: " + e.getMessage(), e);
    }
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
   * like it should do. A multi-row or multi-column result has no unambiguous single answer, so it is drained for
   * its effects and treated as a pass, same as DML.
   */
  private static boolean consume(final ResultSet result) {
    try (result) {
      Result single = null;
      long   count  = 0;
      while (result.hasNext()) {
        final Result row = result.next();
        single = count == 0 ? row : null;
        count++;
      }

      if (count == 1 && single != null && single.isProjection()) {
        final Set<String> properties = single.getPropertyNames();
        if (properties.size() == 1 && Boolean.FALSE.equals(single.getProperty(properties.iterator().next())))
          return false;
      }
      return true;
    }
  }

  @Override
  public void close() {
    // No resources to cleanup for SQL executor
  }
}
