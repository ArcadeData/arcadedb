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
import com.arcadedb.database.Record;
import com.arcadedb.log.LogManager;

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
      // Prepare context variables for SQL execution
      final Map<String, Object> params = new HashMap<>();
      params.put("record", record);
      params.put("$record", record);
      if (oldRecord != null) {
        params.put("oldRecord", oldRecord);
        params.put("$oldRecord", oldRecord);
      }

      // Execute SQL with context parameters
      database.command("sql", sql, params);
      return true;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error executing SQL trigger '%s': %s", e, triggerName, e.getMessage());
      throw new TriggerExecutionException("SQL trigger '" + triggerName + "' failed: " + e.getMessage(), e);
    }
  }

  @Override
  public void close() {
    // No resources to cleanup for SQL executor
  }
}
