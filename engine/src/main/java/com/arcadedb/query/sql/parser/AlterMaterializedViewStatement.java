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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.MaterializedViewRefreshMode;

import java.util.Map;

public class AlterMaterializedViewStatement extends DDLStatement {
  public Identifier name;
  public String refreshMode;
  public int refreshInterval;
  public String refreshUnit;

  public AlterMaterializedViewStatement() {
  }

  /**
   * Batchable into a DDL script's bulk schema scope (issue #6990). Only the refresh mode and interval change here. The refresh itself is REFRESH MATERIALIZED VIEW, which does not opt in.
   */
  @Override
  public boolean isBulkSchemaScopeSafe(final DatabaseInternal database) {
    return true;
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database database = context.getDatabase();
    final String viewName = name.getStringValue();

    final MaterializedViewRefreshMode mode;
    if ("INCREMENTAL".equalsIgnoreCase(refreshMode))
      mode = MaterializedViewRefreshMode.INCREMENTAL;
    else if ("PERIODIC".equalsIgnoreCase(refreshMode))
      mode = MaterializedViewRefreshMode.PERIODIC;
    else
      mode = MaterializedViewRefreshMode.MANUAL;

    long intervalMs = 0;
    if (mode == MaterializedViewRefreshMode.PERIODIC && refreshInterval > 0) {
      intervalMs = refreshInterval * 1000L; // default seconds
      if ("MINUTE".equalsIgnoreCase(refreshUnit))
        intervalMs = refreshInterval * 60_000L;
      else if ("HOUR".equalsIgnoreCase(refreshUnit))
        intervalMs = refreshInterval * 3_600_000L;
    }

    database.getSchema().alterMaterializedView(viewName, mode, intervalMs);

    final InternalResultSet result = new InternalResultSet();
    final ResultInternal r = new ResultInternal();
    r.setProperty("operation", "alter materialized view");
    r.setProperty("name", viewName);
    r.setProperty("refreshMode", mode.name());
    result.add(r);
    return result;
  }

  /**
   * Overrides the two-arg form (not just the no-arg debug one), same fix as {@link CreateMaterializedViewStatement}
   * and for the same reason: only that override lets this render as SQL inside an enclosing {@code IF}/script block
   * instead of throwing {@code UnsupportedOperationException}.
   */
  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("ALTER MATERIALIZED VIEW ");
    name.toString(params, builder);
    builder.append(" REFRESH ");
    if ("PERIODIC".equalsIgnoreCase(refreshMode) && refreshInterval > 0)
      builder.append("EVERY ").append(refreshInterval).append(' ').append(refreshUnit);
    else
      builder.append(refreshMode);
  }

  @Override
  public AlterMaterializedViewStatement copy() {
    final AlterMaterializedViewStatement result = new AlterMaterializedViewStatement();
    result.name = name == null ? null : name.copy();
    result.refreshMode = refreshMode;
    result.refreshInterval = refreshInterval;
    result.refreshUnit = refreshUnit;
    return result;
  }

  @Override
  protected Object[] getIdentityElements() {
    return new Object[] { name, refreshMode, refreshInterval, refreshUnit };
  }
}
