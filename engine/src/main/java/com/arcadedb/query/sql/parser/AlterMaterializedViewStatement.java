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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.MaterializedViewRefreshMode;

public class AlterMaterializedViewStatement extends DDLStatement {
  public Identifier name;
  public String refreshMode;
  public int refreshInterval;
  public String refreshUnit;

  public AlterMaterializedViewStatement(final int id) {
    super(id);
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

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ALTER MATERIALIZED VIEW ");
    sb.append(name).append(" REFRESH ");
    if ("PERIODIC".equalsIgnoreCase(refreshMode) && refreshInterval > 0)
      sb.append("EVERY ").append(refreshInterval).append(' ').append(refreshUnit);
    else
      sb.append(refreshMode);
    return sb.toString();
  }
}
