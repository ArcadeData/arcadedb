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

public class CreateMaterializedViewStatement extends DDLStatement {
  public Identifier name;
  public SelectStatement selectStatement;
  public String refreshMode;  // MANUAL, INCREMENTAL, or null (default=MANUAL)
  public int refreshInterval; // for EVERY N SECOND/MINUTE/HOUR
  public String refreshUnit;  // SECOND, MINUTE, HOUR
  public int buckets;
  public boolean ifNotExists = false;

  public CreateMaterializedViewStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database database = context.getDatabase();
    final String viewName = name.getStringValue();

    final MaterializedViewRefreshMode mode;
    if (refreshMode == null || "MANUAL".equalsIgnoreCase(refreshMode))
      mode = MaterializedViewRefreshMode.MANUAL;
    else if ("INCREMENTAL".equalsIgnoreCase(refreshMode))
      mode = MaterializedViewRefreshMode.INCREMENTAL;
    else
      mode = MaterializedViewRefreshMode.PERIODIC;

    final var builder = database.getSchema().buildMaterializedView()
        .withName(viewName)
        .withQuery(selectStatement.toString())
        .withRefreshMode(mode)
        .withIgnoreIfExists(ifNotExists);

    if (buckets > 0)
      builder.withTotalBuckets(buckets);

    if (mode == MaterializedViewRefreshMode.PERIODIC && refreshInterval > 0) {
      long intervalMs = refreshInterval * 1000L; // default seconds
      if ("MINUTE".equalsIgnoreCase(refreshUnit))
        intervalMs = refreshInterval * 60_000L;
      else if ("HOUR".equalsIgnoreCase(refreshUnit))
        intervalMs = refreshInterval * 3_600_000L;
      builder.withRefreshInterval(intervalMs);
    }

    builder.create();

    final InternalResultSet result = new InternalResultSet();
    final ResultInternal r = new ResultInternal();
    r.setProperty("operation", "create materialized view");
    r.setProperty("name", viewName);
    result.add(r);
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("CREATE MATERIALIZED VIEW ");
    if (ifNotExists)
      sb.append("IF NOT EXISTS ");
    sb.append(name);
    sb.append(" AS ").append(selectStatement);
    if (refreshMode != null) {
      sb.append(" REFRESH ").append(refreshMode);
      if ("PERIODIC".equalsIgnoreCase(refreshMode) || refreshInterval > 0)
        sb.append(" EVERY ").append(refreshInterval).append(' ').append(refreshUnit);
    }
    if (buckets > 0)
      sb.append(" BUCKETS ").append(buckets);
    return sb.toString();
  }
}
