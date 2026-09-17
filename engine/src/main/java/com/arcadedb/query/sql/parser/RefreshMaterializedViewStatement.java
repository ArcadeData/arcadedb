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

import java.util.Map;

public class RefreshMaterializedViewStatement extends DDLStatement {
  public Identifier name;

  public RefreshMaterializedViewStatement() {
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database database = context.getDatabase();
    final String viewName = name.getStringValue();

    database.getSchema().getMaterializedView(viewName).refresh();

    final InternalResultSet result = new InternalResultSet();
    final ResultInternal r = new ResultInternal();
    r.setProperty("operation", "refresh materialized view");
    r.setProperty("name", viewName);
    result.add(r);
    return result;
  }

  /** Overrides the two-arg form, not just the no-arg one - see {@link CreateMaterializedViewStatement} (issue #7800/#7794). */
  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("REFRESH MATERIALIZED VIEW ");
    name.toString(params, builder);
  }

  @Override
  public RefreshMaterializedViewStatement copy() {
    final RefreshMaterializedViewStatement result = new RefreshMaterializedViewStatement();
    result.name = name == null ? null : name.copy();
    return result;
  }
}
