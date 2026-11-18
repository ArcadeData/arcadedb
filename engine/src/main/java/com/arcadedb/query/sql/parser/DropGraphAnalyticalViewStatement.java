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

import com.arcadedb.graph.olap.GraphAnalyticalViewCommands;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Map;

public class DropGraphAnalyticalViewStatement extends DDLStatement {
  public Identifier name;
  public boolean    ifExists = false;

  public DropGraphAnalyticalViewStatement() {
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final String viewName = name.getStringValue();
    final boolean dropped = GraphAnalyticalViewCommands.drop(context.getDatabase(), viewName, ifExists);

    final InternalResultSet result = new InternalResultSet();
    final ResultInternal r = new ResultInternal();
    r.setProperty("operation", "drop graph analytical view");
    r.setProperty("name", viewName);
    r.setProperty("dropped", dropped);
    result.add(r);
    return result;
  }

  /** Overrides the two-arg form, not just the no-arg one - see {@link CreateMaterializedViewStatement} (issue #7800/#7794). */
  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("DROP GRAPH ANALYTICAL VIEW ");
    if (ifExists)
      builder.append("IF EXISTS ");
    name.toString(params, builder);
  }

  @Override
  public DropGraphAnalyticalViewStatement copy() {
    final DropGraphAnalyticalViewStatement result = new DropGraphAnalyticalViewStatement();
    result.name = name == null ? null : name.copy();
    result.ifExists = ifExists;
    return result;
  }

  @Override
  protected Object[] getIdentityElements() {
    return new Object[] { name, ifExists };
  }
}
