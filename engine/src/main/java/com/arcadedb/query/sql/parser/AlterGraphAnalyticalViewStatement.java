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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;

public class AlterGraphAnalyticalViewStatement extends DDLStatement {
  public Identifier name;
  public String     updateModeStr;

  public AlterGraphAnalyticalViewStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database database = context.getDatabase();
    final String viewName = name.getStringValue();

    // Validate the view exists
    final JSONObject allGavs = database.getSchema().getExtension("graphAnalyticalViews");
    if (allGavs == null || !allGavs.has(viewName))
      throw new CommandExecutionException("Graph Analytical View '" + viewName + "' does not exist");

    final GraphAnalyticalView.UpdateMode newMode = GraphAnalyticalView.UpdateMode.valueOf(updateModeStr.toUpperCase());

    // Update the persisted definition
    final JSONObject gavDef = allGavs.getJSONObject(viewName);
    gavDef.put("updateMode", newMode.name());
    database.getSchema().setExtension("graphAnalyticalViews", allGavs);

    // Update the live view if it exists in the registry
    final GraphAnalyticalView liveView = GraphAnalyticalViewRegistry.get(database, viewName);
    if (liveView != null)
      liveView.setUpdateMode(newMode);

    final InternalResultSet result = new InternalResultSet();
    final ResultInternal r = new ResultInternal();
    r.setProperty("operation", "alter graph analytical view");
    r.setProperty("name", viewName);
    r.setProperty("updateMode", newMode.name());
    result.add(r);
    return result;
  }

  @Override
  public String toString() {
    return "ALTER GRAPH ANALYTICAL VIEW " + name + " UPDATE MODE " + updateModeStr.toUpperCase();
  }
}
