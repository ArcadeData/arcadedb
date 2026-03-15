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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewPersistence;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONObject;

public class AlterGraphAnalyticalViewStatement extends DDLStatement {
  public Identifier name;
  public String     updateModeStr;
  public int        compactionThreshold = -1; // -1 means not set

  public AlterGraphAnalyticalViewStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final DatabaseInternal database = context.getDatabase();
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (updateModeStr == null && compactionThreshold < 0)
      throw new CommandExecutionException("ALTER GRAPH ANALYTICAL VIEW requires at least one parameter (UPDATE MODE or COMPACTION THRESHOLD)");

    final String viewName = name.getStringValue();

    // Validate the view exists
    final JSONObject allGavs = database.getSchema().getExtension(GraphAnalyticalViewPersistence.EXTENSION_KEY);
    if (allGavs == null || !allGavs.has(viewName))
      throw new CommandExecutionException("Graph Analytical View '" + viewName + "' does not exist");

    final JSONObject gavDef = allGavs.getJSONObject(viewName);
    final InternalResultSet result = new InternalResultSet();
    final ResultInternal r = new ResultInternal();
    r.setProperty("operation", "alter graph analytical view");
    r.setProperty("name", viewName);

    final GraphAnalyticalView liveView = GraphAnalyticalViewRegistry.get(database, viewName);

    // Validate and apply to live view FIRST — if this throws, schema remains unchanged
    GraphAnalyticalView.UpdateMode newMode = null;
    if (updateModeStr != null) {
      try {
        newMode = GraphAnalyticalView.UpdateMode.valueOf(updateModeStr.toUpperCase());
      } catch (final IllegalArgumentException e) {
        throw new CommandExecutionException(
            "Unknown update mode: '" + updateModeStr + "'. Valid values: OFF, SYNCHRONOUS, ASYNCHRONOUS");
      }
      if (liveView != null)
        liveView.setUpdateMode(newMode);
      gavDef.put("updateMode", newMode.name());
      r.setProperty("updateMode", newMode.name());
    }

    if (compactionThreshold >= 0) {
      if (liveView != null)
        liveView.setCompactionThreshold(compactionThreshold);
      gavDef.put("compactionThreshold", compactionThreshold);
      r.setProperty("compactionThreshold", compactionThreshold);
    }

    // Persist to schema after live view is successfully updated
    database.getSchema().setExtension(GraphAnalyticalViewPersistence.EXTENSION_KEY, allGavs);

    result.add(r);
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ALTER GRAPH ANALYTICAL VIEW ");
    sb.append(name);
    if (updateModeStr != null)
      sb.append(" UPDATE MODE ").append(updateModeStr.toUpperCase());
    if (compactionThreshold >= 0)
      sb.append(" COMPACTION THRESHOLD ").append(compactionThreshold);
    return sb.toString();
  }
}
