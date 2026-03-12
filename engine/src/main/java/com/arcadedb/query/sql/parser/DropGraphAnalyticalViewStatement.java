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
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewPersistence;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;

public class DropGraphAnalyticalViewStatement extends DDLStatement {
  public Identifier name;
  public boolean    ifExists = false;

  public DropGraphAnalyticalViewStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database database = context.getDatabase();
    final String viewName = name.getStringValue();

    // Check if exists in schema extensions
    final JSONObject allGavs = database.getSchema().getExtension(GraphAnalyticalViewPersistence.EXTENSION_KEY);
    final boolean existsInSchema = allGavs != null && allGavs.has(viewName);

    if (!existsInSchema) {
      if (ifExists) {
        final InternalResultSet result = new InternalResultSet();
        final ResultInternal r = new ResultInternal();
        r.setProperty("operation", "drop graph analytical view");
        r.setProperty("name", viewName);
        r.setProperty("dropped", false);
        result.add(r);
        return result;
      }
      throw new CommandExecutionException("Graph Analytical View '" + viewName + "' does not exist");
    }

    // Drop the in-memory view if it exists (also removes from schema)
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, viewName);
    if (view != null) {
      view.drop();
    } else {
      // No in-memory view, remove directly from schema extensions
      allGavs.remove(viewName);
      if (allGavs.isEmpty())
        database.getSchema().setExtension(GraphAnalyticalViewPersistence.EXTENSION_KEY, null);
      else
        database.getSchema().setExtension(GraphAnalyticalViewPersistence.EXTENSION_KEY, allGavs);

      // Unregister any orphaned traversal provider with this name
      for (final GraphTraversalProvider provider : GraphTraversalProviderRegistry.getProviders(database))
        if (viewName.equals(provider.getName()))
          GraphTraversalProviderRegistry.unregister(database, provider);
    }

    final InternalResultSet result = new InternalResultSet();
    final ResultInternal r = new ResultInternal();
    r.setProperty("operation", "drop graph analytical view");
    r.setProperty("name", viewName);
    r.setProperty("dropped", true);
    result.add(r);
    return result;
  }

  @Override
  public String toString() {
    return "DROP GRAPH ANALYTICAL VIEW " + (ifExists ? "IF EXISTS " : "") + name;
  }
}
