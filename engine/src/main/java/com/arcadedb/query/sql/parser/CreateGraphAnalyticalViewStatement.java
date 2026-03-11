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
import com.arcadedb.graph.olap.GraphAnalyticalViewBuilder;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

public class CreateGraphAnalyticalViewStatement extends DDLStatement {
  public Identifier   name;
  public Identifier[] vertexTypes;
  public Identifier[] edgeTypes;
  public Identifier[] properties;
  public String       updateModeStr;          // "OFF", "SYNCHRONOUS", "ASYNCHRONOUS"
  public int          compactionThreshold = -1; // -1 means not set (use default)
  public boolean      ifNotExists  = false;

  public CreateGraphAnalyticalViewStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database database = context.getDatabase();
    final String viewName = name.getStringValue();

    // Check if already exists
    final JSONObject allGavs = database.getSchema().getExtension("graphAnalyticalViews");
    if (allGavs != null && allGavs.has(viewName)) {
      if (ifNotExists) {
        final InternalResultSet result = new InternalResultSet();
        final ResultInternal r = new ResultInternal();
        r.setProperty("operation", "create graph analytical view");
        r.setProperty("name", viewName);
        r.setProperty("created", false);
        result.add(r);
        return result;
      }
      throw new CommandExecutionException("Graph Analytical View '" + viewName + "' already exists");
    }

    final String[] vtArray = toStringArray(vertexTypes);
    final String[] etArray = toStringArray(edgeTypes);
    final String[] propArray = toStringArray(properties);

    // Persist the definition to schema extensions
    JSONObject gavDefs = allGavs != null ? allGavs : new JSONObject();
    final JSONObject def = new JSONObject();
    def.put("name", viewName);
    if (vtArray != null)
      def.put("vertexTypes", toJsonArray(vtArray));
    if (etArray != null)
      def.put("edgeTypes", toJsonArray(etArray));
    if (propArray != null)
      def.put("propertyFilter", toJsonArray(propArray));
    def.put("updateMode", resolveUpdateMode().name());
    if (compactionThreshold > 0)
      def.put("compactionThreshold", compactionThreshold);
    gavDefs.put(viewName, def);
    database.getSchema().setExtension("graphAnalyticalViews", gavDefs);

    // Trigger the actual CSR build
    final GraphAnalyticalViewBuilder builder = GraphAnalyticalView.builder(database).withName(viewName);
    if (vtArray != null && vtArray.length > 0)
      builder.withVertexTypes(vtArray);
    if (etArray != null && etArray.length > 0)
      builder.withEdgeTypes(etArray);
    if (propArray != null && propArray.length > 0)
      builder.withProperties(propArray);
    builder.withUpdateMode(resolveUpdateMode());
    if (compactionThreshold > 0)
      builder.withCompactionThreshold(compactionThreshold);
    builder.buildAsync();

    final InternalResultSet result = new InternalResultSet();
    final ResultInternal r = new ResultInternal();
    r.setProperty("operation", "create graph analytical view");
    r.setProperty("name", viewName);
    r.setProperty("created", true);
    result.add(r);
    return result;
  }

  private static String[] toStringArray(final Identifier[] identifiers) {
    if (identifiers == null || identifiers.length == 0)
      return null;
    final String[] result = new String[identifiers.length];
    for (int i = 0; i < identifiers.length; i++)
      result[i] = identifiers[i].getStringValue();
    return result;
  }

  private static JSONArray toJsonArray(final String[] values) {
    final JSONArray arr = new JSONArray();
    for (final String v : values)
      arr.put(v);
    return arr;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("CREATE GRAPH ANALYTICAL VIEW ");
    if (ifNotExists)
      sb.append("IF NOT EXISTS ");
    sb.append(name);
    if (vertexTypes != null && vertexTypes.length > 0) {
      sb.append(" VERTEX TYPES (");
      appendIdentifiers(sb, vertexTypes);
      sb.append(')');
    }
    if (edgeTypes != null && edgeTypes.length > 0) {
      sb.append(" EDGE TYPES (");
      appendIdentifiers(sb, edgeTypes);
      sb.append(')');
    }
    if (properties != null && properties.length > 0) {
      sb.append(" PROPERTIES (");
      appendIdentifiers(sb, properties);
      sb.append(')');
    }
    final GraphAnalyticalView.UpdateMode mode = resolveUpdateMode();
    if (mode != GraphAnalyticalView.UpdateMode.OFF)
      sb.append(" UPDATE MODE ").append(mode.name());
    if (compactionThreshold > 0)
      sb.append(" COMPACTION THRESHOLD ").append(compactionThreshold);
    return sb.toString();
  }

  private GraphAnalyticalView.UpdateMode resolveUpdateMode() {
    if (updateModeStr != null)
      return GraphAnalyticalView.UpdateMode.valueOf(updateModeStr.toUpperCase());
    return GraphAnalyticalView.UpdateMode.OFF;
  }

  private static void appendIdentifiers(final StringBuilder sb, final Identifier[] ids) {
    for (int i = 0; i < ids.length; i++) {
      if (i > 0)
        sb.append(", ");
      sb.append(ids[i]);
    }
  }
}
