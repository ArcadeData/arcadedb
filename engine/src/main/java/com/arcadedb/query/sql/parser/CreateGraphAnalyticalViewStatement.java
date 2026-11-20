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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewCommands;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CreateGraphAnalyticalViewStatement extends DDLStatement {
  public Identifier   name;
  public Identifier[] vertexTypes;
  public Identifier[] edgeTypes;
  public Identifier[] properties;
  public Identifier[] edgeProperties;
  public String       updateModeStr;          // "OFF", "SYNCHRONOUS", "ASYNCHRONOUS"
  public int          compactionThreshold = -1; // -1 means not set (use default)
  public Identifier[] cchWeights;               // weight properties to keep a contraction hierarchy for
  public boolean      ifNotExists  = false;

  public CreateGraphAnalyticalViewStatement() {
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final DatabaseInternal database = context.getDatabase();
    final String viewName = name.getStringValue();

    // toStringArray() maps an empty array to null, so PROPERTIES () is kept explicitly: it copies no vertex properties
    // while an omitted clause copies all (issue #9831)
    final String[] propertyArray = properties == null ? null : properties.length == 0 ? new String[0] : toStringArray(properties);
    final GraphAnalyticalViewCommands.Definition definition = new GraphAnalyticalViewCommands.Definition(viewName,
        toStringArray(vertexTypes), toStringArray(edgeTypes), propertyArray, toStringArray(edgeProperties), resolveUpdateMode(),
        compactionThreshold, toStringArray(cchWeights));
    final boolean created = GraphAnalyticalViewCommands.create(database, definition, ifNotExists) != null;

    final InternalResultSet result = new InternalResultSet();
    final ResultInternal r = new ResultInternal();
    r.setProperty("operation", "create graph analytical view");
    r.setProperty("name", viewName);
    r.setProperty("created", created);
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

  /** Overrides the two-arg form, not just the no-arg one - see {@link CreateMaterializedViewStatement} (issue #7800/#7794). */
  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("CREATE GRAPH ANALYTICAL VIEW ");
    if (ifNotExists)
      builder.append("IF NOT EXISTS ");
    name.toString(params, builder);
    if (vertexTypes != null && vertexTypes.length > 0) {
      builder.append(" VERTEX TYPES (");
      appendIdentifiers(params, builder, vertexTypes);
      builder.append(')');
    }
    if (edgeTypes != null && edgeTypes.length > 0) {
      builder.append(" EDGE TYPES (");
      appendIdentifiers(params, builder, edgeTypes);
      builder.append(')');
    }
    if (properties != null) {
      builder.append(" PROPERTIES (");
      appendIdentifiers(params, builder, properties);
      builder.append(')');
    }
    if (edgeProperties != null && edgeProperties.length > 0) {
      builder.append(" EDGE PROPERTIES (");
      appendIdentifiers(params, builder, edgeProperties);
      builder.append(')');
    }
    final GraphAnalyticalView.UpdateMode mode = resolveUpdateMode();
    if (mode != GraphAnalyticalView.UpdateMode.OFF)
      builder.append(" UPDATE MODE ").append(mode.name());
    if (compactionThreshold >= 0)
      builder.append(" COMPACTION THRESHOLD ").append(compactionThreshold);
    if (cchWeights != null && cchWeights.length > 0) {
      builder.append(" CCH (");
      appendIdentifiers(params, builder, cchWeights);
      builder.append(')');
    }
  }

  private GraphAnalyticalView.UpdateMode resolveUpdateMode() {
    return updateModeStr != null ? GraphAnalyticalViewCommands.parseUpdateMode(updateModeStr) : GraphAnalyticalView.UpdateMode.OFF;
  }

  private static void appendIdentifiers(final Map<String, Object> params, final StringBuilder builder, final Identifier[] ids) {
    for (int i = 0; i < ids.length; i++) {
      if (i > 0)
        builder.append(", ");
      ids[i].toString(params, builder);
    }
  }

  private static Identifier[] copyIdentifiers(final Identifier[] ids) {
    if (ids == null)
      return null;
    final Identifier[] result = new Identifier[ids.length];
    for (int i = 0; i < ids.length; i++)
      result[i] = ids[i].copy();
    return result;
  }

  @Override
  public CreateGraphAnalyticalViewStatement copy() {
    final CreateGraphAnalyticalViewStatement result = new CreateGraphAnalyticalViewStatement();
    result.name = name == null ? null : name.copy();
    result.vertexTypes = copyIdentifiers(vertexTypes);
    result.edgeTypes = copyIdentifiers(edgeTypes);
    result.properties = copyIdentifiers(properties);
    result.edgeProperties = copyIdentifiers(edgeProperties);
    result.updateModeStr = updateModeStr;
    result.compactionThreshold = compactionThreshold;
    result.cchWeights = copyIdentifiers(cchWeights);
    result.ifNotExists = ifNotExists;
    return result;
  }

  /**
   * The array fields are wrapped in {@code Arrays.asList()} rather than compared as-is: {@code Object[].equals()}
   * is reference identity, so two structurally-equal arrays from an original and its {@code copy()} would never
   * compare equal otherwise. {@code null} is normalized to an empty list so a never-set clause does not need special
   * casing, except for {@code properties}: there no clause copies every vertex property and {@code PROPERTIES ()}
   * copies none, so the two must not compare equal (issue #9831).
   */
  @Override
  protected Object[] getIdentityElements() {
    return new Object[] {
        name,
        vertexTypes == null ? List.of() : Arrays.asList(vertexTypes),
        edgeTypes == null ? List.of() : Arrays.asList(edgeTypes),
        properties == null ? null : Arrays.asList(properties),
        edgeProperties == null ? List.of() : Arrays.asList(edgeProperties),
        updateModeStr, compactionThreshold, ifNotExists,
        cchWeights == null ? List.of() : Arrays.asList(cchWeights) };
  }
}
