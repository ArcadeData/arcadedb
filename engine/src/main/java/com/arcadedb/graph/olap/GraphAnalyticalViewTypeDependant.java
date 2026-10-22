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
package com.arcadedb.graph.olap;

import com.arcadedb.database.Database;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.TypeNameDependant;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * A Graph Analytical View as a {@link TypeNameDependant} (issue #10368). The view's definition, persisted in the schema
 * extension {@code graphAnalyticalViews} and restored from it on every open, names its vertex types, its edge types and the
 * edge types of its contraction hierarchies: a type it lists cannot be dropped, the next open's build would fail on it and
 * leave the view unbuilt for good, and a rename of one is carried to it. The carry is always exact, a definition names its
 * types as a list rather than inside a query, so there is never a rename to refuse.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class GraphAnalyticalViewTypeDependant implements TypeNameDependant {
  private static final String KIND = "graph analytical view";

  private final Database   database;
  private final String     name;
  private final JSONObject definition;

  GraphAnalyticalViewTypeDependant(final Database database, final String name, final JSONObject definition) {
    this.database = database;
    this.name = name;
    this.definition = definition;
  }

  @Override
  public void checkTypeDrop(final LocalSchema schema, final String typeName, final String canonicalTypeName) {
    final List<String> vertexTypes = names(definition.getJSONArray("vertexTypes", null));
    final List<String> edgeTypes = edgeTypeNames();
    final List<String> all = new ArrayList<>(vertexTypes.size() + edgeTypes.size());
    all.addAll(vertexTypes);
    all.addAll(edgeTypes);
    TypeNameDependant.checkNamesResolve(schema, typeName, KIND, name, dropStatement(), all);

    if (namesType(schema, vertexTypes, canonicalTypeName))
      throw new SchemaException("Cannot drop type '" + typeName + "' because it is a vertex type of graph analytical view '" + name
          + "'. Drop the graph analytical view first with: " + dropStatement());
    if (namesType(schema, edgeTypes, canonicalTypeName))
      throw new SchemaException("Cannot drop type '" + typeName + "' because it is an edge type of graph analytical view '" + name
          + "'. Drop the graph analytical view first with: " + dropStatement());
  }

  /**
   * Rewrites the definition with the new name and replaces the live view with one built from it: the live view's snapshot, its
   * overlay and its persisted CSR are all keyed by the old name, so it is retired rather than patched, and the new one is built
   * in the background. Until it is ready, queries take the record-by-record path, as for a view just created.
   */
  @Override
  public void carryTypeRename(final LocalSchema schema, final String oldName, final String newName) {
    final JSONObject renamed = definition.copy();
    boolean changed = rename(renamed.getJSONArray("vertexTypes", null), oldName, newName);
    changed |= rename(renamed.getJSONArray("edgeTypes", null), oldName, newName);
    final JSONArray hierarchies = renamed.getJSONArray("contractionHierarchies", null);
    if (hierarchies != null)
      for (int i = 0; i < hierarchies.length(); i++)
        changed |= rename(hierarchies.getJSONObject(i).getJSONArray("edgeTypes", null), oldName, newName);
    if (!changed)
      return;

    final JSONObject allGavs = schema.getExtension(GraphAnalyticalViewPersistence.EXTENSION_KEY);
    allGavs.put(name, renamed);
    schema.setExtension(GraphAnalyticalViewPersistence.EXTENSION_KEY, allGavs);

    // The definition above is the part that has to be right, and it is saved with the schema by the session publishing the
    // rename. Replacing the live view is not allowed to fail the rename, whose files have moved by now: a view that cannot be
    // replaced here is built from the renamed definition on the next open, or by REBUILD GRAPH ANALYTICAL VIEW
    try {
      final GraphAnalyticalView live = GraphAnalyticalViewRegistry.get(database, name);
      if (live != null)
        live.retireForTypeRename();
      GraphAnalyticalViewPersistence.builderFor(database, name, renamed).skipPersistence().buildAsync();
    } catch (final RuntimeException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot rebuild graph analytical view '%s' after the rename of type '%s' to '%s': it is built again on the next open, "
              + "or by REBUILD GRAPH ANALYTICAL VIEW %s", e, name, oldName, newName, name);
    }
  }

  private String dropStatement() {
    return "DROP GRAPH ANALYTICAL VIEW " + name;
  }

  /** The edge types of the view and of its contraction hierarchies. */
  private List<String> edgeTypeNames() {
    final List<String> edgeTypes = new ArrayList<>(names(definition.getJSONArray("edgeTypes", null)));
    final JSONArray hierarchies = definition.getJSONArray("contractionHierarchies", null);
    if (hierarchies != null)
      for (int i = 0; i < hierarchies.length(); i++)
        edgeTypes.addAll(names(hierarchies.getJSONObject(i).getJSONArray("edgeTypes", null)));
    return edgeTypes;
  }

  private static boolean namesType(final LocalSchema schema, final List<String> typeNames, final String canonicalTypeName) {
    for (final String typeName : typeNames)
      if (canonicalTypeName.equals(TypeNameDependant.canonicalNameOf(schema, typeName)))
        return true;
    return false;
  }

  private static List<String> names(final JSONArray array) {
    if (array == null)
      return List.of();
    final List<String> names = new ArrayList<>(array.length());
    for (int i = 0; i < array.length(); i++)
      names.add(array.getString(i));
    return names;
  }

  /** Replaces {@code oldName} with {@code newName} in {@code array}, in place: a name stored as an alias stays as it is. */
  private static boolean rename(final JSONArray array, final String oldName, final String newName) {
    if (array == null)
      return false;
    boolean changed = false;
    for (int i = 0; i < array.length(); i++)
      if (oldName.equals(array.getString(i))) {
        array.put(i, newName);
        changed = true;
      }
    return changed;
  }
}
