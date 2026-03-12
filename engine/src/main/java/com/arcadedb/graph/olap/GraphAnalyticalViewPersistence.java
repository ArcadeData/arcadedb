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
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * Handles persistence and restoration of {@link GraphAnalyticalView} definitions
 * in the database schema.
 * <p>
 * GAV definitions are stored in the schema's extension mechanism under the key
 * {@code "graphAnalyticalViews"}. Each named GAV stores its configuration
 * (vertex types, edge types, property filter, auto-update) as JSON.
 * <p>
 * Usage:
 * <pre>
 *   // After opening a database, restore all previously saved GAVs:
 *   GraphAnalyticalViewPersistence.restoreAll(database);
 *
 *   // This rebuilds all GAVs asynchronously from their saved definitions.
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphAnalyticalViewPersistence {
  static final String EXTENSION_KEY = "graphAnalyticalViews";

  /**
   * Saves a GAV definition to the schema.
   * Called automatically by the builder when a name is set.
   */
  public static void save(final Database database, final GraphAnalyticalView view) {
    if (view.getName() == null)
      throw new IllegalArgumentException("Cannot persist a GraphAnalyticalView without a name");

    JSONObject allGavs = database.getSchema().getExtension(EXTENSION_KEY);
    if (allGavs == null)
      allGavs = new JSONObject();

    allGavs.put(view.getName(), toJSON(view));
    database.getSchema().setExtension(EXTENSION_KEY, allGavs);
  }

  /**
   * Removes a GAV definition from the schema.
   */
  public static void remove(final Database database, final String name) {
    final JSONObject allGavs = database.getSchema().getExtension(EXTENSION_KEY);
    if (allGavs != null && allGavs.has(name)) {
      allGavs.remove(name);
      if (allGavs.isEmpty())
        database.getSchema().setExtension(EXTENSION_KEY, null);
      else
        database.getSchema().setExtension(EXTENSION_KEY, allGavs);
    }
  }

  /**
   * Restores all GAV definitions from the schema and rebuilds them asynchronously.
   * Call this after opening a database to restore previously saved GAVs.
   *
   * @return the number of GAVs being restored
   */
  public static int restoreAll(final Database database) {
    final JSONObject allGavs = database.getSchema().getExtension(EXTENSION_KEY);
    if (allGavs == null || allGavs.isEmpty())
      return 0;

    // Clear any stale entries from a previous lifecycle of the same database path
    GraphAnalyticalViewRegistry.clearAll(database);

    int count = 0;
    List<String> failedKeys = null;
    for (final String gavName : allGavs.keySet()) {
      try {
        final JSONObject gavDef = allGavs.getJSONObject(gavName);
        final GraphAnalyticalViewBuilder builder = GraphAnalyticalView.builder(database).withName(gavName);

        if (gavDef.has("vertexTypes")) {
          final JSONArray vt = gavDef.getJSONArray("vertexTypes");
          final String[] types = new String[vt.length()];
          for (int i = 0; i < vt.length(); i++)
            types[i] = vt.getString(i);
          builder.withVertexTypes(types);
        }

        if (gavDef.has("edgeTypes")) {
          final JSONArray et = gavDef.getJSONArray("edgeTypes");
          final String[] types = new String[et.length()];
          for (int i = 0; i < et.length(); i++)
            types[i] = et.getString(i);
          builder.withEdgeTypes(types);
        }

        if (gavDef.has("propertyFilter")) {
          final JSONArray pf = gavDef.getJSONArray("propertyFilter");
          final String[] props = new String[pf.length()];
          for (int i = 0; i < pf.length(); i++)
            props[i] = pf.getString(i);
          builder.withProperties(props);
        }

        final String updateModeStr = gavDef.getString("updateMode", "OFF");
        builder.withUpdateMode(GraphAnalyticalView.UpdateMode.valueOf(updateModeStr));
        final int ct = gavDef.getInt("compactionThreshold", -1);
        if (ct >= 0)
          builder.withCompactionThreshold(ct);
        builder.skipPersistence().buildAsync();
        count++;

        LogManager.instance().log(GraphAnalyticalViewPersistence.class, Level.INFO,
            "Restoring GraphAnalyticalView '%s' (async)", gavName);

      } catch (final Exception e) {
        LogManager.instance().log(GraphAnalyticalViewPersistence.class, Level.WARNING,
            "Failed to restore GraphAnalyticalView '%s', removing corrupted definition from schema", e, gavName);
        if (failedKeys == null)
          failedKeys = new ArrayList<>();
        failedKeys.add(gavName);
      }
    }

    // Remove corrupted definitions after iteration to avoid ConcurrentModificationException
    if (failedKeys != null) {
      for (final String failedKey : failedKeys)
        allGavs.remove(failedKey);
      try {
        if (allGavs.isEmpty())
          database.getSchema().setExtension(EXTENSION_KEY, null);
        else
          database.getSchema().setExtension(EXTENSION_KEY, allGavs);
      } catch (final Exception cleanupEx) {
        LogManager.instance().log(GraphAnalyticalViewPersistence.class, Level.WARNING,
            "Failed to remove corrupted GraphAnalyticalView definitions from schema", cleanupEx);
      }
    }

    if (count > 0)
      LogManager.instance().log(GraphAnalyticalViewPersistence.class, Level.INFO,
          "Restoring %d Graph Analytical View(s) asynchronously on database open", count);

    return count;
  }

  /**
   * Returns all persisted GAV definitions from the schema.
   */
  public static Map<String, Object> getDefinitions(final Database database) {
    final JSONObject allGavs = database.getSchema().getExtension(EXTENSION_KEY);
    return allGavs != null ? allGavs.toMap() : Map.of();
  }

  static JSONObject toJSON(final GraphAnalyticalView view) {
    final JSONObject json = new JSONObject();
    json.put("name", view.getName());

    if (view.getVertexTypes() != null) {
      final JSONArray vt = new JSONArray();
      for (final String t : view.getVertexTypes())
        vt.put(t);
      json.put("vertexTypes", vt);
    }

    if (view.getEdgeTypeFilter() != null) {
      final JSONArray et = new JSONArray();
      for (final String t : view.getEdgeTypeFilter())
        et.put(t);
      json.put("edgeTypes", et);
    }

    if (view.getPropertyFilter() != null) {
      final JSONArray pf = new JSONArray();
      for (final String p : view.getPropertyFilter())
        pf.put(p);
      json.put("propertyFilter", pf);
    }

    json.put("updateMode", view.getUpdateMode().name());
    if (view.getCompactionThreshold() != 10000)
      json.put("compactionThreshold", view.getCompactionThreshold());
    return json;
  }
}
