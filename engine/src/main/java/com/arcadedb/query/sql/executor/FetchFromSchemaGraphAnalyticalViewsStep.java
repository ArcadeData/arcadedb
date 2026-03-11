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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Database;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;

/**
 * Returns an Result containing metadata regarding the graph analytical views.
 */
public class FetchFromSchemaGraphAnalyticalViewsStep extends AbstractExecutionStep {

  private final List<ResultInternal> result = new ArrayList<>();

  private int cursor = 0;

  public FetchFromSchemaGraphAnalyticalViewsStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    if (cursor == 0) {
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        final Database database = context.getDatabase();
        final JSONObject extension = database.getSchema().getExtension("graphAnalyticalViews");
        if (extension != null) {
          final boolean hasProvider = !GraphTraversalProviderRegistry.getProviders(database).isEmpty();

          final List<String> names = new ArrayList<>(extension.keySet());
          Collections.sort(names, String::compareToIgnoreCase);

          for (final String name : names) {
            final JSONObject gavDef = extension.getJSONObject(name);
            final ResultInternal r = new ResultInternal(database);
            result.add(r);

            r.setProperty("name", gavDef.getString("name"));
            r.setProperty("vertexTypes", jsonArrayToList(gavDef.getJSONArray("vertexTypes", null)));
            r.setProperty("edgeTypes", jsonArrayToList(gavDef.getJSONArray("edgeTypes", null)));
            r.setProperty("propertyFilter", jsonArrayToList(gavDef.getJSONArray("propertyFilter", null)));
            r.setProperty("autoUpdate", gavDef.getBoolean("autoUpdate", false));

            // Enrich with live stats from in-memory registry
            final GraphAnalyticalView liveView = GraphAnalyticalViewRegistry.get(database, name);
            if (liveView != null && liveView.isBuilt()) {
              r.setProperty("status", liveView.getStatus().name());
              r.setProperty("nodeCount", liveView.getNodeCount());
              r.setProperty("edgeCount", liveView.getEdgeCount());
              r.setProperty("memoryUsageBytes", liveView.getMemoryUsageBytes());
              r.setProperty("buildTimestamp", liveView.getBuildTimestamp());
            } else {
              r.setProperty("status", hasProvider ? "ACTIVE" : "PERSISTED");
              r.setProperty("nodeCount", 0);
              r.setProperty("edgeCount", 0);
              r.setProperty("memoryUsageBytes", 0L);
            }

            context.setVariable("current", r);
          }
        }
      } finally {
        if (context.isProfiling()) {
          cost += (System.nanoTime() - begin);
        }
      }
    }
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return cursor < result.size();
      }

      @Override
      public Result next() {
        return result.get(cursor++);
      }

      @Override
      public void close() {
        // nothing to release — backing data is schema metadata held in memory
      }

      @Override
      public void reset() {
        cursor = 0;
      }
    };
  }

  private static List<String> jsonArrayToList(final JSONArray array) {
    if (array == null || array.length() == 0)
      return Collections.emptyList();
    final List<String> list = new ArrayList<>(array.length());
    for (int i = 0; i < array.length(); i++)
      list.add(array.getString(i));
    return list;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE METADATA GRAPH ANALYTICAL VIEWS";
    if (context.isProfiling()) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

}
