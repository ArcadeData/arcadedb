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
package com.arcadedb.function.sql.graph;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.function.sql.FunctionOptions;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * Dijkstra's algorithm describes how to find the cheapest path from one node to another node in a directed weighted graph.
 * <p>
 * The first parameter is source record. The second parameter is destination record. The third parameter is a name of property that
 * represents 'weight'.
 * <p>
 * If property is not defined in edge or is null, distance between vertexes are 0.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionDijkstra extends SQLFunctionPathFinder {
  public static final String NAME = "dijkstra";

  private static final Set<String> OPTIONS = Set.of("direction", "edgeTypeNames", "maxDepth", "emptyIfMaxDepth");

  public SQLFunctionDijkstra() {
    super(NAME);
  }

  public LinkedList<RID> execute(final Object thisObj, final Identifiable currentRecord, final Object currentResult,
      final Object[] params, final CommandContext context) {
    return new SQLFunctionAstar().execute(thisObj, currentRecord, currentResult, toAStarParams(params), context);
  }

  private Object[] toAStarParams(final Object[] params) {
    final Object[] result = new Object[4];
    result[0] = params[0];
    result[1] = params[1];
    result[2] = params[2];

    final Map<String, Object> options = new HashMap<>();
    options.put("emptyIfMaxDepth", true);

    if (params.length > 3 && params[3] != null) {
      if (params[3] instanceof Map<?, ?> rawMap) {
        // Validate against the set of dijkstra-specific option keys.
        final FunctionOptions opts = new FunctionOptions(NAME, rawMap, OPTIONS);
        if (opts.containsKey("direction"))
          options.put("direction", opts.getString("direction", null));
        if (opts.containsKey("edgeTypeNames"))
          options.put("edgeTypeNames", opts.get("edgeTypeNames"));
        if (opts.containsKey("maxDepth"))
          options.put("maxDepth", opts.getLong("maxDepth", Long.MAX_VALUE));
        if (opts.containsKey("emptyIfMaxDepth"))
          options.put("emptyIfMaxDepth", opts.getBoolean("emptyIfMaxDepth", true));
      } else {
        options.put("direction", params[3]);
      }
    }

    result[3] = options;
    return result;
  }

  public String getSyntax() {
    return "dijkstra(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>"
        + " [, <direction> | { direction, edgeTypeNames, maxDepth, emptyIfMaxDepth }])";
  }

  protected float getDistance(final Vertex node, final Vertex target) {
    return -1;//not used anymore
  }

  @Override
  protected boolean isVariableEdgeWeight() {
    return true;
  }
}
