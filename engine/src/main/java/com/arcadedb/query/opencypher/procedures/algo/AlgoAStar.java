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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.function.sql.graph.SQLFunctionAstar;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.aStar(startNode, endNode, relTypes, weightProperty, latProperty, lonProperty)
 * <p>
 * Finds the shortest path between two nodes using the A* algorithm with geographic heuristics.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * MATCH (a:City {name: 'A'}), (b:City {name: 'B'})
 * CALL algo.aStar(a, b, 'ROAD', 'distance', 'lat', 'lon')
 * YIELD path, weight
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class AlgoAStar extends AbstractAlgoProcedure {
  public static final String NAME = "algo.astar";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 4;
  }

  @Override
  public int getMaxArgs() {
    return 6;
  }

  @Override
  public String getDescription() {
    return "Find the shortest path between two nodes using A* algorithm with optional geographic heuristics";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("path", "weight");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode = extractVertex(args[0], "startNode");
    final Vertex endNode = extractVertex(args[1], "endNode");
    final String relType = extractString(args[2], "relTypes");
    final String weightProperty = extractString(args[3], "weightProperty");

    // Build options map for A*
    final Map<String, Object> options = new HashMap<>();
    options.put("emptyIfMaxDepth", true);

    if (relType != null && !relType.isEmpty())
      options.put("edgeTypeNames", new String[] { relType });

    // Geographic heuristic coordinates
    if (args.length > 4 && args[4] != null && args.length > 5 && args[5] != null) {
      final String latProperty = extractString(args[4], "latProperty");
      final String lonProperty = extractString(args[5], "lonProperty");
      options.put("vertexAxisNames", new String[] { latProperty, lonProperty });
      options.put("heuristicFormula", "EUCLIDEAN");
    }

    // Use ArcadeDB's existing A* implementation
    final SQLFunctionAstar astar = new SQLFunctionAstar();
    final Object[] params = new Object[] { startNode, endNode, weightProperty, options };
    final LinkedList<RID> pathRids = astar.execute(null, null, null, params, context);

    if (pathRids == null || pathRids.isEmpty()) {
      return Stream.empty();
    }

    // Calculate total weight
    double totalWeight = 0.0;
    for (int i = 0; i < pathRids.size() - 1; i++) {
      final RID current = pathRids.get(i);
      final var currentDoc = context.getDatabase().lookupByRID(current, true);

      if (currentDoc.getRecord() instanceof Edge edge) {
        final Object weight = edge.get(weightProperty);
        if (weight instanceof Number num) {
          totalWeight += num.doubleValue();
        }
      }
    }

    // Build path representation
    final Map<String, Object> path = buildPath(pathRids, context.getDatabase());

    final ResultInternal result = new ResultInternal();
    result.setProperty("path", path);
    result.setProperty("weight", totalWeight);

    return Stream.of(result);
  }
}
