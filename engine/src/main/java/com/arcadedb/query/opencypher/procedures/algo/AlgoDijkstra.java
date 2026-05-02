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

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.function.sql.graph.SQLFunctionAstar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.dijkstra(startNode, endNode, relTypes, weightProperty)
 * <p>
 * Finds the shortest weighted path between two nodes using Dijkstra's algorithm.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * MATCH (a:City {name: 'A'}), (b:City {name: 'B'})
 * CALL algo.dijkstra(a, b, 'ROAD', 'distance')
 * YIELD path, weight
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class AlgoDijkstra extends AbstractAlgoProcedure {
  public static final String NAME = "algo.dijkstra";

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
    return 5;
  }

  @Override
  public String getDescription() {
    return "Find the shortest weighted path between two nodes using Dijkstra's algorithm";
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
    final String direction = args.length > 4 ? extractString(args[4], "direction") : "BOTH";

    // Build options map for edge type filtering
    final Map<String, Object> options = new HashMap<>();
    options.put("emptyIfMaxDepth", true);
    if (direction != null && !direction.isEmpty())
      options.put("direction", direction);
    if (relType != null && !relType.isEmpty())
      options.put("edgeTypeNames", new String[] { relType });

    // Use ArcadeDB's existing A* implementation (Dijkstra internally uses A*)
    final SQLFunctionAstar astar = new SQLFunctionAstar();
    final Object[] params = new Object[]{startNode, endNode, weightProperty, options};
    final LinkedList<RID> pathRids = astar.execute(null, null, null, params, context);

    if (pathRids == null || pathRids.isEmpty())
      return Stream.empty();

    // The A* implementation returns vertices only. Traverse the edges between consecutive
    // vertices to reconstruct the path's total weight and to expose the relationships.
    final Database db = context.getDatabase();
    final Vertex.DIRECTION dir = parseDirection(direction);
    final String[] edgeTypeFilter = relType != null && !relType.isEmpty() ? new String[] { relType } : null;

    final List<RID> pathWithEdges = new ArrayList<>(pathRids.size() * 2 - 1);
    pathWithEdges.add(pathRids.get(0));

    double totalWeight = 0.0;
    for (int i = 0; i < pathRids.size() - 1; i++) {
      final Vertex from = pathRids.get(i).asVertex();
      final RID toRid = pathRids.get(i + 1);

      Edge bestEdge = null;
      double bestWeight = Double.POSITIVE_INFINITY;
      for (final Edge edge : edgeTypeFilter != null ? from.getEdges(dir, edgeTypeFilter) : from.getEdges(dir)) {
        final RID otherRid = edge.getOut().equals(from.getIdentity()) ? edge.getIn() : edge.getOut();
        if (!toRid.equals(otherRid))
          continue;
        final Object w = edge.get(weightProperty);
        final double edgeWeight = w instanceof Number num ? num.doubleValue() : 0.0;
        if (edgeWeight < bestWeight) {
          bestWeight = edgeWeight;
          bestEdge = edge;
        }
      }

      if (bestEdge != null) {
        totalWeight += bestWeight;
        pathWithEdges.add(bestEdge.getIdentity());
      }
      pathWithEdges.add(toRid);
    }

    // Build path representation including edges
    final Map<String, Object> path = buildPath(pathWithEdges, db);

    final ResultInternal result = new ResultInternal();
    result.setProperty("path", path);
    result.setProperty("weight", totalWeight);

    return Stream.of(result);
  }
}
