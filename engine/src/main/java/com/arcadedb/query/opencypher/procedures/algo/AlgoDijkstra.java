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
import com.arcadedb.function.sql.graph.SQLFunctionAstar;
import com.arcadedb.graph.EdgeWeight;
import com.arcadedb.graph.RelationshipPattern;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: algo.dijkstra(startNode, endNode, relTypesAndDirections, weightProperty, [defaultWeight])
 * <p>
 * Finds the shortest weighted path between two nodes using Dijkstra's algorithm.
 * </p>
 * <p>
 * {@code relTypesAndDirections} is APOC's relationship pattern ({@code 'ROAD>|<RAIL'}, {@code '>'}, a blank one for every
 * relationship), and the fifth argument APOC's {@code defaultWeight}, the weight of a relationship without the weight property
 * (issue #12103). A text fifth argument is still read as the direction ArcadeDB took there before, applied to the alternatives
 * without a direction marker.
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
    final RelationshipPattern pattern = extractRelationshipPattern(args[2], context);
    final String weightProperty = extractString(args[3], "weightProperty");

    // APOC's defaultWeight; APOC defaults it to NaN, which leaves ArcadeDB's rule, an edge without a weight weighs 1
    final Object fifth = argOrDefault(args, 4, null);
    Vertex.DIRECTION unmarked = Vertex.DIRECTION.BOTH;
    double missingWeight = EdgeWeight.MISSING;
    if (fifth instanceof Number number) {
      if (!Double.isNaN(number.doubleValue()))
        missingWeight = number.doubleValue();
    } else if (fifth != null)
      unmarked = parseDirection(extractString(fifth, "direction"));

    final Map<String, Object> options = new HashMap<>();
    options.put("emptyIfMaxDepth", true);

    // Use ArcadeDB's existing A* implementation (Dijkstra internally uses A*)
    final SQLFunctionAstar astar = new SQLFunctionAstar().setMissingWeight(missingWeight);
    configureSearch(astar, options, pattern, unmarked);
    final Object[] params = new Object[] { startNode, endNode, weightProperty, options };
    final LinkedList<RID> pathRids = astar.execute(null, null, null, params, context);

    if (pathRids == null || pathRids.isEmpty())
      return Stream.empty();

    // The A* implementation returns vertices only. Traverse the edges between consecutive
    // vertices to reconstruct the path's total weight and to expose the relationships.
    final WeightedPath weighted = attachEdges(pathRids, pattern, unmarked, weightProperty, missingWeight);

    // Build path representation including edges
    final TraversalPath path = buildPath(weighted.ridsWithEdges(), context.getDatabase());
    final double totalWeight = weighted.weight();

    final ResultInternal result = new ResultInternal();
    result.setProperty("path", path);
    result.setProperty("weight", totalWeight);

    return Stream.of(result);
  }
}
