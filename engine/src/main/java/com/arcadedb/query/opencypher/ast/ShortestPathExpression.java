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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.executor.steps.ShortestPathStep;
import com.arcadedb.query.opencypher.executor.steps.ShortestPathStep.EdgeConstraint;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.function.sql.graph.SQLFunctionShortestPath;

import java.util.ArrayList;
import java.util.List;

/**
 * Expression representing a shortestPath() or allShortestPaths() pattern in Cypher.
 * <p>
 * Syntax:
 * - shortestPath((a)-[:KNOWS*]-(b))
 * - allShortestPaths((a)-[:KNOWS*]-(b))
 * <p>
 * The path pattern must:
 * - Have exactly 2 nodes (start and end)
 * - Have exactly 1 relationship (with variable length allowed)
 * - Both endpoints must be bound to variables
 * <p>
 * This expression uses the existing SQLFunctionShortestPath for path computation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ShortestPathExpression implements Expression {
  private final PathPattern pathPattern;
  private final boolean allPaths;
  private final String text;

  public ShortestPathExpression(final PathPattern pathPattern, final boolean allPaths, final String text) {
    this.pathPattern = pathPattern;
    this.allPaths = allPaths;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    // Get the start and end vertices from the bound variables
    if (pathPattern.getNodes().size() != 2) {
      throw new IllegalArgumentException("shortestPath pattern must have exactly 2 nodes, got: " + pathPattern.getNodes().size());
    }

    final NodePattern startNode = pathPattern.getNodes().get(0);
    final NodePattern endNode = pathPattern.getNodes().get(1);

    // Get the bound vertex values from the result
    final String startVar = startNode.getVariable();
    final String endVar = endNode.getVariable();

    if (startVar == null || endVar == null) {
      throw new IllegalArgumentException("shortestPath endpoints must be bound to variables");
    }

    final Object startValue = result.getProperty(startVar);
    final Object endValue = result.getProperty(endVar);

    if (!(startValue instanceof Vertex) || !(endValue instanceof Vertex)) {
      // If either endpoint is not resolved, return null (no path)
      return null;
    }

    final Vertex startVertex = (Vertex) startValue;
    final Vertex endVertex = (Vertex) endValue;

    final RelationshipPattern relationship = pathPattern.getRelationships().size() == 1 ?
        pathPattern.getRelationships().get(0) :
        null;

    // Collect every relationship type declared in the pattern. With [:R1|R2*] all of them
    // must reach SQLFunctionShortestPath, otherwise paths that walk across more than one type
    // are silently dropped (issue #4190).
    List<String> edgeTypes = null;
    if (relationship != null && relationship.getTypes() != null && !relationship.getTypes().isEmpty())
      edgeTypes = relationship.getTypes();

    // Get direction. SQLFunctionShortestPath takes it as a string, the edge-aware traversals as an enum.
    final Direction patternDirection = relationship != null ? relationship.getDirection() : Direction.BOTH;
    final String direction;
    final Vertex.DIRECTION traversalDirection;
    switch (patternDirection) {
      case OUT:
        direction = "OUT";
        traversalDirection = Vertex.DIRECTION.OUT;
        break;
      case IN:
        direction = "IN";
        traversalDirection = Vertex.DIRECTION.IN;
        break;
      default:
        direction = "BOTH";
        traversalDirection = Vertex.DIRECTION.BOTH;
    }

    // The inline property map and the inline WHERE predicate constrain every relationship on the path.
    // SQLFunctionShortestPath only sees vertices, so a constrained pattern must run the edge-aware BFS
    // shared with the MATCH form instead.
    final EdgeConstraint constraint = EdgeConstraint.from(relationship, result, context);
    if (constraint != null) {
      final String[] typesArray = edgeTypes == null || edgeTypes.isEmpty() ? null : edgeTypes.toArray(new String[0]);
      final List<Object> filtered = ShortestPathStep.computeFilteredShortestPath(startVertex, endVertex,
          traversalDirection, typesArray, constraint);
      if (filtered == null || filtered.isEmpty())
        return allPaths ? new ArrayList<>() : null;
      // allShortestPaths() in expression position still yields the single shortest path found, matching the
      // unconstrained branch below; enumerating every co-shortest path here is a separate concern.
      return allPaths ? singlePathList(filtered) : filtered;
    }

    // Use SQLFunctionShortestPath to compute the path (returns vertex RIDs only).
    // Pass the type list as-is so multi-type alternation is preserved.
    final SQLFunctionShortestPath shortestPathFunction = new SQLFunctionShortestPath();
    final Object edgeTypeParam;
    if (edgeTypes == null || edgeTypes.isEmpty())
      edgeTypeParam = null;
    else if (edgeTypes.size() == 1)
      edgeTypeParam = edgeTypes.get(0);
    else
      edgeTypeParam = edgeTypes;

    final Object[] params = edgeTypeParam != null ?
        new Object[] { startVertex, endVertex, direction, edgeTypeParam } :
        new Object[] { startVertex, endVertex, direction };

    final List<RID> pathRids = shortestPathFunction.execute(null, null, null, params, context);

    if (pathRids == null || pathRids.isEmpty())
      return allPaths ? new ArrayList<>() : null;

    // Resolve vertex RIDs and find connecting edges to build a proper path
    final List<Object> resolved = ShortestPathStep.resolvePathWithEdges(pathRids, traversalDirection, edgeTypes,
        context.getDatabase());

    if (allPaths) {
      // For allShortestPaths, we return a list containing the single shortest path
      // (In a complete implementation, this would find ALL paths of the same length)
      return singlePathList(resolved);
    } else {
      // For shortestPath, return the single path
      return resolved;
    }
  }

  /**
   * Wraps a single path into the list shape allShortestPaths() returns in expression position.
   */
  private static List<Object> singlePathList(final List<Object> path) {
    final List<Object> allPathsList = new ArrayList<>(1);
    allPathsList.add(path);
    return allPathsList;
  }

  @Override
  public boolean isAggregation() {
    return false;
  }

  @Override
  public String getText() {
    return text;
  }

  public PathPattern getPathPattern() {
    return pathPattern;
  }

  public boolean isAllPaths() {
    return allPaths;
  }
}
