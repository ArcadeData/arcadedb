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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.function.graph.SQLFunctionShortestPath;

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

    // Get relationship type if specified
    String edgeType = null;
    if (pathPattern.getRelationships().size() == 1) {
      final RelationshipPattern rel = pathPattern.getRelationships().get(0);
      if (rel.getTypes() != null && !rel.getTypes().isEmpty()) {
        edgeType = rel.getTypes().get(0);
      }
    }

    // Get direction
    String direction = "BOTH";
    if (pathPattern.getRelationships().size() == 1) {
      final RelationshipPattern rel = pathPattern.getRelationships().get(0);
      switch (rel.getDirection()) {
        case OUT:
          direction = "OUT";
          break;
        case IN:
          direction = "IN";
          break;
        default:
          direction = "BOTH";
      }
    }

    // Use SQLFunctionShortestPath to compute the path (returns vertex RIDs only)
    final SQLFunctionShortestPath shortestPathFunction = new SQLFunctionShortestPath();
    final Object[] params = edgeType != null ?
        new Object[] { startVertex, endVertex, direction, edgeType } :
        new Object[] { startVertex, endVertex, direction };

    final List<RID> pathRids = shortestPathFunction.execute(null, null, null, params, context);

    if (pathRids == null || pathRids.isEmpty())
      return allPaths ? new ArrayList<>() : null;

    // Resolve vertex RIDs and find connecting edges to build a proper path
    final Vertex.DIRECTION vertexDirection;
    switch (direction) {
      case "OUT":
        vertexDirection = Vertex.DIRECTION.OUT;
        break;
      case "IN":
        vertexDirection = Vertex.DIRECTION.IN;
        break;
      default:
        vertexDirection = Vertex.DIRECTION.BOTH;
    }

    final List<Object> resolved = ShortestPathStep.resolvePathWithEdges(pathRids, vertexDirection, edgeType,
        context.getDatabase());

    if (allPaths) {
      // For allShortestPaths, we return a list containing the single shortest path
      // (In a complete implementation, this would find ALL paths of the same length)
      final List<Object> allPathsList = new ArrayList<>();
      allPathsList.add(resolved);
      return allPathsList;
    } else {
      // For shortestPath, return the single path
      return resolved;
    }
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
