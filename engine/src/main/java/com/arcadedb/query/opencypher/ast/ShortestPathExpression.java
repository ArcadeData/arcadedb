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
import com.arcadedb.exception.CommandExecutionException;
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
 * - Both endpoints must already be bound to vertices in the current row
 * <p>
 * The last rule is what separates this from the {@code MATCH p = shortestPath(...)} spelling. That
 * one searches for an unbound endpoint, because the planner binds it with a node scan and then emits
 * one path per candidate; an expression yields a single value per row and cannot multiply rows that
 * way. An unbound endpoint here therefore raises rather than returning null, so the unsupported
 * shape stays distinguishable from a genuine "no path" answer.
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

    // Both endpoints must already be bound in this row. The expression form deliberately does not
    // search for an unbound endpoint: the MATCH form does that by having the planner bind it with a
    // node scan and then emitting one path per candidate, which multiplies rows, and an expression
    // yields a single value per row. Raising here keeps an unsupported shape distinguishable from a
    // genuine "no path" answer, which is null (issue #5496).
    if (startVar == null || startVar.isEmpty())
      throw unboundEndpoint("the start endpoint declares no variable");
    if (endVar == null || endVar.isEmpty())
      throw unboundEndpoint("the end endpoint declares no variable");
    // hasProperty rather than getPropertyNames().contains: the latter allocates a set of every name
    // on the row, twice, on the valid both-bound path. Both answer the same question here, including
    // for a name mapped to null, which is the distinction the null-propagation branch below relies on.
    if (!result.hasProperty(startVar))
      throw unboundEndpoint("'" + startVar + "' is not bound");
    if (!result.hasProperty(endVar))
      throw unboundEndpoint("'" + endVar + "' is not bound");

    final Object startValue = result.getProperty(startVar);
    final Object endValue = result.getProperty(endVar);

    if (!(startValue instanceof Vertex) || !(endValue instanceof Vertex)) {
      // The variable exists but does not hold a vertex, typically null from a non-matching OPTIONAL
      // MATCH. Null propagates through the expression as it does elsewhere in Cypher, so this stays a
      // null answer rather than an error: only a variable that is not bound at all is unsupported.
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

  /**
   * Builds the error raised when an endpoint is not bound to a vertex in the current row. The message
   * names the offending endpoint and the spelling that does support searching for it, because the
   * failure is a query-shape problem the author can fix rather than a data-dependent outcome.
   */
  private CommandExecutionException unboundEndpoint(final String detail) {
    return new CommandExecutionException((allPaths ? "allShortestPaths()" : "shortestPath()")
        + " as an expression requires both endpoints bound to vertices, but " + detail
        + ". Use MATCH p = shortestPath(...) to search for an unbound endpoint.");
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
